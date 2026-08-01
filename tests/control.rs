//! Unix-socket control plane (U9 / R6, R11, R12, R22, R30, R31, R32, R36).
//!
//! Everything here goes over a real unix socket with real HTTP/1.1, the way an
//! agent (or `curl --unix-socket`) talks to it.

#![cfg(all(unix, feature = "control"))]

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, UnixStream};
use tokio::time::timeout;

use realm::conf::EndpointConf;
use realm::control::{CAPABILITIES, ControlServer, MAX_BODY_BYTES, SCHEMA_VERSION};
use realm::core::lifecycle::{CancellationToken, Reconciler};

/// A private directory for one test's socket, removed on drop.
struct TempDir(PathBuf);

impl TempDir {
    fn new(name: &str) -> Self {
        // A short base: macOS caps unix socket paths at ~104 bytes, and the CI
        // runner's $TMPDIR (`/var/folders/...`) is long enough that a socket
        // under it overflows `sun_path` and `bind` fails with EINVAL. `/tmp` is
        // short and always present on the unix platforms this file compiles for.
        let mut path = PathBuf::from("/tmp");
        path.push(format!("realm-control-{}-{}", name, std::process::id()));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }

    fn socket(&self) -> PathBuf {
        self.0.join("realm.sock")
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// One HTTP/1.1 request over the control socket.
async fn call(socket: &Path, method: &str, target: &str, body: Option<&[u8]>) -> (u16, Value) {
    let (code, raw) = call_raw(socket, method, target, body).await;
    let value = serde_json::from_slice(&raw).unwrap_or_else(|e| panic!("response is not json ({}): {:?}", e, raw));
    (code, value)
}

async fn call_raw(socket: &Path, method: &str, target: &str, body: Option<&[u8]>) -> (u16, Vec<u8>) {
    let mut stream = UnixStream::connect(socket).await.expect("control socket is reachable");

    let body = body.unwrap_or_default();
    let head = format!(
        "{} {} HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        method,
        target,
        body.len()
    );

    stream.write_all(head.as_bytes()).await.unwrap();
    stream.write_all(body).await.unwrap();
    stream.flush().await.unwrap();

    let mut raw = Vec::new();
    timeout(Duration::from_secs(5), stream.read_to_end(&mut raw))
        .await
        .expect("the control plane must answer")
        .unwrap();

    let split = raw
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .expect("response has headers");
    let head = String::from_utf8_lossy(&raw[..split]).into_owned();
    let body = raw[split + 4..].to_vec();

    let code: u16 = head
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|code| code.parse().ok())
        .expect("response has a status code");

    (code, body)
}

async fn spawn_echo(tag: &'static str) -> SocketAddr {
    let lis = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = lis.local_addr().unwrap();

    tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = lis.accept().await else {
                return;
            };
            tokio::spawn(async move {
                let mut buf = vec![0u8; 64];
                loop {
                    match stream.read(&mut buf).await {
                        Ok(0) | Err(_) => return,
                        Ok(n) => {
                            let mut answer = Vec::from(tag.as_bytes());
                            answer.extend_from_slice(&buf[..n]);
                            if stream.write_all(&answer).await.is_err() {
                                return;
                            }
                        }
                    }
                }
            });
        }
    });

    addr
}

fn free_addr() -> SocketAddr {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
}

/// Start a control plane over a fresh reconciler, and return its socket path.
async fn serve(dir: &TempDir, reconciler: Reconciler<EndpointConf>) -> (PathBuf, CancellationToken) {
    let socket = dir.socket();
    let shutdown = CancellationToken::new();

    let server = ControlServer::new(reconciler.spawn(), Default::default(), &socket);
    let listener = server.bind().await.expect("control socket binds");
    tokio::spawn(server.serve(listener, shutdown.clone()));

    (socket, shutdown)
}

fn desired(generation: u64, endpoints: Value) -> Vec<u8> {
    serde_json::to_vec(&json!({ "generation": generation, "endpoints": endpoints })).unwrap()
}

/// Covers R22/R32: an agent can tell this fork from upstream realm and learn
/// which contract version it speaks.
#[tokio::test]
async fn version_reports_the_contract_and_capabilities() {
    let dir = TempDir::new("version");
    let (socket, _shutdown) = serve(&dir, Reconciler::new()).await;

    let (code, body) = call(&socket, "GET", "/v1/version", None).await;
    assert_eq!(code, 200);
    assert_eq!(body["schema_version"], SCHEMA_VERSION);
    assert_eq!(body["implementation"], "realm-hot-reload-fork");

    let advertised: Vec<String> = body["capabilities"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c.as_str().unwrap().to_string())
        .collect();
    for capability in CAPABILITIES {
        assert!(advertised.contains(&capability.to_string()), "missing {}", capability);
    }

    // the same document answers on /v1/capabilities
    let (code, same) = call(&socket, "GET", "/v1/capabilities", None).await;
    assert_eq!(code, 200);
    assert_eq!(same["schema_version"], body["schema_version"]);
}

/// Covers AE14 over http (R6, R11, R36): a desired state applied over the
/// socket really forwards, and the status shows live connections and the
/// draining cohort a replacement leaves behind.
#[tokio::test]
async fn a_desired_state_applies_and_shows_up_in_the_status() {
    let dir = TempDir::new("apply");
    let (socket, _shutdown) = serve(&dir, Reconciler::new()).await;

    let echo1 = spawn_echo("v1:").await;
    let echo2 = spawn_echo("v2:").await;
    let laddr = free_addr();

    let (code, body) = call(
        &socket,
        "PUT",
        "/v1/desired-state",
        Some(&desired(
            1,
            json!([{ "id": "rule-1", "listen": laddr.to_string(), "remote": echo1.to_string() }]),
        )),
    )
    .await;

    assert_eq!(code, 200, "{}", body);
    assert_eq!(body["state"], "applied");
    assert_eq!(body["results"][0]["id"], "rule-1");
    assert_eq!(body["results"][0]["protocol"], "tcp");
    assert_eq!(body["results"][0]["action"], "created");

    // it forwards
    let mut established = TcpStream::connect(laddr).await.unwrap();
    established.write_all(b"ping").await.unwrap();
    let mut buf = vec![0u8; 64];
    let n = established.read(&mut buf).await.unwrap();
    assert_eq!(&buf[..n], b"v1:ping");

    let (code, body) = call(&socket, "GET", "/v1/status", None).await;
    assert_eq!(code, 200);
    assert_eq!(body["active_generation"], 1);
    assert_eq!(body["generation_state"], "applied");
    assert_eq!(body["ready"], true);
    let slot = &body["endpoints"][0]["slots"][0];
    assert_eq!(slot["state"], "running");
    assert_eq!(slot["listen"], laddr.to_string());
    assert_eq!(slot["connections"], 1);
    assert!(body["process"]["version"].is_string(), "process settings are exposed");

    // an update leaves the established connection on the old generation, and
    // the status shows that cohort
    let (code, body) = call(
        &socket,
        "PUT",
        "/v1/desired-state",
        Some(&desired(
            2,
            json!([{ "id": "rule-1", "listen": laddr.to_string(), "remote": echo2.to_string() }]),
        )),
    )
    .await;
    assert_eq!(code, 200);
    assert_eq!(body["results"][0]["action"], "updated");

    let (_, body) = call(&socket, "GET", "/v1/status", None).await;
    let slot = &body["endpoints"][0]["slots"][0];
    assert_eq!(slot["generation"], 2);
    let draining = slot["draining"].as_array().unwrap();
    assert_eq!(draining.len(), 1, "the superseded cohort is visible");
    assert_eq!(draining[0]["generation"], 1);
    assert_eq!(draining[0]["connections"], 1);
    assert!(draining[0]["age_secs"].as_f64().unwrap() >= 0.0);

    // the old connection still talks to the old remote
    established.write_all(b"still").await.unwrap();
    let n = established.read(&mut buf).await.unwrap();
    assert_eq!(&buf[..n], b"v1:still");
}

/// Covers R31: a stale generation is a terminal conflict that names the active
/// generation.
#[tokio::test]
async fn a_stale_generation_is_a_terminal_conflict() {
    let dir = TempDir::new("stale");
    let (socket, _shutdown) = serve(&dir, Reconciler::new()).await;

    let echo = spawn_echo("v1:").await;
    let laddr = free_addr();
    let endpoints = json!([{ "id": "rule-1", "listen": laddr.to_string(), "remote": echo.to_string() }]);

    call(
        &socket,
        "PUT",
        "/v1/desired-state",
        Some(&desired(9, endpoints.clone())),
    )
    .await;

    let (code, body) = call(&socket, "PUT", "/v1/desired-state", Some(&desired(8, endpoints))).await;
    assert_eq!(code, 409);
    assert_eq!(body["error"]["kind"], "stale-generation");
    assert_eq!(body["error"]["retryable"], false);
    assert_eq!(body["error"]["active_generation"], 9);
}

/// Covers AE13 on the http side: while the snapshot is being restored, the
/// control plane answers not-ready and marks it retryable.
#[tokio::test]
async fn submissions_before_readiness_are_retryable() {
    let dir = TempDir::new("not-ready");
    let (socket, _shutdown) = serve(&dir, Reconciler::not_ready()).await;

    let echo = spawn_echo("v1:").await;
    let laddr = free_addr();
    let endpoints = json!([{ "id": "rule-1", "listen": laddr.to_string(), "remote": echo.to_string() }]);

    let (code, body) = call(
        &socket,
        "PUT",
        "/v1/desired-state",
        Some(&desired(1, endpoints.clone())),
    )
    .await;
    assert_eq!(code, 503);
    assert_eq!(body["error"]["kind"], "not-ready");
    assert_eq!(body["error"]["retryable"], true);

    let (code, body) = call(&socket, "GET", "/v1/readiness", None).await;
    assert_eq!(code, 503);
    assert_eq!(body["ready"], false);
}

/// Covers R4/R9 over http: an invalid endpoint is reported per endpoint while
/// the rest of the generation applies.
#[tokio::test]
async fn an_invalid_endpoint_is_reported_per_endpoint() {
    let dir = TempDir::new("invalid");
    let (socket, _shutdown) = serve(&dir, Reconciler::new()).await;

    let echo = spawn_echo("v1:").await;
    let good = free_addr();

    let (code, body) = call(
        &socket,
        "PUT",
        "/v1/desired-state",
        Some(&desired(
            1,
            json!([
                { "id": "good", "listen": good.to_string(), "remote": echo.to_string() },
                { "id": "bad", "listen": "not an address", "remote": echo.to_string() },
            ]),
        )),
    )
    .await;

    assert_eq!(code, 200, "a partial failure is still a processed request");
    assert_eq!(body["state"], "partially-applied");

    let results = body["results"].as_array().unwrap();
    let bad = results.iter().find(|r| r["id"] == "bad").unwrap();
    assert_eq!(bad["action"], "failed");
    assert!(bad["error"].as_str().unwrap().contains("listen"));

    let good_result = results.iter().find(|r| r["id"] == "good").unwrap();
    assert_eq!(good_result["action"], "created");
}

/// Covers R31 per endpoint: an agent must be able to tell a failure worth
/// retrying from one that will fail again unchanged.
#[tokio::test]
async fn endpoint_failures_say_whether_a_retry_can_help() {
    let dir = TempDir::new("retryable");
    let (socket, _shutdown) = serve(&dir, Reconciler::new()).await;

    let echo = spawn_echo("v1:").await;

    // somebody else owns this port: a bind race, which can resolve on its own
    let taken = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let taken_addr = taken.local_addr().unwrap();

    let (code, body) = call(
        &socket,
        "PUT",
        "/v1/desired-state",
        Some(&desired(
            1,
            json!([
                { "id": "occupied", "listen": taken_addr.to_string(), "remote": echo.to_string() },
                { "id": "nonsense", "listen": "not an address", "remote": echo.to_string() },
            ]),
        )),
    )
    .await;

    assert_eq!(code, 200);
    let results = body["results"].as_array().unwrap();

    let occupied = results.iter().find(|r| r["id"] == "occupied").unwrap();
    assert_eq!(occupied["action"], "failed");
    assert_eq!(
        occupied["retryable"], true,
        "a lost bind race may resolve itself: {}",
        occupied
    );

    let nonsense = results.iter().find(|r| r["id"] == "nonsense").unwrap();
    assert_eq!(nonsense["action"], "failed");
    assert_eq!(
        nonsense["retryable"], false,
        "an unparseable endpoint will not parse on a retry: {}",
        nonsense
    );

    // a successful result carries no retryable field at all
    let (_, body) = call(
        &socket,
        "PUT",
        "/v1/desired-state",
        Some(&desired(
            2,
            json!([{ "id": "fine", "listen": free_addr().to_string(), "remote": echo.to_string() }]),
        )),
    )
    .await;
    assert_eq!(body["results"][0]["action"], "created");
    assert!(body["results"][0].get("retryable").is_none());
}

/// Covers R12: the control plane refuses to allocate without bound.
#[tokio::test]
async fn an_oversized_request_is_refused_as_terminal() {
    let dir = TempDir::new("too-large");
    let (socket, _shutdown) = serve(&dir, Reconciler::new()).await;

    let huge = vec![b'x'; MAX_BODY_BYTES + 1024];
    let (code, body) = call(&socket, "PUT", "/v1/desired-state", Some(&huge)).await;

    assert_eq!(code, 413);
    assert_eq!(body["error"]["kind"], "request-too-large");
    assert_eq!(body["error"]["retryable"], false);
}

/// A malformed body is a terminal client error, and an unknown route is a 404.
#[tokio::test]
async fn malformed_requests_and_unknown_routes_are_terminal() {
    let dir = TempDir::new("malformed");
    let (socket, _shutdown) = serve(&dir, Reconciler::new()).await;

    let (code, body) = call(&socket, "PUT", "/v1/desired-state", Some(b"{ not json")).await;
    assert_eq!(code, 400);
    assert_eq!(body["error"]["kind"], "malformed-request");
    assert_eq!(body["error"]["retryable"], false);

    let (code, body) = call(&socket, "GET", "/v1/nope", None).await;
    assert_eq!(code, 404);
    assert_eq!(body["error"]["kind"], "unknown-route");
}

/// Covers R12/R30: the socket is owner-only, and so is a directory realm
/// creates for it.
#[tokio::test]
async fn the_socket_is_owner_only() {
    use std::os::unix::fs::PermissionsExt;

    let dir = TempDir::new("perms");
    // a path realm has to create itself
    let socket = dir.0.join("run").join("realm.sock");

    let shutdown = CancellationToken::new();
    let server = ControlServer::new(Reconciler::<EndpointConf>::new().spawn(), Default::default(), &socket);
    let listener = server.bind().await.expect("control socket binds");
    tokio::spawn(server.serve(listener, shutdown));

    let mode = std::fs::metadata(&socket).unwrap().permissions().mode() & 0o777;
    assert_eq!(mode, 0o700, "the socket must not be reachable by others");

    let dir_mode = std::fs::metadata(socket.parent().unwrap())
        .unwrap()
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(
        dir_mode, 0o700,
        "a directory realm creates must not be traversable by others"
    );

    let (code, _) = call(&socket, "GET", "/v1/version", None).await;
    assert_eq!(code, 200);
}

/// An existing directory is never tightened behind the operator's back: a
/// control socket in a shared directory like /run must not take that directory
/// away from everybody else.
#[tokio::test]
async fn an_existing_directory_keeps_its_permissions() {
    use std::os::unix::fs::PermissionsExt;

    let dir = TempDir::new("shared-dir");
    let shared = dir.0.join("shared");
    std::fs::create_dir_all(&shared).unwrap();
    std::fs::set_permissions(&shared, std::fs::Permissions::from_mode(0o755)).unwrap();

    let socket = shared.join("realm.sock");
    let shutdown = CancellationToken::new();
    let server = ControlServer::new(Reconciler::<EndpointConf>::new().spawn(), Default::default(), &socket);
    let listener = server.bind().await.expect("binding in a shared directory works");
    tokio::spawn(server.serve(listener, shutdown));

    let mode = std::fs::metadata(&shared).unwrap().permissions().mode() & 0o777;
    assert_eq!(mode, 0o755, "an existing directory keeps the permissions it had");

    // the socket itself is still owner-only
    let socket_mode = std::fs::metadata(&socket).unwrap().permissions().mode() & 0o777;
    assert_eq!(socket_mode, 0o700);

    let (code, _) = call(&socket, "GET", "/v1/version", None).await;
    assert_eq!(code, 200);
}

/// The freshly-bound control socket is owner-only
/// at creation, even under a permissive umask and in a group/world-reachable
/// parent directory. Binding under `umask(0)` (the state a daemonized realm is
/// in) would otherwise create the socket 0o777 — world-connectable in the
/// window before any chmod could tighten it. The umask guard around `bind` is
/// what closes that window; this asserts the socket's creation-time mode, so it
/// exercises the guard rather than a post-hoc chmod.
#[tokio::test]
async fn the_socket_is_owner_only_under_a_permissive_umask() {
    use std::os::unix::fs::PermissionsExt;

    let dir = TempDir::new("permissive-umask");
    // a shared, world-reachable parent directory, like /run or /tmp
    let shared = dir.0.join("shared");
    std::fs::create_dir_all(&shared).unwrap();
    std::fs::set_permissions(&shared, std::fs::Permissions::from_mode(0o755)).unwrap();
    let socket = shared.join("realm.sock");

    // the most permissive umask possible: without the guard the socket would be
    // created 0o777
    let previous = unsafe { libc::umask(0) };

    let shutdown = CancellationToken::new();
    let server = ControlServer::new(Reconciler::<EndpointConf>::new().spawn(), Default::default(), &socket);
    let listener = server.bind().await.expect("control socket binds");

    // restore before asserting, so a failing assertion cannot leak umask 0 into
    // the rest of the process
    unsafe { libc::umask(previous) };

    tokio::spawn(server.serve(listener, shutdown));

    let mode = std::fs::metadata(&socket).unwrap().permissions().mode() & 0o777;
    assert_eq!(
        mode, 0o700,
        "the socket must be owner-only at creation even under a permissive umask"
    );

    let (code, _) = call(&socket, "GET", "/v1/version", None).await;
    assert_eq!(code, 200);
}

/// A client that connects and never finishes sending its request headers must
/// not pin a control-plane task open forever: the header-read timeout closes
/// the stalled connection on its own (a residual DoS otherwise).
#[tokio::test]
async fn a_stalled_connection_is_closed_by_the_header_timeout() {
    let dir = TempDir::new("idle");
    let (socket, _shutdown) = serve(&dir, Reconciler::new()).await;

    let mut stream = UnixStream::connect(&socket).await.expect("control socket is reachable");
    // a request line but never the blank line that ends the headers, so the
    // server is left waiting for the rest of the request
    stream.write_all(b"GET /v1/version HTTP/1.1\r\n").await.unwrap();
    stream.flush().await.unwrap();

    // without the header-read timeout this read would block until the outer
    // timeout trips; with it, the server closes the stalled connection itself
    let mut buf = Vec::new();
    let closed = timeout(Duration::from_secs(30), stream.read_to_end(&mut buf)).await;
    assert!(
        closed.is_ok(),
        "a stalled connection must be closed by the header-read timeout, not held open forever"
    );
}

/// Covers R30: a socket left behind by a crashed process is replaced, but a
/// live one is never stolen.
#[tokio::test]
async fn a_stale_socket_is_replaced_and_a_live_one_is_not() {
    let dir = TempDir::new("stale-socket");
    let socket = dir.socket();

    // a leftover file that nothing is listening on
    std::fs::write(&socket, b"leftover").unwrap();

    let shutdown = CancellationToken::new();
    let server = ControlServer::new(Reconciler::new().spawn(), Default::default(), &socket);
    let listener = server.bind().await.expect("a stale socket must be replaced");
    tokio::spawn(server.serve(listener, shutdown.clone()));

    let (code, _) = call(&socket, "GET", "/v1/version", None).await;
    assert_eq!(code, 200);

    // a second server on the same path must refuse, not steal it
    let second = ControlServer::new(Reconciler::<EndpointConf>::new().spawn(), Default::default(), &socket);
    let err = second.bind().await.expect_err("a live socket must not be stolen");
    assert_eq!(err.kind(), std::io::ErrorKind::AddrInUse);
    assert!(err.to_string().contains("already"), "{}", err);

    // the first one is still serving
    let (code, _) = call(&socket, "GET", "/v1/version", None).await;
    assert_eq!(code, 200);
}

/// Covers R15 over http: an endpoint removed from the desired state releases
/// its port.
#[tokio::test]
async fn removing_an_endpoint_releases_its_port() {
    let dir = TempDir::new("remove");
    let (socket, _shutdown) = serve(&dir, Reconciler::new()).await;

    let echo = spawn_echo("v1:").await;
    let laddr = free_addr();

    call(
        &socket,
        "PUT",
        "/v1/desired-state",
        Some(&desired(
            1,
            json!([{ "id": "rule-1", "listen": laddr.to_string(), "remote": echo.to_string() }]),
        )),
    )
    .await;

    let (code, body) = call(&socket, "PUT", "/v1/desired-state", Some(&desired(2, json!([])))).await;
    assert_eq!(code, 200);
    assert_eq!(body["results"][0]["action"], "deleted");

    assert!(TcpListener::bind(laddr).await.is_ok(), "the port was released");
}
