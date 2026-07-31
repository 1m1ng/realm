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

/// An address no other test in this binary has been handed.
///
/// Binding port 0 asks the kernel for an ephemeral port and then releases it,
/// so two tests running in parallel can be handed the same one — whichever
/// endpoint binds it second fails with `AddrInUse`, and the test reading that
/// result sees `failed` where it expected an outcome of its own. Walking a
/// private range *below* the ephemeral one gives every caller a port to
/// itself, and keeps them clear of whatever the kernel hands out elsewhere.
fn free_addr() -> SocketAddr {
    use std::sync::atomic::{AtomicU16, Ordering};
    static NEXT: AtomicU16 = AtomicU16::new(0);

    for _ in 0..2000 {
        let port = 20000 + NEXT.fetch_add(1, Ordering::Relaxed) % 10000;
        let addr = SocketAddr::from(([127, 0, 0, 1], port));
        // somebody outside this process may still hold it
        if std::net::TcpListener::bind(addr).is_ok() {
            return addr;
        }
    }
    panic!("no free port in the test range");
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

/// Covers R12/R30 (finding #13): the freshly-bound control socket is owner-only
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

// ------------------------------------------------- certificate rotation ----
//
// Rotating a certificate replaces the bytes of a file the description names,
// and nothing else: the agent resubmits the same document under the next
// generation. The endpoint that references the rotated material must be
// rebuilt on it, every other endpoint must be left alone, and a rotation that
// produces unusable material must not take a serving listener down with it.

#[cfg(feature = "transport")]
mod rotation {
    use super::*;

    use std::process::{Command, Stdio};
    use std::sync::Once;

    /// The rustls provider is a process-wide singleton and installing it twice
    /// panics; realm's binary does it once at startup, so a test that builds a
    /// tls transport has to do the same.
    fn install_tls_provider() {
        static ONCE: Once = Once::new();
        ONCE.call_once(realm::core::kaminari::install_tls_provider);
    }

    /// Write a fresh self-signed leaf and its key to `cert`/`key`.
    ///
    /// Real material, generated per run: a certificate checked into the tree
    /// would eventually expire, and the acceptor really does parse what it is
    /// handed.
    fn self_signed(cert: &Path, key: &Path, cn: &str) {
        let out = Command::new("openssl")
            .args([
                "req",
                "-x509",
                "-newkey",
                "ec",
                "-pkeyopt",
                "ec_paramgen_curve:prime256v1",
                "-nodes",
                "-days",
                "3650",
                "-subj",
                &format!("/CN={}", cn),
                "-addext",
                "subjectAltName=DNS:example.com",
                "-keyout",
                key.to_str().unwrap(),
                "-out",
                cert.to_str().unwrap(),
            ])
            .output()
            .expect("openssl generates the test material");
        assert!(
            out.status.success(),
            "openssl failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }

    /// A leaf for `example.com` signed by the root in `ca`/`ca_key`.
    ///
    /// A trust anchor cannot double as the certificate a listener presents:
    /// rustls rejects that outright (`CaUsedAsEndEntity`), so any test that
    /// wants a connection to actually verify needs two certificates.
    fn leaf_signed_by(dir: &Path, ca: &Path, ca_key: &Path, cert: &Path, key: &Path) {
        let csr = dir.join("leaf.csr");
        let ext = dir.join("leaf.ext");
        std::fs::write(
            &ext,
            "subjectAltName=DNS:example.com\nbasicConstraints=critical,CA:FALSE\nextendedKeyUsage=serverAuth\n",
        )
        .unwrap();

        let openssl = |args: &[&str]| {
            let out = Command::new("openssl")
                .args(args)
                .output()
                .expect("openssl generates the test material");
            assert!(
                out.status.success(),
                "openssl {:?} failed: {}",
                args,
                String::from_utf8_lossy(&out.stderr)
            );
        };

        openssl(&[
            "req",
            "-newkey",
            "ec",
            "-pkeyopt",
            "ec_paramgen_curve:prime256v1",
            "-nodes",
            "-subj",
            "/CN=example.com",
            "-keyout",
            key.to_str().unwrap(),
            "-out",
            csr.to_str().unwrap(),
        ]);
        openssl(&[
            "x509",
            "-req",
            "-in",
            csr.to_str().unwrap(),
            "-CA",
            ca.to_str().unwrap(),
            "-CAkey",
            ca_key.to_str().unwrap(),
            "-set_serial",
            "1",
            "-days",
            "3650",
            "-extfile",
            ext.to_str().unwrap(),
            "-out",
            cert.to_str().unwrap(),
        ]);
    }

    /// The base64 body of the first certificate in a pem document.
    fn first_pem_cert(pem: &str) -> Option<String> {
        let body = pem.split_once("-----BEGIN CERTIFICATE-----")?.1;
        let body = body.split_once("-----END CERTIFICATE-----")?.0;
        Some(body.split_whitespace().collect())
    }

    /// The leaf a tls listener actually presents on the wire.
    async fn presented_leaf(addr: SocketAddr) -> String {
        // A blocking handshake: the test runtime is single-threaded, so this
        // has to leave the reactor free to drive realm's acceptor.
        tokio::task::spawn_blocking(move || {
            let out = Command::new("openssl")
                .args([
                    "s_client",
                    "-connect",
                    &addr.to_string(),
                    "-servername",
                    "example.com",
                    "-showcerts",
                ])
                .stdin(Stdio::null())
                .output()
                .expect("openssl s_client is available");

            first_pem_cert(&String::from_utf8_lossy(&out.stdout)).unwrap_or_else(|| {
                panic!(
                    "the listener presented no certificate: {}",
                    String::from_utf8_lossy(&out.stderr)
                )
            })
        })
        .await
        .expect("the handshake completes")
    }

    fn result_for<'a>(body: &'a Value, id: &str) -> &'a Value {
        body["results"]
            .as_array()
            .unwrap_or_else(|| panic!("results is an array: {}", body))
            .iter()
            .find(|r| r["id"] == id)
            .unwrap_or_else(|| panic!("no result for {}: {}", id, body))
    }

    /// A resubmission of the active generation replays its first answer, which
    /// is right for a genuine retry and wrong once the material behind the
    /// description has rotated: replaying "converged" for material the endpoint
    /// is not running is a false success. The digest is what tells the two
    /// apart — the documents are byte-identical either way.
    #[tokio::test]
    async fn resubmitting_a_generation_after_a_rotation_is_refused_not_replayed() {
        install_tls_provider();

        let dir = TempDir::new("ca-replay");
        let ca = dir.0.join("ca.pem");
        let unused_key = dir.0.join("ca.key");
        self_signed(&ca, &unused_key, "before rotation");

        let (socket, _shutdown) = serve(&dir, Reconciler::new()).await;
        let echo = spawn_echo("v1:").await;

        let endpoints = json!([{
            "id": "trusting",
            "listen": free_addr().to_string(),
            "remote": echo.to_string(),
            "remote_transport": format!("tls;sni=example.com;ca={}", ca.display()),
        }]);

        let (code, body) = call(
            &socket,
            "PUT",
            "/v1/desired-state",
            Some(&desired(1, endpoints.clone())),
        )
        .await;
        assert_eq!(code, 200, "{}", body);
        assert_eq!(result_for(&body, "trusting")["action"], "created");

        // a genuine retry: nothing moved, so the first answer is replayed
        let (code, body) = call(
            &socket,
            "PUT",
            "/v1/desired-state",
            Some(&desired(1, endpoints.clone())),
        )
        .await;
        assert_eq!(code, 200, "{}", body);
        assert_eq!(result_for(&body, "trusting")["action"], "created", "{}", body);

        // the same document, but the material under it has been replaced
        self_signed(&ca, &unused_key, "after rotation");

        let (code, body) = call(&socket, "PUT", "/v1/desired-state", Some(&desired(1, endpoints))).await;
        assert_eq!(
            code, 409,
            "a rotation under the active generation must not replay a converged answer: {}",
            body
        );
        assert_eq!(body["error"]["kind"], "stale-generation");
        assert_eq!(body["error"]["active_generation"], 1);
    }

    /// Replacing the bytes behind a client's trust anchor must rebuild the
    /// endpoint that names it — under a byte-identical description — and no
    /// other endpoint.
    #[tokio::test]
    async fn rotating_a_trust_anchor_rebuilds_only_the_referencing_endpoint() {
        install_tls_provider();

        let dir = TempDir::new("ca-rotation");
        let ca = dir.0.join("ca.pem");
        let unused_key = dir.0.join("ca.key");
        self_signed(&ca, &unused_key, "before rotation");

        let (socket, _shutdown) = serve(&dir, Reconciler::new()).await;
        let echo = spawn_echo("v1:").await;

        let endpoints = json!([
            {
                "id": "trusting",
                "listen": free_addr().to_string(),
                "remote": echo.to_string(),
                "remote_transport": format!("tls;sni=example.com;ca={}", ca.display()),
            },
            { "id": "bystander", "listen": free_addr().to_string(), "remote": echo.to_string() },
        ]);

        let (code, body) = call(
            &socket,
            "PUT",
            "/v1/desired-state",
            Some(&desired(1, endpoints.clone())),
        )
        .await;
        assert_eq!(code, 200, "{}", body);
        assert_eq!(result_for(&body, "trusting")["action"], "created");
        assert_eq!(result_for(&body, "bystander")["action"], "created");

        // the anchor is replaced in place: the description does not change
        self_signed(&ca, &unused_key, "after rotation");

        let (code, body) = call(&socket, "PUT", "/v1/desired-state", Some(&desired(2, endpoints))).await;
        assert_eq!(code, 200, "{}", body);
        assert_eq!(
            result_for(&body, "trusting")["action"],
            "updated",
            "the endpoint naming the rotated anchor must be rebuilt on it: {}",
            body
        );
        assert_eq!(
            result_for(&body, "bystander")["action"],
            "unchanged",
            "a rotation must not churn endpoints that name no material: {}",
            body
        );
    }

    /// A `ca=` naming a file that is not there must fail that endpoint and be
    /// reported as such. The alternative — building on the public bundle and
    /// reporting success — is a downgrade the agent has no way to notice.
    #[tokio::test]
    async fn an_absent_trust_anchor_fails_only_its_own_endpoint() {
        install_tls_provider();

        let dir = TempDir::new("ca-absent");
        let missing = dir.0.join("nowhere.pem");

        let (socket, _shutdown) = serve(&dir, Reconciler::new()).await;
        let echo = spawn_echo("v1:").await;
        let bystander_addr = free_addr();

        let (code, body) = call(
            &socket,
            "PUT",
            "/v1/desired-state",
            Some(&desired(
                1,
                json!([
                    {
                        "id": "trusting",
                        "listen": free_addr().to_string(),
                        "remote": echo.to_string(),
                        "remote_transport": format!("tls;sni=example.com;ca={}", missing.display()),
                    },
                    { "id": "bystander", "listen": bystander_addr.to_string(), "remote": echo.to_string() },
                ]),
            )),
        )
        .await;

        assert_eq!(code, 200, "a partial failure is still a processed request: {}", body);
        assert_eq!(
            body["state"], "partially-applied",
            "a generation one of whose endpoints failed is partially applied: {}",
            body
        );

        let trusting = result_for(&body, "trusting");
        assert_eq!(
            trusting["action"], "failed",
            "a trust anchor that is not there must fail the endpoint: {}",
            body
        );
        let error = trusting["error"].as_str().unwrap_or_else(|| panic!("{}", body));
        assert!(
            error.contains(&missing.display().to_string()),
            "the reported error must name the material: {}",
            error
        );

        assert_eq!(
            result_for(&body, "bystander")["action"],
            "created",
            "one endpoint's unusable material must not fail its siblings: {}",
            body
        );

        // the sibling is not merely reported as created, it is serving
        let mut probe = TcpStream::connect(bystander_addr).await.unwrap();
        probe.write_all(b"ping").await.unwrap();
        let mut buf = vec![0u8; 64];
        let n = timeout(Duration::from_secs(5), probe.read(&mut buf))
            .await
            .expect("the sibling answers")
            .unwrap();
        assert_eq!(&buf[..n], b"v1:ping");
    }

    /// The same for a server: after the rotation the listener presents the new
    /// leaf, which is the only thing a peer can actually observe. This is what
    /// a resolver cached by key path would defeat: the rebuilt acceptor would
    /// hand back the material the first construction happened to read.
    #[tokio::test]
    async fn rotating_a_leaf_makes_the_listener_present_it() {
        install_tls_provider();

        let dir = TempDir::new("leaf-rotation");
        let cert = dir.0.join("cert.pem");
        let key = dir.0.join("key.pem");
        self_signed(&cert, &key, "before rotation");

        let (socket, _shutdown) = serve(&dir, Reconciler::new()).await;
        let echo = spawn_echo("v1:").await;
        let laddr = free_addr();

        let endpoints = json!([{
            "id": "server",
            "listen": laddr.to_string(),
            "remote": echo.to_string(),
            "listen_transport": format!("tls;cert={};key={}", cert.display(), key.display()),
        }]);

        let (code, body) = call(
            &socket,
            "PUT",
            "/v1/desired-state",
            Some(&desired(1, endpoints.clone())),
        )
        .await;
        assert_eq!(code, 200, "{}", body);
        assert_eq!(result_for(&body, "server")["action"], "created");

        let first = first_pem_cert(&std::fs::read_to_string(&cert).unwrap()).expect("the test material is a pem cert");
        assert_eq!(presented_leaf(laddr).await, first, "the listener presents its leaf");

        // rotate both halves in place
        self_signed(&cert, &key, "after rotation");
        let second = first_pem_cert(&std::fs::read_to_string(&cert).unwrap()).expect("the test material is a pem cert");
        assert_ne!(first, second, "the rotation produced a different leaf");

        let (code, body) = call(&socket, "PUT", "/v1/desired-state", Some(&desired(2, endpoints))).await;
        assert_eq!(code, 200, "{}", body);
        assert_eq!(result_for(&body, "server")["action"], "updated", "{}", body);

        assert_eq!(
            presented_leaf(laddr).await,
            second,
            "the rebuilt acceptor must present the rotated leaf"
        );
    }

    /// A rotation that produced unusable material must fail that endpoint and
    /// leave its listener serving on the material it already has — and must not
    /// record the broken state as applied.
    ///
    /// The peer is a second endpoint terminating tls on the very leaf the
    /// anchor pins, so "still serving" is answered by real traffic over a
    /// verified connection rather than by the listener merely accepting.
    #[tokio::test]
    async fn a_corrupt_anchor_fails_the_endpoint_without_disturbing_its_listener() {
        install_tls_provider();

        let dir = TempDir::new("ca-corrupt");
        let ca = dir.0.join("ca.pem");
        let ca_key = dir.0.join("ca.key");
        self_signed(&ca, &ca_key, "realm test root");

        // the peer presents a leaf the anchor signed, and reads neither of the
        // two root files: corrupting what the client trusts must leave the
        // server's own material alone, or one rotation would fail both
        let leaf = dir.0.join("leaf.pem");
        let leaf_key = dir.0.join("leaf.key");
        leaf_signed_by(&dir.0, &ca, &ca_key, &leaf, &leaf_key);

        let intact = std::fs::read(&ca).unwrap();

        let (socket, _shutdown) = serve(&dir, Reconciler::new()).await;
        let echo = spawn_echo("v1:").await;
        let laddr = free_addr();
        let peer = free_addr();

        let endpoints = json!([
            {
                "id": "trusting",
                "listen": laddr.to_string(),
                "remote": peer.to_string(),
                "remote_transport": format!("tls;sni=example.com;ca={}", ca.display()),
            },
            {
                "id": "peer",
                "listen": peer.to_string(),
                "remote": echo.to_string(),
                "listen_transport": format!("tls;cert={};key={}", leaf.display(), leaf_key.display()),
            },
        ]);

        let (code, body) = call(
            &socket,
            "PUT",
            "/v1/desired-state",
            Some(&desired(1, endpoints.clone())),
        )
        .await;
        assert_eq!(code, 200, "{}", body);
        assert_eq!(result_for(&body, "trusting")["action"], "created");
        assert_eq!(result_for(&body, "peer")["action"], "created");

        let mut established = TcpStream::connect(laddr).await.unwrap();

        std::fs::write(&ca, b"this is not a certificate").unwrap();

        let (code, body) = call(
            &socket,
            "PUT",
            "/v1/desired-state",
            Some(&desired(2, endpoints.clone())),
        )
        .await;
        assert_eq!(code, 200, "{}", body);
        assert_eq!(
            result_for(&body, "trusting")["action"],
            "failed",
            "unusable material must fail the endpoint: {}",
            body
        );

        // the listener the failed rotation could not replace is still serving
        established.write_all(b"ping").await.unwrap();
        let mut buf = vec![0u8; 64];
        let n = timeout(Duration::from_secs(5), established.read(&mut buf))
            .await
            .expect("the established connection still answers")
            .unwrap();
        assert_eq!(&buf[..n], b"v1:ping");
        assert!(
            TcpStream::connect(laddr).await.is_ok(),
            "the listener is still accepting"
        );

        // the applied digest never moved to the broken material: putting the
        // original bytes back leaves nothing to do
        std::fs::write(&ca, &intact).unwrap();
        let (code, body) = call(&socket, "PUT", "/v1/desired-state", Some(&desired(3, endpoints))).await;
        assert_eq!(code, 200, "{}", body);
        assert_eq!(
            result_for(&body, "trusting")["action"],
            "unchanged",
            "the failed rotation must not have been recorded as applied: {}",
            body
        );
    }

    /// The invariant behind the scenario above, on material the acceptor does
    /// read today: a rebuild that cannot produce a working endpoint fails that
    /// endpoint, keeps the running listener, and is not recorded as applied.
    ///
    /// The last third is the one that needs construction to return an error
    /// rather than panic: a panic taken inside the shared resolver mutex used
    /// to poison it for the life of the process, which failed every later
    /// server construction in this binary, including this test's own final
    /// `unchanged`.
    #[tokio::test]
    async fn a_failed_rebuild_leaves_the_serving_listener_alone() {
        install_tls_provider();

        let dir = TempDir::new("leaf-corrupt");
        let cert = dir.0.join("cert.pem");
        let key = dir.0.join("key.pem");
        self_signed(&cert, &key, "serving");

        // unusable material under a path of its own: the rebuild has to be
        // driven by a change in the description, not only in the bytes
        let broken_cert = dir.0.join("broken.pem");
        let broken_key = dir.0.join("broken.key");
        std::fs::write(&broken_cert, b"this is not a certificate").unwrap();
        std::fs::write(&broken_key, b"this is not a private key").unwrap();

        let (socket, _shutdown) = serve(&dir, Reconciler::new()).await;
        let echo = spawn_echo("v1:").await;
        let laddr = free_addr();

        let working = json!([{
            "id": "server",
            "listen": laddr.to_string(),
            "remote": echo.to_string(),
            "listen_transport": format!("tls;cert={};key={}", cert.display(), key.display()),
        }]);
        let broken = json!([{
            "id": "server",
            "listen": laddr.to_string(),
            "remote": echo.to_string(),
            "listen_transport": format!("tls;cert={};key={}", broken_cert.display(), broken_key.display()),
        }]);

        let (code, body) = call(&socket, "PUT", "/v1/desired-state", Some(&desired(1, working.clone()))).await;
        assert_eq!(code, 200, "{}", body);
        assert_eq!(result_for(&body, "server")["action"], "created");

        let leaf = first_pem_cert(&std::fs::read_to_string(&cert).unwrap()).expect("the test material is a pem cert");
        assert_eq!(presented_leaf(laddr).await, leaf);

        let (code, body) = call(&socket, "PUT", "/v1/desired-state", Some(&desired(2, broken))).await;
        assert_eq!(code, 200, "{}", body);
        assert_eq!(
            result_for(&body, "server")["action"],
            "failed",
            "unusable material must fail the endpoint: {}",
            body
        );

        assert_eq!(
            presented_leaf(laddr).await,
            leaf,
            "the listener must keep serving the material it already has"
        );

        // the broken description was never recorded as applied
        let (code, body) = call(&socket, "PUT", "/v1/desired-state", Some(&desired(3, working))).await;
        assert_eq!(code, 200, "{}", body);
        assert_eq!(
            result_for(&body, "server")["action"],
            "unchanged",
            "the failed rebuild must not have been recorded as applied: {}",
            body
        );
    }
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
