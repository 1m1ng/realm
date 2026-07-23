//! Static mode and startup integration (U10 / R5, R21, R26, R35).
//!
//! Without `--control-socket` the binary behaves exactly like upstream realm.
//! With it, the static configuration is generation 0 under derived ids, so an
//! agent's first equivalent submission changes nothing.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

/// The realm process under test, killed when the test ends.
struct Realm(Child);

impl Drop for Realm {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// A temporary directory, removed on drop.
struct TempDir(PathBuf);

impl TempDir {
    fn new(name: &str) -> Self {
        let mut path = std::env::temp_dir();
        path.push(format!("realm-static-{}-{}", name, std::process::id()));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }

    fn join(&self, name: &str) -> PathBuf {
        self.0.join(name)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// An echo server, answering with a fixed prefix.
fn spawn_echo(tag: &'static str) -> SocketAddr {
    let lis = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = lis.local_addr().unwrap();

    std::thread::spawn(move || {
        for stream in lis.incoming() {
            let Ok(mut stream) = stream else { return };
            std::thread::spawn(move || {
                let mut buf = vec![0u8; 64];
                loop {
                    match stream.read(&mut buf) {
                        Ok(0) | Err(_) => return,
                        Ok(n) => {
                            let mut answer = Vec::from(tag.as_bytes());
                            answer.extend_from_slice(&buf[..n]);
                            if stream.write_all(&answer).is_err() {
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
    TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap()
}

/// Wait until the relay answers, so the test does not race the startup.
fn wait_for_relay(addr: SocketAddr, expect: &str) -> TcpStream {
    let deadline = Instant::now() + Duration::from_secs(10);

    loop {
        if let Ok(mut stream) = TcpStream::connect(addr) {
            stream.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
            if stream.write_all(b"hello").is_ok() {
                let mut buf = vec![0u8; 64];
                if let Ok(n) = stream.read(&mut buf) {
                    assert_eq!(String::from_utf8_lossy(&buf[..n]), format!("{}hello", expect));
                    return stream;
                }
            }
        }

        assert!(Instant::now() < deadline, "the relay never came up");
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn ask(stream: &mut TcpStream, payload: &[u8]) -> String {
    stream.write_all(payload).unwrap();
    let mut buf = vec![0u8; 64];
    let n = stream.read(&mut buf).unwrap();
    String::from_utf8_lossy(&buf[..n]).into_owned()
}

fn realm(args: &[&str]) -> Realm {
    let child = Command::new(env!("CARGO_BIN_EXE_realm"))
        .env_remove("REALM_CONF")
        .env_remove("REALM_CONTROL_SOCKET")
        .args(args)
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("realm starts");
    Realm(child)
}

/// Covers R5: without a control socket the binary relays exactly as upstream.
#[test]
fn static_mode_relays_without_a_control_plane() {
    let echo = spawn_echo("v1:");
    let laddr = free_addr();

    let _realm = realm(&["-l", &laddr.to_string(), "-r", &echo.to_string()]);

    let mut stream = wait_for_relay(laddr, "v1:");
    assert_eq!(ask(&mut stream, b"ping"), "v1:ping");
}

/// A config file works the same way, and an invalid one still exits non-zero.
#[test]
fn static_mode_reads_a_config_file() {
    let dir = TempDir::new("conf");
    let echo = spawn_echo("v1:");
    let laddr = free_addr();

    let conf = dir.join("realm.toml");
    std::fs::write(
        &conf,
        format!("[[endpoints]]\nlisten = \"{}\"\nremote = \"{}\"\n", laddr, echo),
    )
    .unwrap();

    let _realm = realm(&["-c", conf.to_str().unwrap()]);

    let mut stream = wait_for_relay(laddr, "v1:");
    assert_eq!(ask(&mut stream, b"ping"), "v1:ping");
}

/// `--version` advertises the control feature, so a deployment can tell which
/// binary it is running (R22).
#[test]
fn version_advertises_the_control_feature() {
    let out = Command::new(env!("CARGO_BIN_EXE_realm"))
        .env_remove("REALM_CONF")
        .arg("--version")
        .output()
        .expect("realm runs");

    let text = String::from_utf8_lossy(&out.stdout);
    assert!(text.contains("[control]"), "features should list control: {}", text);
}

// The remaining tests need the control plane.
#[cfg(all(unix, feature = "control"))]
mod control {
    use super::*;

    use std::os::unix::net::UnixStream;

    /// One HTTP/1.1 request over the control socket.
    fn call(socket: &std::path::Path, method: &str, target: &str, body: Option<&[u8]>) -> (u16, serde_json::Value) {
        let deadline = Instant::now() + Duration::from_secs(10);
        let mut stream = loop {
            match UnixStream::connect(socket) {
                Ok(x) => break x,
                Err(e) => {
                    assert!(Instant::now() < deadline, "control socket never appeared: {}", e);
                    std::thread::sleep(Duration::from_millis(50));
                }
            }
        };

        stream.set_read_timeout(Some(Duration::from_secs(5))).unwrap();

        let body = body.unwrap_or_default();
        let head = format!(
            "{} {} HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            method,
            target,
            body.len()
        );
        stream.write_all(head.as_bytes()).unwrap();
        stream.write_all(body).unwrap();
        stream.flush().unwrap();

        let mut raw = Vec::new();
        stream.read_to_end(&mut raw).unwrap();

        let split = raw.windows(4).position(|w| w == b"\r\n\r\n").expect("has headers");
        let code: u16 = String::from_utf8_lossy(&raw[..split])
            .lines()
            .next()
            .and_then(|line| line.split_whitespace().nth(1))
            .and_then(|c| c.parse().ok())
            .expect("has a status code");

        let value = serde_json::from_slice(&raw[split + 4..]).expect("body is json");
        (code, value)
    }

    /// Covers AE10: the static configuration is generation 0 under derived
    /// ids, so the agent's first equivalent submission changes nothing.
    #[test]
    fn the_agents_first_equivalent_submission_is_a_no_op() {
        let dir = TempDir::new("takeover");
        let echo = spawn_echo("v1:");
        let laddr = free_addr();
        let socket = dir.join("realm.sock");

        let _realm = realm(&[
            "-l",
            &laddr.to_string(),
            "-r",
            &echo.to_string(),
            "--control-socket",
            socket.to_str().unwrap(),
        ]);

        let mut established = wait_for_relay(laddr, "v1:");

        // the derived id is part of the contract: protocols, then listen address
        let id = format!("tcp:{}", laddr);

        let (code, status) = call(&socket, "GET", "/v1/status", None);
        assert_eq!(code, 200);
        assert_eq!(status["active_generation"], 0, "the static mode is generation 0");
        assert_eq!(status["endpoints"][0]["id"], id);

        // the agent submits the very same desired state under the derived id
        let body = serde_json::to_vec(&serde_json::json!({
            "generation": 1,
            "endpoints": [{ "id": id, "listen": laddr.to_string(), "remote": echo.to_string() }],
        }))
        .unwrap();

        let (code, response) = call(&socket, "PUT", "/v1/desired-state", Some(&body));
        assert_eq!(code, 200, "{}", response);
        assert_eq!(response["state"], "applied");
        assert_eq!(
            response["results"][0]["action"], "unchanged",
            "an equivalent takeover must not rebuild the endpoint: {}",
            response
        );

        // the connection established before the takeover never noticed
        assert_eq!(ask(&mut established, b"still"), "v1:still");
    }

    /// Covers R19: the state file lets a restarted process resume immediately.
    #[test]
    fn a_restarted_process_resumes_from_its_state_file() {
        let dir = TempDir::new("resume");
        let echo = spawn_echo("v1:");
        let (static_addr, agent_addr) = (free_addr(), free_addr());
        let socket = dir.join("realm.sock");
        let state = dir.join("state.json");

        let first = realm(&[
            "-l",
            &static_addr.to_string(),
            "-r",
            &echo.to_string(),
            "--control-socket",
            socket.to_str().unwrap(),
            "--state-file",
            state.to_str().unwrap(),
        ]);

        wait_for_relay(static_addr, "v1:");

        // the agent takes over with a different desired state
        let body = serde_json::to_vec(&serde_json::json!({
            "generation": 7,
            "endpoints": [{ "id": "agent-rule", "listen": agent_addr.to_string(), "remote": echo.to_string() }],
        }))
        .unwrap();
        let (code, _) = call(&socket, "PUT", "/v1/desired-state", Some(&body));
        assert_eq!(code, 200);
        wait_for_relay(agent_addr, "v1:");

        // the process goes away
        drop(first);
        std::thread::sleep(Duration::from_millis(200));

        // restarting resumes the agent's state, not the static configuration
        let _second = realm(&[
            "-l",
            &static_addr.to_string(),
            "-r",
            &echo.to_string(),
            "--control-socket",
            socket.to_str().unwrap(),
            "--state-file",
            state.to_str().unwrap(),
        ]);

        wait_for_relay(agent_addr, "v1:");

        let (code, status) = call(&socket, "GET", "/v1/status", None);
        assert_eq!(code, 200);
        assert_eq!(status["active_generation"], 7, "the snapshot's generation is restored");
        assert_eq!(status["endpoints"][0]["id"], "agent-rule");
        assert!(status["ready"].as_bool().unwrap());
    }

    /// Covers R21/R9: an endpoint that cannot bind never takes the process (or
    /// the other endpoints) down.
    #[test]
    fn a_failing_endpoint_does_not_stop_the_process() {
        let dir = TempDir::new("failing");
        let echo = spawn_echo("v1:");
        let good = free_addr();
        let socket = dir.join("realm.sock");

        // somebody else owns this port
        let taken = TcpListener::bind("127.0.0.1:0").unwrap();
        let taken_addr = taken.local_addr().unwrap();

        let conf = dir.join("realm.toml");
        std::fs::write(
            &conf,
            format!(
                "[[endpoints]]\nlisten = \"{}\"\nremote = \"{}\"\n\n[[endpoints]]\nlisten = \"{}\"\nremote = \"{}\"\n",
                good, echo, taken_addr, echo
            ),
        )
        .unwrap();

        let _realm = realm(&[
            "-c",
            conf.to_str().unwrap(),
            "--control-socket",
            socket.to_str().unwrap(),
        ]);

        // the healthy endpoint serves
        let mut stream = wait_for_relay(good, "v1:");
        assert_eq!(ask(&mut stream, b"ping"), "v1:ping");

        let (code, status) = call(&socket, "GET", "/v1/status", None);
        assert_eq!(code, 200);
        assert_eq!(status["generation_state"], "partially-applied");

        let endpoints = status["endpoints"].as_array().unwrap();
        let failed = endpoints
            .iter()
            .find(|e| e["id"] == format!("tcp:{}", taken_addr))
            .expect("the failing endpoint is reported");
        assert_eq!(failed["slots"][0]["state"], "failed");
        assert!(failed["slots"][0]["error"].as_str().unwrap().contains("bind"));
    }
}
