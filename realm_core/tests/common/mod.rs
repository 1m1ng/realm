//! Shared scaffolding for the lifecycle integration tests.
//!
//! Every test drives real sockets: an echo server tagging its answers so a
//! connection can be told which generation of the configuration it reached,
//! plus the small helpers needed to observe leaks.

#![allow(dead_code)]

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio::time::timeout;

/// A tcp echo server that prefixes every answer with `tag`.
pub async fn spawn_echo(tag: &'static str) -> SocketAddr {
    let lis = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = lis.local_addr().unwrap();

    tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = lis.accept().await else {
                return;
            };
            tokio::spawn(async move {
                let mut buf = vec![0u8; 1500];
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

/// A udp echo server that prefixes every answer with `tag`.
pub async fn spawn_udp_echo(tag: &'static str) -> SocketAddr {
    let sock = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let addr = sock.local_addr().unwrap();

    tokio::spawn(async move {
        let mut buf = vec![0u8; 1500];
        loop {
            let Ok((n, peer)) = sock.recv_from(&mut buf).await else {
                return;
            };
            let mut answer = Vec::from(tag.as_bytes());
            answer.extend_from_slice(&buf[..n]);
            if sock.send_to(&answer, peer).await.is_err() {
                return;
            }
        }
    });

    addr
}

/// An address nothing is listening on, on the given family.
pub fn free_addr_on(host: &str) -> SocketAddr {
    std::net::TcpListener::bind((host, 0)).unwrap().local_addr().unwrap()
}

/// An ipv4 address no other test in this binary has been handed.
///
/// Binding port 0 asks the kernel for an ephemeral port and then releases it,
/// so two tests running in parallel can be handed the same one. That alone was
/// a flake risk; it became a correctness problem once tests started keying
/// per-test state on the listen address, where a shared port silently makes two
/// tests read and write each other's entry. Walking a private range *below* the
/// ephemeral one gives every caller a port to itself and keeps them clear of
/// whatever the kernel hands out elsewhere.
pub fn free_addr() -> SocketAddr {
    use std::sync::atomic::{AtomicU16, Ordering};
    static NEXT: AtomicU16 = AtomicU16::new(0);

    for _ in 0..2000 {
        let port = 30000 + NEXT.fetch_add(1, Ordering::Relaxed) % 10000;
        let addr = SocketAddr::from(([127, 0, 0, 1], port));
        // somebody outside this process may still hold it
        if std::net::TcpListener::bind(addr).is_ok() {
            return addr;
        }
    }
    panic!("no free port in the test range");
}

/// Send and read back one answer, failing the test if the relay stalls.
pub async fn ask(stream: &mut TcpStream, payload: &[u8]) -> String {
    stream.write_all(payload).await.unwrap();
    let mut buf = vec![0u8; 1500];
    let n = timeout(Duration::from_secs(5), stream.read(&mut buf))
        .await
        .expect("relay must answer in time")
        .expect("relay must stay readable");
    String::from_utf8_lossy(&buf[..n]).into_owned()
}

/// Same for udp.
pub async fn ask_udp(sock: &UdpSocket, relay: SocketAddr, payload: &[u8]) -> String {
    sock.send_to(payload, relay).await.unwrap();
    let mut buf = vec![0u8; 1500];
    let (n, _) = timeout(Duration::from_secs(5), sock.recv_from(&mut buf))
        .await
        .expect("relay must answer in time")
        .unwrap();
    String::from_utf8_lossy(&buf[..n]).into_owned()
}

/// Number of file descriptors this process holds, for leak assertions.
pub fn open_fds() -> usize {
    #[cfg(target_os = "linux")]
    {
        std::fs::read_dir("/proc/self/fd").map(|d| d.count()).unwrap_or(0)
    }

    #[cfg(not(target_os = "linux"))]
    {
        0
    }
}

/// Whether ipv6 loopback is usable here.
pub fn has_ipv6() -> bool {
    std::net::TcpListener::bind("[::1]:0").is_ok()
}

/// A private directory, removed on drop.
pub struct TempDir(pub PathBuf);

impl TempDir {
    pub fn new(name: &str) -> Self {
        let mut path = std::env::temp_dir();
        path.push(format!("realm-test-{}-{}", name, std::process::id()));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }

    pub fn join(&self, name: &str) -> PathBuf {
        self.0.join(name)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
