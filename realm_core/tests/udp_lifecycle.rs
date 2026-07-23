//! UDP data plane ownership (U5 / R2, R16, R36).
//!
//! Associations outlive the receive loop that created them, so they must own
//! everything they touch and observe cancellation themselves. Stopping an
//! endpoint terminates every association and releases its socket.

use std::sync::Arc;
use std::time::Duration;

use tokio::net::UdpSocket;
use tokio::time::timeout;

use realm_core::endpoint::{RemoteAddr, UdpRuntime};
use realm_core::lifecycle::{CancellationToken, Cohort};
use realm_core::udp::{bind_udp, serve_udp};

mod common;
use common::spawn_udp_echo;

/// Send a datagram through the relay and assert the echo comes back intact.
async fn roundtrip(client: &UdpSocket, relay: std::net::SocketAddr, payload: &[u8]) {
    client.send_to(payload, relay).await.unwrap();
    let mut buf = vec![0u8; 1500];
    let (n, _) = timeout(Duration::from_secs(5), client.recv_from(&mut buf))
        .await
        .expect("relay must answer in time")
        .unwrap();
    assert_eq!(&buf[..n], payload);
}

fn runtime_for(remote: std::net::SocketAddr) -> Arc<UdpRuntime> {
    Arc::new(UdpRuntime {
        raddr: RemoteAddr::SocketAddr(remote),
        conn_opts: Default::default(),
    })
}

/// Covers AE7: a stopped udp endpoint terminates its associations in a
/// controlled way — every task exits, the count drops to zero, and the
/// outbound sockets are released.
#[tokio::test]
async fn stopping_an_endpoint_terminates_all_associations() {
    let echo = spawn_udp_echo("").await;

    let lis = bind_udp(&"127.0.0.1:0".parse().unwrap(), Default::default()).unwrap();
    let laddr = lis.local_addr().unwrap();

    let mut cohort = Cohort::new();
    let shutdown = CancellationToken::new();
    let serving = tokio::spawn(serve_udp(lis, runtime_for(echo), cohort.handle(), shutdown.clone()));

    let mut clients = Vec::new();
    for _ in 0..4 {
        let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        roundtrip(&client, laddr, b"ping").await;
        clients.push(client);
    }
    assert_eq!(cohort.count(), 4, "every association is tracked");

    // controlled teardown: stop receiving, cancel, wait for actual exit
    shutdown.cancel();
    timeout(Duration::from_secs(2), serving)
        .await
        .expect("receive loop must stop promptly")
        .unwrap()
        .unwrap();

    cohort.cancel();
    timeout(Duration::from_secs(5), cohort.wait_drained())
        .await
        .expect("associations must actually exit");
    // the count reaching zero means every association task actually returned
    // and released its guard — and with it the last reference to its outbound
    // socket. Descriptor-level leak detection lives in `stress.rs`, which runs
    // alone in its own process: a process-wide fd count cannot tell this test's
    // sockets from those of the tests running next to it.
    assert_eq!(cohort.count(), 0, "association count drops monotonically to zero");
}

/// Covers R16: after a controlled rebuild the client's next datagram creates a
/// fresh association against the new configuration.
#[tokio::test]
async fn associations_are_rebuilt_after_a_restart() {
    let echo = spawn_udp_echo("").await;
    let laddr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();

    let lis = bind_udp(&laddr, Default::default()).unwrap();
    let bound = lis.local_addr().unwrap();

    let mut first = Cohort::new();
    let shutdown = CancellationToken::new();
    let serving = tokio::spawn(serve_udp(lis, runtime_for(echo), first.handle(), shutdown.clone()));

    let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    roundtrip(&client, bound, b"first generation").await;
    assert_eq!(first.count(), 1);

    shutdown.cancel();
    let _ = timeout(Duration::from_secs(2), serving).await.expect("stops promptly");
    first.cancel();
    timeout(Duration::from_secs(5), first.wait_drained())
        .await
        .expect("old associations exit");

    // rebind the same port and serve a new generation
    let echo2 = spawn_udp_echo("").await;
    let lis = bind_udp(&bound, Default::default()).unwrap();
    let second = Cohort::new();
    let shutdown2 = CancellationToken::new();
    tokio::spawn(serve_udp(lis, runtime_for(echo2), second.handle(), shutdown2));

    roundtrip(&client, bound, b"second generation").await;
    assert_eq!(second.count(), 1, "the client rebuilt its association");
}

/// Covers R10 for udp: a failing bind is an error, not a panic.
#[tokio::test]
async fn bind_failure_is_reported_as_an_error() {
    let occupied = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let addr = occupied.local_addr().unwrap();

    let err = bind_udp(&addr, Default::default()).expect_err("port is taken");
    assert_eq!(err.kind(), std::io::ErrorKind::AddrInUse);
}
