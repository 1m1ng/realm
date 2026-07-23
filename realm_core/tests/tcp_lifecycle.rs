//! TCP data plane ownership (U4 / R1, R2, R10, R36).
//!
//! The listener may stop at any time; connections it accepted own their
//! configuration and must keep relaying until they end naturally or are
//! explicitly cancelled. Nothing borrows the listener's stack frame.

use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::{sleep, timeout};

use realm_core::endpoint::{RemoteAddr, TcpRuntime};
use realm_core::lifecycle::{CancellationToken, Cohort};
use realm_core::tcp::{bind_tcp, serve_tcp};

mod common;
use common::spawn_echo;

/// Send a payload and read the echo back, asserting it round-tripped intact.
async fn roundtrip(stream: &mut TcpStream, payload: &[u8]) {
    stream.write_all(payload).await.unwrap();
    let mut buf = vec![0u8; payload.len()];
    timeout(Duration::from_secs(5), stream.read_exact(&mut buf))
        .await
        .expect("relay must answer in time")
        .expect("relay must stay readable");
    assert_eq!(&buf, payload);
}

fn runtime_for(remote: std::net::SocketAddr) -> Arc<TcpRuntime> {
    Arc::new(TcpRuntime {
        raddr: RemoteAddr::SocketAddr(remote),
        conn_opts: Default::default(),
        extra_raddrs: Vec::new(),
    })
}

/// Covers AE2 (core path): stopping the listener must not disturb the
/// connections it already accepted.
#[tokio::test]
async fn established_connections_survive_stop_accept() {
    let echo = spawn_echo("").await;

    let lis = bind_tcp(&"127.0.0.1:0".parse().unwrap(), Default::default()).unwrap();
    let laddr = lis.local_addr().unwrap();

    let cohort = Cohort::new();
    let handle = cohort.handle();
    let shutdown = CancellationToken::new();
    let serving = tokio::spawn(serve_tcp(lis, runtime_for(echo), handle, shutdown.clone()));

    let mut stream = TcpStream::connect(laddr).await.unwrap();
    roundtrip(&mut stream, b"before stop").await;
    assert_eq!(cohort.count(), 1, "the connection is tracked by its cohort");

    // stop accepting: the listener goes away, the connection does not
    shutdown.cancel();
    timeout(Duration::from_secs(2), serving)
        .await
        .expect("accept loop must stop promptly")
        .unwrap()
        .unwrap();

    roundtrip(&mut stream, b"after stop").await;
    roundtrip(&mut stream, b"and still alive").await;
    assert_eq!(cohort.count(), 1, "the connection is still counted while draining");

    // and the port no longer accepts new connections
    assert!(TcpStream::connect(laddr).await.is_err());
}

/// Covers R2/R36: cancellation terminates connections through their own await
/// points, and the cohort only reports drained once they really exited.
#[tokio::test]
async fn cancelling_a_cohort_terminates_connections_and_zeroes_the_count() {
    let echo = spawn_echo("").await;

    let lis = bind_tcp(&"127.0.0.1:0".parse().unwrap(), Default::default()).unwrap();
    let laddr = lis.local_addr().unwrap();

    let mut cohort = Cohort::new();
    let shutdown = CancellationToken::new();
    tokio::spawn(serve_tcp(lis, runtime_for(echo), cohort.handle(), shutdown.clone()));

    let mut streams = Vec::new();
    for _ in 0..4 {
        let mut stream = TcpStream::connect(laddr).await.unwrap();
        roundtrip(&mut stream, b"hello").await;
        streams.push(stream);
    }
    assert_eq!(cohort.count(), 4);

    shutdown.cancel();
    cohort.cancel();

    timeout(Duration::from_secs(5), cohort.wait_drained())
        .await
        .expect("cancelled connections must actually exit");
    assert_eq!(cohort.count(), 0, "no connection is left behind");

    // the peers observe the close
    for mut stream in streams {
        let mut buf = [0u8; 8];
        let n = timeout(Duration::from_secs(2), stream.read(&mut buf))
            .await
            .expect("closed connection must not hang")
            .unwrap_or(0);
        assert_eq!(n, 0);
    }
}

/// Covers R10: a failing bind is reported, never a panic.
#[tokio::test]
async fn bind_failure_is_reported_as_an_error() {
    let occupied = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = occupied.local_addr().unwrap();

    let err = bind_tcp(&addr, Default::default()).expect_err("port is taken");
    // Unix reports AddrInUse; Windows reports PermissionDenied (WSAEACCES) for a
    // conflicting bind. What matters for R10 is that the failure is reported as
    // an error, not raised as a panic.
    assert!(
        matches!(
            err.kind(),
            std::io::ErrorKind::AddrInUse | std::io::ErrorKind::PermissionDenied
        ),
        "unexpected bind error kind: {:?}",
        err.kind()
    );
}

/// Covers R15: an unused drain deadline finishes as soon as connections end.
#[tokio::test]
async fn drain_with_deadline_forces_remaining_connections() {
    let echo = spawn_echo("").await;

    let lis = bind_tcp(&"127.0.0.1:0".parse().unwrap(), Default::default()).unwrap();
    let laddr = lis.local_addr().unwrap();

    let mut cohort = Cohort::new();
    let shutdown = CancellationToken::new();
    tokio::spawn(serve_tcp(lis, runtime_for(echo), cohort.handle(), shutdown.clone()));

    let mut stream = TcpStream::connect(laddr).await.unwrap();
    roundtrip(&mut stream, b"idle connection").await;
    shutdown.cancel();

    let outcome = timeout(Duration::from_secs(5), cohort.drain(Some(Duration::from_millis(200))))
        .await
        .expect("drain must respect its deadline");
    assert_eq!(outcome, realm_core::lifecycle::DrainOutcome::Forced(1));
    assert_eq!(cohort.count(), 0);

    // draining an already empty cohort finishes immediately
    let mut empty = Cohort::new();
    sleep(Duration::from_millis(10)).await;
    assert_eq!(
        empty.drain(Some(Duration::from_millis(50))).await,
        realm_core::lifecycle::DrainOutcome::Finished
    );
}
