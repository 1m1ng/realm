//! Endpoint lifecycle management (U6 / R3, R9, R10, R13, R14, R15, R23, R36).
//!
//! The manager owns one state machine per (id, protocol) slot: it binds before
//! reporting running, replaces a listener without disturbing the connections of
//! the previous generation, and drains those connections under a deadline it
//! is given.

use std::net::SocketAddr;
use std::time::Duration;

use tokio::io::AsyncReadExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;

use realm_core::endpoint::{Endpoint, RemoteAddr};
use realm_core::lifecycle::{DrainPolicy, EndpointManager, EndpointSpec, Proto, SlotAction, SlotState};

mod common;
use common::{ask, ask_udp, free_addr, spawn_echo, spawn_udp_echo};

fn spec(laddr: SocketAddr, remote: SocketAddr) -> EndpointSpec {
    EndpointSpec {
        endpoint: Endpoint {
            laddr,
            raddr: RemoteAddr::SocketAddr(remote),
            bind_opts: Default::default(),
            conn_opts: Default::default(),
            extra_raddrs: Vec::new(),
        },
        tcp: true,
        udp: false,
        drain: None,
        material: None,
    }
}

fn slot(mgr: &mut EndpointManager, id: &str, proto: Proto) -> realm_core::lifecycle::SlotStatus {
    mgr.status()
        .into_iter()
        .find(|s| s.id == id)
        .expect("endpoint is known")
        .slots
        .into_iter()
        .find(|s| s.proto == proto)
        .expect("slot is known")
}

/// A bound endpoint reports running only once it is actually serving.
#[tokio::test]
async fn a_started_endpoint_reports_running_and_relays() {
    let echo = spawn_echo("v1:").await;
    let laddr = free_addr();

    let mut mgr = EndpointManager::new();
    let outcome = mgr.apply("a".into(), 1, spec(laddr, echo)).await;

    assert_eq!(outcome.len(), 1);
    assert_eq!(outcome[0].action, SlotAction::Created);
    assert_eq!(outcome[0].proto, Proto::Tcp);

    let status = slot(&mut mgr, "a", Proto::Tcp);
    assert_eq!(status.state, SlotState::Running);
    assert_eq!(status.generation, 1);
    assert_eq!(status.laddr, Some(laddr));

    let mut stream = TcpStream::connect(laddr).await.unwrap();
    assert_eq!(ask(&mut stream, b"ping").await, "v1:ping");
    assert_eq!(slot(&mut mgr, "a", Proto::Tcp).connections, 1);
}

/// Covers AE3: a failing bind leaves the previous listener serving and the
/// slot is reported failed — never running.
#[tokio::test]
async fn failed_bind_restores_the_previous_listener() {
    let echo = spawn_echo("v1:").await;
    let laddr = free_addr();

    let mut mgr = EndpointManager::new();
    mgr.apply("a".into(), 1, spec(laddr, echo)).await;

    let mut established = TcpStream::connect(laddr).await.unwrap();
    assert_eq!(ask(&mut established, b"one").await, "v1:one");

    // a new generation that moves to an address somebody else already owns
    let taken = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let taken_addr = taken.local_addr().unwrap();
    let echo2 = spawn_echo("v2:").await;

    let outcome = mgr.apply("a".into(), 2, spec(taken_addr, echo2)).await;
    assert_eq!(outcome[0].action, SlotAction::Failed);
    assert!(outcome[0].error.as_deref().unwrap_or_default().contains("bind"));

    let status = slot(&mut mgr, "a", Proto::Tcp);
    assert!(
        matches!(status.state, SlotState::Failed(_)),
        "state must be failed, got {:?}",
        status.state
    );
    assert_eq!(status.generation, 1, "the serving generation is still the old one");

    // the old listener kept serving, on its old address, with its old remote
    let mut fresh = TcpStream::connect(laddr).await.unwrap();
    assert_eq!(ask(&mut fresh, b"two").await, "v1:two");
    assert_eq!(ask(&mut established, b"three").await, "v1:three");
}

/// Covers AE2, AE14: replacing an endpoint on the same address keeps the
/// established connections on the old generation, visible as a draining cohort.
#[tokio::test]
async fn same_address_replacement_drains_the_old_generation() {
    let echo1 = spawn_echo("v1:").await;
    let echo2 = spawn_echo("v2:").await;
    let laddr = free_addr();

    let mut mgr = EndpointManager::new();
    mgr.apply("a".into(), 1, spec(laddr, echo1)).await;

    let mut old = TcpStream::connect(laddr).await.unwrap();
    assert_eq!(ask(&mut old, b"x").await, "v1:x");

    let outcome = mgr.apply("a".into(), 2, spec(laddr, echo2)).await;
    assert_eq!(outcome[0].action, SlotAction::Updated);

    // the established connection keeps talking to the old remote
    assert_eq!(ask(&mut old, b"y").await, "v1:y");

    // new connections get the new one
    let mut new = TcpStream::connect(laddr).await.unwrap();
    assert_eq!(ask(&mut new, b"z").await, "v2:z");

    let status = slot(&mut mgr, "a", Proto::Tcp);
    assert_eq!(status.state, SlotState::Running);
    assert_eq!(status.generation, 2);
    assert_eq!(status.connections, 1, "one connection on the new generation");

    let draining = status.draining;
    assert_eq!(draining.len(), 1, "the old generation is a draining cohort");
    assert_eq!(draining[0].generation, 1);
    assert_eq!(draining[0].connections, 1);

    // an update drains indefinitely by default: the old connection stays
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(ask(&mut old, b"still here").await, "v1:still here");

    // when it ends on its own, the cohort disappears from the status
    drop(old);
    let mut tries = 0;
    loop {
        let status = slot(&mut mgr, "a", Proto::Tcp);
        if status.draining.is_empty() {
            break;
        }
        tries += 1;
        assert!(tries < 50, "cohort should disappear once empty");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Covers AE6: deleting an endpoint releases the listener immediately and
/// force-closes whatever is left after the drain deadline.
#[tokio::test]
async fn delete_releases_the_port_and_forces_the_drain_deadline() {
    let echo = spawn_echo("v1:").await;
    let laddr = free_addr();

    let mut mgr = EndpointManager::with_policy(DrainPolicy {
        on_update: None,
        on_delete: Some(Duration::from_millis(200)),
    });
    mgr.apply("a".into(), 1, spec(laddr, echo)).await;

    let mut lingering = TcpStream::connect(laddr).await.unwrap();
    assert_eq!(ask(&mut lingering, b"x").await, "v1:x");

    let outcome = mgr.remove("a", 2).await;
    assert_eq!(outcome[0].action, SlotAction::Deleted);

    // the port is reusable right away
    let rebound = TcpListener::bind(laddr).await;
    assert!(rebound.is_ok(), "listener socket must be released on delete");
    drop(rebound);

    // the lingering connection is force-closed once the deadline expires
    let mut buf = [0u8; 8];
    let n = timeout(Duration::from_secs(3), lingering.read(&mut buf))
        .await
        .expect("forced close must happen within the deadline")
        .unwrap_or(0);
    assert_eq!(n, 0);

    assert!(
        mgr.status().iter().all(|e| e.id != "a"),
        "a deleted endpoint disappears from the status"
    );
}

/// Covers R23: the two protocols of one rule succeed or fail independently.
///
/// Unix-only: it forces the udp side to fail by pre-binding its port, which
/// relies on the bind-conflict semantics of the target platform. On Windows a
/// conflicting udp bind is permitted (SO_REUSEADDR) or returns PermissionDenied,
/// so the "one side failed" premise does not hold. The hot-reload feature ships
/// on unix (the control plane is unix-only), so unix is the platform under test.
#[cfg(unix)]
#[tokio::test]
async fn protocols_of_one_rule_are_tracked_independently() {
    let echo = spawn_echo("v1:").await;
    let laddr = free_addr();

    // hold the udp port so that only the udp slot fails
    let _blocker = tokio::net::UdpSocket::bind(laddr).await.unwrap();

    let mut mgr = EndpointManager::new();
    let mut spec = spec(laddr, echo);
    spec.udp = true;

    let outcome = mgr.apply("a".into(), 1, spec).await;
    assert_eq!(outcome.len(), 2, "one result per (id, protocol)");

    let tcp = outcome.iter().find(|o| o.proto == Proto::Tcp).unwrap();
    let udp = outcome.iter().find(|o| o.proto == Proto::Udp).unwrap();
    assert_eq!(tcp.action, SlotAction::Created);
    assert_eq!(udp.action, SlotAction::Failed);

    assert_eq!(slot(&mut mgr, "a", Proto::Tcp).state, SlotState::Running);
    assert!(matches!(slot(&mut mgr, "a", Proto::Udp).state, SlotState::Failed(_)));
}

/// Covers R3: an endpoint can be validated without any side effect.
#[tokio::test]
async fn validation_has_no_side_effect() {
    let echo = spawn_echo("v1:").await;
    let laddr = free_addr();

    let mgr = EndpointManager::new();
    assert!(mgr.validate(&spec(laddr, echo)).is_ok());

    // nothing was bound by validating
    let lis = TcpListener::bind(laddr).await;
    assert!(lis.is_ok(), "validation must not bind anything");
}

/// #15: a spec that enables neither protocol is rejected by validation, which
/// has no protocol to report under — it must still produce an outcome so the
/// generation is seen as failed instead of silently applied.
#[tokio::test]
async fn a_spec_with_no_protocol_is_reported_failed_not_silently_applied() {
    let echo = spawn_echo("v1:").await;
    let laddr = free_addr();

    let mut mgr = EndpointManager::new();
    let mut s = spec(laddr, echo);
    s.tcp = false;
    s.udp = false;

    let outcome = mgr.apply("a".into(), 1, s).await;
    assert!(!outcome.is_empty(), "a rejected spec must still produce an outcome");
    assert!(
        outcome.iter().all(|o| o.action == SlotAction::Failed),
        "every reported protocol is failed: {:?}",
        outcome
    );
    assert_eq!(
        outcome[0].retryable,
        Some(false),
        "a spec realm cannot make sense of is terminal"
    );
    assert!(outcome[0].error.as_deref().unwrap_or_default().contains("neither"));
}

/// #22: a same-address udp replacement while an association is alive. The old
/// listener is replaced, the manager reports it updated, and a fresh datagram
/// reaches the new remote.
///
/// Unix-only: the release-then-rebind on the same udp address depends on the
/// platform's bind semantics (on Windows SO_REUSEADDR changes what a same-port
/// rebind observes). The feature ships on unix, so unix is the platform tested.
#[cfg(unix)]
#[tokio::test]
async fn same_address_udp_replacement_reaches_the_new_remote() {
    let echo1 = spawn_udp_echo("v1:").await;
    let echo2 = spawn_udp_echo("v2:").await;
    let laddr = free_addr();

    let mut mgr = EndpointManager::new();
    let mut s = spec(laddr, echo1);
    s.tcp = false;
    s.udp = true;
    let outcome = mgr.apply("a".into(), 1, s).await;
    assert_eq!(outcome.len(), 1);
    assert_eq!(outcome[0].proto, Proto::Udp);
    assert_eq!(outcome[0].action, SlotAction::Created);

    // establish an association through the udp endpoint
    let client = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    assert_eq!(ask_udp(&client, laddr, b"x").await, "v1:x");

    // replace on the same address with a different remote
    let mut s2 = spec(laddr, echo2);
    s2.tcp = false;
    s2.udp = true;
    let outcome = mgr.apply("a".into(), 2, s2).await;
    assert_eq!(
        outcome[0].action,
        SlotAction::Updated,
        "a same-address udp change is a replacement: {:?}",
        outcome
    );

    let status = slot(&mut mgr, "a", Proto::Udp);
    assert_eq!(status.state, SlotState::Running);
    assert_eq!(status.generation, 2);

    // a fresh association reaches the new remote
    let fresh = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    assert_eq!(ask_udp(&fresh, laddr, b"z").await, "v2:z");
}
