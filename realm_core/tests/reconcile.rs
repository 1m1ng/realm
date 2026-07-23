//! Desired-state reconciliation (U7 / R6, R7, R8, R17, R24-R29).
//!
//! An agent publishes the node's complete desired state; realm computes the
//! difference and only touches what actually changed. Generations are the
//! caller's, monotonic and idempotent; a repeated submission must neither
//! duplicate endpoints nor disturb traffic a second time.

use std::net::SocketAddr;

use serde::{Deserialize, Serialize};
use tokio::net::{TcpListener, TcpStream};

use realm_core::endpoint::{Endpoint, RemoteAddr};
use realm_core::lifecycle::{
    DesiredEndpoint, EndpointSource, EndpointSpec, GenerationState, Proto, ReconcileError, ReconcileRequest,
    Reconciler, SlotAction, derive_id,
};

mod common;
use common::{ask, free_addr, spawn_echo};

/// Minimal stand-in for the top-level `EndpointConf`: the reconciler only
/// needs to compare specs and turn them into lifecycle specs.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct TestSpec {
    listen: String,
    remote: SocketAddr,
    tcp: bool,
    udp: bool,
}

impl TestSpec {
    fn tcp(listen: SocketAddr, remote: SocketAddr) -> Self {
        Self {
            listen: listen.to_string(),
            remote,
            tcp: true,
            udp: false,
        }
    }
}

impl EndpointSource for TestSpec {
    fn build(&self) -> Result<EndpointSpec, String> {
        let laddr: SocketAddr = self
            .listen
            .parse()
            .map_err(|e| format!("invalid `listen` = `{}`: {}", self.listen, e))?;

        Ok(EndpointSpec {
            endpoint: Endpoint {
                laddr,
                raddr: RemoteAddr::SocketAddr(self.remote),
                bind_opts: Default::default(),
                conn_opts: Default::default(),
                extra_raddrs: Vec::new(),
            },
            tcp: self.tcp,
            udp: self.udp,
            drain: None,
        })
    }
}

fn request(generation: u64, endpoints: &[(&str, TestSpec)]) -> ReconcileRequest<TestSpec> {
    ReconcileRequest {
        generation,
        endpoints: endpoints
            .iter()
            .map(|(id, spec)| DesiredEndpoint {
                id: (*id).to_string(),
                spec: spec.clone(),
            })
            .collect(),
    }
}

/// Covers AE1: changing one endpoint leaves the others completely alone.
#[tokio::test]
async fn only_the_changed_endpoint_is_touched() {
    let echo1 = spawn_echo("v1:").await;
    let echo2 = spawn_echo("v2:").await;
    let (a, b, c) = (free_addr(), free_addr(), free_addr());

    let mut rec = Reconciler::new();
    let response = rec
        .reconcile(request(
            1,
            &[
                ("a", TestSpec::tcp(a, echo1)),
                ("b", TestSpec::tcp(b, echo1)),
                ("c", TestSpec::tcp(c, echo1)),
            ],
        ))
        .await
        .expect("first generation applies");
    assert_eq!(response.state, GenerationState::Applied);
    assert_eq!(response.results.len(), 3);
    assert!(response.results.iter().all(|r| r.action == SlotAction::Created));

    // established connections on b and c
    let mut on_b = TcpStream::connect(b).await.unwrap();
    let mut on_c = TcpStream::connect(c).await.unwrap();
    assert_eq!(ask(&mut on_b, b"x").await, "v1:x");
    assert_eq!(ask(&mut on_c, b"x").await, "v1:x");

    // only a changes
    let response = rec
        .reconcile(request(
            2,
            &[
                ("a", TestSpec::tcp(a, echo2)),
                ("b", TestSpec::tcp(b, echo1)),
                ("c", TestSpec::tcp(c, echo1)),
            ],
        ))
        .await
        .expect("second generation applies");

    let action = |id: &str| response.results.iter().find(|r| r.id == id).unwrap().action;
    assert_eq!(action("a"), SlotAction::Updated);
    assert_eq!(action("b"), SlotAction::Unchanged);
    assert_eq!(action("c"), SlotAction::Unchanged);

    // b and c were not disturbed at all
    assert_eq!(ask(&mut on_b, b"y").await, "v1:y");
    assert_eq!(ask(&mut on_c, b"y").await, "v1:y");

    // and a serves the new remote
    let mut on_a = TcpStream::connect(a).await.unwrap();
    assert_eq!(ask(&mut on_a, b"y").await, "v2:y");
}

/// Covers AE4: resubmitting a generation returns the first result and changes
/// nothing.
#[tokio::test]
async fn resubmitting_a_generation_is_idempotent() {
    let echo = spawn_echo("v1:").await;
    let a = free_addr();

    let mut rec = Reconciler::new();
    let req = request(7, &[("a", TestSpec::tcp(a, echo))]);
    let first = rec.reconcile(req.clone()).await.unwrap();

    let mut established = TcpStream::connect(a).await.unwrap();
    assert_eq!(ask(&mut established, b"x").await, "v1:x");

    let second = rec.reconcile(req).await.unwrap();
    assert_eq!(first.results.len(), second.results.len());
    assert_eq!(second.results[0].action, first.results[0].action);
    assert_eq!(second.generation, first.generation);

    // the established connection was never disturbed
    assert_eq!(ask(&mut established, b"y").await, "v1:y");
    assert_eq!(rec.status().len(), 1, "no duplicate endpoint was created");
}

/// Covers AE5: an older generation is refused, and the response says which
/// generation is active.
#[tokio::test]
async fn stale_generations_are_refused() {
    let echo = spawn_echo("v1:").await;
    let a = free_addr();

    let mut rec = Reconciler::new();
    rec.reconcile(request(42, &[("a", TestSpec::tcp(a, echo))]))
        .await
        .unwrap();

    let err = rec
        .reconcile(request(41, &[("a", TestSpec::tcp(a, echo))]))
        .await
        .expect_err("older generation must be refused");

    match err {
        ReconcileError::Stale { active } => assert_eq!(active, 42),
        other => panic!("expected a stale-generation error, got {:?}", other),
    }
    assert!(!err.is_retryable(), "a stale generation is terminal");
}

/// Covers AE8: an invalid endpoint is reported with a structured error while
/// the rest of the generation is applied.
#[tokio::test]
async fn an_invalid_endpoint_does_not_block_the_others() {
    let echo = spawn_echo("v1:").await;
    let good = free_addr();

    let bad = TestSpec {
        listen: "definitely not an address".into(),
        remote: echo,
        tcp: true,
        udp: false,
    };

    let mut rec = Reconciler::new();
    let response = rec
        .reconcile(request(1, &[("good", TestSpec::tcp(good, echo)), ("bad", bad)]))
        .await
        .unwrap();

    assert_eq!(response.state, GenerationState::PartiallyApplied);

    let bad_result = response.results.iter().find(|r| r.id == "bad").unwrap();
    assert_eq!(bad_result.action, SlotAction::Failed);
    assert!(bad_result.error.as_deref().unwrap_or_default().contains("listen"));

    let good_result = response.results.iter().find(|r| r.id == "good").unwrap();
    assert_eq!(good_result.action, SlotAction::Created);

    // the good one really is serving
    let mut stream = TcpStream::connect(good).await.unwrap();
    assert_eq!(ask(&mut stream, b"x").await, "v1:x");
}

/// Covers AE10: the static mode's derived ids make the agent's first
/// equivalent submission a no-op.
#[tokio::test]
async fn derived_ids_make_the_first_takeover_a_no_op() {
    let echo = spawn_echo("v1:").await;
    let a = free_addr();
    let spec = TestSpec::tcp(a, echo);

    // generation 0, as the static mode submits it
    let derived = derive_id(&a, true, false);
    let mut rec = Reconciler::new();
    rec.reconcile(request(0, &[(derived.as_str(), spec.clone())]))
        .await
        .unwrap();

    let mut established = TcpStream::connect(a).await.unwrap();
    assert_eq!(ask(&mut established, b"x").await, "v1:x");

    // the agent computes the same id and submits the same desired state
    let response = rec.reconcile(request(1, &[(derived.as_str(), spec)])).await.unwrap();

    assert_eq!(response.state, GenerationState::Applied);
    assert!(
        response.results.iter().all(|r| r.action == SlotAction::Unchanged),
        "an equivalent desired state must not rebuild anything: {:?}",
        response.results
    );
    assert_eq!(ask(&mut established, b"y").await, "v1:y");
}

/// Covers AE11: two concurrent submissions of one generation are applied once
/// and answered identically.
#[tokio::test]
async fn concurrent_submissions_of_one_generation_collapse() {
    let echo = spawn_echo("v1:").await;
    let a = free_addr();
    let spec = TestSpec::tcp(a, echo);

    let handle = Reconciler::new().spawn();

    let (first, second) = tokio::join!(
        handle.reconcile(request(3, &[("a", spec.clone())])),
        handle.reconcile(request(3, &[("a", spec)])),
    );

    let first = first.unwrap();
    let second = second.unwrap();

    // both submissions answer with the same result: the second one replayed it
    assert_eq!(first.generation, second.generation);
    assert_eq!(first.state, second.state);
    assert_eq!(first.results.len(), second.results.len());
    for (a, b) in first.results.iter().zip(second.results.iter()) {
        assert_eq!(a.id, b.id);
        assert_eq!(a.proto, b.proto);
        assert_eq!(a.action, b.action);
    }

    // and the generation was applied exactly once
    assert_eq!(handle.status().await.len(), 1, "no duplicate endpoint");
    let statuses = handle.status().await;
    assert_eq!(statuses[0].slots.len(), 1);
    assert_eq!(statuses[0].slots[0].generation, 3);
}

/// Covers AE12: the two protocols of a rule report separately, and a partial
/// failure surfaces as a partially-applied generation.
#[tokio::test]
async fn one_protocol_failing_yields_a_partially_applied_generation() {
    let echo = spawn_echo("v1:").await;
    let a = free_addr();

    // occupy the udp side of the address
    let _blocker = tokio::net::UdpSocket::bind(a).await.unwrap();

    let spec = TestSpec {
        listen: a.to_string(),
        remote: echo,
        tcp: true,
        udp: true,
    };

    let mut rec = Reconciler::new();
    let response = rec.reconcile(request(1, &[("a", spec)])).await.unwrap();

    assert_eq!(response.state, GenerationState::PartiallyApplied);
    let tcp = response
        .results
        .iter()
        .find(|r| r.id == "a" && r.proto == Proto::Tcp)
        .unwrap();
    let udp = response
        .results
        .iter()
        .find(|r| r.id == "a" && r.proto == Proto::Udp)
        .unwrap();
    assert_eq!(tcp.action, SlotAction::Created);
    assert_eq!(udp.action, SlotAction::Failed);
}

/// Covers R27: two endpoints swapping their listen addresses converge in one
/// reconcile, without either bind hitting an address-in-use.
#[tokio::test]
async fn endpoints_can_swap_their_addresses_in_one_generation() {
    let echo1 = spawn_echo("v1:").await;
    let echo2 = spawn_echo("v2:").await;
    let (x, y) = (free_addr(), free_addr());

    let mut rec = Reconciler::new();
    rec.reconcile(request(
        1,
        &[("a", TestSpec::tcp(x, echo1)), ("b", TestSpec::tcp(y, echo2))],
    ))
    .await
    .unwrap();

    let response = rec
        .reconcile(request(
            2,
            &[("a", TestSpec::tcp(y, echo1)), ("b", TestSpec::tcp(x, echo2))],
        ))
        .await
        .unwrap();

    assert_eq!(
        response.state,
        GenerationState::Applied,
        "the swap must succeed: {:?}",
        response.results
    );

    // a now answers on y, b on x
    let mut on_y = TcpStream::connect(y).await.unwrap();
    assert_eq!(ask(&mut on_y, b"q").await, "v1:q");
    let mut on_x = TcpStream::connect(x).await.unwrap();
    assert_eq!(ask(&mut on_x, b"q").await, "v2:q");
}

/// Covers R28: duplicate listen address + protocol within one generation is
/// rejected deterministically, before anything is bound.
#[tokio::test]
async fn duplicate_listen_addresses_in_one_generation_fail_both() {
    let echo = spawn_echo("v1:").await;
    let a = free_addr();

    let mut rec = Reconciler::new();
    let response = rec
        .reconcile(request(
            1,
            &[("first", TestSpec::tcp(a, echo)), ("second", TestSpec::tcp(a, echo))],
        ))
        .await
        .unwrap();

    assert_eq!(response.state, GenerationState::PartiallyApplied);
    assert!(
        response.results.iter().all(|r| r.action == SlotAction::Failed),
        "both conflicting endpoints must fail: {:?}",
        response.results
    );
    assert!(
        response
            .results
            .iter()
            .all(|r| r.error.as_deref().unwrap_or_default().contains("duplicate")),
        "the error must name the conflict"
    );

    // nothing was bound
    assert!(TcpListener::bind(a).await.is_ok());
}

/// Covers R29: an empty desired state deletes everything.
#[tokio::test]
async fn an_empty_desired_state_removes_every_endpoint() {
    let echo = spawn_echo("v1:").await;
    let a = free_addr();

    let mut rec = Reconciler::new();
    rec.reconcile(request(1, &[("a", TestSpec::tcp(a, echo))]))
        .await
        .unwrap();

    let response = rec.reconcile(request(2, &[])).await.unwrap();
    assert_eq!(response.state, GenerationState::Applied);
    assert_eq!(response.results.len(), 1);
    assert_eq!(response.results[0].action, SlotAction::Deleted);

    assert!(rec.status().is_empty());
    assert!(TcpListener::bind(a).await.is_ok(), "the port was released");
}

/// Covers R25: a failed endpoint heals when a later generation resubmits it.
#[tokio::test]
async fn a_failed_endpoint_is_retried_by_the_next_generation() {
    let echo = spawn_echo("v1:").await;
    let a = free_addr();
    let blocker = TcpListener::bind(a).await.unwrap();

    let mut rec = Reconciler::new();
    let response = rec
        .reconcile(request(1, &[("a", TestSpec::tcp(a, echo))]))
        .await
        .unwrap();
    assert_eq!(response.results[0].action, SlotAction::Failed);
    assert_eq!(response.state, GenerationState::PartiallyApplied);
    assert_eq!(rec.active_generation(), Some(1));

    // the conflict goes away and the same desired state is resubmitted
    drop(blocker);
    let response = rec
        .reconcile(request(2, &[("a", TestSpec::tcp(a, echo))]))
        .await
        .unwrap();
    assert_eq!(response.state, GenerationState::Applied);
    assert_eq!(response.results[0].action, SlotAction::Created);

    let mut stream = TcpStream::connect(a).await.unwrap();
    assert_eq!(ask(&mut stream, b"x").await, "v1:x");
}
