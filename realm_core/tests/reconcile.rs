//! Desired-state reconciliation (U7 / R6, R7, R8, R17, R24-R29).
//!
//! An agent publishes the node's complete desired state; realm computes the
//! difference and only touches what actually changed. Generations are the
//! caller's, monotonic and idempotent; a repeated submission must neither
//! duplicate endpoints nor disturb traffic a second time.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Mutex;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::io::AsyncReadExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;

use realm_core::endpoint::{Endpoint, RemoteAddr};
use realm_core::lifecycle::{
    DesiredEndpoint, DrainPolicy, EndpointSource, EndpointSpec, GenerationState, Proto, ReconcileError,
    ReconcileRequest, Reconciler, SlotAction, derive_id,
};

mod common;
use common::{ask, free_addr, rotate_material, spawn_echo};

/// How many times `refresh` ran, per listen address. Unlike the material store
/// in `common`, only this file asserts on it.
static REFRESHES: Mutex<BTreeMap<String, usize>> = Mutex::new(BTreeMap::new());

/// How many times the reconciler refreshed the endpoint listening here.
fn refreshes(listen: &SocketAddr) -> usize {
    REFRESHES.lock().unwrap().get(&listen.to_string()).copied().unwrap_or(0)
}

/// Minimal stand-in for the top-level `EndpointConf`: the reconciler only
/// needs to compare specs and turn them into lifecycle specs.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct TestSpec {
    listen: String,
    remote: SocketAddr,
    tcp: bool,
    udp: bool,
    /// per-endpoint force-close deadline for a delete, in milliseconds
    #[serde(default)]
    delete_drain_ms: Option<u64>,
    /// derived from material realm does not own, recomputed by `refresh`.
    /// `#[serde(skip)]` is what makes it enter the diff — which compares specs
    /// by `PartialEq` — while staying out of the submission hash and the
    /// snapshot, which both go through serde. It reaches the built
    /// endpoint, since a rebuild the manager cannot tell apart from what it is
    /// already serving is not a rebuild.
    #[serde(skip)]
    material: Option<SocketAddr>,
}

impl TestSpec {
    fn tcp(listen: SocketAddr, remote: SocketAddr) -> Self {
        Self {
            listen: listen.to_string(),
            remote,
            tcp: true,
            udp: false,
            delete_drain_ms: None,
            material: None,
        }
    }

    fn with_delete_drain_ms(mut self, ms: u64) -> Self {
        self.delete_drain_ms = Some(ms);
        self
    }
}

impl EndpointSource for TestSpec {
    fn build(&self) -> Result<EndpointSpec, String> {
        let laddr: SocketAddr = self
            .listen
            .parse()
            .map_err(|e| format!("invalid `listen` = `{}`: {}", self.listen, e))?;

        let drain = self.delete_drain_ms.map(|ms| DrainPolicy {
            on_update: None,
            on_delete: Some(Duration::from_millis(ms)),
        });

        Ok(EndpointSpec {
            endpoint: Endpoint {
                laddr,
                // what the material says wins over what the description says,
                // the way a certificate's bytes decide what a `cert = <path>`
                // endpoint actually presents
                raddr: RemoteAddr::SocketAddr(self.material.unwrap_or(self.remote)),
                bind_opts: Default::default(),
                conn_opts: Default::default(),
                extra_raddrs: Vec::new(),
            },
            tcp: self.tcp,
            udp: self.udp,
            drain,
            material: None,
        })
    }

    fn refresh(&mut self) {
        *REFRESHES.lock().unwrap().entry(self.listen.clone()).or_default() += 1;
        self.material = common::material_for(&self.listen);
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
        delete_drain_ms: None,
        material: None,
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
        delete_drain_ms: None,
        material: None,
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

/// A replace whose bind fails keeps the old listener serving, so the id is
/// still one the manager holds — a later empty desired state must delete it,
/// not report success while it keeps accepting traffic.
#[tokio::test]
async fn a_failed_replacement_is_still_deleted_by_a_later_empty_state() {
    let echo1 = spawn_echo("v1:").await;
    let echo2 = spawn_echo("v2:").await;
    let x = free_addr();

    let mut rec = Reconciler::new();
    rec.reconcile(request(1, &[("a", TestSpec::tcp(x, echo1))]))
        .await
        .unwrap();

    // block the address a is asked to move to, so the move fails
    let y = free_addr();
    let _blocker = TcpListener::bind(y).await.unwrap();

    let response = rec
        .reconcile(request(2, &[("a", TestSpec::tcp(y, echo2))]))
        .await
        .unwrap();
    assert_eq!(response.state, GenerationState::PartiallyApplied);
    assert_eq!(response.results[0].action, SlotAction::Failed);

    // the old listener is still serving on x
    let mut on_x = TcpStream::connect(x).await.unwrap();
    assert_eq!(ask(&mut on_x, b"p").await, "v1:p");

    // an empty desired state must delete the retained listener too
    let response = rec.reconcile(request(3, &[])).await.unwrap();
    assert!(
        response
            .results
            .iter()
            .any(|r| r.id == "a" && r.action == SlotAction::Deleted),
        "the retained old listener must be deleted: {:?}",
        response.results
    );
    assert!(
        rec.status().is_empty(),
        "no endpoint may survive an empty desired state"
    );
    assert!(TcpListener::bind(x).await.is_ok(), "x must be released");
}

/// One protocol failing must not evict the healthy sibling. Resubmitting
/// the same desired state reports the healthy protocol `unchanged` and retries
/// only the failed one, never stopping and rebinding the one that was serving.
#[tokio::test]
async fn a_partial_protocol_failure_leaves_the_healthy_sibling_untouched() {
    let echo = spawn_echo("v1:").await;
    let a = free_addr();

    // hold the udp side so only udp fails
    let _blocker = tokio::net::UdpSocket::bind(a).await.unwrap();

    let spec = TestSpec {
        listen: a.to_string(),
        remote: echo,
        tcp: true,
        udp: true,
        delete_drain_ms: None,
        material: None,
    };

    let mut rec = Reconciler::new();
    let response = rec.reconcile(request(1, &[("a", spec.clone())])).await.unwrap();
    assert_eq!(response.state, GenerationState::PartiallyApplied);

    // an established connection on the healthy tcp side
    let mut established = TcpStream::connect(a).await.unwrap();
    assert_eq!(ask(&mut established, b"x").await, "v1:x");

    // resubmit the same desired state
    let response = rec.reconcile(request(2, &[("a", spec)])).await.unwrap();
    let tcp = response.results.iter().find(|r| r.proto == Proto::Tcp).unwrap();
    let udp = response.results.iter().find(|r| r.proto == Proto::Udp).unwrap();
    assert_eq!(
        tcp.action,
        SlotAction::Unchanged,
        "the healthy tcp side must be left untouched: {:?}",
        response.results
    );
    assert_eq!(udp.action, SlotAction::Failed, "the failed udp side is retried");

    // the established connection was never rebound
    assert_eq!(ask(&mut established, b"y").await, "v1:y");
}

/// A failed endpoint the agent stops declaring must disappear from status,
/// instead of lingering forever because it was never part of `applied`.
#[tokio::test]
async fn a_dropped_failed_endpoint_disappears_from_status() {
    let echo = spawn_echo("v1:").await;
    let a = free_addr();
    let blocker = TcpListener::bind(a).await.unwrap();

    let mut rec = Reconciler::new();
    let response = rec
        .reconcile(request(1, &[("a", TestSpec::tcp(a, echo))]))
        .await
        .unwrap();
    assert_eq!(response.results[0].action, SlotAction::Failed);
    assert!(
        rec.status().iter().any(|e| e.id == "a"),
        "a failed but still-desired endpoint is visible"
    );

    // stop declaring it
    drop(blocker);
    rec.reconcile(request(2, &[])).await.unwrap();
    assert!(
        rec.status().iter().all(|e| e.id != "a"),
        "a failed endpoint the agent dropped must not linger: {:?}",
        rec.status()
    );
}

/// A new endpoint colliding with a still-listening `unchanged` incumbent
/// must fail as a deterministic terminal duplicate, not race into a retryable
/// address-in-use — and the incumbent must be untouched (R28).
#[tokio::test]
async fn a_new_endpoint_colliding_with_an_unchanged_incumbent_is_terminal() {
    let echo = spawn_echo("v1:").await;
    let a = free_addr();

    let mut rec = Reconciler::new();
    rec.reconcile(request(1, &[("a", TestSpec::tcp(a, echo))]))
        .await
        .unwrap();

    let response = rec
        .reconcile(request(
            2,
            &[("a", TestSpec::tcp(a, echo)), ("b", TestSpec::tcp(a, echo))],
        ))
        .await
        .unwrap();

    assert_eq!(response.state, GenerationState::PartiallyApplied);
    let a_res = response.results.iter().find(|r| r.id == "a").unwrap();
    assert_eq!(a_res.action, SlotAction::Unchanged, "the incumbent is untouched");

    let b_res = response.results.iter().find(|r| r.id == "b").unwrap();
    assert_eq!(b_res.action, SlotAction::Failed);
    assert_eq!(
        b_res.retryable,
        Some(false),
        "a collision with a live incumbent is terminal, not a retryable bind race"
    );
    assert!(
        b_res.error.as_deref().unwrap_or_default().contains("duplicate"),
        "the error must name the conflict: {:?}",
        b_res.error
    );

    // the incumbent keeps serving
    let mut on_a = TcpStream::connect(a).await.unwrap();
    assert_eq!(ask(&mut on_a, b"x").await, "v1:x");
}

/// A deleted endpoint whose address is taken over by another endpoint of
/// the same generation must still force-close its connections on the delete
/// deadline, not drain them indefinitely.
#[tokio::test]
async fn a_delete_keeps_its_force_close_deadline_when_its_address_is_taken_over() {
    let echo1 = spawn_echo("v1:").await;
    let echo2 = spawn_echo("v2:").await;
    let x = free_addr();

    let mut rec = Reconciler::new();
    rec.reconcile(request(1, &[("a", TestSpec::tcp(x, echo1).with_delete_drain_ms(200))]))
        .await
        .unwrap();

    // a lingering connection on a
    let mut lingering = TcpStream::connect(x).await.unwrap();
    assert_eq!(ask(&mut lingering, b"x").await, "v1:x");

    // delete a, and let b take over its address x in one generation
    rec.reconcile(request(2, &[("b", TestSpec::tcp(x, echo2))]))
        .await
        .unwrap();

    // b serves on x now
    let mut on_x = TcpStream::connect(x).await.unwrap();
    assert_eq!(ask(&mut on_x, b"q").await, "v2:q");

    // a's lingering connection is force-closed on a's own delete deadline
    let mut buf = [0u8; 8];
    let n = timeout(Duration::from_secs(3), lingering.read(&mut buf))
        .await
        .expect("the delete deadline must still force-close the connection")
        .unwrap_or(0);
    assert_eq!(n, 0, "the lingering connection must be closed");
}

/// An address swap where one side's new config is bind-invalid must not
/// permanently strand a listener that was serving. The side whose address was
/// released for the handoff is put back on its original address once the taker
/// fails, and the taker keeps its own old listener (R27).
///
/// Unix-only: it drives one side to a bind failure and relies on that failure
/// being observed, which depends on the platform's bind-conflict semantics
/// (Windows permits or differently reports a conflicting bind). The feature
/// ships on unix, so unix is the platform under test.
#[cfg(unix)]
#[tokio::test]
async fn an_address_swap_with_a_bind_invalid_side_never_strands_a_listener() {
    let echo_a = spawn_echo("a1:").await;
    let echo_z1 = spawn_echo("z1:").await;
    let echo_z2 = spawn_echo("z2:").await;
    let y = free_addr(); // "a" listens here
    let x = free_addr(); // "z" listens here

    let mut rec = Reconciler::new();
    rec.reconcile(request(
        1,
        &[("a", TestSpec::tcp(y, echo_a)), ("z", TestSpec::tcp(x, echo_z1))],
    ))
    .await
    .unwrap();

    // an address that cannot be bound, held by a foreign listener
    let bad = free_addr();
    let _blocker = TcpListener::bind(bad).await.unwrap();

    // "a" moves onto the bind-invalid address; "z" moves onto a's old address y.
    // y is released for z, then a's move fails: a must not be left with nothing.
    let response = rec
        .reconcile(request(
            2,
            &[("a", TestSpec::tcp(bad, echo_a)), ("z", TestSpec::tcp(y, echo_z2))],
        ))
        .await
        .unwrap();
    assert_eq!(response.state, GenerationState::PartiallyApplied);

    // a still has its original listener on y
    let mut on_y = TcpStream::connect(y).await.expect("y must still have a listener");
    assert_eq!(
        ask(&mut on_y, b"p").await,
        "a1:p",
        "a's original listener must survive on y"
    );

    // z keeps its own old listener on x, since its move onto y could not complete
    let mut on_x = TcpStream::connect(x).await.expect("z must keep its listener on x");
    assert_eq!(ask(&mut on_x, b"q").await, "z1:q");
}

/// Two controllers reusing one generation for different desired states must
/// not both be told the first one succeeded. A same-generation content mismatch
/// is a terminal refusal, while an identical resubmission still replays (R8).
#[tokio::test]
async fn a_same_generation_with_different_content_is_refused() {
    let echo1 = spawn_echo("v1:").await;
    let echo2 = spawn_echo("v2:").await;
    let a = free_addr();
    let b = free_addr();

    let mut rec = Reconciler::new();
    rec.reconcile(request(5, &[("a", TestSpec::tcp(a, echo1))]))
        .await
        .unwrap();

    let err = rec
        .reconcile(request(5, &[("b", TestSpec::tcp(b, echo2))]))
        .await
        .expect_err("a same-generation content mismatch must be refused");
    match err {
        ReconcileError::Stale { active } => assert_eq!(active, 5),
        other => panic!("expected a terminal refusal, got {:?}", other),
    }

    // the first desired state is untouched: a serves, b was never bound
    let mut on_a = TcpStream::connect(a).await.unwrap();
    assert_eq!(ask(&mut on_a, b"x").await, "v1:x");
    assert!(
        TcpListener::bind(b).await.is_ok(),
        "the mismatched submission bound nothing"
    );

    // an identical resubmission of generation 5 still replays cleanly
    let replay = rec
        .reconcile(request(5, &[("a", TestSpec::tcp(a, echo1))]))
        .await
        .unwrap();
    assert_eq!(replay.generation, 5);
    assert_eq!(replay.results.len(), 1);
    assert_eq!(replay.results[0].action, SlotAction::Created);
}

/// Every submitted endpoint has its caller-owned derived state refreshed
/// exactly once per generation — not zero times, which would let rotated
/// material go unnoticed, and not twice, which would double every read.
#[tokio::test]
async fn every_submitted_endpoint_is_refreshed_once_per_generation() {
    let echo = spawn_echo("v1:").await;
    let (a, b) = (free_addr(), free_addr());

    let mut rec = Reconciler::new();
    rec.reconcile(request(
        1,
        &[("a", TestSpec::tcp(a, echo)), ("b", TestSpec::tcp(b, echo))],
    ))
    .await
    .unwrap();
    assert_eq!(refreshes(&a), 1, "a is refreshed once for generation 1");
    assert_eq!(refreshes(&b), 1, "b is refreshed once for generation 1");

    rec.reconcile(request(
        2,
        &[("a", TestSpec::tcp(a, echo)), ("b", TestSpec::tcp(b, echo))],
    ))
    .await
    .unwrap();
    assert_eq!(refreshes(&a), 2, "a is refreshed once more for generation 2");
    assert_eq!(refreshes(&b), 2, "b is refreshed once more for generation 2");
}

/// Material realm does not own changes without any field of the
/// description changing — a certificate replaced in place. The refreshed
/// derived state must make that a replace, or the endpoint keeps serving
/// pre-rotation material while the control plane reports convergence.
#[tokio::test]
async fn rotated_material_makes_an_otherwise_identical_spec_a_replace() {
    let echo1 = spawn_echo("v1:").await;
    let echo2 = spawn_echo("v2:").await;
    let a = free_addr();
    let spec = TestSpec::tcp(a, echo1);

    let mut rec = Reconciler::new();
    rec.reconcile(request(1, &[("a", spec.clone())])).await.unwrap();

    // the material behind the endpoint is replaced in place: the description
    // the agent submits is byte-for-byte the one it submitted before
    rotate_material(&a, echo2);

    let response = rec.reconcile(request(2, &[("a", spec)])).await.unwrap();
    assert_eq!(
        response.results[0].action,
        SlotAction::Updated,
        "rotated material must rebuild the endpoint: {:?}",
        response.results
    );

    // and the endpoint really serves the rotated material
    let mut on_a = TcpStream::connect(a).await.unwrap();
    assert_eq!(ask(&mut on_a, b"x").await, "v2:x");
}

/// The derived field must stay out of the submission hash, so the replay
/// contract (R8) is unchanged for genuine retries. A genuine
/// retry — same generation, same description, unchanged material — still
/// replays the first answer, even when the derived state is not its default.
#[tokio::test]
async fn a_retry_with_unchanged_material_still_replays() {
    let echo1 = spawn_echo("v1:").await;
    let echo2 = spawn_echo("v2:").await;
    let a = free_addr();
    // the material is already something other than its default, so the derived
    // field is non-default in both submissions
    rotate_material(&a, echo2);
    let spec = TestSpec::tcp(a, echo1);

    let mut rec = Reconciler::new();
    let first = rec.reconcile(request(5, &[("a", spec.clone())])).await.unwrap();
    assert_eq!(first.results[0].action, SlotAction::Created);

    // the endpoint serves what the material says, not what the description says
    let mut established = TcpStream::connect(a).await.unwrap();
    assert_eq!(ask(&mut established, b"x").await, "v2:x");

    let replay = rec
        .reconcile(request(5, &[("a", spec)]))
        .await
        .expect("an unchanged retry of the active generation replays");
    assert_eq!(replay.generation, 5);
    assert_eq!(replay.results.len(), first.results.len());
    assert_eq!(replay.results[0].action, first.results[0].action);

    // nothing was disturbed a second time
    assert_eq!(ask(&mut established, b"y").await, "v2:y");
    assert_eq!(rec.status().len(), 1, "no duplicate endpoint was created");
}

/// Once the material has rotated, the active generation's answer no
/// longer describes what the endpoint is serving. Replaying it would be a
/// false success that persists until something unrelated forces the next
/// generation, so the resubmission is refused as a conflict instead.
#[tokio::test]
async fn a_generation_resubmitted_after_its_material_rotated_is_refused() {
    let echo1 = spawn_echo("v1:").await;
    let echo2 = spawn_echo("v2:").await;
    let a = free_addr();
    let spec = TestSpec::tcp(a, echo1);

    let mut rec = Reconciler::new();
    rec.reconcile(request(5, &[("a", spec.clone())])).await.unwrap();

    rotate_material(&a, echo2);

    let err = rec
        .reconcile(request(5, &[("a", spec)]))
        .await
        .expect_err("the active generation's answer no longer holds");
    match err {
        ReconcileError::Stale { active } => assert_eq!(active, 5),
        other => panic!("expected a terminal refusal, got {:?}", other),
    }
    assert!(!err.is_retryable(), "resubmitting the same generation cannot help");

    // the endpoint was not disturbed: the refusal tells the caller to advance
    // the generation, it does not tear anything down
    let mut on_a = TcpStream::connect(a).await.unwrap();
    assert_eq!(ask(&mut on_a, b"x").await, "v1:x");

    // and the next generation applies the rotated material
    let response = rec
        .reconcile(request(6, &[("a", TestSpec::tcp(a, echo1))]))
        .await
        .unwrap();
    assert_eq!(response.results[0].action, SlotAction::Updated);
    let mut on_a = TcpStream::connect(a).await.unwrap();
    assert_eq!(ask(&mut on_a, b"x").await, "v2:x");
}

/// A desired-state shape whose spec comparison panics, used only to prove the
/// reconciler survives a panic while handling one request.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct PanicSpec {
    listen: String,
    remote: SocketAddr,
    /// poison the comparison the reconciler makes while diffing
    panic_on_eq: bool,
    /// poison the derived-state refresh instead, so the failure lands on a
    /// different call site with the same "one endpoint, not the reconciler"
    /// expectation
    panic_on_refresh: bool,
}

impl PartialEq for PanicSpec {
    fn eq(&self, other: &Self) -> bool {
        if self.panic_on_eq || other.panic_on_eq {
            panic!("boom: comparing a poisoned spec");
        }
        self.listen == other.listen && self.remote == other.remote
    }
}

impl EndpointSource for PanicSpec {
    fn refresh(&mut self) {
        if self.panic_on_refresh {
            panic!("boom: refreshing a poisoned spec");
        }
    }

    fn build(&self) -> Result<EndpointSpec, String> {
        let laddr: SocketAddr = self.listen.parse().map_err(|e| format!("bad listen: {}", e))?;
        Ok(EndpointSpec {
            endpoint: Endpoint {
                laddr,
                raddr: RemoteAddr::SocketAddr(self.remote),
                bind_opts: Default::default(),
                conn_opts: Default::default(),
                extra_raddrs: Vec::new(),
            },
            tcp: true,
            udp: false,
            drain: None,
            material: None,
        })
    }
}

/// A panic while handling one request must not permanently kill the
/// reconciler. The panicking request is answered with an error and the loop
/// keeps serving every later request.
#[tokio::test]
async fn a_panic_handling_one_request_does_not_kill_the_reconciler() {
    let echo = spawn_echo("v1:").await;
    let a = free_addr();
    let b = free_addr();

    let panic_spec = |listen: SocketAddr, panic_on_eq: bool| PanicSpec {
        listen: listen.to_string(),
        remote: echo,
        panic_on_eq,
        panic_on_refresh: false,
    };
    let one = |id: &str, spec: PanicSpec| ReconcileRequest {
        generation: 0,
        endpoints: vec![DesiredEndpoint {
            id: id.to_string(),
            spec,
        }],
    };

    let handle = Reconciler::<PanicSpec>::new().spawn();

    // gen1 applies cleanly: there is no previous spec to compare
    handle
        .reconcile(ReconcileRequest {
            generation: 1,
            ..one("a", panic_spec(a, false))
        })
        .await
        .unwrap();

    // gen2 resubmits "a", forcing a comparison that panics inside the handler
    let boom = handle
        .reconcile(ReconcileRequest {
            generation: 2,
            ..one("a", panic_spec(a, true))
        })
        .await;
    assert!(boom.is_err(), "the panicking request is answered with an error");

    // the reconciler is still alive: a later request is served normally
    handle
        .reconcile(ReconcileRequest {
            generation: 3,
            endpoints: vec![
                DesiredEndpoint {
                    id: "a".into(),
                    spec: panic_spec(a, false),
                },
                DesiredEndpoint {
                    id: "b".into(),
                    spec: panic_spec(b, false),
                },
            ],
        })
        .await
        .expect("the reconciler must survive a panic and keep serving");

    assert!(
        !handle.status().await.is_empty(),
        "status must still answer after a panic"
    );
}

#[tokio::test]
async fn an_endpoint_whose_refresh_panics_fails_alone() {
    let echo = spawn_echo("v1:").await;
    let a = free_addr();

    let mut rec = Reconciler::new();
    let response = rec
        .reconcile(ReconcileRequest {
            generation: 1,
            endpoints: vec![DesiredEndpoint {
                id: "a".into(),
                spec: PanicSpec {
                    listen: a.to_string(),
                    remote: echo,
                    panic_on_eq: false,
                    panic_on_refresh: true,
                },
            }],
        })
        .await
        .expect("the reconciler survives a panicking refresh");

    assert_eq!(response.state, GenerationState::PartiallyApplied);
    assert_eq!(response.results[0].action, SlotAction::Failed);
    let error = response.results[0].error.as_deref().unwrap_or_default();
    assert!(
        error.contains("refresh"),
        "the failure must name what could not run, got: {}",
        error
    );

    // the reconciler is still answering
    assert_eq!(rec.status().len(), 0);
}
