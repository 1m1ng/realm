//! Desired-state reconciliation.
//!
//! The agent publishes the node's *complete* desired state for a generation;
//! realm computes the difference against what it is serving and only touches
//! the endpoints that actually changed. Two properties make this safe to retry:
//!
//! - **single-flight** — every submission goes through one serial consumer, so
//!   two generations can never be applied at the same time (R24). The property
//!   comes from the structure, not from lock discipline.
//! - **idempotent generations** — the caller's generation is monotonic; a
//!   repeated submission returns the first result without disturbing traffic a
//!   second time, and an older one is refused (R8).
//!
//! Endpoints succeed or fail one by one: a generation in which some endpoint
//! failed is *partially applied*, still advances the active generation, and is
//! healed by submitting a later generation (R9, R25).

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use std::io;
use std::net::SocketAddr;
use std::panic::AssertUnwindSafe;

use futures::FutureExt;
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::sync::{mpsc, oneshot};

use super::manager::{
    EndpointId, EndpointManager, EndpointSpec, EndpointStatus, Generation, Proto, SlotAction, SlotOutcome,
};
use super::snapshot::{RestoreOutcome, Snapshot, SnapshotStore};

/// A desired endpoint, as submitted by the caller.
#[derive(Debug, Clone)]
pub struct DesiredEndpoint<S> {
    /// caller-provided stable key, opaque to realm (R7)
    pub id: EndpointId,
    pub spec: S,
}

/// One desired-state submission (R6).
#[derive(Debug, Clone)]
pub struct ReconcileRequest<S> {
    pub generation: Generation,
    pub endpoints: Vec<DesiredEndpoint<S>>,
}

/// Whether a generation was applied in full.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GenerationState {
    Applied,
    PartiallyApplied,
}

/// Per `(id, protocol)` outcome of a submission (R9, R23).
#[derive(Debug, Clone)]
pub struct EndpointResult {
    pub id: EndpointId,
    pub proto: Proto,
    pub action: SlotAction,
    pub error: Option<String>,
    /// For a failure: whether resubmitting the same desired state may succeed
    /// (R31). `None` when this endpoint did not fail.
    pub retryable: Option<bool>,
}

/// Answer to a submission.
#[derive(Debug, Clone)]
pub struct ReconcileResponse {
    pub generation: Generation,
    pub state: GenerationState,
    pub results: Vec<EndpointResult>,
}

/// Why a submission was not applied at all.
///
/// Split along the axis the caller needs in order to decide what to do (R31):
/// a terminal error will fail again unchanged, a retryable one may not.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReconcileError {
    /// the submitted generation is older than the active one — terminal
    Stale { active: Generation },
    /// the process has not finished restoring its snapshot yet — retryable
    NotReady,
    /// the reconciler is gone — retryable
    Internal(String),
}

impl ReconcileError {
    /// Whether retrying the same request may succeed.
    pub fn is_retryable(&self) -> bool {
        match self {
            ReconcileError::Stale { .. } => false,
            ReconcileError::NotReady | ReconcileError::Internal(_) => true,
        }
    }
}

impl Display for ReconcileError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ReconcileError::Stale { active } => {
                write!(f, "stale generation, active generation is {}", active)
            }
            ReconcileError::NotReady => write!(f, "not ready: snapshot restore has not finished"),
            ReconcileError::Internal(e) => write!(f, "internal error: {}", e),
        }
    }
}

impl std::error::Error for ReconcileError {}

/// A desired endpoint description the reconciler can diff and build.
///
/// Implemented by the caller's own configuration shape, so that realm_core
/// stays free of the serde-facing configuration types. It must round-trip
/// through serde, since the last-known-good snapshot stores exactly these
/// descriptions (KTD9).
pub trait EndpointSource: Clone + PartialEq + Send + Sync + Serialize + DeserializeOwned + 'static {
    /// Turn the description into a lifecycle spec, or say why it is invalid.
    ///
    /// Must have no side effect: it runs during the validation phase, before
    /// anything is bound (R3, R4).
    fn build(&self) -> Result<EndpointSpec, String>;

    /// Recompute the state this description derives from material realm does
    /// not own, so the diff sees what is on disk *now*.
    ///
    /// The reconciler decides what to touch by comparing descriptions, and a
    /// description that names a file says nothing about the file's contents:
    /// replacing a certificate in place leaves every field identical, so the
    /// diff sees nothing to do and the endpoint keeps serving pre-rotation
    /// material. A description that carries a digest of such material — in a
    /// field serde does not see, so it enters the diff without disturbing the
    /// submission hash — recomputes it here.
    ///
    /// Called on every incoming description before it is diffed, and on every
    /// description restored from a snapshot. Runs off the reconciler task, so
    /// blocking reads are allowed. The default does nothing.
    fn refresh(&mut self) {}
}

/// Stable id derived from a listen address and the protocols of a rule (R26).
///
/// The static mode uses this to submit its configuration as generation 0, and
/// an agent that computes the same id sees its first equivalent submission as
/// unchanged instead of a rebuild. The format is part of the contract:
/// `"<protos>:<listen address>"`, where `<protos>` is `tcp`, `udp` or `tcpudp`.
pub fn derive_id(laddr: &SocketAddr, tcp: bool, udp: bool) -> EndpointId {
    let protos = match (tcp, udp) {
        (true, true) => "tcpudp",
        (false, true) => "udp",
        _ => "tcp",
    };
    format!("{}:{}", protos, laddr)
}

/// What the diff decided for one endpoint, before anything is applied.
enum Plan {
    Unchanged,
    Start { built: EndpointSpec },
    Replace { built: EndpointSpec },
    Delete,
    Invalid { error: String },
}

/// Applies desired states, one generation at a time.
pub struct Reconciler<S: EndpointSource> {
    manager: EndpointManager,
    /// desired state currently applied, per id
    applied: BTreeMap<EndpointId, S>,
    /// the generation that is active, once one has been applied (R25)
    active: Option<Generation>,
    /// response of the active generation, replayed for idempotent retries (R8)
    last: Option<ReconcileResponse>,
    /// content digest of the active generation's request, so a same-generation
    /// resubmission with *different* content is caught instead of being handed
    /// the first payload's success (R8)
    last_digest: Option<u64>,
    /// whether the active generation is only partially applied (R25, R34)
    partial: bool,
    /// false until a snapshot restore has finished (R33)
    ready: bool,
    /// where the last-known-good state is persisted (R19)
    snapshot: Option<SnapshotStore>,
}

impl<S: EndpointSource> Default for Reconciler<S> {
    fn default() -> Self {
        Self::new()
    }
}

impl<S: EndpointSource> Reconciler<S> {
    pub fn new() -> Self {
        Self {
            manager: EndpointManager::new(),
            applied: BTreeMap::new(),
            active: None,
            last: None,
            last_digest: None,
            partial: false,
            ready: true,
            snapshot: None,
        }
    }

    /// Reconciler that refuses submissions until [`Reconciler::set_ready`]
    /// is called — used while a snapshot is being restored (R33).
    pub fn not_ready() -> Self {
        Self {
            ready: false,
            ..Self::new()
        }
    }

    /// Reconciler backed by a last-known-good snapshot.
    ///
    /// Starts out not ready: [`Reconciler::restore`] must run first, so that a
    /// submission arriving during startup is refused as retryable instead of
    /// being applied against an empty state (R33).
    pub fn with_snapshot(store: SnapshotStore) -> Self {
        Self {
            ready: false,
            snapshot: Some(store),
            ..Self::new()
        }
    }

    /// Mark the process as ready to accept submissions.
    pub fn set_ready(&mut self, ready: bool) {
        self.ready = ready;
    }

    pub fn is_ready(&self) -> bool {
        self.ready
    }

    /// The generation currently in effect, if any (R11).
    pub fn active_generation(&self) -> Option<Generation> {
        self.active
    }

    /// Whether the active generation was only partially applied (R25, R34).
    pub fn is_partial(&self) -> bool {
        self.partial
    }

    /// The desired state currently applied, for snapshotting (R19).
    pub fn applied_state(&self) -> &BTreeMap<EndpointId, S> {
        &self.applied
    }

    /// Live state of every endpoint (R11, R36).
    pub fn status(&mut self) -> Vec<EndpointStatus> {
        self.manager.status()
    }

    /// Access to the underlying manager, for callers that drive single
    /// endpoints (the static mode's generation 0, snapshot restore).
    pub fn manager_mut(&mut self) -> &mut EndpointManager {
        &mut self.manager
    }

    /// Apply a desired state.
    pub async fn reconcile(&mut self, request: ReconcileRequest<S>) -> Result<ReconcileResponse, ReconcileError> {
        if !self.ready {
            return Err(ReconcileError::NotReady);
        }

        let generation = request.generation;
        let digest = digest_of(&request.endpoints);
        let mut endpoints = request.endpoints;

        if let Some(active) = self.active {
            if generation < active {
                log::warn!(
                    "[reconcile]refused generation {}, active generation is {}",
                    generation,
                    active
                );
                return Err(ReconcileError::Stale { active });
            }
        }

        // Recompute what the descriptions derive from material realm does not
        // own, once per submission and before anything looks at them: both the
        // replay decision below and the diff must judge the desired state
        // against what is on disk now, not against what it said when it was
        // written. Off the reconciler task, since a refresh may read files.
        let mut unrefreshed: BTreeMap<EndpointId, String> = BTreeMap::new();
        for endpoint in endpoints.iter_mut() {
            match refresh_offloaded(&endpoint.spec).await {
                Ok(refreshed) => endpoint.spec = refreshed,
                Err(error) => {
                    log::error!("[reconcile]cannot refresh {}: {}", endpoint.id, error);
                    unrefreshed.insert(endpoint.id.clone(), error);
                }
            }
        }

        // a repeated submission of the active generation replays the first
        // answer: no duplicate endpoint, no second disturbance (R8, AE4) —
        // but only when the *content* matches. Two controllers reusing one
        // generation for different desired states must not both be told the
        // first one succeeded, so a same-generation content mismatch is a
        // terminal conflict rather than a false replay.
        if self.active == Some(generation) && unrefreshed.is_empty() {
            match &self.last {
                Some(last) if self.last_digest == Some(digest) => {
                    // the description is the one that was applied, but the
                    // material behind it is not. Replaying "converged" for
                    // material the endpoint is no longer running would be a
                    // false success lasting until some unrelated change forces
                    // the next generation, so this is a conflict too.
                    if let Some(id) = self.material_drifted(&endpoints) {
                        log::warn!(
                            "[reconcile]generation {} resubmitted after {}'s material changed, refusing",
                            generation,
                            id
                        );
                        return Err(ReconcileError::Stale { active: generation });
                    }
                    log::debug!("[reconcile]replaying generation {}", generation);
                    return Ok(last.clone());
                }
                Some(_) => {
                    log::warn!(
                        "[reconcile]generation {} resubmitted with different content, refusing",
                        generation
                    );
                    return Err(ReconcileError::Stale { active: generation });
                }
                None => {}
            }
        }

        if endpoints.is_empty() {
            log::warn!(
                "[reconcile]generation {} declares an empty desired state: removing every endpoint",
                generation
            );
        }

        let response = self.apply_generation(generation, endpoints, unrefreshed).await;

        self.active = Some(generation);
        self.partial = response.state == GenerationState::PartiallyApplied;
        self.last = Some(response.clone());
        self.last_digest = Some(digest);
        self.persist();
        Ok(response)
    }

    /// Write the last-known-good snapshot, if one is configured.
    ///
    /// A failure here never fails the reconcile: the forwarding change is
    /// already in effect, and losing the snapshot only costs a slower recovery
    /// after a restart.
    fn persist(&self) {
        let Some(store) = &self.snapshot else {
            return;
        };

        let snapshot = Snapshot {
            generation: self.active.unwrap_or_default(),
            partial: self.partial,
            endpoints: self.applied.clone(),
        };

        if let Err(e) = store.store(&snapshot) {
            log::error!("[reconcile]failed to write snapshot to {:?}: {}", store.path(), e);
        }
    }

    /// Restore the last-known-good snapshot and become ready.
    ///
    /// Endpoints are restored one by one: one that cannot come back is marked
    /// failed and the process keeps serving the rest, with the active
    /// generation carrying the partial mark (R34). Only an unreadable or
    /// corrupt snapshot is an error — restoring nothing where something was
    /// expected would silently drop every endpoint.
    pub async fn restore(&mut self) -> io::Result<RestoreOutcome> {
        let Some(store) = self.snapshot.clone() else {
            self.ready = true;
            return Ok(RestoreOutcome::default());
        };

        let snapshot: Option<Snapshot<S>> = store.load()?;

        let Some(snapshot) = snapshot else {
            log::info!("[reconcile]no snapshot at {:?}, starting empty", store.path());
            self.ready = true;
            return Ok(RestoreOutcome::default());
        };

        let mut outcome = RestoreOutcome {
            generation: Some(snapshot.generation),
            partial: snapshot.partial,
            ..RestoreOutcome::default()
        };

        for (id, spec) in snapshot.endpoints {
            // the snapshot describes the material of the moment it was written,
            // and that material may have been replaced while the process was
            // down. Refresh before building *and* before the description enters
            // the applied set, so the restart comes back on what is on disk now
            // and the agent's first submission does not diff as changed.
            let spec = match refresh_offloaded(&spec).await {
                Ok(refreshed) => refreshed,
                Err(e) => {
                    log::error!("[reconcile]cannot restore {}: {}", id, e);
                    outcome.failed.push((id, e));
                    continue;
                }
            };

            // build off the reconciler task: it may resolve dns, which blocks
            let built = match build_offloaded(&spec).await {
                Ok(x) => x,
                Err(e) => {
                    log::error!("[reconcile]cannot restore {}: {}", id, e);
                    outcome.failed.push((id, e));
                    continue;
                }
            };

            let outcomes = self.manager.apply(id.clone(), snapshot.generation, built).await;
            match outcomes.iter().find(|o| o.action == SlotAction::Failed) {
                Some(failure) => {
                    let error = failure.error.clone().unwrap_or_else(|| "failed to start".into());
                    log::error!("[reconcile]cannot restore {}: {}", id, error);
                    outcome.failed.push((id, error));
                }
                None => {
                    outcome.restored += 1;
                    self.applied.insert(id, spec);
                }
            }
        }

        outcome.partial |= !outcome.failed.is_empty();

        self.active = Some(snapshot.generation);
        self.partial = outcome.partial;
        self.ready = true;

        log::info!(
            "[reconcile]restored generation {}: {} endpoints, {} failed",
            snapshot.generation,
            outcome.restored,
            outcome.failed.len()
        );

        Ok(outcome)
    }

    /// Stop every endpoint this reconciler manages.
    ///
    /// Used when a process gives up its endpoints deliberately; the snapshot is
    /// left untouched, since it describes the desired state, not the runtime.
    pub async fn shutdown(&mut self) {
        for id in self.manager.ids() {
            self.manager.remove(&id, self.active.unwrap_or_default()).await;
        }
    }

    async fn apply_generation(
        &mut self,
        generation: Generation,
        endpoints: Vec<DesiredEndpoint<S>>,
        mut unrefreshed: BTreeMap<EndpointId, String>,
    ) -> ReconcileResponse {
        // ---- validation, without side effects (R3, R4) -------------------
        let mut desired: BTreeMap<EndpointId, S> = BTreeMap::new();
        let mut plans: BTreeMap<EndpointId, Plan> = BTreeMap::new();

        for DesiredEndpoint { id, spec } in endpoints {
            if plans.contains_key(&id) {
                plans.insert(
                    id.clone(),
                    Plan::Invalid {
                        error: format!("duplicate endpoint id `{}` in one generation", id),
                    },
                );
                continue;
            }

            // an endpoint whose derived state could not be recomputed is not
            // safe to diff: its comparison would turn on a default value rather
            // than on what is actually on disk. Fail it here, where the invalid
            // arm below keeps a serving listener up and reports the reason.
            if let Some(error) = unrefreshed.remove(&id) {
                plans.insert(id, Plan::Invalid { error });
                continue;
            }

            // build off the reconciler task: `build` may resolve dns, which
            // blocks the single serial consumer that also answers status and
            // readiness. A join failure (including a build panic) is a failed
            // endpoint, not a dead reconciler.
            match build_offloaded(&spec).await {
                Ok(built) => {
                    desired.insert(id.clone(), spec.clone());
                    let plan = match self.applied.get(&id) {
                        Some(previous) if previous == &spec && self.all_running(&id, &built) => Plan::Unchanged,
                        Some(_) => Plan::Replace { built },
                        None => Plan::Start { built },
                    };
                    plans.insert(id, plan);
                }
                Err(error) => {
                    plans.insert(id, Plan::Invalid { error });
                }
            }
        }

        // ---- cross-endpoint validation: duplicate listen + protocol (R28) --
        // an endpoint left unchanged is still listening, so it counts against a
        // new endpoint trying to take its address just like a fresh collision.
        let incumbents = self.unchanged_incumbents(&plans);
        mark_duplicates(&mut plans, &incumbents);

        // ---- removals: every id the manager still holds that nothing desires -
        // deriving the deletion set from what the manager actually serves — not
        // from `self.applied` — means an endpoint whose failed replace kept its
        // old listener still gets deleted when a later generation drops it (#5).
        for id in self.manager.ids() {
            plans.entry(id).or_insert(Plan::Delete);
        }

        // ---- ordering: free the addresses somebody else needs first (R27) --
        let released = self.release_contended_addresses(&plans).await;

        // ---- application ---------------------------------------------------
        let mut results = Vec::new();

        for (id, plan) in plans {
            match plan {
                Plan::Unchanged => {
                    for proto in self.protos_of(&id) {
                        results.push(EndpointResult {
                            id: id.clone(),
                            proto,
                            action: SlotAction::Unchanged,
                            error: None,
                            retryable: None,
                        });
                    }
                }
                Plan::Invalid { error } => {
                    // an invalid endpoint keeps whatever was serving before.
                    // A brand new one has no slots yet and its protocols could
                    // not be determined either — validation is what failed — so
                    // it is reported once, under tcp.
                    let protos = self.protos_of(&id);
                    let protos = if protos.is_empty() { vec![Proto::Tcp] } else { protos };
                    // keep the previous applied spec: an invalid resubmission of
                    // an endpoint that is still serving must not drop it from the
                    // applied set, or a later deletion would never reach it.
                    if !self.manager.ids().contains(&id) {
                        desired.remove(&id);
                    } else if let Some(prev) = self.applied.get(&id) {
                        desired.insert(id.clone(), prev.clone());
                    }
                    for proto in protos {
                        results.push(EndpointResult {
                            id: id.clone(),
                            proto,
                            action: SlotAction::Failed,
                            error: Some(error.clone()),
                            // a description realm cannot make sense of will not
                            // start making sense on a retry (R31)
                            retryable: Some(false),
                        });
                    }
                }
                Plan::Start { built } | Plan::Replace { built } => {
                    let previous_spec = self.applied.get(&id).cloned();
                    let outcomes = self.manager.apply(id.clone(), generation, built).await;
                    let failed = outcomes.iter().any(|o| o.action == SlotAction::Failed);
                    results.extend(into_results(&id, outcomes));

                    if failed {
                        // this endpoint's address may have been released for a
                        // contended handoff; if its replacement then failed, try
                        // to put it back on its original address so a swap gone
                        // wrong never strands a listener that was serving (#10,
                        // R27).
                        if released.contains(&id) && !self.serving(&id) {
                            self.restore_released(&id, generation, previous_spec.as_ref(), &mut results)
                                .await;
                        }

                        // keep the applied set matching what the manager really
                        // serves: an id that kept its old listener stays applied
                        // under its *previous* spec, one that is still partly up
                        // keeps the new spec, one left with nothing is dropped so
                        // a later generation retries it (#5, #9, R25).
                        if self.serving(&id) {
                            if let Some(prev) = previous_spec {
                                desired.insert(id.clone(), prev);
                            }
                        } else {
                            desired.remove(&id);
                        }
                    }
                }
                Plan::Delete => {
                    let outcomes = self.manager.remove(&id, generation).await;
                    desired.remove(&id);
                    results.extend(into_results(&id, outcomes));
                }
            }
        }

        self.applied = desired;

        let state = if results.iter().any(|r| r.action == SlotAction::Failed) {
            GenerationState::PartiallyApplied
        } else {
            GenerationState::Applied
        };

        log::info!(
            "[reconcile]generation {} {}: {} endpoint results",
            generation,
            match state {
                GenerationState::Applied => "applied",
                GenerationState::PartiallyApplied => "partially applied",
            },
            results.len()
        );

        ReconcileResponse {
            generation,
            state,
            results,
        }
    }

    /// The first endpoint of a same-generation resubmission whose refreshed
    /// derived state no longer matches what is applied, if any.
    ///
    /// Only a difference serde cannot see counts. The digest has already
    /// established that the submitted descriptions are byte-identical to the
    /// active generation's, so a difference serde *can* see comes from a
    /// partial application having kept a previous spec (#5, #9) — that is a
    /// genuine retry and must still replay, not a rotation of the material
    /// behind an unchanged description.
    fn material_drifted<'a>(&self, endpoints: &'a [DesiredEndpoint<S>]) -> Option<&'a EndpointId> {
        endpoints
            .iter()
            .find(|DesiredEndpoint { id, spec }| {
                self.applied
                    .get(id)
                    .is_some_and(|applied| applied != spec && serializes_alike(applied, spec))
            })
            .map(|endpoint| &endpoint.id)
    }

    /// Whether every protocol the desired spec asks for is actually running.
    ///
    /// A spec that did not change but whose slot is failed must be retried, not
    /// reported as unchanged (R25).
    fn all_running(&self, id: &str, built: &EndpointSpec) -> bool {
        built
            .protos()
            .into_iter()
            .all(|proto| self.manager.active_endpoint(id, proto).is_some())
    }

    fn protos_of(&self, id: &str) -> Vec<Proto> {
        [Proto::Tcp, Proto::Udp]
            .into_iter()
            .filter(|p| self.manager.active_endpoint(id, *p).is_some())
            .collect()
    }

    /// Whether any protocol of `id` is currently serving.
    fn serving(&self, id: &str) -> bool {
        [Proto::Tcp, Proto::Udp]
            .into_iter()
            .any(|p| self.manager.active_endpoint(id, p).is_some())
    }

    /// Listen addresses still held by endpoints this generation leaves
    /// unchanged, so `mark_duplicates` can reject a new endpoint colliding with
    /// a still-listening incumbent (R28). The incumbent carries no `built` spec
    /// of its own for the duplicate check to see.
    fn unchanged_incumbents(&self, plans: &BTreeMap<EndpointId, Plan>) -> BTreeMap<(SocketAddr, Proto), EndpointId> {
        let mut incumbents = BTreeMap::new();
        for (id, plan) in plans {
            if matches!(plan, Plan::Unchanged) {
                for proto in [Proto::Tcp, Proto::Udp] {
                    if let Some(ep) = self.manager.active_endpoint(id, proto) {
                        incumbents.insert((ep.laddr, proto), id.clone());
                    }
                }
            }
        }
        incumbents
    }

    /// Put a released endpoint back on its original address after its move
    /// failed.
    ///
    /// Best-effort: the original address is free only if the endpoint that was
    /// going to take it also failed. When the restore succeeds the endpoint is
    /// serving its previous configuration again; when it cannot, the earlier
    /// failure already stands and nothing more is reported (#10, R27).
    async fn restore_released(
        &mut self,
        id: &str,
        generation: Generation,
        previous: Option<&S>,
        results: &mut [EndpointResult],
    ) {
        let Some(previous) = previous else {
            return;
        };
        let Ok(built) = build_offloaded(previous).await else {
            return;
        };

        let outcomes = self.manager.apply(id.to_string(), generation, built).await;
        if outcomes.iter().any(|o| o.action == SlotAction::Failed) {
            return;
        }

        log::warn!("[reconcile]{} could not move, restored its previous listener", id);
        // the endpoint is serving its old configuration again: the recorded
        // failure keeps its retryable classification, but the connections are
        // no longer stranded.
        let restored: BTreeSet<Proto> = outcomes.iter().map(|o| o.proto).collect();
        for result in results.iter_mut() {
            if result.id == id && restored.contains(&result.proto) && result.action == SlotAction::Failed {
                result.error = Some(match result.error.take() {
                    Some(e) => format!("{} (previous listener restored)", e),
                    None => "move failed, previous listener restored".into(),
                });
            }
        }
    }

    /// Stop accepting on endpoints whose address another endpoint of this
    /// generation is about to take.
    ///
    /// Without this, swapping two addresses in one generation could not
    /// converge: each bind would hit the other's listener. Stopping is safe
    /// here because the connections keep running in their cohort.
    async fn release_contended_addresses(&mut self, plans: &BTreeMap<EndpointId, Plan>) -> BTreeSet<EndpointId> {
        let mut released = BTreeSet::new();

        // addresses this generation wants to bind
        let mut wanted: BTreeSet<(SocketAddr, Proto)> = BTreeSet::new();
        for (id, plan) in plans {
            if let Plan::Start { built } | Plan::Replace { built } = plan {
                for proto in built.protos() {
                    // an endpoint keeping its own address is handled by the
                    // state machine's stop-accept -> join -> bind sequence
                    if self
                        .manager
                        .active_endpoint(id, proto)
                        .map(|e| e.laddr != built.endpoint.laddr)
                        .unwrap_or(true)
                    {
                        wanted.insert((built.endpoint.laddr, proto));
                    }
                }
            }
        }

        if wanted.is_empty() {
            return released;
        }

        // addresses this generation is going to release, and whether the
        // endpoint holding each is being deleted (force-close on the delete
        // deadline) or only replaced (drain under the update policy) (#23)
        let mut releasing: Vec<(EndpointId, Proto, SocketAddr, bool)> = Vec::new();
        for (id, plan) in plans {
            let (leaving, deleting) = match plan {
                Plan::Delete => (true, true),
                Plan::Replace { built } => (
                    [Proto::Tcp, Proto::Udp].into_iter().any(|proto| {
                        self.manager
                            .active_endpoint(id, proto)
                            .map(|e| e.laddr != built.endpoint.laddr || !built.protos().contains(&proto))
                            .unwrap_or(false)
                    }),
                    false,
                ),
                _ => (false, false),
            };

            if !leaving {
                continue;
            }

            for proto in [Proto::Tcp, Proto::Udp] {
                if let Some(endpoint) = self.manager.active_endpoint(id, proto) {
                    releasing.push((id.clone(), proto, endpoint.laddr, deleting));
                }
            }
        }

        for (id, proto, laddr, deleting) in releasing {
            if wanted.contains(&(laddr, proto)) {
                let policy = self.manager.drain_policy(&id).unwrap_or_default();
                // a delete keeps its force-close deadline even when the address
                // is taken over by another endpoint; a replace drains under the
                // update policy as it would without contention (#23)
                let deadline = if deleting { policy.on_delete } else { policy.on_update };
                log::debug!(
                    "[reconcile]releasing {} on {}/{} for another endpoint",
                    id,
                    laddr,
                    proto
                );
                self.manager.stop_accept(&id, proto, deadline).await;
                // a replaced endpoint whose address was handed off is a
                // restore candidate if its own move then fails (#10)
                if !deleting {
                    released.insert(id);
                }
            }
        }

        released
    }

    /// Run this reconciler as the single consumer of a submission queue.
    ///
    /// The returned handle is cheap to clone and safe to share: submissions are
    /// serialized by the queue itself, which is what makes reconciliation
    /// single-flight (R24).
    pub fn spawn(self) -> ReconcileHandle<S> {
        let (tx, mut rx) = mpsc::channel::<Message<S>>(32);

        tokio::spawn(async move {
            let mut this = self;
            while let Some(message) = rx.recv().await {
                // A panic while handling one message must not take the whole
                // reconciler down: the handle would then degrade to "reconciler
                // is gone" forever, poisoning every future status and submission.
                // Catch it, answer that one request, and keep serving (#6). The
                // release profile unwinds, so this is sound.
                match message {
                    Message::Reconcile(request, reply) => {
                        let response = match AssertUnwindSafe(this.reconcile(request)).catch_unwind().await {
                            Ok(response) => response,
                            Err(_) => Err(ReconcileError::Internal(
                                "reconciler panicked while handling the request".into(),
                            )),
                        };
                        let _ = reply.send(response);
                    }
                    Message::Status(reply) => {
                        let statuses = AssertUnwindSafe(async { this.status() })
                            .catch_unwind()
                            .await
                            .unwrap_or_default();
                        let _ = reply.send(statuses);
                    }
                    Message::Generation(reply) => {
                        let generation =
                            AssertUnwindSafe(async { (this.active_generation(), this.is_partial(), this.is_ready()) })
                                .catch_unwind()
                                .await
                                .unwrap_or((None, false, false));
                        let _ = reply.send(generation);
                    }
                    Message::SetReady(ready, reply) => {
                        let _ = AssertUnwindSafe(async { this.set_ready(ready) }).catch_unwind().await;
                        let _ = reply.send(());
                    }
                }
            }
        });

        ReconcileHandle { tx }
    }
}

fn into_results(id: &str, outcomes: Vec<SlotOutcome>) -> Vec<EndpointResult> {
    outcomes
        .into_iter()
        .map(|o| EndpointResult {
            id: id.to_string(),
            proto: o.proto,
            action: o.action,
            error: o.error,
            retryable: o.retryable,
        })
        .collect()
}

/// Build a spec without blocking the reconciler task.
///
/// `EndpointSource::build` may resolve dns synchronously; running it on the
/// serial consumer would stall status, readiness and every other submission.
/// A join failure — a panic inside `build`, or a cancelled blocking pool —
/// becomes a failed endpoint rather than a failed generation (#11, R31).
async fn build_offloaded<S: EndpointSource>(spec: &S) -> Result<EndpointSpec, String> {
    let spec = spec.clone();
    match tokio::task::spawn_blocking(move || spec.build()).await {
        Ok(result) => result,
        Err(join) => Err(format!("building the endpoint failed: {}", join)),
    }
}

/// Refresh a description's derived state without blocking the reconciler task.
///
/// `EndpointSource::refresh` may read files — a certificate on a hung mount,
/// say — and the reconciler is the single serial consumer that also answers
/// status and readiness, so this goes to the blocking pool just as `build`
/// does. A join failure — a panic inside `refresh`, or a cancelled blocking
/// pool — is a failed endpoint, not a dead reconciler, the same way a build
/// join failure is (#11).
///
/// Falling back to the description *as submitted* would be worse than failing
/// it. A hook that never ran leaves the derived state at its default on one
/// side of the comparison while the applied side still carries the real value,
/// so the two differ for a reason that has nothing to do with the material:
/// the endpoint is torn down and rebuilt for nothing, or — on the replay path —
/// a retry is refused as a conflict that no later generation explains. Failing
/// the endpoint keeps a serving listener up, says so, and heals on the next
/// generation.
async fn refresh_offloaded<S: EndpointSource>(spec: &S) -> Result<S, String> {
    let mut owned = spec.clone();
    match tokio::task::spawn_blocking(move || {
        owned.refresh();
        owned
    })
    .await
    {
        Ok(refreshed) => Ok(refreshed),
        Err(join) if join.is_panic() => Err(format!("refreshing the endpoint panicked: {}", join)),
        Err(join) => Err(format!("refreshing the endpoint failed: {}", join)),
    }
}

/// Whether two descriptions serialize identically — that is, whether they
/// differ only in fields serde does not see.
fn serializes_alike<S: EndpointSource>(a: &S, b: &S) -> bool {
    match (serde_json::to_vec(a), serde_json::to_vec(b)) {
        (Ok(a), Ok(b)) => a == b,
        _ => false,
    }
}

/// Order-independent content digest of a submission.
///
/// Two submissions carrying the same generation but different desired states
/// must be told apart, so a same-generation resubmission is only replayed when
/// the content matches (R8).
fn digest_of<S: EndpointSource>(endpoints: &[DesiredEndpoint<S>]) -> u64 {
    let mut sorted: Vec<(&EndpointId, &S)> = endpoints.iter().map(|e| (&e.id, &e.spec)).collect();
    sorted.sort_by(|a, b| a.0.cmp(b.0));
    let bytes = serde_json::to_vec(&sorted).unwrap_or_default();
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    bytes.hash(&mut hasher);
    hasher.finish()
}

/// Fail every endpoint that shares a listen address and protocol with another
/// endpoint of the same generation, deterministically and before any bind.
///
/// `incumbents` carries the addresses of endpoints this generation leaves
/// unchanged: they are still listening, so a newcomer colliding with one is
/// just as terminal as two newcomers colliding, but only the newcomer fails —
/// the incumbent is not disturbed (R28).
fn mark_duplicates(plans: &mut BTreeMap<EndpointId, Plan>, incumbents: &BTreeMap<(SocketAddr, Proto), EndpointId>) {
    let mut seen: BTreeMap<(SocketAddr, Proto), Vec<EndpointId>> = BTreeMap::new();

    for (id, plan) in plans.iter() {
        if let Plan::Start { built } | Plan::Replace { built } = plan {
            for proto in built.protos() {
                seen.entry((built.endpoint.laddr, proto)).or_default().push(id.clone());
            }
        }
    }

    for ((laddr, proto), ids) in seen {
        if ids.len() >= 2 {
            let error = format!("duplicate listen address {} for {} in one generation", laddr, proto);
            for id in ids {
                plans.insert(id, Plan::Invalid { error: error.clone() });
            }
        } else if let Some(incumbent) = incumbents.get(&(laddr, proto)) {
            let id = &ids[0];
            if id != incumbent {
                let error = format!(
                    "duplicate listen address {} for {}: already served by endpoint `{}`",
                    laddr, proto, incumbent
                );
                plans.insert(id.clone(), Plan::Invalid { error });
            }
        }
    }
}

enum Message<S> {
    Reconcile(
        ReconcileRequest<S>,
        oneshot::Sender<Result<ReconcileResponse, ReconcileError>>,
    ),
    Status(oneshot::Sender<Vec<EndpointStatus>>),
    Generation(oneshot::Sender<(Option<Generation>, bool, bool)>),
    SetReady(bool, oneshot::Sender<()>),
}

/// Shared handle to a running reconciler.
pub struct ReconcileHandle<S> {
    tx: mpsc::Sender<Message<S>>,
}

impl<S> Clone for ReconcileHandle<S> {
    fn clone(&self) -> Self {
        Self { tx: self.tx.clone() }
    }
}

impl<S: EndpointSource> ReconcileHandle<S> {
    /// Submit a desired state.
    pub async fn reconcile(&self, request: ReconcileRequest<S>) -> Result<ReconcileResponse, ReconcileError> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(Message::Reconcile(request, tx))
            .await
            .map_err(|_| ReconcileError::Internal("reconciler is gone".into()))?;
        rx.await
            .map_err(|_| ReconcileError::Internal("reconciler dropped the request".into()))?
    }

    /// Live state of every endpoint.
    pub async fn status(&self) -> Vec<EndpointStatus> {
        let (tx, rx) = oneshot::channel();
        if self.tx.send(Message::Status(tx)).await.is_err() {
            return Vec::new();
        }
        rx.await.unwrap_or_default()
    }

    /// Active generation, whether it is partial, and whether the process is
    /// ready to accept submissions.
    pub async fn generation(&self) -> (Option<Generation>, bool, bool) {
        let (tx, rx) = oneshot::channel();
        if self.tx.send(Message::Generation(tx)).await.is_err() {
            return (None, false, false);
        }
        rx.await.unwrap_or((None, false, false))
    }

    /// Mark the process ready (or not) to accept submissions.
    pub async fn set_ready(&self, ready: bool) {
        let (tx, rx) = oneshot::channel();
        if self.tx.send(Message::SetReady(ready, tx)).await.is_ok() {
            let _ = rx.await;
        }
    }
}
