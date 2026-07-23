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
use std::io;
use std::net::SocketAddr;

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

        if let Some(active) = self.active {
            if generation < active {
                log::warn!(
                    "[reconcile]refused generation {}, active generation is {}",
                    generation,
                    active
                );
                return Err(ReconcileError::Stale { active });
            }

            // a repeated submission of the active generation replays the first
            // answer: no duplicate endpoint, no second disturbance (R8, AE4)
            if generation == active {
                if let Some(last) = &self.last {
                    log::debug!("[reconcile]replaying generation {}", generation);
                    return Ok(last.clone());
                }
            }
        }

        if request.endpoints.is_empty() {
            log::warn!(
                "[reconcile]generation {} declares an empty desired state: removing every endpoint",
                generation
            );
        }

        let response = self.apply_generation(generation, request.endpoints).await;

        self.active = Some(generation);
        self.partial = response.state == GenerationState::PartiallyApplied;
        self.last = Some(response.clone());
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
            let built = match spec.build() {
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

            match spec.build() {
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
        mark_duplicates(&mut plans);

        // ---- removals: everything applied but no longer desired ------------
        for id in self.applied.keys() {
            if !plans.contains_key(id) {
                plans.insert(id.clone(), Plan::Delete);
            }
        }

        // ---- ordering: free the addresses somebody else needs first (R27) --
        self.release_contended_addresses(&plans).await;

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
                        });
                    }
                }
                Plan::Invalid { error } => {
                    // an invalid endpoint keeps whatever was serving before
                    let protos = self.protos_of(&id);
                    let protos = if protos.is_empty() { vec![Proto::Tcp] } else { protos };
                    desired.remove(&id);
                    for proto in protos {
                        results.push(EndpointResult {
                            id: id.clone(),
                            proto,
                            action: SlotAction::Failed,
                            error: Some(error.clone()),
                        });
                    }
                }
                Plan::Start { built } | Plan::Replace { built } => {
                    let outcomes = self.manager.apply(id.clone(), generation, built).await;
                    if outcomes.iter().any(|o| o.action == SlotAction::Failed) {
                        // a failed slot is not part of the applied state, so a
                        // later generation retries it instead of seeing it as
                        // unchanged (R25)
                        desired.remove(&id);
                    }
                    results.extend(into_results(&id, outcomes));
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

    /// Stop accepting on endpoints whose address another endpoint of this
    /// generation is about to take.
    ///
    /// Without this, swapping two addresses in one generation could not
    /// converge: each bind would hit the other's listener. Stopping is safe
    /// here because the connections keep running in their cohort.
    async fn release_contended_addresses(&mut self, plans: &BTreeMap<EndpointId, Plan>) {
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
            return;
        }

        // addresses this generation is going to release
        let mut releasing: Vec<(EndpointId, Proto, SocketAddr)> = Vec::new();
        for (id, plan) in plans {
            let leaving = match plan {
                Plan::Delete => true,
                Plan::Replace { built } => [Proto::Tcp, Proto::Udp].into_iter().any(|proto| {
                    self.manager
                        .active_endpoint(id, proto)
                        .map(|e| e.laddr != built.endpoint.laddr || !built.protos().contains(&proto))
                        .unwrap_or(false)
                }),
                _ => false,
            };

            if !leaving {
                continue;
            }

            for proto in [Proto::Tcp, Proto::Udp] {
                if let Some(endpoint) = self.manager.active_endpoint(id, proto) {
                    releasing.push((id.clone(), proto, endpoint.laddr));
                }
            }
        }

        for (id, proto, laddr) in releasing {
            if wanted.contains(&(laddr, proto)) {
                log::debug!(
                    "[reconcile]releasing {} on {}/{} for another endpoint",
                    id,
                    laddr,
                    proto
                );
                self.manager.stop_accept(&id, proto).await;
            }
        }
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
                match message {
                    Message::Reconcile(request, reply) => {
                        let response = this.reconcile(request).await;
                        let _ = reply.send(response);
                    }
                    Message::Status(reply) => {
                        let _ = reply.send(this.status());
                    }
                    Message::Generation(reply) => {
                        let _ = reply.send((this.active_generation(), this.is_partial(), this.is_ready()));
                    }
                    Message::SetReady(ready, reply) => {
                        this.set_ready(ready);
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
        })
        .collect()
}

/// Fail every endpoint that shares a listen address and protocol with another
/// endpoint of the same generation, deterministically and before any bind.
fn mark_duplicates(plans: &mut BTreeMap<EndpointId, Plan>) {
    let mut seen: BTreeMap<(SocketAddr, Proto), Vec<EndpointId>> = BTreeMap::new();

    for (id, plan) in plans.iter() {
        if let Plan::Start { built } | Plan::Replace { built } = plan {
            for proto in built.protos() {
                seen.entry((built.endpoint.laddr, proto)).or_default().push(id.clone());
            }
        }
    }

    for ((laddr, proto), ids) in seen {
        if ids.len() < 2 {
            continue;
        }
        let error = format!("duplicate listen address {} for {} in one generation", laddr, proto);
        for id in ids {
            plans.insert(id, Plan::Invalid { error: error.clone() });
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
