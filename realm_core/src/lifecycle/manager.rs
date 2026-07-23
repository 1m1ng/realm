//! Endpoint state machine.
//!
//! One state machine per `(id, protocol)` slot — the smallest unit that can
//! succeed or fail on its own. The manager binds before reporting a slot as
//! running, replaces a listener without touching the connections the previous
//! generation is serving, and hands those connections to a draining cohort
//! with the deadline the operation calls for.
//!
//! Timing rules, all derived from the endpoint contract:
//!
//! - an update keeps established connections (drains indefinitely by default),
//! - a delete releases the listener socket immediately and force-closes what is
//!   left once the drain deadline expires,
//! - a listener is only ever stopped through stop-accept plus a join, so its
//!   socket is provably released before anything rebinds the address.

use std::collections::BTreeMap;
use std::fmt::{Display, Formatter};
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::endpoint::Endpoint;
use crate::tcp::{bind_tcp, serve_tcp};
use crate::udp::{bind_udp, serve_udp};

use super::cohort::{Cohort, CohortStat};

/// Caller-provided stable key of an endpoint. Opaque to realm (R7).
pub type EndpointId = String;

/// Caller-provided monotonic desired-state version (R8).
pub type Generation = u64;

/// Default deadline before a deleted endpoint's connections are force-closed.
pub const DEFAULT_DELETE_DRAIN: Duration = Duration::from_secs(30);

/// One data plane of a rule.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Proto {
    Tcp,
    Udp,
}

impl Display for Proto {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Proto::Tcp => write!(f, "tcp"),
            Proto::Udp => write!(f, "udp"),
        }
    }
}

/// State of one `(id, protocol)` slot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SlotState {
    /// being validated, nothing bound yet
    Validating,
    /// bind in progress
    Binding,
    /// bound and serving
    Running,
    /// no longer accepting, existing connections still draining
    Draining,
    /// stopped, nothing left
    Stopped,
    /// validation, bind or the serving task failed
    Failed(String),
}

/// What an operation did to one slot (R9).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlotAction {
    Unchanged,
    Created,
    Updated,
    Draining,
    Deleted,
    Failed,
}

/// Result of an operation on one slot.
#[derive(Debug, Clone)]
pub struct SlotOutcome {
    pub proto: Proto,
    pub action: SlotAction,
    pub error: Option<String>,
    /// For a failure: whether resubmitting the same desired state may succeed.
    ///
    /// A bind that lost a race with another process is worth retrying; a
    /// configuration realm cannot make sense of is not (R31). `None` when the
    /// operation did not fail.
    pub retryable: Option<bool>,
}

impl SlotOutcome {
    fn ok(proto: Proto, action: SlotAction) -> Self {
        Self {
            proto,
            action,
            error: None,
            retryable: None,
        }
    }

    /// A failure the caller may retry: binding, and anything else the operating
    /// system refused for a reason that can change on its own.
    fn failed(proto: Proto, error: impl Display) -> Self {
        Self {
            proto,
            action: SlotAction::Failed,
            error: Some(error.to_string()),
            retryable: Some(true),
        }
    }

    /// A failure that will fail again unchanged.
    fn rejected(proto: Proto, error: impl Display) -> Self {
        Self {
            proto,
            action: SlotAction::Failed,
            error: Some(error.to_string()),
            retryable: Some(false),
        }
    }
}

/// How long established connections may keep running after their generation
/// has been superseded or removed (R13, R15, R17).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DrainPolicy {
    /// `None` drains indefinitely: an update must not terminate established
    /// connections
    pub on_update: Option<Duration>,
    /// deadline after which a deleted endpoint's connections are force-closed
    pub on_delete: Option<Duration>,
}

impl Default for DrainPolicy {
    fn default() -> Self {
        Self {
            on_update: None,
            on_delete: Some(DEFAULT_DELETE_DRAIN),
        }
    }
}

/// Desired state of one endpoint, as the lifecycle sees it.
#[derive(Debug, Clone)]
pub struct EndpointSpec {
    pub endpoint: Endpoint,
    /// serve tcp for this rule
    pub tcp: bool,
    /// serve udp for this rule
    pub udp: bool,
    /// per-endpoint override of the drain deadlines
    pub drain: Option<DrainPolicy>,
}

impl EndpointSpec {
    /// Protocols this rule asks for, in a stable order.
    pub fn protos(&self) -> Vec<Proto> {
        let mut protos = Vec::with_capacity(2);
        if self.tcp {
            protos.push(Proto::Tcp);
        }
        if self.udp {
            protos.push(Proto::Udp);
        }
        protos
    }
}

/// Observable state of one draining cohort (R36).
#[derive(Debug, Clone)]
pub struct DrainingStatus {
    pub generation: Generation,
    pub connections: usize,
    pub age: Duration,
    pub draining_for: Option<Duration>,
}

/// Observable state of one `(id, protocol)` slot.
#[derive(Debug, Clone)]
pub struct SlotStatus {
    pub proto: Proto,
    pub state: SlotState,
    pub generation: Generation,
    pub laddr: Option<SocketAddr>,
    pub connections: usize,
    pub draining: Vec<DrainingStatus>,
}

/// Observable state of one endpoint.
#[derive(Debug, Clone)]
pub struct EndpointStatus {
    pub id: EndpointId,
    pub slots: Vec<SlotStatus>,
}

/// A running data plane of one slot.
struct Active {
    generation: Generation,
    laddr: SocketAddr,
    shutdown: CancellationToken,
    cohort: Cohort,
    task: JoinHandle<io::Result<()>>,
}

/// A superseded generation whose connections are still finishing.
struct Draining {
    generation: Generation,
    stat: CohortStat,
    task: JoinHandle<()>,
}

struct Slot {
    state: SlotState,
    /// generation of whatever is actually serving, not of the last attempt
    generation: Generation,
    endpoint: Endpoint,
    active: Option<Active>,
    draining: Vec<Draining>,
}

struct Entry {
    slots: BTreeMap<Proto, Slot>,
    drain: DrainPolicy,
}

/// Owns every endpoint's state machine.
#[derive(Default)]
pub struct EndpointManager {
    entries: BTreeMap<EndpointId, Entry>,
    policy: DrainPolicy,
}

impl EndpointManager {
    pub fn new() -> Self {
        Self::default()
    }

    /// Manager with a non-default drain policy, used when the deployment wants
    /// other deadlines than the contract's defaults.
    pub fn with_policy(policy: DrainPolicy) -> Self {
        Self {
            entries: BTreeMap::new(),
            policy,
        }
    }

    /// Check a desired endpoint without touching the process state (R3).
    pub fn validate(&self, spec: &EndpointSpec) -> Result<(), String> {
        if !spec.tcp && !spec.udp {
            return Err("endpoint enables neither tcp nor udp".into());
        }
        Ok(())
    }

    /// Ids currently known to the manager.
    pub fn ids(&self) -> Vec<EndpointId> {
        self.entries.keys().cloned().collect()
    }

    /// Whether an id is currently managed.
    pub fn contains(&self, id: &str) -> bool {
        self.entries.contains_key(id)
    }

    /// Endpoint currently serving under `id`, per protocol.
    pub fn active_endpoint(&self, id: &str, proto: Proto) -> Option<&Endpoint> {
        let slot = self.entries.get(id)?.slots.get(&proto)?;
        slot.active.as_ref().map(|_| &slot.endpoint)
    }

    /// Bring `id` to `spec` at `generation`.
    ///
    /// Each protocol is applied independently: one failing slot never stops the
    /// other, and a slot that fails keeps whatever was serving before (R9, R23).
    pub async fn apply(&mut self, id: EndpointId, generation: Generation, spec: EndpointSpec) -> Vec<SlotOutcome> {
        self.refresh();

        if let Err(e) = self.validate(&spec) {
            return spec
                .protos()
                .into_iter()
                .map(|proto| SlotOutcome::rejected(proto, e.clone()))
                .collect();
        }

        let drain = spec.drain.unwrap_or(self.policy);
        self.entries
            .entry(id.clone())
            .or_insert_with(|| Entry {
                slots: BTreeMap::new(),
                drain,
            })
            .drain = drain;

        let wanted = spec.protos();
        let mut outcomes = Vec::with_capacity(2);

        for proto in [Proto::Tcp, Proto::Udp] {
            if wanted.contains(&proto) {
                outcomes.push(self.apply_slot(&id, generation, &spec, proto, drain).await);
            } else if self.slot(&id, proto).is_some() {
                // the rule dropped this data plane: stop it like a delete
                outcomes.push(self.stop_slot(&id, proto, drain.on_delete).await);
            }
        }

        self.prune(&id);
        outcomes
    }

    /// Remove `id` entirely: listeners are released at once, established
    /// connections drain under the delete deadline (R15).
    pub async fn remove(&mut self, id: &str, _generation: Generation) -> Vec<SlotOutcome> {
        self.refresh();

        let Some(entry) = self.entries.get(id) else {
            return Vec::new();
        };

        let deadline = entry.drain.on_delete;
        let protos: Vec<Proto> = entry.slots.keys().copied().collect();

        let mut outcomes = Vec::with_capacity(protos.len());
        for proto in protos {
            outcomes.push(self.stop_slot(id, proto, deadline).await);
        }

        self.entries.remove(id);
        outcomes
    }

    /// Stop accepting on one slot without removing it (R3).
    ///
    /// The listener socket is released; the connections it accepted keep
    /// running until they end or the caller drains them.
    pub async fn stop_accept(&mut self, id: &str, proto: Proto) -> Option<SlotOutcome> {
        let drain = self.entries.get(id)?.drain;
        let slot = self.entries.get_mut(id)?.slots.get_mut(&proto)?;
        let active = slot.active.take()?;

        let generation = active.generation;
        let cohort = stop_active(active).await;
        slot.draining.push(spawn_drain(cohort, generation, drain.on_update));
        slot.state = SlotState::Draining;

        Some(SlotOutcome::ok(proto, SlotAction::Draining))
    }

    /// Current state of every endpoint (R11, R36).
    pub fn status(&mut self) -> Vec<EndpointStatus> {
        self.refresh();

        self.entries
            .iter()
            .map(|(id, entry)| EndpointStatus {
                id: id.clone(),
                slots: entry
                    .slots
                    .iter()
                    .map(|(proto, slot)| SlotStatus {
                        proto: *proto,
                        state: slot.state.clone(),
                        generation: slot.generation,
                        laddr: slot.active.as_ref().map(|a| a.laddr),
                        connections: slot.active.as_ref().map(|a| a.cohort.count()).unwrap_or(0),
                        draining: slot
                            .draining
                            .iter()
                            .map(|d| DrainingStatus {
                                generation: d.generation,
                                connections: d.stat.count(),
                                age: d.stat.age(),
                                draining_for: d.stat.draining_for(),
                            })
                            .collect(),
                    })
                    .collect(),
            })
            .collect()
    }

    /// Wait until every draining cohort of `id` has been released (R3).
    pub async fn wait_drained(&mut self, id: &str) {
        let Some(entry) = self.entries.get_mut(id) else {
            return;
        };

        for slot in entry.slots.values_mut() {
            for draining in slot.draining.drain(..) {
                let _ = draining.task.await;
            }
        }
    }

    fn slot(&self, id: &str, proto: Proto) -> Option<&Slot> {
        self.entries.get(id)?.slots.get(&proto)
    }

    /// Reconcile the recorded state with reality: a serving task that exited on
    /// its own means the slot is not running, whatever it claimed before (R10).
    fn refresh(&mut self) {
        for entry in self.entries.values_mut() {
            for slot in entry.slots.values_mut() {
                if let Some(active) = &slot.active {
                    if active.task.is_finished() {
                        slot.state = SlotState::Failed("serving task exited unexpectedly".into());
                        slot.active = None;
                    }
                }

                slot.draining.retain(|d| !d.task.is_finished());
            }
        }
    }

    /// Drop slots that hold nothing anymore, and ids that hold no slot.
    fn prune(&mut self, id: &str) {
        if let Some(entry) = self.entries.get_mut(id) {
            entry.slots.retain(|_, slot| {
                slot.active.is_some() || !slot.draining.is_empty() || slot.state != SlotState::Stopped
            });

            if entry.slots.is_empty() {
                self.entries.remove(id);
            }
        }
    }

    async fn apply_slot(
        &mut self,
        id: &str,
        generation: Generation,
        spec: &EndpointSpec,
        proto: Proto,
        drain: DrainPolicy,
    ) -> SlotOutcome {
        let existing = self
            .entries
            .get_mut(id)
            .and_then(|e| e.slots.get_mut(&proto))
            .and_then(|slot| slot.active.take().map(|active| (slot, active)));

        let Some((slot, active)) = existing else {
            // nothing serving: a plain start
            return match start(spec, proto, generation).await {
                Ok(active) => {
                    let laddr = active.laddr;
                    self.insert_slot(
                        id,
                        proto,
                        Slot {
                            state: SlotState::Running,
                            generation,
                            endpoint: spec.endpoint.clone(),
                            active: Some(active),
                            draining: Vec::new(),
                        },
                    );
                    log::info!(
                        "[lifecycle]{}/{} started on {} at generation {}",
                        id,
                        proto,
                        laddr,
                        generation
                    );
                    SlotOutcome::ok(proto, SlotAction::Created)
                }
                Err(e) => {
                    self.fail_slot(id, proto, generation, spec, &e);
                    SlotOutcome::failed(proto, e)
                }
            };
        };

        let same_address =
            active.laddr.port() == spec.endpoint.laddr.port() && active.laddr.ip() == spec.endpoint.laddr.ip();
        let old_generation = active.generation;

        if same_address {
            // the address is occupied by ourselves: stop accepting and wait for
            // the socket to be released before rebinding it
            let cohort = stop_active(active).await;

            match start(spec, proto, generation).await {
                Ok(new_active) => {
                    let laddr = new_active.laddr;
                    slot.active = Some(new_active);
                    slot.state = SlotState::Running;
                    slot.generation = generation;
                    slot.endpoint = spec.endpoint.clone();
                    slot.draining.push(spawn_drain(cohort, old_generation, drain.on_update));
                    log::info!(
                        "[lifecycle]{}/{} replaced on {} at generation {}",
                        id,
                        proto,
                        laddr,
                        generation
                    );
                    SlotOutcome::ok(proto, SlotAction::Updated)
                }
                Err(e) => {
                    // best-effort restore of the generation that was serving
                    let restored = start_endpoint(&slot.endpoint.clone(), proto, old_generation).await;
                    match restored {
                        Ok(active) => {
                            slot.active = Some(active);
                            log::error!("[lifecycle]{}/{} bind failed, restored old listener: {}", id, proto, e);
                        }
                        Err(restore_err) => {
                            log::error!(
                                "[lifecycle]{}/{} bind failed and the old listener could not be restored: {} / {}",
                                id,
                                proto,
                                e,
                                restore_err
                            );
                        }
                    }
                    slot.state = SlotState::Failed(e.to_string());
                    slot.draining.push(spawn_drain(cohort, old_generation, drain.on_update));
                    SlotOutcome::failed(proto, e)
                }
            }
        } else {
            // moving to another address: bring the new one up first, so a
            // failure leaves the old address untouched (R27)
            match start(spec, proto, generation).await {
                Ok(new_active) => {
                    let laddr = new_active.laddr;
                    let cohort = stop_active(active).await;
                    slot.active = Some(new_active);
                    slot.state = SlotState::Running;
                    slot.generation = generation;
                    slot.endpoint = spec.endpoint.clone();
                    slot.draining.push(spawn_drain(cohort, old_generation, drain.on_update));
                    log::info!(
                        "[lifecycle]{}/{} moved to {} at generation {}",
                        id,
                        proto,
                        laddr,
                        generation
                    );
                    SlotOutcome::ok(proto, SlotAction::Updated)
                }
                Err(e) => {
                    // the old listener never stopped
                    slot.active = Some(active);
                    slot.state = SlotState::Failed(e.to_string());
                    log::error!(
                        "[lifecycle]{}/{} bind failed, keeping the old listener: {}",
                        id,
                        proto,
                        e
                    );
                    SlotOutcome::failed(proto, e)
                }
            }
        }
    }

    async fn stop_slot(&mut self, id: &str, proto: Proto, deadline: Option<Duration>) -> SlotOutcome {
        let Some(entry) = self.entries.get_mut(id) else {
            return SlotOutcome::ok(proto, SlotAction::Unchanged);
        };
        let Some(mut slot) = entry.slots.remove(&proto) else {
            return SlotOutcome::ok(proto, SlotAction::Unchanged);
        };

        if let Some(active) = slot.active.take() {
            let generation = active.generation;
            let cohort = stop_active(active).await;
            // the listener socket is gone at this point: the address is free
            slot.draining.push(spawn_drain(cohort, generation, deadline));
        }

        // detach whatever is still draining; it finishes on its own
        for draining in slot.draining {
            drop(draining);
        }

        log::info!("[lifecycle]{}/{} deleted", id, proto);
        SlotOutcome::ok(proto, SlotAction::Deleted)
    }

    fn insert_slot(&mut self, id: &str, proto: Proto, slot: Slot) {
        if let Some(entry) = self.entries.get_mut(id) {
            match entry.slots.get_mut(&proto) {
                Some(existing) => {
                    existing.state = slot.state;
                    existing.generation = slot.generation;
                    existing.endpoint = slot.endpoint;
                    existing.active = slot.active;
                    existing.draining.extend(slot.draining);
                }
                None => {
                    entry.slots.insert(proto, slot);
                }
            }
        }
    }

    fn fail_slot(&mut self, id: &str, proto: Proto, generation: Generation, spec: &EndpointSpec, error: &io::Error) {
        log::error!(
            "[lifecycle]{}/{} failed at generation {}: {}",
            id,
            proto,
            generation,
            error
        );
        self.insert_slot(
            id,
            proto,
            Slot {
                state: SlotState::Failed(error.to_string()),
                generation,
                endpoint: spec.endpoint.clone(),
                active: None,
                draining: Vec::new(),
            },
        );
    }
}

/// Bind and start serving one protocol of an endpoint.
///
/// Returns only once the socket is bound, so a caller that gets `Ok` may
/// report the slot as running without lying (R10).
async fn start(spec: &EndpointSpec, proto: Proto, generation: Generation) -> io::Result<Active> {
    start_endpoint(&spec.endpoint, proto, generation).await
}

async fn start_endpoint(endpoint: &Endpoint, proto: Proto, generation: Generation) -> io::Result<Active> {
    let shutdown = CancellationToken::new();
    let cohort = Cohort::new();

    // keep the io error kind: the control plane classifies `AddrInUse` as
    // retryable and a bad address as terminal (R31)
    let context = |e: io::Error| io::Error::new(e.kind(), format!("failed to bind {}: {}", endpoint.laddr, e));

    let (laddr, task): (SocketAddr, JoinHandle<io::Result<()>>) = match proto {
        Proto::Tcp => {
            let lis = bind_tcp(&endpoint.laddr, endpoint.bind_opts.clone()).map_err(context)?;
            let laddr = lis.local_addr().unwrap_or(endpoint.laddr);
            let runtime = Arc::new(endpoint.tcp_runtime());
            (
                laddr,
                tokio::spawn(serve_tcp(lis, runtime, cohort.handle(), shutdown.clone())),
            )
        }
        Proto::Udp => {
            let lis = bind_udp(&endpoint.laddr, endpoint.bind_opts.clone()).map_err(context)?;
            let laddr = lis.local_addr().unwrap_or(endpoint.laddr);
            let runtime = Arc::new(endpoint.udp_runtime());
            (
                laddr,
                tokio::spawn(serve_udp(lis, runtime, cohort.handle(), shutdown.clone())),
            )
        }
    };

    Ok(Active {
        generation,
        laddr,
        shutdown,
        cohort,
        task,
    })
}

/// Stop accepting and wait for the serving task to finish, which is what
/// guarantees the listener socket has been released.
async fn stop_active(active: Active) -> Cohort {
    let Active {
        shutdown, cohort, task, ..
    } = active;

    shutdown.cancel();
    if let Err(e) = task.await {
        log::warn!("[lifecycle]serving task ended abnormally: {}", e);
    }

    cohort
}

/// Hand a superseded cohort to a background drain.
fn spawn_drain(mut cohort: Cohort, generation: Generation, deadline: Option<Duration>) -> Draining {
    cohort.start_draining();
    let stat = cohort.stat();

    let task = tokio::spawn(async move {
        match cohort.drain(deadline).await {
            super::DrainOutcome::Finished => {
                log::debug!("[lifecycle]generation {} drained", generation);
            }
            super::DrainOutcome::Forced(n) => {
                log::info!(
                    "[lifecycle]generation {} drain deadline expired, closed {}",
                    generation,
                    n
                );
            }
        }
    });

    Draining { generation, stat, task }
}
