//! Endpoint lifecycle.
//!
//! Everything an endpoint needs in order to be started, replaced and stopped
//! without disturbing the rest of the process: owned per-generation runtime
//! configuration, cohort tracking for the connections each generation serves,
//! and (from the reconciler upwards) desired-state application.

pub mod cohort;
pub mod manager;
pub mod reconcile;

pub use cohort::{Cohort, CohortHandle, CohortStat, ConnGuard, DrainOutcome};
pub use manager::{
    DEFAULT_DELETE_DRAIN, DrainPolicy, DrainingStatus, EndpointId, EndpointManager, EndpointSpec, EndpointStatus,
    Generation, Proto, SlotAction, SlotOutcome, SlotState, SlotStatus,
};

pub use reconcile::{
    DesiredEndpoint, EndpointResult, EndpointSource, GenerationState, ReconcileError, ReconcileHandle,
    ReconcileRequest, ReconcileResponse, Reconciler, derive_id,
};

/// Re-exported so that users of the lifecycle API do not need to depend on
/// `tokio-util` directly.
pub use tokio_util::sync::CancellationToken;
