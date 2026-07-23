//! Endpoint lifecycle.
//!
//! Everything an endpoint needs in order to be started, replaced and stopped
//! without disturbing the rest of the process: owned per-generation runtime
//! configuration, cohort tracking for the connections each generation serves,
//! and (from the reconciler upwards) desired-state application.

pub mod cohort;

pub use cohort::{Cohort, CohortHandle, ConnGuard, DrainOutcome};

/// Re-exported so that users of the lifecycle API do not need to depend on
/// `tokio-util` directly.
pub use tokio_util::sync::CancellationToken;
