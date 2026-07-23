//! Unix-socket control plane.
//!
//! An adapter over the endpoint lifecycle, not a second source of truth: it
//! translates HTTP/1.1 + JSON requests into reconciler calls and reports what
//! the lifecycle says. Everything that decides *what happens* lives in
//! `realm_core::lifecycle`.

mod api;
mod server;

pub use api::{ApiState, CAPABILITIES, MAX_BODY_BYTES, SCHEMA_VERSION};
pub use server::{ControlServer, bind};
