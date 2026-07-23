//! Process-wide settings, frozen at startup.
//!
//! Dns, logging, file-descriptor limits, pipe capacity, the pre-connect hook
//! and the tls provider are all installed once during startup and never change
//! afterwards — they are deliberately outside what a reconcile may touch (R35).
//!
//! They are recorded here so the control plane can report the values actually
//! in effect: an agent that believes a node should be running with a certain
//! limit can detect the drift instead of assuming.

use std::sync::RwLock;

/// What this process was started with.
#[derive(Debug, Clone, Default)]
pub struct ProcessSettings {
    pub log_level: Option<String>,
    pub log_output: Option<String>,
    /// soft and hard `RLIMIT_NOFILE`
    pub nofile: Option<(u64, u64)>,
    /// pipe capacity in pages, when it was set explicitly
    pub pipe_page: Option<usize>,
    /// path of the pre-connect hook library, when one was loaded
    pub pre_conn_hook: Option<String>,
}

impl ProcessSettings {
    const fn empty() -> Self {
        Self {
            log_level: None,
            log_output: None,
            nofile: None,
            pipe_page: None,
            pre_conn_hook: None,
        }
    }
}

/// Startup fills these in from two places — the argument parser knows the
/// system limits, the configuration knows the logging — so they are built up
/// rather than written in one go. Everything here happens before the runtime
/// starts; afterwards it is read-only in practice.
static SETTINGS: RwLock<ProcessSettings> = RwLock::new(ProcessSettings::empty());

/// Record part of the settings.
pub fn amend(f: impl FnOnce(&mut ProcessSettings)) {
    if let Ok(mut settings) = SETTINGS.write() {
        f(&mut settings);
    }
}

/// The settings in effect.
pub fn effective() -> ProcessSettings {
    SETTINGS.read().map(|s| s.clone()).unwrap_or_default()
}
