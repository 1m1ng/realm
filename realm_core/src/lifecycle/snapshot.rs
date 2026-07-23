//! Last-known-good snapshot.
//!
//! The backend remains the single source of truth for the desired state; this
//! snapshot only records what realm itself was serving, so that a restarted
//! process resumes forwarding immediately instead of waiting for the agent's
//! next reconcile (R18, R19). It lives in realm's own runtime directory and
//! never touches the configuration files somebody else manages.
//!
//! Writes are atomic: a temporary file in the same directory is flushed and
//! then renamed over the snapshot, so a crash mid-write leaves the previous
//! complete snapshot in place (R20). Ordering comes from the reconciler being
//! single-flight — only the serial consumer ever writes.

use std::collections::BTreeMap;
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use serde::de::DeserializeOwned;

use super::manager::{EndpointId, Generation};

/// Format version of the snapshot file, so a future change can be detected
/// instead of misread.
pub const SNAPSHOT_VERSION: u32 = 1;

/// What realm was serving when the snapshot was written.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Snapshot<S> {
    /// generation the desired state belongs to
    pub generation: Generation,
    /// whether that generation was only partially applied (R34)
    #[serde(default)]
    pub partial: bool,
    /// desired endpoints, by caller-provided id
    pub endpoints: BTreeMap<EndpointId, S>,
}

#[derive(Deserialize)]
struct Envelope<S> {
    version: u32,
    #[serde(flatten)]
    snapshot: Snapshot<S>,
}

#[derive(Serialize)]
struct EnvelopeRef<'a, S> {
    version: u32,
    #[serde(flatten)]
    snapshot: &'a Snapshot<S>,
}

/// Reads and writes the snapshot file.
#[derive(Debug, Clone)]
pub struct SnapshotStore {
    path: PathBuf,
}

impl SnapshotStore {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Load the snapshot, if there is one.
    ///
    /// `Ok(None)` means there is nothing to restore; an unreadable or corrupt
    /// snapshot is an error, never a silent empty state — restoring nothing
    /// when something was expected would drop every endpoint.
    pub fn load<S: DeserializeOwned>(&self) -> io::Result<Option<Snapshot<S>>> {
        let data = match fs::read(&self.path) {
            Ok(x) => x,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(e) => {
                return Err(io::Error::new(
                    e.kind(),
                    format!("failed to read {:?}: {}", self.path, e),
                ))
            }
        };

        let envelope: Envelope<S> = serde_json::from_slice(&data)
            .map_err(|e| io::Error::other(format!("failed to parse {:?}: {}", self.path, e)))?;

        if envelope.version != SNAPSHOT_VERSION {
            return Err(io::Error::other(format!(
                "unsupported snapshot version {} in {:?}, expected {}",
                envelope.version, self.path, SNAPSHOT_VERSION
            )));
        }

        Ok(Some(envelope.snapshot))
    }

    /// Replace the snapshot atomically.
    pub fn store<S: Serialize>(&self, snapshot: &Snapshot<S>) -> io::Result<()> {
        let dir = self.path.parent().unwrap_or_else(|| Path::new("."));
        fs::create_dir_all(dir)?;

        let data = serde_json::to_vec_pretty(&EnvelopeRef {
            version: SNAPSHOT_VERSION,
            snapshot,
        })
        .map_err(|e| io::Error::other(format!("failed to serialize snapshot: {}", e)))?;

        // same directory, so the rename stays within one filesystem
        let tmp = self.path.with_extension("json.tmp");
        {
            let mut file = fs::File::create(&tmp)?;
            file.write_all(&data)?;
            file.sync_all()?;
        }

        fs::rename(&tmp, &self.path)?;

        // make the rename itself durable
        if let Ok(dir) = fs::File::open(dir) {
            let _ = dir.sync_all();
        }

        Ok(())
    }
}

/// Result of restoring a snapshot at startup.
#[derive(Debug, Clone, Default)]
pub struct RestoreOutcome {
    /// generation the restored state belongs to
    pub generation: Option<Generation>,
    /// endpoints that came back
    pub restored: usize,
    /// endpoints that could not, with the reason
    pub failed: Vec<(EndpointId, String)>,
    /// whether the restored generation is partial (R34)
    pub partial: bool,
}
