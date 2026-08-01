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
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

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
    /// Feeds the unique temp-file name so concurrent or repeated writes never
    /// collide on a fixed, guessable path. Shared across clones so
    /// two handles to the same store cannot pick the same sequence number.
    tmp_counter: Arc<AtomicU64>,
}

impl SnapshotStore {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            tmp_counter: Arc::new(AtomicU64::new(0)),
        }
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
        create_dir_owner_only(dir)?;

        let data = serde_json::to_vec_pretty(&EnvelopeRef {
            version: SNAPSHOT_VERSION,
            snapshot,
        })
        .map_err(|e| io::Error::other(format!("failed to serialize snapshot: {}", e)))?;

        // Write to a UNIQUE temp file in the same directory (so the rename
        // stays within one filesystem), opened with `create_new` => `O_EXCL`.
        // `O_EXCL` refuses to follow a symlink at the final component and fails
        // if the path already exists, so the write can never land on a
        // pre-existing file. A fixed, guessable temp name let a
        // local user pre-plant it as a symlink and have realm write straight
        // through it with realm's (typically root) privileges.
        let base_name = self.path.file_name().and_then(|n| n.to_str()).unwrap_or("snapshot");

        let (tmp, mut file) = loop {
            let seq = self.tmp_counter.fetch_add(1, Ordering::Relaxed);
            let tmp = dir.join(format!("{}.{}.{}.tmp", base_name, std::process::id(), seq));

            let mut options = fs::OpenOptions::new();
            options.write(true).create_new(true);

            // the snapshot spells out every forwarding rule, transport
            // parameters included: it is nobody else's business
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                options.mode(0o600);
            }

            match options.open(&tmp) {
                Ok(file) => break (tmp, file),
                // the unique name was taken (a concurrent write, a rerun, or a
                // squatter): pick the next one rather than touch what is there
                Err(e) if e.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(e) => return Err(e),
            }
        };

        // any failure after the temp file exists must not leave it behind
        let written = file.write_all(&data).and_then(|()| file.sync_all());
        drop(file);
        if let Err(e) = written {
            let _ = fs::remove_file(&tmp);
            return Err(e);
        }

        if let Err(e) = fs::rename(&tmp, &self.path) {
            let _ = fs::remove_file(&tmp);
            return Err(e);
        }

        // make the rename itself durable
        if let Ok(dir) = fs::File::open(dir) {
            let _ = dir.sync_all();
        }

        Ok(())
    }
}

/// Create the snapshot's directory owner-only, so its contents — which spell
/// out every forwarding rule — are never readable or writable by other users.
/// Under the daemon's `umask(0)` a plain `create_dir_all` would
/// make the first persist create it world-writable. An existing directory
/// keeps whatever mode it already has.
#[cfg(unix)]
fn create_dir_owner_only(dir: &Path) -> io::Result<()> {
    use std::os::unix::fs::DirBuilderExt;
    fs::DirBuilder::new().recursive(true).mode(0o700).create(dir)
}

#[cfg(not(unix))]
fn create_dir_owner_only(dir: &Path) -> io::Result<()> {
    fs::create_dir_all(dir)
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
