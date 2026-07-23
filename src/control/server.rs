//! Unix domain socket server.
//!
//! The control plane is reachable from this host only: a unix socket with
//! 0700 permissions on both the socket and its parent directory, never a TCP
//! port, so reachability is a filesystem question rather than a network one
//! (R12). The socket's lifecycle is managed: a leftover socket from a crashed
//! process is probed and, if dead, replaced (R30).

use std::fs;
use std::io;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};
use std::time::Duration;

use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper_util::rt::{TokioIo, TokioTimer};
use tokio::net::{UnixListener, UnixStream};

use realm_core::lifecycle::{CancellationToken, ReconcileHandle};

use crate::conf::{EndpointConf, NetConf};

use super::api::{ApiState, handle};

/// Permissions of the socket and of a directory realm creates for it (R30).
const OWNER_ONLY: u32 = 0o700;

/// A connection that has not finished sending its request headers within this
/// window is closed, so a client that connects and then stalls cannot pin a
/// control-plane task open indefinitely (a residual DoS). The control socket is
/// host-local, so anything this slow to send a few header bytes is broken; the
/// bound is far tighter than hyper's WAN-oriented 30s default for that reason.
const HEADER_READ_TIMEOUT: Duration = Duration::from_secs(5);

/// Make sure the directory holding the socket exists and is owner-only.
///
/// Only a directory realm creates itself is tightened: an existing one may be
/// shared (`/run`, `/tmp`), and silently taking it away from everybody else
/// would be far worse than the exposure it is meant to prevent. An existing
/// directory that others can reach is reported instead — the socket itself is
/// still owner-only, so this is about closing the window between `bind` and
/// `chmod`, not about the socket's own permissions.
fn prepare_directory(dir: &Path) -> io::Result<()> {
    if dir.is_dir() {
        let mode = fs::metadata(dir)?.permissions().mode() & 0o777;
        if mode & 0o077 != 0 {
            log::warn!(
                "[control]{:?} is reachable by other users (mode {:o}); \
                 prefer a directory only this process's user can enter",
                dir,
                mode
            );
        }
        return Ok(());
    }

    fs::create_dir_all(dir)?;
    fs::set_permissions(dir, fs::Permissions::from_mode(OWNER_ONLY))
}

/// Bind the control socket, cleaning up after a crashed predecessor.
///
/// A leftover socket file is probed first: if something answers, another realm
/// owns this path and binding fails with a clear message; if nothing does, the
/// stale file is removed and the socket rebound.
pub async fn bind(path: &Path) -> io::Result<UnixListener> {
    if let Some(dir) = path.parent() {
        if !dir.as_os_str().is_empty() {
            prepare_directory(dir)?;
        }
    }

    if fs::symlink_metadata(path).is_ok() {
        match UnixStream::connect(path).await {
            Ok(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::AddrInUse,
                    format!("another process is already serving the control socket at {:?}", path),
                ));
            }
            Err(_) => {
                log::warn!("[control]removing the stale control socket at {:?}", path);
                fs::remove_file(path)?;
            }
        }
    }

    // Create the socket owner-only from the outset. Its mode at creation is
    // `0o777 & ~umask`, and a daemonized realm runs under `umask(0)`, so without
    // this guard the socket is world-connectable in the window between `bind`
    // and any later chmod. Force a `0o077` umask across the bind and restore the
    // previous one immediately, so nothing else observes the tightened value.
    let previous_umask = unsafe { libc::umask(0o077) };
    let bound = UnixListener::bind(path);
    unsafe { libc::umask(previous_umask) };
    let listener = bound?;

    // Apply the final mode on the listener's own file descriptor rather than by
    // path: `fchmod` cannot be redirected through a symlink at `path` the way a
    // path-based `set_permissions` can, and it needs no second lookup.
    if unsafe { libc::fchmod(listener.as_raw_fd(), OWNER_ONLY as libc::mode_t) } != 0 {
        return Err(io::Error::last_os_error());
    }

    log::info!("[control]listening on {:?}", path);
    Ok(listener)
}

/// The control plane.
pub struct ControlServer {
    state: ApiState,
    path: PathBuf,
}

impl ControlServer {
    pub fn new(reconciler: ReconcileHandle<EndpointConf>, global: NetConf, path: impl Into<PathBuf>) -> Self {
        Self {
            state: ApiState { reconciler, global },
            path: path.into(),
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Bind the socket, so that a failure is reported before the process
    /// claims to be serving.
    pub async fn bind(&self) -> io::Result<UnixListener> {
        bind(&self.path).await
    }

    /// Serve until `shutdown` fires, on an already bound listener.
    pub async fn serve(self, listener: UnixListener, shutdown: CancellationToken) {
        // Accept errors must not spin the CPU. A transient (`ConnectionAborted`)
        // retries immediately; a persistent error — fd exhaustion is the usual
        // one — backs off exponentially up to a cap and resets on the next
        // success, so the control plane recovers without busy-looping. Mirrors
        // `realm_core::tcp::serve_tcp`, except the control plane is long-lived
        // and never stops accepting.
        const BACKOFF_MIN: Duration = Duration::from_millis(5);
        const BACKOFF_MAX: Duration = Duration::from_millis(500);
        let mut backoff = BACKOFF_MIN;

        loop {
            let accepted = tokio::select! {
                biased;
                _ = shutdown.cancelled() => break,
                res = listener.accept() => res,
            };

            let (stream, _) = match accepted {
                Ok(x) => {
                    backoff = BACKOFF_MIN;
                    x
                }
                Err(e) if e.kind() == io::ErrorKind::ConnectionAborted => {
                    log::warn!("[control]failed to accept: {}", e);
                    continue;
                }
                Err(e) => {
                    log::error!("[control]failed to accept: {} (retrying in {:?})", e, backoff);
                    tokio::select! {
                        biased;
                        _ = shutdown.cancelled() => break,
                        _ = tokio::time::sleep(backoff) => {}
                    }
                    backoff = (backoff * 2).min(BACKOFF_MAX);
                    continue;
                }
            };

            let state = self.state.clone();
            let connection_shutdown = shutdown.clone();

            tokio::spawn(async move {
                let io = TokioIo::new(stream);
                let service = service_fn(move |req| handle(state.clone(), req));

                let connection = http1::Builder::new()
                    .timer(TokioTimer::new())
                    .header_read_timeout(HEADER_READ_TIMEOUT)
                    .serve_connection(io, service);
                tokio::pin!(connection);

                tokio::select! {
                    res = connection.as_mut() => {
                        if let Err(e) = res {
                            log::debug!("[control]connection error: {}", e);
                        }
                    }
                    _ = connection_shutdown.cancelled() => {
                        connection.as_mut().graceful_shutdown();
                        let _ = connection.await;
                    }
                }
            });
        }

        // the socket file outlives the listener otherwise, and would then look
        // like a live control plane to the next process
        let _ = fs::remove_file(&self.path);
        log::info!("[control]stopped serving {:?}", self.path);
    }

    /// Bind and serve in one step.
    pub async fn run(self, shutdown: CancellationToken) -> io::Result<()> {
        let listener = self.bind().await?;
        self.serve(listener, shutdown).await;
        Ok(())
    }
}
