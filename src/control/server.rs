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
use std::path::{Path, PathBuf};

use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use tokio::net::{UnixListener, UnixStream};

use realm_core::lifecycle::{CancellationToken, ReconcileHandle};

use crate::conf::{EndpointConf, NetConf};

use super::api::{ApiState, handle};

/// Permissions of the socket and of the directory holding it (R30).
const OWNER_ONLY: u32 = 0o700;

/// Bind the control socket, cleaning up after a crashed predecessor.
///
/// A leftover socket file is probed first: if something answers, another realm
/// owns this path and binding fails with a clear message; if nothing does, the
/// stale file is removed and the socket rebound.
pub async fn bind(path: &Path) -> io::Result<UnixListener> {
    if let Some(dir) = path.parent() {
        if !dir.as_os_str().is_empty() {
            fs::create_dir_all(dir)?;
            fs::set_permissions(dir, fs::Permissions::from_mode(OWNER_ONLY))?;
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

    let listener = UnixListener::bind(path)?;
    fs::set_permissions(path, fs::Permissions::from_mode(OWNER_ONLY))?;

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
        loop {
            let accepted = tokio::select! {
                biased;
                _ = shutdown.cancelled() => break,
                res = listener.accept() => res,
            };

            let (stream, _) = match accepted {
                Ok(x) => x,
                Err(e) => {
                    log::error!("[control]failed to accept: {}", e);
                    continue;
                }
            };

            let state = self.state.clone();
            let connection_shutdown = shutdown.clone();

            tokio::spawn(async move {
                let io = TokioIo::new(stream);
                let service = service_fn(move |req| handle(state.clone(), req));

                let connection = http1::Builder::new().serve_connection(io, service);
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
