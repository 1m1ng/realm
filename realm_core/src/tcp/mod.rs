//! TCP relay entrance.

mod socket;
mod middle;
mod plain;

#[cfg(feature = "hook")]
mod hook;

#[cfg(feature = "proxy")]
mod proxy;

#[cfg(feature = "transport")]
mod transport;

use std::io::{ErrorKind, Result};
use std::net::SocketAddr;
use std::sync::Arc;

use tokio::net::TcpListener;

use crate::endpoint::{BindOpts, Endpoint, TcpRuntime};
use crate::lifecycle::{CancellationToken, Cohort, CohortHandle};

use middle::connect_and_relay;

pub use middle::connect_and_relay as relay_connection;

/// Bind a tcp listener, reporting failures instead of aborting.
pub fn bind_tcp(laddr: &SocketAddr, bind_opts: BindOpts) -> Result<TcpListener> {
    socket::bind(laddr, bind_opts)
}

/// Launch a tcp relay.
///
/// Kept for the static mode and for callers that do not manage the endpoint
/// lifecycle themselves; it binds, then serves until the listener fails.
pub async fn run_tcp(endpoint: Endpoint) -> Result<()> {
    let runtime = Arc::new(endpoint.tcp_runtime());

    let lis = match bind_tcp(&endpoint.laddr, endpoint.bind_opts) {
        Ok(x) => x,
        Err(e) => {
            log::error!("[tcp]failed to bind {}: {}", &endpoint.laddr, e);
            return Err(e);
        }
    };

    let cohort = Cohort::new();
    serve_tcp(lis, runtime, cohort.handle(), CancellationToken::new()).await
}

/// Serve an already bound listener until `shutdown` fires.
///
/// Every accepted connection captures an owned `Arc` of the runtime
/// configuration and registers itself in `cohort`, so that the caller can
/// replace the listener at any time while still tracking — and, when it
/// decides to, terminating — the connections of this generation.
pub async fn serve_tcp(
    lis: TcpListener,
    runtime: Arc<TcpRuntime>,
    cohort: CohortHandle,
    shutdown: CancellationToken,
) -> Result<()> {
    let keepalive = socket::keepalive::build(&runtime.conn_opts);

    loop {
        let accepted = tokio::select! {
            biased;
            _ = shutdown.cancelled() => {
                log::debug!("[tcp]stop accepting on {:?}", lis.local_addr());
                break;
            }
            res = lis.accept() => res,
        };

        let (local, addr) = match accepted {
            Ok(x) => x,
            Err(e) if e.kind() == ErrorKind::ConnectionAborted => {
                log::warn!("[tcp]failed to accept: {}", e);
                continue;
            }
            Err(e) => {
                log::error!("[tcp]failed to accept: {}", e);
                break;
            }
        };

        // ignore error
        let _ = local.set_nodelay(true);
        // set tcp_keepalive
        if let Some(kpa) = &keepalive {
            use socket::keepalive::SockRef;
            if let Err(e) = SockRef::from(&local).set_tcp_keepalive(kpa) {
                log::warn!("[tcp]failed to set keepalive for {}: {}", addr, e);
            }
        }

        let runtime = Arc::clone(&runtime);
        let guard = cohort.register();

        tokio::spawn(async move {
            let raddr = runtime.raddr.clone();
            tokio::select! {
                _ = guard.token().cancelled() => {
                    log::debug!("[tcp]{} => {}, cancelled", addr, &raddr);
                }
                res = connect_and_relay(local, Arc::clone(&runtime)) => match res {
                    Ok(..) => log::debug!("[tcp]{} => {}, finish", addr, &raddr),
                    Err(e) => log::error!("[tcp]{} => {}, error: {}", addr, &raddr, e),
                },
            }
            // the guard is released here, and only here: the cohort reports
            // this connection as gone once the task has actually finished
            drop(guard);
        });
    }

    Ok(())
}
