//! UDP relay entrance.

mod socket;
mod sockmap;
mod middle;
mod batched;

use std::io::Result;
use std::net::SocketAddr;
use std::sync::Arc;

use tokio::net::UdpSocket;

use crate::endpoint::{BindOpts, Endpoint, UdpRuntime};
use crate::lifecycle::{CancellationToken, Cohort, CohortHandle};

use sockmap::SockMap;
use middle::{associate_and_relay, Registry};

/// Bind a udp socket, reporting failures instead of aborting.
pub fn bind_udp(laddr: &SocketAddr, bind_opts: BindOpts) -> Result<UdpSocket> {
    socket::bind(laddr, bind_opts)
}

/// Launch a udp relay.
///
/// Kept for the static mode and for callers that do not manage the endpoint
/// lifecycle themselves.
pub async fn run_udp(endpoint: Endpoint) -> Result<()> {
    let runtime = Arc::new(endpoint.udp_runtime());

    let lis = match bind_udp(&endpoint.laddr, endpoint.bind_opts) {
        Ok(x) => x,
        Err(e) => {
            log::error!("[udp]failed to bind {}: {}", endpoint.laddr, e);
            return Err(e);
        }
    };

    let cohort = Cohort::new();
    serve_udp(lis, runtime, cohort.handle(), CancellationToken::new()).await
}

/// Serve an already bound udp socket until `shutdown` fires.
///
/// Associations outlive this loop, so each one owns its sockets, its runtime
/// configuration and its own cancellation token. Stopping the loop takes the
/// association map down with it: no later datagram may reuse an association of
/// the generation that is going away, while the running ones are terminated
/// through their cohort (R16: udp associations are rebuilt, not drained).
pub async fn serve_udp(
    lis: UdpSocket,
    runtime: Arc<UdpRuntime>,
    cohort: CohortHandle,
    shutdown: CancellationToken,
) -> Result<()> {
    let lis = Arc::new(lis);
    let sockmap = Arc::new(SockMap::new());

    // Allocated once per socket and reused across batches: the entry receive
    // buffer (~205 KiB) must not be reallocated on every wakeup of the loop.
    let mut registry = Registry::new(batched::MAX_PACKETS);

    loop {
        let relayed = tokio::select! {
            biased;
            _ = shutdown.cancelled() => {
                log::debug!("[udp]stop receiving on {:?}", lis.local_addr());
                break;
            }
            res = associate_and_relay(&lis, &runtime, &sockmap, &cohort, &mut registry) => res,
        };

        if let Err(e) = relayed {
            log::error!("[udp]error: {}", e);
        }
    }

    // controlled teardown: drop the association map so that nothing is reused.
    // The association tasks exit through their cohort token or their idle
    // timeout, and the cohort is what confirms they are actually gone.
    sockmap.clear();

    Ok(())
}
