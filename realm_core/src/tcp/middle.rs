use std::io::Result;
use std::sync::Arc;

use tokio::net::TcpStream;

use super::socket;
use super::plain;

#[cfg(feature = "hook")]
use super::hook;

#[cfg(feature = "proxy")]
use super::proxy;

#[cfg(feature = "transport")]
use super::transport;

use crate::endpoint::{ConnectOpts, TcpRuntime};

/// Connect to the remote peer and relay, using configuration this connection
/// owns: the `Arc` keeps the generation it was accepted under alive for as
/// long as the connection runs, independently of the listener.
#[allow(unused)]
pub async fn connect_and_relay(mut local: TcpStream, runtime: Arc<TcpRuntime>) -> Result<()> {
    let TcpRuntime {
        raddr,
        conn_opts,
        extra_raddrs,
    } = runtime.as_ref();

    let ConnectOpts {
        #[cfg(feature = "proxy")]
        proxy_opts,

        #[cfg(feature = "transport")]
        transport,

        #[cfg(feature = "balance")]
        balancer,

        tcp_keepalive,
        ..
    } = conn_opts;

    // before connect:
    // - pre-connect hook
    // - load balance
    // ..
    let raddr = {
        #[cfg(feature = "hook")]
        {
            // accept or deny connection.
            #[cfg(feature = "balance")]
            {
                hook::pre_connect_hook(&mut local, raddr, extra_raddrs).await?;
            }

            // accept or deny connection, or select a remote peer.
            #[cfg(not(feature = "balance"))]
            {
                hook::pre_connect_hook(&mut local, raddr, extra_raddrs).await?
            }
        }

        #[cfg(feature = "balance")]
        {
            use realm_lb::{Token, BalanceCtx};
            let token = balancer.next(BalanceCtx {
                src_ip: &local.peer_addr()?.ip(),
            });
            log::debug!("[tcp]select remote peer, token: {:?}", token);
            match token {
                None | Some(Token(0)) => raddr,
                Some(Token(idx)) => extra_raddrs
                    .get(idx as usize - 1)
                    .ok_or_else(|| std::io::Error::other("balancer selected an unknown remote"))?,
            }
        }

        #[cfg(not(any(feature = "hook", feature = "balance")))]
        raddr
    };

    // connect!
    let mut remote = socket::connect(raddr, conn_opts).await?;
    log::info!("[tcp]{} => {} as {}", local.peer_addr()?, raddr, remote.peer_addr()?);

    // after connected
    // ..
    #[cfg(feature = "proxy")]
    if proxy_opts.enabled() {
        proxy::handle_proxy(&mut local, &mut remote, *proxy_opts).await?;
    }

    // relay
    let res = {
        #[cfg(feature = "transport")]
        {
            if let Some((ac, cc)) = transport {
                transport::run_relay(local, remote, ac, cc).await
            } else {
                plain::run_relay(local, remote).await
            }
        }
        #[cfg(not(feature = "transport"))]
        {
            plain::run_relay(local, remote).await
        }
    };

    // ignore relay error
    if let Err(e) = res {
        log::debug!("[tcp]forward error: {}, ignored", e);
    }

    Ok(())
}
