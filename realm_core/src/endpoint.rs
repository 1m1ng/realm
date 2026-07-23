//! Relay endpoint.

use std::fmt::{Display, Formatter};
use std::net::SocketAddr;

#[cfg(feature = "transport")]
use kaminari::mix::{MixAccept, MixConnect};

#[cfg(feature = "balance")]
use realm_lb::Balancer;

/// Remote address.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RemoteAddr {
    SocketAddr(SocketAddr),
    DomainName(String, u16),
}

/// Proxy protocol options.
#[cfg(feature = "proxy")]
#[derive(Debug, Default, Clone, Copy)]
pub struct ProxyOpts {
    pub send_proxy: bool,
    pub accept_proxy: bool,
    pub send_proxy_version: usize,
    pub accept_proxy_timeout: usize,
}

#[cfg(feature = "proxy")]
impl ProxyOpts {
    #[inline]
    pub(crate) const fn enabled(&self) -> bool {
        self.send_proxy || self.accept_proxy
    }
}

/// Connect or associate options.
#[derive(Debug, Default, Clone)]
pub struct ConnectOpts {
    pub send_mptcp: bool,
    pub connect_timeout: usize,
    pub associate_timeout: usize,
    pub tcp_keepalive: usize,
    pub tcp_keepalive_probe: usize,
    pub bind_address: Option<SocketAddr>,
    pub bind_interface: Option<String>,

    #[cfg(feature = "proxy")]
    pub proxy_opts: ProxyOpts,

    #[cfg(feature = "transport")]
    pub transport: Option<(MixAccept, MixConnect)>,

    #[cfg(feature = "balance")]
    pub balancer: Balancer,
}

#[derive(Debug, Default, Clone)]
pub struct BindOpts {
    pub ipv6_only: bool,
    pub accept_mptcp: bool,
    pub bind_interface: Option<String>,
}

/// Relay endpoint.
#[derive(Debug, Clone)]
pub struct Endpoint {
    pub laddr: SocketAddr,
    pub raddr: RemoteAddr,
    pub bind_opts: BindOpts,
    pub conn_opts: ConnectOpts,
    pub extra_raddrs: Vec<RemoteAddr>,
}

/// Immutable per-generation runtime configuration of a tcp data plane.
///
/// A connection captures an `Arc` of this at accept time and keeps using it
/// until it ends, so later changes to the endpoint cannot affect a connection
/// that is already running.
#[derive(Debug, Clone)]
pub struct TcpRuntime {
    pub raddr: RemoteAddr,
    pub conn_opts: ConnectOpts,
    pub extra_raddrs: Vec<RemoteAddr>,
}

/// Immutable per-generation runtime configuration of a udp data plane.
#[derive(Debug, Clone)]
pub struct UdpRuntime {
    pub raddr: RemoteAddr,
    pub conn_opts: ConnectOpts,
}

impl Endpoint {
    /// Split off the parts a tcp data plane needs at runtime.
    pub fn tcp_runtime(&self) -> TcpRuntime {
        TcpRuntime {
            raddr: self.raddr.clone(),
            conn_opts: self.conn_opts.clone(),
            extra_raddrs: self.extra_raddrs.clone(),
        }
    }

    /// Split off the parts a udp data plane needs at runtime.
    pub fn udp_runtime(&self) -> UdpRuntime {
        UdpRuntime {
            raddr: self.raddr.clone(),
            conn_opts: self.conn_opts.clone(),
        }
    }
}

// display impl below

impl Display for RemoteAddr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        use RemoteAddr::*;
        match self {
            SocketAddr(addr) => write!(f, "{}", addr),
            DomainName(host, port) => write!(f, "{}:{}", host, port),
        }
    }
}

impl Display for Endpoint {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} -> [{}", &self.laddr, &self.raddr)?;
        for raddr in self.extra_raddrs.iter() {
            write!(f, "|{}", raddr)?;
        }
        write!(f, "]; options: {}; {}", &self.bind_opts, &self.conn_opts)
    }
}

impl Display for BindOpts {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let BindOpts {
            accept_mptcp,
            ipv6_only,
            bind_interface,
        } = self;
        if let Some(iface) = bind_interface {
            write!(f, "listen-iface={}, ", iface)?;
        }
        write!(f, "ipv6-only={}, ", ipv6_only)?;
        write!(f, "accept-mptcp={}", accept_mptcp)?;
        Ok(())
    }
}

impl Display for ConnectOpts {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let ConnectOpts {
            send_mptcp,
            connect_timeout,
            associate_timeout,
            tcp_keepalive,
            tcp_keepalive_probe,
            bind_address,
            bind_interface,

            #[cfg(feature = "proxy")]
            proxy_opts,

            #[cfg(feature = "transport")]
            transport,

            #[cfg(feature = "balance")]
            balancer,
        } = self;

        if let Some(iface) = bind_interface {
            write!(f, "send-iface={}, ", iface)?;
        }

        if let Some(send_through) = bind_address {
            write!(f, "send-through={}, ", send_through)?;
        }

        write!(f, "send-mptcp={}; ", send_mptcp)?;

        #[cfg(feature = "proxy")]
        {
            let ProxyOpts {
                send_proxy,
                accept_proxy,
                send_proxy_version,
                accept_proxy_timeout,
            } = proxy_opts;
            write!(
                f,
                "send-proxy={0}, send-proxy-version={2}, accept-proxy={1}, accept-proxy-timeout={3}s; ",
                send_proxy, accept_proxy, send_proxy_version, accept_proxy_timeout
            )?;
        }

        write!(
            f,
            "tcp-keepalive={}s[{}] connect-timeout={}s, associate-timeout={}s; ",
            tcp_keepalive, tcp_keepalive_probe, connect_timeout, associate_timeout
        )?;

        #[cfg(feature = "transport")]
        if let Some((ac, cc)) = transport {
            write!(f, "transport={}||{}; ", ac, cc)?;
        }

        #[cfg(feature = "balance")]
        write!(f, "balance={}", balancer.strategy())?;
        Ok(())
    }
}
