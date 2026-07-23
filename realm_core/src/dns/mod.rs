//! Global dns resolver.
//!
//! The resolver is a process-wide singleton frozen at the first use: it is set
//! up once during startup and never changes afterwards (per-endpoint resolver
//! configuration is a deliberate product limit, not an oversight). Both the
//! configuration and the resolver live in `OnceLock`s, so a second attempt to
//! configure it reports an error instead of aborting the process.

use std::io::{Result, Error};
use std::net::SocketAddr;
use std::sync::OnceLock;

use hickory_resolver as resolver;
use resolver::TokioResolver;
use resolver::system_conf::read_system_conf;
use resolver::lookup_ip::{LookupIp, LookupIpIter};
use resolver::config::{ResolverOpts, ResolverConfig};

use crate::endpoint::RemoteAddr;

pub mod config {
    use super::resolver;
    pub use resolver::config::*;
}

/// Dns config.
#[derive(Debug, Clone)]
pub struct DnsConf {
    pub conf: ResolverConfig,
    pub opts: ResolverOpts,
}

/// Use system config on unix(except android) or windows,
/// otherwise use google's public dns servers.
impl Default for DnsConf {
    fn default() -> Self {
        #[cfg(any(all(unix, not(target_os = "android")), windows))]
        let (conf, opts) = read_system_conf().unwrap_or_default();

        #[cfg(not(any(all(unix, not(target_os = "android")), windows)))]
        let (conf, opts) = (ResolverConfig::udp_and_tcp(&config::GOOGLE), Default::default());

        Self { conf, opts }
    }
}

static DNS_CONF: OnceLock<DnsConf> = OnceLock::new();

/// `None` records that the resolver could not be built with the effective
/// configuration; lookups then fail instead of taking the process down.
static DNS: OnceLock<Option<TokioResolver>> = OnceLock::new();

/// Get the global dns resolver, building it on first use.
///
/// Building freezes the effective configuration: either the one installed by
/// [`build_lazy`], or the system defaults when nothing was installed.
fn resolver() -> Result<&'static TokioResolver> {
    DNS.get_or_init(|| {
        use resolver::net::runtime::TokioRuntimeProvider as Tokio;

        let DnsConf { conf, opts } = DNS_CONF.get_or_init(DnsConf::default).clone();

        match TokioResolver::builder_with_config(conf, Tokio::default())
            .with_options(opts)
            .build()
        {
            Ok(x) => Some(x),
            Err(e) => {
                log::error!("[dns]failed to build resolver: {}", e);
                None
            }
        }
    })
    .as_ref()
    .ok_or_else(|| Error::other("dns resolver is unavailable"))
}

/// Force initialization, freezing the effective configuration.
pub fn force_init() {
    let _ = resolver();
}

/// Setup global dns resolver.
///
/// Returns an error if the resolver has already been configured or initialized.
pub fn build(conf: Option<ResolverConfig>, opts: Option<ResolverOpts>) -> Result<()> {
    build_lazy(conf, opts)?;
    force_init();
    Ok(())
}

/// Setup config of global dns resolver, without initialization.
///
/// Returns an error if the resolver has already been configured or initialized.
pub fn build_lazy(conf: Option<ResolverConfig>, opts: Option<ResolverOpts>) -> Result<()> {
    let mut dns_conf = DnsConf::default();

    if let Some(conf) = conf {
        dns_conf.conf = conf;
    }

    if let Some(opts) = opts {
        dns_conf.opts = opts;
    }

    DNS_CONF
        .set(dns_conf)
        .map_err(|_| Error::other("dns resolver is already configured"))
}

/// Effective dns configuration, once it has been frozen.
///
/// `None` means nothing has been installed and the resolver has not been used
/// yet. Exposed so that the control plane can report the process-wide settings
/// an agent cannot change at runtime.
pub fn effective_conf() -> Option<&'static DnsConf> {
    DNS_CONF.get()
}

/// Lookup ip with global dns resolver.
pub async fn resolve_ip(ip: &str) -> Result<LookupIp> {
    resolver()?.lookup_ip(ip).await.map_err(Error::other)
}

/// Lookup socketaddr with global dns resolver.
pub async fn resolve_addr(addr: &RemoteAddr) -> Result<LookupRemoteAddr<'_>> {
    use RemoteAddr::*;
    use LookupRemoteAddr::*;
    match addr {
        SocketAddr(addr) => Ok(NoLookup(addr)),
        DomainName(ip, port) => resolve_ip(ip).await.map(|ip| Dolookup(ip, *port)),
    }
}

/// Resolved result.
pub enum LookupRemoteAddr<'a> {
    NoLookup(&'a SocketAddr),
    Dolookup(LookupIp, u16),
}

impl LookupRemoteAddr<'_> {
    /// Get view of resolved result.
    pub fn iter(&self) -> LookupRemoteAddrIter<'_> {
        use LookupRemoteAddr::*;
        match self {
            NoLookup(addr) => LookupRemoteAddrIter::NoLookup(std::iter::once(addr)),
            Dolookup(ip, port) => LookupRemoteAddrIter::DoLookup(ip.iter(), *port),
        }
    }
}

/// View of resolved result.
pub enum LookupRemoteAddrIter<'a> {
    NoLookup(std::iter::Once<&'a SocketAddr>),
    DoLookup(LookupIpIter<'a>, u16),
}

impl Iterator for LookupRemoteAddrIter<'_> {
    type Item = SocketAddr;

    fn next(&mut self) -> Option<Self::Item> {
        use LookupRemoteAddrIter::*;
        match self {
            NoLookup(addr) => addr.next().copied(),
            DoLookup(ip, port) => ip.next().map(|ip| SocketAddr::new(ip, *port)),
        }
    }
}
