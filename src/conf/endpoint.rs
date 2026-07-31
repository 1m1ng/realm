use serde::{Serialize, Deserialize};
use std::net::{IpAddr, SocketAddr, ToSocketAddrs};
use std::time::Duration;

use realm_core::endpoint::{Endpoint, RemoteAddr};
use realm_core::lifecycle::{DrainPolicy, EndpointSource, EndpointSpec};

#[cfg(feature = "balance")]
use realm_core::balance::Balancer;

#[cfg(feature = "transport")]
use realm_core::kaminari::mix::{MixAccept, MixConnect};

use super::{BuildError, Config, NetConf, NetInfo};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EndpointConf {
    pub listen: String,

    pub remote: String,

    #[serde(default)]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub extra_remotes: Vec<String>,

    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub balance: Option<String>,

    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub through: Option<String>,

    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub interface: Option<String>,

    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub listen_interface: Option<String>,

    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub listen_transport: Option<String>,

    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub remote_transport: Option<String>,

    /// seconds established connections may keep running after this endpoint
    /// was updated; absent means indefinitely, which is the contract's default
    /// (R13, R17)
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub update_drain_timeout: Option<u64>,

    /// seconds established connections may keep running after this endpoint
    /// was deleted; absent means the 30s default (R15)
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delete_drain_timeout: Option<u64>,

    #[serde(default)]
    #[serde(skip_serializing_if = "Config::is_empty")]
    pub network: NetConf,

    /// digest of the certificate material the transport options name, as of
    /// the last `refresh`; absent when they name none
    ///
    /// Not part of the configuration document, and deliberately so. The
    /// reconciler rebuilds an endpoint only when its desired state differs
    /// from the active generation's, and that state is this struct: replacing
    /// the *bytes* of a certificate file leaves every other field identical,
    /// so a rotation in place would be invisible and the endpoint would keep
    /// serving pre-rotation material while the control plane reported
    /// convergence. Folding the material into the value makes the rotation a
    /// difference the diff can see.
    ///
    /// `#[serde(skip)]` is what keeps that from breaking the other half of
    /// the contract: the submission hash and the replay decision go through
    /// serde, so a genuine retry of a byte-identical document is still a
    /// replay, and an agent neither sends this nor sees it.
    ///
    /// An endpoint that names no material keeps this absent rather than
    /// digesting emptiness, so it is identical whether it has been refreshed
    /// or not — a refresh that never ran, because the blocking pool refused
    /// it, leaves such an endpoint exactly where a refresh would have.
    #[serde(skip)]
    pub material_digest: Option<u64>,
}

impl EndpointConf {
    fn build_local(&self) -> Result<SocketAddr, BuildError> {
        self.listen
            .to_socket_addrs()
            .map_err(|e| BuildError::new("listen", &self.listen, format!("cannot be resolved: {}", e)))?
            .next()
            .ok_or_else(|| BuildError::new("listen", &self.listen, "resolved to no address"))
    }

    fn build_remote(&self) -> Result<RemoteAddr, BuildError> {
        Self::build_remote_x("remote", &self.remote)
    }

    fn build_remote_x(field: &'static str, remote: &str) -> Result<RemoteAddr, BuildError> {
        if let Ok(sockaddr) = remote.parse::<SocketAddr>() {
            return Ok(RemoteAddr::SocketAddr(sockaddr));
        }

        let Some((addr, port)) = remote.rsplit_once(':') else {
            return Err(BuildError::new(field, remote, "missing `:port` suffix"));
        };

        let port = port
            .parse::<u16>()
            .map_err(|e| BuildError::new(field, remote, format!("invalid port: {}", e)))?;

        if addr.is_empty() {
            return Err(BuildError::new(field, remote, "missing host part"));
        }

        Ok(RemoteAddr::DomainName(addr.to_string(), port))
    }

    fn build_send_through(&self) -> Result<Option<SocketAddr>, BuildError> {
        let Self { through, .. } = self;
        let Some(through) = through else {
            return Ok(None);
        };

        if let Ok(mut addrs) = through.to_socket_addrs() {
            if let Some(addr) = addrs.next() {
                return Ok(Some(addr));
            }
        }

        let mut ipstr = String::from(through);
        ipstr.retain(|c| c != '[' && c != ']');
        ipstr
            .parse::<IpAddr>()
            .map(|ip| Some(SocketAddr::new(ip, 0)))
            .map_err(|e| BuildError::new("through", through, format!("neither an address nor an ip: {}", e)))
    }

    #[cfg(feature = "balance")]
    fn build_balancer(&self) -> Result<Balancer, BuildError> {
        let Some(s) = &self.balance else {
            return Ok(Balancer::default());
        };

        // `Balancer::parse_from_str` panics without the strategy separator
        let Some((strategy, weights)) = s.split_once(':') else {
            return Err(BuildError::new("balance", s, "expected `strategy: weight, ...`"));
        };

        // `realm_lb::Strategy::from` panics on anything but these; validate the
        // token before `parse_from_str` reaches it, so a control-plane request
        // cannot panic the reconciler (finding #2)
        if !matches!(strategy.trim(), "off" | "iphash" | "roundrobin") {
            return Err(BuildError::new(
                "balance",
                s,
                format!(
                    "unknown strategy `{}` (expected off, iphash or roundrobin)",
                    strategy.trim()
                ),
            ));
        }

        let balancer = Balancer::parse_from_str(s);

        // a weight that failed to parse is silently dropped upstream: catch the
        // mismatch here so that it cannot silently change the remote selection
        let given = weights.trim();
        let given = if given.is_empty() {
            0
        } else {
            given.split(',').filter(|x| !x.trim().is_empty()).count()
        };
        if given != balancer.total() as usize {
            return Err(BuildError::new("balance", s, "contains an invalid weight"));
        }

        if given != 0 && given != self.extra_remotes.len() + 1 {
            return Err(BuildError::new(
                "balance",
                s,
                format!(
                    "expected {} weights for `remote` plus {} `extra_remotes`, got {}",
                    self.extra_remotes.len() + 1,
                    self.extra_remotes.len(),
                    given
                ),
            ));
        }

        Ok(balancer)
    }

    #[cfg(feature = "transport")]
    fn build_transport(&self) -> Result<Option<(MixAccept, MixConnect)>, BuildError> {
        use realm_core::kaminari::mix::{MixClientConf, MixServerConf};
        use realm_core::kaminari::opt::get_ws_conf;
        use realm_core::kaminari::opt::get_tls_client_conf;
        use realm_core::kaminari::opt::get_tls_server_conf;

        let Self {
            listen_transport,
            remote_transport,
            ..
        } = self;

        // kaminari's option parsers `panic!` on a malformed string (`get_ws_conf`
        // when `ws` is present but host/path are missing, `get_tls_*_conf` when
        // `tls` is present but sni/cert are missing). Since this runs on the
        // reconciler task for a control-plane request, a panic would take the
        // whole control plane down (finding #2) — so any panic is caught here
        // and turned into a structured error naming the field.
        fn guard<T>(field: &'static str, value: &Option<String>, f: impl FnOnce() -> T) -> Result<T, BuildError> {
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)).map_err(|e| {
                let reason = e
                    .downcast_ref::<&str>()
                    .map(|s| s.to_string())
                    .or_else(|| e.downcast_ref::<String>().cloned())
                    .unwrap_or_else(|| String::from("invalid transport options"));
                BuildError::new(field, value.as_deref().unwrap_or_default(), reason)
            })
        }

        let listen_ws = guard("listen_transport", listen_transport, || {
            listen_transport.as_ref().and_then(|s| get_ws_conf(s))
        })?;
        let listen_tls = guard("listen_transport", listen_transport, || {
            listen_transport.as_ref().and_then(|s| get_tls_server_conf(s))
        })?;

        let remote_ws = guard("remote_transport", remote_transport, || {
            remote_transport.as_ref().and_then(|s| get_ws_conf(s))
        })?;
        let remote_tls = guard("remote_transport", remote_transport, || {
            remote_transport.as_ref().and_then(|s| get_tls_client_conf(s))
        })?;

        if matches!(
            (&listen_ws, &listen_tls, &remote_ws, &remote_tls),
            (None, None, None, None)
        ) {
            Ok(None)
        } else {
            let ac = MixAccept::new_shared(MixServerConf {
                ws: listen_ws,
                tls: listen_tls,
            });
            let cc = MixConnect::new_shared(MixClientConf {
                ws: remote_ws,
                tls: remote_tls,
            });
            Ok(Some((ac, cc)))
        }
    }
}

#[derive(Debug)]
pub struct EndpointInfo {
    pub no_tcp: bool,
    pub use_udp: bool,
    pub endpoint: Endpoint,
}

impl Config for EndpointConf {
    type Output = Result<EndpointInfo, BuildError>;

    fn is_empty(&self) -> bool {
        false
    }

    fn build(self) -> Self::Output {
        let laddr = self.build_local()?;
        let raddr = self.build_remote()?;

        let extra_raddrs = self
            .extra_remotes
            .iter()
            .map(|r| Self::build_remote_x("extra_remotes", r))
            .collect::<Result<Vec<_>, _>>()?;

        let send_through = self.build_send_through()?;

        #[cfg(feature = "balance")]
        let balancer = self.build_balancer()?;

        // build partial conn_opts from netconf
        let NetInfo {
            mut bind_opts,
            mut conn_opts,
            no_tcp,
            use_udp,
        } = self.network.build();

        #[cfg(feature = "balance")]
        {
            conn_opts.balancer = balancer;
        }

        #[cfg(feature = "transport")]
        {
            conn_opts.transport = self.build_transport()?;
        }

        // build left fields of bind_opts and conn_opts
        conn_opts.bind_address = send_through;
        conn_opts.bind_interface = self.interface;
        bind_opts.bind_interface = self.listen_interface;

        Ok(EndpointInfo {
            no_tcp,
            use_udp,
            endpoint: Endpoint {
                laddr,
                raddr,
                bind_opts,
                conn_opts,
                extra_raddrs,
            },
        })
    }

    fn rst_field(&mut self, _: &Self) -> &mut Self {
        unreachable!()
    }

    fn take_field(&mut self, _: &Self) -> &mut Self {
        unreachable!()
    }

    fn from_cmd_args(matches: &clap::ArgMatches) -> Self {
        // both are guaranteed present by the caller (`cmd::handle_matches`)
        let listen = matches.get_one("local").cloned().unwrap_or_default();
        let remote = matches.get_one("remote").cloned().unwrap_or_default();
        let through = matches.get_one("through").cloned();
        let interface = matches.get_one("interface").cloned();
        let listen_interface = matches.get_one("listen_interface").cloned();
        let listen_transport = matches.get_one("listen_transport").cloned();
        let remote_transport = matches.get_one("remote_transport").cloned();

        EndpointConf {
            listen,
            remote,
            through,
            interface,
            listen_interface,
            listen_transport,
            remote_transport,
            network: Default::default(),
            extra_remotes: Vec::new(),
            balance: None,
            update_drain_timeout: None,
            delete_drain_timeout: None,
            material_digest: None,
        }
    }
}

impl EndpointConf {
    /// Canonical form used for diffing (KTD3).
    ///
    /// Fills in the process-wide network options the same way the static mode
    /// does, so that two descriptions meaning the same thing compare equal and
    /// an agent's first equivalent submission is `unchanged`, not a rebuild.
    pub fn normalized(&self, global: &NetConf) -> Self {
        let mut conf = self.clone();
        conf.network.take_field(global);
        conf
    }

    /// Largest piece of certificate material that is read for the digest.
    ///
    /// A certificate, a key, or a whole trust bundle is small. An operator who
    /// points `ca=` at something huge — by accident or otherwise — must not be
    /// able to make the reconciler read all of it on every submission, so
    /// anything past this cap is digested as unreadable instead.
    #[cfg(feature = "transport")]
    const MAX_MATERIAL_BYTES: u64 = 1024 * 1024;

    /// Digest of the certificate material this endpoint's transport options
    /// name: `cert` and `key` on the listen side, `ca` on the remote side.
    ///
    /// The options are read with kaminari's own `has_opt!`/`get_opt!`, so the
    /// digest's view of a transport string is exactly the view the constructor
    /// gets — a second parser here would drift from the one that decides what
    /// is actually loaded, and the digest would then track material the
    /// endpoint does not use. That includes the `tls` gate: without it the
    /// constructor never looks at these options, so neither does this.
    ///
    /// Each path is hashed together with its contents, and a path that cannot
    /// be read is marked as such rather than skipped: material appearing,
    /// disappearing, or moving to a different file is a change to the desired
    /// state just as much as a change to its bytes.
    ///
    /// `ocsp` is deliberately absent. A stapled response has its own lifetime
    /// and is refreshed far more often than the certificate it belongs to;
    /// including it would rebuild the endpoint on that cadence.
    #[cfg(feature = "transport")]
    fn digest_material(&self) -> Option<u64> {
        use std::hash::{Hash, Hasher};
        use realm_core::kaminari::opt::{get_opt, has_opt};

        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        let mut any = false;

        for (field, option, transport) in [
            ("listen_transport", "cert", &self.listen_transport),
            ("listen_transport", "key", &self.listen_transport),
            ("remote_transport", "ca", &self.remote_transport),
        ] {
            field.hash(&mut hasher);
            option.hash(&mut hasher);

            let tls = transport.as_deref().filter(|s| has_opt!(*s => "tls"));

            let Some(path) = tls.and_then(|s| get_opt!(s => option)) else {
                // the option is absent: no material, and that is itself a state
                MaterialTag::Absent.hash(&mut hasher);
                continue;
            };

            any = true;
            MaterialTag::Named.hash(&mut hasher);
            path.hash(&mut hasher);

            match Self::read_material(path) {
                Some(bytes) => {
                    MaterialTag::Read.hash(&mut hasher);
                    bytes.hash(&mut hasher);
                }
                None => MaterialTag::Unreadable.hash(&mut hasher),
            }
        }

        any.then(|| hasher.finish())
    }

    /// The contents of `path`, or `None` if it cannot be read or holds more
    /// than [`Self::MAX_MATERIAL_BYTES`].
    #[cfg(feature = "transport")]
    fn read_material(path: &str) -> Option<Vec<u8>> {
        use std::io::Read;

        let file = std::fs::File::open(path).ok()?;
        let mut bytes = Vec::new();
        // one byte past the cap, so a file that sits exactly on it is still
        // told apart from one that overruns it
        file.take(Self::MAX_MATERIAL_BYTES + 1).read_to_end(&mut bytes).ok()?;

        (bytes.len() as u64 <= Self::MAX_MATERIAL_BYTES).then_some(bytes)
    }

    /// Drain deadlines this endpoint overrides, if any (R13, R15).
    fn drain_policy(&self) -> Option<DrainPolicy> {
        if self.update_drain_timeout.is_none() && self.delete_drain_timeout.is_none() {
            return None;
        }

        let default = DrainPolicy::default();
        Some(DrainPolicy {
            on_update: self.update_drain_timeout.map(Duration::from_secs).or(default.on_update),
            on_delete: self.delete_drain_timeout.map(Duration::from_secs).or(default.on_delete),
        })
    }
}

/// What the digest found at one certificate option, so that "absent",
/// "unreadable" and "empty file" cannot collapse into the same digest.
#[cfg(feature = "transport")]
#[derive(Hash)]
enum MaterialTag {
    /// the transport string does not carry the option at all
    Absent,
    /// the option names a path
    Named,
    /// the path was read, and its contents follow
    Read,
    /// the path could not be read, or holds more than the cap allows
    Unreadable,
}

impl EndpointSource for EndpointConf {
    fn build(&self) -> Result<EndpointSpec, String> {
        let drain = self.drain_policy();
        let info = Config::build(self.clone()).map_err(|e| e.to_string())?;

        Ok(EndpointSpec {
            endpoint: info.endpoint,
            tcp: !info.no_tcp,
            udp: info.use_udp,
            drain,
            material: self.material_digest,
        })
    }

    fn refresh(&mut self) {
        // Only a build with transports can name certificate material, and only
        // such a build has kaminari to read the option strings the same way the
        // constructor does. Elsewhere the digest stays at its default, so it
        // never contributes a difference to the diff.
        #[cfg(feature = "transport")]
        {
            self.material_digest = self.digest_material();
        }
    }
}
