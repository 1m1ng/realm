use serde::{Serialize, Deserialize};
use std::net::{IpAddr, SocketAddr, ToSocketAddrs};

use realm_core::endpoint::{Endpoint, RemoteAddr};

#[cfg(feature = "balance")]
use realm_core::balance::Balancer;

#[cfg(feature = "transport")]
use realm_core::kaminari::mix::{MixAccept, MixConnect};

use super::{BuildError, Config, NetConf, NetInfo};

#[derive(Debug, Serialize, Deserialize)]
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

    #[serde(default)]
    #[serde(skip_serializing_if = "Config::is_empty")]
    pub network: NetConf,
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
        let Some((_, weights)) = s.split_once(':') else {
            return Err(BuildError::new("balance", s, "expected `strategy: weight, ...`"));
        };

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
    fn build_transport(&self) -> Option<(MixAccept, MixConnect)> {
        use realm_core::kaminari::mix::{MixClientConf, MixServerConf};
        use realm_core::kaminari::opt::get_ws_conf;
        use realm_core::kaminari::opt::get_tls_client_conf;
        use realm_core::kaminari::opt::get_tls_server_conf;

        let Self {
            listen_transport,
            remote_transport,
            ..
        } = self;

        let listen_ws = listen_transport.as_ref().and_then(|s| get_ws_conf(s));
        let listen_tls = listen_transport.as_ref().and_then(|s| get_tls_server_conf(s));

        let remote_ws = remote_transport.as_ref().and_then(|s| get_ws_conf(s));
        let remote_tls = remote_transport.as_ref().and_then(|s| get_tls_client_conf(s));

        if matches!(
            (&listen_ws, &listen_tls, &remote_ws, &remote_tls),
            (None, None, None, None)
        ) {
            None
        } else {
            let ac = MixAccept::new_shared(MixServerConf {
                ws: listen_ws,
                tls: listen_tls,
            });
            let cc = MixConnect::new_shared(MixClientConf {
                ws: remote_ws,
                tls: remote_tls,
            });
            Some((ac, cc))
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
            conn_opts.transport = self.build_transport();
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
        }
    }
}
