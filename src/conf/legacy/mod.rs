use serde::{Serialize, Deserialize};

use super::{FullConf, EndpointConf, BuildError};

// from https://github.com/zhboner/realm/blob/8ad8f0405e97cc470ba8b76c059c203b7381d2fb/src/lib.rs#L58-L63
// pub struct ConfigFile {
//     pub listening_addresses: Vec<String>,
//     pub listening_ports: Vec<String>,
//     pub remote_addresses: Vec<String>,
//     pub remote_ports: Vec<String>,
// }
#[derive(Serialize, Deserialize)]
pub struct LegacyConf {
    #[serde(rename = "listening_addresses")]
    pub listen_addrs: Vec<String>,
    #[serde(rename = "listening_ports")]
    pub listen_ports: Vec<String>,
    #[serde(rename = "remote_addresses")]
    pub remote_addrs: Vec<String>,
    #[serde(rename = "remote_ports")]
    pub remote_ports: Vec<String>,
}

fn parse_port(field: &'static str, s: &str) -> Result<u16, BuildError> {
    s.trim()
        .parse::<u16>()
        .map_err(|e| BuildError::new(field, s, format!("not a valid port: {}", e)))
}

fn flatten_ports(field: &'static str, ports: Vec<String>) -> Result<Vec<u16>, BuildError> {
    let mut flat = Vec::with_capacity(ports.len());

    for range in ports {
        let mut parts = range.splitn(2, '-');
        let start = parse_port(field, parts.next().unwrap_or(""))?;
        let end = match parts.next() {
            Some(end) => parse_port(field, end)?,
            None => start,
        };

        if end < start {
            return Err(BuildError::new(field, &range, "port range ends before it starts"));
        }

        flat.extend(start..=end);
    }

    Ok(flat)
}

fn join_addr_port(
    addr_field: &'static str,
    addrs: Vec<String>,
    ports: Vec<u16>,
    len: usize,
) -> Result<Vec<String>, BuildError> {
    use std::iter::repeat;

    let Some(&port0) = ports.first() else {
        return Err(BuildError::new(addr_field, "", "no port given"));
    };
    let Some(addr0) = addrs.first().cloned() else {
        return Err(BuildError::new(addr_field, "", "no address given"));
    };

    let port_iter = ports.into_iter().take(len).chain(repeat(port0)).take(len);
    let addr_iter = addrs.into_iter().take(len).chain(repeat(addr0)).take(len);

    Ok(addr_iter
        .zip(port_iter)
        .map(|(addr, port)| format!("{}:{}", addr, port))
        .collect())
}

impl TryFrom<LegacyConf> for FullConf {
    type Error = BuildError;

    fn try_from(x: LegacyConf) -> Result<Self, Self::Error> {
        let LegacyConf {
            listen_addrs,
            listen_ports,
            remote_addrs,
            remote_ports,
        } = x;

        let listen_ports = flatten_ports("listening_ports", listen_ports)?;
        let remote_ports = flatten_ports("remote_ports", remote_ports)?;

        let len = listen_ports.len();

        let listen = join_addr_port("listening_addresses", listen_addrs, listen_ports, len)?;
        let remote = join_addr_port("remote_addresses", remote_addrs, remote_ports, len)?;

        let endpoints = listen
            .into_iter()
            .zip(remote)
            .map(|(listen, remote)| EndpointConf {
                listen,
                remote,
                through: None,
                interface: None,
                listen_interface: None,
                listen_transport: None,
                remote_transport: None,
                network: Default::default(),
                extra_remotes: Vec::new(),
                balance: None,
            })
            .collect();

        Ok(FullConf {
            endpoints,
            ..Default::default()
        })
    }
}

#[cfg(test)]
mod tests {
    macro_rules! strvec {
        ( $( $x: expr ),+ ) => {
            vec![
                $(
                    String::from($x),
                )+
            ]
        };
    }

    #[test]
    fn flatten_ports() {
        let v1 = strvec!["1-4"];
        let v2 = strvec!["1-2", "3-4"];
        let v3 = strvec!["1-3", "4"];
        let v4 = strvec!["1", "2", "3", "4"];
        let expect = Ok(vec![1, 2, 3, 4]);
        assert_eq!(super::flatten_ports("listening_ports", v1), expect);
        assert_eq!(super::flatten_ports("listening_ports", v2), expect);
        assert_eq!(super::flatten_ports("listening_ports", v3), expect);
        assert_eq!(super::flatten_ports("listening_ports", v4), expect);

        // malformed input is rejected instead of panicking
        assert!(super::flatten_ports("listening_ports", strvec!["not-a-port"]).is_err());
        assert!(super::flatten_ports("listening_ports", strvec!["4-1"]).is_err());
        // the upper bound no longer overflows
        assert_eq!(
            super::flatten_ports("listening_ports", strvec!["65534-65535"]),
            Ok(vec![65534, 65535])
        );
    }

    #[test]
    fn join_addr_port() {
        const FIELD: &str = "listening_addresses";

        let addrs = strvec!["a.com", "b.com", "c.com"];
        let ports = vec![1, 2, 3];
        let result = strvec!["a.com:1", "b.com:2", "c.com:3"];
        assert_eq!(super::join_addr_port(FIELD, addrs, ports, 3), Ok(result));

        let addrs = strvec!["a.com", "b.com", "c.com"];
        let ports = vec![1, 2, 3];
        let result = strvec!["a.com:1", "b.com:2"];
        assert_eq!(super::join_addr_port(FIELD, addrs, ports, 2), Ok(result));

        let addrs = strvec!["a.com", "b.com", "c.com"];
        let ports = vec![1, 2, 3];
        let result = strvec!["a.com:1", "b.com:2", "c.com:3", "a.com:1"];
        assert_eq!(super::join_addr_port(FIELD, addrs, ports, 4), Ok(result));

        let addrs = strvec!["a.com", "b.com", "c.com"];
        let ports = vec![1, 2, 3, 4, 5, 6];
        let result = strvec!["a.com:1", "b.com:2", "c.com:3", "a.com:4"];
        assert_eq!(super::join_addr_port(FIELD, addrs, ports, 4), Ok(result));

        let addrs = strvec!["a.com", "b.com", "c.com", "d.com", "e.com"];
        let ports = vec![1, 2, 3];
        let result = strvec!["a.com:1", "b.com:2", "c.com:3", "d.com:1"];
        assert_eq!(super::join_addr_port(FIELD, addrs, ports, 4), Ok(result));

        let addrs = strvec!["a.com", "b.com", "c.com"];
        let ports = vec![1, 2, 3];
        let result = strvec!["a.com:1", "b.com:2", "c.com:3", "a.com:1", "a.com:1"];
        assert_eq!(super::join_addr_port(FIELD, addrs, ports, 5), Ok(result));

        // empty input is rejected instead of panicking on index 0
        assert!(super::join_addr_port(FIELD, Vec::new(), vec![1], 1).is_err());
        assert!(super::join_addr_port(FIELD, strvec!["a.com"], Vec::new(), 1).is_err());
    }
}
