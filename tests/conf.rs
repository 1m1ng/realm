//! Structured configuration validation (U2 / R4).
//!
//! Building an endpoint from user input must never panic: every rejection is a
//! structured error naming the offending field, and the CLI turns it into a
//! non-zero exit instead of an abort.

use std::io::Write;
use std::process::Command;

use realm::conf::{Config, EndpointConf, FullConf};
use realm::core::lifecycle::EndpointSource;

fn parse(conf: &str) -> FullConf {
    FullConf::from_conf_str(conf).expect("config text should parse")
}

fn single_endpoint(conf: &str) -> EndpointConf {
    parse(conf).endpoints.into_iter().next().expect("one endpoint")
}

#[test]
fn invalid_listen_address_reports_field() {
    let ep = single_endpoint(
        r#"
[[endpoints]]
listen = "definitely not an address"
remote = "127.0.0.1:20000"
"#,
    );

    let err = ep.build().expect_err("invalid listen address must be rejected");
    let msg = err.to_string();
    assert!(msg.contains("listen"), "error should name the field: {}", msg);
}

#[test]
fn invalid_remote_port_reports_field() {
    let ep = single_endpoint(
        r#"
[[endpoints]]
listen = "127.0.0.1:10000"
remote = "example.com:not-a-port"
"#,
    );

    let err = ep.build().expect_err("invalid remote port must be rejected");
    let msg = err.to_string();
    assert!(msg.contains("remote"), "error should name the field: {}", msg);
}

#[test]
fn invalid_extra_remote_reports_field() {
    let ep = single_endpoint(
        r#"
[[endpoints]]
listen = "127.0.0.1:10000"
remote = "127.0.0.1:20000"
extra_remotes = ["no-port-here"]
"#,
    );

    let err = ep.build().expect_err("invalid extra remote must be rejected");
    let msg = err.to_string();
    assert!(msg.contains("extra_remotes"), "error should name the field: {}", msg);
}

#[test]
fn valid_endpoint_builds_expected_addresses() {
    use realm::core::endpoint::RemoteAddr;

    let ep = single_endpoint(
        r#"
[[endpoints]]
listen = "127.0.0.1:10000"
remote = "example.com:20000"
extra_remotes = ["127.0.0.1:20001"]
"#,
    );

    let info = ep.build().expect("valid endpoint must build");
    assert_eq!(info.endpoint.laddr, "127.0.0.1:10000".parse().unwrap());
    assert_eq!(info.endpoint.raddr, RemoteAddr::DomainName("example.com".into(), 20000));
    assert_eq!(
        info.endpoint.extra_raddrs,
        vec![RemoteAddr::SocketAddr("127.0.0.1:20001".parse().unwrap())]
    );
    assert!(!info.no_tcp);
    assert!(!info.use_udp);
}

#[test]
fn missing_config_file_is_an_error_not_a_panic() {
    let err = FullConf::from_conf_file("/nonexistent/realm-test-config.toml")
        .expect_err("missing config file must be an error");
    assert!(err.to_string().contains("/nonexistent/realm-test-config.toml"));
}

#[test]
fn malformed_config_file_is_an_error_not_a_panic() {
    let mut path = std::env::temp_dir();
    path.push(format!("realm-conf-test-{}.toml", std::process::id()));
    std::fs::write(&path, "this is not = valid = toml").unwrap();

    let err = FullConf::from_conf_file(path.to_str().unwrap()).expect_err("malformed config must be an error");
    assert!(err.to_string().contains("parse"), "{}", err);

    let _ = std::fs::remove_file(&path);
}

#[test]
fn cli_rejects_invalid_config_with_nonzero_exit() {
    let mut path = std::env::temp_dir();
    path.push(format!("realm-cli-test-{}.toml", std::process::id()));
    let mut file = std::fs::File::create(&path).unwrap();
    writeln!(
        file,
        "[[endpoints]]\nlisten = \"definitely not an address\"\nremote = \"127.0.0.1:20000\""
    )
    .unwrap();
    drop(file);

    let out = Command::new(env!("CARGO_BIN_EXE_realm"))
        .env_remove("REALM_CONF")
        .args(["-c", path.to_str().unwrap()])
        .output()
        .expect("realm binary should run");

    let _ = std::fs::remove_file(&path);

    assert!(!out.status.success(), "invalid config must exit non-zero");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("listen"),
        "stderr should describe the error: {}",
        stderr
    );
}

// The load balancer and transport option strings are handed to third-party
// parsers (`realm_lb`, `kaminari`) that `panic!` on malformed input. Since a
// control-plane request reaches `EndpointConf::build` on the reconciler task, a
// panic there would take the whole control plane down (finding #2). Building
// must reject every malformed value with a structured error instead.

#[cfg(feature = "balance")]
#[test]
fn balance_without_strategy_separator_reports_field() {
    let ep = single_endpoint(
        r#"
[[endpoints]]
listen = "127.0.0.1:10000"
remote = "127.0.0.1:20000"
balance = "roundrobin"
"#,
    );

    let err = ep.build().expect_err("a balance value with no `:` must be rejected");
    assert!(err.to_string().contains("balance"), "{}", err);
}

#[cfg(feature = "balance")]
#[test]
fn balance_unknown_strategy_is_rejected_not_panicking() {
    // `realm_lb::Strategy::from` panics on anything but off/iphash/roundrobin.
    let ep = single_endpoint(
        r#"
[[endpoints]]
listen = "127.0.0.1:10000"
remote = "127.0.0.1:20000"
balance = "nonsense: 1"
"#,
    );

    let err = ep
        .build()
        .expect_err("an unknown balance strategy must be rejected, not panic");
    assert!(err.to_string().contains("balance"), "{}", err);
}

#[cfg(feature = "balance")]
#[test]
fn balance_weight_count_mismatch_reports_field() {
    // one remote (no extra_remotes) but three weights.
    let ep = single_endpoint(
        r#"
[[endpoints]]
listen = "127.0.0.1:10000"
remote = "127.0.0.1:20000"
balance = "roundrobin: 1, 2, 3"
"#,
    );

    let err = ep
        .build()
        .expect_err("a weight count that does not match the remotes must be rejected");
    assert!(err.to_string().contains("balance"), "{}", err);
}

#[cfg(feature = "balance")]
#[test]
fn valid_balance_still_builds() {
    let ep = single_endpoint(
        r#"
[[endpoints]]
listen = "127.0.0.1:10000"
remote = "127.0.0.1:20000"
extra_remotes = ["127.0.0.1:20001"]
balance = "roundrobin: 1, 2"
"#,
    );

    ep.build().expect("a well-formed balance value must still build");
}

#[cfg(feature = "transport")]
#[test]
fn malformed_listen_transport_is_rejected_not_panicking() {
    // `kaminari::get_ws_conf("ws")` panics: the `ws` option is present but host
    // and path are missing.
    let ep = single_endpoint(
        r#"
[[endpoints]]
listen = "127.0.0.1:10000"
remote = "127.0.0.1:20000"
listen_transport = "ws"
"#,
    );

    let err = ep
        .build()
        .expect_err("a malformed listen transport must be rejected, not panic");
    assert!(err.to_string().contains("listen_transport"), "{}", err);
}

#[cfg(feature = "transport")]
#[test]
fn malformed_remote_transport_is_rejected_not_panicking() {
    // `kaminari::get_tls_client_conf("tls")` panics: `tls` is present but `sni`
    // is missing.
    let ep = single_endpoint(
        r#"
[[endpoints]]
listen = "127.0.0.1:10000"
remote = "127.0.0.1:20000"
remote_transport = "tls"
"#,
    );

    let err = ep
        .build()
        .expect_err("a malformed remote transport must be rejected, not panic");
    assert!(err.to_string().contains("remote_transport"), "{}", err);
}

#[test]
fn an_endpoint_naming_no_transport_still_builds() {
    // the transport path is optional: an endpoint that names neither a listen
    // nor a remote transport must build without going near tls at all.
    let ep = single_endpoint(
        r#"
[[endpoints]]
listen = "127.0.0.1:10000"
remote = "127.0.0.1:20000"
"#,
    );

    ep.build().expect("an endpoint with no transport must still build");
}

#[cfg(feature = "transport")]
#[test]
fn valid_transport_still_builds() {
    let ep = single_endpoint(
        r#"
[[endpoints]]
listen = "127.0.0.1:10000"
remote = "127.0.0.1:20000"
listen_transport = "ws;host=example.com;path=/tunnel"
"#,
    );

    ep.build().expect("a well-formed transport value must still build");
}

// The reconciler rebuilds an endpoint only when its desired state differs from
// the active generation's, and that state is the configuration document.
// Replacing the *bytes* of a certificate file leaves every field of the
// document identical, so a rotation in place would be invisible: the endpoint
// would go on serving pre-rotation material while the control plane reported
// convergence. `EndpointSource::refresh` folds the contents of the files the
// transport options name into the value itself, so a rotation becomes a
// difference the diff can see — and, because the field is invisible to serde,
// one the submission hash still cannot see.

fn plain_endpoint() -> EndpointConf {
    single_endpoint(
        r#"
[[endpoints]]
listen = "127.0.0.1:10000"
remote = "127.0.0.1:20000"
"#,
    )
}

#[test]
fn an_endpoint_naming_no_material_never_forces_a_rebuild() {
    let submitted = plain_endpoint();

    let mut refreshed = submitted.clone();
    refreshed.refresh();
    assert_eq!(
        submitted, refreshed,
        "an endpoint that names no certificate material must not change under refresh"
    );

    let mut again = refreshed.clone();
    again.refresh();
    assert_eq!(refreshed, again, "the digest must be stable across refreshes");
}

#[test]
fn the_digest_is_not_part_of_the_configuration_document() {
    let mut ep = plain_endpoint();
    ep.refresh();

    let document = serde_json::to_value(&ep).expect("an endpoint serializes");
    let keys: Vec<&String> = document
        .as_object()
        .expect("an endpoint is a json object")
        .keys()
        .collect();
    assert!(
        !keys.iter().any(|k| k.contains("digest")),
        "the digest must not appear in the configuration document: {:?}",
        keys
    );

    // a document that carries no digest — every document an agent sends —
    // still deserializes
    let restored: EndpointConf = serde_json::from_value(document).expect("a document without a digest deserializes");
    assert_eq!(restored.listen, ep.listen);
    assert_eq!(restored.remote, ep.remote);
}

#[cfg(feature = "transport")]
mod material {
    use super::*;

    use std::path::{Path, PathBuf};

    /// A private directory for one test's certificate material, removed on drop.
    pub(super) struct MaterialDir(pub(super) PathBuf);

    impl MaterialDir {
        pub(super) fn new(name: &str) -> Self {
            let mut path = std::env::temp_dir();
            path.push(format!("realm-material-{}-{}", name, std::process::id()));
            let _ = std::fs::remove_dir_all(&path);
            std::fs::create_dir_all(&path).unwrap();
            Self(path)
        }

        pub(super) fn write(&self, name: &str, content: &str) -> PathBuf {
            let path = self.0.join(name);
            std::fs::write(&path, content).unwrap();
            path
        }
    }

    impl Drop for MaterialDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    /// A toml basic string: `Debug` escapes exactly what toml needs, so a
    /// windows path with backslashes survives being embedded in a document.
    pub(super) fn quoted(s: &str) -> String {
        format!("{:?}", s)
    }

    fn refreshed(conf: &str) -> EndpointConf {
        let mut ep = single_endpoint(conf);
        ep.refresh();
        ep
    }

    /// A client endpoint whose trust anchor is `ca`.
    fn client_endpoint(ca: &Path) -> EndpointConf {
        refreshed(&format!(
            r#"
[[endpoints]]
listen = "127.0.0.1:10000"
remote = "127.0.0.1:20000"
remote_transport = {}
"#,
            quoted(&format!("tls;sni=example.com;ca={}", ca.display()))
        ))
    }

    /// A server endpoint presenting `cert` and `key`.
    fn server_endpoint(cert: &Path, key: &Path) -> EndpointConf {
        refreshed(&format!(
            r#"
[[endpoints]]
listen = "127.0.0.1:10000"
remote = "127.0.0.1:20000"
listen_transport = {}
"#,
            quoted(&format!("tls;cert={};key={}", cert.display(), key.display()))
        ))
    }

    #[test]
    fn replacing_the_ca_bytes_changes_the_desired_state() {
        let dir = MaterialDir::new("ca-bytes");
        let ca = dir.write(
            "ca.pem",
            "-----BEGIN CERTIFICATE-----\nbefore\n-----END CERTIFICATE-----\n",
        );

        let before = client_endpoint(&ca);

        dir.write(
            "ca.pem",
            "-----BEGIN CERTIFICATE-----\nafter\n-----END CERTIFICATE-----\n",
        );
        let after = client_endpoint(&ca);

        assert_ne!(
            before, after,
            "a trust anchor replaced in place must change the endpoint's desired state"
        );
    }

    #[test]
    fn replacing_the_cert_bytes_changes_the_desired_state() {
        let dir = MaterialDir::new("cert-bytes");
        let cert = dir.write("cert.pem", "leaf before\n");
        let key = dir.write("key.pem", "key\n");

        let before = server_endpoint(&cert, &key);

        dir.write("cert.pem", "leaf after\n");
        let after = server_endpoint(&cert, &key);

        assert_ne!(
            before, after,
            "a leaf certificate replaced in place must change the endpoint's desired state"
        );
    }

    #[test]
    fn replacing_the_key_bytes_changes_the_desired_state() {
        let dir = MaterialDir::new("key-bytes");
        let cert = dir.write("cert.pem", "leaf\n");
        let key = dir.write("key.pem", "key before\n");

        let before = server_endpoint(&cert, &key);

        dir.write("key.pem", "key after\n");
        let after = server_endpoint(&cert, &key);

        assert_ne!(
            before, after,
            "a private key replaced in place must change the endpoint's desired state"
        );
    }

    #[test]
    fn untouched_material_leaves_the_desired_state_alone() {
        let dir = MaterialDir::new("untouched");
        let ca = dir.write("ca.pem", "anchor\n");
        let cert = dir.write("cert.pem", "leaf\n");
        let key = dir.write("key.pem", "key\n");

        assert_eq!(
            client_endpoint(&ca),
            client_endpoint(&ca),
            "material nobody touched must not churn a running endpoint"
        );
        assert_eq!(
            server_endpoint(&cert, &key),
            server_endpoint(&cert, &key),
            "material nobody touched must not churn a running endpoint"
        );
    }

    #[test]
    fn material_that_disappears_changes_the_desired_state() {
        let dir = MaterialDir::new("deleted");
        let ca = dir.write("ca.pem", "anchor\n");

        let before = client_endpoint(&ca);

        std::fs::remove_file(&ca).unwrap();
        let after = client_endpoint(&ca);

        assert_ne!(
            before, after,
            "a trust anchor that disappeared must change the endpoint's desired state"
        );
    }

    #[test]
    fn material_the_constructor_never_reads_is_not_digested() {
        // kaminari looks at `cert`/`key` only when the string carries `tls`, so
        // here it loads nothing. A file the endpoint never reads must not churn
        // it: the digest parses the string the way the constructor does.
        let dir = MaterialDir::new("no-tls");
        let cert = dir.write("cert.pem", "leaf before\n");

        let transport = quoted(&format!("ws;host=example.com;path=/x;cert={}", cert.display()));
        let conf = format!(
            r#"
[[endpoints]]
listen = "127.0.0.1:10000"
remote = "127.0.0.1:20000"
listen_transport = {}
"#,
            transport
        );

        let before = refreshed(&conf);
        dir.write("cert.pem", "leaf after\n");
        let after = refreshed(&conf);

        assert_eq!(
            before, after,
            "material no transport loads must not change the desired state"
        );
    }

    #[test]
    fn the_same_bytes_at_a_different_path_change_the_desired_state() {
        let dir = MaterialDir::new("moved");
        let here = dir.write("here.pem", "anchor\n");
        let there = dir.write("there.pem", "anchor\n");

        assert_ne!(
            client_endpoint(&here),
            client_endpoint(&there),
            "the path is part of the material's identity"
        );
    }

    #[test]
    fn a_refreshed_digest_does_not_travel_through_serde() {
        let dir = MaterialDir::new("serde");
        let ca = dir.write("ca.pem", "anchor\n");

        let refreshed = client_endpoint(&ca);
        let document = serde_json::to_value(&refreshed).expect("an endpoint serializes");
        let restored: EndpointConf = serde_json::from_value(document).expect("an endpoint deserializes");

        // The submission hash goes through serde, so the digest has to be
        // invisible to it: a byte-identical resubmission must still replay.
        let mut submitted = restored.clone();
        submitted.refresh();
        assert_eq!(
            refreshed, submitted,
            "a restored document must reach the same digest once refreshed"
        );
        assert_ne!(refreshed, restored, "the digest must not survive a serde round trip");
    }

    #[test]
    fn material_beyond_the_read_cap_is_treated_as_unreadable() {
        let dir = MaterialDir::new("oversized");
        let ca = dir.write("ca.pem", "anchor\n");
        let readable = client_endpoint(&ca);

        // A certificate bundle is small. An operator pointing `ca=` at a huge
        // file must not make the reconciler read all of it, so anything past
        // the cap is the unreadable case — and two files nobody can read look
        // alike, whatever their contents.
        dir.write("ca.pem", &"a".repeat(4 * 1024 * 1024 + 1));
        let first = client_endpoint(&ca);
        dir.write("ca.pem", &"b".repeat(4 * 1024 * 1024 + 2));
        let second = client_endpoint(&ca);

        assert_ne!(readable, first, "material that went over the cap is a change");
        assert_eq!(
            first, second,
            "a path past the read cap must digest as unreadable, not as its contents"
        );
    }
}

// A `ca=` names the roots this endpoint is willing to trust *instead of* the
// public bundle. Material the endpoint cannot load therefore has exactly one
// safe answer — fail the endpoint. Building anyway would leave a connection
// that verifies against the very roots the operator replaced, and it would
// look like success from every angle the control plane can see.

#[cfg(feature = "transport")]
mod trust {
    use super::material::{MaterialDir, quoted};
    use super::*;

    use std::path::Path;
    use std::sync::Once;

    use rcgen::{BasicConstraints, CertificateParams, DistinguishedName, DnType, IsCa, KeyPair, KeyUsagePurpose};

    /// The rustls provider is a process-wide singleton and installing it twice
    /// panics; realm's binary does it once at startup, so a test that builds a
    /// tls transport has to do the same.
    fn install_tls_provider() {
        static ONCE: Once = Once::new();
        ONCE.call_once(realm::core::kaminari::install_tls_provider);
    }

    /// Real material: the trust anchor is parsed by rustls, so a placeholder
    /// pem body would fail the load for the wrong reason. Issued in process
    /// rather than by a command line tool the runner may not have, and freshly
    /// per run rather than checked in, since a fixture would eventually expire.
    fn self_signed(dir: &MaterialDir, cert: &str, key: &str) -> std::path::PathBuf {
        let signing_key = KeyPair::generate().expect("a test key");

        let mut dn = DistinguishedName::new();
        dn.push(DnType::CommonName, "realm test root");

        let mut params = CertificateParams::new(Vec::<String>::new()).expect("root params");
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
        params.distinguished_name = dn;

        let root = params.self_signed(&signing_key).expect("a self signed root");

        dir.write(key, &signing_key.serialize_pem());
        dir.write(cert, &root.pem())
    }

    fn client_endpoint(options: &str) -> EndpointConf {
        single_endpoint(&format!(
            r#"
[[endpoints]]
listen = "127.0.0.1:10000"
remote = "127.0.0.1:20000"
remote_transport = {}
"#,
            quoted(options)
        ))
    }

    fn trusting(ca: &Path) -> EndpointConf {
        client_endpoint(&format!("tls;sni=example.com;ca={}", ca.display()))
    }

    #[test]
    fn a_trust_anchor_that_is_not_there_fails_the_endpoint() {
        install_tls_provider();

        let dir = MaterialDir::new("absent-anchor");
        let missing = dir.0.join("nowhere.pem");

        let err = trusting(&missing)
            .build()
            .expect_err("a trust anchor that cannot be read must fail the endpoint, not fall back to the public roots");

        let msg = err.to_string();
        assert!(
            msg.contains("remote_transport"),
            "the error must name the field: {}",
            msg
        );
        assert!(
            msg.contains(&missing.display().to_string()),
            "the error must name the material it could not load: {}",
            msg
        );
    }

    #[test]
    fn a_trust_anchor_together_with_insecure_fails_the_endpoint() {
        install_tls_provider();

        // pinning to a private root and disabling verification are opposite
        // instructions; honouring either one silently is a downgrade.
        let err = client_endpoint("tls;sni=example.com;ca=/does/not/matter.pem;insecure")
            .build()
            .expect_err("`ca` together with `insecure` must be rejected");

        let msg = err.to_string();
        assert!(
            msg.contains("remote_transport"),
            "the error must name the field: {}",
            msg
        );
        assert!(
            msg.contains("insecure"),
            "the error must say which two options conflict: {}",
            msg
        );
    }

    #[test]
    fn a_usable_trust_anchor_builds() {
        install_tls_provider();

        let dir = MaterialDir::new("usable-anchor");
        let ca = self_signed(&dir, "ca.pem", "ca.key");

        trusting(&ca).build().expect("a readable trust anchor must build");
    }
}
