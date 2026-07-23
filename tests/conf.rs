//! Structured configuration validation (U2 / R4).
//!
//! Building an endpoint from user input must never panic: every rejection is a
//! structured error naming the offending field, and the CLI turns it into a
//! non-zero exit instead of an abort.

use std::io::Write;
use std::process::Command;

use realm::conf::{Config, EndpointConf, FullConf};

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
