//! Global dns resolver, explicitly configured path (U3 / R35, KTD5).
//!
//! The resolver is a process-wide singleton that is frozen after the first
//! configuration: a second attempt must report an error instead of panicking,
//! and no path may rely on `static mut`.

use realm_core::dns;

#[tokio::test]
async fn configure_once_then_reject_reconfiguration() {
    let conf = dns::config::ResolverConfig::udp_and_tcp(&dns::config::GOOGLE);

    assert!(dns::effective_conf().is_none(), "nothing is configured yet");

    dns::build_lazy(Some(conf.clone()), None).expect("first configuration must be accepted");

    let effective = dns::effective_conf().expect("configuration is readable for status reporting");
    assert_eq!(effective.conf.name_servers().len(), conf.name_servers().len());

    // a second configuration is rejected, and the process stays alive
    let err = dns::build_lazy(Some(conf), None).expect_err("reconfiguration must be rejected");
    assert!(err.to_string().contains("already"), "{}", err);

    // building the resolver itself still works
    dns::force_init();
}
