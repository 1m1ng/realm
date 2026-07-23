//! Global dns resolver, unconfigured path (U3 / R35, KTD5).
//!
//! Without an explicit configuration the resolver falls back to the system
//! defaults, exactly as upstream did, and the effective configuration becomes
//! readable once it is frozen.

use std::net::SocketAddr;

use realm_core::dns;
use realm_core::endpoint::RemoteAddr;

#[tokio::test]
async fn default_resolver_is_used_and_then_frozen() {
    // a literal address never touches the resolver
    let addr: SocketAddr = "127.0.0.1:20000".parse().unwrap();
    let raddr = RemoteAddr::SocketAddr(addr);
    let resolved = dns::resolve_addr(&raddr)
        .await
        .expect("literal address needs no lookup");
    assert_eq!(resolved.iter().next(), Some(addr));

    // forcing initialization without a prior `build_lazy` uses system defaults
    dns::force_init();

    let effective = dns::effective_conf().expect("defaults are frozen and readable");
    assert!(!effective.conf.name_servers().is_empty() || effective.conf.domain().is_some());

    // once frozen, late configuration is refused rather than silently ignored
    assert!(dns::build_lazy(None, None).is_err());
}
