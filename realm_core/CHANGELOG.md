# Changelog

## 0.6.0

Breaking changes relative to the upstream v2.9.4 baseline (0.5.1), introduced by
the hot-reload fork:

- **`trick::Ref<T>` removed.** The `trick` module and its raw-pointer reference
  wrapper are gone. Connections and udp associations now own their per-generation
  configuration as an `Arc<TcpRuntime>` / `Arc<UdpRuntime>` captured at accept
  time, so a listener can be torn down without dangling its in-flight tasks.
- **`dns::build` / `dns::build_lazy` now return `Result<()>`.** The global
  resolver is an `OnceLock`; configuring it a second time returns an error
  instead of panicking, and a failed resolver build fails lookups instead of
  aborting the process.
- **`tcp::run_tcp` / `udp::run_udp` no longer panic on bind failure.** Their
  signatures are unchanged (`-> io::Result<()>`), but a bind error is now
  returned as `Err` rather than raised via `panic!`.
- **New `lifecycle` module.** `EndpointManager`, `Reconciler`, cohort tracking,
  and the last-known-good snapshot live here; see `docs/control-api.md`.

Migration: callers of `dns::build`/`build_lazy` must handle the `Result`; anyone
who depended on `realm_core::trick::Ref` must switch to owned `Arc` runtime
configuration.
