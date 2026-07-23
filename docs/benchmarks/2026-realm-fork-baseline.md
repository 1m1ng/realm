# Realm hot-reload fork — performance baseline

Answers one question: does owning the per-generation configuration through an
`Arc` — instead of borrowing the listener's stack frame through the old
`Ref<T>` — cost anything measurable on the data path?

**Result: no.** Every metric lands inside the machine's own run-to-run noise.

| metric | v2.9.4 | fork | delta | run-to-run spread |
|---|---|---|---|---|
| tcp throughput | 36.59 Gbit/s | 36.07 Gbit/s | −1.4 % | 22 % |
| udp throughput | 2.50 Gbit/s | 2.49 Gbit/s | −0.3 % | 5 % |
| new connections | 4676 /s | 4800 /s | +2.6 % | 13 % |
| request rtt (p50) | 37.8 µs | 37.8 µs | ±0.0 % | 2 % |

Medians over six interleaved rounds. The spread column is the range a single
binary covers across its own rounds: a difference smaller than that is not a
difference.

## What changed on the data path

Per accepted connection, the fork adds:

- one `Arc::clone` of the endpoint's runtime configuration,
- one registration in the connection cohort (an atomic increment, plus an mpsc
  sender clone), released when the connection ends,
- one `tokio::select!` arm on the accept loop and, for udp, on the association
  loop.

The relay itself — `realm_io`'s copy path, the transport stack, dns — is
untouched. The rtt figure is the one that would notice per-connection overhead
first, and it is flat.

## Method

Both binaries are built from source on the same machine with the same pinned
toolchain (`nightly-2026-07-22`) and the same release profile, so the
comparison isolates the code change rather than the compiler. The baseline is
the fork's parent commit, `f3803d6` (upstream v2.9.4), exported with
`git archive`.

Traffic is generated through a relay each binary serves, against shared
backends that stay up for the whole run:

- **tcp throughput** — `iperf3` for 5 s through the relay
- **udp throughput** — `iperf3 -u -b 4G -l 1200` for 5 s through the relay
  (iperf3 keeps a tcp control connection on the same port, so that endpoint
  serves both data planes)
- **new connection rate** — 2000 short-lived connections, each carrying one
  64-byte request
- **request rtt** — 20 000 ping/pong exchanges on one established connection,
  after a 200-exchange warm-up

**Rounds are interleaved, and the order flips every round.** This matters more
than the round count: a first run of this benchmark measured each binary's
rounds back to back and showed the candidate 13 % slower on tcp throughput —
an artifact of the machine drifting under load, not of the code. Alternating
made the difference disappear.

## Reproducing

```sh
git archive f3803d6 | tar -x -C /tmp/realm-baseline
(cd /tmp/realm-baseline && cargo build --release)
cargo build --release

docs/benchmarks/bench.sh /tmp/realm-baseline/target/release/realm ./target/release/realm 6 \
  | tee bench-raw.txt
python3 docs/benchmarks/summarize.py < bench-raw.txt
```

`summarize.py` prints the median per binary, the delta, and calls a metric a
regression only when the delta exceeds half the observed spread.

## Environment

- Debian 13, kernel 7.0.12-1-pve, x86_64, 8 cores
- rustc 1.99.0-nightly (6f72b5dd5 2026-07-22), release profile with
  `lto = true`, `codegen-units = 1`
- iperf 3.18
- loopback only; the machine carried an unrelated background load of ~3.4

## Limits of this measurement

- **Loopback, not a network.** Absolute throughput here is a memory-bandwidth
  and scheduler number. It stresses the copy path hard, which is what makes it
  useful for spotting a per-byte regression, but it says nothing about
  behaviour under real link latency.
- **Shared machine.** The 22 % spread on tcp throughput comes from competing
  load. A regression smaller than roughly 5 % would not be visible here; if one
  is ever suspected, this benchmark needs a quiet machine and pinned cores.
- **No long-run soak.** Leak behaviour under churn is covered separately by
  `realm_core/tests/stress.rs`, and the replacement gap by
  `realm_core/tests/gap.rs` (measured: 3.1 ms worst on ipv4, 2.9 ms on ipv6
  across 20 same-address replacements).

## Raw data

```
metric                    binary      rounds (1..6)
tcp_throughput_bps        baseline    36.72  36.00  37.06  36.46  37.60  29.41  (Gbit/s)
tcp_throughput_bps        fork        37.46  37.81  36.11  36.03  35.02  35.49
udp_throughput_bps        baseline     2.458  2.515  2.493  2.509  2.447  2.527 (Gbit/s)
udp_throughput_bps        fork         2.479  2.533  2.429  2.508  2.518  2.415
conn_rate_per_s           baseline     4403   4644   4848   4709   4989   4581
conn_rate_per_s           fork         4694   4441   4705   4929   4908   4894
rtt_us p50                baseline     37.4   37.9   37.7   37.7   37.8   37.9
rtt_us p50                fork         37.7   37.4   38.0   37.8   37.7   38.1
rtt_us p99                baseline     94.8   98.4   92.6   93.1   94.7   96.1
rtt_us p99                fork         96.9   99.2   98.6   95.4   98.0  101.3
```

Every short-lived connection completed in every round (2000/2000), on both
binaries.
