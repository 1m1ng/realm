//! New-connection gap on same-address replacement (U11 / Success Criteria 3).
//!
//! Replacing an endpoint on its own address is stop-accept → join → bind: the
//! listener socket must be provably released before it is rebound, which leaves
//! a short window in which a *new* connection is refused. Established
//! connections are unaffected — that is what the drain cohort is for.
//!
//! This test measures that window on both address families by hammering the
//! address with connection attempts while generations are applied. The measured
//! values are printed, so `cargo test -- --nocapture` doubles as the data
//! source for the benchmark record.

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use tokio::net::TcpStream;

use realm_core::endpoint::{Endpoint, RemoteAddr};
use realm_core::lifecycle::{
    CancellationToken, DesiredEndpoint, EndpointSource, EndpointSpec, ReconcileRequest, Reconciler,
};

mod common;
use common::{free_addr_on, has_ipv6, spawn_echo};

/// replacements performed per family
const ROUNDS: usize = 20;
/// how often the address is probed
const PROBE_INTERVAL: Duration = Duration::from_micros(200);
/// the window has to stay in the low-millisecond range; the bound is loose
/// enough for a loaded CI machine, the printed value is the real signal
const MAX_GAP: Duration = Duration::from_millis(250);

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Spec {
    listen: String,
    remote: SocketAddr,
}

impl EndpointSource for Spec {
    fn build(&self) -> Result<EndpointSpec, String> {
        let laddr: SocketAddr = self.listen.parse().map_err(|e| format!("invalid listen: {}", e))?;

        Ok(EndpointSpec {
            endpoint: Endpoint {
                laddr,
                raddr: RemoteAddr::SocketAddr(self.remote),
                bind_opts: Default::default(),
                conn_opts: Default::default(),
                extra_raddrs: Vec::new(),
            },
            tcp: true,
            udp: false,
            drain: None,
        })
    }
}

/// Keep connecting to `addr` until told to stop, recording what happened.
async fn probe(addr: SocketAddr, stop: CancellationToken) -> Vec<(Instant, bool)> {
    let mut samples = Vec::with_capacity(4096);

    loop {
        if stop.is_cancelled() {
            return samples;
        }

        let now = Instant::now();
        let ok = tokio::time::timeout(Duration::from_millis(500), TcpStream::connect(addr))
            .await
            .map(|r| r.is_ok())
            .unwrap_or(false);
        samples.push((now, ok));

        tokio::time::sleep(PROBE_INTERVAL).await;
    }
}

/// Longest stretch between two successful connections that had a failure in
/// between, and how many attempts failed in total.
fn worst_gap(samples: &[(Instant, bool)]) -> (Duration, usize, usize) {
    let mut worst = Duration::ZERO;
    let mut failures = 0;
    let mut last_ok: Option<Instant> = None;
    let mut failed_since_ok = false;

    for (at, ok) in samples {
        if *ok {
            if failed_since_ok {
                if let Some(prev) = last_ok {
                    worst = worst.max(at.duration_since(prev));
                }
            }
            last_ok = Some(*at);
            failed_since_ok = false;
        } else {
            failures += 1;
            failed_since_ok = true;
        }
    }

    (worst, failures, samples.len())
}

async fn measure(host: &str) -> (Duration, usize, usize) {
    let echo_a = spawn_echo("a:").await;
    let echo_b = spawn_echo("b:").await;
    let laddr = free_addr_on(host);

    let mut rec = Reconciler::new();
    let spec = |remote: SocketAddr| Spec {
        listen: laddr.to_string(),
        remote,
    };

    rec.reconcile(ReconcileRequest {
        generation: 1,
        endpoints: vec![DesiredEndpoint {
            id: "a".into(),
            spec: spec(echo_a),
        }],
    })
    .await
    .expect("the endpoint starts");

    // make sure the probe sees a running listener before the first replacement
    TcpStream::connect(laddr).await.expect("the relay is up");

    let stop = CancellationToken::new();
    let probing = tokio::spawn(probe(laddr, stop.clone()));
    tokio::time::sleep(Duration::from_millis(20)).await;

    for round in 0..ROUNDS {
        let remote = if round % 2 == 0 { echo_b } else { echo_a };
        rec.reconcile(ReconcileRequest {
            generation: (round + 2) as u64,
            endpoints: vec![DesiredEndpoint {
                id: "a".into(),
                spec: spec(remote),
            }],
        })
        .await
        .expect("every replacement applies");

        // give the probe room to sample between replacements, otherwise the
        // whole run finishes inside a handful of attempts and the measurement
        // says nothing
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    tokio::time::sleep(Duration::from_millis(20)).await;
    stop.cancel();
    let samples = probing.await.expect("the probe finishes");

    rec.shutdown().await;
    worst_gap(&samples)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn same_address_replacement_gap_is_milliseconds_ipv4() {
    let (worst, failures, attempts) = measure("127.0.0.1").await;

    println!(
        "[gap] ipv4: {} replacements, worst gap {:?}, {}/{} attempts refused",
        ROUNDS, worst, failures, attempts
    );

    assert!(
        worst < MAX_GAP,
        "the new-connection gap grew beyond the milliseconds range: {:?}",
        worst
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn same_address_replacement_gap_is_milliseconds_ipv6() {
    if !has_ipv6() {
        println!("[gap] ipv6: skipped, no ipv6 loopback on this host");
        return;
    }

    let (worst, failures, attempts) = measure("::1").await;

    println!(
        "[gap] ipv6: {} replacements, worst gap {:?}, {}/{} attempts refused",
        ROUNDS, worst, failures, attempts
    );

    assert!(
        worst < MAX_GAP,
        "the new-connection gap grew beyond the milliseconds range: {:?}",
        worst
    );
}
