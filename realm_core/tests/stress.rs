//! Churn under load (U11 / Success Criteria 2).
//!
//! High-frequency create/update/delete while long-lived tcp connections and
//! udp associations stay resident: nothing may accumulate — no leaked task, no
//! leaked socket, no draining cohort that never goes away.
//!
//! The parameters below are deliberately modest so the test stays usable in
//! CI. Raise `ROUNDS` and `RESIDENT` when hunting a suspected leak: a real leak
//! grows with the round count, while normal churn does not.

use std::net::SocketAddr;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::net::{TcpStream, UdpSocket};

use realm_core::endpoint::{Endpoint, RemoteAddr};
use realm_core::lifecycle::{DesiredEndpoint, EndpointSource, EndpointSpec, ReconcileRequest, Reconciler};

mod common;
use common::{ask, ask_udp, free_addr, open_fds, spawn_echo, spawn_udp_echo};

/// reconcile generations per run
const ROUNDS: usize = 30;
/// endpoints churned in every generation
const CHURN: usize = 4;
/// long-lived connections kept open across every generation
const RESIDENT: usize = 5;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Spec {
    listen: String,
    remote: SocketAddr,
    udp: bool,
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
            tcp: !self.udp,
            udp: self.udp,
            // deleting must not wait 30s in a test
            drain: Some(realm_core::lifecycle::DrainPolicy {
                on_update: Some(Duration::from_millis(200)),
                on_delete: Some(Duration::from_millis(200)),
            }),
        })
    }
}

fn desired(id: &str, spec: Spec) -> DesiredEndpoint<Spec> {
    DesiredEndpoint {
        id: id.to_string(),
        spec,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn churn_with_resident_traffic_does_not_leak() {
    let tcp_echo = spawn_echo("v:").await;
    let udp_echo = spawn_udp_echo("v:").await;

    let resident_tcp = free_addr();
    let resident_udp = free_addr();
    let churn_addrs: Vec<SocketAddr> = (0..CHURN).map(|_| free_addr()).collect();

    let mut rec = Reconciler::new();

    // generation 1: the two endpoints that stay for the whole run
    let base = vec![
        desired(
            "resident-tcp",
            Spec {
                listen: resident_tcp.to_string(),
                remote: tcp_echo,
                udp: false,
            },
        ),
        desired(
            "resident-udp",
            Spec {
                listen: resident_udp.to_string(),
                remote: udp_echo,
                udp: true,
            },
        ),
    ];

    rec.reconcile(ReconcileRequest {
        generation: 1,
        endpoints: base.clone(),
    })
    .await
    .expect("the resident endpoints start");

    // long-lived traffic that must survive every generation
    let mut streams = Vec::new();
    for _ in 0..RESIDENT {
        let mut stream = TcpStream::connect(resident_tcp).await.unwrap();
        assert_eq!(ask(&mut stream, b"hello").await, "v:hello");
        streams.push(stream);
    }

    let mut clients = Vec::new();
    for _ in 0..RESIDENT {
        let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        assert_eq!(ask_udp(&client, resident_udp, b"hello").await, "v:hello");
        clients.push(client);
    }

    let baseline_fds = open_fds();

    // churn: every generation replaces the churned endpoints, and every other
    // generation removes them entirely
    for round in 0..ROUNDS {
        let generation = (round + 2) as u64;
        let mut endpoints = base.clone();

        if round % 2 == 0 {
            for (i, addr) in churn_addrs.iter().enumerate() {
                endpoints.push(desired(
                    &format!("churn-{}", i),
                    Spec {
                        listen: addr.to_string(),
                        remote: tcp_echo,
                        udp: false,
                    },
                ));
            }
        }

        let response = rec
            .reconcile(ReconcileRequest { generation, endpoints })
            .await
            .expect("every generation applies");

        assert!(
            response.results.iter().all(|r| r.error.is_none()),
            "round {} had failures: {:?}",
            round,
            response.results
        );

        // the resident traffic keeps working throughout
        for stream in streams.iter_mut() {
            assert_eq!(ask(stream, b"alive").await, "v:alive");
        }
        for client in clients.iter().take(1) {
            assert_eq!(ask_udp(client, resident_udp, b"alive").await, "v:alive");
        }
    }

    // resident endpoints were never rebuilt, so nothing is draining on them
    let status = rec.status();
    let resident = status
        .iter()
        .find(|e| e.id == "resident-tcp")
        .expect("the resident endpoint is still there");
    assert_eq!(resident.slots[0].connections, RESIDENT);
    assert!(
        resident.slots[0].draining.is_empty(),
        "an unchanged endpoint must never drain: {:?}",
        resident.slots[0].draining
    );

    // let every churned cohort finish
    tokio::time::sleep(Duration::from_millis(600)).await;
    let status = rec.status();
    let draining: usize = status
        .iter()
        .flat_map(|e| e.slots.iter())
        .map(|s| s.draining.len())
        .sum();
    assert_eq!(draining, 0, "every cohort of a churned generation is gone");

    let after_churn = open_fds();
    println!(
        "[stress] fds: baseline={} after {} generations={}",
        baseline_fds, ROUNDS, after_churn
    );

    // the churned endpoints are gone, so the descriptor count must be back at
    // the level the resident traffic needs, with slack for runtime internals
    #[cfg(target_os = "linux")]
    assert!(
        after_churn <= baseline_fds + 8,
        "descriptors accumulated across {} generations: {} -> {}",
        ROUNDS,
        baseline_fds,
        after_churn
    );

    // and the resident traffic still works after all of it
    for stream in streams.iter_mut() {
        assert_eq!(ask(stream, b"final").await, "v:final");
    }

    rec.shutdown().await;
    tokio::time::sleep(Duration::from_millis(400)).await;

    let after_shutdown = open_fds();
    println!("[stress] fds after shutdown={}", after_shutdown);
    #[cfg(target_os = "linux")]
    assert!(
        after_shutdown <= baseline_fds + 8,
        "descriptors leaked on shutdown: {} -> {}",
        baseline_fds,
        after_shutdown
    );
}
