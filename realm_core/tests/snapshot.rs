//! Last-known-good snapshot (U8 / R18, R19, R20, R33, R34).
//!
//! Realm keeps its own snapshot of the desired state it is serving, so a
//! restarted process resumes forwarding immediately instead of waiting for the
//! agent's next reconcile. The snapshot is written atomically, and restoring it
//! reuses the partially-applied semantics when some endpoint cannot come back.

use std::net::SocketAddr;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;

use realm_core::endpoint::{Endpoint, RemoteAddr};
use realm_core::lifecycle::{
    DesiredEndpoint, EndpointSource, EndpointSpec, ReconcileError, ReconcileRequest, Reconciler, Snapshot,
    SnapshotStore,
};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct TestSpec {
    listen: String,
    remote: SocketAddr,
}

impl EndpointSource for TestSpec {
    fn build(&self) -> Result<EndpointSpec, String> {
        let laddr: SocketAddr = self
            .listen
            .parse()
            .map_err(|e| format!("invalid `listen` = `{}`: {}", self.listen, e))?;

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

async fn spawn_echo(tag: &'static str) -> SocketAddr {
    let lis = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = lis.local_addr().unwrap();

    tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = lis.accept().await else {
                return;
            };
            tokio::spawn(async move {
                let mut buf = vec![0u8; 64];
                loop {
                    match stream.read(&mut buf).await {
                        Ok(0) | Err(_) => return,
                        Ok(n) => {
                            let mut answer = Vec::from(tag.as_bytes());
                            answer.extend_from_slice(&buf[..n]);
                            if stream.write_all(&answer).await.is_err() {
                                return;
                            }
                        }
                    }
                }
            });
        }
    });

    addr
}

fn free_addr() -> SocketAddr {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
}

async fn ask(stream: &mut TcpStream, payload: &[u8]) -> String {
    stream.write_all(payload).await.unwrap();
    let mut buf = vec![0u8; 128];
    let n = timeout(Duration::from_secs(2), stream.read(&mut buf))
        .await
        .expect("relay must answer in time")
        .expect("relay must stay readable");
    String::from_utf8_lossy(&buf[..n]).into_owned()
}

/// A private directory for one test's snapshot, removed on drop.
struct TempDir(std::path::PathBuf);

impl TempDir {
    fn new(name: &str) -> Self {
        let mut path = std::env::temp_dir();
        path.push(format!("realm-snapshot-{}-{}", name, std::process::id()));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }

    fn file(&self) -> std::path::PathBuf {
        self.0.join("state.json")
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn request(generation: u64, endpoints: &[(&str, TestSpec)]) -> ReconcileRequest<TestSpec> {
    ReconcileRequest {
        generation,
        endpoints: endpoints
            .iter()
            .map(|(id, spec)| DesiredEndpoint {
                id: (*id).to_string(),
                spec: spec.clone(),
            })
            .collect(),
    }
}

/// Covers AE9: a restarted process restores the snapshot and serves again at
/// the generation the snapshot carries.
#[tokio::test]
async fn a_restarted_process_restores_its_snapshot() {
    let dir = TempDir::new("restore");
    let echo = spawn_echo("v1:").await;
    let a = free_addr();
    let spec = TestSpec {
        listen: a.to_string(),
        remote: echo,
    };

    // first process: apply and shut down
    {
        let mut rec = Reconciler::with_snapshot(SnapshotStore::new(dir.file()));
        rec.restore().await.expect("empty snapshot restores");
        rec.reconcile(request(11, &[("a", spec.clone())])).await.unwrap();
        assert_eq!(rec.active_generation(), Some(11));
        // dropping the reconciler ends the process's endpoints
        rec.shutdown().await;
    }

    assert!(dir.file().exists(), "the snapshot was written");
    assert!(TcpListener::bind(a).await.is_ok(), "the old listener is gone");

    // second process: restore
    let mut rec = Reconciler::<TestSpec>::with_snapshot(SnapshotStore::new(dir.file()));
    let outcome = rec.restore().await.expect("snapshot restores");

    assert_eq!(outcome.generation, Some(11));
    assert_eq!(outcome.restored, 1);
    assert!(outcome.failed.is_empty());
    assert!(!outcome.partial);
    assert_eq!(rec.active_generation(), Some(11));

    let mut stream = TcpStream::connect(a).await.unwrap();
    assert_eq!(ask(&mut stream, b"x").await, "v1:x");
}

/// Covers AE13: submissions before the restore finished are refused with a
/// retryable not-ready error, and succeed afterwards.
#[tokio::test]
async fn submissions_before_the_restore_are_not_ready() {
    let dir = TempDir::new("not-ready");
    let echo = spawn_echo("v1:").await;
    let a = free_addr();
    let spec = TestSpec {
        listen: a.to_string(),
        remote: echo,
    };

    let mut rec = Reconciler::with_snapshot(SnapshotStore::new(dir.file()));

    let err = rec
        .reconcile(request(1, &[("a", spec.clone())]))
        .await
        .expect_err("submissions before the restore are refused");
    assert_eq!(err, ReconcileError::NotReady);
    assert!(err.is_retryable(), "not-ready is retryable");
    assert!(!rec.is_ready());

    rec.restore().await.expect("restore finishes");
    assert!(rec.is_ready());

    let response = rec.reconcile(request(1, &[("a", spec)])).await;
    assert!(response.is_ok(), "the retry succeeds once ready");
}

/// Covers R34: an endpoint that cannot come back is marked failed while the
/// rest is restored and the process stays alive, with a partial generation.
#[tokio::test]
async fn a_partial_restore_marks_the_failed_endpoint_only() {
    let dir = TempDir::new("partial");
    let echo = spawn_echo("v1:").await;
    let (good, blocked) = (free_addr(), free_addr());

    {
        let mut rec = Reconciler::with_snapshot(SnapshotStore::new(dir.file()));
        rec.restore().await.unwrap();
        rec.reconcile(request(
            5,
            &[
                (
                    "good",
                    TestSpec {
                        listen: good.to_string(),
                        remote: echo,
                    },
                ),
                (
                    "blocked",
                    TestSpec {
                        listen: blocked.to_string(),
                        remote: echo,
                    },
                ),
            ],
        ))
        .await
        .unwrap();
        rec.shutdown().await;
    }

    // somebody else took the address while realm was down
    let _squatter = TcpListener::bind(blocked).await.unwrap();

    let mut rec = Reconciler::<TestSpec>::with_snapshot(SnapshotStore::new(dir.file()));
    let outcome = rec.restore().await.expect("a partial restore is not an error");

    assert_eq!(outcome.generation, Some(5));
    assert_eq!(outcome.restored, 1);
    assert_eq!(outcome.failed.len(), 1);
    assert_eq!(outcome.failed[0].0, "blocked");
    assert!(outcome.partial);
    assert!(rec.is_partial(), "the active generation carries the partial mark");
    assert!(rec.is_ready(), "the process serves what it could restore");

    let mut stream = TcpStream::connect(good).await.unwrap();
    assert_eq!(ask(&mut stream, b"x").await, "v1:x");
}

/// Covers R20: the snapshot is replaced atomically, so a leftover temporary
/// file never shadows the last complete one.
#[test]
fn a_leftover_temporary_file_does_not_shadow_the_snapshot() {
    let dir = TempDir::new("atomic");
    let store = SnapshotStore::new(dir.file());

    let snapshot = Snapshot {
        generation: 3,
        partial: false,
        endpoints: [(
            "a".to_string(),
            TestSpec {
                listen: "127.0.0.1:1".into(),
                remote: "127.0.0.1:2".parse().unwrap(),
            },
        )]
        .into_iter()
        .collect(),
    };
    store.store(&snapshot).expect("snapshot is written");

    // a crash mid-write leaves a temporary file behind
    let leftover = dir.file().with_extension("json.tmp");
    std::fs::write(&leftover, b"{ truncated").unwrap();

    let loaded: Snapshot<TestSpec> = store
        .load()
        .expect("load must not fail because of a leftover")
        .expect("the previous complete snapshot is still there");
    assert_eq!(loaded.generation, 3);
    assert_eq!(loaded.endpoints.len(), 1);
}

/// A corrupt snapshot is reported, never a panic and never a silent wipe.
#[test]
fn a_corrupt_snapshot_is_an_error() {
    let dir = TempDir::new("corrupt");
    std::fs::write(dir.file(), b"{ this is not a snapshot").unwrap();

    let store = SnapshotStore::new(dir.file());
    let loaded = store.load::<TestSpec>();
    assert!(loaded.is_err(), "a corrupt snapshot must be reported");
}

/// The snapshot follows the applied desired state, including deletions.
#[tokio::test]
async fn the_snapshot_tracks_the_applied_state() {
    let dir = TempDir::new("tracks");
    let echo = spawn_echo("v1:").await;
    let a = free_addr();

    let mut rec = Reconciler::with_snapshot(SnapshotStore::new(dir.file()));
    rec.restore().await.unwrap();

    rec.reconcile(request(
        1,
        &[(
            "a",
            TestSpec {
                listen: a.to_string(),
                remote: echo,
            },
        )],
    ))
    .await
    .unwrap();

    let store = SnapshotStore::new(dir.file());
    let loaded: Snapshot<TestSpec> = store.load().unwrap().unwrap();
    assert_eq!(loaded.generation, 1);
    assert_eq!(loaded.endpoints.len(), 1);

    rec.reconcile(request(2, &[])).await.unwrap();

    let loaded: Snapshot<TestSpec> = store.load().unwrap().unwrap();
    assert_eq!(loaded.generation, 2);
    assert!(loaded.endpoints.is_empty(), "a deletion is recorded too");

    rec.shutdown().await;
}
