//! Connection cohorts.
//!
//! A *cohort* is the set of connections (or udp associations) that a single
//! generation of an endpoint's configuration is serving. Every connection task
//! holds an owned handle into its cohort, so the endpoint can always answer:
//!
//! - how many connections of that generation are still alive,
//! - how long the cohort has been draining,
//! - and, when the drain deadline expires, terminate them deterministically.
//!
//! Termination goes through a [`CancellationToken`] plus a join/drain
//! confirmation — never abort-and-forget. Tasks are cancelled at an await
//! point they own, and the cohort is only reported as drained once every
//! registered connection has actually released its handle.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

struct Inner {
    /// broadcast cancellation for every connection of this cohort
    token: CancellationToken,
    /// number of connections currently registered
    count: AtomicUsize,
    /// when the cohort was created
    created: Instant,
    /// when the cohort stopped accepting new connections, if it has
    draining_since: std::sync::Mutex<Option<Instant>>,
}

/// Owner side of a cohort: created together with the endpoint generation that
/// serves it, and kept by the endpoint state machine.
pub struct Cohort {
    inner: Arc<Inner>,
    /// dropped before draining so that `done` completes once the last
    /// connection guard is gone
    alive: Option<mpsc::Sender<()>>,
    done: mpsc::Receiver<()>,
}

/// Registration side of a cohort, handed to the accept loop.
#[derive(Clone)]
pub struct CohortHandle {
    inner: Arc<Inner>,
    alive: mpsc::Sender<()>,
}

/// Held by one connection task for as long as it runs.
pub struct ConnGuard {
    inner: Arc<Inner>,
    token: CancellationToken,
    _alive: mpsc::Sender<()>,
}

impl Default for Cohort {
    fn default() -> Self {
        Self::new()
    }
}

impl Cohort {
    pub fn new() -> Self {
        let (alive, done) = mpsc::channel(1);
        Self {
            inner: Arc::new(Inner {
                token: CancellationToken::new(),
                count: AtomicUsize::new(0),
                created: Instant::now(),
                draining_since: std::sync::Mutex::new(None),
            }),
            alive: Some(alive),
            done,
        }
    }

    /// Handle used to register connections.
    pub fn handle(&self) -> CohortHandle {
        CohortHandle {
            inner: Arc::clone(&self.inner),
            // `alive` is only taken when the cohort starts draining, at which
            // point no new connection may be registered anyway
            alive: self.alive.clone().unwrap_or_else(|| {
                let (tx, _rx) = mpsc::channel(1);
                tx
            }),
        }
    }

    /// Number of connections currently alive in this cohort.
    pub fn count(&self) -> usize {
        self.inner.count.load(Ordering::Acquire)
    }

    /// How long this cohort has existed.
    pub fn age(&self) -> Duration {
        self.inner.created.elapsed()
    }

    /// How long this cohort has been draining, if it is.
    pub fn draining_for(&self) -> Option<Duration> {
        self.inner
            .draining_since
            .lock()
            .ok()
            .and_then(|x| *x)
            .map(|since| since.elapsed())
    }

    /// Mark the cohort as draining: no new connection is registered from now
    /// on, and existing connections are left to finish naturally.
    pub fn start_draining(&mut self) {
        if self.alive.take().is_some() {
            if let Ok(mut slot) = self.inner.draining_since.lock() {
                slot.get_or_insert_with(Instant::now);
            }
        }
    }

    /// Terminate every connection of this cohort at its next await point.
    pub fn cancel(&mut self) {
        self.start_draining();
        self.inner.token.cancel();
    }

    /// Wait until every connection has released its guard.
    ///
    /// Implies [`Cohort::start_draining`]; without it the cohort's own handle
    /// would keep the wait alive forever.
    pub async fn wait_drained(&mut self) {
        self.start_draining();
        // resolves once every sender (the cohort's own and every guard's) is
        // dropped; senders are never used to send, only to be held
        while self.done.recv().await.is_some() {}
    }

    /// Drain with a deadline: wait up to `timeout` for connections to finish
    /// naturally, then cancel the rest and wait for them to actually exit.
    ///
    /// `None` waits indefinitely — the default for a configuration change,
    /// which must not disturb established connections.
    pub async fn drain(&mut self, timeout: Option<Duration>) -> DrainOutcome {
        self.start_draining();

        let Some(timeout) = timeout else {
            self.wait_drained().await;
            return DrainOutcome::Finished;
        };

        if tokio::time::timeout(timeout, self.wait_drained()).await.is_ok() {
            return DrainOutcome::Finished;
        }

        let forced = self.count();
        self.cancel();
        self.wait_drained().await;
        DrainOutcome::Forced(forced)
    }
}

/// How a cohort reached the drained state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DrainOutcome {
    /// every connection ended on its own
    Finished,
    /// the deadline expired and this many connections were terminated
    Forced(usize),
}

impl CohortHandle {
    /// Register a connection. The returned guard must be held for the whole
    /// lifetime of the connection task.
    pub fn register(&self) -> ConnGuard {
        self.inner.count.fetch_add(1, Ordering::AcqRel);
        ConnGuard {
            inner: Arc::clone(&self.inner),
            token: self.inner.token.child_token(),
            _alive: self.alive.clone(),
        }
    }

    /// Cancellation token of the whole cohort.
    pub fn token(&self) -> &CancellationToken {
        &self.inner.token
    }

    /// Number of connections currently alive in this cohort.
    pub fn count(&self) -> usize {
        self.inner.count.load(Ordering::Acquire)
    }
}

impl ConnGuard {
    /// Cancellation token of this connection: fires when the connection is
    /// cancelled individually or the whole cohort is.
    pub fn token(&self) -> &CancellationToken {
        &self.token
    }
}

impl Drop for ConnGuard {
    fn drop(&mut self) {
        self.inner.count.fetch_sub(1, Ordering::AcqRel);
    }
}
