//! Test-only `IoBackend` wrapper that injects write failures targeting a
//! specific PD, used by `--ignored` fault-injection tests to exercise
//! partial-failure / crash-mid-write code paths without an actual disk
//! error.
//!
//! # How it works
//!
//! `FaultInjectingBackend` wraps another `IoBackend` (typically the pool's
//! existing `SyncBackend`). On each `submit_writes` call it splits the ops
//! into "target_pd ops" and "other ops":
//!
//! - Other ops are forwarded to the inner backend immediately, so they
//!   succeed on disk like normal.
//! - Target ops are either forwarded to inner (the first `succeed_first`
//!   times this backend is invoked with target ops) or short-circuited to
//!   an `Io` error (every subsequent invocation).
//!
//! Net effect: a single `parallel_strip_writes` call where one of K copies
//! lives on `target_pd` ends up with K-1 copies durably written and 1
//! copy returning an error — the same shape as a real per-PD disk error
//! mid-batch.
//!
//! # Installation
//!
//! ```text
//! let pd = pool.pd(target_pd_id).unwrap();
//! let inner = pd.backend();
//! pd.set_backend(Arc::new(FaultInjectingBackend::new(inner, target_pd_id, 0)));
//! ```
//!
//! Since the wrapper is installed on the TARGET PD only, batches that
//! happen to fan out via a different PD's backend (`StripWrite[0].pd`
//! decides which backend is used by `submit_strip_writes`) won't see the
//! injection. For mirror / R5 / R6 writes the destination PDs all share
//! a backend Arc, so installing on any one of them — or any healthy PD
//! that participates — works.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use onyx_chunklet::error::ChunkletError;
use onyx_chunklet::io::{IoBackend, StripWrite};
use onyx_chunklet::types::PdId;

pub struct FaultInjectingBackend {
    inner: Arc<dyn IoBackend>,
    target_pd: PdId,
    succeed_remaining: AtomicUsize,
    failed_count: AtomicUsize,
}

impl FaultInjectingBackend {
    /// Wrap `inner`. The next `succeed_first` invocations that touch
    /// `target_pd` succeed via `inner`; every subsequent invocation
    /// fails with an `Io` error. Non-target ops always go through.
    pub fn new(inner: Arc<dyn IoBackend>, target_pd: PdId, succeed_first: usize) -> Self {
        Self {
            inner,
            target_pd,
            succeed_remaining: AtomicUsize::new(succeed_first),
            failed_count: AtomicUsize::new(0),
        }
    }

    /// How many target-PD-touching submissions have been failed by this
    /// wrapper. Lets tests assert "fault was actually exercised".
    pub fn failed_count(&self) -> usize {
        self.failed_count.load(Ordering::Relaxed)
    }
}

fn clone_op<'a>(o: &StripWrite<'a>) -> StripWrite<'a> {
    StripWrite {
        pd: o.pd.clone(),
        chunklet_index: o.chunklet_index,
        in_chunklet_off: o.in_chunklet_off,
        data: o.data,
    }
}

impl IoBackend for FaultInjectingBackend {
    fn name(&self) -> &'static str {
        "fault-injecting"
    }

    fn submit_writes(&self, ops: &[StripWrite<'_>]) -> Result<(), ChunkletError> {
        let other: Vec<StripWrite<'_>> = ops
            .iter()
            .filter(|o| o.pd.pd_id() != self.target_pd)
            .map(clone_op)
            .collect();
        let target: Vec<StripWrite<'_>> = ops
            .iter()
            .filter(|o| o.pd.pd_id() == self.target_pd)
            .map(clone_op)
            .collect();

        // Always issue non-target writes first so they land on disk even
        // when we're about to inject a failure on the target. Mirrors a
        // real K-of-N partial-failure shape.
        if !other.is_empty() {
            self.inner.submit_writes(&other)?;
        }

        if target.is_empty() {
            return Ok(());
        }
        let prev = self.succeed_remaining.fetch_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |v| if v == 0 { None } else { Some(v - 1) },
        );
        match prev {
            Ok(_remaining_before_decrement) => {
                // We had budget; let target writes through.
                self.inner.submit_writes(&target)
            }
            Err(_) => {
                self.failed_count.fetch_add(1, Ordering::Relaxed);
                Err(ChunkletError::Io(std::io::Error::other(format!(
                    "fault inject: write to PD {} failed",
                    self.target_pd
                ))))
            }
        }
    }
}
