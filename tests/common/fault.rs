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

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use onyx_chunklet::error::{ChunkletError, ChunkletResult};
use onyx_chunklet::io::{IoBackend, StripRead, StripWrite};
use onyx_chunklet::types::PdId;

pub struct FaultInjectingBackend {
    inner: Arc<dyn IoBackend>,
    targets: BTreeSet<PdId>,
    succeed_remaining: AtomicUsize,
    failed_count: AtomicUsize,
}

impl FaultInjectingBackend {
    /// Wrap `inner`. The next `succeed_first` invocations that touch
    /// `target_pd` succeed via `inner`; every subsequent invocation
    /// fails writes to `target_pd` with an `Io` error. Non-target ops
    /// always go through.
    pub fn new(inner: Arc<dyn IoBackend>, target_pd: PdId, succeed_first: usize) -> Self {
        Self::new_multi(inner, [target_pd].into_iter().collect(), succeed_first)
    }

    /// Like [`Self::new`] but fails writes to ANY PD in `targets` — used to
    /// exercise multi-member failure (e.g. an R6 F=2 case, or a mirror with all
    /// copies down) in one invocation.
    pub fn new_multi(
        inner: Arc<dyn IoBackend>,
        targets: BTreeSet<PdId>,
        succeed_first: usize,
    ) -> Self {
        Self {
            inner,
            targets,
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

    fn submit_reads(&self, ops: &mut [StripRead<'_>]) -> Result<(), ChunkletError> {
        self.inner.submit_reads(ops)
    }

    /// Per-op results in INPUT order (the primitive the inline-degrade LD write
    /// paths consume). Non-target ops go to the inner backend and get its real
    /// per-op result; target ops fail with an injected `Io` error once the
    /// success budget is exhausted (so a K-of-N batch lands K-1 durable copies
    /// and one injected error — the real partial-failure shape). The historical
    /// first-error `submit_writes` is the trait default over this.
    fn submit_writes_detailed(&self, ops: &[StripWrite<'_>]) -> Vec<ChunkletResult<()>> {
        let has_target = ops.iter().any(|o| self.targets.contains(&o.pd.pd_id()));
        // Consume one budget unit per invocation that touches a target; once
        // exhausted, this invocation fails the target ops.
        let fail_target = has_target
            && self
                .succeed_remaining
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                    if v == 0 {
                        None
                    } else {
                        Some(v - 1)
                    }
                })
                .is_err();
        if fail_target {
            self.failed_count.fetch_add(1, Ordering::Relaxed);
        }

        // Route everything except failed-target ops through the inner backend as
        // ONE batch (so surviving copies land durably), then scatter the inner
        // results + injected errors back to the caller's input positions.
        let mut inner_ops: Vec<StripWrite<'_>> = Vec::with_capacity(ops.len());
        let mut inner_idx: Vec<usize> = Vec::with_capacity(ops.len());
        let mut results: Vec<Option<ChunkletResult<()>>> = (0..ops.len()).map(|_| None).collect();
        for (i, o) in ops.iter().enumerate() {
            if fail_target && self.targets.contains(&o.pd.pd_id()) {
                results[i] = Some(Err(ChunkletError::Io(std::io::Error::other(format!(
                    "fault inject: write to PD {} failed",
                    o.pd.pd_id()
                )))));
            } else {
                inner_ops.push(clone_op(o));
                inner_idx.push(i);
            }
        }
        let inner_results = self.inner.submit_writes_detailed(&inner_ops);
        for (k, r) in inner_results.into_iter().enumerate() {
            results[inner_idx[k]] = Some(r);
        }
        results
            .into_iter()
            .map(|o| o.expect("every op position filled"))
            .collect()
    }
}
