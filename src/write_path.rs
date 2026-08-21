//! Runtime attribution for the batched write path (RAID6 flusher hot path +
//! the io_uring submit waves underneath it).
//!
//! Every counter is a free-running `Relaxed` atomic written from whichever
//! caller thread (or execution worker) happens to run the batch, and read by
//! differencing two snapshots — the same shape as [`crate::ld::gf256`]'s SIMD
//! ledger. Nothing here is load-bearing for correctness, so a torn read across
//! two counters only skews one interval's attribution.
//!
//! It exists because `write_many_at`'s wall clock was the only number the caller
//! could see: onyx measured 6.13-6.31 ms per LV3 batch with the disks nowhere
//! near saturated, and had no way to say whether that was stripe-lock queueing,
//! P/Q compute, or the submit waves. The phase split below is exactly that
//! discrimination, so keep the phases MUTUALLY EXCLUSIVE and covering — a
//! `plan + lock + read + compute + write` sum that drifts away from the caller's
//! own timer means a new cost centre appeared that nothing measures.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use crate::io::scheduler::IoClass;

macro_rules! counters {
    ($($name:ident),* $(,)?) => {
        $(pub(crate) static $name: AtomicU64 = AtomicU64::new(0);)*
    };
}

// `RuntimeLogicalDisk::write_many_at` — the wrapper the caller actually enters,
// OUTSIDE the RAID6 ledger below. It was instrumented only as a `tracing::warn!`,
// so on the box it showed up as 23.84 of 54.22 ms per LV3 call (44 %) that no
// counter could attribute. Its own range-lock batch is a second footprint on top
// of `R6_LOCK_NS`, which is exactly what needed measuring.
counters!(
    RT_WRITE_MANY_CALLS,
    RT_WRITE_MANY_KEYS,
    RT_LIFECYCLE_NS,
    RT_KEY_BUILD_NS,
    RT_RANGE_LOCK_NS,
    RT_INNER_NS,
    RT_TOTAL_NS,
);

counters!(
    R6_BATCH_CALLS,
    R6_BATCH_OPS,
    R6_BATCH_STRIPES,
    R6_BATCH_SERIAL_BAILS,
    R6_PLAN_NS,
    R6_LOCK_NS,
    R6_READ_NS,
    R6_COMPUTE_NS,
    R6_WRITE_NS,
    R6_PIPELINE_NS,
    R6_TOTAL_NS,
    R6_TOTAL_NS_MAX,
);

/// Submit-side counters are per [`IoClass`]: LV3 (`DrainData`), LV2
/// (`Foreground`) and metadb (`DrainMeta`) all share one backend, and a metadb
/// page write is two orders of magnitude smaller than an LV3 stripe batch. A
/// single global set mixes them and reports a merge factor and wave width that
/// belong to no actual caller.
macro_rules! class_counters {
    ($($name:ident),* $(,)?) => {
        $(pub(crate) static $name: [AtomicU64; IoClass::ALL.len()] =
            [AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0)];)*
    };
}

class_counters!(
    SUBMIT_CALLS,
    SUBMIT_WAVES,
    SUBMIT_PUSHES,
    SUBMIT_ENTERS,
    SUBMIT_OPS,
    SUBMIT_SQES,
    SUBMIT_BOUNCE_BYTES,
    SUBMIT_BOUNCE_ALLOCS,
    SUBMIT_GROUP_NS,
    SUBMIT_COPY_NS,
    SUBMIT_BUILD_NS,
    SUBMIT_WAIT_NS,
);

/// Index of the class the calling thread is currently submitting under.
pub(crate) fn class_slot() -> usize {
    crate::io::scheduler::current_io_class() as usize
}

/// Add `start.elapsed()` to `counter`. Returns the instant it stopped at so a
/// caller can chain phases without a second clock read per boundary.
pub(crate) fn record_since(counter: &AtomicU64, start: Instant) -> Instant {
    let now = Instant::now();
    counter.fetch_add(
        now.saturating_duration_since(start)
            .as_nanos()
            .min(u64::MAX as u128) as u64,
        Ordering::Relaxed,
    );
    now
}

pub(crate) fn add(counter: &AtomicU64, value: u64) {
    counter.fetch_add(value, Ordering::Relaxed);
}

pub(crate) fn record_max(counter: &AtomicU64, value: u64) {
    let mut seen = counter.load(Ordering::Relaxed);
    while value > seen {
        match counter.compare_exchange_weak(seen, value, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => return,
            Err(current) => seen = current,
        }
    }
}

/// One read of the batched-write ledger. All `_ns` fields are sums over the
/// interval, so difference two snapshots and divide by the call/wave count.
#[derive(Clone, Copy, Debug, Default)]
pub struct WritePathStats {
    /// `RuntimeLogicalDisk::write_many_at` entries (ALL raid levels — this is the
    /// wrapper, above the per-level ledger).
    pub rt_write_many_calls: u64,
    /// Range-lock keys those calls built. Keyed per `max(strip, block)` unit, so
    /// one 24 KiB op contributes 6 keys at a 4 KiB strip — `keys / calls` is the
    /// outer lock footprint before grouping dedups it.
    pub rt_write_many_keys: u64,
    /// `io_lock.read()` — waiting out a rebuild/drain/drop that holds it for write.
    pub rt_lifecycle_ns: u64,
    pub rt_key_build_ns: u64,
    /// `range_locks.write_keys` — the SECOND lock footprint, on top of
    /// `r6_lock_ns`. Held across the whole inner call.
    pub rt_range_lock_ns: u64,
    /// The inner per-level `write_many_at`, i.e. what `r6_*` below measures.
    pub rt_inner_ns: u64,
    /// Whole wrapper call. `rt_total - rt_inner` is what the RAID6 ledger cannot
    /// see, and the four legs above must cover it.
    pub rt_total_ns: u64,
    /// `write_many_at` entries that took the batched RAID6 path.
    pub r6_batch_calls: u64,
    /// Caller-supplied ops (one per LD-level write) across those batches.
    pub r6_batch_ops: u64,
    /// Physical stripes those ops decomposed into after same-stripe merging.
    pub r6_batch_stripes: u64,
    /// Batches that bailed to the serial `write_at` loop (degraded set, set
    /// under rebuild, byte-overlapping stripe, or a Phase-1 read fault). A
    /// non-zero rate here invalidates the phase split for those batches.
    pub r6_batch_serial_bails: u64,
    /// Phase 0/0b: decompose + classify + scratch allocation.
    pub r6_plan_ns: u64,
    /// `stripe_locks.write_keys` — queueing behind another batch or a reader on
    /// the same stripe buckets.
    pub r6_lock_ns: u64,
    /// Phase 1 RMW reads (zero for a clean full-stripe batch).
    pub r6_read_ns: u64,
    /// Phase 2 P/Q recompute. ⚠ When `r6_pipeline_ns` is non-zero this is a
    /// SUB-interval of `r6_write_ns`, not disjoint from it: the pipelined writer
    /// computes a segment's parity inside the submit leg so it overlaps device
    /// time. `compute / write` is then the fraction of the submit leg that is
    /// still CPU, which is the number the pipeline is trying to drive down.
    pub r6_compute_ns: u64,
    /// Phase 3 submit + degrade absorption. Includes the interleaved compute
    /// when the pipelined writer is in use.
    pub r6_write_ns: u64,
    /// Non-zero only when the pipelined (compute-interleaved-with-submit)
    /// writer ran, and then equal to its share of `r6_write_ns`. Exists so a
    /// snapshot says WHICH writer produced it — the phase split means something
    /// different in each mode.
    pub r6_pipeline_ns: u64,
    /// Whole batched call, for the covering check against the phases above.
    pub r6_total_ns: u64,
    pub r6_total_ns_max: u64,
    /// Submit ledger per `IoClass`, indexed by `IoClass as usize`
    /// (0 Foreground = LV2, 1 DrainData = LV3, 2 DrainMeta = metadb,
    /// 3 Maintenance = rebuild/rebalance).
    pub submit: [SubmitClassStats; IoClass::ALL.len()],
}

/// One `IoClass`'s slice of the submit ledger.
#[derive(Clone, Copy, Debug, Default)]
pub struct SubmitClassStats {
    /// `submit_writes_detailed` entries (LD batches reaching the ring).
    pub calls: u64,
    /// Stop-and-wait waves those calls were split into. `waves / calls` is the
    /// barrier count per batch, driven by `uring_write_chunk_ops`.
    pub waves: u64,
    /// SQ top-ups. Without windowing this equals `waves` (one atomic push per
    /// stop-and-wait wave). With `uring_write_window_sqes` set it is how many
    /// times the loop refilled the ring while earlier SQEs were still in flight,
    /// so `pushes / waves` is the pipeline depth the window actually achieved.
    /// Reported separately from `waves` so a window arm stays comparable to a
    /// barrier arm on the same counter.
    pub pushes: u64,
    /// `io_uring_enter` calls made while waiting those waves out. This is the
    /// direct read on `uring_coalesced_wait`: with it off, `enters` tracks `sqes`
    /// (one wake per staggered NVMe completion); with it on, `enters` should fall
    /// to roughly `waves`. Separating it from `wait_ns` is what distinguishes
    /// "the syscalls went away" from "the device got faster".
    pub enters: u64,
    /// Per-strip ops handed in, before adjacency merging.
    pub ops: u64,
    /// SQEs actually pushed. `ops / sqes` is the merge factor; `sqes / waves`
    /// is how wide each barrier actually is.
    pub sqes: u64,
    /// Bytes memcpy'd into bounce buffers to merge adjacent strips.
    pub bounce_bytes: u64,
    /// Bounce buffers allocated. `copy_ns / bounce_allocs` separates "the copy is
    /// slow" from "the allocation is slow" — the 2026-08-02 box read could not,
    /// and 205 µs/call on the LV2 class was ~10x what a linear copy of
    /// `bounce_bytes` can explain.
    pub bounce_allocs: u64,
    /// Adjacency grouping only (`coalesced_write_groups`).
    pub group_ns: u64,
    /// Bounce buffer allocation + fill + the strip copies into it.
    pub copy_ns: u64,
    /// Building the per-SQE descriptors handed to the ring.
    pub build_ns: u64,
    /// Push + `io_uring_enter` + CQ drain, summed over waves. With
    /// `uring_coalesced_wait = false` this includes one enter per completion.
    pub wait_ns: u64,
}

impl SubmitClassStats {
    /// The whole pre-submit stage. Was one opaque `bounce_ns` counter until
    /// 2026-08-02; kept as a derived sum so existing readers and the box
    /// baselines stay comparable. The three parts are mutually exclusive and
    /// cover the stage, so a caller-side residual means a new cost centre.
    pub fn bounce_ns(&self) -> u64 {
        self.group_ns
            .saturating_add(self.copy_ns)
            .saturating_add(self.build_ns)
    }
}

pub fn stats() -> WritePathStats {
    let g = |c: &AtomicU64| c.load(Ordering::Relaxed);
    WritePathStats {
        rt_write_many_calls: g(&RT_WRITE_MANY_CALLS),
        rt_write_many_keys: g(&RT_WRITE_MANY_KEYS),
        rt_lifecycle_ns: g(&RT_LIFECYCLE_NS),
        rt_key_build_ns: g(&RT_KEY_BUILD_NS),
        rt_range_lock_ns: g(&RT_RANGE_LOCK_NS),
        rt_inner_ns: g(&RT_INNER_NS),
        rt_total_ns: g(&RT_TOTAL_NS),
        r6_batch_calls: g(&R6_BATCH_CALLS),
        r6_batch_ops: g(&R6_BATCH_OPS),
        r6_batch_stripes: g(&R6_BATCH_STRIPES),
        r6_batch_serial_bails: g(&R6_BATCH_SERIAL_BAILS),
        r6_plan_ns: g(&R6_PLAN_NS),
        r6_lock_ns: g(&R6_LOCK_NS),
        r6_read_ns: g(&R6_READ_NS),
        r6_compute_ns: g(&R6_COMPUTE_NS),
        r6_write_ns: g(&R6_WRITE_NS),
        r6_pipeline_ns: g(&R6_PIPELINE_NS),
        r6_total_ns: g(&R6_TOTAL_NS),
        r6_total_ns_max: g(&R6_TOTAL_NS_MAX),
        submit: std::array::from_fn(|i| SubmitClassStats {
            calls: g(&SUBMIT_CALLS[i]),
            waves: g(&SUBMIT_WAVES[i]),
            pushes: g(&SUBMIT_PUSHES[i]),
            enters: g(&SUBMIT_ENTERS[i]),
            ops: g(&SUBMIT_OPS[i]),
            sqes: g(&SUBMIT_SQES[i]),
            bounce_bytes: g(&SUBMIT_BOUNCE_BYTES[i]),
            bounce_allocs: g(&SUBMIT_BOUNCE_ALLOCS[i]),
            group_ns: g(&SUBMIT_GROUP_NS[i]),
            copy_ns: g(&SUBMIT_COPY_NS[i]),
            build_ns: g(&SUBMIT_BUILD_NS[i]),
            wait_ns: g(&SUBMIT_WAIT_NS[i]),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_max_keeps_the_high_water_mark() {
        let c = AtomicU64::new(0);
        record_max(&c, 5);
        record_max(&c, 3);
        record_max(&c, 9);
        assert_eq!(c.load(Ordering::Relaxed), 9);
    }

    #[test]
    fn bounce_ns_is_the_sum_of_the_three_stage_parts() {
        let s = SubmitClassStats {
            group_ns: 7,
            copy_ns: 11,
            build_ns: 13,
            ..Default::default()
        };
        assert_eq!(s.bounce_ns(), 31);
    }

    #[test]
    fn record_since_accumulates_and_returns_the_stop_instant() {
        let c = AtomicU64::new(0);
        let start = Instant::now();
        let mid = record_since(&c, start);
        let first = c.load(Ordering::Relaxed);
        record_since(&c, mid);
        assert!(c.load(Ordering::Relaxed) >= first);
    }
}
