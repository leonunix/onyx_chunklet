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
    SUBMIT_OPS,
    SUBMIT_SQES,
    SUBMIT_BOUNCE_BYTES,
    SUBMIT_BOUNCE_NS,
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
        now.saturating_duration_since(start).as_nanos().min(u64::MAX as u128) as u64,
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
    /// Phase 2 P/Q recompute.
    pub r6_compute_ns: u64,
    /// Phase 3 submit + degrade absorption.
    pub r6_write_ns: u64,
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
    /// Per-strip ops handed in, before adjacency merging.
    pub ops: u64,
    /// SQEs actually pushed. `ops / sqes` is the merge factor; `sqes / waves`
    /// is how wide each barrier actually is.
    pub sqes: u64,
    /// Bytes memcpy'd into bounce buffers to merge adjacent strips.
    pub bounce_bytes: u64,
    pub bounce_ns: u64,
    /// Push + `io_uring_enter` + CQ drain, summed over waves. With
    /// `uring_coalesced_wait = false` this includes one enter per completion.
    pub wait_ns: u64,
}

pub fn stats() -> WritePathStats {
    let g = |c: &AtomicU64| c.load(Ordering::Relaxed);
    WritePathStats {
        r6_batch_calls: g(&R6_BATCH_CALLS),
        r6_batch_ops: g(&R6_BATCH_OPS),
        r6_batch_stripes: g(&R6_BATCH_STRIPES),
        r6_batch_serial_bails: g(&R6_BATCH_SERIAL_BAILS),
        r6_plan_ns: g(&R6_PLAN_NS),
        r6_lock_ns: g(&R6_LOCK_NS),
        r6_read_ns: g(&R6_READ_NS),
        r6_compute_ns: g(&R6_COMPUTE_NS),
        r6_write_ns: g(&R6_WRITE_NS),
        r6_total_ns: g(&R6_TOTAL_NS),
        r6_total_ns_max: g(&R6_TOTAL_NS_MAX),
        submit: std::array::from_fn(|i| SubmitClassStats {
            calls: g(&SUBMIT_CALLS[i]),
            waves: g(&SUBMIT_WAVES[i]),
            ops: g(&SUBMIT_OPS[i]),
            sqes: g(&SUBMIT_SQES[i]),
            bounce_bytes: g(&SUBMIT_BOUNCE_BYTES[i]),
            bounce_ns: g(&SUBMIT_BOUNCE_NS[i]),
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
    fn record_since_accumulates_and_returns_the_stop_instant() {
        let c = AtomicU64::new(0);
        let start = Instant::now();
        let mid = record_since(&c, start);
        let first = c.load(Ordering::Relaxed);
        record_since(&c, mid);
        assert!(c.load(Ordering::Relaxed) >= first);
    }
}
