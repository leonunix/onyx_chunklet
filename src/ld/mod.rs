//! Logical disk layer.
//!
//! An `LogicalDisk` is a linear virtual block device built from one or more
//! chunklets. Each variant (Plain, Mirror, Raid5, Raid6) implements the trait
//! with its own encoding / striping / parity logic but exposes the same
//! `read_at` / `write_at` shape to upstream callers.
//!
//! # Concurrency
//!
//! Each LD is wrapped in `RwLock<LdState>`:
//! - `read_at` / `write_at` take `read()` so multiple stripes / chunklet IOs
//!   can run in parallel.
//! - `rebuild` / `drop` (Phase 5+) take `write()` to ensure no in-flight IO
//!   races with member-set mutations.

pub mod degrade;
pub mod descriptor;
pub mod gf256;
pub mod mirror;
pub mod parity;
pub mod plain;
pub mod raid0;
pub mod raid5;
pub mod raid6;

pub use descriptor::{LdDescriptor, LdList};
pub use mirror::LdMirror;
pub use plain::LdPlain;
pub use raid0::LdRaid0;
pub use raid5::LdRaid5;
pub use raid6::LdRaid6;

use std::sync::Arc;

use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::error::{ChunkletError, ChunkletResult};
use crate::pd::PhysicalDisk;
use crate::types::{LdId, BLOCK_SIZE};

/// Hashed stripe locks shared by foreground IO, rebuild, and checkpoint page
/// writes. A 4 MiB batch contains 1024 distinct 4 KiB stripes; with the old
/// 1024-bucket table that batch locked every bucket and serialized all otherwise
/// disjoint callers. 64K keeps collision probability low for bounded batches
/// while adding only a few MiB across the live LD/runtime lock tables.
const STRIPE_LOCK_BUCKETS: usize = 64 * 1024;
// `StripeLockTable::bucket` derives its shift from `trailing_zeros`.
const _: () = assert!(STRIPE_LOCK_BUCKETS.is_power_of_two());

// `StripWrite` and the cross-PD batched-write submission helper live in
// `src/io/backend.rs` now (selectable between SyncBackend and
// UringBackend per Pool). Re-export so existing `use crate::ld::{...,
// StripWrite}` import sites keep working.
pub(crate) use crate::io::backend::{
    submit_strip_reads as parallel_strip_reads, submit_strip_writes_detailed, StripRead, StripWrite,
};

pub fn healthy_pd_map(
    pds: &std::collections::BTreeMap<crate::types::PdId, Arc<PhysicalDisk>>,
) -> std::collections::BTreeMap<crate::types::PdId, crate::pool::PdHealth> {
    pds.keys()
        .map(|pd| (*pd, crate::pool::PdHealth::Healthy))
        .collect()
}

/// Minimal per-member reconstruct surface the online-rebuild Phase B backfill
/// needs, so it can drive Mirror / Raid5 / Raid6 through one generic loop
/// (`Box<dyn ReconstructEngine>`). Each redundant LD already has these inherent
/// methods; the impls just delegate.
pub(crate) trait ReconstructEngine: Send + Sync {
    fn strip_bytes(&self) -> u64;
    fn stripes_per_chunklet(&self) -> u64;
    fn reconstruct_member_strip(
        &self,
        failed_member_idx: usize,
        in_chunklet_off: u64,
        out: &mut [u8],
    ) -> ChunkletResult<()>;
}

/// Public interface every LD implementation exposes.
pub trait LogicalDisk: Send + Sync {
    fn id(&self) -> LdId;

    /// Total user-addressable bytes on this LD (excludes per-chunklet headers,
    /// parity overhead, etc.).
    fn capacity_bytes(&self) -> u64;

    /// Block size for reads/writes; always 4 KiB for now.
    fn block_size(&self) -> usize;

    /// RAID strip size (bytes). Upstream packers should align writes to
    /// multiples of `strip_size` to hit the full-stripe fast path.
    /// For `LdPlain` this is the PD block size — there is no parity penalty.
    fn strip_size(&self) -> usize;

    /// Read exactly `buf.len()` bytes from `offset`. `offset` and `buf.len()`
    /// must be `block_size()`-aligned.
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> ChunkletResult<()>;

    /// Read multiple independent aligned buffers. Default fallback is a
    /// simple loop; LDs/backends with real batching can override it.
    fn read_many_at(&self, ops: &mut [(u64, &mut [u8])]) -> ChunkletResult<()> {
        for (offset, buf) in ops {
            self.read_at(*offset, buf)?;
        }
        Ok(())
    }

    /// Write exactly `buf.len()` bytes at `offset`. Same alignment rules.
    fn write_at(&self, offset: u64, buf: &[u8]) -> ChunkletResult<()>;

    /// Write multiple independent aligned buffers. Default fallback is a
    /// simple loop; mirror/striped LDs can override to fan out one backend
    /// batch across many user writes.
    fn write_many_at(&self, ops: &[(u64, &[u8])]) -> ChunkletResult<()> {
        for (offset, buf) in ops {
            self.write_at(*offset, buf)?;
        }
        Ok(())
    }

    /// Make every prior `write_at` / `write_many_at` durable.
    ///
    /// `write_at` issues O_DIRECT `pwrite`s to the member PDs. `flush` is the
    /// persistence barrier used by upstream durability gates (onyx's LV2
    /// ack-after-durable, metadb checkpoint sync). Implementations fan
    /// `PhysicalDisk::sync()` out across the LD's distinct member PDs. A PD
    /// explicitly reported by Linux as `write through` needs no additional
    /// cache command because write completion is already durable; unknown and
    /// write-back devices retain the explicit sync. Degraded (absent) members
    /// are skipped, their data being reconstructed on read.
    fn flush(&self) -> ChunkletResult<()>;
}

/// Fan `PhysicalDisk::sync()` out across the distinct member PDs of an LD.
///
/// Multiple chunklets of one LD can live on the same PD, so each PD is synced
/// at most once per call. `None` members (failed PD / scrub-quarantined
/// chunklet) are skipped — a redundant LD reconstructs them on read, and a
/// non-redundant LD would already have errored on the write that preceded
/// this flush. Shared by every `LogicalDisk::flush` implementation.
pub(crate) fn flush_members(members: &[Option<Arc<PhysicalDisk>>]) -> ChunkletResult<()> {
    let mut seen: Vec<crate::types::PdId> = Vec::new();
    let mut pds = Vec::new();
    for pd in members.iter().flatten() {
        let id = pd.pd_id();
        if seen.contains(&id) {
            continue;
        }
        seen.push(id);
        pds.push(pd.clone());
    }
    crate::io::backend::submit_pd_flushes(&pds)
}

pub(crate) struct StripeLockTable {
    buckets: Vec<RwLock<()>>,
    /// Keys are grouped by `key >> group_shift` before hashing, so a run of
    /// contiguous keys takes ONE bucket instead of one each. `0` = no grouping
    /// (one bucket per key, the historical mapping).
    ///
    /// This exists because a batch holds every one of its buckets for the WHOLE
    /// call, so two concurrent batches of N keys are disjoint only with
    /// probability `exp(-N^2/STRIPE_LOCK_BUCKETS)` — and ANY single overlap
    /// serializes both calls end to end. Box-measured 2026-08-14 on nvme-box:
    /// onyx's LV3 flusher hands in 272 stripes per call (1632 block keys to the
    /// outer range table), so `write_keys` took ~272 of 65536 buckets, P(disjoint)
    /// fell to 32 %, and the `lock` phase went 0.045 -> 5.669 ms/call (126x) for
    /// only a 4x key increase. `chunklet-perf` at 67 stripes/call sits at 93 %
    /// disjoint and scales to 24 concurrent callers — that gap WAS the gap between
    /// chunklet's own numbers and onyx's LV3 drain.
    ///
    /// Grouping is always SAFE: mapping more keys onto one bucket can only add
    /// exclusion, never remove it. What it trades away is false sharing between
    /// neighbouring keys, which is why it is opt-in per LD
    /// ([`crate::pool::build_ld_runtime`]) — a sequential-append log like onyx's
    /// LV2 ring writes adjacent keys from independent callers and must stay
    /// ungrouped, or grouping would serialize the ack path.
    group_shift: u32,
}

impl StripeLockTable {
    /// One bucket per key — the historical mapping. Used by every LD's own
    /// fallback table and by callers with no batching.
    pub(crate) fn new() -> Self {
        Self::with_group_shift(0)
    }

    pub(crate) fn with_group_shift(group_shift: u32) -> Self {
        Self {
            buckets: (0..STRIPE_LOCK_BUCKETS).map(|_| RwLock::new(())).collect(),
            group_shift,
        }
    }

    /// Key -> bucket. Grouping happens BEFORE the mix so a contiguous run
    /// collapses to one bucket while groups stay well distributed; the mix also
    /// keeps distinct RAID sets apart in the `(set_idx << 32) | stripe` key space.
    ///
    /// ⚠ Every acquisition helper must route through here. A caller that mapped
    /// the same key to a different bucket than another caller would exclude on
    /// different buckets, i.e. not exclude at all.
    fn bucket(&self, key: u64) -> usize {
        let group = key >> self.group_shift;
        // Fibonacci hashing — the HIGH bits of the product, not the low ones.
        // Taking the low bits is blind to low-order zeros in the input, and
        // grouping manufactures exactly those: `LdRaid6::stripe_key` is
        // `(set_idx << 32) | stripe`, so `>> 10` leaves 22 zero low bits, the
        // multiply contributes nothing to the low 16, and EVERY set's stripe 0
        // hashed to bucket 0. The old ungrouped mapping only escaped that by
        // XOR-ing in `key >> 32`, which happened to be the set index.
        // Pinned by `grouping_keeps_raid_sets_apart`.
        let mixed = group.wrapping_mul(0x9e37_79b9_7f4a_7c15);
        (mixed >> (u64::BITS - STRIPE_LOCK_BUCKETS.trailing_zeros())) as usize
    }

    pub(crate) fn write_key(&self, key: u64) -> RwLockWriteGuard<'_, ()> {
        self.buckets[self.bucket(key)].write()
    }

    pub(crate) fn read_key_range(&self, first: u64, last: u64) -> Vec<RwLockReadGuard<'_, ()>> {
        let mut buckets: Vec<usize> = (first..=last).map(|key| self.bucket(key)).collect();
        buckets.sort_unstable();
        buckets.dedup();
        buckets
            .into_iter()
            .map(|bucket| self.buckets[bucket].read())
            .collect()
    }

    pub(crate) fn write_key_range(&self, first: u64, last: u64) -> Vec<RwLockWriteGuard<'_, ()>> {
        let mut buckets: Vec<usize> = (first..=last).map(|key| self.bucket(key)).collect();
        buckets.sort_unstable();
        buckets.dedup();
        buckets
            .into_iter()
            .map(|bucket| self.buckets[bucket].write())
            .collect()
    }

    pub(crate) fn write_keys(&self, keys: &[u64]) -> Vec<RwLockWriteGuard<'_, ()>> {
        let mut buckets: Vec<usize> = keys.iter().copied().map(|key| self.bucket(key)).collect();
        buckets.sort_unstable();
        buckets.dedup();
        buckets
            .into_iter()
            .map(|bucket| self.buckets[bucket].write())
            .collect()
    }

    /// Read-lock the union of `keys`' buckets in ONE globally-sorted batch.
    /// Mirrors `write_keys`' acquisition order exactly (same `lock_bucket`
    /// mapping, `sort_unstable` + `dedup`) so a multi-range reader and a
    /// concurrent multi-range writer always take overlapping buckets in the
    /// same order. Acquiring per-range instead (one `read_key_range` per op,
    /// each sorted only within itself) lets a reader grab buckets in a
    /// different global order than `write_keys` → AB-BA deadlock.
    pub(crate) fn read_keys(&self, keys: &[u64]) -> Vec<RwLockReadGuard<'_, ()>> {
        let mut buckets: Vec<usize> = keys.iter().copied().map(|key| self.bucket(key)).collect();
        buckets.sort_unstable();
        buckets.dedup();
        buckets
            .into_iter()
            .map(|bucket| self.buckets[bucket].read())
            .collect()
    }

    /// How many buckets a batch of `keys` would take. Lets a test assert the
    /// footprint directly, which is the property the grouping exists to bound.
    #[cfg(test)]
    pub(crate) fn footprint(&self, keys: &[u64]) -> usize {
        let mut buckets: Vec<usize> = keys.iter().copied().map(|key| self.bucket(key)).collect();
        buckets.sort_unstable();
        buckets.dedup();
        buckets.len()
    }
}

/// Group 2^10 consecutive keys per bucket for LDs that receive large batched
/// writes (RAID5/RAID6 — onyx's LV3). One group spans far more than one
/// allocator run (onyx box: `blocks_per_run` 38.6 blocks ≈ 6.4 stripes), so each
/// run collapses to a single bucket and a 272-stripe call drops from ~272 buckets
/// to ~42 — P(disjoint) 32 % -> 97 %. It stays well below one allocator region
/// (76800 blocks), so two writers served from different regions still land in
/// different groups and gain no false sharing.
pub(crate) const STRIPE_LOCK_GROUP_SHIFT_BATCHED: u32 = 10;

/// Convert the descriptor's strip-size encoding into bytes.
///
/// `0` preserves the historical default of one 4 KiB block. Non-zero strip
/// sizes must also be block-aligned and fit in a u64 shift. This keeps invalid
/// admin input from turning into tiny stripes or shift overflows inside RAID
/// mapping code.
pub(crate) fn compute_strip_bytes(strip_size_log2: u8) -> ChunkletResult<u64> {
    if strip_size_log2 == 0 {
        return Ok(BLOCK_SIZE);
    }
    if !(12..63).contains(&strip_size_log2) {
        return Err(ChunkletError::Invariant(format!(
            "strip_size_log2 must be 0 or in 12..63, got {}",
            strip_size_log2
        )));
    }
    let strip = 1u64.checked_shl(strip_size_log2 as u32).ok_or_else(|| {
        ChunkletError::Invariant(format!("invalid strip_size_log2 {}", strip_size_log2))
    })?;
    if strip % BLOCK_SIZE != 0 {
        return Err(ChunkletError::Invariant(format!(
            "strip size {} is not block-aligned to {}",
            strip, BLOCK_SIZE
        )));
    }
    Ok(strip)
}

/// Look up the `Arc<PhysicalDisk>` for each member listed in a descriptor,
/// returning a vector aligned with `desc.members`. A `None` entry means the
/// member is unavailable — either the owning PD is missing (Failed) or the
/// chunklet's bitmap state on its PD is `Bad` (quarantined by scrub).
/// LDs with redundancy (Mirror / Raid5 / Raid6) tolerate `None` entries via
/// reconstruct paths; LDs without redundancy (Plain / Raid0) return an error
/// on first IO.
pub(crate) fn resolve_members(
    pds: &std::collections::BTreeMap<crate::types::PdId, Arc<PhysicalDisk>>,
    pd_health: &std::collections::BTreeMap<crate::types::PdId, crate::pool::PdHealth>,
    desc: &LdDescriptor,
) -> ChunkletResult<Vec<Option<Arc<PhysicalDisk>>>> {
    let mut out = Vec::with_capacity(desc.members.len());
    for m in &desc.members {
        if pd_health.get(&m.pd) == Some(&crate::pool::PdHealth::Failed) {
            out.push(None);
            continue;
        }
        match pds.get(&m.pd) {
            None => out.push(None),
            Some(pd) => {
                let (_, bitmap, _) = pd.snapshot();
                let bad = bitmap
                    .get(m.chunklet_index)
                    .map(|s| s == crate::types::ChunkletState::Bad)
                    .unwrap_or(false);
                if bad {
                    out.push(None);
                } else {
                    out.push(Some(pd.clone()));
                }
            }
        }
    }
    Ok(out)
}

#[cfg(test)]
mod stripe_lock_tests {
    use super::*;
    use std::collections::HashSet;

    /// The UNGROUPED table (mirror/plain — onyx's LV2 ring and metadb window)
    /// must keep spreading adjacent keys, because a ring log's concurrent
    /// appenders write neighbouring keys and must not land on one bucket.
    #[test]
    fn ungrouped_table_does_not_alias_adjacent_keys() {
        let table = StripeLockTable::new();
        let occupied: HashSet<usize> = (0..1024).map(|key| table.bucket(key)).collect();
        assert_eq!(occupied.len(), 1024, "adjacent stripes should not alias");
        assert!(
            occupied.len() * 64 <= STRIPE_LOCK_BUCKETS,
            "a 4 MiB key window must leave room for disjoint writers"
        );
    }

    /// Every input bit must reach the bucket index. The mapping is a pure runtime
    /// hash (nothing on disk depends on it), so it is free to change — but a
    /// variant that ignores some bits silently collapses independent keys onto one
    /// bucket, which is how `grouping_keeps_raid_sets_apart` first failed.
    #[test]
    fn every_key_bit_can_change_the_bucket() {
        for shift in [0, STRIPE_LOCK_GROUP_SHIFT_BATCHED] {
            let table = StripeLockTable::with_group_shift(shift);
            for bit in shift..u64::BITS {
                let base = table.bucket(0);
                let flipped = table.bucket(1u64 << bit);
                assert_ne!(
                    base, flipped,
                    "shift {shift}: bit {bit} does not reach the bucket index"
                );
            }
        }
    }

    /// The property the grouping exists for: a batch's footprint tracks how many
    /// RUNS it touches, not how many keys. This is what makes two concurrent
    /// batched writes likely to be disjoint.
    #[test]
    fn grouped_footprint_tracks_runs_not_keys() {
        let table = StripeLockTable::with_group_shift(STRIPE_LOCK_GROUP_SHIFT_BATCHED);
        let ungrouped = StripeLockTable::new();

        // The onyx box shape: ~42 scattered runs of ~39 consecutive keys each,
        // spread over one allocator region (76800 blocks).
        let runs = 42u64;
        let run_len = 39u64;
        let mut keys = Vec::new();
        for run in 0..runs {
            let base = run * (76_800 / runs);
            keys.extend(base..base + run_len);
        }
        assert_eq!(keys.len() as u64, runs * run_len);

        let grouped_footprint = table.footprint(&keys);
        assert_eq!(
            ungrouped.footprint(&keys),
            keys.len(),
            "ungrouped must take one bucket per key"
        );
        assert!(
            grouped_footprint <= runs as usize * 2,
            "grouped footprint {grouped_footprint} must track the {runs} runs, not the \
             {} keys (a run may straddle one group boundary, hence 2x)",
            keys.len()
        );
    }

    #[test]
    fn a_run_shorter_than_a_group_takes_one_bucket() {
        let table = StripeLockTable::with_group_shift(STRIPE_LOCK_GROUP_SHIFT_BATCHED);
        let group = 1u64 << STRIPE_LOCK_GROUP_SHIFT_BATCHED;
        // Anchored at a group boundary so it cannot straddle.
        let keys: Vec<u64> = (group..group + 64).collect();
        assert_eq!(table.footprint(&keys), 1);
    }

    /// `LdRaid6::stripe_key` is `(set_idx << 32) | stripe_index`. Grouping shifts
    /// that key right, so it must not merge two RAID sets onto one bucket — sets
    /// are independent redundancy groups and must stay independently lockable.
    #[test]
    fn grouping_keeps_raid_sets_apart() {
        let table = StripeLockTable::with_group_shift(STRIPE_LOCK_GROUP_SHIFT_BATCHED);
        let key = |set_idx: u64, stripe: u64| (set_idx << 32) | stripe;
        let mut seen = HashSet::new();
        for set_idx in 0..8u64 {
            assert!(
                seen.insert(table.bucket(key(set_idx, 0))),
                "set {set_idx} aliased another set's stripe 0"
            );
        }
    }

    /// Pins the policy: only the parity levels group. Regressing this would
    /// serialize onyx's LV2 ack path.
    #[test]
    fn only_parity_levels_group_their_lock_keys() {
        use crate::RaidLevel;
        for level in [RaidLevel::Raid5, RaidLevel::Raid6] {
            assert_eq!(
                crate::pool::lock_group_shift_for(level),
                STRIPE_LOCK_GROUP_SHIFT_BATCHED,
                "{level:?} takes large batched writes and must group"
            );
        }
        for level in [RaidLevel::Plain, RaidLevel::Mirror, RaidLevel::Raid0] {
            assert_eq!(
                crate::pool::lock_group_shift_for(level),
                0,
                "{level:?} carries sequential-append traffic and must not group"
            );
        }
    }
}
