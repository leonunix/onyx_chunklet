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
    /// `write_at` issues O_DIRECT `pwrite`s to the member PDs, which bypasses
    /// the page cache but does **not** flush each drive's write cache — the
    /// data is not yet crash-durable when the call returns. `flush` is the
    /// persistence barrier: upstream durability gates (onyx's LV2
    /// ack-after-durable, metadb checkpoint sync) call it before treating a
    /// write as committed. Implementations fan `PhysicalDisk::sync()` out
    /// across the LD's distinct member PDs; degraded (absent) members are
    /// skipped, their data being reconstructed on read.
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
}

impl StripeLockTable {
    pub(crate) fn new() -> Self {
        Self {
            buckets: (0..STRIPE_LOCK_BUCKETS).map(|_| RwLock::new(())).collect(),
        }
    }

    pub(crate) fn write_key(&self, key: u64) -> RwLockWriteGuard<'_, ()> {
        self.buckets[lock_bucket(key)].write()
    }

    pub(crate) fn read_key_range(&self, first: u64, last: u64) -> Vec<RwLockReadGuard<'_, ()>> {
        let mut buckets: Vec<usize> = (first..=last).map(lock_bucket).collect();
        buckets.sort_unstable();
        buckets.dedup();
        buckets
            .into_iter()
            .map(|bucket| self.buckets[bucket].read())
            .collect()
    }

    pub(crate) fn write_key_range(&self, first: u64, last: u64) -> Vec<RwLockWriteGuard<'_, ()>> {
        let mut buckets: Vec<usize> = (first..=last).map(lock_bucket).collect();
        buckets.sort_unstable();
        buckets.dedup();
        buckets
            .into_iter()
            .map(|bucket| self.buckets[bucket].write())
            .collect()
    }

    pub(crate) fn write_keys(&self, keys: &[u64]) -> Vec<RwLockWriteGuard<'_, ()>> {
        let mut buckets: Vec<usize> = keys.iter().copied().map(lock_bucket).collect();
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
        let mut buckets: Vec<usize> = keys.iter().copied().map(lock_bucket).collect();
        buckets.sort_unstable();
        buckets.dedup();
        buckets
            .into_iter()
            .map(|bucket| self.buckets[bucket].read())
            .collect()
    }
}

fn lock_bucket(key: u64) -> usize {
    let mixed = key.wrapping_mul(0x9e37_79b9_7f4a_7c15) ^ (key >> 32);
    (mixed as usize) & (STRIPE_LOCK_BUCKETS - 1)
}

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

    #[test]
    fn checkpoint_sized_key_window_does_not_saturate_lock_table() {
        let occupied: HashSet<usize> = (0..1024).map(lock_bucket).collect();
        assert_eq!(occupied.len(), 1024, "adjacent stripes should not alias");
        assert!(
            occupied.len() * 64 <= STRIPE_LOCK_BUCKETS,
            "a 4 MiB key window must leave room for disjoint writers"
        );
    }
}
