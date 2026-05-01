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

const STRIPE_LOCK_BUCKETS: usize = 1024;

// `StripWrite` and the cross-PD batched-write submission helper live in
// `src/io/backend.rs` now (selectable between SyncBackend and
// UringBackend per Pool). Re-export so existing `use crate::ld::{...,
// StripWrite}` import sites keep working.
pub(crate) use crate::io::backend::{submit_strip_writes as parallel_strip_writes, StripWrite};

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

    /// Write exactly `buf.len()` bytes at `offset`. Same alignment rules.
    fn write_at(&self, offset: u64, buf: &[u8]) -> ChunkletResult<()>;
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
    desc: &LdDescriptor,
) -> ChunkletResult<Vec<Option<Arc<PhysicalDisk>>>> {
    let mut out = Vec::with_capacity(desc.members.len());
    for m in &desc.members {
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
