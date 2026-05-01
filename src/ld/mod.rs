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

use crate::error::ChunkletResult;
use crate::pd::PhysicalDisk;
use crate::types::LdId;

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
