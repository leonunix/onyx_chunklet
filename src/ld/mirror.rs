//! `LdMirror` — N-way mirror, optionally striped (RAID-1 / RAID-10).
//!
//! # Layout
//!
//! - `set_size = N` mirror copies per set. All N members hold identical data.
//! - `row_size = K` sets striped together within one row.
//! - `num_rows = R` rows of K sets, concatenated.
//! - `strip_size = 1 << strip_size_log2` bytes (0 ⇒ default to one block).
//!
//! Logical capacity = `K * R * chunklet_user_size`.
//! Total chunklets   = `N * K * R`.
//!
//! Members in the descriptor are stored row-major / set-major / copy-major:
//! `members[((row * K) + set) * N + copy]`.
//!
//! # Address mapping
//!
//! For an LD offset `O`:
//! ```text
//! row_n             = O / (K * chunklet_user_size)
//! in_row            = O % (K * chunklet_user_size)
//! global_strip      = in_row / strip_bytes
//! in_strip_off      = in_row % strip_bytes
//! set_in_row        = global_strip % K
//! strip_in_chunklet = global_strip / K
//! in_chunklet_off   = strip_in_chunklet * strip_bytes + in_strip_off
//! ```
//!
//! For the resolved (row_n, set_in_row), all N copies hold the data.
//!
//! # IO
//!
//! - `read_at`: walk the IO range, split at strip boundaries; for each
//!   segment pick one of N copies (round-robin per LD instance via an
//!   atomic counter) and read.
//! - `write_at`: walk the IO range, split at strip boundaries; for each
//!   segment fan out a parallel write to every live copy via
//!   `parallel_strip_writes` (so K copies hit K disks simultaneously).
//!   `parallel_strip_writes` returns the first error; on partial-failure
//!   the LD is **torn** (some copies new, others old / partial) until the
//!   admin re-runs the IO or scrub repairs divergence. 3+ way mirrors
//!   self-heal via `Pool::scrub_ld` (majority vote); 2-way mirrors require
//!   `Pool::mark_chunklet_bad` to identify the bad copy explicitly.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use crate::error::{ChunkletError, ChunkletResult};
use crate::ld::descriptor::LdDescriptor;
use crate::ld::{parallel_strip_writes, resolve_members, LogicalDisk, StripWrite};
use crate::pd::PhysicalDisk;
use crate::types::{
    LdId, PdId, RaidLevel, BLOCK_SIZE, CHUNKLET_HEADER_BYTES, CHUNKLET_SIZE,
};

const CHUNKLET_USER_BYTES: u64 = CHUNKLET_SIZE - CHUNKLET_HEADER_BYTES;

pub struct LdMirror {
    desc: LdDescriptor,
    members: Vec<Option<Arc<PhysicalDisk>>>,
    capacity: u64,
    strip_bytes: u64,
    /// Per-set round-robin cursor for read-side copy selection. One
    /// counter per (row, set) so concurrent readers spreading across
    /// different sets don't all converge on the same copy of one set.
    /// Indexed as `row * row_size + set_in_row`.
    read_cursors: Vec<AtomicUsize>,
}

impl LdMirror {
    pub fn open(
        desc: LdDescriptor,
        pds: &BTreeMap<PdId, Arc<PhysicalDisk>>,
    ) -> ChunkletResult<Self> {
        if desc.raid_level != RaidLevel::Mirror {
            return Err(ChunkletError::Invariant(format!(
                "LdMirror::open with raid_level={:?}",
                desc.raid_level
            )));
        }
        if desc.set_size < 2 {
            return Err(ChunkletError::Invariant(format!(
                "Mirror set_size must be >= 2, got {}",
                desc.set_size
            )));
        }
        let expected = (desc.set_size as usize)
            * (desc.row_size as usize)
            * (desc.num_rows as usize);
        if desc.members.len() != expected {
            return Err(ChunkletError::Invariant(format!(
                "Mirror member count {} != set_size*row_size*num_rows ({})",
                desc.members.len(),
                expected
            )));
        }

        let strip_bytes = compute_strip_bytes(desc.strip_size_log2);
        if strip_bytes > CHUNKLET_USER_BYTES {
            return Err(ChunkletError::Invariant(format!(
                "strip_bytes {} > chunklet_user_size {}",
                strip_bytes, CHUNKLET_USER_BYTES
            )));
        }
        let usable_per_chunklet = (CHUNKLET_USER_BYTES / strip_bytes) * strip_bytes;
        let members = resolve_members(pds, &desc)?;
        let capacity = (desc.row_size as u64) * (desc.num_rows as u64) * usable_per_chunklet;
        let n_sets = (desc.row_size as usize) * (desc.num_rows as usize);
        let read_cursors = (0..n_sets).map(|_| AtomicUsize::new(0)).collect();
        Ok(Self {
            desc,
            members,
            capacity,
            strip_bytes,
            read_cursors,
        })
    }

    pub fn descriptor(&self) -> &LdDescriptor {
        &self.desc
    }

    fn ensure_aligned(&self, offset: u64, len: usize) -> ChunkletResult<()> {
        let bs = self.block_size() as u64;
        if offset % bs != 0 || (len as u64) % bs != 0 {
            return Err(ChunkletError::Invariant(format!(
                "Mirror IO not block-aligned: offset={} len={} block_size={}",
                offset, len, bs
            )));
        }
        let end = offset.checked_add(len as u64).ok_or_else(|| {
            ChunkletError::Invariant("Mirror IO offset overflow".into())
        })?;
        if end > self.capacity {
            return Err(ChunkletError::Invariant(format!(
                "Mirror IO out of range: offset={} len={} capacity={}",
                offset, len, self.capacity
            )));
        }
        Ok(())
    }

    /// Walk the IO range and call `op` for each (row, set, in_chunklet_off,
    /// segment range in caller's buffer). Each segment is contained within a
    /// single (row, set, strip), so the call below maps to exactly one
    /// chunklet-relative IO per copy.
    fn for_each_segment<F>(&self, offset: u64, total_len: usize, mut op: F) -> ChunkletResult<()>
    where
        F: FnMut(usize /* row */, usize /* set */, u64 /* in_chunklet_off */, std::ops::Range<usize>) -> ChunkletResult<()>,
    {
        let usable_per_chunklet = (CHUNKLET_USER_BYTES / self.strip_bytes) * self.strip_bytes;
        let row_bytes = (self.desc.row_size as u64) * usable_per_chunklet;
        let strip_bytes = self.strip_bytes;
        let row_size = self.desc.row_size as u64;

        let mut remaining = total_len;
        let mut cursor = offset;
        let mut buf_start = 0usize;
        while remaining > 0 {
            let row_n = (cursor / row_bytes) as usize;
            let in_row = cursor % row_bytes;
            let global_strip = in_row / strip_bytes;
            let in_strip_off = in_row % strip_bytes;
            let set_in_row = (global_strip % row_size) as usize;
            let strip_in_chunklet = global_strip / row_size;
            let in_chunklet_off = strip_in_chunklet * strip_bytes + in_strip_off;

            // Bytes left in current strip within the LD address space.
            let strip_remain = strip_bytes - in_strip_off;
            let take = std::cmp::min(remaining as u64, strip_remain) as usize;

            op(row_n, set_in_row, in_chunklet_off, buf_start..buf_start + take)?;
            buf_start += take;
            cursor += take as u64;
            remaining -= take;
        }
        Ok(())
    }

    fn member_indices_for(&self, row: usize, set: usize) -> std::ops::Range<usize> {
        let n = self.desc.set_size as usize;
        let k = self.desc.row_size as usize;
        let base = (row * k + set) * n;
        base..base + n
    }

    pub fn strip_bytes(&self) -> u64 {
        self.strip_bytes
    }

    pub fn stripes_per_chunklet(&self) -> u64 {
        CHUNKLET_USER_BYTES / self.strip_bytes
    }

    /// Read the strip at `in_chunklet_off` for `failed_member_idx`'s position
    /// from a live sibling copy in the same set. Used by rebuild.
    pub fn reconstruct_member_strip(
        &self,
        failed_member_idx: usize,
        in_chunklet_off: u64,
        out: &mut [u8],
    ) -> ChunkletResult<()> {
        let n = self.desc.set_size as usize;
        let set_base = (failed_member_idx / n) * n;
        for i in 0..n {
            let m = set_base + i;
            if m == failed_member_idx {
                continue;
            }
            if let Some(pd) = self.members[m].as_ref() {
                let chunklet_idx = self.desc.members[m].chunklet_index;
                return pd.read_chunklet_user(chunklet_idx, in_chunklet_off, out);
            }
        }
        Err(ChunkletError::Invariant(format!(
            "Mirror set base={} has no live sibling for failed member {}",
            set_base, failed_member_idx
        )))
    }
}

impl LogicalDisk for LdMirror {
    fn id(&self) -> LdId {
        self.desc.id
    }

    fn capacity_bytes(&self) -> u64 {
        self.capacity
    }

    fn block_size(&self) -> usize {
        BLOCK_SIZE as usize
    }

    fn strip_size(&self) -> usize {
        self.strip_bytes as usize
    }

    fn read_at(&self, offset: u64, buf: &mut [u8]) -> ChunkletResult<()> {
        self.ensure_aligned(offset, buf.len())?;
        let row_size = self.desc.row_size as usize;
        self.for_each_segment(offset, buf.len(), |row, set, off_in_c, range| {
            // Round-robin pick a live copy using THIS set's cursor (so
            // concurrent reads on different sets don't share a counter
            // and converge on the same copy). If the chosen copy is
            // Failed, walk through the rest of the ring; all-N-Failed
            // returns an error.
            let copies = self.member_indices_for(row, set);
            let n = copies.end - copies.start;
            let cursor_idx = row * row_size + set;
            let start =
                self.read_cursors[cursor_idx].fetch_add(1, Ordering::Relaxed) % n;
            for offset_pick in 0..n {
                let pick = (start + offset_pick) % n;
                let member_idx = copies.start + pick;
                if let Some(pd) = self.members[member_idx].as_ref() {
                    let chunklet_idx = self.desc.members[member_idx].chunklet_index;
                    return pd.read_chunklet_user(chunklet_idx, off_in_c, &mut buf[range]);
                }
            }
            Err(ChunkletError::Invariant(format!(
                "Mirror set (row={}, set={}) has no live copy",
                row, set
            )))
        })
    }

    fn write_at(&self, offset: u64, buf: &[u8]) -> ChunkletResult<()> {
        self.ensure_aligned(offset, buf.len())?;
        self.for_each_segment(offset, buf.len(), |row, set, off_in_c, range| {
            // Build a parallel fan-out across every live copy. Failed
            // copies (PD missing or chunklet Bad) are skipped — caller has
            // already errored on a fully-dead set during open.
            let copies = self.member_indices_for(row, set);
            let mut ops: Vec<StripWrite> = Vec::with_capacity(copies.end - copies.start);
            for member_idx in copies.clone() {
                if let Some(pd) = self.members[member_idx].as_ref() {
                    ops.push(StripWrite {
                        pd: pd.clone(),
                        chunklet_index: self.desc.members[member_idx].chunklet_index,
                        in_chunklet_off: off_in_c,
                        data: &buf[range.clone()],
                    });
                }
            }
            if ops.is_empty() {
                return Err(ChunkletError::Invariant(format!(
                    "Mirror set (row={}, set={}) write: no live copy",
                    row, set
                )));
            }
            parallel_strip_writes(ops)
        })
    }
}

fn compute_strip_bytes(strip_size_log2: u8) -> u64 {
    if strip_size_log2 == 0 {
        BLOCK_SIZE
    } else {
        1u64 << strip_size_log2
    }
}
