//! `LdRaid0` — pure striping, no redundancy.
//!
//! Distinct from `LdPlain`:
//! - **LdPlain**: linear concat. LD bytes [0, chunklet_user) live entirely on
//!   chunklet 0; [chunklet_user, 2*chunklet_user) on chunklet 1; etc. Cross-
//!   chunklet IO sees one chunklet's bytes, then the next.
//! - **LdRaid0**: round-robin striping at `strip_size_log2` granularity across
//!   `row_size` chunklets. Sequential IO of length L is split into roughly
//!   L/strip_bytes pieces, each landing on a different PD — true RAID-0
//!   parallelism.
//!
//! # Layout
//!
//! - `set_size = 1` (no redundancy).
//! - `row_size = K` chunklets striped within one row.
//! - `num_rows = R` rows of K chunklets, concatenated.
//! - Logical capacity = `K * R * chunklet_user_size`.
//! - Total chunklets   = `K * R`.
//!
//! Members in row-major / position-major order: `members[row * K + pos]`.
//!
//! # Address mapping
//!
//! ```text
//! row_n             = O / (K * chunklet_user_size)
//! in_row            = O % (K * chunklet_user_size)
//! global_strip      = in_row / strip_bytes
//! in_strip_off      = in_row % strip_bytes
//! pos_in_row        = global_strip % K        // which of K chunklets
//! strip_in_chunklet = global_strip / K
//! in_chunklet_off   = strip_in_chunklet * strip_bytes + in_strip_off
//! ```

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::error::{ChunkletError, ChunkletResult};
use crate::ld::descriptor::LdDescriptor;
use crate::ld::{resolve_members, LogicalDisk};
use crate::pd::PhysicalDisk;
use crate::types::{
    LdId, PdId, RaidLevel, BLOCK_SIZE, CHUNKLET_HEADER_BYTES, CHUNKLET_SIZE,
};

const CHUNKLET_USER_BYTES: u64 = CHUNKLET_SIZE - CHUNKLET_HEADER_BYTES;

pub struct LdRaid0 {
    desc: LdDescriptor,
    members: Vec<Arc<PhysicalDisk>>,
    capacity: u64,
    strip_bytes: u64,
}

impl LdRaid0 {
    pub fn open(
        desc: LdDescriptor,
        pds: &BTreeMap<PdId, Arc<PhysicalDisk>>,
    ) -> ChunkletResult<Self> {
        if desc.raid_level != RaidLevel::Raid0 {
            return Err(ChunkletError::Invariant(format!(
                "LdRaid0::open with raid_level={:?}",
                desc.raid_level
            )));
        }
        if desc.set_size != 1 {
            return Err(ChunkletError::Invariant(format!(
                "Raid0 requires set_size=1, got {}",
                desc.set_size
            )));
        }
        if desc.row_size < 2 {
            return Err(ChunkletError::Invariant(format!(
                "Raid0 requires row_size >= 2 (use Plain for unstriped), got {}",
                desc.row_size
            )));
        }
        let expected = (desc.row_size as usize) * (desc.num_rows as usize);
        if desc.members.len() != expected {
            return Err(ChunkletError::Invariant(format!(
                "Raid0 member count {} != row_size * num_rows ({})",
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
        Ok(Self {
            desc,
            members,
            capacity,
            strip_bytes,
        })
    }

    pub fn descriptor(&self) -> &LdDescriptor {
        &self.desc
    }

    fn ensure_aligned(&self, offset: u64, len: usize) -> ChunkletResult<()> {
        let bs = self.block_size() as u64;
        if offset % bs != 0 || (len as u64) % bs != 0 {
            return Err(ChunkletError::Invariant(format!(
                "Raid0 IO not block-aligned: offset={} len={}",
                offset, len
            )));
        }
        let end = offset.checked_add(len as u64).ok_or_else(|| {
            ChunkletError::Invariant("Raid0 IO offset overflow".into())
        })?;
        if end > self.capacity {
            return Err(ChunkletError::Invariant(format!(
                "Raid0 IO out of range: offset={} len={} capacity={}",
                offset, len, self.capacity
            )));
        }
        Ok(())
    }

    fn for_each_segment<F>(&self, offset: u64, total_len: usize, mut op: F) -> ChunkletResult<()>
    where
        F: FnMut(usize /* member_idx */, u64 /* in_chunklet_off */, std::ops::Range<usize>) -> ChunkletResult<()>,
    {
        let usable_per_chunklet = (CHUNKLET_USER_BYTES / self.strip_bytes) * self.strip_bytes;
        let row_bytes = (self.desc.row_size as u64) * usable_per_chunklet;
        let row_size = self.desc.row_size as u64;
        let strip = self.strip_bytes;

        let mut remaining = total_len;
        let mut cursor = offset;
        let mut buf_start = 0usize;
        while remaining > 0 {
            let row_n = (cursor / row_bytes) as usize;
            let in_row = cursor % row_bytes;
            let global_strip = in_row / strip;
            let in_strip_off = in_row % strip;
            let pos_in_row = (global_strip % row_size) as usize;
            let strip_in_chunklet = global_strip / row_size;
            let in_chunklet_off = strip_in_chunklet * strip + in_strip_off;
            let strip_remain = strip - in_strip_off;
            let take = std::cmp::min(remaining as u64, strip_remain) as usize;
            let member_idx = row_n * (self.desc.row_size as usize) + pos_in_row;
            op(member_idx, in_chunklet_off, buf_start..buf_start + take)?;
            buf_start += take;
            cursor += take as u64;
            remaining -= take;
        }
        Ok(())
    }
}

impl LogicalDisk for LdRaid0 {
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
        self.for_each_segment(offset, buf.len(), |member_idx, off, range| {
            let chunklet_idx = self.desc.members[member_idx].chunklet_index;
            self.members[member_idx].read_chunklet_user(chunklet_idx, off, &mut buf[range])
        })
    }

    fn write_at(&self, offset: u64, buf: &[u8]) -> ChunkletResult<()> {
        self.ensure_aligned(offset, buf.len())?;
        self.for_each_segment(offset, buf.len(), |member_idx, off, range| {
            let chunklet_idx = self.desc.members[member_idx].chunklet_index;
            self.members[member_idx].write_chunklet_user(chunklet_idx, off, &buf[range])
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
