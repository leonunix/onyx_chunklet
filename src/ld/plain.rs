//! `LdPlain` — linear concatenation of chunklets, no redundancy.
//!
//! Layout: chunklets are concatenated in `descriptor.members` order. An IO
//! at LD `offset` maps to:
//!
//! ```text
//! chunklet_n  = offset / CHUNKLET_USER_SIZE
//! chunklet_off = offset % CHUNKLET_USER_SIZE
//! ```
//!
//! Cross-chunklet IOs are split into per-chunklet pieces and dispatched in
//! the natural order. There is no parity work; this implementation exists to
//! validate the LD trait + descriptor + allocator wiring before P2 introduces
//! mirror semantics.

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::error::{ChunkletError, ChunkletResult};
use crate::ld::descriptor::LdDescriptor;
use crate::ld::{resolve_members, LogicalDisk};
use crate::pd::PhysicalDisk;
use crate::types::{
    ChunkletId, LdId, LdRole, PdId, RaidLevel, BLOCK_SIZE, CHUNKLET_HEADER_BYTES, CHUNKLET_SIZE,
};

const CHUNKLET_USER_BYTES: u64 = CHUNKLET_SIZE - CHUNKLET_HEADER_BYTES;

pub struct LdPlain {
    desc: LdDescriptor,
    members: Vec<Arc<PhysicalDisk>>,
    capacity: u64,
}

impl LdPlain {
    /// Resolve the descriptor's members against the live PD map and build the
    /// in-memory LD handle. Caller (`Pool::open_ld`) is responsible for
    /// validating that `desc.raid_level == RaidLevel::Plain`.
    pub fn open(
        desc: LdDescriptor,
        pds: &BTreeMap<PdId, Arc<PhysicalDisk>>,
    ) -> ChunkletResult<Self> {
        if desc.raid_level != RaidLevel::Plain {
            return Err(ChunkletError::Invariant(format!(
                "LdPlain::open with raid_level={:?}",
                desc.raid_level
            )));
        }
        if desc.members.is_empty() {
            return Err(ChunkletError::Invariant(
                "LdPlain has no members".into(),
            ));
        }
        let members = resolve_members(pds, &desc)?;
        let capacity = (desc.members.len() as u64) * CHUNKLET_USER_BYTES;
        Ok(Self {
            desc,
            members,
            capacity,
        })
    }

    pub fn descriptor(&self) -> &LdDescriptor {
        &self.desc
    }

    /// Iterate the (PD, chunklet_index) tuples for every member.
    pub fn member_chunklets(&self) -> impl Iterator<Item = ChunkletId> + '_ {
        self.desc
            .members
            .iter()
            .map(|m| ChunkletId::new(m.pd, m.chunklet_index))
    }

    fn ensure_aligned(&self, offset: u64, len: usize) -> ChunkletResult<()> {
        let bs = self.block_size() as u64;
        if offset % bs != 0 || (len as u64) % bs != 0 {
            return Err(ChunkletError::Invariant(format!(
                "LD IO not block-aligned: offset={} len={} block_size={}",
                offset, len, bs
            )));
        }
        let end = offset
            .checked_add(len as u64)
            .ok_or_else(|| ChunkletError::Invariant("LD IO offset overflow".into()))?;
        if end > self.capacity {
            return Err(ChunkletError::Invariant(format!(
                "LD IO out of range: offset={} len={} capacity={}",
                offset, len, self.capacity
            )));
        }
        Ok(())
    }

    /// Walk the IO range and call `op` for each (member_index, chunklet_offset, slice_len).
    fn for_each_segment<F>(&self, offset: u64, total_len: usize, mut op: F) -> ChunkletResult<()>
    where
        F: FnMut(usize /* member_idx */, u64 /* offset_in_chunklet */, std::ops::Range<usize>) -> ChunkletResult<()>,
    {
        let mut remaining = total_len;
        let mut cursor = offset;
        let mut buf_start = 0usize;
        while remaining > 0 {
            let member_idx = (cursor / CHUNKLET_USER_BYTES) as usize;
            if member_idx >= self.desc.members.len() {
                return Err(ChunkletError::Invariant(format!(
                    "computed member_idx {} >= len {}",
                    member_idx,
                    self.desc.members.len()
                )));
            }
            let chunklet_off = cursor % CHUNKLET_USER_BYTES;
            let take = std::cmp::min(remaining as u64, CHUNKLET_USER_BYTES - chunklet_off) as usize;
            op(member_idx, chunklet_off, buf_start..buf_start + take)?;
            buf_start += take;
            cursor += take as u64;
            remaining -= take;
        }
        Ok(())
    }
}

impl LogicalDisk for LdPlain {
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
        // No striping; upstream packers can pick any block-aligned size.
        BLOCK_SIZE as usize
    }

    fn read_at(&self, offset: u64, buf: &mut [u8]) -> ChunkletResult<()> {
        self.ensure_aligned(offset, buf.len())?;
        self.for_each_segment(offset, buf.len(), |member_idx, off_in_c, range| {
            // Only Data members in Plain. Defensive check; alloc enforces it.
            debug_assert_eq!(self.desc.members[member_idx].role, LdRole::Data);
            let pd = &self.members[member_idx];
            let chunklet_idx = self.desc.members[member_idx].chunklet_index;
            pd.read_chunklet_user(chunklet_idx, off_in_c, &mut buf[range])
        })
    }

    fn write_at(&self, offset: u64, buf: &[u8]) -> ChunkletResult<()> {
        self.ensure_aligned(offset, buf.len())?;
        self.for_each_segment(offset, buf.len(), |member_idx, off_in_c, range| {
            debug_assert_eq!(self.desc.members[member_idx].role, LdRole::Data);
            let pd = &self.members[member_idx];
            let chunklet_idx = self.desc.members[member_idx].chunklet_index;
            pd.write_chunklet_user(chunklet_idx, off_in_c, &buf[range])
        })
    }
}
