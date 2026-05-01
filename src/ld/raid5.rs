//! `LdRaid5` — N data + 1 parity per set, XOR parity.
//!
//! # Layout
//!
//! - `set_size = K + 1` chunklets per RAID-5 set: K data + 1 parity.
//! - `row_size` sets striped together within one row.
//! - `num_rows` rows of `row_size` sets, concatenated.
//! - Strip striping happens at FULL-STRIPE granularity across sets (i.e. one
//!   set holds K * strip_bytes contiguous bytes before the next set takes
//!   over). This keeps full-stripe writes contiguous in the LD address space,
//!   which is the path onyx packer is built to hit.
//!
//! Logical capacity = `K * row_size * num_rows * chunklet_user_size`.
//! Total chunklets   = `(K + 1) * row_size * num_rows`.
//!
//! Members in the descriptor are stored row-major / set-major / member-major.
//! Within each set, the LAST member is the parity chunklet.
//!
//! # Address mapping (LD offset → set + position)
//!
//! ```text
//! K                   = set_size - 1
//! full_stripe_bytes   = K * strip_bytes
//! row_user            = row_size * K * chunklet_user_size
//! row_n               = O / row_user
//! in_row              = O % row_user
//! global_fs_in_row    = in_row / full_stripe_bytes
//! in_full_stripe      = in_row % full_stripe_bytes
//! set_in_row          = global_fs_in_row % row_size
//! set_stripe_n        = global_fs_in_row / row_size
//! data_pos_in_stripe  = in_full_stripe / strip_bytes      // 0..K-1
//! in_strip_off        = in_full_stripe % strip_bytes
//! in_chunklet_off     = set_stripe_n * strip_bytes + in_strip_off
//! ```
//!
//! # Write path
//!
//! - **Full-stripe** (offset full-stripe-aligned, len ≥ full_stripe_bytes):
//!   compute `P = D0 ^ D1 ^ … ^ D(K-1)` from the new data, write all K data
//!   strips and the parity strip. No reads.
//! - **Partial RMW**: for each touched data position in the stripe,
//!     `delta_p ^= old_data ^ new_data`
//!   Then `new_p = old_p ^ delta_p` and write the modified data positions
//!   plus the new parity. Costs `M+1` reads and `M+1` writes for `M`
//!   modified positions out of K.
//!
//! # Read path
//!
//! Healthy reads hit the data chunklet directly. Degraded reconstruction
//! (one data chunklet missing) walks the surviving data + parity strips and
//! XORs them together. PD failure semantics land in P5; for now we wire the
//! reconstruct math into a helper but the production read path still assumes
//! all chunklets are healthy.

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::error::{ChunkletError, ChunkletResult};
use crate::ld::descriptor::LdDescriptor;
use crate::ld::{resolve_members, LogicalDisk};
use crate::pd::PhysicalDisk;
use crate::types::{
    LdId, LdRole, PdId, RaidLevel, BLOCK_SIZE, CHUNKLET_HEADER_BYTES, CHUNKLET_SIZE,
};

const CHUNKLET_USER_BYTES: u64 = CHUNKLET_SIZE - CHUNKLET_HEADER_BYTES;

pub struct LdRaid5 {
    desc: LdDescriptor,
    members: Vec<Option<Arc<PhysicalDisk>>>,
    capacity: u64,
    strip_bytes: u64,
    /// K = set_size - 1 = number of data chunklets per set.
    data_per_set: usize,
    /// K * strip_bytes; the per-set full-stripe size.
    full_stripe_bytes: u64,
}

impl LdRaid5 {
    pub fn open(
        desc: LdDescriptor,
        pds: &BTreeMap<PdId, Arc<PhysicalDisk>>,
    ) -> ChunkletResult<Self> {
        if desc.raid_level != RaidLevel::Raid5 {
            return Err(ChunkletError::Invariant(format!(
                "LdRaid5::open with raid_level={:?}",
                desc.raid_level
            )));
        }
        if desc.set_size < 3 {
            return Err(ChunkletError::Invariant(format!(
                "Raid5 set_size must be >= 3 (>=2+1), got {}",
                desc.set_size
            )));
        }
        let expected = (desc.set_size as usize)
            * (desc.row_size as usize)
            * (desc.num_rows as usize);
        if desc.members.len() != expected {
            return Err(ChunkletError::Invariant(format!(
                "Raid5 member count {} != expected {}",
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
        let data_per_set = (desc.set_size - 1) as usize;
        let full_stripe_bytes = (data_per_set as u64) * strip_bytes;

        // Validate roles within each set: K data + 1 parity, parity last.
        let n = desc.set_size as usize;
        for set_n in 0..((desc.row_size as usize) * (desc.num_rows as usize)) {
            let base = set_n * n;
            for i in 0..(n - 1) {
                if desc.members[base + i].role != LdRole::Data {
                    return Err(ChunkletError::Invariant(format!(
                        "Raid5 set {} member {} is not Data role: {:?}",
                        set_n, i, desc.members[base + i].role
                    )));
                }
            }
            if desc.members[base + n - 1].role != LdRole::ParityP {
                return Err(ChunkletError::Invariant(format!(
                    "Raid5 set {} last member is not ParityP: {:?}",
                    set_n, desc.members[base + n - 1].role
                )));
            }
        }

        let members = resolve_members(pds, &desc)?;
        let usable_per_chunklet = (CHUNKLET_USER_BYTES / strip_bytes) * strip_bytes;
        let capacity = (desc.row_size as u64)
            * (desc.num_rows as u64)
            * (data_per_set as u64)
            * usable_per_chunklet;
        Ok(Self {
            desc,
            members,
            capacity,
            strip_bytes,
            data_per_set,
            full_stripe_bytes,
        })
    }

    pub fn descriptor(&self) -> &LdDescriptor {
        &self.desc
    }

    fn ensure_aligned(&self, offset: u64, len: usize) -> ChunkletResult<()> {
        let bs = self.block_size() as u64;
        if offset % bs != 0 || (len as u64) % bs != 0 {
            return Err(ChunkletError::Invariant(format!(
                "Raid5 IO not block-aligned: offset={} len={}",
                offset, len
            )));
        }
        let end = offset.checked_add(len as u64).ok_or_else(|| {
            ChunkletError::Invariant("Raid5 IO offset overflow".into())
        })?;
        if end > self.capacity {
            return Err(ChunkletError::Invariant(format!(
                "Raid5 IO out of range: offset={} len={} capacity={}",
                offset, len, self.capacity
            )));
        }
        Ok(())
    }

    /// Resolve an LD offset to its (set_index, in_chunklet_off) tuple plus
    /// the data position within the stripe.
    fn locate(&self, ld_offset: u64) -> StripeAddr {
        let usable_per_chunklet = (CHUNKLET_USER_BYTES / self.strip_bytes) * self.strip_bytes;
        let row_user = (self.desc.row_size as u64)
            * (self.data_per_set as u64)
            * usable_per_chunklet;
        let row_n = (ld_offset / row_user) as usize;
        let in_row = ld_offset % row_user;
        let global_fs_in_row = in_row / self.full_stripe_bytes;
        let in_full_stripe = in_row % self.full_stripe_bytes;
        let set_in_row = (global_fs_in_row % (self.desc.row_size as u64)) as usize;
        let set_stripe_n = global_fs_in_row / (self.desc.row_size as u64);
        let data_pos = (in_full_stripe / self.strip_bytes) as usize;
        let in_strip_off = in_full_stripe % self.strip_bytes;
        let in_chunklet_off = set_stripe_n * self.strip_bytes + in_strip_off;
        StripeAddr {
            set_idx: row_n * (self.desc.row_size as usize) + set_in_row,
            data_pos,
            set_stripe_n,
            in_strip_off,
            in_chunklet_off,
        }
    }

    fn member_idx_data(&self, set_idx: usize, data_pos: usize) -> usize {
        set_idx * (self.desc.set_size as usize) + data_pos
    }

    fn member_idx_parity(&self, set_idx: usize) -> usize {
        set_idx * (self.desc.set_size as usize) + self.data_per_set
    }

    fn member_pd(&self, idx: usize) -> ChunkletResult<&Arc<PhysicalDisk>> {
        self.members[idx].as_ref().ok_or_else(|| {
            ChunkletError::Invariant(format!(
                "Raid5 member idx={} on failed PD {} — caller must rebuild before writing",
                idx, self.desc.members[idx].pd
            ))
        })
    }

    fn write_data_strip(
        &self,
        set_idx: usize,
        data_pos: usize,
        in_chunklet_off: u64,
        bytes: &[u8],
    ) -> ChunkletResult<()> {
        let m = self.member_idx_data(set_idx, data_pos);
        let pd = self.member_pd(m)?;
        let chunklet_idx = self.desc.members[m].chunklet_index;
        pd.write_chunklet_user(chunklet_idx, in_chunklet_off, bytes)
    }

    fn read_data_strip(
        &self,
        set_idx: usize,
        data_pos: usize,
        in_chunklet_off: u64,
        bytes: &mut [u8],
    ) -> ChunkletResult<()> {
        let m = self.member_idx_data(set_idx, data_pos);
        let pd = self.member_pd(m)?;
        let chunklet_idx = self.desc.members[m].chunklet_index;
        pd.read_chunklet_user(chunklet_idx, in_chunklet_off, bytes)
    }

    fn write_parity_strip(
        &self,
        set_idx: usize,
        in_chunklet_off: u64,
        bytes: &[u8],
    ) -> ChunkletResult<()> {
        let m = self.member_idx_parity(set_idx);
        let pd = self.member_pd(m)?;
        let chunklet_idx = self.desc.members[m].chunklet_index;
        pd.write_chunklet_user(chunklet_idx, in_chunklet_off, bytes)
    }

    fn read_parity_strip(
        &self,
        set_idx: usize,
        in_chunklet_off: u64,
        bytes: &mut [u8],
    ) -> ChunkletResult<()> {
        let m = self.member_idx_parity(set_idx);
        let pd = self.member_pd(m)?;
        let chunklet_idx = self.desc.members[m].chunklet_index;
        pd.read_chunklet_user(chunklet_idx, in_chunklet_off, bytes)
    }

    /// True when the given data position's PD is currently absent.
    fn data_position_failed(&self, set_idx: usize, data_pos: usize) -> bool {
        let m = self.member_idx_data(set_idx, data_pos);
        self.members[m].is_none()
    }

    /// Encode the parity strip from all K data strips at `in_chunklet_off`.
    /// Used by rebuild when the parity chunklet itself is what was lost.
    pub fn encode_parity_strip(
        &self,
        set_idx: usize,
        in_chunklet_off: u64,
        out: &mut [u8],
    ) -> ChunkletResult<()> {
        out.fill(0);
        let mut tmp = vec![0u8; out.len()];
        for pos in 0..self.data_per_set {
            self.read_data_strip(set_idx, pos, in_chunklet_off, &mut tmp)?;
            xor_into(out, &tmp);
        }
        Ok(())
    }

    /// How many strips fit in one chunklet at the current strip size.
    pub fn stripes_per_chunklet(&self) -> u64 {
        CHUNKLET_USER_BYTES / self.strip_bytes
    }

    pub fn strip_bytes(&self) -> u64 {
        self.strip_bytes
    }

    pub fn data_per_set(&self) -> usize {
        self.data_per_set
    }

    /// Generic per-member rebuild helper: dispatches to the right reconstruct
    /// path based on whether `failed_member_idx` lands on data or parity.
    pub fn reconstruct_member_strip(
        &self,
        failed_member_idx: usize,
        in_chunklet_off: u64,
        out: &mut [u8],
    ) -> ChunkletResult<()> {
        let n = self.desc.set_size as usize;
        let set_idx = failed_member_idx / n;
        let pos = failed_member_idx % n;
        if pos == self.data_per_set {
            // Parity slot.
            self.encode_parity_strip(set_idx, in_chunklet_off, out)
        } else {
            self.reconstruct_data(set_idx, pos, in_chunklet_off, out)
        }
    }

    /// Reconstruct one missing data position by XORing the surviving K-1
    /// data strips with the parity strip. Used by degraded read + rebuild.
    ///
    /// `out.len()` may be any block-aligned length up to `chunklet_user_size`;
    /// reads pull the same length from each surviving member at
    /// `in_chunklet_off`. Rebuild calls this with megabyte-sized buffers to
    /// minimize per-strip overhead.
    pub fn reconstruct_data(
        &self,
        set_idx: usize,
        missing_data_pos: usize,
        in_chunklet_off: u64,
        out: &mut [u8],
    ) -> ChunkletResult<()> {
        if missing_data_pos >= self.data_per_set {
            return Err(ChunkletError::Invariant(format!(
                "missing_data_pos {} >= data_per_set {}",
                missing_data_pos, self.data_per_set
            )));
        }
        // Start with parity, XOR every surviving data strip in.
        self.read_parity_strip(set_idx, in_chunklet_off, out)?;
        let mut tmp = vec![0u8; out.len()];
        for pos in 0..self.data_per_set {
            if pos == missing_data_pos {
                continue;
            }
            self.read_data_strip(set_idx, pos, in_chunklet_off, &mut tmp)?;
            xor_into(out, &tmp);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug)]
struct StripeAddr {
    /// Linear set index across rows: `row * row_size + set_in_row`.
    set_idx: usize,
    /// Which data position in the set: `0..data_per_set`.
    data_pos: usize,
    /// Stripe ordinal within the set's chunklets.
    set_stripe_n: u64,
    /// Byte offset within the strip.
    in_strip_off: u64,
    /// Byte offset on the chunklet (data or parity).
    in_chunklet_off: u64,
}

impl LogicalDisk for LdRaid5 {
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
        self.full_stripe_bytes as usize
    }

    fn read_at(&self, offset: u64, buf: &mut [u8]) -> ChunkletResult<()> {
        self.ensure_aligned(offset, buf.len())?;
        let mut remaining = buf.len();
        let mut cursor = offset;
        let mut buf_start = 0usize;
        while remaining > 0 {
            let addr = self.locate(cursor);
            let strip_remain = self.strip_bytes - addr.in_strip_off;
            let take = std::cmp::min(remaining as u64, strip_remain) as usize;
            if self.data_position_failed(addr.set_idx, addr.data_pos) {
                // Degraded read: reconstruct the full strip on a temp buffer,
                // then slice out the in_strip_off..in_strip_off+take range.
                let strip_len = self.strip_bytes as usize;
                let mut tmp = vec![0u8; strip_len];
                self.reconstruct_data(
                    addr.set_idx,
                    addr.data_pos,
                    addr.in_chunklet_off - addr.in_strip_off,
                    &mut tmp,
                )?;
                buf[buf_start..buf_start + take].copy_from_slice(
                    &tmp[addr.in_strip_off as usize..addr.in_strip_off as usize + take],
                );
            } else {
                self.read_data_strip(
                    addr.set_idx,
                    addr.data_pos,
                    addr.in_chunklet_off,
                    &mut buf[buf_start..buf_start + take],
                )?;
            }
            buf_start += take;
            cursor += take as u64;
            remaining -= take;
        }
        Ok(())
    }

    fn write_at(&self, offset: u64, buf: &[u8]) -> ChunkletResult<()> {
        self.ensure_aligned(offset, buf.len())?;
        // Group the IO into per-(set, set_stripe_n) units, then for each
        // unit pick the full-stripe fast path or partial RMW.
        let mut remaining = buf.len();
        let mut cursor = offset;
        let mut buf_start = 0usize;
        while remaining > 0 {
            let addr = self.locate(cursor);
            // Bytes in this stripe still ahead of `cursor` (limited by IO end).
            let stripe_remain = self.full_stripe_bytes
                - (addr.data_pos as u64 * self.strip_bytes + addr.in_strip_off);
            let take = std::cmp::min(remaining as u64, stripe_remain) as usize;
            self.write_one_stripe_segment(
                addr,
                &buf[buf_start..buf_start + take],
            )?;
            buf_start += take;
            cursor += take as u64;
            remaining -= take;
        }
        Ok(())
    }
}

impl LdRaid5 {
    /// Write the segment `buf` that starts at `addr` and stays within a
    /// single set + set_stripe.
    ///
    /// The segment may cover anywhere from one byte of one data position up
    /// to the full stripe across all K data positions.
    fn write_one_stripe_segment(
        &self,
        start: StripeAddr,
        buf: &[u8],
    ) -> ChunkletResult<()> {
        let strip = self.strip_bytes;
        let k = self.data_per_set;

        // Decompose the segment into (data_pos, in_strip_off, len) chunks.
        let mut positions: Vec<(usize, u64, std::ops::Range<usize>)> =
            Vec::with_capacity(k);
        let mut consumed = 0usize;
        let mut cur_pos = start.data_pos;
        let mut cur_off = start.in_strip_off;
        while consumed < buf.len() {
            let strip_remain = strip - cur_off;
            let take =
                std::cmp::min((buf.len() - consumed) as u64, strip_remain) as usize;
            positions.push((cur_pos, cur_off, consumed..consumed + take));
            consumed += take;
            cur_pos += 1;
            cur_off = 0;
        }
        debug_assert_eq!(consumed, buf.len());

        // Are we doing a full-stripe write?
        // Definition: every data position is touched, each spans the entire strip.
        let is_full_stripe = positions.len() == k
            && positions.iter().all(|(_pos, off, range)| {
                *off == 0 && (range.end - range.start) as u64 == strip
            });

        if is_full_stripe {
            self.write_full_stripe(start.set_idx, start.in_chunklet_off, &positions, buf)?;
        } else {
            self.write_partial_stripe(
                start.set_idx,
                start.in_chunklet_off,
                start.set_stripe_n,
                &positions,
                buf,
            )?;
        }
        Ok(())
    }

    fn write_full_stripe(
        &self,
        set_idx: usize,
        in_chunklet_off: u64,
        positions: &[(usize, u64, std::ops::Range<usize>)],
        buf: &[u8],
    ) -> ChunkletResult<()> {
        let strip = self.strip_bytes as usize;
        let mut parity = vec![0u8; strip];
        for (pos, _off, range) in positions {
            let data = &buf[range.clone()];
            // Write the data strip first.
            self.write_data_strip(set_idx, *pos, in_chunklet_off, data)?;
            xor_into(&mut parity, data);
        }
        self.write_parity_strip(set_idx, in_chunklet_off, &parity)?;
        Ok(())
    }

    fn write_partial_stripe(
        &self,
        set_idx: usize,
        in_chunklet_off: u64,
        _set_stripe_n: u64,
        positions: &[(usize, u64, std::ops::Range<usize>)],
        buf: &[u8],
    ) -> ChunkletResult<()> {
        let strip = self.strip_bytes as usize;
        let mut delta_full_strip = vec![0u8; strip];
        // For each touched data position, read old data slice, XOR delta.
        for (pos, off, range) in positions {
            let new_data = &buf[range.clone()];
            let len = new_data.len();
            let mut old_data = vec![0u8; len];
            // Each modified data position occupies its own data chunklet,
            // at the same `in_chunklet_off` (because parity covers the
            // whole stripe row at that offset).
            self.read_data_strip(set_idx, *pos, in_chunklet_off, &mut old_data)?;
            // delta = new ^ old, stamped into the right slice of the
            // full-stripe-wide delta buffer.
            let dst = &mut delta_full_strip
                [(*off as usize)..(*off as usize) + len];
            for i in 0..len {
                dst[i] ^= old_data[i] ^ new_data[i];
            }
        }
        // Read full parity strip, XOR delta in, write back.
        let mut parity = vec![0u8; strip];
        self.read_parity_strip(set_idx, in_chunklet_off, &mut parity)?;
        xor_into(&mut parity, &delta_full_strip);

        // Write the modified data strips, then parity.
        for (pos, _off, range) in positions {
            self.write_data_strip(set_idx, *pos, in_chunklet_off, &buf[range.clone()])?;
        }
        self.write_parity_strip(set_idx, in_chunklet_off, &parity)?;
        Ok(())
    }
}

/// XOR `src` into `dst` byte-wise. Both slices must have equal length.
fn xor_into(dst: &mut [u8], src: &[u8]) {
    debug_assert_eq!(dst.len(), src.len());
    for i in 0..dst.len() {
        dst[i] ^= src[i];
    }
}

fn compute_strip_bytes(strip_size_log2: u8) -> u64 {
    if strip_size_log2 == 0 {
        BLOCK_SIZE
    } else {
        1u64 << strip_size_log2
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn xor_into_basic() {
        let mut a = vec![0xff, 0x00, 0xaa];
        let b = vec![0xf0, 0x0f, 0x55];
        xor_into(&mut a, &b);
        assert_eq!(a, vec![0x0f, 0x0f, 0xff]);
    }
}
