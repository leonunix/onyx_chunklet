//! `LdRaid6` — N data + 2 parity (P, Q) per set, Anvin convention.
//!
//! # Layout
//!
//! - `set_size = K + 2` chunklets per RAID-6 set: K data + 1 P + 1 Q.
//! - Member order within a set: `[D_0, D_1, ..., D_(K-1), P, Q]`.
//! - `row_size`, `num_rows`, full-stripe striping across sets — same as
//!   `LdRaid5`.
//!
//! # Parity formulas (Anvin convention, GF(2^8) with poly 0x11d)
//!
//! - `P = D_0 ⊕ D_1 ⊕ … ⊕ D_(K-1)`
//! - `Q = g^0·D_0 ⊕ g^1·D_1 ⊕ … ⊕ g^(K-1)·D_(K-1)` (g = 2)
//!
//! # Write paths
//!
//! - **Full-stripe**: P + Q encoded directly from new data; K + 2 writes.
//! - **Partial RMW**: per touched data position i,
//!     `delta = old_D_i ⊕ new_D_i`
//!     `delta_P ⊕= delta`
//!     `delta_Q ⊕= g^i · delta`
//!   then `new_P = old_P ⊕ delta_P`, `new_Q = old_Q ⊕ delta_Q`. Reads:
//!   M old data + old P + old Q. Writes: M new data + new P + new Q.
//!
//! # Reconstruct (used by Phase 5 degraded reads)
//!
//! - **1 missing data position**: pure XOR via P, identical to RAID-5.
//! - **1 missing parity**: re-encode that parity from data.
//! - **2 missing data positions** (positions x < y): solve the PQ system
//!   per Anvin §4. With the simplifications:
//!     `P_xy = ⊕_{i ∉ {x,y}} D_i`
//!     `Q_xy = ⊕_{i ∉ {x,y}} g^i · D_i`
//!     `Pd = P ⊕ P_xy = D_x ⊕ D_y`
//!     `Qd = Q ⊕ Q_xy = g^x · D_x ⊕ g^y · D_y`
//!   Solving:
//!     `D_x = (g^(255-y+x) · Pd ⊕ g^(255-y) · Qd) · (1 / (1 ⊕ g^(x-y)))`
//!   Then `D_y = D_x ⊕ Pd`. We pre-compute the (1 / (1 ⊕ g^(x-y))) factor.
//! - **1 data + parity** missing: rebuild data from the surviving parity, then
//!   re-encode the missing parity. Implemented as two passes.

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::error::{ChunkletError, ChunkletResult};
use crate::ld::descriptor::LdDescriptor;
use crate::ld::gf256;
use crate::ld::{resolve_members, LogicalDisk};
use crate::pd::PhysicalDisk;
use crate::types::{
    LdId, LdRole, PdId, RaidLevel, BLOCK_SIZE, CHUNKLET_HEADER_BYTES, CHUNKLET_SIZE,
};

const CHUNKLET_USER_BYTES: u64 = CHUNKLET_SIZE - CHUNKLET_HEADER_BYTES;

pub struct LdRaid6 {
    desc: LdDescriptor,
    members: Vec<Arc<PhysicalDisk>>,
    capacity: u64,
    strip_bytes: u64,
    data_per_set: usize,
    full_stripe_bytes: u64,
}

impl LdRaid6 {
    pub fn open(
        desc: LdDescriptor,
        pds: &BTreeMap<PdId, Arc<PhysicalDisk>>,
    ) -> ChunkletResult<Self> {
        if desc.raid_level != RaidLevel::Raid6 {
            return Err(ChunkletError::Invariant(format!(
                "LdRaid6::open with raid_level={:?}",
                desc.raid_level
            )));
        }
        if desc.set_size < 4 {
            return Err(ChunkletError::Invariant(format!(
                "Raid6 set_size must be >= 4 (>= 2+2), got {}",
                desc.set_size
            )));
        }
        let expected = (desc.set_size as usize)
            * (desc.row_size as usize)
            * (desc.num_rows as usize);
        if desc.members.len() != expected {
            return Err(ChunkletError::Invariant(format!(
                "Raid6 member count {} != expected {}",
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
        let data_per_set = (desc.set_size - 2) as usize;
        let full_stripe_bytes = (data_per_set as u64) * strip_bytes;

        let n = desc.set_size as usize;
        for set_n in 0..((desc.row_size as usize) * (desc.num_rows as usize)) {
            let base = set_n * n;
            for i in 0..(n - 2) {
                if desc.members[base + i].role != LdRole::Data {
                    return Err(ChunkletError::Invariant(format!(
                        "Raid6 set {} member {} must be Data role: {:?}",
                        set_n, i, desc.members[base + i].role
                    )));
                }
            }
            if desc.members[base + n - 2].role != LdRole::ParityP {
                return Err(ChunkletError::Invariant(format!(
                    "Raid6 set {} P slot is not ParityP: {:?}",
                    set_n, desc.members[base + n - 2].role
                )));
            }
            if desc.members[base + n - 1].role != LdRole::ParityQ {
                return Err(ChunkletError::Invariant(format!(
                    "Raid6 set {} Q slot is not ParityQ: {:?}",
                    set_n, desc.members[base + n - 1].role
                )));
            }
        }

        let usable_per_chunklet = (CHUNKLET_USER_BYTES / strip_bytes) * strip_bytes;
        let members = resolve_members(pds, &desc)?;
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
                "Raid6 IO not block-aligned: offset={} len={}",
                offset, len
            )));
        }
        let end = offset.checked_add(len as u64).ok_or_else(|| {
            ChunkletError::Invariant("Raid6 IO offset overflow".into())
        })?;
        if end > self.capacity {
            return Err(ChunkletError::Invariant(format!(
                "Raid6 IO out of range: offset={} len={} capacity={}",
                offset, len, self.capacity
            )));
        }
        Ok(())
    }

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
            in_strip_off,
            in_chunklet_off,
        }
    }

    fn member_idx_data(&self, set_idx: usize, data_pos: usize) -> usize {
        set_idx * (self.desc.set_size as usize) + data_pos
    }

    fn member_idx_p(&self, set_idx: usize) -> usize {
        set_idx * (self.desc.set_size as usize) + self.data_per_set
    }

    fn member_idx_q(&self, set_idx: usize) -> usize {
        set_idx * (self.desc.set_size as usize) + self.data_per_set + 1
    }

    fn read_data_strip(
        &self,
        set_idx: usize,
        data_pos: usize,
        in_chunklet_off: u64,
        bytes: &mut [u8],
    ) -> ChunkletResult<()> {
        let m = self.member_idx_data(set_idx, data_pos);
        let chunklet_idx = self.desc.members[m].chunklet_index;
        self.members[m].read_chunklet_user(chunklet_idx, in_chunklet_off, bytes)
    }

    fn write_data_strip(
        &self,
        set_idx: usize,
        data_pos: usize,
        in_chunklet_off: u64,
        bytes: &[u8],
    ) -> ChunkletResult<()> {
        let m = self.member_idx_data(set_idx, data_pos);
        let chunklet_idx = self.desc.members[m].chunklet_index;
        self.members[m].write_chunklet_user(chunklet_idx, in_chunklet_off, bytes)
    }

    fn read_p(&self, set_idx: usize, off: u64, bytes: &mut [u8]) -> ChunkletResult<()> {
        let m = self.member_idx_p(set_idx);
        let chunklet_idx = self.desc.members[m].chunklet_index;
        self.members[m].read_chunklet_user(chunklet_idx, off, bytes)
    }

    fn read_q(&self, set_idx: usize, off: u64, bytes: &mut [u8]) -> ChunkletResult<()> {
        let m = self.member_idx_q(set_idx);
        let chunklet_idx = self.desc.members[m].chunklet_index;
        self.members[m].read_chunklet_user(chunklet_idx, off, bytes)
    }

    fn write_p(&self, set_idx: usize, off: u64, bytes: &[u8]) -> ChunkletResult<()> {
        let m = self.member_idx_p(set_idx);
        let chunklet_idx = self.desc.members[m].chunklet_index;
        self.members[m].write_chunklet_user(chunklet_idx, off, bytes)
    }

    fn write_q(&self, set_idx: usize, off: u64, bytes: &[u8]) -> ChunkletResult<()> {
        let m = self.member_idx_q(set_idx);
        let chunklet_idx = self.desc.members[m].chunklet_index;
        self.members[m].write_chunklet_user(chunklet_idx, off, bytes)
    }

    /// Reconstruct one missing data position via P (XOR of surviving data
    /// strips with P). Identical to RAID-5's reconstruct.
    pub fn reconstruct_one_data(
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
        self.read_p(set_idx, in_chunklet_off, out)?;
        let mut tmp = vec![0u8; out.len()];
        for pos in 0..self.data_per_set {
            if pos == missing_data_pos {
                continue;
            }
            self.read_data_strip(set_idx, pos, in_chunklet_off, &mut tmp)?;
            gf256::xor_into(out, &tmp);
        }
        Ok(())
    }

    /// Reconstruct two missing data positions x < y via the PQ system.
    /// Outputs `(D_x, D_y)`.
    pub fn reconstruct_two_data(
        &self,
        set_idx: usize,
        x: usize,
        y: usize,
        in_chunklet_off: u64,
        strip_len: usize,
    ) -> ChunkletResult<(Vec<u8>, Vec<u8>)> {
        if !(x < y && y < self.data_per_set) {
            return Err(ChunkletError::Invariant(format!(
                "two-failure positions invalid: x={} y={} K={}",
                x, y, self.data_per_set
            )));
        }
        // Compute Pd = ⊕ surviving D_i ⊕ P,  Qd = ⊕ g^i·surviving D_i ⊕ Q.
        let mut pd = vec![0u8; strip_len];
        let mut qd = vec![0u8; strip_len];
        let mut tmp = vec![0u8; strip_len];

        self.read_p(set_idx, in_chunklet_off, &mut pd)?;
        self.read_q(set_idx, in_chunklet_off, &mut qd)?;
        for i in 0..self.data_per_set {
            if i == x || i == y {
                continue;
            }
            self.read_data_strip(set_idx, i, in_chunklet_off, &mut tmp)?;
            gf256::xor_into(&mut pd, &tmp);
            gf256::mul_xor_into(&mut qd, &tmp, gf256::g_pow(i));
        }

        // Solve:
        //   Pd = D_x ⊕ D_y
        //   Qd = g^x · D_x ⊕ g^y · D_y
        // ⇒ D_x = (g^(-y) · Qd ⊕ g^(x-y) · Pd) · 1 / (g^(x-y) ⊕ 1)  -- nope, let me redo
        //
        // From Pd: D_y = D_x ⊕ Pd
        // Substitute into Qd:
        //   Qd = g^x · D_x ⊕ g^y · (D_x ⊕ Pd)
        //      = (g^x ⊕ g^y) · D_x ⊕ g^y · Pd
        // ⇒ D_x = (Qd ⊕ g^y · Pd) · 1 / (g^x ⊕ g^y)
        let gx = gf256::g_pow(x);
        let gy = gf256::g_pow(y);
        let denom = gx ^ gy;
        let denom_inv = gf256::inv(denom);

        let mut dx = vec![0u8; strip_len];
        for i in 0..strip_len {
            let qbyte = qd[i] ^ gf256::mul(gy, pd[i]);
            dx[i] = gf256::mul(qbyte, denom_inv);
        }
        let mut dy = pd; // re-use
        // dy currently = Pd; we want dy = D_x ⊕ Pd. So XOR dx into dy.
        gf256::xor_into(&mut dy, &dx);

        Ok((dx, dy))
    }
}

#[derive(Clone, Copy, Debug)]
struct StripeAddr {
    set_idx: usize,
    data_pos: usize,
    in_strip_off: u64,
    in_chunklet_off: u64,
}

impl LogicalDisk for LdRaid6 {
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
            self.read_data_strip(
                addr.set_idx,
                addr.data_pos,
                addr.in_chunklet_off,
                &mut buf[buf_start..buf_start + take],
            )?;
            buf_start += take;
            cursor += take as u64;
            remaining -= take;
        }
        Ok(())
    }

    fn write_at(&self, offset: u64, buf: &[u8]) -> ChunkletResult<()> {
        self.ensure_aligned(offset, buf.len())?;
        let mut remaining = buf.len();
        let mut cursor = offset;
        let mut buf_start = 0usize;
        while remaining > 0 {
            let addr = self.locate(cursor);
            let stripe_remain = self.full_stripe_bytes
                - (addr.data_pos as u64 * self.strip_bytes + addr.in_strip_off);
            let take = std::cmp::min(remaining as u64, stripe_remain) as usize;
            self.write_one_stripe_segment(addr, &buf[buf_start..buf_start + take])?;
            buf_start += take;
            cursor += take as u64;
            remaining -= take;
        }
        Ok(())
    }
}

impl LdRaid6 {
    fn write_one_stripe_segment(
        &self,
        start: StripeAddr,
        buf: &[u8],
    ) -> ChunkletResult<()> {
        let strip = self.strip_bytes;
        let k = self.data_per_set;
        let mut positions: Vec<(usize, u64, std::ops::Range<usize>)> = Vec::with_capacity(k);
        let mut consumed = 0usize;
        let mut cur_pos = start.data_pos;
        let mut cur_off = start.in_strip_off;
        while consumed < buf.len() {
            let strip_remain = strip - cur_off;
            let take = std::cmp::min((buf.len() - consumed) as u64, strip_remain) as usize;
            positions.push((cur_pos, cur_off, consumed..consumed + take));
            consumed += take;
            cur_pos += 1;
            cur_off = 0;
        }

        let is_full_stripe = positions.len() == k
            && positions.iter().all(|(_p, off, range)| {
                *off == 0 && (range.end - range.start) as u64 == strip
            });

        if is_full_stripe {
            self.write_full_stripe(start.set_idx, start.in_chunklet_off, &positions, buf)?;
        } else {
            self.write_partial_stripe(start.set_idx, start.in_chunklet_off, &positions, buf)?;
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
        let mut p = vec![0u8; strip];
        let mut q = vec![0u8; strip];
        for (pos, _off, range) in positions {
            let data = &buf[range.clone()];
            self.write_data_strip(set_idx, *pos, in_chunklet_off, data)?;
            gf256::xor_into(&mut p, data);
            gf256::mul_xor_into(&mut q, data, gf256::g_pow(*pos));
        }
        self.write_p(set_idx, in_chunklet_off, &p)?;
        self.write_q(set_idx, in_chunklet_off, &q)?;
        Ok(())
    }

    fn write_partial_stripe(
        &self,
        set_idx: usize,
        in_chunklet_off: u64,
        positions: &[(usize, u64, std::ops::Range<usize>)],
        buf: &[u8],
    ) -> ChunkletResult<()> {
        let strip = self.strip_bytes as usize;
        let mut delta_p = vec![0u8; strip];
        let mut delta_q = vec![0u8; strip];
        for (pos, off, range) in positions {
            let new_data = &buf[range.clone()];
            let len = new_data.len();
            let mut old_data = vec![0u8; len];
            self.read_data_strip(set_idx, *pos, in_chunklet_off, &mut old_data)?;
            let g_i = gf256::g_pow(*pos);
            let dst_p = &mut delta_p[(*off as usize)..(*off as usize) + len];
            let dst_q = &mut delta_q[(*off as usize)..(*off as usize) + len];
            for i in 0..len {
                let d = old_data[i] ^ new_data[i];
                dst_p[i] ^= d;
                dst_q[i] ^= gf256::mul(g_i, d);
            }
        }
        let mut p = vec![0u8; strip];
        let mut q = vec![0u8; strip];
        self.read_p(set_idx, in_chunklet_off, &mut p)?;
        self.read_q(set_idx, in_chunklet_off, &mut q)?;
        gf256::xor_into(&mut p, &delta_p);
        gf256::xor_into(&mut q, &delta_q);

        for (pos, _off, range) in positions {
            self.write_data_strip(set_idx, *pos, in_chunklet_off, &buf[range.clone()])?;
        }
        self.write_p(set_idx, in_chunklet_off, &p)?;
        self.write_q(set_idx, in_chunklet_off, &q)?;
        Ok(())
    }
}

fn compute_strip_bytes(strip_size_log2: u8) -> u64 {
    if strip_size_log2 == 0 {
        BLOCK_SIZE
    } else {
        1u64 << strip_size_log2
    }
}
