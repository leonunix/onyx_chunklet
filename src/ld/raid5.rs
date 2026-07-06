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
//! All write fan-outs go through `parallel_strip_writes`, which spawns one
//! thread per surviving member so the K+1 strips of a healthy R5 set hit the
//! disks in parallel. Three sub-paths feed it:
//!
//! - **Full-stripe** (offset full-stripe-aligned, len ≥ full_stripe_bytes):
//!   compute `P = D0 ^ D1 ^ … ^ D(K-1)` from the new data, write all K data
//!   strips + parity. No reads. Skips writes to failed PDs (degraded fast
//!   path).
//! - **Partial RMW**: for each touched data position,
//!     `delta_p ^= old_data ^ new_data`; `new_p = old_p ^ delta_p`. Costs
//!   M+1 reads and M+1 writes. Used only when the set is fully healthy AND
//!   the M-vs-K threshold favors RMW over RW.
//! - **Partial RW** (reconstruct-write): materialize all K new strips
//!   (read unmodified ones, reconstruct any that sit on a failed PD, copy
//!   modified bytes from `buf`), recompute parity from scratch, write
//!   modified data + parity. Costs K-M reads + M+1 writes. Used when:
//!     * `(K-M) < (M+1)` and all modified positions are full-strip aligned
//!       (the threshold where RW beats RMW), OR
//!     * the set is degraded with one data position failed (RMW can't
//!       compute correct parity when a modified position is on a failed
//!       PD).
//!
//! Degraded write tolerates **F ≤ 1** failed members (data or parity).
//! - F=1 parity-only: short-circuit to a data-only write path (no parity
//!   computation, no reads).
//! - F=1 data-only: always RW.
//! - F ≥ 2: rejected (caller must rebuild first).
//!
//! # Read path
//!
//! Healthy reads hit the data chunklet directly. Degraded reconstruction
//! (one data chunklet missing) walks the surviving data + parity strips and
//! XORs them together via `reconstruct_data`.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::Ordering;
use std::sync::Arc;

use crate::error::{ChunkletError, ChunkletResult};
use crate::ld::descriptor::LdDescriptor;
use crate::ld::gf256::xor_into;
use crate::ld::{
    compute_strip_bytes, parallel_strip_reads, parallel_strip_writes, resolve_members, LogicalDisk,
    ReconstructEngine, StripRead, StripWrite, StripeLockTable,
};
use crate::pd::PhysicalDisk;
use crate::pool::{new_rebuild_cell, PdHealth, RebuildCell};
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
    /// Per-full-stripe serialization, SHARED with the online-rebuild worker via
    /// `attach_shared` (default: a private table). RMW/RW updates parity from
    /// old stripe state, so overlapping writes to one stripe must not interleave.
    stripe_locks: Arc<StripeLockTable>,
    /// Online-rebuild plan cell (shared via `attach_shared`; default empty).
    rebuild: RebuildCell,
}

impl LdRaid5 {
    pub fn open(
        desc: LdDescriptor,
        pds: &BTreeMap<PdId, Arc<PhysicalDisk>>,
    ) -> ChunkletResult<Self> {
        let pd_health = crate::ld::healthy_pd_map(pds);
        Self::open_with_health(desc, pds, &pd_health)
    }

    pub(crate) fn open_with_health(
        desc: LdDescriptor,
        pds: &BTreeMap<PdId, Arc<PhysicalDisk>>,
        pd_health: &BTreeMap<PdId, PdHealth>,
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
        let expected =
            (desc.set_size as usize) * (desc.row_size as usize) * (desc.num_rows as usize);
        if desc.members.len() != expected {
            return Err(ChunkletError::Invariant(format!(
                "Raid5 member count {} != expected {}",
                desc.members.len(),
                expected
            )));
        }
        let strip_bytes = compute_strip_bytes(desc.strip_size_log2)?;
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
                        set_n,
                        i,
                        desc.members[base + i].role
                    )));
                }
            }
            if desc.members[base + n - 1].role != LdRole::ParityP {
                return Err(ChunkletError::Invariant(format!(
                    "Raid5 set {} last member is not ParityP: {:?}",
                    set_n,
                    desc.members[base + n - 1].role
                )));
            }
        }

        let members = resolve_members(pds, pd_health, &desc)?;
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
            stripe_locks: Arc::new(StripeLockTable::new()),
            rebuild: new_rebuild_cell(),
        })
    }

    pub fn descriptor(&self) -> &LdDescriptor {
        &self.desc
    }

    /// Install the shared stripe-lock table + rebuild cell (see `LdRaid6`).
    pub(crate) fn attach_shared(&mut self, stripe_locks: Arc<StripeLockTable>, rebuild: RebuildCell) {
        self.stripe_locks = stripe_locks;
        self.rebuild = rebuild;
    }

    fn set_being_rebuilt(&self, set_idx: usize) -> bool {
        self.rebuild
            .read()
            .as_ref()
            .map(|p| {
                p.targets_by_set
                    .get(set_idx)
                    .map(|o| o.is_some())
                    .unwrap_or(false)
            })
            .unwrap_or(false)
    }

    /// Mirror this stripe's computed strips to the online-rebuild shadow(s) for
    /// `set_idx`, below the set cursor, while holding the stripe lock. See
    /// `LdRaid6::write_forward`. `strip_for(pos_in_set)`: data `0..K`, parity=`K`.
    fn write_forward(&self, set_idx: usize, strip_base: u64, mut strip_for: impl FnMut(usize) -> Vec<u8>) {
        let guard = self.rebuild.read();
        let Some(progress) = guard.as_ref() else {
            return;
        };
        if progress.aborted.load(Ordering::Relaxed) {
            return;
        }
        let Some(sr) = progress.targets_by_set.get(set_idx).and_then(|o| o.as_ref()) else {
            return;
        };
        let set_stripe_n = strip_base / self.strip_bytes;
        if set_stripe_n >= sr.cursor.load(Ordering::Acquire) {
            return;
        }
        for shadow in &sr.shadows {
            let bytes = strip_for(shadow.pos_in_set);
            if let Err(e) =
                shadow
                    .pd
                    .write_chunklet_user(shadow.chunklet_index, strip_base, &bytes)
            {
                tracing::error!(
                    "online rebuild (r5): shadow write-forward failed (set {} pos {}): {} — aborting",
                    set_idx, shadow.pos_in_set, e
                );
                progress.aborted.store(true, Ordering::Relaxed);
                return;
            }
        }
    }

    fn ensure_aligned(&self, offset: u64, len: usize) -> ChunkletResult<()> {
        let bs = self.block_size() as u64;
        if offset % bs != 0 || (len as u64) % bs != 0 {
            return Err(ChunkletError::Invariant(format!(
                "Raid5 IO not block-aligned: offset={} len={}",
                offset, len
            )));
        }
        let end = offset
            .checked_add(len as u64)
            .ok_or_else(|| ChunkletError::Invariant("Raid5 IO offset overflow".into()))?;
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
        let row_user =
            (self.desc.row_size as u64) * (self.data_per_set as u64) * usable_per_chunklet;
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

    fn member_idx_parity(&self, set_idx: usize) -> usize {
        set_idx * (self.desc.set_size as usize) + self.data_per_set
    }

    fn stripe_key(&self, set_idx: usize, strip_base: u64) -> u64 {
        ((set_idx as u64) << 32) | (strip_base / self.strip_bytes)
    }

    fn member_pd(&self, idx: usize) -> ChunkletResult<&Arc<PhysicalDisk>> {
        self.members[idx].as_ref().ok_or_else(|| {
            ChunkletError::Invariant(format!(
                "Raid5 member idx={} on failed PD {} — caller must rebuild before writing",
                idx, self.desc.members[idx].pd
            ))
        })
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

    /// Indices of data positions whose PD is absent (subset of `0..data_per_set`).
    fn failed_data_positions(&self, set_idx: usize) -> Vec<usize> {
        (0..self.data_per_set)
            .filter(|&pos| self.data_position_failed(set_idx, pos))
            .collect()
    }

    /// True when the parity chunklet's PD is currently absent.
    fn parity_failed(&self, set_idx: usize) -> bool {
        self.members[self.member_idx_parity(set_idx)].is_none()
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
    /// Byte offset within the strip.
    in_strip_off: u64,
    /// Byte offset on the chunklet (data or parity).
    in_chunklet_off: u64,
}

impl ReconstructEngine for LdRaid5 {
    fn strip_bytes(&self) -> u64 {
        self.strip_bytes
    }
    fn stripes_per_chunklet(&self) -> u64 {
        LdRaid5::stripes_per_chunklet(self)
    }
    fn reconstruct_member_strip(
        &self,
        failed_member_idx: usize,
        in_chunklet_off: u64,
        out: &mut [u8],
    ) -> ChunkletResult<()> {
        LdRaid5::reconstruct_member_strip(self, failed_member_idx, in_chunklet_off, out)
    }
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

    fn read_many_at(&self, ops: &mut [(u64, &mut [u8])]) -> ChunkletResult<()> {
        for (offset, buf) in ops.iter() {
            self.ensure_aligned(*offset, buf.len())?;
        }
        let mut reads = Vec::with_capacity(ops.len());
        for (offset, buf) in ops.iter_mut() {
            if buf.len() != self.strip_bytes as usize {
                self.read_at(*offset, buf)?;
                continue;
            }
            let addr = self.locate(*offset);
            if addr.in_strip_off != 0 || self.data_position_failed(addr.set_idx, addr.data_pos) {
                self.read_at(*offset, buf)?;
                continue;
            }
            let m = self.member_idx_data(addr.set_idx, addr.data_pos);
            let pd = self.members[m]
                .as_ref()
                .expect("healthy read_many path requires data PD healthy");
            reads.push(StripRead {
                pd: pd.clone(),
                chunklet_index: self.desc.members[m].chunklet_index,
                in_chunklet_off: addr.in_chunklet_off,
                data: &mut **buf,
            });
        }
        parallel_strip_reads(&mut reads)
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
            self.write_one_stripe_segment(addr, &buf[buf_start..buf_start + take])?;
            buf_start += take;
            cursor += take as u64;
            remaining -= take;
        }
        Ok(())
    }

    fn write_many_at(&self, ops: &[(u64, &[u8])]) -> ChunkletResult<()> {
        self.write_many_batched(ops)
    }

    fn flush(&self) -> ChunkletResult<()> {
        crate::ld::flush_members(&self.members)
    }
}

impl LdRaid5 {
    /// Write the segment `buf` that starts at `addr` and stays within a
    /// single set + set_stripe.
    ///
    /// The segment may cover anywhere from one byte of one data position up
    /// to the full stripe across all K data positions.
    fn write_one_stripe_segment(&self, start: StripeAddr, buf: &[u8]) -> ChunkletResult<()> {
        // Compute failure pattern once and reuse for the F-budget check
        // and the path dispatch below.
        let f_data = self.failed_data_positions(start.set_idx).len();
        let p_failed = self.parity_failed(start.set_idx);
        let f_total = f_data + p_failed as usize;
        if f_total > 1 {
            return Err(ChunkletError::WriteRedundancyExceeded {
                raid: RaidLevel::Raid5,
                set_idx: start.set_idx,
                failed: f_total,
                budget: 1,
            });
        }

        let strip = self.strip_bytes;
        let k = self.data_per_set;

        // Decompose the segment into (data_pos, in_strip_off, len) chunks.
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
        debug_assert_eq!(consumed, buf.len());

        // Strip-aligned chunklet offset shared by every position + the parity
        // strip in this (set, set_stripe). Helpers below compute each
        // position's chunklet IO at `strip_base + off`. NEVER pass
        // `start.in_chunklet_off` (which embeds `start.in_strip_off`) directly
        // — that only matches pos[0] and corrupts pos[1+] / parity.
        let strip_base = start.in_chunklet_off - start.in_strip_off;
        let _stripe = self
            .stripe_locks
            .write_key(self.stripe_key(start.set_idx, strip_base));

        // Full-stripe = every data position covered, each spans the entire strip.
        let is_full_stripe = positions.len() == k
            && positions
                .iter()
                .all(|(_pos, off, range)| *off == 0 && (range.end - range.start) as u64 == strip);
        if is_full_stripe {
            return self.write_full_stripe(start.set_idx, strip_base, &positions, buf);
        }

        if f_data == 0 && p_failed && !self.set_being_rebuilt(start.set_idx) {
            // Only parity is gone: skip parity entirely, write data direct.
            // A rebuilding parity target needs parity computed to write-forward,
            // so a rebuilding set falls through to RW instead of this fast path.
            return self.write_data_only(start.set_idx, strip_base, &positions, buf);
        }

        if f_data == 0 && !p_failed {
            // Healthy set — RMW vs RW based on M-vs-K threshold.
            // RW only beats RMW when modifications are full-strip; sub-strip
            // modifications add gap-fill reads to RW that wipe out the win.
            let m = positions.len();
            let all_full_strip = positions
                .iter()
                .all(|(_p, off, range)| *off == 0 && (range.end - range.start) as u64 == strip);
            if all_full_strip && (k - m) < (m + 1) {
                self.write_partial_stripe_rw(start.set_idx, strip_base, &positions, buf)
            } else {
                self.write_partial_stripe_rmw(start.set_idx, strip_base, &positions, buf)
            }
        } else {
            // F=1 data-failed (parity healthy). RMW would need to read the
            // failed PD's old data for delta computation when that position
            // is in the modified set; RW handles it uniformly via reconstruct.
            self.write_partial_stripe_rw(start.set_idx, strip_base, &positions, buf)
        }
    }

    /// Fan out one write per surviving member, computed from the new data.
    /// Skips writes to failed PDs (degraded fast path; data parity must be
    /// rebuilt later for redundancy to come back).
    ///
    /// `strip_base` is the strip-aligned chunklet offset shared by every
    /// position and the parity strip. For full-stripe writes every position
    /// has `off == 0`, so each position's chunklet offset equals `strip_base`.
    fn write_full_stripe(
        &self,
        set_idx: usize,
        strip_base: u64,
        positions: &[(usize, u64, std::ops::Range<usize>)],
        buf: &[u8],
    ) -> ChunkletResult<()> {
        let strip = self.strip_bytes as usize;
        let mut parity = vec![0u8; strip];
        for (_pos, _off, range) in positions {
            xor_into(&mut parity, &buf[range.clone()]);
        }

        let mut ops: Vec<StripWrite> = Vec::with_capacity(positions.len() + 1);
        for (pos, off, range) in positions {
            let m = self.member_idx_data(set_idx, *pos);
            if let Some(pd) = &self.members[m] {
                ops.push(StripWrite {
                    pd: pd.clone(),
                    chunklet_index: self.desc.members[m].chunklet_index,
                    in_chunklet_off: strip_base + off,
                    data: &buf[range.clone()],
                });
            }
        }
        let pm = self.member_idx_parity(set_idx);
        if let Some(pd) = &self.members[pm] {
            ops.push(StripWrite {
                pd: pd.clone(),
                chunklet_index: self.desc.members[pm].chunklet_index,
                in_chunklet_off: strip_base,
                data: &parity,
            });
        }
        parallel_strip_writes(ops)?;
        let strip = self.strip_bytes as usize;
        self.write_forward(set_idx, strip_base, |pos| {
            if pos < self.data_per_set {
                buf[pos * strip..pos * strip + strip].to_vec()
            } else {
                parity.clone()
            }
        });
        Ok(())
    }

    /// F=1 parity-only fast path: skip parity entirely, write data only.
    /// Zero reads, ≤K writes. Parity is recovered later by rebuild.
    fn write_data_only(
        &self,
        set_idx: usize,
        strip_base: u64,
        positions: &[(usize, u64, std::ops::Range<usize>)],
        buf: &[u8],
    ) -> ChunkletResult<()> {
        let mut ops: Vec<StripWrite> = Vec::with_capacity(positions.len());
        for (pos, off, range) in positions {
            let m = self.member_idx_data(set_idx, *pos);
            // Healthy data PDs only — F=1 with parity failed means all data
            // PDs are alive.
            let pd = self.members[m].as_ref().expect(
                "write_data_only invariant: data PDs must be healthy when only parity is failed",
            );
            ops.push(StripWrite {
                pd: pd.clone(),
                chunklet_index: self.desc.members[m].chunklet_index,
                in_chunklet_off: strip_base + off,
                data: &buf[range.clone()],
            });
        }
        parallel_strip_writes(ops)
    }

    /// Read-modify-write: for M modified positions, read old slices, XOR
    /// into a delta, apply delta to old parity, write modified data + new
    /// parity. Healthy-set only — caller routes degraded sets to RW.
    ///
    /// Each position's old data is read at `strip_base + off` (the
    /// position-specific chunklet offset). Parity (full strip) lives at
    /// `strip_base`.
    fn write_partial_stripe_rmw(
        &self,
        set_idx: usize,
        strip_base: u64,
        positions: &[(usize, u64, std::ops::Range<usize>)],
        buf: &[u8],
    ) -> ChunkletResult<()> {
        let strip = self.strip_bytes as usize;
        let mut delta_full_strip = vec![0u8; strip];
        for (pos, off, range) in positions {
            let new_data = &buf[range.clone()];
            let len = new_data.len();
            let mut old_data = vec![0u8; len];
            self.read_data_strip(set_idx, *pos, strip_base + off, &mut old_data)?;
            let dst = &mut delta_full_strip[(*off as usize)..(*off as usize) + len];
            for i in 0..len {
                dst[i] ^= old_data[i] ^ new_data[i];
            }
        }
        let mut parity = vec![0u8; strip];
        self.read_parity_strip(set_idx, strip_base, &mut parity)?;
        xor_into(&mut parity, &delta_full_strip);

        let mut ops: Vec<StripWrite> = Vec::with_capacity(positions.len() + 1);
        for (pos, off, range) in positions {
            let m = self.member_idx_data(set_idx, *pos);
            let pd = self.members[m]
                .as_ref()
                .expect("RMW path requires all data PDs healthy");
            ops.push(StripWrite {
                pd: pd.clone(),
                chunklet_index: self.desc.members[m].chunklet_index,
                in_chunklet_off: strip_base + off,
                data: &buf[range.clone()],
            });
        }
        let pm = self.member_idx_parity(set_idx);
        let pd = self.members[pm]
            .as_ref()
            .expect("RMW path requires parity PD healthy");
        ops.push(StripWrite {
            pd: pd.clone(),
            chunklet_index: self.desc.members[pm].chunklet_index,
            in_chunklet_off: strip_base,
            data: &parity,
        });
        parallel_strip_writes(ops)
    }

    /// Reconstruct-write: build full new strips for all K data positions
    /// (read unmodified, reconstruct unmodified-failed via parity, copy
    /// modified bytes into old/reconstructed strip for partial mods),
    /// recompute parity from scratch, write modified data + new parity.
    /// Skips writes to failed PDs.
    ///
    /// Precondition: parity PD is healthy. Caller routes parity-failed sets
    /// to `write_data_only` (no parity computation needed at all).
    ///
    /// All full-strip reads/reconstructs use `strip_base` (strip-aligned
    /// chunklet offset). Modified data writes use `strip_base + off` for
    /// each position; parity writes the full strip at `strip_base`.
    fn write_partial_stripe_rw(
        &self,
        set_idx: usize,
        strip_base: u64,
        positions: &[(usize, u64, std::ops::Range<usize>)],
        buf: &[u8],
    ) -> ChunkletResult<()> {
        let strip = self.strip_bytes as usize;
        let k = self.data_per_set;

        let modified_map: HashMap<usize, (u64, std::ops::Range<usize>)> = positions
            .iter()
            .map(|(p, off, r)| (*p, (*off, r.clone())))
            .collect();

        let mut new_strips: Vec<Vec<u8>> = (0..k).map(|_| vec![0u8; strip]).collect();
        for pos in 0..k {
            let pd_failed = self.data_position_failed(set_idx, pos);
            match modified_map.get(&pos) {
                Some((off, range)) => {
                    let new_data = &buf[range.clone()];
                    let off = *off as usize;
                    let len = new_data.len();
                    if off == 0 && len == strip {
                        new_strips[pos].copy_from_slice(new_data);
                    } else {
                        if pd_failed {
                            self.reconstruct_data(set_idx, pos, strip_base, &mut new_strips[pos])?;
                        } else {
                            self.read_data_strip(set_idx, pos, strip_base, &mut new_strips[pos])?;
                        }
                        new_strips[pos][off..off + len].copy_from_slice(new_data);
                    }
                }
                None => {
                    if pd_failed {
                        self.reconstruct_data(set_idx, pos, strip_base, &mut new_strips[pos])?;
                    } else {
                        self.read_data_strip(set_idx, pos, strip_base, &mut new_strips[pos])?;
                    }
                }
            }
        }

        let mut parity = vec![0u8; strip];
        for s in &new_strips {
            xor_into(&mut parity, s);
        }

        let mut ops: Vec<StripWrite> = Vec::with_capacity(positions.len() + 1);
        for (pos, off, range) in positions {
            let m = self.member_idx_data(set_idx, *pos);
            if let Some(pd) = &self.members[m] {
                ops.push(StripWrite {
                    pd: pd.clone(),
                    chunklet_index: self.desc.members[m].chunklet_index,
                    in_chunklet_off: strip_base + off,
                    data: &buf[range.clone()],
                });
            }
        }
        // Parity may be the failed member during an online rebuild (routed here
        // instead of write_data_only so `parity` is computed for write-forward);
        // skip the live write when it is down, exactly like a failed data pos.
        let pm = self.member_idx_parity(set_idx);
        if let Some(pd) = &self.members[pm] {
            ops.push(StripWrite {
                pd: pd.clone(),
                chunklet_index: self.desc.members[pm].chunklet_index,
                in_chunklet_off: strip_base,
                data: &parity,
            });
        }
        parallel_strip_writes(ops)?;
        self.write_forward(set_idx, strip_base, |pos| {
            if pos < self.data_per_set {
                new_strips[pos].clone()
            } else {
                parity.clone()
            }
        });
        Ok(())
    }
}

/// Which healthy-set write path a batched segment takes. Degraded segments
/// bail the whole batch to the serial `write_at` path (which owns the
/// reconstruct plumbing) before reaching here.
#[derive(Clone, Copy)]
enum Kind5 {
    Full,
    Rmw,
    Rw,
}

/// One planned stripe segment of a batched `write_many_at`. Owns the scratch
/// buffers that span the read → compute → write phases so the whole batch
/// issues ONE `parallel_strip_reads` + ONE `parallel_strip_writes` instead of a
/// serial submit pair per segment (the flusher→RAID de-batching that turned µs
/// disk IO into ~200 ms wall-clock — see `LdRaid6::write_many_batched`).
struct Seg5<'a> {
    set_idx: usize,
    strip_base: u64,
    kind: Kind5,
    /// (data_pos, in_strip_off, new_data) per modified position, stripe order.
    mods: Vec<(usize, u64, &'a [u8])>,
    /// New parity strip. Full/Rw compute it from scratch; Rmw reads the old
    /// value here then applies the delta in place.
    parity: Vec<u8>,
    /// Rmw only: old data per modified position (each sized to its mod's len).
    old_data: Vec<Vec<u8>>,
    /// Rw only: K full new-data strips, parity recomputed from them.
    new_strips: Vec<Vec<u8>>,
}

impl LdRaid5 {
    /// Batched multi-op writer — the flusher's hot path. Groups every op's
    /// stripe segments BY physical stripe (merging colliding segments into one
    /// write), then collapses the batch into a single cross-PD read submit + a
    /// single write submit under one stripe-lock acquisition. Merging turns
    /// dense sub-stripe units into zero-RMW full-stripe writes. Bails to serial
    /// `write_at` only for a degraded set or a byte-overlapping stripe.
    fn write_many_batched(&self, ops: &[(u64, &[u8])]) -> ChunkletResult<()> {
        for (offset, buf) in ops {
            self.ensure_aligned(*offset, buf.len())?;
        }
        let strip = self.strip_bytes as usize;
        let k = self.data_per_set;

        // Phase 0: decompose every op into per-position mods, GROUPED by
        // physical stripe. Segments colliding on one stripe are MERGED into a
        // single stripe write (their disjoint sub-ranges combine) instead of
        // bailing the whole batch to serial; a fully-covered stripe is promoted
        // to a zero-RMW full-stripe write. See LdRaid6::write_many_batched.
        struct RawStripe<'a> {
            set_idx: usize,
            strip_base: u64,
            mods: Vec<(usize, u64, &'a [u8])>,
        }
        let mut by_stripe: std::collections::BTreeMap<u64, RawStripe> =
            std::collections::BTreeMap::new();
        let mut serialize = false;
        'outer: for (offset, buf) in ops {
            let mut remaining = buf.len();
            let mut cursor = *offset;
            let mut buf_start = 0usize;
            while remaining > 0 {
                let addr = self.locate(cursor);
                let stripe_remain = self.full_stripe_bytes
                    - (addr.data_pos as u64 * self.strip_bytes + addr.in_strip_off);
                let take = std::cmp::min(remaining as u64, stripe_remain) as usize;
                let seg_buf = &buf[buf_start..buf_start + take];

                // Degraded set → the serial path owns reconstruct reads.
                if !self.failed_data_positions(addr.set_idx).is_empty()
                    || self.parity_failed(addr.set_idx)
                {
                    serialize = true;
                    break 'outer;
                }

                let strip_base = addr.in_chunklet_off - addr.in_strip_off;
                let key = self.stripe_key(addr.set_idx, strip_base);
                let entry = by_stripe.entry(key).or_insert_with(|| RawStripe {
                    set_idx: addr.set_idx,
                    strip_base,
                    mods: Vec::new(),
                });
                let mut consumed = 0usize;
                let mut cur_pos = addr.data_pos;
                let mut cur_off = addr.in_strip_off;
                while consumed < seg_buf.len() {
                    let strip_remain = self.strip_bytes - cur_off;
                    let t =
                        std::cmp::min((seg_buf.len() - consumed) as u64, strip_remain) as usize;
                    entry
                        .mods
                        .push((cur_pos, cur_off, &seg_buf[consumed..consumed + t]));
                    consumed += t;
                    cur_pos += 1;
                    cur_off = 0;
                }

                buf_start += take;
                cursor += take as u64;
                remaining -= take;
            }
        }

        if serialize {
            for (offset, buf) in ops {
                self.write_at(*offset, buf)?;
            }
            return Ok(());
        }

        // Phase 0b: one merged Seg5 per stripe. Byte-disjointness check (overlap
        // → serial last-writer-wins). Promote a fully-covered stripe to Full
        // (zero RMW); never promote a partially-covered stripe (would clobber
        // unmodified positions).
        let mut segs: Vec<Seg5> = Vec::with_capacity(by_stripe.len());
        let mut stripe_keys: Vec<u64> = Vec::with_capacity(by_stripe.len());
        for (key, raw) in by_stripe {
            let mut spans: Vec<(u64, u64)> = raw
                .mods
                .iter()
                .map(|(p, off, nd)| {
                    let s = *p as u64 * self.strip_bytes + off;
                    (s, s + nd.len() as u64)
                })
                .collect();
            spans.sort_unstable_by_key(|(s, _)| *s);
            if spans.windows(2).any(|w| w[0].1 > w[1].0) {
                for (offset, buf) in ops {
                    self.write_at(*offset, buf)?;
                }
                return Ok(());
            }

            let mut clean_full = raw.mods.len() == k;
            if clean_full {
                let mut seen = vec![false; k];
                for (p, off, nd) in &raw.mods {
                    if *off != 0 || nd.len() != strip || *p >= k || seen[*p] {
                        clean_full = false;
                        break;
                    }
                    seen[*p] = true;
                }
            }
            let kind = if clean_full {
                Kind5::Full
            } else {
                // Same RMW-vs-RW threshold as the serial path, on merged mods:
                // RW only wins when every mod is full-strip and (K-M) < (M+1).
                let m = raw.mods.len();
                let all_full_strip = raw
                    .mods
                    .iter()
                    .all(|(_p, off, nd)| *off == 0 && nd.len() as u64 == self.strip_bytes);
                if all_full_strip && (k - m.min(k)) < (m + 1) {
                    Kind5::Rw
                } else {
                    Kind5::Rmw
                }
            };
            let old_data = match kind {
                Kind5::Rmw => raw
                    .mods
                    .iter()
                    .map(|(_p, _o, nd)| vec![0u8; nd.len()])
                    .collect(),
                _ => Vec::new(),
            };
            let new_strips = match kind {
                Kind5::Rw => (0..k).map(|_| vec![0u8; strip]).collect(),
                _ => Vec::new(),
            };
            stripe_keys.push(key);
            segs.push(Seg5 {
                set_idx: raw.set_idx,
                strip_base: raw.strip_base,
                kind,
                mods: raw.mods,
                parity: vec![0u8; strip],
                old_data,
                new_strips,
            });
        }

        let _guards = self.stripe_locks.write_keys(&stripe_keys);

        // Phase 1: one batched read submit for every segment's RMW reads.
        let mut reads: Vec<StripRead> = Vec::new();
        for seg in segs.iter_mut() {
            self.r5_collect_reads(seg, &mut reads);
        }
        parallel_strip_reads(&mut reads)?;
        drop(reads);

        // Phase 2: recompute parity per segment from the freshly-read state.
        for seg in segs.iter_mut() {
            self.r5_compute(seg);
        }

        // Phase 3: one batched write submit for all data + parity strips.
        let mut writes: Vec<StripWrite> = Vec::new();
        for seg in segs.iter() {
            self.r5_collect_writes(seg, &mut writes);
        }
        parallel_strip_writes(writes)
    }

    /// Append `seg`'s RMW reads (into its owned scratch) to the shared batch.
    fn r5_collect_reads<'s>(&self, seg: &'s mut Seg5<'_>, reads: &mut Vec<StripRead<'s>>) {
        let set_idx = seg.set_idx;
        let strip_base = seg.strip_base;
        let strip = self.strip_bytes as usize;
        match seg.kind {
            Kind5::Full => {}
            Kind5::Rmw => {
                let Seg5 {
                    mods,
                    old_data,
                    parity,
                    ..
                } = &mut *seg;
                for ((pos, off, _nd), old) in mods.iter().zip(old_data.iter_mut()) {
                    let m = self.member_idx_data(set_idx, *pos);
                    let pd = self.members[m].as_ref().expect("healthy RMW data PD");
                    reads.push(StripRead {
                        pd: pd.clone(),
                        chunklet_index: self.desc.members[m].chunklet_index,
                        in_chunklet_off: strip_base + off,
                        data: old.as_mut_slice(),
                    });
                }
                let pm = self.member_idx_parity(set_idx);
                let pdp = self.members[pm].as_ref().expect("healthy RMW parity");
                reads.push(StripRead {
                    pd: pdp.clone(),
                    chunklet_index: self.desc.members[pm].chunklet_index,
                    in_chunklet_off: strip_base,
                    data: parity.as_mut_slice(),
                });
            }
            Kind5::Rw => {
                let Seg5 {
                    mods, new_strips, ..
                } = &mut *seg;
                for (pos, strip_buf) in new_strips.iter_mut().enumerate() {
                    let full_mod = mods
                        .iter()
                        .any(|(p, off, nd)| *p == pos && *off == 0 && nd.len() == strip);
                    if full_mod {
                        continue;
                    }
                    let m = self.member_idx_data(set_idx, pos);
                    let pd = self.members[m].as_ref().expect("healthy RW data PD");
                    reads.push(StripRead {
                        pd: pd.clone(),
                        chunklet_index: self.desc.members[m].chunklet_index,
                        in_chunklet_off: strip_base,
                        data: strip_buf.as_mut_slice(),
                    });
                }
            }
        }
    }

    /// Recompute the segment's parity from the read-phase results.
    fn r5_compute(&self, seg: &mut Seg5<'_>) {
        let strip = self.strip_bytes as usize;
        match seg.kind {
            Kind5::Full => {
                seg.parity.iter_mut().for_each(|b| *b = 0);
                for &(_pos, _off, nd) in &seg.mods {
                    xor_into(&mut seg.parity, nd);
                }
            }
            Kind5::Rmw => {
                let mut delta = vec![0u8; strip];
                for ((_pos, off, nd), old) in seg.mods.iter().zip(seg.old_data.iter()) {
                    let off = *off as usize;
                    for i in 0..nd.len() {
                        delta[off + i] ^= old[i] ^ nd[i];
                    }
                }
                xor_into(&mut seg.parity, &delta);
            }
            Kind5::Rw => {
                for &(pos, off, nd) in &seg.mods {
                    let off = off as usize;
                    if off == 0 && nd.len() == strip {
                        seg.new_strips[pos].copy_from_slice(nd);
                    } else {
                        seg.new_strips[pos][off..off + nd.len()].copy_from_slice(nd);
                    }
                }
                seg.parity.iter_mut().for_each(|b| *b = 0);
                for s in seg.new_strips.iter() {
                    xor_into(&mut seg.parity, s);
                }
            }
        }
    }

    /// Append `seg`'s data + parity writes to the shared batch. Data payloads
    /// borrow the caller's buffers; parity borrows the computed scratch.
    fn r5_collect_writes<'s>(&self, seg: &'s Seg5<'s>, writes: &mut Vec<StripWrite<'s>>) {
        let set_idx = seg.set_idx;
        let strip_base = seg.strip_base;
        for &(pos, off, nd) in &seg.mods {
            let m = self.member_idx_data(set_idx, pos);
            let pd = self.members[m].as_ref().expect("healthy write data PD");
            writes.push(StripWrite {
                pd: pd.clone(),
                chunklet_index: self.desc.members[m].chunklet_index,
                in_chunklet_off: strip_base + off,
                data: nd,
            });
        }
        let pm = self.member_idx_parity(set_idx);
        let pdp = self.members[pm].as_ref().expect("healthy write parity");
        writes.push(StripWrite {
            pd: pdp.clone(),
            chunklet_index: self.desc.members[pm].chunklet_index,
            in_chunklet_off: strip_base,
            data: &seg.parity,
        });
    }
}
