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
use crate::ld::degrade::{absorb_degraded, is_runtime_read_fault, SuspectMember};
use crate::ld::{
    compute_strip_bytes, parallel_strip_reads, resolve_members, submit_strip_writes_detailed,
    LogicalDisk, ReconstructEngine, StripRead, StripWrite, StripeLockTable,
};
use crate::pd::PhysicalDisk;
use crate::pool::{new_rebuild_cell, PdHealth, RebuildCell};
use crossbeam_channel::Sender;
use crate::types::{
    LdId, LdRole, PdId, RaidLevel, BLOCK_SIZE, CHUNKLET_HEADER_BYTES, CHUNKLET_SIZE,
};

mod batched;
mod reconstruct;
mod write_paths;

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
    /// Fast-isolation channel (shared via `attach_shared`; `None` for a bare
    /// handle). An inline-degraded write (F ≤ 1 failed) reports the failed
    /// member here for fast isolation + rebuild.
    suspect_tx: Option<Sender<SuspectMember>>,
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
            suspect_tx: None,
        })
    }

    pub fn descriptor(&self) -> &LdDescriptor {
        &self.desc
    }

    /// Install the shared stripe-lock table + rebuild cell (see `LdRaid6`) plus
    /// the pool's fast-isolation sender.
    pub(crate) fn attach_shared(
        &mut self,
        stripe_locks: Arc<StripeLockTable>,
        rebuild: RebuildCell,
        suspect_tx: Sender<SuspectMember>,
    ) {
        self.stripe_locks = stripe_locks;
        self.rebuild = rebuild;
        self.suspect_tx = Some(suspect_tx);
    }

    /// Best-effort report of members that failed a write but were absorbed by
    /// surviving parity/data (see `LdRaid6::report_suspects`).
    fn report_suspects(&self, suspects: Vec<SuspectMember>) {
        if let Some(tx) = &self.suspect_tx {
            for s in suspects {
                let _ = tx.try_send(s);
            }
        }
    }

    /// Submit ONE set's strip writes with inline-degrade (see
    /// `LdRaid6::submit_set_absorb`). `max_fail = 1 − open_time_failures` for R5.
    fn submit_set_absorb(&self, ops: Vec<StripWrite<'_>>, max_fail: u32) -> ChunkletResult<()> {
        let results = submit_strip_writes_detailed(&ops);
        let group_of = vec![0u32; ops.len()];
        let suspects = absorb_degraded(&ops, &results, &group_of, &[max_fail])?;
        self.report_suspects(suspects);
        Ok(())
    }

    /// Report a member that faulted on the READ side (dead-but-not-yet-isolated),
    /// so a fault surfaced only by reads still triggers fast isolation. Best
    /// effort; a missing channel is ignored.
    fn report_read_suspect(&self, member_idx: usize) {
        if let Some(pd) = self.members[member_idx].as_ref() {
            self.report_suspects(vec![SuspectMember {
                pd_id: pd.pd_id(),
                chunklet_index: self.desc.members[member_idx].chunklet_index,
            }]);
        }
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
                match self.read_data_strip(
                    addr.set_idx,
                    addr.data_pos,
                    addr.in_chunklet_off,
                    &mut buf[buf_start..buf_start + take],
                ) {
                    Ok(()) => {}
                    Err(e) if is_runtime_read_fault(&e) => {
                        // The data member faulted although the LD still believes
                        // it healthy (dead-but-not-yet-isolated). Within R5's
                        // budget of 1 — i.e. no data position is already
                        // open-failed — reconstruct the strip from parity + the
                        // surviving data. A 2nd fault (open-failed member, or a
                        // fault on parity/another strip mid-reconstruct) surfaces
                        // the error (over budget). Emit a suspect so the fault
                        // also drives isolation from the read side.
                        if !self.failed_data_positions(addr.set_idx).is_empty() {
                            return Err(e);
                        }
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
                        self.report_read_suspect(self.member_idx_data(addr.set_idx, addr.data_pos));
                    }
                    Err(e) => return Err(e),
                }
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
        // Parallel to `reads`: (set_idx, data_pos) so the reconstruct-on-EIO
        // fallback can rebuild a faulting strip from parity + survivors. Every
        // batched read is a full strip at a strip boundary of a healthy data
        // position, so reconstruct writes straight into the read's buffer.
        let mut ctxs: Vec<(usize, usize)> = Vec::new();
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
            ctxs.push((addr.set_idx, addr.data_pos));
        }
        match parallel_strip_reads(&mut reads) {
            Ok(()) => Ok(()),
            Err(e) if is_runtime_read_fault(&e) => self.reconstruct_read_batch(&mut reads, &ctxs),
            Err(e) => Err(e),
        }
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


