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
//! All write fan-outs go through `parallel_strip_writes`, which spawns one
//! thread per surviving member (K+2 strips on a healthy R6 set). Three
//! sub-paths feed it:
//!
//! - **Full-stripe**: P + Q encoded directly from new data; up to K+2
//!   writes (skips writes to failed PDs in degraded mode). No reads.
//! - **Parity-delta write (PDW)**: Ceph FastEC-style overwrite:
//!   `delta = old_D_i ⊕ new_D_i`,
//!   `P' = P ⊕ delta`, `Q' = Q ⊕ g^i·delta`. Costs M+2 reads,
//!   M+2 writes. Healthy-set only — degraded sets always take RW because
//!   PDW can't compute correct parity when a modified position sits on a
//!   failed PD, and can't read old P/Q when a parity is gone.
//! - **Partial RW** (reconstruct-write): materialize all K new strips
//!   (read unmodified, reconstruct unmodified-failed via available parity,
//!   copy modified bytes), recompute P + Q from scratch, write modified
//!   data + parities. Costs K-M full-strip reads plus any partial gap-fill
//!   reads, then M+2 writes. Used when:
//!     * its read cost is lower than PDW (for example 3+2 single-strip
//!       writes, where reading two untouched data strips beats old data+P+Q), OR
//!     * the set is degraded with at least one data position failed.
//!
//! Degraded write tolerates **F ≤ 2** failed members (any combination of
//! data + parity).
//! - F=2 P+Q both failed: short-circuit to data-only write (no parity
//!   computation).
//! - F=1 with one parity gone: RW computes both parities, writes only the
//!   surviving one.
//! - F=2 with one data + one parity gone: RW reconstructs the missing data
//!   via the surviving parity (P → XOR formula; Q → g^(-x) · (Q ⊕ Σ
//!   g^i·D_i)); writes the surviving parity.
//! - F=2 with two data gone: RW reconstructs both via `reconstruct_two_data`.
//! - F ≥ 3: rejected.
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

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::Ordering;
use std::sync::Arc;

use crate::error::{ChunkletError, ChunkletResult};
use crate::ld::descriptor::LdDescriptor;
use crate::ld::gf256;
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

pub struct LdRaid6 {
    desc: LdDescriptor,
    members: Vec<Option<Arc<PhysicalDisk>>>,
    capacity: u64,
    strip_bytes: u64,
    data_per_set: usize,
    full_stripe_bytes: u64,
    /// Per-full-stripe serialization, SHARED with the online-rebuild worker via
    /// `attach_shared` (default: a private table for tests / the reconstruct
    /// engine). RMW/RW updates parity from old stripe state, so overlapping
    /// writes to one stripe must not interleave — and the rebuild worker takes
    /// the SAME lock so its shadow backfill serializes with foreground writes.
    stripe_locks: Arc<StripeLockTable>,
    /// Online-rebuild plan cell (shared via `attach_shared`; default empty).
    /// Foreground writes consult it to write-forward reconstructed strips to
    /// the shadow spare for stripes below the set cursor.
    rebuild: RebuildCell,
}

impl LdRaid6 {
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
        let expected =
            (desc.set_size as usize) * (desc.row_size as usize) * (desc.num_rows as usize);
        if desc.members.len() != expected {
            return Err(ChunkletError::Invariant(format!(
                "Raid6 member count {} != expected {}",
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
        let data_per_set = (desc.set_size - 2) as usize;
        let full_stripe_bytes = (data_per_set as u64) * strip_bytes;

        let n = desc.set_size as usize;
        for set_n in 0..((desc.row_size as usize) * (desc.num_rows as usize)) {
            let base = set_n * n;
            for i in 0..(n - 2) {
                if desc.members[base + i].role != LdRole::Data {
                    return Err(ChunkletError::Invariant(format!(
                        "Raid6 set {} member {} must be Data role: {:?}",
                        set_n,
                        i,
                        desc.members[base + i].role
                    )));
                }
            }
            if desc.members[base + n - 2].role != LdRole::ParityP {
                return Err(ChunkletError::Invariant(format!(
                    "Raid6 set {} P slot is not ParityP: {:?}",
                    set_n,
                    desc.members[base + n - 2].role
                )));
            }
            if desc.members[base + n - 1].role != LdRole::ParityQ {
                return Err(ChunkletError::Invariant(format!(
                    "Raid6 set {} Q slot is not ParityQ: {:?}",
                    set_n,
                    desc.members[base + n - 1].role
                )));
            }
        }

        let usable_per_chunklet = (CHUNKLET_USER_BYTES / strip_bytes) * strip_bytes;
        let members = resolve_members(pds, pd_health, &desc)?;
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

    /// Install the shared stripe-lock table + rebuild cell from `LdRuntime`
    /// (called by `open_ld`). Handles built for tests / the rebuild worker's
    /// reconstruct engine skip this and keep their private table + empty cell,
    /// so they never write-forward and lock only within themselves.
    pub(crate) fn attach_shared(&mut self, stripe_locks: Arc<StripeLockTable>, rebuild: RebuildCell) {
        self.stripe_locks = stripe_locks;
        self.rebuild = rebuild;
    }

    /// True iff an online rebuild is currently backfilling `set_idx`.
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

    /// While holding the stripe lock, mirror this stripe's just-computed strips
    /// to the online-rebuild shadow spare(s) for `set_idx` — but only for
    /// stripes strictly below the set cursor (already-backfilled stripes that
    /// must stay current). `strip_for(pos_in_set)` yields the full strip bytes
    /// for a member position (data `0..K`, P=`K`, Q=`K+1`). A shadow write
    /// failure sets `aborted` (Phase C then skips the swap) but does NOT fail
    /// the foreground write — live data is intact via reconstruct.
    fn write_forward(
        &self,
        set_idx: usize,
        strip_base: u64,
        mut strip_for: impl FnMut(usize) -> Vec<u8>,
    ) {
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
                    "online rebuild: shadow write-forward failed (set {} pos {} chunklet {}): {} — aborting rebuild",
                    set_idx,
                    shadow.pos_in_set,
                    shadow.chunklet_index,
                    e
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
                "Raid6 IO not block-aligned: offset={} len={}",
                offset, len
            )));
        }
        let end = offset
            .checked_add(len as u64)
            .ok_or_else(|| ChunkletError::Invariant("Raid6 IO offset overflow".into()))?;
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

    fn member_idx_p(&self, set_idx: usize) -> usize {
        set_idx * (self.desc.set_size as usize) + self.data_per_set
    }

    fn member_idx_q(&self, set_idx: usize) -> usize {
        set_idx * (self.desc.set_size as usize) + self.data_per_set + 1
    }

    fn stripe_key(&self, set_idx: usize, strip_base: u64) -> u64 {
        ((set_idx as u64) << 32) | (strip_base / self.strip_bytes)
    }

    fn member_pd(&self, idx: usize) -> ChunkletResult<&Arc<PhysicalDisk>> {
        self.members[idx].as_ref().ok_or_else(|| {
            ChunkletError::Invariant(format!(
                "Raid6 member idx={} on failed PD {} — caller must rebuild before writing",
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

    fn read_p(&self, set_idx: usize, off: u64, bytes: &mut [u8]) -> ChunkletResult<()> {
        let m = self.member_idx_p(set_idx);
        let pd = self.member_pd(m)?;
        let chunklet_idx = self.desc.members[m].chunklet_index;
        pd.read_chunklet_user(chunklet_idx, off, bytes)
    }

    fn read_q(&self, set_idx: usize, off: u64, bytes: &mut [u8]) -> ChunkletResult<()> {
        let m = self.member_idx_q(set_idx);
        let pd = self.member_pd(m)?;
        let chunklet_idx = self.desc.members[m].chunklet_index;
        pd.read_chunklet_user(chunklet_idx, off, bytes)
    }

    /// Indices of failed data positions in the given set (subset of 0..K).
    fn failed_data_positions(&self, set_idx: usize) -> Vec<usize> {
        (0..self.data_per_set)
            .filter(|&pos| self.members[self.member_idx_data(set_idx, pos)].is_none())
            .collect()
    }

    fn parity_p_failed(&self, set_idx: usize) -> bool {
        self.members[self.member_idx_p(set_idx)].is_none()
    }

    fn parity_q_failed(&self, set_idx: usize) -> bool {
        self.members[self.member_idx_q(set_idx)].is_none()
    }

    /// Encode P from all K data strips at `in_chunklet_off`.
    pub fn encode_p_strip(
        &self,
        set_idx: usize,
        in_chunklet_off: u64,
        out: &mut [u8],
    ) -> ChunkletResult<()> {
        out.fill(0);
        let mut tmp = vec![0u8; out.len()];
        for pos in 0..self.data_per_set {
            self.read_data_strip(set_idx, pos, in_chunklet_off, &mut tmp)?;
            gf256::xor_into(out, &tmp);
        }
        Ok(())
    }

    /// Encode Q from all K data strips at `in_chunklet_off`.
    pub fn encode_q_strip(
        &self,
        set_idx: usize,
        in_chunklet_off: u64,
        out: &mut [u8],
    ) -> ChunkletResult<()> {
        out.fill(0);
        let mut tmp = vec![0u8; out.len()];
        for pos in 0..self.data_per_set {
            self.read_data_strip(set_idx, pos, in_chunklet_off, &mut tmp)?;
            gf256::mul_xor_into(out, &tmp, gf256::g_pow(pos));
        }
        Ok(())
    }

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
    /// path based on member role + how many failures share this set.
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
            self.encode_p_strip(set_idx, in_chunklet_off, out)
        } else if pos == self.data_per_set + 1 {
            self.encode_q_strip(set_idx, in_chunklet_off, out)
        } else {
            // Data position. Check how many data positions in this set are
            // failed: 1 → use P; 2 → use PQ-solve and pick the right output.
            let failed = self.failed_data_positions(set_idx);
            if failed.len() == 1 && failed[0] == pos {
                self.reconstruct_one_data(set_idx, pos, in_chunklet_off, out)
            } else if failed.len() == 2 && failed.contains(&pos) {
                let (x, y) = (failed[0], failed[1]);
                let (dx, dy) =
                    self.reconstruct_two_data(set_idx, x, y, in_chunklet_off, out.len())?;
                if pos == x {
                    out.copy_from_slice(&dx);
                } else {
                    out.copy_from_slice(&dy);
                }
                Ok(())
            } else {
                Err(ChunkletError::Invariant(format!(
                    "Raid6 set {} has {} failed data positions (need <= 2 with this position included)",
                    set_idx,
                    failed.len()
                )))
            }
        }
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

    /// Reconstruct one missing data position via Q (used when P is the
    /// missing parity).
    ///
    /// `Q = Σ g^i · D_i` ⇒  `g^x · D_x = Q ⊕ Σ_{i≠x} g^i · D_i`
    /// ⇒  `D_x = g^(-x) · (Q ⊕ Σ_{i≠x} g^i · D_i)`
    pub fn reconstruct_one_data_via_q(
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
        self.read_q(set_idx, in_chunklet_off, out)?;
        let mut tmp = vec![0u8; out.len()];
        for pos in 0..self.data_per_set {
            if pos == missing_data_pos {
                continue;
            }
            self.read_data_strip(set_idx, pos, in_chunklet_off, &mut tmp)?;
            gf256::mul_xor_into(out, &tmp, gf256::g_pow(pos));
        }
        // out now = g^x · D_x; multiply by g^(-x) = inv(g^x).
        let g_neg_x = gf256::inv(gf256::g_pow(missing_data_pos));
        for byte in out.iter_mut() {
            *byte = gf256::mul(*byte, g_neg_x);
        }
        Ok(())
    }

    /// Reconstruct one unmodified-failed data position. Picks the cheapest
    /// surviving parity (P preferred over Q since XOR is cheaper than the
    /// Q-based formula). Used by the partial-stripe RW write path when
    /// computing new parity needs the old value of a failed PD's strip.
    fn reconstruct_unmodified_data(
        &self,
        set_idx: usize,
        missing_data_pos: usize,
        in_chunklet_off: u64,
        out: &mut [u8],
    ) -> ChunkletResult<()> {
        let failed = self.failed_data_positions(set_idx);
        let p_ok = !self.parity_p_failed(set_idx);
        let q_ok = !self.parity_q_failed(set_idx);

        if failed.len() == 2 {
            // Both data losses share this set — single-parity reconstruct
            // can't isolate them. Use the PQ system (needs both parities).
            if !(p_ok && q_ok) {
                return Err(ChunkletError::Invariant(format!(
                    "Raid6 set {}: 2 data losses + parity loss exceeds redundancy",
                    set_idx
                )));
            }
            let (x, y) = (failed[0], failed[1]);
            let (dx, dy) = self.reconstruct_two_data(set_idx, x, y, in_chunklet_off, out.len())?;
            if missing_data_pos == x {
                out.copy_from_slice(&dx);
            } else if missing_data_pos == y {
                out.copy_from_slice(&dy);
            } else {
                return Err(ChunkletError::Invariant(format!(
                    "reconstruct_unmodified_data pos {} not in failed set {:?}",
                    missing_data_pos, failed
                )));
            }
            Ok(())
        } else if p_ok {
            self.reconstruct_one_data(set_idx, missing_data_pos, in_chunklet_off, out)
        } else if q_ok {
            self.reconstruct_one_data_via_q(set_idx, missing_data_pos, in_chunklet_off, out)
        } else {
            Err(ChunkletError::Invariant(format!(
                "Raid6 set {}: cannot reconstruct (no surviving parity)",
                set_idx
            )))
        }
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

fn parity_delta_read_cost(positions: &[(usize, u64, std::ops::Range<usize>)]) -> usize {
    positions.len() + 2
}

fn reconstruct_write_read_cost(
    data_per_set: usize,
    strip_bytes: u64,
    positions: &[(usize, u64, std::ops::Range<usize>)],
) -> usize {
    let modified_full_strips = positions
        .iter()
        .filter(|(_pos, off, range)| *off == 0 && (range.end - range.start) as u64 == strip_bytes)
        .count();
    let modified_partial_strips = positions.len() - modified_full_strips;
    data_per_set - modified_full_strips + modified_partial_strips
}

#[derive(Clone, Copy, Debug)]
struct StripeAddr {
    set_idx: usize,
    data_pos: usize,
    in_strip_off: u64,
    in_chunklet_off: u64,
}

impl ReconstructEngine for LdRaid6 {
    fn strip_bytes(&self) -> u64 {
        self.strip_bytes
    }
    fn stripes_per_chunklet(&self) -> u64 {
        LdRaid6::stripes_per_chunklet(self)
    }
    fn reconstruct_member_strip(
        &self,
        failed_member_idx: usize,
        in_chunklet_off: u64,
        out: &mut [u8],
    ) -> ChunkletResult<()> {
        LdRaid6::reconstruct_member_strip(self, failed_member_idx, in_chunklet_off, out)
    }
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
            // Are we reading a failed data position? If so, route through
            // `reconstruct_unmodified_data`, which picks the cheapest
            // surviving parity (P preferred, falls back to Q when P is
            // also down) and dispatches to the 1- or 2-failure formula.
            let failed = self.failed_data_positions(addr.set_idx);
            if failed.contains(&addr.data_pos) {
                let strip_len = self.strip_bytes as usize;
                let strip_base = addr.in_chunklet_off - addr.in_strip_off;
                let mut tmp = vec![0u8; strip_len];
                self.reconstruct_unmodified_data(
                    addr.set_idx,
                    addr.data_pos,
                    strip_base,
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
            let failed = self.failed_data_positions(addr.set_idx);
            if addr.in_strip_off != 0 || failed.contains(&addr.data_pos) {
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

    fn write_many_at(&self, ops: &[(u64, &[u8])]) -> ChunkletResult<()> {
        self.write_many_batched(ops)
    }

    fn flush(&self) -> ChunkletResult<()> {
        crate::ld::flush_members(&self.members)
    }
}

impl LdRaid6 {
    fn write_one_stripe_segment(&self, start: StripeAddr, buf: &[u8]) -> ChunkletResult<()> {
        // Compute failure pattern once and reuse for the F-budget check
        // and the path dispatch below.
        let f_data = self.failed_data_positions(start.set_idx).len();
        let p_failed = self.parity_p_failed(start.set_idx);
        let q_failed = self.parity_q_failed(start.set_idx);
        let f_total = f_data + p_failed as usize + q_failed as usize;
        if f_total > 2 {
            return Err(ChunkletError::WriteRedundancyExceeded {
                raid: RaidLevel::Raid6,
                set_idx: start.set_idx,
                failed: f_total,
                budget: 2,
            });
        }

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

        // Strip-aligned chunklet offset shared by every position + P + Q.
        // Helpers compute each position's chunklet IO at `strip_base + off`.
        let strip_base = start.in_chunklet_off - start.in_strip_off;
        let _stripe = self
            .stripe_locks
            .write_key(self.stripe_key(start.set_idx, strip_base));

        let is_full_stripe = positions.len() == k
            && positions
                .iter()
                .all(|(_p, off, range)| *off == 0 && (range.end - range.start) as u64 == strip);
        if is_full_stripe {
            return self.write_full_stripe(start.set_idx, strip_base, &positions, buf);
        }

        if f_data == 0 && p_failed && q_failed && !self.set_being_rebuilt(start.set_idx) {
            // Both parities gone (F=2): just write data, no parity work.
            // An online rebuild targeting a parity needs P/Q computed so it can
            // write-forward to the shadow, so a rebuilding set falls through to
            // RW (which computes both parities) instead of this fast path.
            return self.write_data_only(start.set_idx, strip_base, &positions, buf);
        }

        let healthy = f_data == 0 && !p_failed && !q_failed;
        if healthy {
            // Pick between Ceph FastEC-style parity-delta write (PDW)
            // and reconstruct-write (RW) by the number of strip reads
            // each path needs. RW wins for narrow 3+2 single-strip writes;
            // PDW wins as K grows or partial gap-fill makes RW expensive.
            let pdw_reads = parity_delta_read_cost(&positions);
            let rw_reads = reconstruct_write_read_cost(k, strip, &positions);
            if rw_reads < pdw_reads {
                self.write_partial_stripe_rw(start.set_idx, strip_base, &positions, buf)
            } else {
                self.write_partial_stripe_pdw(start.set_idx, strip_base, &positions, buf)
            }
        } else {
            // Any failure: unified RW handles all sub-cases (single
            // surviving parity, single missing data, two missing data).
            self.write_partial_stripe_rw(start.set_idx, strip_base, &positions, buf)
        }
    }

    /// Fan out one write per surviving member, computed from the new data.
    /// Skips writes to failed PDs (degraded fast path).
    ///
    /// `strip_base` is the strip-aligned chunklet offset; for full-stripe
    /// writes every position has `off == 0` so each chunklet write lands at
    /// `strip_base`. P/Q always live at `strip_base`.
    fn write_full_stripe(
        &self,
        set_idx: usize,
        strip_base: u64,
        positions: &[(usize, u64, std::ops::Range<usize>)],
        buf: &[u8],
    ) -> ChunkletResult<()> {
        let strip = self.strip_bytes as usize;
        let mut p = vec![0u8; strip];
        let mut q = vec![0u8; strip];
        for (pos, _off, range) in positions {
            let data = &buf[range.clone()];
            gf256::xor_into(&mut p, data);
            gf256::mul_xor_into(&mut q, data, gf256::g_pow(*pos));
        }

        let mut ops: Vec<StripWrite> = Vec::with_capacity(positions.len() + 2);
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
        let pm = self.member_idx_p(set_idx);
        if let Some(pd) = &self.members[pm] {
            ops.push(StripWrite {
                pd: pd.clone(),
                chunklet_index: self.desc.members[pm].chunklet_index,
                in_chunklet_off: strip_base,
                data: &p,
            });
        }
        let qm = self.member_idx_q(set_idx);
        if let Some(pd) = &self.members[qm] {
            ops.push(StripWrite {
                pd: pd.clone(),
                chunklet_index: self.desc.members[qm].chunklet_index,
                in_chunklet_off: strip_base,
                data: &q,
            });
        }
        parallel_strip_writes(ops)?;
        // Mirror to online-rebuild shadow(s) below the cursor: full-stripe gives
        // each data pos its slice of `buf`; P/Q are the strips just computed.
        let strip = self.strip_bytes as usize;
        self.write_forward(set_idx, strip_base, |pos| {
            if pos < self.data_per_set {
                buf[pos * strip..pos * strip + strip].to_vec()
            } else if pos == self.data_per_set {
                p.clone()
            } else {
                q.clone()
            }
        });
        Ok(())
    }

    /// F=2 P+Q failed fast path: skip both parities, write data only.
    /// Zero reads, ≤K writes. Both parities recovered later by rebuild.
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
            let pd = self.members[m].as_ref().expect(
                "write_data_only invariant: data PDs must be healthy when both parities are failed",
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

    /// Parity-delta write: per touched data position compute deltas for P
    /// and Q, apply to old P/Q, write modified data + new P + new Q.
    /// Healthy-set only (needs old data, P, Q all readable).
    ///
    /// Each position's old data is read at `strip_base + off`. P and Q are
    /// full-strip reads/writes at `strip_base`.
    fn write_partial_stripe_pdw(
        &self,
        set_idx: usize,
        strip_base: u64,
        positions: &[(usize, u64, std::ops::Range<usize>)],
        buf: &[u8],
    ) -> ChunkletResult<()> {
        let strip = self.strip_bytes as usize;
        let mut delta_p = vec![0u8; strip];
        let mut delta_q = vec![0u8; strip];

        let mut old_data: Vec<Vec<u8>> = positions
            .iter()
            .map(|(_pos, _off, range)| vec![0u8; range.end - range.start])
            .collect();
        let mut p = vec![0u8; strip];
        let mut q = vec![0u8; strip];

        let mut read_ops: Vec<StripRead> = Vec::with_capacity(positions.len() + 2);
        for ((pos, off, _range), old) in positions.iter().zip(old_data.iter_mut()) {
            let m = self.member_idx_data(set_idx, *pos);
            let pd = self.members[m]
                .as_ref()
                .expect("PDW path requires all data PDs healthy");
            read_ops.push(StripRead {
                pd: pd.clone(),
                chunklet_index: self.desc.members[m].chunklet_index,
                in_chunklet_off: strip_base + off,
                data: old,
            });
        }
        let pm = self.member_idx_p(set_idx);
        let pd_p = self.members[pm]
            .as_ref()
            .expect("PDW path requires P healthy");
        read_ops.push(StripRead {
            pd: pd_p.clone(),
            chunklet_index: self.desc.members[pm].chunklet_index,
            in_chunklet_off: strip_base,
            data: &mut p,
        });
        let qm = self.member_idx_q(set_idx);
        let pd_q = self.members[qm]
            .as_ref()
            .expect("PDW path requires Q healthy");
        read_ops.push(StripRead {
            pd: pd_q.clone(),
            chunklet_index: self.desc.members[qm].chunklet_index,
            in_chunklet_off: strip_base,
            data: &mut q,
        });
        parallel_strip_reads(&mut read_ops)?;
        drop(read_ops);

        for ((pos, off, range), old_data) in positions.iter().zip(old_data.iter()) {
            let new_data = &buf[range.clone()];
            let len = new_data.len();
            let g_i = gf256::g_pow(*pos);
            let dst_p = &mut delta_p[(*off as usize)..(*off as usize) + len];
            let dst_q = &mut delta_q[(*off as usize)..(*off as usize) + len];
            for i in 0..len {
                let d = old_data[i] ^ new_data[i];
                dst_p[i] ^= d;
                dst_q[i] ^= gf256::mul(g_i, d);
            }
        }
        gf256::xor_into(&mut p, &delta_p);
        gf256::xor_into(&mut q, &delta_q);

        let mut ops: Vec<StripWrite> = Vec::with_capacity(positions.len() + 2);
        for (pos, off, range) in positions {
            let m = self.member_idx_data(set_idx, *pos);
            let pd = self.members[m]
                .as_ref()
                .expect("PDW path requires all data PDs healthy");
            ops.push(StripWrite {
                pd: pd.clone(),
                chunklet_index: self.desc.members[m].chunklet_index,
                in_chunklet_off: strip_base + off,
                data: &buf[range.clone()],
            });
        }
        ops.push(StripWrite {
            pd: pd_p.clone(),
            chunklet_index: self.desc.members[pm].chunklet_index,
            in_chunklet_off: strip_base,
            data: &p,
        });
        ops.push(StripWrite {
            pd: pd_q.clone(),
            chunklet_index: self.desc.members[qm].chunklet_index,
            in_chunklet_off: strip_base,
            data: &q,
        });
        parallel_strip_writes(ops)
    }

    /// Reconstruct-write: build full new strips for every data position
    /// (read unmodified, reconstruct unmodified-failed via available
    /// parity, copy modified bytes), recompute P + Q from scratch, write
    /// modified data + parities. Skips writes to failed PDs.
    ///
    /// Handles every degraded sub-case the redundancy budget allows
    /// (F ≤ 2 with at least one parity surviving). Caller routes the
    /// double-parity-failed case to `write_data_only`.
    ///
    /// Full-strip reads/reconstructs use `strip_base`; modified data writes
    /// use `strip_base + off` per position; P and Q write the full strip
    /// at `strip_base`.
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
        let mut copy_after_reads: Vec<(usize, usize, std::ops::Range<usize>)> = Vec::new();
        let mut read_ops: Vec<StripRead> = Vec::with_capacity(k);
        for (pos, strip_buf) in new_strips.iter_mut().enumerate() {
            let pd_failed = self.members[self.member_idx_data(set_idx, pos)].is_none();
            match modified_map.get(&pos) {
                Some((off, range)) => {
                    let new_data = &buf[range.clone()];
                    let off = *off as usize;
                    let len = new_data.len();
                    if off == 0 && len == strip {
                        strip_buf.copy_from_slice(new_data);
                    } else {
                        if pd_failed {
                            self.reconstruct_unmodified_data(set_idx, pos, strip_base, strip_buf)?;
                            strip_buf[off..off + len].copy_from_slice(new_data);
                        } else {
                            let m = self.member_idx_data(set_idx, pos);
                            let pd = self.members[m]
                                .as_ref()
                                .expect("RW direct-read path requires data PD healthy");
                            read_ops.push(StripRead {
                                pd: pd.clone(),
                                chunklet_index: self.desc.members[m].chunklet_index,
                                in_chunklet_off: strip_base,
                                data: strip_buf,
                            });
                            copy_after_reads.push((pos, off, range.clone()));
                        }
                    }
                }
                None => {
                    if pd_failed {
                        self.reconstruct_unmodified_data(set_idx, pos, strip_base, strip_buf)?;
                    } else {
                        let m = self.member_idx_data(set_idx, pos);
                        let pd = self.members[m]
                            .as_ref()
                            .expect("RW direct-read path requires data PD healthy");
                        read_ops.push(StripRead {
                            pd: pd.clone(),
                            chunklet_index: self.desc.members[m].chunklet_index,
                            in_chunklet_off: strip_base,
                            data: strip_buf,
                        });
                    }
                }
            }
        }
        parallel_strip_reads(&mut read_ops)?;
        drop(read_ops);
        for (pos, off, range) in copy_after_reads {
            let new_data = &buf[range];
            new_strips[pos][off..off + new_data.len()].copy_from_slice(new_data);
        }

        let mut p = vec![0u8; strip];
        let mut q = vec![0u8; strip];
        for (i, s) in new_strips.iter().enumerate() {
            gf256::xor_into(&mut p, s);
            gf256::mul_xor_into(&mut q, s, gf256::g_pow(i));
        }

        let mut ops: Vec<StripWrite> = Vec::with_capacity(positions.len() + 2);
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
        let pm = self.member_idx_p(set_idx);
        if let Some(pd) = &self.members[pm] {
            ops.push(StripWrite {
                pd: pd.clone(),
                chunklet_index: self.desc.members[pm].chunklet_index,
                in_chunklet_off: strip_base,
                data: &p,
            });
        }
        let qm = self.member_idx_q(set_idx);
        if let Some(pd) = &self.members[qm] {
            ops.push(StripWrite {
                pd: pd.clone(),
                chunklet_index: self.desc.members[qm].chunklet_index,
                in_chunklet_off: strip_base,
                data: &q,
            });
        }
        parallel_strip_writes(ops)?;
        // Mirror to online-rebuild shadow(s) below the cursor. RW built the full
        // new strip for every data position plus P and Q.
        self.write_forward(set_idx, strip_base, |pos| {
            if pos < self.data_per_set {
                new_strips[pos].clone()
            } else if pos == self.data_per_set {
                p.clone()
            } else {
                q.clone()
            }
        });
        Ok(())
    }
}

/// Which healthy-set write path a batched segment takes. Degraded segments
/// never reach here — [`LdRaid6::write_many_batched`] bails the whole batch to
/// the serial `write_at` path, which owns the reconstruct plumbing.
#[derive(Clone, Copy)]
enum Kind6 {
    Full,
    Pdw,
    Rw,
}

/// One planned stripe segment of a batched `write_many_at`. Owns the scratch
/// buffers that span the read → compute → write phases so the whole batch
/// issues ONE `parallel_strip_reads` + ONE `parallel_strip_writes` instead of a
/// serial submit pair per segment. That per-segment serialization was the
/// flusher→RAID6 de-batching that turned µs disk IO into ~200 ms wall-clock:
/// onyx hands one deep-QD batch to `write_many_at`, and the trait-default
/// `for op { write_at(op) }` loop unrolled it into N serial read→compute→write
/// RMW chains, so the disk queue never went deep.
struct Seg6<'a> {
    set_idx: usize,
    strip_base: u64,
    kind: Kind6,
    /// (data_pos, in_strip_off, new_data) per modified position, stripe order.
    mods: Vec<(usize, u64, &'a [u8])>,
    /// New P / Q strip. Full/Rw compute it from scratch; Pdw reads the old
    /// value here then applies the delta in place.
    p: Vec<u8>,
    q: Vec<u8>,
    /// Pdw only: old data per modified position (each sized to its mod's len).
    old_data: Vec<Vec<u8>>,
    /// Rw only: K full new-data strips, parity recomputed from them.
    new_strips: Vec<Vec<u8>>,
}

impl LdRaid6 {
    /// Batched multi-op writer — the flusher's hot path. Groups every op's
    /// stripe segments BY physical stripe (merging segments that collide on one
    /// stripe instead of bailing), then collapses the whole batch into a single
    /// cross-PD read submit + a single write submit under one stripe-lock
    /// acquisition. Merging turns the flusher's dense sub-stripe units into
    /// zero-RMW full-stripe writes and eliminates the old duplicate-stripe
    /// serial fallback. Bails the whole batch to the serial `write_at` loop only
    /// for a degraded set (which needs reconstruct reads) or a byte-overlapping
    /// stripe (two ops writing the same bytes — last-writer-wins for safety).
    fn write_many_batched(&self, ops: &[(u64, &[u8])]) -> ChunkletResult<()> {
        for (offset, buf) in ops {
            self.ensure_aligned(*offset, buf.len())?;
        }
        let strip = self.strip_bytes as usize;
        let k = self.data_per_set;

        // Phase 0: decompose every op into per-position mods, GROUPED by
        // physical stripe. Two segments landing on one stripe (dense-PBA
        // packing, ~13% of flusher batches) are MERGED into a single stripe
        // write instead of bailing the whole batch to serial: their disjoint
        // sub-ranges combine, and a stripe that ends up fully covered is
        // promoted to a zero-RMW full-stripe write (the dense sub-stripe units
        // the flusher emits become full stripes at 4 KiB strip). BTreeMap keyed
        // by stripe_key keeps segment/lock order deterministic.
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
                    || self.parity_p_failed(addr.set_idx)
                    || self.parity_q_failed(addr.set_idx)
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
                // Split this segment into per-position mods (same walk as
                // `write_one_stripe_segment`) and append to the stripe's list.
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

        // Phase 0b: one merged Seg6 per stripe. Verify the stripe's mods are
        // byte-disjoint (distinct sub-PBAs guarantee this; an overlap would mean
        // two ops writing the same bytes — bail to serial last-writer-wins for
        // safety). Promote a fully-covered stripe to Full (zero RMW); otherwise
        // classify PDW/RW on the merged mods with the same read-cost heuristic
        // as the serial path. Never promote a partially-covered stripe to Full
        // (that would clobber the unmodified positions — chunklet invariant).
        let mut segs: Vec<Seg6> = Vec::with_capacity(by_stripe.len());
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

            // Clean full stripe: exactly one full-strip mod per data position.
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
                Kind6::Full
            } else {
                // Same PDW-vs-RW read-cost heuristic as `write_one_stripe_segment`,
                // evaluated on the merged mods.
                let modified_full = raw
                    .mods
                    .iter()
                    .filter(|(_p, off, nd)| *off == 0 && nd.len() as u64 == self.strip_bytes)
                    .count();
                let modified_partial = raw.mods.len() - modified_full;
                let pdw_reads = raw.mods.len() + 2;
                let rw_reads = k - modified_full.min(k) + modified_partial;
                if rw_reads < pdw_reads {
                    Kind6::Rw
                } else {
                    Kind6::Pdw
                }
            };
            let old_data = match kind {
                Kind6::Pdw => raw
                    .mods
                    .iter()
                    .map(|(_p, _o, nd)| vec![0u8; nd.len()])
                    .collect(),
                _ => Vec::new(),
            };
            let new_strips = match kind {
                Kind6::Rw => (0..k).map(|_| vec![0u8; strip]).collect(),
                _ => Vec::new(),
            };
            stripe_keys.push(key);
            segs.push(Seg6 {
                set_idx: raw.set_idx,
                strip_base: raw.strip_base,
                kind,
                mods: raw.mods,
                p: vec![0u8; strip],
                q: vec![0u8; strip],
                old_data,
                new_strips,
            });
        }

        // Hold every touched stripe lock across read+compute+write, acquired in
        // one globally-sorted batch (matches `write_key`'s bucket order).
        let _guards = self.stripe_locks.write_keys(&stripe_keys);

        // Phase 1: one batched read submit for every segment's RMW reads.
        let mut reads: Vec<StripRead> = Vec::new();
        for seg in segs.iter_mut() {
            self.r6_collect_reads(seg, &mut reads);
        }
        parallel_strip_reads(&mut reads)?;
        drop(reads);

        // Phase 2: recompute P/Q per segment from the freshly-read state.
        for seg in segs.iter_mut() {
            self.r6_compute(seg);
        }

        // Phase 3: one batched write submit for all data + parity strips.
        let mut writes: Vec<StripWrite> = Vec::new();
        for seg in segs.iter() {
            self.r6_collect_writes(seg, &mut writes);
        }
        parallel_strip_writes(writes)
    }

    /// Append `seg`'s RMW reads (into its owned scratch) to the shared batch.
    fn r6_collect_reads<'s>(&self, seg: &'s mut Seg6<'_>, reads: &mut Vec<StripRead<'s>>) {
        let set_idx = seg.set_idx;
        let strip_base = seg.strip_base;
        let strip = self.strip_bytes as usize;
        match seg.kind {
            Kind6::Full => {}
            Kind6::Pdw => {
                let Seg6 {
                    mods,
                    old_data,
                    p,
                    q,
                    ..
                } = &mut *seg;
                for ((pos, off, _nd), old) in mods.iter().zip(old_data.iter_mut()) {
                    let m = self.member_idx_data(set_idx, *pos);
                    let pd = self.members[m].as_ref().expect("healthy PDW data PD");
                    reads.push(StripRead {
                        pd: pd.clone(),
                        chunklet_index: self.desc.members[m].chunklet_index,
                        in_chunklet_off: strip_base + off,
                        data: old.as_mut_slice(),
                    });
                }
                let pm = self.member_idx_p(set_idx);
                let pdp = self.members[pm].as_ref().expect("healthy PDW P");
                reads.push(StripRead {
                    pd: pdp.clone(),
                    chunklet_index: self.desc.members[pm].chunklet_index,
                    in_chunklet_off: strip_base,
                    data: p.as_mut_slice(),
                });
                let qm = self.member_idx_q(set_idx);
                let pdq = self.members[qm].as_ref().expect("healthy PDW Q");
                reads.push(StripRead {
                    pd: pdq.clone(),
                    chunklet_index: self.desc.members[qm].chunklet_index,
                    in_chunklet_off: strip_base,
                    data: q.as_mut_slice(),
                });
            }
            Kind6::Rw => {
                let Seg6 {
                    mods, new_strips, ..
                } = &mut *seg;
                for (pos, strip_buf) in new_strips.iter_mut().enumerate() {
                    // Fully-modified positions are filled from new data in the
                    // compute phase; every other position reads its base strip.
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

    /// Recompute the segment's P/Q from the read-phase results.
    fn r6_compute(&self, seg: &mut Seg6<'_>) {
        let strip = self.strip_bytes as usize;
        match seg.kind {
            Kind6::Full => {
                seg.p.iter_mut().for_each(|b| *b = 0);
                seg.q.iter_mut().for_each(|b| *b = 0);
                for &(pos, _off, nd) in &seg.mods {
                    gf256::xor_into(&mut seg.p, nd);
                    gf256::mul_xor_into(&mut seg.q, nd, gf256::g_pow(pos));
                }
            }
            Kind6::Pdw => {
                let mut delta_p = vec![0u8; strip];
                let mut delta_q = vec![0u8; strip];
                for ((pos, off, nd), old) in seg.mods.iter().zip(seg.old_data.iter()) {
                    let g_i = gf256::g_pow(*pos);
                    let off = *off as usize;
                    for i in 0..nd.len() {
                        let d = old[i] ^ nd[i];
                        delta_p[off + i] ^= d;
                        delta_q[off + i] ^= gf256::mul(g_i, d);
                    }
                }
                gf256::xor_into(&mut seg.p, &delta_p);
                gf256::xor_into(&mut seg.q, &delta_q);
            }
            Kind6::Rw => {
                for &(pos, off, nd) in &seg.mods {
                    let off = off as usize;
                    if off == 0 && nd.len() == strip {
                        seg.new_strips[pos].copy_from_slice(nd);
                    } else {
                        seg.new_strips[pos][off..off + nd.len()].copy_from_slice(nd);
                    }
                }
                seg.p.iter_mut().for_each(|b| *b = 0);
                seg.q.iter_mut().for_each(|b| *b = 0);
                for (i, s) in seg.new_strips.iter().enumerate() {
                    gf256::xor_into(&mut seg.p, s);
                    gf256::mul_xor_into(&mut seg.q, s, gf256::g_pow(i));
                }
            }
        }
    }

    /// Append `seg`'s data + P + Q writes to the shared batch. Data payloads
    /// borrow the caller's buffers; P/Q borrow the segment's computed scratch.
    fn r6_collect_writes<'s>(&self, seg: &'s Seg6<'s>, writes: &mut Vec<StripWrite<'s>>) {
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
        let pm = self.member_idx_p(set_idx);
        let pdp = self.members[pm].as_ref().expect("healthy write P");
        writes.push(StripWrite {
            pd: pdp.clone(),
            chunklet_index: self.desc.members[pm].chunklet_index,
            in_chunklet_off: strip_base,
            data: &seg.p,
        });
        let qm = self.member_idx_q(set_idx);
        let pdq = self.members[qm].as_ref().expect("healthy write Q");
        writes.push(StripWrite {
            pd: pdq.clone(),
            chunklet_index: self.desc.members[qm].chunklet_index,
            in_chunklet_off: strip_base,
            data: &seg.q,
        });
    }
}
