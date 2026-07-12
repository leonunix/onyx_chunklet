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

use crossbeam_channel::Sender;

use crate::error::{ChunkletError, ChunkletResult};
use crate::ld::degrade::{absorb_degraded, is_runtime_read_fault, SuspectMember};
use crate::ld::descriptor::LdDescriptor;
use crate::ld::gf256;
use crate::ld::{
    compute_strip_bytes, parallel_strip_reads, resolve_members, submit_strip_writes_detailed,
    LogicalDisk, ReconstructEngine, StripRead, StripWrite, StripeLockTable,
};
use crate::pd::PhysicalDisk;
use crate::pool::{new_rebuild_cell, PdHealth, RebuildCell};
use crate::types::{
    LdId, LdRole, PdId, RaidLevel, BLOCK_SIZE, CHUNKLET_HEADER_BYTES, CHUNKLET_SIZE,
};

mod batched;
mod reconstruct;
mod write_paths;

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
    /// Fast-isolation channel (shared via `attach_shared`; `None` for a bare
    /// handle). An inline-degraded write (F ≤ 2 members failed at runtime) sends
    /// the failed member(s) here so onyx isolates + rebuilds them fast.
    suspect_tx: Option<Sender<SuspectMember>>,
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
            suspect_tx: None,
        })
    }

    pub fn descriptor(&self) -> &LdDescriptor {
        &self.desc
    }

    /// Install the shared stripe-lock table + rebuild cell from `LdRuntime`
    /// (called by `open_ld`), plus the pool's fast-isolation sender. Handles
    /// built for tests / the rebuild worker's reconstruct engine skip this and
    /// keep their private table + empty cell, so they never write-forward and
    /// lock only within themselves.
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
    /// surviving parity/data, so onyx's reactor isolates them fast. Ignored when
    /// no channel is attached (bare handle) or the reactor is gone.
    fn report_suspects(&self, suspects: Vec<SuspectMember>) {
        if let Some(tx) = &self.suspect_tx {
            for s in suspects {
                let _ = tx.try_send(s);
            }
        }
    }

    /// Report a member that faulted on the READ side (dead-but-not-yet-isolated),
    /// so a fault surfaced only by reads still triggers fast isolation.
    fn report_read_suspect(&self, member_idx: usize) {
        if let Some(pd) = self.members[member_idx].as_ref() {
            self.report_suspects(vec![SuspectMember {
                pd_id: pd.pd_id(),
                chunklet_index: self.desc.members[member_idx].chunklet_index,
            }]);
        }
    }

    /// Reconstruct a data range whose member faulted at RUNTIME while the LD
    /// still believes it healthy (dead-but-not-yet-isolated). Treats that
    /// position as failed IN ADDITION to any open-failed data positions (the
    /// "effective failed set"), and dispatches to the P/Q reconstruct that fits
    /// it — within R6's budget of 2. Unlike `reconstruct_unmodified_data`, which
    /// derives its failed set from `failed_data_positions()` (is_none only) and
    /// would wrongly re-read the still-`is_some` faulting member, this builds the
    /// effective set explicitly. Errors (never returns wrong data) when the
    /// effective set exceeds budget or a survivor read also faults. `out.len()`
    /// may cover any block-aligned range contained within one strip; parity
    /// reconstruction is byte-wise, so every survivor is read at the same
    /// `in_chunklet_off` for exactly `out.len()` bytes.
    fn reconstruct_data_range(
        &self,
        set_idx: usize,
        data_pos: usize,
        in_chunklet_off: u64,
        out: &mut [u8],
    ) -> ChunkletResult<()> {
        let mut ef = self.failed_data_positions(set_idx);
        if !ef.contains(&data_pos) {
            ef.push(data_pos);
        }
        ef.sort_unstable();
        let p_ok = !self.parity_p_failed(set_idx);
        let q_ok = !self.parity_q_failed(set_idx);
        match ef.len() {
            1 => {
                if p_ok {
                    self.reconstruct_one_data(set_idx, data_pos, in_chunklet_off, out)
                } else if q_ok {
                    self.reconstruct_one_data_via_q(set_idx, data_pos, in_chunklet_off, out)
                } else {
                    Err(ChunkletError::Invariant(format!(
                        "Raid6 set {}: 1 data + both parity unavailable — read reconstruct over budget",
                        set_idx
                    )))
                }
            }
            2 if p_ok && q_ok => {
                let (x, y) = (ef[0], ef[1]);
                let (dx, dy) =
                    self.reconstruct_two_data(set_idx, x, y, in_chunklet_off, out.len())?;
                out.copy_from_slice(if data_pos == x { &dx } else { &dy });
                Ok(())
            }
            _ => Err(ChunkletError::Invariant(format!(
                "Raid6 set {}: effective failed data set {:?} + parity(P_ok={}, Q_ok={}) exceeds budget 2",
                set_idx, ef, p_ok, q_ok
            ))),
        }
    }

    /// Fallback for the fast batched read (`read_many_at`) when a member faults:
    /// re-read each non-crossing strip range, reconstructing any that returns a
    /// runtime read fault via [`Self::reconstruct_data_range`]. Suspects are
    /// reported on both the Ok and Err paths so an over-budget read still drives
    /// isolation.
    fn reconstruct_read_batch(
        &self,
        reads: &mut [StripRead<'_>],
        ctxs: &[(usize, usize)],
    ) -> ChunkletResult<()> {
        debug_assert_eq!(reads.len(), ctxs.len());
        let mut suspects: Vec<SuspectMember> = Vec::new();
        let mut result = Ok(());
        for (r, &(set_idx, data_pos)) in reads.iter_mut().zip(ctxs) {
            match self.read_data_strip(set_idx, data_pos, r.in_chunklet_off, r.data) {
                Ok(()) => {}
                Err(e) if is_runtime_read_fault(&e) => {
                    if let Err(e2) =
                        self.reconstruct_data_range(set_idx, data_pos, r.in_chunklet_off, r.data)
                    {
                        result = Err(e2);
                        break;
                    }
                    let m = self.member_idx_data(set_idx, data_pos);
                    if let Some(pd) = self.members[m].as_ref() {
                        let pd_id = pd.pd_id();
                        if !suspects.iter().any(|s| s.pd_id == pd_id) {
                            suspects.push(SuspectMember {
                                pd_id,
                                chunklet_index: self.desc.members[m].chunklet_index,
                            });
                        }
                    }
                }
                Err(e) => {
                    result = Err(e);
                    break;
                }
            }
        }
        self.report_suspects(suspects);
        result
    }

    /// Submit ONE set's strip writes with inline-degrade. All backends
    /// submit-and-wait every leg, so the surviving members are durable on
    /// return; absorb up to `max_fail` runtime member failures (`2 −
    /// open_time_failures` for the set — any ≤budget missing member reconstructs
    /// to the correct new value from the parity that DID land), report the
    /// failed members for fast isolation, and surface an error only when the set
    /// exceeded its budget (genuine data loss).
    fn submit_set_absorb(&self, ops: Vec<StripWrite<'_>>, max_fail: u32) -> ChunkletResult<()> {
        let results = submit_strip_writes_detailed(&ops);
        let group_of = vec![0u32; ops.len()];
        let suspects = absorb_degraded(&ops, &results, &group_of, &[max_fail])?;
        self.report_suspects(suspects);
        Ok(())
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
        let Some(sr) = progress
            .targets_by_set
            .get(set_idx)
            .and_then(|o| o.as_ref())
        else {
            return;
        };
        let set_stripe_n = strip_base / self.strip_bytes;
        if set_stripe_n >= sr.cursor.load(Ordering::Acquire) {
            return;
        }
        for shadow in &sr.shadows {
            let bytes = strip_for(shadow.pos_in_set);
            if let Err(e) = shadow
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
                match self.read_data_strip(
                    addr.set_idx,
                    addr.data_pos,
                    addr.in_chunklet_off,
                    &mut buf[buf_start..buf_start + take],
                ) {
                    Ok(()) => {}
                    Err(e) if is_runtime_read_fault(&e) => {
                        // Data member faulted although the LD still believes it
                        // healthy. Reconstruct via the effective-failed-set
                        // dispatch (this position + any open-failed data), within
                        // R6's budget of 2; over budget or a survivor fault
                        // surfaces the error. Emit a suspect so the fault also
                        // drives isolation from the read side.
                        let strip_len = self.strip_bytes as usize;
                        let strip_base = addr.in_chunklet_off - addr.in_strip_off;
                        let mut tmp = vec![0u8; strip_len];
                        self.reconstruct_data_range(
                            addr.set_idx,
                            addr.data_pos,
                            strip_base,
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
        // Parallel to `reads`: (set_idx, data_pos) so reconstruct-on-EIO can
        // rebuild the same non-crossing range via the effective-failed-set
        // dispatch.
        let mut ctxs: Vec<(usize, usize)> = Vec::new();
        for (offset, buf) in ops.iter_mut() {
            if buf.is_empty() {
                continue;
            }
            let addr = self.locate(*offset);
            let strip_remaining = self.strip_bytes - addr.in_strip_off;
            if buf.len() as u64 > strip_remaining {
                self.read_at(*offset, buf)?;
                continue;
            }
            let failed = self.failed_data_positions(addr.set_idx);
            if failed.contains(&addr.data_pos) {
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
