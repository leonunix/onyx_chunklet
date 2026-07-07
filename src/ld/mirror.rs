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

use crossbeam_channel::Sender;

use crate::error::{ChunkletError, ChunkletResult};
use crate::ld::degrade::{absorb_degraded, is_runtime_read_fault, SuspectMember};
use crate::ld::descriptor::LdDescriptor;
use crate::ld::{
    compute_strip_bytes, parallel_strip_reads, resolve_members, submit_strip_writes_detailed,
    LogicalDisk, ReconstructEngine, StripRead, StripWrite, StripeLockTable,
};
use crate::pd::PhysicalDisk;
use crate::pool::{new_rebuild_cell, PdHealth, RebuildCell};
use crate::types::{LdId, PdId, RaidLevel, BLOCK_SIZE, CHUNKLET_HEADER_BYTES, CHUNKLET_SIZE};

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
    /// Per-stripe serialization, SHARED with the online-rebuild worker via
    /// `attach_shared` (default: private). Overlapping mirror writes must not
    /// leave copies with different write orders; unrelated stripes proceed.
    stripe_locks: Arc<StripeLockTable>,
    /// Online-rebuild plan cell (shared via `attach_shared`; default empty).
    rebuild: RebuildCell,
    /// Fast-isolation channel (shared via `attach_shared`; `None` for a bare
    /// `open`/`open_with_health` handle not owned by a pool reactor, e.g. a unit
    /// test). An inline-degraded write sends the failed member here so onyx
    /// isolates it in ~ms. Best-effort: a full/disconnected channel is ignored.
    suspect_tx: Option<Sender<SuspectMember>>,
}

impl LdMirror {
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
        let expected =
            (desc.set_size as usize) * (desc.row_size as usize) * (desc.num_rows as usize);
        if desc.members.len() != expected {
            return Err(ChunkletError::Invariant(format!(
                "Mirror member count {} != set_size*row_size*num_rows ({})",
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
        let usable_per_chunklet = (CHUNKLET_USER_BYTES / strip_bytes) * strip_bytes;
        let members = resolve_members(pds, pd_health, &desc)?;
        let capacity = (desc.row_size as u64) * (desc.num_rows as u64) * usable_per_chunklet;
        let n_sets = (desc.row_size as usize) * (desc.num_rows as usize);
        let read_cursors = (0..n_sets).map(|_| AtomicUsize::new(0)).collect();
        Ok(Self {
            desc,
            members,
            capacity,
            strip_bytes,
            read_cursors,
            stripe_locks: Arc::new(StripeLockTable::new()),
            rebuild: new_rebuild_cell(),
            suspect_tx: None,
        })
    }

    pub fn descriptor(&self) -> &LdDescriptor {
        &self.desc
    }

    /// Install the shared stripe-lock table + rebuild cell (see `LdRaid6`) and
    /// the pool's fast-isolation sender so an inline-degraded write can report a
    /// failed member without touching a pool lock on the hot path.
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
    /// surviving redundancy, so onyx's reactor can isolate them fast. A missing
    /// or disconnected channel is ignored (a bare test handle, or a shut-down
    /// reactor) — correctness of the current write already held on the survivors.
    fn report_suspects(&self, suspects: Vec<SuspectMember>) {
        if let Some(tx) = &self.suspect_tx {
            for s in suspects {
                let _ = tx.try_send(s);
            }
        }
    }

    /// Mirror the just-written segments to the online-rebuild shadow copies for
    /// each rebuilding set, below that set's cursor, while the stripe locks are
    /// still held. A mirror shadow is just another identical copy, so the value
    /// is the same `buf` slice — no parity math. Shadow-write failure sets
    /// `aborted` (Phase C aborts) but does NOT fail the foreground write.
    fn write_forward(&self, ops: &[(u64, &[u8])]) {
        let guard = self.rebuild.read();
        let Some(progress) = guard.as_ref() else {
            return;
        };
        if progress.aborted.load(Ordering::Relaxed) {
            return;
        }
        let row_size = self.desc.row_size as usize;
        for (offset, buf) in ops {
            let _ = self.for_each_segment(*offset, buf.len(), |row, set, off_in_c, range| {
                let set_idx = row * row_size + set;
                if let Some(sr) = progress
                    .targets_by_set
                    .get(set_idx)
                    .and_then(|o| o.as_ref())
                {
                    let strip_n = off_in_c / self.strip_bytes;
                    if strip_n < sr.cursor.load(Ordering::Acquire) {
                        for shadow in &sr.shadows {
                            if let Err(e) = shadow.pd.write_chunklet_user(
                                shadow.chunklet_index,
                                off_in_c,
                                &buf[range.clone()],
                            ) {
                                tracing::error!(
                                    "online rebuild (mirror): shadow write-forward failed (set {} pos {}): {} — aborting",
                                    set_idx, shadow.pos_in_set, e
                                );
                                progress.aborted.store(true, Ordering::Relaxed);
                            }
                        }
                    }
                }
                Ok(())
            });
        }
    }

    fn ensure_aligned(&self, offset: u64, len: usize) -> ChunkletResult<()> {
        let bs = self.block_size() as u64;
        if offset % bs != 0 || (len as u64) % bs != 0 {
            return Err(ChunkletError::Invariant(format!(
                "Mirror IO not block-aligned: offset={} len={} block_size={}",
                offset, len, bs
            )));
        }
        let end = offset
            .checked_add(len as u64)
            .ok_or_else(|| ChunkletError::Invariant("Mirror IO offset overflow".into()))?;
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
        F: FnMut(
            usize, /* row */
            usize, /* set */
            u64,   /* in_chunklet_off */
            std::ops::Range<usize>,
        ) -> ChunkletResult<()>,
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

            op(
                row_n,
                set_in_row,
                in_chunklet_off,
                buf_start..buf_start + take,
            )?;
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

    fn stripe_key(&self, row: usize, set: usize, in_chunklet_off: u64) -> u64 {
        let row_size = self.desc.row_size as u64;
        let strip_in_chunklet = in_chunklet_off / self.strip_bytes;
        ((row as u64 * row_size + set as u64) << 32) | strip_in_chunklet
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

    /// Round-robin pick a live copy for the (row, set) segment using that
    /// set's cursor, then walk the ring skipping missing/Failed copies.
    /// Errors only when every copy in the set is dead. Mirror "reconstruction"
    /// is just reading a surviving sibling — no parity math.
    fn pick_read_copy(
        &self,
        row: usize,
        set: usize,
    ) -> ChunkletResult<(Arc<PhysicalDisk>, u32, usize)> {
        let row_size = self.desc.row_size as usize;
        let copies = self.member_indices_for(row, set);
        let n = copies.end - copies.start;
        let start = self.read_cursors[row * row_size + set].fetch_add(1, Ordering::Relaxed) % n;
        for off in 0..n {
            let m = copies.start + (start + off) % n;
            if let Some(pd) = self.members[m].as_ref() {
                return Ok((pd.clone(), self.desc.members[m].chunklet_index, m));
            }
        }
        Err(ChunkletError::Invariant(format!(
            "Mirror set (row={}, set={}) has no live copy",
            row, set
        )))
    }

    /// Expand a batch of variable-length writes into one `StripWrite` per
    /// (strip segment × live copy), plus the per-segment stripe keys. Each
    /// write fans to EVERY live copy of its stripe; a segment with no live copy
    /// is a hard error. The flat write list + key list feed a single
    /// `parallel_strip_writes` under one `write_keys` acquisition (see
    /// `write_many_at`), so the whole coalesced batch is one cross-PD submit
    /// instead of one submit per 4 KiB strip.
    /// Expand a batch into one `StripWrite` per (segment × live copy), plus the
    /// per-segment stripe keys, the per-op redundancy-group index (`group_of`,
    /// one compact group per stripe segment), and each group's runtime failure
    /// budget (`max_fail = live_copies − 1`). The last two feed
    /// [`absorb_degraded`] so a within-budget subset of member EIOs rides
    /// through on the surviving copies instead of failing the write.
    #[allow(clippy::type_complexity)]
    fn collect_strip_writes<'a>(
        &self,
        ops: &[(u64, &'a [u8])],
    ) -> ChunkletResult<(Vec<StripWrite<'a>>, Vec<u64>, Vec<u32>, Vec<u32>)> {
        let mut writes: Vec<StripWrite<'a>> =
            Vec::with_capacity(ops.len() * self.desc.set_size as usize);
        let mut stripe_keys: Vec<u64> = Vec::new();
        let mut group_of: Vec<u32> = Vec::new();
        let mut max_fail: Vec<u32> = Vec::new();
        for (offset, buf) in ops {
            self.for_each_segment(*offset, buf.len(), |row, set, off_in_c, range| {
                let group = max_fail.len() as u32;
                stripe_keys.push(self.stripe_key(row, set, off_in_c));
                let before = writes.len();
                for member_idx in self.member_indices_for(row, set) {
                    if let Some(pd) = self.members[member_idx].as_ref() {
                        writes.push(StripWrite {
                            pd: pd.clone(),
                            chunklet_index: self.desc.members[member_idx].chunklet_index,
                            in_chunklet_off: off_in_c,
                            data: &buf[range.clone()],
                        });
                        group_of.push(group);
                    }
                }
                let copies = (writes.len() - before) as u32;
                if copies == 0 {
                    return Err(ChunkletError::Invariant(format!(
                        "Mirror set (row={}, set={}) write: no live copy",
                        row, set
                    )));
                }
                // ≥1 surviving copy keeps the segment durable, so a `copies`-way
                // mirror segment tolerates `copies - 1` runtime member failures.
                max_fail.push(copies - 1);
                Ok(())
            })?;
        }
        Ok((writes, stripe_keys, group_of, max_fail))
    }

    /// Carve `buf` into one `StripRead` per strip segment, appended to `out`.
    /// A single `&mut [u8]` can't be re-sliced by `range` inside the
    /// `for_each_segment` closure while a moving split cursor also walks it, so
    /// we collect the layout first (immutable) then peel disjoint `&mut`
    /// sub-slices with `split_at_mut`. The copy for each segment is chosen here
    /// (round-robin), so the later `parallel_strip_reads` is one batched submit.
    fn carve_reads<'b>(
        &self,
        offset: u64,
        buf: &'b mut [u8],
        out: &mut Vec<StripRead<'b>>,
        ctxs: &mut Vec<(usize, usize, usize)>,
    ) -> ChunkletResult<()> {
        let mut layout: Vec<(u64, Arc<PhysicalDisk>, u32, usize, usize, usize, usize)> = Vec::new();
        self.for_each_segment(offset, buf.len(), |row, set, off_in_c, range| {
            let (pd, chunklet_index, chosen) = self.pick_read_copy(row, set)?;
            layout.push((off_in_c, pd, chunklet_index, range.end - range.start, row, set, chosen));
            Ok(())
        })?;
        let mut rest: &mut [u8] = buf;
        for (in_chunklet_off, pd, chunklet_index, seg_len, row, set, chosen) in layout {
            let (head, tail) = rest.split_at_mut(seg_len);
            rest = tail;
            out.push(StripRead {
                pd,
                chunklet_index,
                in_chunklet_off,
                data: head,
            });
            // Parallel to `out`: the (row, set) each StripRead belongs to plus the
            // member index the fast path chose, so the reconstruct-on-EIO fallback
            // re-reads that (likely-faulting) copy first — guaranteeing the fault
            // is recorded as a suspect — then walks the surviving siblings.
            ctxs.push((row, set, chosen));
        }
        debug_assert!(rest.is_empty(), "segment lengths must sum to buf.len()");
        Ok(())
    }

    /// Fallback for the fast batched read when a member returns a runtime read
    /// fault (dead-but-not-yet-isolated). Re-reads each segment from any live
    /// copy, skipping copies that fault (recording each as a [`SuspectMember`]),
    /// so a single-member fault is transparent on the READ side — no reliance on
    /// an upper-layer journal. Suspects are reported on BOTH the Ok and Err
    /// paths so an over-budget read still triggers isolation of the dead members.
    /// Errors only when every live copy of some segment faults.
    fn reconstruct_reads(
        &self,
        reads: &mut [StripRead<'_>],
        ctxs: &[(usize, usize, usize)],
    ) -> ChunkletResult<()> {
        debug_assert_eq!(reads.len(), ctxs.len());
        let mut suspects: Vec<SuspectMember> = Vec::new();
        let mut result = Ok(());
        for (r, &(row, set, chosen)) in reads.iter_mut().zip(ctxs) {
            if let Err(e) = self.read_segment_reconstruct(
                row,
                set,
                chosen,
                r.in_chunklet_off,
                r.data,
                &mut suspects,
            ) {
                result = Err(e);
                break;
            }
        }
        self.report_suspects(suspects);
        result
    }

    /// Read one (row, set) segment for the reconstruct fallback. Tries the
    /// fast-path `chosen` copy FIRST — if it faults (the reason we fell back),
    /// that copy is recorded as a suspect and we walk the surviving siblings.
    /// Any copy that returns a runtime read fault is skipped + recorded; a
    /// structural error (Crc / Invariant / …) surfaces immediately (never
    /// reconstructed over — that could mask corruption). Errors when no live
    /// copy can serve the segment (every copy faulted / absent).
    fn read_segment_reconstruct(
        &self,
        row: usize,
        set: usize,
        chosen: usize,
        in_chunklet_off: u64,
        out: &mut [u8],
        suspects: &mut Vec<SuspectMember>,
    ) -> ChunkletResult<()> {
        let copies = self.member_indices_for(row, set);
        let mut last_err: Option<ChunkletError> = None;
        // Order: the fast-path copy first, then the rest of the set ring.
        let order = std::iter::once(chosen).chain(copies.clone().filter(|&m| m != chosen));
        for m in order {
            let Some(pd) = self.members[m].as_ref() else {
                continue;
            };
            let chunklet_idx = self.desc.members[m].chunklet_index;
            match pd.read_chunklet_user(chunklet_idx, in_chunklet_off, out) {
                Ok(()) => return Ok(()),
                Err(e) if is_runtime_read_fault(&e) => {
                    let pd_id = pd.pd_id();
                    if !suspects.iter().any(|s| s.pd_id == pd_id) {
                        suspects.push(SuspectMember {
                            pd_id,
                            chunklet_index: chunklet_idx,
                        });
                    }
                    last_err = Some(e);
                    continue;
                }
                Err(e) => return Err(e),
            }
        }
        Err(last_err.unwrap_or_else(|| {
            ChunkletError::Invariant(format!(
                "Mirror set (row={}, set={}) has no live copy for reconstruct-read",
                row, set
            ))
        }))
    }
}

impl ReconstructEngine for LdMirror {
    fn strip_bytes(&self) -> u64 {
        self.strip_bytes
    }
    fn stripes_per_chunklet(&self) -> u64 {
        LdMirror::stripes_per_chunklet(self)
    }
    fn reconstruct_member_strip(
        &self,
        failed_member_idx: usize,
        in_chunklet_off: u64,
        out: &mut [u8],
    ) -> ChunkletResult<()> {
        LdMirror::reconstruct_member_strip(self, failed_member_idx, in_chunklet_off, out)
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
        let mut reads: Vec<StripRead> = Vec::new();
        let mut ctxs: Vec<(usize, usize, usize)> = Vec::new();
        self.carve_reads(offset, buf, &mut reads, &mut ctxs)?;
        // Fast path: one batched submit picking a single copy per segment. On a
        // runtime member fault (dead-but-not-yet-isolated copy) fall back to a
        // per-segment reconstruct that reads a surviving sibling — transparent
        // to the caller, no upper-layer replay needed.
        match parallel_strip_reads(&mut reads) {
            Ok(()) => Ok(()),
            Err(e) if is_runtime_read_fault(&e) => self.reconstruct_reads(&mut reads, &ctxs),
            Err(e) => Err(e),
        }
    }

    fn read_many_at(&self, ops: &mut [(u64, &mut [u8])]) -> ChunkletResult<()> {
        for (offset, buf) in ops.iter() {
            self.ensure_aligned(*offset, buf.len())?;
        }
        // Carve every op's buffer into its strip segments and collect all
        // StripReads across all ops into one batch. `mem::take` lifts each
        // `&mut [u8]` out of `ops` at its original lifetime so the reads can
        // outlive the per-op iteration and submit together.
        let mut reads: Vec<StripRead> = Vec::with_capacity(ops.len());
        let mut ctxs: Vec<(usize, usize, usize)> = Vec::new();
        for (offset, buf_ref) in ops.iter_mut() {
            let buf: &mut [u8] = std::mem::take(buf_ref);
            self.carve_reads(*offset, buf, &mut reads, &mut ctxs)?;
        }
        match parallel_strip_reads(&mut reads) {
            Ok(()) => Ok(()),
            Err(e) if is_runtime_read_fault(&e) => self.reconstruct_reads(&mut reads, &ctxs),
            Err(e) => Err(e),
        }
    }

    fn write_at(&self, offset: u64, buf: &[u8]) -> ChunkletResult<()> {
        self.ensure_aligned(offset, buf.len())?;
        // A single contiguous op walks distinct strips, so its segments never
        // collide — collect them all, take every stripe lock once, and fan the
        // whole write to every live copy in one cross-PD submit.
        let (writes, stripe_keys, group_of, max_fail) =
            self.collect_strip_writes(&[(offset, buf)])?;
        let _guards = self.stripe_locks.write_keys(&stripe_keys);
        // Per-op results (not first-error): a single mirror leg's runtime EIO is
        // absorbed as long as ≥1 copy of each segment survived. Failed members
        // are reported for fast isolation + rebuild.
        let results = submit_strip_writes_detailed(&writes);
        let suspects = absorb_degraded(&writes, &results, &group_of, &max_fail)?;
        self.report_suspects(suspects);
        self.write_forward(&[(offset, buf)]);
        Ok(())
    }

    fn write_many_at(&self, ops: &[(u64, &[u8])]) -> ChunkletResult<()> {
        for (offset, buf) in ops {
            self.ensure_aligned(*offset, buf.len())?;
        }
        // One StripWrite per (segment × live copy) across the whole batch, so
        // a coalesced multi-strip flush span becomes ONE cross-PD submit
        // instead of one submit per 4 KiB strip.
        let (writes, stripe_keys, group_of, max_fail) = self.collect_strip_writes(ops)?;
        // Two segments hitting the same physical strip must serialize — a
        // single batched submit would race them. Disjoint ring spans (the
        // flusher) never collide; this only trips on a misbehaving caller, so
        // fall back to ordered per-op writes.
        if has_duplicate_stripes(&stripe_keys) {
            drop(writes);
            return self.write_many_fallback(ops);
        }
        let stripe_guards = self.stripe_locks.write_keys(&stripe_keys);
        let results = submit_strip_writes_detailed(&writes);
        let outcome = absorb_degraded(&writes, &results, &group_of, &max_fail);
        drop(stripe_guards);
        match outcome {
            Ok(suspects) => {
                self.report_suspects(suspects);
                self.write_forward(ops);
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    fn flush(&self) -> ChunkletResult<()> {
        crate::ld::flush_members(&self.members)
    }
}

impl LdMirror {
    fn write_many_fallback(&self, ops: &[(u64, &[u8])]) -> ChunkletResult<()> {
        for (offset, buf) in ops {
            self.write_at(*offset, buf)?;
        }
        Ok(())
    }
}

/// True if any two segments in the batch target the same physical strip.
/// `stripe_key` already folds (row, set, strip_in_chunklet) into a unique id,
/// so this subsumes exact-offset duplicates AND partial multi-strip overlap.
fn has_duplicate_stripes(keys: &[u64]) -> bool {
    let mut sorted = keys.to_vec();
    sorted.sort_unstable();
    sorted.windows(2).any(|w| w[0] == w[1])
}
