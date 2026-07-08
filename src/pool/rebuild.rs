//! Pool::rebuild_ld — synchronous LD rebuild after PD failure.
//!
//! Walks the LD descriptor, finds members on Failed PDs, allocates
//! replacement chunklets on live PDs (HA constraint: not on a PD already
//! used by other live members of the same set), reconstructs the data
//! via the LD's `reconstruct_member_strip` helper, and commits the new
//! descriptor + bitmap state across all live PDs.
//!
//! Only Mirror / Raid5 / Raid6 can be rebuilt; Plain / Raid0 fail with
//! `Unsupported` since they have no redundancy. Raid5 can only tolerate
//! 1 failure per set, Raid6 up to 2. More failures than the level can
//! cover return `Invariant("unrecoverable")`.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use crate::chunklet::ChunkletHeader;
use crate::error::{ChunkletError, ChunkletResult};
use crate::ld::descriptor::LdDescriptor;
use crate::ld::{LdMirror, LdRaid5, LdRaid6, ReconstructEngine};
use crate::pd::PhysicalDisk;
use crate::pool::{PdHealth, Pool, RebuildProgress, SetRebuild, ShadowTarget};
use crate::types::{ChunkletState, LdId, LdRole, PdId, RaidLevel};

/// Batch size for the online-rebuild backfill: how many contiguous strips a
/// single stripe-lock acquisition covers, reconstructed with one range read
/// per survivor + one range write per shadow.
///
/// This is deliberately SMALL (not throughput-maximizing). Phase B holds the
/// SAME stripe locks foreground writes take (a 1024-bucket hashed table,
/// `ld/mod.rs`), so the batch size directly bounds how much foreground write
/// latency a rebuild can inflict: at 256 KiB / 4 KiB = 64 strips, a batch
/// holds ~64 of 1024 buckets for ~one range-IO round-trip. The disks are
/// near-idle during rebuild — the lock, not bandwidth, was starving foreground
/// — so we keep the window tiny rather than chase rebuild speed. (The old
/// 1 MiB value only batched the LOCK; the IO was still per-4 KiB-strip and
/// synchronous, so a batch held ~256 buckets for ~25 ms and devastated
/// foreground writes.)
const REBUILD_BATCH_BYTES: u64 = 256 * 1024;

#[derive(Clone, Debug)]
pub struct RebuildReport {
    pub ld_id: LdId,
    pub rebuilt_members: usize,
    pub skipped: bool,
}

impl Pool {
    /// Rebuild any failed members of the given LD onto spare chunklets.
    ///
    /// RAID6 uses the ONLINE (non-blocking) path — foreground IO keeps flowing
    /// during the backfill. Other levels still use the blocking path (converted
    /// in a follow-up). `auto_recover` / `drain_pd` reach this same entry point.
    pub fn rebuild_ld(&self, ld_id: LdId) -> ChunkletResult<RebuildReport> {
        let level = self
            .find_ld(ld_id)
            .ok_or_else(|| ChunkletError::Invariant(format!("LD {} not found", ld_id)))?
            .raid_level;
        match level {
            RaidLevel::Mirror | RaidLevel::Raid5 | RaidLevel::Raid6 => {
                self.rebuild_ld_online(ld_id)
            }
            RaidLevel::Plain | RaidLevel::Raid0 => Err(ChunkletError::Unsupported(format!(
                "{:?} has no redundancy; cannot rebuild after PD failure",
                level
            ))),
        }
    }

    /// Online (non-blocking) rebuild for redundant LDs (Mirror / Raid5 / Raid6).
    /// Three phases:
    ///
    /// - **A** (brief `manifest_lock` + `io_lock.write()`): plan spares, write
    ///   their chunklet headers, mark them `Migrating`, install `RebuildProgress`
    ///   into the runtime cell. The descriptor is NOT changed and the epoch is
    ///   NOT bumped, so live handles keep serving and just start observing the
    ///   rebuild cell.
    /// - **B** (`io_lock.read()`, concurrent with foreground): backfill each
    ///   failed member's shadow chunklet from current survivors, one stripe
    ///   batch at a time under the SHARED stripe lock, advancing the per-set
    ///   cursor. Foreground writes below the cursor write-forward to the shadow
    ///   (see `LdRaid6::write_forward`); the failed position stays `None` so
    ///   reads reconstruct as before.
    /// - **C** (brief `manifest_lock` + `io_lock.write()`): if a shadow write
    ///   failed, reclaim the `Migrating` spares and return `Err`. Otherwise swap
    ///   the descriptor to the spares (`Migrating`→`Used`, old chunklet→`Free`),
    ///   clear the cell, and bump the epoch so handles reopen onto the spares.
    fn rebuild_ld_online(&self, ld_id: LdId) -> ChunkletResult<RebuildReport> {
        // ---------------------------- Phase A ----------------------------
        let commit_a = self.manifest_lock.lock();
        let runtime = self
            .state
            .read()
            .ld_runtime
            .get(&ld_id)
            .cloned()
            .ok_or_else(|| ChunkletError::Invariant(format!("LD {} runtime not found", ld_id)))?;
        let io_a = runtime.io_lock.write();

        // Concurrency gate: the `rebuild` cell is the per-LD "migration in flight"
        // mutex. Phase B runs under `io_lock.read()`, so without this a second
        // rebuild / failover / rebalance entering Phase A could clobber the cell
        // and race the first migration's shadow set. Refuse if one is already
        // live. (Also hardens rebalance, which shares this cell.)
        if runtime.rebuild.read().is_some() {
            return Err(ChunkletError::Invariant(format!(
                "LD {} already has an online migration (rebuild/rebalance) in flight",
                ld_id
            )));
        }

        let desc = self
            .find_ld(ld_id)
            .ok_or_else(|| ChunkletError::Invariant(format!("LD {} not found", ld_id)))?;
        let pd_health = self.state.read().pd_health.clone();
        let pds_snapshot: BTreeMap<PdId, Arc<PhysicalDisk>> = self
            .state
            .read()
            .pds
            .iter()
            .filter(|(pd_id, _)| pd_health.get(pd_id) != Some(&PdHealth::Failed))
            .map(|(pd_id, pd)| (*pd_id, pd.clone()))
            .collect();
        let draining = self.state.read().draining.clone();

        // Identify members needing rebuild per set (same rule as blocking).
        let member_needs_rebuild = |m: &crate::types::LdMember| -> ChunkletResult<bool> {
            if draining.contains(&m.pd) {
                return Ok(true);
            }
            match pd_health.get(&m.pd) {
                Some(PdHealth::Failed) => Ok(true),
                Some(PdHealth::Healthy) => {
                    let pd = pds_snapshot.get(&m.pd).ok_or_else(|| {
                        ChunkletError::Invariant(format!(
                            "PD {} health=Healthy but missing from pds map",
                            m.pd
                        ))
                    })?;
                    let (_, bitmap, _) = pd.snapshot();
                    Ok(matches!(bitmap.get(m.chunklet_index)?, ChunkletState::Bad))
                }
                None => Ok(true),
            }
        };
        let n_per_set = desc.set_size as usize;
        let n_sets = (desc.row_size as usize) * (desc.num_rows as usize);
        let mut failed_per_set: Vec<Vec<usize>> = vec![Vec::new(); n_sets];
        let mut total_failed = 0usize;
        for set_idx in 0..n_sets {
            let base = set_idx * n_per_set;
            for pos in 0..n_per_set {
                if member_needs_rebuild(&desc.members[base + pos])? {
                    failed_per_set[set_idx].push(pos);
                    total_failed += 1;
                }
            }
        }
        if total_failed == 0 {
            return Ok(RebuildReport {
                ld_id,
                rebuilt_members: 0,
                skipped: true,
            });
        }
        let max_failed_per_set = match desc.raid_level {
            RaidLevel::Mirror => n_per_set - 1, // must keep >= 1 copy
            RaidLevel::Raid5 => 1,
            RaidLevel::Raid6 => 2,
            _ => 0,
        };
        for (set_idx, failed) in failed_per_set.iter().enumerate() {
            if failed.len() > max_failed_per_set {
                return Err(ChunkletError::Invariant(format!(
                    "{:?} set {} lost {} members (max tolerable: {})",
                    desc.raid_level,
                    set_idx,
                    failed.len(),
                    max_failed_per_set
                )));
            }
        }

        // Plan the spare placement (same as blocking): new_desc points failed
        // positions at fresh chunklets on live PDs outside the surviving set.
        let mut new_desc = desc.clone();
        let mut working_free = self.snapshot_working_free(&pds_snapshot)?;
        for pd in &draining {
            working_free.remove(pd);
        }
        let mut new_alloc_by_pd: BTreeMap<PdId, Vec<(u32, LdRole, u8)>> = BTreeMap::new();
        let mut freed_by_pd: BTreeMap<PdId, Vec<u32>> = BTreeMap::new();
        for (set_idx, failed) in failed_per_set.iter().enumerate() {
            if failed.is_empty() {
                continue;
            }
            let base = set_idx * n_per_set;
            let mut used_pds: Vec<PdId> = (0..n_per_set)
                .filter(|i| !failed.contains(i))
                .map(|i| desc.members[base + i].pd)
                .collect();
            for &failed_pos in failed {
                let old_member = desc.members[base + failed_pos];
                let role = old_member.role;
                let new_gen = old_member.generation.wrapping_add(1);
                let target_pd = pick_replacement_pd(&working_free, &pds_snapshot, &used_pds)?;
                let chunklet_idx = working_free.get_mut(&target_pd).unwrap().remove(0);
                used_pds.push(target_pd);
                new_alloc_by_pd
                    .entry(target_pd)
                    .or_default()
                    .push((chunklet_idx, role, new_gen));
                if pds_snapshot.contains_key(&old_member.pd) {
                    freed_by_pd
                        .entry(old_member.pd)
                        .or_default()
                        .push(old_member.chunklet_index);
                }
                let m = &mut new_desc.members[base + failed_pos];
                m.pd = target_pd;
                m.chunklet_index = chunklet_idx;
                m.generation = new_gen;
            }
        }

        // Write the shadow chunklet headers + mark the spares Migrating (invisible
        // to the allocator; reclaimed at open if we crash before Phase C). The
        // descriptor still names the OLD failed members, so foreground stays
        // degraded (reads reconstruct) until Phase C swaps.
        for (pd_id, allocs) in &new_alloc_by_pd {
            let pd = pds_snapshot
                .get(pd_id)
                .ok_or_else(|| ChunkletError::Invariant(format!("shadow PD {} missing", pd_id)))?;
            for (chunklet_idx, role, gen) in allocs {
                self.write_header(pd, *chunklet_idx, new_desc.id, *role, *gen)?;
            }
            let idxs: Vec<u32> = allocs.iter().map(|(i, _, _)| *i).collect();
            pd.commit_manifest(move |_body, bitmap| {
                for i in &idxs {
                    bitmap.set(*i, ChunkletState::Migrating)?;
                }
                Ok(())
            })?;
        }

        // Build + install the rebuild plan cell. targets_by_set[s] carries the
        // shadow targets for set s (aligned to failed positions in new_desc).
        let mut targets_by_set: Vec<Option<SetRebuild>> = Vec::with_capacity(n_sets);
        for (set_idx, failed) in failed_per_set.iter().enumerate() {
            if failed.is_empty() {
                targets_by_set.push(None);
                continue;
            }
            let base = set_idx * n_per_set;
            let mut shadows = Vec::with_capacity(failed.len());
            for &pos in failed {
                let m = &new_desc.members[base + pos];
                let pd = pds_snapshot.get(&m.pd).ok_or_else(|| {
                    ChunkletError::Invariant(format!("shadow PD {} missing", m.pd))
                })?;
                shadows.push(ShadowTarget {
                    pos_in_set: pos,
                    pd: pd.clone(),
                    chunklet_index: m.chunklet_index,
                    // Rebuild reconstructs from survivors; no copy source.
                    copy_source: None,
                });
            }
            targets_by_set.push(Some(SetRebuild {
                cursor: AtomicU64::new(0),
                shadows,
            }));
        }
        let progress = Arc::new(RebuildProgress {
            targets_by_set,
            aborted: AtomicBool::new(false),
        });
        *runtime.rebuild.write() = Some(progress.clone());

        drop(io_a);
        drop(commit_a);

        // ---------------------------- Phase B ----------------------------
        // Backfill each shadow from current survivors, concurrent with fg IO.
        {
            let _io = runtime.io_lock.read();
            let engine: Box<dyn ReconstructEngine> = match desc.raid_level {
                RaidLevel::Mirror => {
                    Box::new(LdMirror::open_with_health(desc.clone(), &pds_snapshot, &pd_health)?)
                }
                RaidLevel::Raid5 => {
                    Box::new(LdRaid5::open_with_health(desc.clone(), &pds_snapshot, &pd_health)?)
                }
                RaidLevel::Raid6 => {
                    Box::new(LdRaid6::open_with_health(desc.clone(), &pds_snapshot, &pd_health)?)
                }
                _ => unreachable!("online rebuild only reached for redundant levels"),
            };
            let strip_bytes = engine.strip_bytes();
            let stripes = engine.stripes_per_chunklet();
            let batch_stripes = std::cmp::max(1, REBUILD_BATCH_BYTES / strip_bytes);
            // One range buffer reused across batches, sized to a full batch so a
            // batch is a single contiguous reconstruct-read per survivor + a
            // single contiguous shadow write — not `batch_stripes` synchronous
            // per-strip round-trips under the held stripe lock.
            let mut buf = vec![0u8; (batch_stripes * strip_bytes) as usize];
            'backfill: for (set_idx, sr) in progress.targets_by_set.iter().enumerate() {
                let Some(sr) = sr else { continue };
                let mut s = 0u64;
                while s < stripes {
                    if progress.aborted.load(Ordering::Relaxed) {
                        break 'backfill;
                    }
                    let n = std::cmp::min(batch_stripes, stripes - s);
                    let range_len = (n * strip_bytes) as usize;
                    let base_off = s * strip_bytes;
                    // Hold the SAME stripe locks foreground writes take, in the
                    // same globally-sorted order, across reconstruct + shadow
                    // write + cursor advance for this batch. Keeping the batch
                    // small bounds both the hold TIME (one range-IO round-trip,
                    // not `n` synchronous 4 KiB round-trips) and the hold BREADTH
                    // (`n` of 1024 hashed buckets) so foreground writes are not
                    // starved — see REBUILD_BATCH_BYTES.
                    let keys: Vec<u64> =
                        (s..s + n).map(|st| ((set_idx as u64) << 32) | st).collect();
                    let _guards = runtime.stripe_locks.write_keys(&keys);
                    for shadow in &sr.shadows {
                        let midx = set_idx * n_per_set + shadow.pos_in_set;
                        // Range reconstruct: reconstruct_member_strip reads
                        // `range_len` contiguous bytes from each survivor in one
                        // syscall and reconstructs all `n` strips in memory. The
                        // GF math is pointwise with position-indexed constants, so
                        // a range buffer is byte-identical to `n` per-strip calls.
                        engine.reconstruct_member_strip(midx, base_off, &mut buf[..range_len])?;
                        if let Err(e) = shadow.pd.write_chunklet_user(
                            shadow.chunklet_index,
                            base_off,
                            &buf[..range_len],
                        ) {
                            tracing::error!(
                                "online rebuild: shadow backfill write failed (set {} pos {}): {} — aborting",
                                set_idx, shadow.pos_in_set, e
                            );
                            progress.aborted.store(true, Ordering::Relaxed);
                            break 'backfill;
                        }
                    }
                    sr.cursor.store(s + n, Ordering::Release);
                    s += n;
                }
            }
            // Make the shadow data durable before the descriptor swap.
            if !progress.aborted.load(Ordering::Relaxed) {
                for sr in progress.targets_by_set.iter().flatten() {
                    for shadow in &sr.shadows {
                        shadow.pd.sync()?;
                    }
                }
            }
        }

        // ---------------------------- Phase C ----------------------------
        let _commit_c = self.manifest_lock.lock();
        let _io_c = runtime.io_lock.write();
        if progress.aborted.load(Ordering::Relaxed) {
            // Reclaim the Migrating spares → Free; leave the LD degraded (still
            // redundant) and let a later rebuild retry.
            for (pd_id, allocs) in &new_alloc_by_pd {
                if let Some(pd) = pds_snapshot.get(pd_id) {
                    let idxs: Vec<u32> = allocs.iter().map(|(i, _, _)| *i).collect();
                    let _ = pd.commit_manifest(move |_body, bitmap| {
                        for i in &idxs {
                            bitmap.set(*i, ChunkletState::Free)?;
                        }
                        Ok(())
                    });
                }
            }
            *runtime.rebuild.write() = None;
            return Err(ChunkletError::Invariant(format!(
                "online rebuild of LD {} aborted: shadow PD write failure",
                ld_id
            )));
        }
        // Atomic swap: spare Migrating→Used, descriptor→new_desc, old→Free.
        // Clear the cell whether or not the commit succeeds: a mid-loop cross-PD
        // commit failure otherwise returns via `?` BEFORE the clear, leaking the
        // cell so the per-LD migration gate rejects every future rebuild/failover
        // on this LD until restart. A torn cross-PD commit is reconciled at next
        // open (forward_reconcile_bitmaps + reclaim_orphan_migrating).
        let committed = self.commit_rebuild(&new_desc, &new_alloc_by_pd, &freed_by_pd, &pds_snapshot);
        *runtime.rebuild.write() = None;
        committed?;
        runtime.bump();

        Ok(RebuildReport {
            ld_id,
            rebuilt_members: total_failed,
            skipped: false,
        })
    }


    /// Snapshot per-live-PD free chunklet index lists. Spare-state chunklets
    /// are folded in as well so rebuild can dip into the spare pool when free
    /// is exhausted. Thin wrapper over the shared `collect_free_indices_per_pd`
    /// helper in `pool/mod.rs`.
    fn snapshot_working_free(
        &self,
        pds_snapshot: &BTreeMap<PdId, Arc<PhysicalDisk>>,
    ) -> ChunkletResult<BTreeMap<PdId, Vec<u32>>> {
        crate::pool::collect_free_indices_per_pd(pds_snapshot, /* include_spare */ true)
    }

    pub(super) fn write_header(
        &self,
        pd: &PhysicalDisk,
        chunklet_idx: u32,
        owner_ld: LdId,
        role: LdRole,
        generation: u8,
    ) -> ChunkletResult<()> {
        let header = ChunkletHeader {
            owner_ld,
            chunklet_index: chunklet_idx,
            role,
            // ChunkletHeader carries u64 generation but only the low 8 bits
            // are meaningful; LdMember stores u8 to fit a previously-reserved
            // descriptor byte. Wrap-around at 256 same-position rebuilds is
            // theoretical, not realistic.
            generation: generation as u64,
        };
        pd.write_chunklet_header(chunklet_idx, &header.encode())?;
        Ok(())
    }

    pub(super) fn commit_rebuild(
        &self,
        new_desc: &LdDescriptor,
        new_alloc_by_pd: &BTreeMap<PdId, Vec<(u32, LdRole, u8)>>,
        freed_by_pd: &BTreeMap<PdId, Vec<u32>>,
        pds_snapshot: &BTreeMap<PdId, Arc<PhysicalDisk>>,
    ) -> ChunkletResult<()> {
        // Encode the next descriptor without publishing it in memory until
        // every live PD has committed. Existing handles keep seeing the old
        // epoch while rebuild holds the LD runtime write lock.
        let new_ld_bytes = {
            let s = self.state.read();
            let mut next = s.ld_list.clone();
            next.upsert(new_desc.clone());
            next.encode()?
        };

        for (pd_id, pd) in pds_snapshot {
            let owned = new_alloc_by_pd.get(pd_id).cloned().unwrap_or_default();
            let freed = freed_by_pd.get(pd_id).cloned().unwrap_or_default();
            let new_ld_bytes_v = new_ld_bytes.clone();
            pd.commit_manifest(move |body, bitmap| {
                // New chunklets allocated on this PD: Free -> Used.
                for (idx, _role, _gen) in &owned {
                    bitmap.set(*idx, ChunkletState::Used)?;
                }
                // Old chunklets migrated AWAY from this PD: Used (or Bad)
                // -> Free. Same atomic commit ensures bitmap accounting is
                // consistent with the descriptor swap.
                for idx in &freed {
                    bitmap.set(*idx, ChunkletState::Free)?;
                }
                body.ld_list_bytes = new_ld_bytes_v;
                Ok(())
            })?;
        }
        self.state.write().ld_list.upsert(new_desc.clone());
        Ok(())
    }
}

/// Pick a live PD that is not in `forbidden` and has at least one free
/// chunklet in `working_free`. Tie-break by largest free count (most balanced).
fn pick_replacement_pd(
    working_free: &BTreeMap<PdId, Vec<u32>>,
    _pds_snapshot: &BTreeMap<PdId, Arc<PhysicalDisk>>,
    forbidden: &[PdId],
) -> ChunkletResult<PdId> {
    let chosen = working_free
        .iter()
        .filter(|(pd, free)| !forbidden.contains(pd) && !free.is_empty())
        .max_by_key(|(pd, free)| (free.len(), std::cmp::Reverse(*pd)))
        .map(|(pd, _)| *pd)
        .ok_or_else(|| {
            ChunkletError::Config("rebuild: no live PD with a free chunklet outside the set".into())
        })?;
    Ok(chosen)
}
