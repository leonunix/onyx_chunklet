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
use crate::ld::{LdMirror, LdRaid5, LdRaid6};
use crate::pd::PhysicalDisk;
use crate::pool::{PdHealth, Pool, RebuildProgress, SetRebuild, ShadowTarget};
use crate::types::{
    ChunkletState, LdId, LdRole, PdId, RaidLevel, CHUNKLET_HEADER_BYTES, CHUNKLET_SIZE,
};

/// Batch size for rebuild reconstruct work. Trades memory (K+1 batches per
/// LD set) for syscall count. 1 MiB / 4 KiB blocks = 256 strips per IO,
/// reducing the per-stripe overhead by ~256x while keeping memory at
/// (K+1) MiB per active rebuild.
const REBUILD_BATCH_BYTES: u64 = 1024 * 1024;
const CHUNKLET_USER_BYTES: u64 = CHUNKLET_SIZE - CHUNKLET_HEADER_BYTES;
const REBUILD_BATCHES_PER_CHUNKLET: u64 =
    (CHUNKLET_USER_BYTES + REBUILD_BATCH_BYTES - 1) / REBUILD_BATCH_BYTES;

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
            RaidLevel::Raid6 => self.rebuild_ld_online(ld_id),
            _ => self.rebuild_ld_blocking(ld_id),
        }
    }

    /// Online (non-blocking) RAID6 rebuild. Three phases:
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
        for (set_idx, failed) in failed_per_set.iter().enumerate() {
            if failed.len() > 2 {
                return Err(ChunkletError::Invariant(format!(
                    "Raid6 set {} lost {} members (max tolerable: 2)",
                    set_idx,
                    failed.len()
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
                    role: m.role,
                    pd: pd.clone(),
                    chunklet_index: m.chunklet_index,
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
            let engine = LdRaid6::open_with_health(desc.clone(), &pds_snapshot, &pd_health)?;
            let strip_bytes = engine.strip_bytes();
            let stripes = engine.stripes_per_chunklet();
            let batch_stripes = std::cmp::max(1, REBUILD_BATCH_BYTES / strip_bytes);
            let mut buf = vec![0u8; strip_bytes as usize];
            'backfill: for (set_idx, sr) in progress.targets_by_set.iter().enumerate() {
                let Some(sr) = sr else { continue };
                let mut s = 0u64;
                while s < stripes {
                    if progress.aborted.load(Ordering::Relaxed) {
                        break 'backfill;
                    }
                    let n = std::cmp::min(batch_stripes, stripes - s);
                    // Hold the SAME stripe locks foreground writes take, in the
                    // same globally-sorted order, across reconstruct + shadow
                    // write + cursor advance for this batch.
                    let keys: Vec<u64> =
                        (s..s + n).map(|st| ((set_idx as u64) << 32) | st).collect();
                    let _guards = runtime.stripe_locks.write_keys(&keys);
                    for st in s..s + n {
                        let off = st * strip_bytes;
                        for shadow in &sr.shadows {
                            let midx = set_idx * n_per_set + shadow.pos_in_set;
                            engine.reconstruct_member_strip(midx, off, &mut buf)?;
                            if let Err(e) = shadow.pd.write_chunklet_user(
                                shadow.chunklet_index,
                                off,
                                &buf,
                            ) {
                                tracing::error!(
                                    "online rebuild: shadow backfill write failed (set {} pos {}): {} — aborting",
                                    set_idx, shadow.pos_in_set, e
                                );
                                progress.aborted.store(true, Ordering::Relaxed);
                                break 'backfill;
                            }
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
        self.commit_rebuild(&new_desc, &new_alloc_by_pd, &freed_by_pd, &pds_snapshot)?;
        *runtime.rebuild.write() = None;
        runtime.bump();

        Ok(RebuildReport {
            ld_id,
            rebuilt_members: total_failed,
            skipped: false,
        })
    }

    /// Blocking rebuild (Mirror / RAID5 today; RAID6 uses `rebuild_ld_online`).
    ///
    /// On success, the LD descriptor is updated in-memory and persisted on
    /// every live PD's manifest. New chunklets are marked `Used`; old
    /// chunklets being migrated AWAY from a live PD (drain target, or a
    /// healthy PD's scrub-marked-Bad chunklet) are marked `Free` in the
    /// same commit. Failed PDs (gone from `pds_snapshot`) get no bitmap
    /// update — their on-disk state is unreachable until they come back.
    ///
    /// Holds `io_lock.write()` for the whole reconstruct, blocking foreground
    /// IO on this LD — the flaw `rebuild_ld_online` fixes for RAID6.
    pub fn rebuild_ld_blocking(&self, ld_id: LdId) -> ChunkletResult<RebuildReport> {
        let _commit = self.manifest_lock.lock();
        let runtime = self
            .state
            .read()
            .ld_runtime
            .get(&ld_id)
            .cloned()
            .ok_or_else(|| ChunkletError::Invariant(format!("LD {} runtime not found", ld_id)))?;
        let _io = runtime.io_lock.write();

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

        // A member needs rebuild when ANY of:
        //   - its PD is Failed
        //   - its PD is currently draining (P7)
        //   - its chunklet is bitmap-Bad on its (Healthy) PD (P6)
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
                None => Ok(true), // pd_id unknown to pool; treat as failed
            }
        };

        // Identify members needing rebuild per set.
        let n_per_set = desc.set_size as usize;
        let n_sets = (desc.row_size as usize) * (desc.num_rows as usize);
        let mut failed_per_set: Vec<Vec<usize>> = vec![Vec::new(); n_sets];
        let mut total_failed = 0;
        for set_idx in 0..n_sets {
            let base = set_idx * n_per_set;
            for pos in 0..n_per_set {
                let m = &desc.members[base + pos];
                if member_needs_rebuild(m)? {
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

        // Validate redundancy budget per RAID level.
        match desc.raid_level {
            RaidLevel::Plain | RaidLevel::Raid0 => {
                return Err(ChunkletError::Unsupported(format!(
                    "{:?} has no redundancy; cannot rebuild after PD failure",
                    desc.raid_level
                )));
            }
            RaidLevel::Mirror => {
                for (set_idx, failed) in failed_per_set.iter().enumerate() {
                    if failed.len() == n_per_set {
                        return Err(ChunkletError::Invariant(format!(
                            "Mirror set {} lost all {} copies, unrecoverable",
                            set_idx, n_per_set
                        )));
                    }
                }
            }
            RaidLevel::Raid5 => {
                for (set_idx, failed) in failed_per_set.iter().enumerate() {
                    if failed.len() > 1 {
                        return Err(ChunkletError::Invariant(format!(
                            "Raid5 set {} lost {} members (max tolerable: 1)",
                            set_idx,
                            failed.len()
                        )));
                    }
                }
            }
            RaidLevel::Raid6 => {
                for (set_idx, failed) in failed_per_set.iter().enumerate() {
                    if failed.len() > 2 {
                        return Err(ChunkletError::Invariant(format!(
                            "Raid6 set {} lost {} members (max tolerable: 2)",
                            set_idx,
                            failed.len()
                        )));
                    }
                }
            }
        }

        // For each set with failures, plan target (PD, chunklet) for each
        // failed member position. Update the in-memory new descriptor.
        let mut new_desc = desc.clone();
        // Track newly-allocated chunklets per PD so we can take them out of
        // the free pool deterministically (we mutate a working copy of
        // per-PD free indices as we plan).
        let mut working_free = self.snapshot_working_free(&pds_snapshot)?;
        // Draining PDs are excluded from rebuild targets (drain is moving
        // chunklets OFF them, not onto them).
        for pd in &draining {
            working_free.remove(pd);
        }
        // For each set, build the new placement.
        let mut new_alloc_by_pd: BTreeMap<PdId, Vec<(u32, LdRole, u8)>> = BTreeMap::new();
        // Old (pd, chunklet) entries to free in the same commit. Only PDs we
        // can still talk to: draining PDs (alive but being evicted) and
        // healthy PDs whose chunklet was scrub-marked Bad. Failed PDs have
        // no entry in pds_snapshot and their bitmap is unreachable anyway —
        // commit_rebuild's notes call this out.
        let mut freed_by_pd: BTreeMap<PdId, Vec<u32>> = BTreeMap::new();
        for (set_idx, failed) in failed_per_set.iter().enumerate() {
            if failed.is_empty() {
                continue;
            }
            // Distinct PDs already used by the surviving members of this set.
            let base = set_idx * n_per_set;
            let mut used_pds: Vec<PdId> = (0..n_per_set)
                .filter(|i| !failed.contains(i))
                .map(|i| desc.members[base + i].pd)
                .collect();
            for &failed_pos in failed {
                let old_member = desc.members[base + failed_pos];
                let role = old_member.role;
                // Bump the rebuild counter so the freshly-written chunklet
                // header carries a generation > the pre-rebuild value.
                // A crash mid-rebuild then leaves a header gen != descriptor
                // gen, which forward_reconcile_bitmaps logs at next open.
                let new_gen = old_member.generation.wrapping_add(1);
                let target_pd = pick_replacement_pd(&working_free, &pds_snapshot, &used_pds)?;
                let chunklet_idx = working_free.get_mut(&target_pd).unwrap().remove(0);
                used_pds.push(target_pd);
                new_alloc_by_pd
                    .entry(target_pd)
                    .or_default()
                    .push((chunklet_idx, role, new_gen));
                // Schedule the old chunklet for Free if its PD is alive
                // (draining or healthy-with-Bad). Otherwise the PD is
                // Failed; we can't mutate its bitmap.
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

        // Reconstruct + write the new chunklets, dispatched per LD type.
        // We open a temporary LD instance built from the ORIGINAL descriptor +
        // current pds map (failed members will be `None`), which is exactly
        // what the reconstruct math needs.
        // new_desc carries the bumped per-member generation; helpers stamp
        // it into the chunklet header so a crash mid-rebuild is detectable.
        match desc.raid_level {
            RaidLevel::Mirror => {
                self.rebuild_mirror(&desc, &new_desc, &failed_per_set, &pds_snapshot, &pd_health)?
            }
            RaidLevel::Raid5 => {
                self.rebuild_raid5(&desc, &new_desc, &failed_per_set, &pds_snapshot, &pd_health)?
            }
            RaidLevel::Raid6 => {
                self.rebuild_raid6(&desc, &new_desc, &failed_per_set, &pds_snapshot, &pd_health)?
            }
            _ => unreachable!("validated earlier"),
        }

        // Commit: per-PD bitmap update (Used for new allocations + Free for
        // old chunklets being migrated off live PDs) + ld_list refresh.
        self.commit_rebuild(&new_desc, &new_alloc_by_pd, &freed_by_pd, &pds_snapshot)?;
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

    fn rebuild_mirror(
        &self,
        desc: &LdDescriptor,
        new_desc: &LdDescriptor,
        failed_per_set: &[Vec<usize>],
        pds_snapshot: &BTreeMap<PdId, Arc<PhysicalDisk>>,
        pd_health: &BTreeMap<PdId, PdHealth>,
    ) -> ChunkletResult<()> {
        let ld = LdMirror::open_with_health(desc.clone(), pds_snapshot, pd_health)?;
        let n_per_set = desc.set_size as usize;
        let mut buf = vec![0u8; REBUILD_BATCH_BYTES as usize];
        for (set_idx, failed) in failed_per_set.iter().enumerate() {
            for &pos in failed {
                let global_member_idx = set_idx * n_per_set + pos;
                let role = desc.members[global_member_idx].role;
                let target = &new_desc.members[global_member_idx];
                let target_pd = pds_snapshot.get(&target.pd).ok_or_else(|| {
                    ChunkletError::Invariant(format!("rebuild target PD {} missing", target.pd))
                })?;
                self.write_header(
                    target_pd,
                    target.chunklet_index,
                    new_desc.id,
                    role,
                    target.generation,
                )?;
                for batch_n in 0..REBUILD_BATCHES_PER_CHUNKLET {
                    let off = batch_n * REBUILD_BATCH_BYTES;
                    let take = batch_take(off);
                    ld.reconstruct_member_strip(global_member_idx, off, &mut buf[..take])?;
                    target_pd.write_chunklet_user(target.chunklet_index, off, &buf[..take])?;
                }
                target_pd.sync()?;
            }
        }
        Ok(())
    }

    fn rebuild_raid5(
        &self,
        desc: &LdDescriptor,
        new_desc: &LdDescriptor,
        failed_per_set: &[Vec<usize>],
        pds_snapshot: &BTreeMap<PdId, Arc<PhysicalDisk>>,
        pd_health: &BTreeMap<PdId, PdHealth>,
    ) -> ChunkletResult<()> {
        let ld = LdRaid5::open_with_health(desc.clone(), pds_snapshot, pd_health)?;
        let n_per_set = desc.set_size as usize;
        let mut buf = vec![0u8; REBUILD_BATCH_BYTES as usize];
        for (set_idx, failed) in failed_per_set.iter().enumerate() {
            for &pos in failed {
                let global_member_idx = set_idx * n_per_set + pos;
                let role = desc.members[global_member_idx].role;
                let target = &new_desc.members[global_member_idx];
                let target_pd = pds_snapshot.get(&target.pd).ok_or_else(|| {
                    ChunkletError::Invariant(format!("rebuild target PD {} missing", target.pd))
                })?;
                self.write_header(
                    target_pd,
                    target.chunklet_index,
                    new_desc.id,
                    role,
                    target.generation,
                )?;
                for batch_n in 0..REBUILD_BATCHES_PER_CHUNKLET {
                    let off = batch_n * REBUILD_BATCH_BYTES;
                    let take = batch_take(off);
                    ld.reconstruct_member_strip(global_member_idx, off, &mut buf[..take])?;
                    target_pd.write_chunklet_user(target.chunklet_index, off, &buf[..take])?;
                }
                target_pd.sync()?;
            }
        }
        Ok(())
    }

    fn rebuild_raid6(
        &self,
        desc: &LdDescriptor,
        new_desc: &LdDescriptor,
        failed_per_set: &[Vec<usize>],
        pds_snapshot: &BTreeMap<PdId, Arc<PhysicalDisk>>,
        pd_health: &BTreeMap<PdId, PdHealth>,
    ) -> ChunkletResult<()> {
        let ld = LdRaid6::open_with_health(desc.clone(), pds_snapshot, pd_health)?;
        let n_per_set = desc.set_size as usize;
        let mut buf = vec![0u8; REBUILD_BATCH_BYTES as usize];
        for (set_idx, failed) in failed_per_set.iter().enumerate() {
            for &pos in failed {
                let global_member_idx = set_idx * n_per_set + pos;
                let role = desc.members[global_member_idx].role;
                let target = &new_desc.members[global_member_idx];
                let target_pd = pds_snapshot.get(&target.pd).ok_or_else(|| {
                    ChunkletError::Invariant(format!("rebuild target PD {} missing", target.pd))
                })?;
                self.write_header(
                    target_pd,
                    target.chunklet_index,
                    new_desc.id,
                    role,
                    target.generation,
                )?;
                for batch_n in 0..REBUILD_BATCHES_PER_CHUNKLET {
                    let off = batch_n * REBUILD_BATCH_BYTES;
                    let take = batch_take(off);
                    ld.reconstruct_member_strip(global_member_idx, off, &mut buf[..take])?;
                    target_pd.write_chunklet_user(target.chunklet_index, off, &buf[..take])?;
                }
                target_pd.sync()?;
            }
        }
        Ok(())
    }

    fn write_header(
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

    fn commit_rebuild(
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

/// Bytes to read/write for the batch starting at `off` within a chunklet.
/// Capped by the chunklet's user region.
fn batch_take(off: u64) -> usize {
    let remain = CHUNKLET_USER_BYTES.saturating_sub(off);
    std::cmp::min(REBUILD_BATCH_BYTES, remain) as usize
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
