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
use std::sync::Arc;

use crate::chunklet::ChunkletHeader;
use crate::error::{ChunkletError, ChunkletResult};
use crate::ld::descriptor::LdDescriptor;
use crate::ld::{LdMirror, LdRaid5, LdRaid6};
use crate::pd::PhysicalDisk;
use crate::pool::{PdHealth, Pool};
use crate::types::{
    ChunkletState, LdId, LdRole, PdId, RaidLevel, CHUNKLET_HEADER_BYTES, CHUNKLET_SIZE,
};

/// Batch size for rebuild reconstruct work. Trades memory (K+1 batches per
/// LD set) for syscall count. 1 MiB / 4 KiB blocks = 256 strips per IO,
/// reducing the per-stripe overhead by ~256x while keeping memory at
/// (K+1) MiB per active rebuild.
const REBUILD_BATCH_BYTES: u64 = 1024 * 1024;
const CHUNKLET_USER_BYTES: u64 = CHUNKLET_SIZE - CHUNKLET_HEADER_BYTES;
const REBUILD_BATCHES_PER_CHUNKLET: u64 = (CHUNKLET_USER_BYTES + REBUILD_BATCH_BYTES - 1)
    / REBUILD_BATCH_BYTES;

#[derive(Clone, Debug)]
pub struct RebuildReport {
    pub ld_id: LdId,
    pub rebuilt_members: usize,
    pub skipped: bool,
}

impl Pool {
    /// Rebuild any failed members of the given LD onto spare chunklets on
    /// live PDs. Returns a `RebuildReport` summarizing the work done.
    ///
    /// On success, the LD descriptor is updated in-memory and persisted on
    /// every live PD's manifest. The new chunklets are marked `Used` in their
    /// PDs' bitmaps; the OLD failed chunklets are unreachable (their PD is
    /// gone), so no bitmap update is needed for them.
    pub fn rebuild_ld(&self, ld_id: LdId) -> ChunkletResult<RebuildReport> {
        let _commit = self.manifest_lock.lock();

        let desc = self
            .find_ld(ld_id)
            .ok_or_else(|| ChunkletError::Invariant(format!("LD {} not found", ld_id)))?;
        let pd_health = self.state.read().pd_health.clone();
        let pds_snapshot = self.state.read().pds.clone();
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
                let target_pd = pick_replacement_pd(
                    &working_free,
                    &pds_snapshot,
                    &used_pds,
                )?;
                let chunklet_idx = working_free
                    .get_mut(&target_pd)
                    .unwrap()
                    .remove(0);
                used_pds.push(target_pd);
                new_alloc_by_pd
                    .entry(target_pd)
                    .or_default()
                    .push((chunklet_idx, role, new_gen));
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
            RaidLevel::Mirror => self.rebuild_mirror(&desc, &new_desc, &failed_per_set, &pds_snapshot)?,
            RaidLevel::Raid5 => self.rebuild_raid5(&desc, &new_desc, &failed_per_set, &pds_snapshot)?,
            RaidLevel::Raid6 => self.rebuild_raid6(&desc, &new_desc, &failed_per_set, &pds_snapshot)?,
            _ => unreachable!("validated earlier"),
        }

        // Commit: per-PD bitmap update (Used) + ld_list refresh.
        self.commit_rebuild(&new_desc, &new_alloc_by_pd, &pds_snapshot)?;

        Ok(RebuildReport {
            ld_id,
            rebuilt_members: total_failed,
            skipped: false,
        })
    }

    /// Snapshot per-live-PD free chunklet index lists. Spare-state chunklets
    /// are folded in as well so rebuild can dip into the spare pool when free
    /// is exhausted.
    fn snapshot_working_free(
        &self,
        pds_snapshot: &BTreeMap<PdId, Arc<PhysicalDisk>>,
    ) -> ChunkletResult<BTreeMap<PdId, Vec<u32>>> {
        let mut out = BTreeMap::new();
        for (pd_id, pd) in pds_snapshot {
            let (_, bitmap, _) = pd.snapshot();
            let mut free_indices = Vec::new();
            for i in 0..bitmap.len() {
                let st = bitmap.get(i)?;
                if st == ChunkletState::Free || st == ChunkletState::Spare {
                    free_indices.push(i);
                }
            }
            out.insert(*pd_id, free_indices);
        }
        Ok(out)
    }

    fn rebuild_mirror(
        &self,
        desc: &LdDescriptor,
        new_desc: &LdDescriptor,
        failed_per_set: &[Vec<usize>],
        pds_snapshot: &BTreeMap<PdId, Arc<PhysicalDisk>>,
    ) -> ChunkletResult<()> {
        let ld = LdMirror::open(desc.clone(), pds_snapshot)?;
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
                self.write_header(target_pd, target.chunklet_index, new_desc.id, role, target.generation)?;
                for batch_n in 0..REBUILD_BATCHES_PER_CHUNKLET {
                    let off = batch_n * REBUILD_BATCH_BYTES;
                    let take = batch_take(off);
                    ld.reconstruct_member_strip(
                        global_member_idx,
                        off,
                        &mut buf[..take],
                    )?;
                    target_pd.write_chunklet_user(
                        target.chunklet_index,
                        off,
                        &buf[..take],
                    )?;
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
    ) -> ChunkletResult<()> {
        let ld = LdRaid5::open(desc.clone(), pds_snapshot)?;
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
                self.write_header(target_pd, target.chunklet_index, new_desc.id, role, target.generation)?;
                for batch_n in 0..REBUILD_BATCHES_PER_CHUNKLET {
                    let off = batch_n * REBUILD_BATCH_BYTES;
                    let take = batch_take(off);
                    ld.reconstruct_member_strip(
                        global_member_idx,
                        off,
                        &mut buf[..take],
                    )?;
                    target_pd.write_chunklet_user(
                        target.chunklet_index,
                        off,
                        &buf[..take],
                    )?;
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
    ) -> ChunkletResult<()> {
        let ld = LdRaid6::open(desc.clone(), pds_snapshot)?;
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
                self.write_header(target_pd, target.chunklet_index, new_desc.id, role, target.generation)?;
                for batch_n in 0..REBUILD_BATCHES_PER_CHUNKLET {
                    let off = batch_n * REBUILD_BATCH_BYTES;
                    let take = batch_take(off);
                    ld.reconstruct_member_strip(
                        global_member_idx,
                        off,
                        &mut buf[..take],
                    )?;
                    target_pd.write_chunklet_user(
                        target.chunklet_index,
                        off,
                        &buf[..take],
                    )?;
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
        pds_snapshot: &BTreeMap<PdId, Arc<PhysicalDisk>>,
    ) -> ChunkletResult<()> {
        // Update in-memory ld_list first so the encoded bytes reflect the new
        // placement.
        let new_ld_bytes = {
            let mut s = self.state.write();
            s.ld_list.upsert(new_desc.clone());
            s.ld_list.encode()?
        };

        for (pd_id, pd) in pds_snapshot {
            let owned = new_alloc_by_pd.get(pd_id).cloned().unwrap_or_default();
            let new_ld_bytes_v = new_ld_bytes.clone();
            pd.commit_manifest(move |body, bitmap| {
                for (idx, _role, _gen) in &owned {
                    bitmap.set(*idx, ChunkletState::Used)?;
                }
                body.ld_list_bytes = new_ld_bytes_v;
                Ok(())
            })?;
        }
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
            ChunkletError::Config(
                "rebuild: no live PD with a free chunklet outside the set".into(),
            )
        })?;
    Ok(chosen)
}
