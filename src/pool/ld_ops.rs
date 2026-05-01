//! LD lifecycle ops on `Pool`: create / open / drop / list.
//!
//! Lives in its own file because `pool/mod.rs` is bounded at ~400 lines for
//! readability; this module gathers the cross-PD LD-allocation work and
//! manifest commits in one place.
//!
//! All mutating ops take `pool.manifest_lock` exclusively — concurrent
//! `create_ld` / `drop_ld` / `admit` are serialized.

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::allocator::{plan_alloc, AllocRequest, PdFreeView};
use crate::chunklet::ChunkletHeader;
use crate::error::{ChunkletError, ChunkletResult};
use crate::ld::descriptor::LdDescriptor;
use crate::ld::{LdMirror, LdPlain, LdRaid5, LogicalDisk};
use crate::pd::PhysicalDisk;
use crate::pool::Pool;
use crate::types::{ChunkletState, HaDomain, LdId, LdRole, PdId, RaidLevel};

/// Caller-supplied LD-creation spec.
#[derive(Clone, Debug)]
pub struct LdSpec {
    pub raid_level: RaidLevel,
    pub set_size: u8,
    pub row_size: u16,
    pub num_rows: u16,
    pub strip_size_log2: u8,
    pub ha_domain: HaDomain,
}

impl LdSpec {
    /// Linear concat of `chunklet_count` chunklets (no redundancy).
    pub fn plain(chunklet_count: u16) -> Self {
        Self {
            raid_level: RaidLevel::Plain,
            set_size: 1,
            row_size: 1,
            num_rows: chunklet_count,
            strip_size_log2: 0,
            ha_domain: HaDomain::Pd,
        }
    }

    /// Mirror spec. `copies` = mirror factor (e.g. 2 for RAID-1, 3 for triple-
    /// mirror). `row_size` controls striping width across mirror sets — set to
    /// 1 for plain RAID-1, > 1 for RAID-10. `num_rows` extends capacity by
    /// chaining additional rows.
    ///
    /// `strip_size_log2 = 0` defaults the strip to one block (4 KiB).
    pub fn mirror(copies: u8, row_size: u16, num_rows: u16, strip_size_log2: u8) -> Self {
        Self {
            raid_level: RaidLevel::Mirror,
            set_size: copies,
            row_size,
            num_rows,
            strip_size_log2,
            ha_domain: HaDomain::Pd,
        }
    }

    /// RAID-5 spec. `data_per_set` = K (data chunklets per set; total set
    /// size will be K + 1). `row_size` and `num_rows` control how many
    /// independent RAID-5 sets are striped together / chained.
    ///
    /// `strip_size_log2 = 0` defaults the strip to one block (4 KiB).
    pub fn raid5(data_per_set: u8, row_size: u16, num_rows: u16, strip_size_log2: u8) -> Self {
        Self {
            raid_level: RaidLevel::Raid5,
            set_size: data_per_set + 1,
            row_size,
            num_rows,
            strip_size_log2,
            ha_domain: HaDomain::Pd,
        }
    }
}

impl Pool {
    pub fn list_lds(&self) -> Vec<LdDescriptor> {
        self.state.read().ld_list.lds.clone()
    }

    pub fn find_ld(&self, id: LdId) -> Option<LdDescriptor> {
        self.state.read().ld_list.find(id).cloned()
    }

    /// Plan + create a new LD. Phase 1 supports `Plain`; Phase 2 adds
    /// `Mirror`. Other levels return `Unsupported`.
    ///
    /// On success the new LD is durably persisted on every PD's manifest.
    pub fn create_ld(&self, spec: LdSpec) -> ChunkletResult<LdId> {
        match spec.raid_level {
            RaidLevel::Plain => {
                if spec.set_size != 1 {
                    return Err(ChunkletError::Invariant(format!(
                        "Plain LD must have set_size=1, got {}",
                        spec.set_size
                    )));
                }
            }
            RaidLevel::Mirror => {
                if spec.set_size < 2 {
                    return Err(ChunkletError::Invariant(format!(
                        "Mirror LD requires set_size >= 2, got {}",
                        spec.set_size
                    )));
                }
                if spec.row_size == 0 || spec.num_rows == 0 {
                    return Err(ChunkletError::Invariant(
                        "Mirror LD requires row_size >= 1 and num_rows >= 1".into(),
                    ));
                }
            }
            RaidLevel::Raid5 => {
                if spec.set_size < 3 {
                    return Err(ChunkletError::Invariant(format!(
                        "Raid5 LD requires set_size >= 3 (>= 2+1), got {}",
                        spec.set_size
                    )));
                }
                if spec.row_size == 0 || spec.num_rows == 0 {
                    return Err(ChunkletError::Invariant(
                        "Raid5 LD requires row_size >= 1 and num_rows >= 1".into(),
                    ));
                }
            }
            other => {
                return Err(ChunkletError::Unsupported(format!(
                    "raid_level {:?} not yet implemented",
                    other
                )));
            }
        }

        let _commit = self.manifest_lock.lock();

        let pd_views = self.snapshot_free_views();
        let total_members = (spec.set_size as usize)
            * (spec.row_size as usize)
            * (spec.num_rows as usize);
        // Build per-set role pattern, then repeat for every set in the LD.
        let role_per_set: Vec<LdRole> = match spec.raid_level {
            RaidLevel::Plain | RaidLevel::Mirror => {
                vec![LdRole::Data; spec.set_size as usize]
            }
            RaidLevel::Raid5 => {
                let mut v = vec![LdRole::Data; (spec.set_size - 1) as usize];
                v.push(LdRole::ParityP);
                v
            }
            RaidLevel::Raid6 => {
                let mut v = vec![LdRole::Data; (spec.set_size - 2) as usize];
                v.push(LdRole::ParityP);
                v.push(LdRole::ParityQ);
                v
            }
        };
        let mut role_assignments = Vec::with_capacity(total_members);
        for _ in 0..((spec.row_size as usize) * (spec.num_rows as usize)) {
            role_assignments.extend_from_slice(&role_per_set);
        }
        debug_assert_eq!(role_assignments.len(), total_members);
        let req = AllocRequest {
            set_size: spec.set_size,
            row_size: spec.row_size,
            num_rows: spec.num_rows,
            role_assignments,
            ha_domain: spec.ha_domain,
        };
        let plan = plan_alloc(&req, pd_views)?;

        let new_id = LdId::new_v4();
        let desc = LdDescriptor {
            id: new_id,
            raid_level: spec.raid_level,
            set_size: spec.set_size,
            row_size: spec.row_size,
            num_rows: spec.num_rows,
            strip_size_log2: spec.strip_size_log2,
            members: plan.members,
        };

        self.commit_new_ld(desc)?;
        Ok(new_id)
    }

    /// Open an LD by id and return a handle implementing `LogicalDisk`.
    pub fn open_ld(&self, id: LdId) -> ChunkletResult<Arc<dyn LogicalDisk>> {
        let s = self.state.read();
        let desc = s
            .ld_list
            .find(id)
            .cloned()
            .ok_or_else(|| ChunkletError::Invariant(format!("LD {} not found", id)))?;
        match desc.raid_level {
            RaidLevel::Plain => {
                let plain = LdPlain::open(desc, &s.pds)?;
                Ok(Arc::new(plain))
            }
            RaidLevel::Mirror => {
                let mirror = LdMirror::open(desc, &s.pds)?;
                Ok(Arc::new(mirror))
            }
            RaidLevel::Raid5 => {
                let raid5 = LdRaid5::open(desc, &s.pds)?;
                Ok(Arc::new(raid5))
            }
            other => Err(ChunkletError::Unsupported(format!(
                "raid_level {:?} not implemented yet",
                other
            ))),
        }
    }

    /// Drop an LD: free all its chunklets and remove from the pool's LD list.
    pub fn drop_ld(&self, id: LdId) -> ChunkletResult<()> {
        let _commit = self.manifest_lock.lock();
        let removed = {
            let mut s = self.state.write();
            s.ld_list.remove(id)
        }
        .ok_or_else(|| ChunkletError::Invariant(format!("LD {} not found", id)))?;

        let new_ld_bytes = self.state.read().ld_list.encode()?;
        let mut by_pd: BTreeMap<PdId, Vec<u32>> = BTreeMap::new();
        for m in &removed.members {
            by_pd.entry(m.pd).or_default().push(m.chunklet_index);
        }
        let pds_snapshot = self.state.read().pds.clone();
        for (pd_id, chunklets) in &by_pd {
            let pd = pds_snapshot.get(pd_id).ok_or_else(|| {
                ChunkletError::Invariant(format!("LD member references unknown PD {}", pd_id))
            })?;
            let new_ld_bytes_clone = new_ld_bytes.clone();
            let chunklets_clone = chunklets.clone();
            pd.commit_manifest(move |body, bitmap| {
                for &idx in &chunklets_clone {
                    bitmap.set(idx, ChunkletState::Free)?;
                }
                body.ld_list_bytes = new_ld_bytes_clone;
                Ok(())
            })?;
        }
        // PDs that don't own any chunklet of this LD still need their
        // ld_list_bytes refreshed to drop the descriptor.
        for (pd_id, pd) in &pds_snapshot {
            if by_pd.contains_key(pd_id) {
                continue;
            }
            let new_ld_bytes_clone = new_ld_bytes.clone();
            pd.commit_manifest(move |body, _bitmap| {
                body.ld_list_bytes = new_ld_bytes_clone;
                Ok(())
            })?;
        }
        Ok(())
    }

    pub(crate) fn snapshot_free_views(&self) -> Vec<PdFreeView> {
        let s = self.state.read();
        let mut out = Vec::with_capacity(s.pds.len());
        for (pd_id, pd) in &s.pds {
            let (_, bitmap, _) = pd.snapshot();
            let mut free_indices = Vec::new();
            for i in 0..bitmap.len() {
                if bitmap
                    .get(i)
                    .map(|st| st == ChunkletState::Free)
                    .unwrap_or(false)
                {
                    free_indices.push(i);
                }
            }
            out.push(PdFreeView {
                pd: *pd_id,
                free_indices,
            });
        }
        out
    }

    /// Persist a freshly-allocated LD: write chunklet headers, mark bitmap
    /// entries Used, and update every PD's `ld_list_bytes` to include the new
    /// descriptor. Single mid-commit failure rolls back in-memory state and
    /// returns the error; on-disk repair is a P5+ tooling task.
    fn commit_new_ld(&self, desc: LdDescriptor) -> ChunkletResult<()> {
        let new_ld_bytes = {
            let mut s = self.state.write();
            s.ld_list.upsert(desc.clone());
            s.ld_list.encode()?
        };

        let mut new_chunklets_by_pd: BTreeMap<PdId, Vec<(u32, LdRole)>> = BTreeMap::new();
        for m in &desc.members {
            new_chunklets_by_pd
                .entry(m.pd)
                .or_default()
                .push((m.chunklet_index, m.role));
        }

        let pds_snapshot = self.state.read().pds.clone();
        let commit_result =
            self.do_per_pd_commits(&desc, &new_chunklets_by_pd, &pds_snapshot, &new_ld_bytes);

        if let Err(e) = commit_result {
            let mut s = self.state.write();
            s.ld_list.remove(desc.id);
            tracing::error!(
                "create_ld failed mid-commit; in-memory rolled back, on-disk may be inconsistent: {}",
                e
            );
            return Err(e);
        }
        Ok(())
    }

    fn do_per_pd_commits(
        &self,
        desc: &LdDescriptor,
        chunklets_by_pd: &BTreeMap<PdId, Vec<(u32, LdRole)>>,
        pds_snapshot: &BTreeMap<PdId, Arc<PhysicalDisk>>,
        new_ld_bytes: &[u8],
    ) -> ChunkletResult<()> {
        for (pd_id, pd) in pds_snapshot {
            let owned = chunklets_by_pd.get(pd_id);

            if let Some(members) = owned {
                for &(chunklet_idx, role) in members {
                    let header = ChunkletHeader {
                        owner_ld: desc.id,
                        chunklet_index: chunklet_idx,
                        role,
                        generation: 1,
                    };
                    pd.write_chunklet_header(chunklet_idx, &header.encode())?;
                }
                pd.sync()?;
            }

            let owned = owned.cloned().unwrap_or_default();
            let new_ld_bytes_v = new_ld_bytes.to_vec();
            pd.commit_manifest(move |body, bitmap| {
                for (chunklet_idx, _role) in &owned {
                    bitmap.set(*chunklet_idx, ChunkletState::Used)?;
                }
                body.ld_list_bytes = new_ld_bytes_v;
                Ok(())
            })?;
        }
        Ok(())
    }
}
