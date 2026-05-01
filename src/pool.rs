//! Pool: multi-PD container with cross-PD manifest consistency.
//!
//! # Concurrency
//!
//! - `manifest_lock: Mutex<()>` — held for the entire duration of any
//!   cross-PD commit (create, admit, future LD create / drop). Single writer
//!   serializes commits across PDs; per-PD `commit_manifest` then runs under
//!   it sequentially. Acquire **before** taking any PD locks.
//! - `state: RwLock<PoolState>` — read for normal queries (list_pds, find_pd),
//!   write only when adding / removing PDs from the in-memory map.
//!
//! # Invariants
//!
//! - All PDs in the pool share the same `pool_id`.
//! - `pd_seq_in_pool` is a stable, dense ordinal in `[0, pd_count)`. Phase 7
//!   may add `is_drained` etc; for P0 a PD's seq never changes after admit.
//! - Every PD's `pd_list` describes the full set of PDs in the pool. They
//!   should be identical at rest. Pool::open accepts a quorum mismatch and
//!   logs a warning; explicit repair is a Phase 7 task.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::Arc;

use parking_lot::{Mutex, RwLock};

use crate::allocator::{plan_alloc, AllocRequest, PdFreeView};
use crate::chunklet::ChunkletHeader;
use crate::error::{ChunkletError, ChunkletResult};
use crate::io::RawDevice;
use crate::ld::descriptor::{LdDescriptor, LdList};
use crate::ld::{LdPlain, LogicalDisk};
use crate::pd::{PdInfo, PhysicalDisk};
use crate::superblock::PoolPdEntry;
use crate::types::{
    ChunkletState, HaDomain, LdId, LdRole, PdId, PoolId, RaidLevel,
};

#[derive(Clone, Debug)]
pub struct PoolConfig {
    pub spare_pct: u8,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self { spare_pct: 5 }
    }
}

pub struct Pool {
    pool_id: PoolId,
    state: RwLock<PoolState>,
    manifest_lock: Mutex<()>,
}

struct PoolState {
    pds: BTreeMap<PdId, Arc<PhysicalDisk>>,
    pd_seq_to_id: BTreeMap<u32, PdId>,
    /// Authoritative LD list, mirrored on every PD's manifest. P1 picks the
    /// view from the PD with the highest manifest_gen on Pool::open and writes
    /// the same list to every PD on every LD-list mutation (create / drop).
    ld_list: LdList,
}

/// Caller-supplied LD-creation spec. Phase 1 only supports `RaidLevel::Plain`.
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
    /// Convenience: linear concat of `chunklet_count` chunklets.
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
}

impl Pool {
    /// Create a fresh pool from a list of blank devices. All devices are
    /// initialized with a freshly-generated `pool_id` and dense `pd_seq`s.
    pub fn create(devices: Vec<RawDevice>, cfg: PoolConfig) -> ChunkletResult<Arc<Self>> {
        if devices.is_empty() {
            return Err(ChunkletError::Config("create: no devices".into()));
        }

        let pool_id = PoolId::new_v4();
        let pd_count = devices.len() as u32;

        // Generate deterministic pd_ids and the canonical pd_list up front.
        let pd_ids: Vec<PdId> = (0..pd_count).map(|_| PdId::new_v4()).collect();
        let pd_list: Vec<PoolPdEntry> = pd_ids
            .iter()
            .enumerate()
            .map(|(i, &id)| PoolPdEntry {
                pd_id: id,
                pd_seq: i as u32,
                flags: 0,
            })
            .collect();

        let mut pds = BTreeMap::new();
        let mut pd_seq_to_id = BTreeMap::new();
        for (i, raw) in devices.into_iter().enumerate() {
            let pd_id = pd_ids[i];
            let pd = PhysicalDisk::init(
                raw,
                pool_id,
                pd_id,
                i as u32,
                pd_count,
                pd_list.clone(),
                cfg.spare_pct,
                vec![], // fresh pool: empty LD list
            )?;
            pds.insert(pd_id, pd);
            pd_seq_to_id.insert(i as u32, pd_id);
        }

        Ok(Arc::new(Self {
            pool_id,
            state: RwLock::new(PoolState {
                pds,
                pd_seq_to_id,
                ld_list: LdList::default(),
            }),
            manifest_lock: Mutex::new(()),
        }))
    }

    /// Open an existing pool from a list of devices.
    ///
    /// Cross-checks: all PDs share the same `pool_id`; `pd_seq` values are
    /// dense & unique. Quorum repair (e.g., one PD lagging behind) is a
    /// Phase 7 task — for P0 we just enforce strict consistency and reject
    /// the open if it fails.
    pub fn open(devices: Vec<RawDevice>) -> ChunkletResult<Arc<Self>> {
        if devices.is_empty() {
            return Err(ChunkletError::Config("open: no devices".into()));
        }

        let mut opened: Vec<Arc<PhysicalDisk>> = Vec::with_capacity(devices.len());
        for raw in devices {
            opened.push(PhysicalDisk::open(raw)?);
        }

        // Determine the pool_id by majority.
        let pool_id = majority_pool_id(&opened)?;
        let mut pds = BTreeMap::new();
        let mut seqs = BTreeSet::new();
        let mut pd_seq_to_id = BTreeMap::new();
        let mut declared_count: Option<u32> = None;
        // Pick the LD list from the PD with the highest manifest_gen — this
        // is the "most recent commit" view. If multiple PDs tie at the highest
        // gen with different LD lists, we log a warning and pick the first.
        let mut best_ld_view: Option<(u64, Vec<u8>)> = None;

        for pd in opened {
            if pd.pool_id() != pool_id {
                return Err(ChunkletError::PoolMismatch(format!(
                    "PD {} declares pool {}, expected {}",
                    pd.pd_id(),
                    pd.pool_id(),
                    pool_id
                )));
            }
            let info = pd.info();
            let (body, _, gen) = pd.snapshot();
            // pool_pd_count consistency check: all PDs must agree.
            match declared_count {
                None => declared_count = Some(body.pool_pd_count),
                Some(c) if c != body.pool_pd_count => {
                    return Err(ChunkletError::PoolMismatch(format!(
                        "PD {} reports pool_pd_count={}, expected {}",
                        info.pd_id, body.pool_pd_count, c
                    )));
                }
                _ => {}
            }
            if !seqs.insert(info.pd_seq_in_pool) {
                return Err(ChunkletError::PoolMismatch(format!(
                    "duplicate pd_seq {} on PD {}",
                    info.pd_seq_in_pool, info.pd_id
                )));
            }
            // Track the highest-gen LD list view.
            match &best_ld_view {
                Some((best_gen, _)) if *best_gen >= gen => {}
                _ => best_ld_view = Some((gen, body.ld_list_bytes.clone())),
            }
            pd_seq_to_id.insert(info.pd_seq_in_pool, info.pd_id);
            pds.insert(info.pd_id, pd);
        }

        let actual_count = pds.len() as u32;
        if let Some(declared) = declared_count {
            if declared != actual_count {
                return Err(ChunkletError::PoolMismatch(format!(
                    "pool_pd_count={} but {} PDs opened",
                    declared, actual_count
                )));
            }
        }
        // Dense seq check: 0..count must all be present.
        for i in 0..actual_count {
            if !pd_seq_to_id.contains_key(&i) {
                return Err(ChunkletError::PoolMismatch(format!(
                    "missing pd_seq {} (pool has {} PDs)",
                    i, actual_count
                )));
            }
        }

        let ld_list = match best_ld_view {
            Some((_, bytes)) => LdList::decode(&bytes)?,
            None => LdList::default(),
        };

        Ok(Arc::new(Self {
            pool_id,
            state: RwLock::new(PoolState {
                pds,
                pd_seq_to_id,
                ld_list,
            }),
            manifest_lock: Mutex::new(()),
        }))
    }

    /// Add a new blank PD to the pool. Initializes the PD with the current
    /// pool view, then bumps every existing PD's manifest to include the new
    /// entry.
    pub fn admit(&self, raw: RawDevice, cfg: PoolConfig) -> ChunkletResult<PdId> {
        let _commit = self.manifest_lock.lock();

        let new_pd_id = PdId::new_v4();
        let new_pd_seq;
        let new_pd_list: Vec<PoolPdEntry>;
        {
            let s = self.state.read();
            new_pd_seq = s.pds.len() as u32;
            new_pd_list = (0..new_pd_seq)
                .map(|seq| {
                    let id = s.pd_seq_to_id[&seq];
                    PoolPdEntry { pd_id: id, pd_seq: seq, flags: 0 }
                })
                .chain(std::iter::once(PoolPdEntry {
                    pd_id: new_pd_id,
                    pd_seq: new_pd_seq,
                    flags: 0,
                }))
                .collect();
        }

        let new_pd_count = new_pd_list.len() as u32;

        // Snapshot the current LD list so the new PD inherits it.
        let current_ld_bytes = {
            let s = self.state.read();
            s.ld_list.encode()?
        };

        // Init the new PD with the new full pd_list.
        let new_pd = PhysicalDisk::init(
            raw,
            self.pool_id,
            new_pd_id,
            new_pd_seq,
            new_pd_count,
            new_pd_list.clone(),
            cfg.spare_pct,
            current_ld_bytes,
        )?;

        // Bump every existing PD's manifest to include the new entry. We
        // collect existing PDs in pd_seq order so iteration order is
        // deterministic.
        let existing: Vec<Arc<PhysicalDisk>> = {
            let s = self.state.read();
            s.pd_seq_to_id
                .values()
                .map(|id| s.pds[id].clone())
                .collect()
        };
        for pd in &existing {
            pd.commit_manifest(|body, _bitmap| {
                body.pd_list = new_pd_list.clone();
                body.pool_pd_count = new_pd_count;
                Ok(())
            })?;
        }

        // Publish in-memory.
        let mut s = self.state.write();
        s.pds.insert(new_pd_id, new_pd);
        s.pd_seq_to_id.insert(new_pd_seq, new_pd_id);
        Ok(new_pd_id)
    }

    pub fn id(&self) -> PoolId {
        self.pool_id
    }

    pub fn pd_count(&self) -> usize {
        self.state.read().pds.len()
    }

    pub fn list_pds(&self) -> Vec<PdInfo> {
        let s = self.state.read();
        s.pd_seq_to_id
            .values()
            .map(|id| s.pds[id].info())
            .collect()
    }

    pub fn pd(&self, id: PdId) -> Option<Arc<PhysicalDisk>> {
        self.state.read().pds.get(&id).cloned()
    }

    pub fn pd_by_seq(&self, seq: u32) -> Option<Arc<PhysicalDisk>> {
        let s = self.state.read();
        s.pd_seq_to_id
            .get(&seq)
            .and_then(|id| s.pds.get(id))
            .cloned()
    }

    // ---- LD APIs (P1) -----------------------------------------------------

    pub fn list_lds(&self) -> Vec<LdDescriptor> {
        self.state.read().ld_list.lds.clone()
    }

    pub fn find_ld(&self, id: LdId) -> Option<LdDescriptor> {
        self.state.read().ld_list.find(id).cloned()
    }

    /// Plan + create a new LD. Phase 1 only supports `RaidLevel::Plain`.
    /// On success the new LD is durably persisted on every PD's manifest.
    pub fn create_ld(&self, spec: LdSpec) -> ChunkletResult<LdId> {
        if spec.raid_level != RaidLevel::Plain {
            return Err(ChunkletError::Unsupported(format!(
                "P1 only supports RaidLevel::Plain, got {:?}",
                spec.raid_level
            )));
        }
        if spec.set_size != 1 {
            return Err(ChunkletError::Invariant(format!(
                "Plain LD must have set_size=1, got {}",
                spec.set_size
            )));
        }
        let _commit = self.manifest_lock.lock();

        // Snapshot per-PD free chunklet indices.
        let pd_views = self.snapshot_free_views();
        let total_members = (spec.set_size as usize)
            * (spec.row_size as usize)
            * (spec.num_rows as usize);
        let role_assignments = vec![LdRole::Data; total_members];
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

        // Group members by PD; for each PD, free the listed chunklets and
        // commit a new LD list (which already excludes the removed LD).
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

    fn snapshot_free_views(&self) -> Vec<PdFreeView> {
        let s = self.state.read();
        let mut out = Vec::with_capacity(s.pds.len());
        for (pd_id, pd) in &s.pds {
            let (_, bitmap, _) = pd.snapshot();
            let mut free_indices = Vec::new();
            for i in 0..bitmap.len() {
                if bitmap.get(i).map(|st| st == ChunkletState::Free).unwrap_or(false) {
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
    /// entries Used, and update every PD's `ld_list_bytes` to include the
    /// new descriptor.
    fn commit_new_ld(&self, desc: LdDescriptor) -> ChunkletResult<()> {
        // 1. Insert into in-memory ld_list, encode, then commit per PD.
        let new_ld_bytes = {
            let mut s = self.state.write();
            s.ld_list.upsert(desc.clone());
            s.ld_list.encode()?
        };

        // 2. Group new-LD members by PD.
        let mut new_chunklets_by_pd: BTreeMap<PdId, Vec<(u32, LdRole)>> = BTreeMap::new();
        for m in &desc.members {
            new_chunklets_by_pd
                .entry(m.pd)
                .or_default()
                .push((m.chunklet_index, m.role));
        }

        // 3. Write chunklet headers + sync, then commit_manifest. We do this
        //    PD by PD; if any commit fails we attempt to roll back the
        //    in-memory list and bubble the error up.
        let pds_snapshot = self.state.read().pds.clone();
        let commit_result =
            self.do_per_pd_commits(&desc, &new_chunklets_by_pd, &pds_snapshot, &new_ld_bytes);

        if let Err(e) = commit_result {
            // Best-effort rollback of in-memory state. On-disk state may be
            // partially committed; chunkletctl repair (Phase 5+) will be the
            // tool to clean up.
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
        // Iterate in pd_seq order (BTreeMap pd_id -> Arc<PD> doesn't give us
        // seq, but Pool::admit / create assign seqs deterministically; for P1
        // commit order doesn't matter for correctness).
        for (pd_id, pd) in pds_snapshot {
            let owned = chunklets_by_pd.get(pd_id);

            // 1. Write chunklet headers if this PD owns any of the new LD's
            //    members. Headers are advisory but we want them present.
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

            // 2. Commit the manifest update (bitmap + ld_list_bytes).
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

/// Majority vote for pool_id across opened PDs. With our quorum policy
/// (no minority partitions allowed), unanimous agreement is required.
fn majority_pool_id(pds: &[Arc<PhysicalDisk>]) -> ChunkletResult<PoolId> {
    let mut counts: BTreeMap<PoolId, usize> = BTreeMap::new();
    for pd in pds {
        *counts.entry(pd.pool_id()).or_insert(0) += 1;
    }
    let need = pds.len() / 2 + 1;
    counts
        .into_iter()
        .find(|(_, count)| *count >= need)
        .map(|(id, _)| id)
        .ok_or_else(|| ChunkletError::PoolMismatch("no pool_id majority".into()))
}

/// Convenience: open a list of paths as raw devices. Used by `chunkletctl`.
pub fn open_paths(paths: &[impl AsRef<Path>]) -> ChunkletResult<Vec<RawDevice>> {
    let mut out = Vec::with_capacity(paths.len());
    for p in paths {
        out.push(RawDevice::open(p.as_ref())?);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    const PD_SIZE: u64 = 4 * 1024 * 1024 * 1024;

    fn sparse(dir: &TempDir, name: &str) -> RawDevice {
        let path = dir.path().join(name);
        RawDevice::open_or_create(&path, PD_SIZE).unwrap()
    }

    fn collect_paths(dir: &TempDir, names: &[&str]) -> Vec<std::path::PathBuf> {
        names.iter().map(|n| dir.path().join(n)).collect()
    }

    #[test]
    fn create_then_open_round_trip() {
        let dir = TempDir::new().unwrap();
        let pool = Pool::create(
            vec![sparse(&dir, "pd0"), sparse(&dir, "pd1"), sparse(&dir, "pd2")],
            PoolConfig::default(),
        )
        .unwrap();
        let pool_id = pool.id();
        assert_eq!(pool.pd_count(), 3);
        drop(pool);

        let paths = collect_paths(&dir, &["pd0", "pd1", "pd2"]);
        let pool2 = Pool::open(open_paths(&paths).unwrap()).unwrap();
        assert_eq!(pool2.id(), pool_id);
        assert_eq!(pool2.pd_count(), 3);
        let infos = pool2.list_pds();
        assert_eq!(infos.len(), 3);
        for (i, info) in infos.iter().enumerate() {
            assert_eq!(info.pd_seq_in_pool, i as u32);
        }
    }

    #[test]
    fn admit_extends_pool() {
        let dir = TempDir::new().unwrap();
        let pool = Pool::create(
            vec![sparse(&dir, "pd0"), sparse(&dir, "pd1")],
            PoolConfig::default(),
        )
        .unwrap();
        assert_eq!(pool.pd_count(), 2);

        let new_id = pool
            .admit(sparse(&dir, "pd2"), PoolConfig::default())
            .unwrap();
        assert_eq!(pool.pd_count(), 3);
        assert!(pool.pd(new_id).is_some());

        // Existing PDs should now report pool_pd_count=3 in their manifests.
        for info in pool.list_pds() {
            let pd = pool.pd(info.pd_id).unwrap();
            let (body, _, gen) = pd.snapshot();
            assert_eq!(body.pool_pd_count, 3);
            // PDs 0 and 1 had their manifests committed to gen 2; new PD is gen 1.
            if info.pd_seq_in_pool < 2 {
                assert_eq!(gen, 2);
            } else {
                assert_eq!(gen, 1);
            }
        }

        // Reopen the full set after admit.
        drop(pool);
        let paths = collect_paths(&dir, &["pd0", "pd1", "pd2"]);
        let pool2 = Pool::open(open_paths(&paths).unwrap()).unwrap();
        assert_eq!(pool2.pd_count(), 3);
    }

    #[test]
    fn rejects_mixed_pool_ids() {
        let dir = TempDir::new().unwrap();
        let pool_a = Pool::create(
            vec![sparse(&dir, "a0"), sparse(&dir, "a1")],
            PoolConfig::default(),
        )
        .unwrap();
        let pool_b = Pool::create(
            vec![sparse(&dir, "b0"), sparse(&dir, "b1")],
            PoolConfig::default(),
        )
        .unwrap();
        drop((pool_a, pool_b));

        let paths = collect_paths(&dir, &["a0", "b0"]); // 50/50 split, no majority
        let err = Pool::open(open_paths(&paths).unwrap())
            .err()
            .expect("expected open to fail");
        assert!(matches!(err, ChunkletError::PoolMismatch(_)));
    }

    #[test]
    fn rejects_missing_pd_seq() {
        let dir = TempDir::new().unwrap();
        let pool = Pool::create(
            vec![sparse(&dir, "pd0"), sparse(&dir, "pd1"), sparse(&dir, "pd2")],
            PoolConfig::default(),
        )
        .unwrap();
        drop(pool);

        // Open with only 2 of 3 PDs — should fail because pool_pd_count=3.
        let paths = collect_paths(&dir, &["pd0", "pd2"]);
        let err = Pool::open(open_paths(&paths).unwrap())
            .err()
            .expect("expected open to fail");
        assert!(matches!(err, ChunkletError::PoolMismatch(_)));
    }
}
