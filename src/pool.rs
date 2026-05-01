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

use crate::error::{ChunkletError, ChunkletResult};
use crate::io::RawDevice;
use crate::pd::{PdInfo, PhysicalDisk};
use crate::superblock::PoolPdEntry;
use crate::types::{PdId, PoolId};

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
            )?;
            pds.insert(pd_id, pd);
            pd_seq_to_id.insert(i as u32, pd_id);
        }

        Ok(Arc::new(Self {
            pool_id,
            state: RwLock::new(PoolState { pds, pd_seq_to_id }),
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
            let (body, _, _) = pd.snapshot();
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

        Ok(Arc::new(Self {
            pool_id,
            state: RwLock::new(PoolState { pds, pd_seq_to_id }),
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

        // Init the new PD with the new full pd_list.
        let new_pd = PhysicalDisk::init(
            raw,
            self.pool_id,
            new_pd_id,
            new_pd_seq,
            new_pd_count,
            new_pd_list.clone(),
            cfg.spare_pct,
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
