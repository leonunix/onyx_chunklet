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
//!   may add `is_drained` etc; today a PD's seq never changes after admit.
//! - Every PD's `pd_list` describes the full set of PDs in the pool. They
//!   should be identical at rest. Pool::open accepts a quorum mismatch and
//!   logs a warning; explicit repair is a Phase 7 task.

mod cpg;
mod drain;
mod ld_ops;
mod rebuild;
mod scrub;

pub use cpg::{CpgDescriptor, CpgList, CpgSpec};
pub use drain::DrainReport;
pub use ld_ops::LdSpec;
pub use rebuild::RebuildReport;
pub use scrub::{ScrubMismatch, ScrubMismatchKind, ScrubReport};

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::Arc;

use parking_lot::{Mutex, RwLock};

use crate::error::{ChunkletError, ChunkletResult};
use crate::io::RawDevice;
use crate::ld::descriptor::LdList;
use crate::pd::{PdInfo, PhysicalDisk};
use crate::superblock::PoolPdEntry;
use crate::types::{PdId, PoolId};

#[derive(Clone, Debug)]
pub struct PoolConfig {
    pub spare_pct: u8,
    /// Cross-PD batched-write backend stamped onto every PD this pool
    /// owns. Defaults to `Sync` (`std::thread::scope` fan-out). Set to
    /// `Uring` to drive writes through a thread-local `io_uring`
    /// instance on Linux; non-Linux silently falls back to `Sync`.
    pub io_backend: crate::io::IoBackendKind,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            spare_pct: 5,
            io_backend: crate::io::IoBackendKind::Sync,
        }
    }
}

pub struct Pool {
    pool_id: PoolId,
    pub(crate) state: RwLock<PoolState>,
    pub(crate) manifest_lock: Mutex<()>,
}

pub(crate) struct PoolState {
    /// Live PDs (any PD that was successfully opened). Failed PDs (declared
    /// in pd_list but not present in `Pool::open_with_missing`'s devices)
    /// have no entry here.
    pub pds: BTreeMap<PdId, Arc<PhysicalDisk>>,
    pub pd_seq_to_id: BTreeMap<u32, PdId>,
    /// Health status keyed by PdId. Every PD declared in pool's pd_list has
    /// an entry. Set to `Healthy` if the PD is in `pds`, `Failed` otherwise.
    pub pd_health: BTreeMap<PdId, PdHealth>,
    /// PDs currently being drained (P7). Treated like Failed PDs by
    /// `Pool::rebuild_ld` / `Pool::drain_pd` so members get migrated off.
    pub draining: std::collections::BTreeSet<PdId>,
    /// Authoritative LD list, mirrored on every PD's manifest.
    pub ld_list: LdList,
    /// Authoritative CPG list, mirrored on every PD's manifest.
    pub cpg_list: cpg::CpgList,
    /// Number of bitmap entries fixed by forward reconciliation during the
    /// open that produced this Pool. 0 on a clean open.
    pub last_reconciliation_count: usize,
}

/// Per-PD health enum. Phase 5 only distinguishes Healthy / Failed (PD-level).
/// Phase 6 will add per-chunklet Bad tracking via bitmap state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PdHealth {
    Healthy,
    Failed,
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

        let backend = crate::io::make_backend(cfg.io_backend);
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
                vec![], // fresh pool: empty CPG list
            )?;
            pd.set_backend(backend.clone());
            pds.insert(pd_id, pd);
            pd_seq_to_id.insert(i as u32, pd_id);
        }

        Ok(Arc::new(Self {
            pool_id,
            state: RwLock::new(PoolState {
                pd_health: pds.keys().map(|id| (*id, PdHealth::Healthy)).collect(),
                pds,
                pd_seq_to_id,
                draining: std::collections::BTreeSet::new(),
                ld_list: LdList::default(),
                cpg_list: cpg::CpgList::default(),
                last_reconciliation_count: 0,
            }),
            manifest_lock: Mutex::new(()),
        }))
    }

    /// Open an existing pool from a list of devices.
    ///
    /// Cross-checks: all PDs share the same `pool_id`; `pd_seq` values are
    /// dense & unique. Quorum repair (e.g., one PD lagging behind) is a
    /// Phase 7 task — for now we enforce strict consistency and reject the
    /// open if it fails.
    pub fn open(devices: Vec<RawDevice>) -> ChunkletResult<Arc<Self>> {
        if devices.is_empty() {
            return Err(ChunkletError::Config("open: no devices".into()));
        }

        let mut opened: Vec<Arc<PhysicalDisk>> = Vec::with_capacity(devices.len());
        for raw in devices {
            opened.push(PhysicalDisk::open(raw)?);
        }

        let pool_id = majority_pool_id(&opened)?;
        let mut pds = BTreeMap::new();
        let mut seqs = BTreeSet::new();
        let mut pd_seq_to_id = BTreeMap::new();
        let mut declared_count: Option<u32> = None;
        // Pick the LD/CPG view from the PD with the highest manifest_gen.
        let mut best_view: Option<(u64, Vec<u8>, Vec<u8>)> = None;

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
            match &best_view {
                Some((best_gen, _, _)) if *best_gen >= gen => {}
                _ => {
                    best_view = Some((
                        gen,
                        body.ld_list_bytes.clone(),
                        body.cpg_list_bytes.clone(),
                    ))
                }
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
        for i in 0..actual_count {
            if !pd_seq_to_id.contains_key(&i) {
                return Err(ChunkletError::PoolMismatch(format!(
                    "missing pd_seq {} (pool has {} PDs)",
                    i, actual_count
                )));
            }
        }

        let (ld_list, cpg_list) = match best_view {
            Some((_, ld_bytes, cpg_bytes)) => {
                (LdList::decode(&ld_bytes)?, cpg::CpgList::decode(&cpg_bytes)?)
            }
            None => (LdList::default(), cpg::CpgList::default()),
        };

        let reconciled = forward_reconcile_bitmaps(&ld_list, &pds)?;

        Ok(Arc::new(Self {
            pool_id,
            state: RwLock::new(PoolState {
                pd_health: pds.keys().map(|id| (*id, PdHealth::Healthy)).collect(),
                pds,
                pd_seq_to_id,
                draining: std::collections::BTreeSet::new(),
                ld_list,
                cpg_list,
                last_reconciliation_count: reconciled,
            }),
            manifest_lock: Mutex::new(()),
        }))
    }

    /// Open an existing pool but tolerate missing PDs.
    ///
    /// Useful for degraded operation: pass in only the live device paths;
    /// the pool will reconstruct the view via the manifests on those PDs
    /// and mark the absent PDs as `Failed`. Reads to LD members on Failed
    /// PDs are routed through the LD's reconstruct path (Mirror copies,
    /// Raid5/6 parity); writes to those positions error until
    /// `Pool::rebuild_ld` swaps the member onto a spare.
    ///
    /// At least one device must be provided. The pool_id is taken from the
    /// majority of opened PDs; if the opened set is too small to form a
    /// majority of the declared pool_pd_count, this returns
    /// `PoolMismatch` (no quorum).
    pub fn open_with_missing(devices: Vec<RawDevice>) -> ChunkletResult<Arc<Self>> {
        if devices.is_empty() {
            return Err(ChunkletError::Config("open_with_missing: no devices".into()));
        }

        let mut opened: Vec<Arc<PhysicalDisk>> = Vec::with_capacity(devices.len());
        for raw in devices {
            opened.push(PhysicalDisk::open(raw)?);
        }

        let pool_id = majority_pool_id(&opened)?;
        let mut pds = BTreeMap::new();
        let mut declared_count: Option<u32> = None;
        let mut declared_pd_list: Option<Vec<PoolPdEntry>> = None;
        let mut best_view: Option<(u64, Vec<u8>, Vec<u8>)> = None;
        let mut best_view_gen: u64 = 0;

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
            match declared_count {
                None => {
                    declared_count = Some(body.pool_pd_count);
                    declared_pd_list = Some(body.pd_list.clone());
                }
                Some(c) if c != body.pool_pd_count => {
                    return Err(ChunkletError::PoolMismatch(format!(
                        "PD {} reports pool_pd_count={}, expected {}",
                        info.pd_id, body.pool_pd_count, c
                    )));
                }
                _ => {}
            }
            if best_view.is_none() || gen > best_view_gen {
                best_view = Some((
                    gen,
                    body.ld_list_bytes.clone(),
                    body.cpg_list_bytes.clone(),
                ));
                best_view_gen = gen;
            }
            pds.insert(info.pd_id, pd);
        }

        let declared_count = declared_count.expect("at least one PD opened");
        let declared_pd_list = declared_pd_list.expect("at least one PD opened");

        // Quorum for pool_id: opened set must be at least majority of declared.
        let need_quorum = (declared_count as usize) / 2 + 1;
        if pds.len() < need_quorum {
            return Err(ChunkletError::PoolMismatch(format!(
                "open_with_missing: opened {} of {} PDs, need {} for quorum",
                pds.len(),
                declared_count,
                need_quorum
            )));
        }

        let mut pd_seq_to_id = BTreeMap::new();
        let mut pd_health = BTreeMap::new();
        // Walk the declared pd_list. Live PDs go to Healthy + populate pds map;
        // missing entries are Failed.
        for entry in &declared_pd_list {
            pd_seq_to_id.insert(entry.pd_seq, entry.pd_id);
            if pds.contains_key(&entry.pd_id) {
                pd_health.insert(entry.pd_id, PdHealth::Healthy);
            } else {
                pd_health.insert(entry.pd_id, PdHealth::Failed);
            }
        }

        let (ld_list, cpg_list) = match best_view {
            Some((_, ld_bytes, cpg_bytes)) => {
                (LdList::decode(&ld_bytes)?, cpg::CpgList::decode(&cpg_bytes)?)
            }
            None => (LdList::default(), cpg::CpgList::default()),
        };

        let reconciled = forward_reconcile_bitmaps(&ld_list, &pds)?;

        Ok(Arc::new(Self {
            pool_id,
            state: RwLock::new(PoolState {
                pds,
                pd_seq_to_id,
                pd_health,
                draining: std::collections::BTreeSet::new(),
                ld_list,
                cpg_list,
                last_reconciliation_count: reconciled,
            }),
            manifest_lock: Mutex::new(()),
        }))
    }

    /// Public read accessor for PD health. Phase 5 only ever returns
    /// `Healthy` or `Failed` — the latter only set by `open_with_missing`.
    pub fn pd_health(&self, id: PdId) -> Option<PdHealth> {
        self.state.read().pd_health.get(&id).copied()
    }

    /// Returns IDs of all PDs marked as Failed. Empty for a fully healthy pool.
    pub fn failed_pds(&self) -> Vec<PdId> {
        self.state
            .read()
            .pd_health
            .iter()
            .filter_map(|(id, h)| (*h == PdHealth::Failed).then_some(*id))
            .collect()
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

        let (current_ld_bytes, current_cpg_bytes) = {
            let s = self.state.read();
            (s.ld_list.encode()?, s.cpg_list.encode()?)
        };

        let new_pd = PhysicalDisk::init(
            raw,
            self.pool_id,
            new_pd_id,
            new_pd_seq,
            new_pd_count,
            new_pd_list.clone(),
            cfg.spare_pct,
            current_ld_bytes,
            current_cpg_bytes,
        )?;
        // Inherit the backend from any existing PD so the new member
        // matches the rest of the pool's IO discipline.
        if let Some(existing_pd) = self.state.read().pds.values().next() {
            new_pd.set_backend(existing_pd.backend());
        }

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

        let mut s = self.state.write();
        s.pds.insert(new_pd_id, new_pd);
        s.pd_seq_to_id.insert(new_pd_seq, new_pd_id);
        s.pd_health.insert(new_pd_id, PdHealth::Healthy);
        Ok(new_pd_id)
    }

    pub fn id(&self) -> PoolId {
        self.pool_id
    }

    /// Swap the IO backend used by every PD in the pool. Useful for tests
    /// + for `chunkletctl`-style runtime backend selection (e.g. open a
    /// pool the default Sync way, then upgrade to Uring before doing IO).
    pub fn set_io_backend(&self, kind: crate::io::IoBackendKind) {
        let backend = crate::io::make_backend(kind);
        let s = self.state.read();
        for pd in s.pds.values() {
            pd.set_backend(backend.clone());
        }
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

    /// Number of bitmap entries forward-reconciliation fixed during the last
    /// `Pool::open` / `Pool::open_with_missing` call. 0 on a clean open;
    /// non-zero indicates a prior cross-PD `commit_manifest` loop didn't
    /// fully land. Mainly for tests + diagnostics.
    pub fn last_reconciliation_count(&self) -> usize {
        self.state.read().last_reconciliation_count
    }

    /// Persistently mark a chunklet `Bad` on its owning PD. Future LD opens
    /// will see this via `resolve_members` and route reads/writes around the
    /// chunklet (mirror falls back to surviving copies; R5/R6 reconstructs).
    /// Idempotent — already-Bad stays Bad.
    ///
    /// Used by scrub when it identifies a divergent / corrupt copy, and by
    /// admin tooling when a chunklet is known-bad outside the scrub path.
    /// Out-of-range `chunklet_index` returns `Invariant`.
    pub fn mark_chunklet_bad(&self, pd_id: PdId, chunklet_index: u32) -> ChunkletResult<()> {
        let _commit = self.manifest_lock.lock();
        let pd = self
            .state
            .read()
            .pds
            .get(&pd_id)
            .cloned()
            .ok_or_else(|| ChunkletError::Invariant(format!("PD {} not in pool", pd_id)))?;
        pd.commit_manifest(move |_body, bitmap| {
            bitmap.set(chunklet_index, crate::types::ChunkletState::Bad)
        })?;
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

/// Forward bitmap reconciliation: walk every loaded LD descriptor and ensure
/// the chunklet referenced by each member is marked `Used` on its owning PD.
///
/// Cross-PD `commit_manifest` loops in `create_ld` / `commit_rebuild` /
/// `drop_ld` / etc. are not atomic. If best-view at open time picks a higher-
/// gen PD that records a new descriptor while a sibling PD lagged behind on
/// its bitmap update, the result is "descriptor says chunklet X is mine"
/// but bitmap says Free. Untouched, the allocator could later hand X out
/// to a different LD → double-allocation.
///
/// Only `Free` → `Used` is auto-corrected. `Bad` / `Spare` / `Migrating`
/// states are deliberate signals (scrub flagged corruption, spare pool
/// reservation, in-flight migration) and we WARN-log without overwriting.
/// FORWARD-only: bitmap entries claimed by no descriptor stay where they
/// are; cleaning those (the `drop_ld` half-commit case) is left to a
/// future `Pool::fsck`.
///
/// Returns the number of `(pd, chunklet_index)` pairs flipped to Used.
fn forward_reconcile_bitmaps(
    ld_list: &LdList,
    pds: &BTreeMap<PdId, Arc<PhysicalDisk>>,
) -> ChunkletResult<usize> {
    use crate::types::ChunkletState;
    let mut fixes_by_pd: BTreeMap<PdId, Vec<u32>> = BTreeMap::new();
    for ld in &ld_list.lds {
        for member in &ld.members {
            let pd = match pds.get(&member.pd) {
                Some(pd) => pd,
                None => continue, // member's PD missing — Phase-7 quorum repair territory
            };
            let (_, bitmap, _) = pd.snapshot();
            let state = match bitmap.get(member.chunklet_index) {
                Ok(s) => s,
                Err(_) => continue, // out-of-range index; superblock corrupt? skip silently
            };
            match state {
                ChunkletState::Used => {} // already correct
                ChunkletState::Free => {
                    fixes_by_pd
                        .entry(member.pd)
                        .or_default()
                        .push(member.chunklet_index);
                }
                other => {
                    // Bad / Spare / Migrating — descriptor still references
                    // this chunklet but operator/scrub flagged it. Don't
                    // clobber, just warn so the operator notices.
                    tracing::warn!(
                        "forward reconcile: ld {} references PD {} chunklet {} but bitmap says {:?}; leaving as-is",
                        ld.id,
                        member.pd,
                        member.chunklet_index,
                        other
                    );
                }
            }
        }
    }

    let mut total = 0usize;
    for (pd_id, indices) in fixes_by_pd {
        let pd = pds.get(&pd_id).expect("pd_id keyed from pds map");
        let n = indices.len();
        let captured = indices.clone();
        pd.commit_manifest(move |_body, bitmap| {
            for idx in &captured {
                bitmap.set(*idx, ChunkletState::Used)?;
            }
            Ok(())
        })?;
        tracing::warn!(
            "forward reconcile fixed {} Free->Used bitmap entries on PD {}",
            n,
            pd_id
        );
        total += n;
    }
    Ok(total)
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

        for info in pool.list_pds() {
            let pd = pool.pd(info.pd_id).unwrap();
            let (body, _, gen) = pd.snapshot();
            assert_eq!(body.pool_pd_count, 3);
            if info.pd_seq_in_pool < 2 {
                assert_eq!(gen, 2);
            } else {
                assert_eq!(gen, 1);
            }
        }

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

        let paths = collect_paths(&dir, &["pd0", "pd2"]);
        let err = Pool::open(open_paths(&paths).unwrap())
            .err()
            .expect("expected open to fail");
        assert!(matches!(err, ChunkletError::PoolMismatch(_)));
    }
}
