//! Reverse bitmap reconciliation — reclaim `Used`-but-unreferenced chunklets.
//!
//! # Responsibility
//!
//! The counterpart to [`super::forward_reconcile_bitmaps`] (which fixes
//! `Free`→`Used` for referenced members) and [`super::reclaim_orphan_migrating`]
//! (which reclaims orphan `Migrating` shadows). Neither touches a chunklet that
//! is marked `Used` on its PD but referenced by NO live descriptor member —
//! that case was historically deferred to a "future `Pool::fsck`" and leaked
//! capacity forever (a PD whose members were rebuilt onto spares elsewhere, or
//! a non-atomic cross-PD `drop_ld` half-commit). This module IS that fsck.
//!
//! # Authority requirement (load-bearing correctness invariant)
//!
//! We free a chunklet iff `(pd, idx)` is absent from the authoritative live
//! descriptor set. That set is only authoritative when the pool is COMPLETE
//! (every declared PD present). If a truly-newer descriptor lived on a PD that
//! is currently *missing*, the in-memory `ld_list` (highest gen among *present*
//! PDs — see `open_with_missing`) could be stale, and a `Used` chunklet on a
//! present PD might be referenced by that absent descriptor. Freeing it then =
//! data loss. Therefore:
//!   - the online [`Pool::fsck`] skips (reclaims nothing) if ANY PD is missing
//!     or draining;
//!   - the open-time hook runs ONLY from `Pool::open` (strict; declared ==
//!     actual ⇒ no missing PD), NEVER from `open_with_missing`.
//! Administratively-`Failed` but PRESENT PDs do NOT block reclaim — recovering
//! their stale (already-rebuilt-away) `Used` chunklets is the whole point.
//!
//! # Concurrency
//!
//! `Pool::fsck` takes `manifest_lock` for the whole pass. Reclaim is `Used`→
//! `Free` via `commit_manifest` (atomic bitmap+superblock COW), per-PD in
//! ascending `PdId`. No epoch bump: the chunklets were already unreferenced, so
//! no LD handle or descriptor changes. A crash between per-PD commits just
//! leaves the rest for the next fsck (idempotent).

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use crate::chunklet::ChunkletHeader;
use crate::error::ChunkletResult;
use crate::ld::descriptor::LdList;
use crate::pd::PhysicalDisk;
use crate::pool::Pool;
use crate::types::{ChunkletState, PdId};

#[derive(Clone, Debug)]
pub struct FsckReport {
    pub reclaimed_by_pd: BTreeMap<PdId, u32>,
    pub total_reclaimed: u32,
    pub scanned_pds: usize,
    /// True if the pool was incomplete (missing/draining PD) so nothing was
    /// reclaimed — the in-memory descriptor set could not be trusted.
    pub skipped_incomplete: bool,
}

/// All `(PdId, chunklet_index)` referenced by any live descriptor member. The
/// single authoritative "referenced set" reused by both the `Migrating`-orphan
/// reclaim (`reclaim_orphan_migrating`) and the `Used`-orphan reclaim here.
pub(crate) fn referenced_chunklets(ld_list: &LdList) -> BTreeSet<(PdId, u32)> {
    let mut referenced = BTreeSet::new();
    for ld in &ld_list.lds {
        for m in &ld.members {
            referenced.insert((m.pd, m.chunklet_index));
        }
    }
    referenced
}

/// Reclaim `Used`-but-unreferenced chunklets on the given PRESENT PDs against
/// the authoritative `ld_list`. Returns per-PD reclaim counts.
///
/// SAFETY: the caller MUST guarantee `ld_list` is authoritative for these PDs
/// (pool complete — see module docs). `Pool::open` satisfies this; the online
/// `Pool::fsck` enforces it via the missing/draining gate.
pub(crate) fn reverse_reconcile_bitmaps(
    ld_list: &LdList,
    pds: &BTreeMap<PdId, Arc<PhysicalDisk>>,
) -> ChunkletResult<BTreeMap<PdId, u32>> {
    let referenced = referenced_chunklets(ld_list);
    let mut reclaimed_by_pd = BTreeMap::new();
    for (pd_id, pd) in pds {
        let (_, bitmap, _) = pd.snapshot();
        let mut orphans: Vec<u32> = Vec::new();
        for idx in 0..bitmap.len() {
            if !matches!(bitmap.get(idx), Ok(ChunkletState::Used)) {
                continue;
            }
            if referenced.contains(&(*pd_id, idx)) {
                continue;
            }
            // Used + unreferenced = candidate orphan. Header cross-check is
            // defense-in-depth: veto ONLY if the on-disk header proves this
            // chunklet still belongs to a live LD that references THIS (pd,idx).
            // With an authoritative ld_list + a held snapshot that never fires,
            // but it hard-stops a free if a future ld_list bug ever produced a
            // referenced set that disagreed with the durable header.
            if header_vetoes_reclaim(pd, idx, ld_list) {
                tracing::warn!(
                    "fsck: PD {} chunklet {} is Used+unreferenced yet its header claims a live owner referencing it; leaving as-is",
                    pd_id,
                    idx
                );
                continue;
            }
            orphans.push(idx);
        }
        if orphans.is_empty() {
            continue;
        }
        let n = orphans.len() as u32;
        let captured = orphans;
        pd.commit_manifest(move |_body, bitmap| {
            for idx in &captured {
                bitmap.set(*idx, ChunkletState::Free)?;
            }
            Ok(())
        })?;
        tracing::warn!(
            "fsck: reclaimed {} Used-but-unreferenced (orphan) chunklets on PD {} -> Free",
            n,
            pd_id
        );
        reclaimed_by_pd.insert(*pd_id, n);
    }
    Ok(reclaimed_by_pd)
}

/// True iff the durable header proves `(pd, idx)` still belongs to a live LD
/// that references exactly this position — a genuine conflict with the caller's
/// "unreferenced" claim, so reclaim must be vetoed. An unreadable/corrupt/blank
/// header, or one naming a dropped or non-referencing LD, does NOT veto.
fn header_vetoes_reclaim(pd: &PhysicalDisk, idx: u32, ld_list: &LdList) -> bool {
    let bytes = match pd.read_chunklet_header_bytes(idx) {
        Ok(b) => b,
        Err(_) => return false,
    };
    let header = match ChunkletHeader::decode(&bytes) {
        Ok(h) => h,
        Err(_) => return false,
    };
    let pd_id = pd.pd_id();
    ld_list
        .lds
        .iter()
        .find(|d| d.id == header.owner_ld)
        .map(|d| {
            d.members
                .iter()
                .any(|m| m.pd == pd_id && m.chunklet_index == idx)
        })
        .unwrap_or(false)
}

impl Pool {
    /// Online whole-pool fsck: reclaim `Used`-but-unreferenced chunklets pool-
    /// wide. Skips (reclaims nothing, `skipped_incomplete = true`) if any PD is
    /// missing or draining — see module docs on the authority requirement.
    /// Administratively-`Failed` but present PDs are scanned (their stale
    /// capacity is exactly what we reclaim).
    pub fn fsck(&self) -> ChunkletResult<FsckReport> {
        let _commit = self.manifest_lock.lock();
        let (ld_list, pds, skip) = {
            let s = self.state.read();
            let has_missing = s.pd_seq_to_id.values().any(|id| !s.pds.contains_key(id));
            let has_draining = !s.draining.is_empty();
            (s.ld_list.clone(), s.pds.clone(), has_missing || has_draining)
        };
        if skip {
            return Ok(FsckReport {
                reclaimed_by_pd: BTreeMap::new(),
                total_reclaimed: 0,
                scanned_pds: 0,
                skipped_incomplete: true,
            });
        }
        let scanned_pds = pds.len();
        let reclaimed_by_pd = reverse_reconcile_bitmaps(&ld_list, &pds)?;
        let total_reclaimed = reclaimed_by_pd.values().copied().sum();
        self.state.write().last_fsck_reclaimed = total_reclaimed as usize;
        Ok(FsckReport {
            reclaimed_by_pd,
            total_reclaimed,
            scanned_pds,
            skipped_incomplete: false,
        })
    }

    /// Reclaim `Used`-but-unreferenced chunklets on a single present PD.
    /// Returns the reclaimed count (0 if the PD is not present).
    ///
    /// Caller MUST hold `manifest_lock` and guarantee the pool is complete
    /// (used by `reintegrate` after a returned disk is admitted). Not a
    /// standalone operator entry point — use [`Pool::fsck`] for that.
    #[allow(dead_code)] // wired into reintegrate in Phase C
    pub(crate) fn fsck_pd(&self, pd_id: PdId) -> ChunkletResult<u32> {
        let (ld_list, pd) = {
            let s = self.state.read();
            (s.ld_list.clone(), s.pds.get(&pd_id).cloned())
        };
        let Some(pd) = pd else {
            return Ok(0);
        };
        let mut one = BTreeMap::new();
        one.insert(pd_id, pd);
        let reclaimed = reverse_reconcile_bitmaps(&ld_list, &one)?;
        Ok(reclaimed.values().copied().sum())
    }
}
