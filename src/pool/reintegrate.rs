//! Returned-disk reintegration (Wipe strategy) + failed-PD retirement.
//!
//! # The gap this closes
//!
//! The fault→isolate→rebuild chain migrates a failed PD's members onto spares on
//! surviving PDs, but leaves the pool one member short: the failed slot lingers
//! as a `Failed` tombstone (present-but-flagged after `mark_pd_failed`, or absent
//! after `open_with_missing`), and a physically-returning disk has had no online
//! way back short of `chunklet-init --force` (destroy + rebuild the whole pool).
//!
//! Two operations close it, both online:
//!
//! - [`Pool::reintegrate_wipe`] — a returned disk rejoins by being WIPED and
//!   re-admitted with a FRESH `PdId` that REUSES the tombstone's pool slot
//!   (`pd_seq`), so `pool_pd_count` is invariant across the whole operation — no
//!   quorum drift by construction, no schema change. The returned disk's stale
//!   contents are discarded by the wipe (never trusted, never reverse-
//!   reconciled); [`Pool::rebalance`] refills the now-empty member afterward.
//! - [`Pool::retire_failed_pd`] — for a disk that is gone for good: drop the
//!   tombstone, re-dense the surviving seqs to `[0, count-1)`, and shrink
//!   `pool_pd_count` so `open_with_missing`'s quorum tracks the smaller pool.
//!
//! # Safety gate (load-bearing)
//!
//! LD member descriptors reference PDs by `PdId`, never by `pd_seq`. So reusing
//! a tombstone's seq for a fresh `PdId` does NOT touch any descriptor — but it
//! also means a member still naming the old `PdId` would be silently orphaned if
//! we wiped the disk out from under it. `reintegrate_wipe` therefore REFUSES to
//! wipe a disk whose old `PdId` is still referenced by any live descriptor; it
//! first `rebuild_ld`s every such LD to migrate the member onto a spare (making
//! the old `PdId` unreferenced), and only then wipes. It never reduces a
//! degraded set's redundancy.
//!
//! # Concurrency / crash-safety
//!
//! Both take `manifest_lock` for the cross-PD `commit_manifest` loop (ascending
//! `PdId`, like `admit`). `reintegrate_wipe` keeps `pool_pd_count` FIXED, so a
//! crash mid-loop can only leave some survivors naming the old tombstone and
//! some the new PD — the highest-gen manifest wins at the next open and every PD
//! still agrees on the count, so the pool opens cleanly and the stale entry is
//! refreshed on the next commit. This count-invariance is the reason reintegrate
//! reuses the slot rather than `retire + admit`.
//!
//! `retire_failed_pd` DOES shrink `pool_pd_count`, which carries the SAME crash
//! window as the shipped `admit` (which grows it): a crash mid cross-PD loop
//! leaves some PDs at the old count and some at the new one, and `open` /
//! `open_with_missing` currently require every present PD to agree on the count,
//! so such a split-brain does NOT self-heal — an operator must re-stamp the
//! stragglers (or the pool is rebuilt). The window is small (a few fsyncs on a
//! quiescent pool) and identical in class to `admit`. Making `open` adopt the
//! highest-gen count (mirroring how it already picks `ld_list`/`pd_list`) would
//! close it for both; that hardening is deferred (it changes the most load-
//! bearing path and wants dedicated crash-simulation coverage). Prefer
//! `reintegrate_wipe` (count-invariant) whenever a replacement disk exists.
//!
//! A physically-returning *retired* disk carries a stale (old-count) superblock;
//! it must rejoin via `admit` as a brand-new PD (its old `PdId` is no longer in
//! the pd_list, so `reintegrate_wipe` correctly rejects it with "use `admit`").

use std::sync::Arc;

use crate::error::{ChunkletError, ChunkletResult};
use crate::io::RawDevice;
use crate::pd::PhysicalDisk;
use crate::pool::{PdHealth, Pool};
use crate::superblock::PoolPdEntry;
use crate::types::{LdId, PdId};

#[derive(Clone, Debug)]
pub struct ReintegrateReport {
    /// Fresh identity the wiped disk rejoined under.
    pub new_pd_id: PdId,
    /// Old tombstone whose pool slot (`pd_seq`) the new PD reused.
    pub replaced_pd_id: PdId,
    /// The reused `pd_seq` (unchanged `pool_pd_count`).
    pub reused_seq: u32,
    /// LDs the safety gate rebuilt to migrate members off the old PD before the
    /// wipe. Empty when the old PD was already unreferenced (auto-failover done).
    pub rebuilt_lds: Vec<LdId>,
    /// How many live descriptor members still named the old PD when reintegrate
    /// began (all migrated away by `rebuilt_lds` before the wipe).
    pub referenced_members_blocking: usize,
}

impl Pool {
    /// Reintegrate a physically-returned disk under the Wipe strategy: erase it
    /// and re-admit it with a fresh `PdId` that reuses the failed tombstone's
    /// pool slot, keeping `pool_pd_count` constant. See the module docs for the
    /// safety gate (never wipe a still-referenced disk) and crash-safety.
    ///
    /// Rejects a blank/foreign-pool device (operator must `admit` a genuinely
    /// new disk explicitly) and a disk whose old `PdId` is Healthy / already
    /// retired / still held live (`PoolLocked`).
    pub fn reintegrate_wipe(&self, raw: RawDevice) -> ChunkletResult<ReintegrateReport> {
        // Claim the returned device up front (non-blocking flock), same as
        // open/admit. If the pool still holds this exact device live, this maps
        // to `PoolLocked` — the safe rejection for an ambiguous double-open.
        super::lock_devices_exclusive(std::slice::from_ref(&raw))?;

        // On-disk identity WITHOUT opening it as a member. A blank / foreign disk
        // is never auto-reintegrated (guards against a wrongly-inserted disk).
        let (disk_pool_id, old_pd_id) = match crate::ops::probe_pool_and_pd_id(&raw)? {
            Some(ids) => ids,
            None => {
                return Err(ChunkletError::Config(
                    "reintegrate: device has no valid superblock (blank/foreign); \
                     use `admit` to add a brand-new disk"
                        .into(),
                ));
            }
        };
        if disk_pool_id != self.pool_id {
            return Err(ChunkletError::PoolMismatch(format!(
                "reintegrate: device belongs to pool {}, this pool is {}",
                disk_pool_id, self.pool_id
            )));
        }

        // The old PdId must be a declared, Failed tombstone. Healthy = an active
        // member (double-insert); unknown = already retired (use `admit`).
        let seq = {
            let s = self.state.read();
            let seq = s
                .pd_seq_to_id
                .iter()
                .find(|(_, id)| **id == old_pd_id)
                .map(|(seq, _)| *seq);
            let Some(seq) = seq else {
                return Err(ChunkletError::Invariant(format!(
                    "reintegrate: PD {} is not in this pool's pd_list (already retired?); use `admit`",
                    old_pd_id
                )));
            };
            if s.pd_health.get(&old_pd_id) != Some(&PdHealth::Failed) {
                return Err(ChunkletError::Invariant(format!(
                    "reintegrate: PD {} is not Failed (still an active member?); refusing to wipe",
                    old_pd_id
                )));
            }
            seq
        };

        // ---- Safety gate: migrate any still-referenced members off the old PD
        // BEFORE wiping. Done WITHOUT manifest_lock (rebuild_ld takes it itself).
        let (affected, referenced_members_blocking) = {
            let s = self.state.read();
            let affected: Vec<LdId> = s
                .ld_list
                .lds
                .iter()
                .filter(|d| d.members.iter().any(|m| m.pd == old_pd_id))
                .map(|d| d.id)
                .collect();
            let referenced = s
                .ld_list
                .lds
                .iter()
                .flat_map(|d| d.members.iter())
                .filter(|m| m.pd == old_pd_id)
                .count();
            (affected, referenced)
        };
        let mut rebuilt_lds = Vec::new();
        for ld_id in &affected {
            // rebuild_ld migrates the Failed member onto a spare on a healthy PD,
            // rewriting the descriptor so it no longer names old_pd_id.
            self.rebuild_ld(*ld_id)?;
            rebuilt_lds.push(*ld_id);
        }
        // Re-verify no live descriptor still references the old PD; a leftover
        // (e.g. unrecoverable set, or a Plain/Raid0 member with no redundancy)
        // must NOT be wiped — that would be data loss.
        if self
            .state
            .read()
            .ld_list
            .lds
            .iter()
            .flat_map(|d| d.members.iter())
            .any(|m| m.pd == old_pd_id)
        {
            return Err(ChunkletError::Invariant(format!(
                "reintegrate: PD {} still referenced after rebuild; cannot safely wipe",
                old_pd_id
            )));
        }

        // ---- Replace-in-place under manifest_lock: wipe + admit a fresh PdId at
        // the same seq; pool_pd_count unchanged.
        let _commit = self.manifest_lock.lock();
        let new_pd_id = PdId::new_v4();

        // Build the pool-wide view from a SURVIVOR (never the old PD): swap the
        // tombstone entry (old_pd_id @ seq, FAILED) for the fresh PD (new @ seq).
        let (spare_pct, ld_bytes, cpg_bytes, new_pd_list, pool_pd_count, shared_backend) = {
            let s = self.state.read();
            let survivor = s
                .pds
                .iter()
                .find(|(id, _)| **id != old_pd_id)
                .map(|(_, pd)| pd.clone())
                .ok_or_else(|| {
                    ChunkletError::Invariant("reintegrate: pool has no other live PD".into())
                })?;
            let (body, _, _) = survivor.snapshot();
            let mut list = body.pd_list.clone();
            let entry = list
                .iter_mut()
                .find(|e| e.pd_id == old_pd_id)
                .ok_or_else(|| {
                    ChunkletError::Invariant(format!(
                        "reintegrate: PD {} vanished from pd_list",
                        old_pd_id
                    ))
                })?;
            entry.pd_id = new_pd_id;
            entry.pd_seq = seq;
            entry.flags = 0;
            (
                body.spare_pct,
                body.ld_list_bytes.clone(),
                body.cpg_list_bytes.clone(),
                list,
                body.pool_pd_count,
                survivor.backend(),
            )
        };

        // No descriptor can issue new IO to old_pd_id after the safety gate.
        // Remove its scheduler/execution state before wiping so a stale queued
        // or active request aborts the replacement without touching the disk.
        shared_backend.unregister_pd(old_pd_id)?;

        // Wipe + initialize the returned device as the fresh PD (init zeroes the
        // bitmap → no stale Used chunklets survive, so no fsck needed).
        let new_pd = PhysicalDisk::init(
            raw,
            self.pool_id,
            new_pd_id,
            seq,
            pool_pd_count,
            new_pd_list.clone(),
            spare_pct,
            ld_bytes,
            cpg_bytes,
        )?;

        // Commit the swapped pd_list to every SURVIVING PD (ascending PdId via
        // the BTreeMap). The old PD is excluded: its device is the one we just
        // wiped into `new_pd`, so writing through a stale handle would clobber
        // the fresh superblock.
        let survivors: Vec<Arc<PhysicalDisk>> = {
            let s = self.state.read();
            s.pds
                .iter()
                .filter(|(id, _)| **id != old_pd_id)
                .map(|(_, pd)| pd.clone())
                .collect()
        };
        for pd in &survivors {
            let list = new_pd_list.clone();
            pd.commit_manifest(move |body, _bitmap| {
                body.pd_list = list;
                // pool_pd_count intentionally unchanged (replace-in-place).
                Ok(())
            })?;
        }
        // Registration is deliberately after every fallible manifest write:
        // an aborted replacement must not leave the fresh ID in shared lane
        // accounting when it was never published into PoolState.
        new_pd.set_backend(shared_backend);

        // Publish: drop the tombstone, install the fresh PD at the reused seq.
        {
            let mut s = self.state.write();
            s.pds.remove(&old_pd_id);
            s.pds.insert(new_pd_id, new_pd);
            s.pd_seq_to_id.insert(seq, new_pd_id);
            s.pd_health.remove(&old_pd_id);
            s.pd_health.insert(new_pd_id, PdHealth::Healthy);
            s.draining.remove(&old_pd_id);
        }

        Ok(ReintegrateReport {
            new_pd_id,
            replaced_pd_id: old_pd_id,
            reused_seq: seq,
            rebuilt_lds,
            referenced_members_blocking,
        })
    }

    /// Retire a `Failed`, ABSENT, unreferenced tombstone: remove it from the
    /// pd_list, re-dense the surviving seqs to `[0, count-1)`, and shrink
    /// `pool_pd_count` so quorum tracks the smaller pool. For a disk that is gone
    /// for good (a returned disk should use [`Pool::reintegrate_wipe`] instead).
    ///
    /// Refuses if the PD is Healthy, still present (a live handle means it isn't
    /// gone — drain it first), still referenced by an LD (rebuild first, else the
    /// member would be orphaned), or if retiring it would empty the pool.
    pub fn retire_failed_pd(&self, old_pd_id: PdId) -> ChunkletResult<()> {
        let _commit = self.manifest_lock.lock();
        {
            let s = self.state.read();
            if !s.pd_seq_to_id.values().any(|id| *id == old_pd_id) {
                return Err(ChunkletError::Invariant(format!(
                    "retire: PD {} is not in this pool",
                    old_pd_id
                )));
            }
            if s.pd_health.get(&old_pd_id) != Some(&PdHealth::Failed) {
                return Err(ChunkletError::Invariant(format!(
                    "retire: PD {} is not Failed; only a failed disk can be retired",
                    old_pd_id
                )));
            }
            if s.pds.contains_key(&old_pd_id) {
                return Err(ChunkletError::Invariant(format!(
                    "retire: PD {} still has a live handle (not gone); drain it first",
                    old_pd_id
                )));
            }
            if s.ld_list
                .lds
                .iter()
                .flat_map(|d| d.members.iter())
                .any(|m| m.pd == old_pd_id)
            {
                return Err(ChunkletError::Invariant(format!(
                    "retire: PD {} is still referenced by an LD; rebuild it away first",
                    old_pd_id
                )));
            }
            if s.pds.is_empty() {
                return Err(ChunkletError::Invariant(
                    "retire: pool has no surviving live PD".into(),
                ));
            }
        }
        // The failed ID is unreachable to new IO and has no live handle. Do
        // this before the first manifest write so scheduler/execution backlog
        // rejects retirement without partially changing the on-disk pd_list.
        self.state
            .read()
            .pds
            .values()
            .next()
            .expect("checked non-empty surviving PD set")
            .backend()
            .unregister_pd(old_pd_id)?;

        // Re-dense: survivors keep their flags, take new seqs [0, count-1) in
        // ascending old-seq order. Descriptors reference PdId (not seq), so no
        // descriptor changes — only the pool bookkeeping does.
        let (new_pd_list, new_count, survivors) = {
            let s = self.state.read();
            let survivor_pd = s.pds.values().next().expect("checked non-empty above");
            let (body, _, _) = survivor_pd.snapshot();
            let mut kept: Vec<PoolPdEntry> = body
                .pd_list
                .iter()
                .filter(|e| e.pd_id != old_pd_id)
                .cloned()
                .collect();
            kept.sort_by_key(|e| e.pd_seq);
            for (new_seq, e) in kept.iter_mut().enumerate() {
                e.pd_seq = new_seq as u32;
            }
            let new_count = kept.len() as u32;
            let survivors: Vec<Arc<PhysicalDisk>> = s.pds.values().cloned().collect();
            (kept, new_count, survivors)
        };

        // Cross-PD commit (ascending PdId via BTreeMap iteration): each survivor
        // records the re-densed pd_list, the shrunk count, AND its OWN new seq.
        for pd in &survivors {
            let my_pd_id = pd.pd_id();
            let my_seq = new_pd_list
                .iter()
                .find(|e| e.pd_id == my_pd_id)
                .map(|e| e.pd_seq)
                .ok_or_else(|| {
                    ChunkletError::Invariant(format!(
                        "retire: survivor PD {} missing from re-densed list",
                        my_pd_id
                    ))
                })?;
            let list = new_pd_list.clone();
            pd.commit_manifest(move |body, _bitmap| {
                body.pd_list = list;
                body.pool_pd_count = new_count;
                body.pd_seq_in_pool = my_seq;
                Ok(())
            })?;
        }

        // Publish: forget the tombstone, rebuild the seq map from the new list.
        {
            let mut s = self.state.write();
            s.pd_health.remove(&old_pd_id);
            s.draining.remove(&old_pd_id);
            s.pd_seq_to_id = new_pd_list.iter().map(|e| (e.pd_seq, e.pd_id)).collect();
        }
        Ok(())
    }
}
