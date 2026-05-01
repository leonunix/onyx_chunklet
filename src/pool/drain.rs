//! Pool::drain_pd — migrate every LD member off a PD prior to removal.
//!
//! Drain marks a PD as "draining" in `PoolState.draining`, then runs a
//! rebuild pass for every LD that touches the PD. The existing rebuild
//! machinery (extended in this commit to consider draining PDs as
//! "needs-rebuild" sources) walks the affected LDs, allocates spare
//! chunklets on healthy non-draining PDs, copies the data over, and
//! commits the new descriptors. After all LDs are off, the PD's
//! `PoolPdEntry.flags` is bumped to include `DRAINED` and persisted on
//! every other PD's manifest.
//!
//! Drain is **synchronous** in P7 — for big LDs it can take a while.
//! Background-mode drain workers are deferred (Phase 9+).

use crate::error::{ChunkletError, ChunkletResult};
use crate::pool::Pool;
use crate::superblock::pool_pd_flags;
use crate::types::{LdId, PdId, RaidLevel};

#[derive(Clone, Debug)]
pub struct DrainReport {
    pub pd_id: PdId,
    pub lds_affected: Vec<LdId>,
    pub members_migrated: usize,
}

impl Pool {
    /// Drain a PD by migrating all of its LD members onto other PDs.
    ///
    /// Returns `DrainReport` summarizing the work done. After successful
    /// drain, the PD is left in the `pd_list` flagged `DRAINED`; subsequent
    /// `Pool::open` will refuse to use it for new allocations and will
    /// emit a warning in the log.
    ///
    /// LDs without redundancy (Plain / Raid0) that have a member on the
    /// draining PD will fail their rebuild step → the entire drain returns
    /// an error and nothing is persisted (caller must drop those LDs first
    /// or accept data loss explicitly).
    pub fn drain_pd(&self, pd_id: PdId) -> ChunkletResult<DrainReport> {
        // Sanity: PD must be present and Healthy. Failed PDs are already gone;
        // there's nothing to drain.
        {
            let s = self.state.read();
            if !s.pds.contains_key(&pd_id) {
                return Err(ChunkletError::Invariant(format!(
                    "drain_pd: PD {} not present (Failed or unknown)",
                    pd_id
                )));
            }
            if s.draining.contains(&pd_id) {
                return Err(ChunkletError::Invariant(format!(
                    "drain_pd: PD {} already draining",
                    pd_id
                )));
            }
        }

        // Mark as draining.
        self.state.write().draining.insert(pd_id);

        // Find affected LDs: those with at least one member.pd == pd_id, AND
        // whose raid_level can be rebuilt (Mirror / Raid5 / Raid6).
        let lds_with_members: Vec<(LdId, usize)> = {
            let s = self.state.read();
            s.ld_list
                .lds
                .iter()
                .map(|d| {
                    let count = d.members.iter().filter(|m| m.pd == pd_id).count();
                    (d.id, count)
                })
                .filter(|(_, c)| *c > 0)
                .collect()
        };

        // Validate redundancy first — fail before doing any work if any
        // affected LD lacks redundancy.
        for (ld_id, _) in &lds_with_members {
            let desc = self.find_ld(*ld_id).unwrap();
            if matches!(desc.raid_level, RaidLevel::Plain | RaidLevel::Raid0) {
                self.state.write().draining.remove(&pd_id);
                return Err(ChunkletError::Unsupported(format!(
                    "drain_pd: LD {} is {:?} (no redundancy); drop it first",
                    ld_id, desc.raid_level
                )));
            }
        }

        let mut affected = Vec::with_capacity(lds_with_members.len());
        let mut members_migrated = 0;
        for (ld_id, _) in &lds_with_members {
            // rebuild_ld treats draining PDs as needs-rebuild sources (see
            // pool/rebuild.rs `member_needs_rebuild`).
            let report = self.rebuild_ld(*ld_id)?;
            members_migrated += report.rebuilt_members;
            affected.push(*ld_id);
        }

        // After rebuild, the PD should have no Used chunklets that belong to
        // any LD. Set DRAINED in pool_pd_list on every (other) PD's manifest.
        self.persist_drained_flag(pd_id)?;

        // Mark as Drained in pd_health (treat as Failed for future IO).
        // We keep the PD's Arc<PhysicalDisk> in `pds` so any in-flight
        // reference can finish, but logically it's gone.
        {
            let mut s = self.state.write();
            s.draining.remove(&pd_id);
            // Leave the PD in pds for now; admin can call remove_drained_pd
            // (Phase 9+) to fully evict.
        }

        Ok(DrainReport {
            pd_id,
            lds_affected: affected,
            members_migrated,
        })
    }

    fn persist_drained_flag(&self, pd_id: PdId) -> ChunkletResult<()> {
        let _commit = self.manifest_lock.lock();
        let pds_snapshot = self.state.read().pds.clone();
        // Build new pd_list with DRAINED flag set on the target.
        let new_pd_list: Vec<crate::superblock::PoolPdEntry> = {
            let s = self.state.read();
            s.pd_seq_to_id
                .values()
                .map(|id| {
                    let mut flags = 0u32;
                    if *id == pd_id {
                        flags |= pool_pd_flags::DRAINED;
                    }
                    crate::superblock::PoolPdEntry {
                        pd_id: *id,
                        pd_seq: s
                            .pds
                            .get(id)
                            .map(|p| p.info().pd_seq_in_pool)
                            .or_else(|| {
                                // Drained PDs are still in pd_seq_to_id even if
                                // they no longer hold an Arc<PD>.
                                s.pd_seq_to_id
                                    .iter()
                                    .find_map(|(seq, pid)| (pid == id).then_some(*seq))
                            })
                            .unwrap_or(0),
                        flags,
                    }
                })
                .collect()
        };

        for (_pid, pd) in &pds_snapshot {
            let pdl = new_pd_list.clone();
            pd.commit_manifest(move |body, _bm| {
                body.pd_list = pdl;
                Ok(())
            })?;
        }
        Ok(())
    }

    /// Returns true if the given PD is currently being drained.
    pub fn is_pd_draining(&self, pd_id: PdId) -> bool {
        self.state.read().draining.contains(&pd_id)
    }
}
