//! Disk lifecycle, spare management, and recovery orchestration.

use crate::error::{ChunkletError, ChunkletResult};
use crate::pool::{PdHealth, Pool};
use crate::superblock::pool_pd_flags;
use crate::types::{ChunkletState, LdId, PdId, RaidLevel};

#[derive(Clone, Debug)]
pub struct PdSpareRebalance {
    pub pd_id: PdId,
    pub wanted_spares: u32,
    pub spares_before: u32,
    pub spares_after: u32,
    pub free_before: u32,
    pub free_after: u32,
}

#[derive(Clone, Debug)]
pub struct SpareRebalanceReport {
    pub spare_pct: u8,
    pub pds: Vec<PdSpareRebalance>,
}

#[derive(Clone, Debug)]
pub struct LdRecoverReport {
    pub ld_id: LdId,
    pub scrub_mismatches: usize,
    pub scrub_marked_bad: usize,
    pub rebuilt_members: usize,
    pub skipped_rebuild: bool,
    pub error: Option<String>,
}

#[derive(Clone, Debug)]
pub struct AutoRecoverReport {
    pub attempted: usize,
    pub recovered: usize,
    pub failed: usize,
    pub lds: Vec<LdRecoverReport>,
}

impl Pool {
    pub fn mark_pd_failed(&self, pd_id: PdId) -> ChunkletResult<()> {
        self.set_pd_failed_flag(pd_id, true)
    }

    pub fn clear_pd_failed(&self, pd_id: PdId) -> ChunkletResult<()> {
        self.set_pd_failed_flag(pd_id, false)
    }

    pub fn rebalance_spares(&self, spare_pct: u8) -> ChunkletResult<SpareRebalanceReport> {
        if spare_pct > 100 {
            return Err(ChunkletError::Config(format!(
                "spare_pct must be <= 100, got {}",
                spare_pct
            )));
        }

        let _commit = self.manifest_lock.lock();
        let pds = self.state.read().pds.clone();
        let mut report = SpareRebalanceReport {
            spare_pct,
            pds: Vec::new(),
        };

        for (pd_id, pd) in &pds {
            let (_, bitmap, _) = pd.snapshot();
            let wanted = spare_count(bitmap.len(), spare_pct);
            let spare_before = bitmap.count(ChunkletState::Spare);
            let free_before = bitmap.count(ChunkletState::Free);
            let mut changed = false;
            let mut after_spare = spare_before;
            let mut after_free = free_before;

            if spare_before < wanted {
                let need = wanted - spare_before;
                if free_before < need {
                    return Err(ChunkletError::Config(format!(
                        "PD {} needs {} additional spare chunklets but has only {} free",
                        pd_id, need, free_before
                    )));
                }
                pd.commit_manifest(|body, bitmap| {
                    body.spare_pct = spare_pct;
                    let mut converted = 0u32;
                    for idx in 0..bitmap.len() {
                        if converted == need {
                            break;
                        }
                        if bitmap.get(idx)? == ChunkletState::Free {
                            bitmap.set(idx, ChunkletState::Spare)?;
                            converted += 1;
                        }
                    }
                    Ok(())
                })?;
                changed = true;
                after_spare = wanted;
                after_free = free_before - need;
            } else if spare_before > wanted {
                let release = spare_before - wanted;
                pd.commit_manifest(|body, bitmap| {
                    body.spare_pct = spare_pct;
                    let mut converted = 0u32;
                    for idx in (0..bitmap.len()).rev() {
                        if converted == release {
                            break;
                        }
                        if bitmap.get(idx)? == ChunkletState::Spare {
                            bitmap.set(idx, ChunkletState::Free)?;
                            converted += 1;
                        }
                    }
                    Ok(())
                })?;
                changed = true;
                after_spare = wanted;
                after_free = free_before + release;
            }

            if !changed {
                pd.commit_manifest(|body, _bitmap| {
                    body.spare_pct = spare_pct;
                    Ok(())
                })?;
            }
            report.pds.push(PdSpareRebalance {
                pd_id: *pd_id,
                wanted_spares: wanted,
                spares_before: spare_before,
                spares_after: after_spare,
                free_before,
                free_after: after_free,
            });
        }
        Ok(report)
    }

    pub fn auto_recover(&self, scrub_first: bool) -> AutoRecoverReport {
        let ld_ids: Vec<LdId> = self
            .state
            .read()
            .ld_list
            .lds
            .iter()
            .filter(|desc| !matches!(desc.raid_level, RaidLevel::Plain | RaidLevel::Raid0))
            .map(|desc| desc.id)
            .collect();

        let mut report = AutoRecoverReport {
            attempted: 0,
            recovered: 0,
            failed: 0,
            lds: Vec::new(),
        };
        for ld_id in ld_ids {
            let mut ld_report = LdRecoverReport {
                ld_id,
                scrub_mismatches: 0,
                scrub_marked_bad: 0,
                rebuilt_members: 0,
                skipped_rebuild: false,
                error: None,
            };

            if scrub_first {
                match self.scrub_ld(ld_id) {
                    Ok(scrub) => {
                        ld_report.scrub_mismatches = scrub.mismatches.len();
                        ld_report.scrub_marked_bad = scrub.marked_bad;
                    }
                    Err(e) => {
                        ld_report.error = Some(format!("scrub: {}", e));
                        report.failed += 1;
                        report.lds.push(ld_report);
                        continue;
                    }
                }
            }

            report.attempted += 1;
            match self.rebuild_ld(ld_id) {
                Ok(rebuild) => {
                    ld_report.rebuilt_members = rebuild.rebuilt_members;
                    ld_report.skipped_rebuild = rebuild.skipped;
                    if !rebuild.skipped || ld_report.scrub_marked_bad > 0 {
                        report.recovered += 1;
                    }
                }
                Err(e) => {
                    ld_report.error = Some(format!("rebuild: {}", e));
                    report.failed += 1;
                }
            }
            report.lds.push(ld_report);
        }
        report
    }

    fn set_pd_failed_flag(&self, pd_id: PdId, failed: bool) -> ChunkletResult<()> {
        let _commit = self.manifest_lock.lock();
        let (pds, affected) = {
            let s = self.state.read();
            let known = s.pd_seq_to_id.values().any(|id| *id == pd_id);
            if !known {
                return Err(ChunkletError::Invariant(format!(
                    "PD {} not in pool",
                    pd_id
                )));
            }
            if !failed && !s.pds.contains_key(&pd_id) {
                return Err(ChunkletError::Invariant(format!(
                    "cannot clear failed mark on missing PD {}",
                    pd_id
                )));
            }
            let mut affected = s
                .ld_list
                .lds
                .iter()
                .filter(|desc| desc.members.iter().any(|m| m.pd == pd_id))
                .filter_map(|desc| {
                    s.ld_runtime
                        .get(&desc.id)
                        .cloned()
                        .map(|runtime| (desc.id, runtime))
                })
                .collect::<Vec<_>>();
            affected.sort_by_key(|(ld_id, _)| *ld_id);
            (s.pds.clone(), affected)
        };
        let _io_guards = affected
            .iter()
            .map(|(_, runtime)| runtime.io_lock.write())
            .collect::<Vec<_>>();
        for pd in pds.values() {
            pd.commit_manifest(|body, _bitmap| {
                let entry = body
                    .pd_list
                    .iter_mut()
                    .find(|entry| entry.pd_id == pd_id)
                    .ok_or_else(|| {
                        ChunkletError::Invariant(format!("PD {} missing from pd_list", pd_id))
                    })?;
                if failed {
                    entry.flags |= pool_pd_flags::FAILED;
                } else {
                    entry.flags &= !pool_pd_flags::FAILED;
                }
                Ok(())
            })?;
        }
        self.state.write().pd_health.insert(
            pd_id,
            if failed {
                PdHealth::Failed
            } else {
                PdHealth::Healthy
            },
        );
        for (_, runtime) in &affected {
            runtime.bump();
        }
        Ok(())
    }
}

fn spare_count(total: u32, spare_pct: u8) -> u32 {
    (((total as u64) * (spare_pct as u64) + 99) / 100) as u32
}
