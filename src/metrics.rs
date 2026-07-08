//! Pool/PD/LD status snapshots for operator diagnostics.
//!
//! This module is intentionally read-only: it clones the current pool view,
//! counts bitmap states, and derives capacity numbers without mutating
//! manifests or opening LD handles.

use std::path::PathBuf;

use crate::error::ChunkletResult;
use crate::ld::compute_strip_bytes;
use crate::pool::{PdHealth, Pool};
use crate::superblock::pool_pd_flags;
use crate::types::{
    ChunkletState, LdId, PdId, PoolId, RaidLevel, CHUNKLET_HEADER_BYTES, CHUNKLET_SIZE,
};

const CHUNKLET_USER_BYTES: u64 = CHUNKLET_SIZE - CHUNKLET_HEADER_BYTES;

#[derive(Clone, Debug)]
pub struct PoolMetrics {
    pub pool_id: PoolId,
    pub pd_count: usize,
    pub healthy_pds: usize,
    pub failed_pds: usize,
    pub draining_pds: usize,
    pub drained_pds: usize,
    pub ld_count: usize,
    pub cpg_count: usize,
    pub raw_bytes: u64,
    pub user_bytes: u64,
    pub allocatable_bytes: u64,
    pub used_bytes: u64,
    pub spare_bytes: u64,
    pub bad_bytes: u64,
    pub migrating_bytes: u64,
    pub total_chunklets: u64,
    pub free_chunklets: u64,
    pub used_chunklets: u64,
    pub spare_chunklets: u64,
    pub bad_chunklets: u64,
    pub migrating_chunklets: u64,
    pub last_reconciliation_count: usize,
    pub last_fsck_reclaimed: usize,
    /// Worst-case per-PD used-chunklet skew over Healthy PDs (max - min). The
    /// signal a data rebalance targets; worst-case, not an average.
    pub used_skew_chunklets: u32,
    /// `used_skew_chunklets` as a percentage of the mean Healthy-PD used count.
    pub used_skew_pct: f64,
    pub pds: Vec<PdMetrics>,
    pub lds: Vec<LdMetrics>,
}

#[derive(Clone, Debug)]
pub struct PdMetrics {
    pub pd_id: PdId,
    pub pd_seq: u32,
    pub state: PdOperationalState,
    pub drained: bool,
    pub draining: bool,
    pub path: Option<PathBuf>,
    pub backend: Option<&'static str>,
    pub numa_node: Option<u16>,
    pub manifest_gen: Option<u64>,
    pub size_bytes: u64,
    pub user_bytes: u64,
    pub allocatable_bytes: u64,
    pub used_bytes: u64,
    pub spare_bytes: u64,
    pub bad_bytes: u64,
    pub migrating_bytes: u64,
    pub total_chunklets: u32,
    pub free_chunklets: u32,
    pub used_chunklets: u32,
    pub spare_chunklets: u32,
    pub bad_chunklets: u32,
    pub migrating_chunklets: u32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PdOperationalState {
    Healthy,
    Failed,
    Draining,
    Drained,
}

#[derive(Clone, Debug)]
pub struct LdMetrics {
    pub ld_id: LdId,
    pub raid_level: RaidLevel,
    pub set_size: u8,
    pub row_size: u16,
    pub num_rows: u16,
    pub ha_domain: crate::types::HaDomain,
    pub strip_size_bytes: u64,
    pub capacity_bytes: u64,
    pub member_count: usize,
    pub unavailable_members: usize,
    pub bad_members: usize,
    pub failed_members: usize,
    pub draining_members: usize,
    pub drained_members: usize,
}

impl Pool {
    pub fn metrics(&self) -> ChunkletResult<PoolMetrics> {
        let s = self.state.read();
        let mut pds = Vec::with_capacity(s.pd_seq_to_id.len());
        let pd_flags = s
            .pds
            .values()
            .next()
            .map(|pd| {
                let (body, _, _) = pd.snapshot();
                body.pd_list
                    .into_iter()
                    .map(|entry| (entry.pd_id, entry.flags))
                    .collect::<std::collections::BTreeMap<_, _>>()
            })
            .unwrap_or_default();

        for (seq, pd_id) in &s.pd_seq_to_id {
            let health = s.pd_health.get(pd_id).copied().unwrap_or(PdHealth::Failed);
            let drained = pd_flags
                .get(pd_id)
                .map(|flags| flags & pool_pd_flags::DRAINED != 0)
                .unwrap_or(false);
            let draining = s.draining.contains(pd_id);
            let state = match (health, draining, drained) {
                (PdHealth::Failed, _, _) => PdOperationalState::Failed,
                (_, true, _) => PdOperationalState::Draining,
                (_, _, true) => PdOperationalState::Drained,
                _ => PdOperationalState::Healthy,
            };

            let metric = match s.pds.get(pd_id) {
                Some(pd) => {
                    let info = pd.info();
                    let (_, bitmap, _) = pd.snapshot();
                    build_live_pd_metrics(
                        info,
                        bitmap,
                        state,
                        drained,
                        draining,
                        pd.backend().name(),
                    )
                }
                None => PdMetrics {
                    pd_id: *pd_id,
                    pd_seq: *seq,
                    state,
                    drained,
                    draining,
                    path: None,
                    backend: None,
                    numa_node: None,
                    manifest_gen: None,
                    size_bytes: 0,
                    user_bytes: 0,
                    allocatable_bytes: 0,
                    used_bytes: 0,
                    spare_bytes: 0,
                    bad_bytes: 0,
                    migrating_bytes: 0,
                    total_chunklets: 0,
                    free_chunklets: 0,
                    used_chunklets: 0,
                    spare_chunklets: 0,
                    bad_chunklets: 0,
                    migrating_chunklets: 0,
                },
            };
            pds.push(metric);
        }

        let mut lds = Vec::with_capacity(s.ld_list.lds.len());
        for desc in &s.ld_list.lds {
            lds.push(build_ld_metrics(
                desc,
                &s.pd_health,
                &s.draining,
                &pd_flags,
                &s.pds,
            )?);
        }

        let mut out = PoolMetrics {
            pool_id: self.id(),
            pd_count: pds.len(),
            healthy_pds: pds
                .iter()
                .filter(|pd| pd.state == PdOperationalState::Healthy)
                .count(),
            failed_pds: pds
                .iter()
                .filter(|pd| pd.state == PdOperationalState::Failed)
                .count(),
            draining_pds: pds
                .iter()
                .filter(|pd| pd.state == PdOperationalState::Draining)
                .count(),
            drained_pds: pds
                .iter()
                .filter(|pd| pd.state == PdOperationalState::Drained)
                .count(),
            ld_count: lds.len(),
            cpg_count: s.cpg_list.cpgs.len(),
            raw_bytes: 0,
            user_bytes: 0,
            allocatable_bytes: 0,
            used_bytes: 0,
            spare_bytes: 0,
            bad_bytes: 0,
            migrating_bytes: 0,
            total_chunklets: 0,
            free_chunklets: 0,
            used_chunklets: 0,
            spare_chunklets: 0,
            bad_chunklets: 0,
            migrating_chunklets: 0,
            last_reconciliation_count: s.last_reconciliation_count,
            last_fsck_reclaimed: s.last_fsck_reclaimed,
            used_skew_chunklets: 0,
            used_skew_pct: 0.0,
            pds,
            lds,
        };
        for pd in &out.pds {
            out.raw_bytes += pd.size_bytes;
            out.user_bytes += pd.user_bytes;
            out.allocatable_bytes += pd.allocatable_bytes;
            out.used_bytes += pd.used_bytes;
            out.spare_bytes += pd.spare_bytes;
            out.bad_bytes += pd.bad_bytes;
            out.migrating_bytes += pd.migrating_bytes;
            out.total_chunklets += pd.total_chunklets as u64;
            out.free_chunklets += pd.free_chunklets as u64;
            out.used_chunklets += pd.used_chunklets as u64;
            out.spare_chunklets += pd.spare_chunklets as u64;
            out.bad_chunklets += pd.bad_chunklets as u64;
            out.migrating_chunklets += pd.migrating_chunklets as u64;
        }
        // Worst-case per-PD used skew over Healthy PDs (max - min) — the signal a
        // data rebalance targets. Worst-case, not an average (per project convention).
        let healthy_used: Vec<u32> = out
            .pds
            .iter()
            .filter(|pd| pd.state == PdOperationalState::Healthy)
            .map(|pd| pd.used_chunklets)
            .collect();
        if let (Some(&min), Some(&max)) = (healthy_used.iter().min(), healthy_used.iter().max()) {
            out.used_skew_chunklets = max - min;
            let mean =
                healthy_used.iter().map(|&u| u as f64).sum::<f64>() / healthy_used.len() as f64;
            out.used_skew_pct = if mean > 0.0 {
                (max - min) as f64 / mean * 100.0
            } else {
                0.0
            };
        }
        Ok(out)
    }
}

fn build_live_pd_metrics(
    info: crate::pd::PdInfo,
    bitmap: crate::bitmap::Bitmap,
    state: PdOperationalState,
    drained: bool,
    draining: bool,
    backend: &'static str,
) -> PdMetrics {
    let free = bitmap.count(ChunkletState::Free);
    let used = bitmap.count(ChunkletState::Used);
    let spare = bitmap.count(ChunkletState::Spare);
    let bad = bitmap.count(ChunkletState::Bad);
    let migrating = bitmap.count(ChunkletState::Migrating);
    PdMetrics {
        pd_id: info.pd_id,
        pd_seq: info.pd_seq_in_pool,
        state,
        drained,
        draining,
        path: Some(info.path),
        backend: Some(backend),
        numa_node: info.numa_node,
        manifest_gen: Some(info.manifest_gen),
        size_bytes: info.size_bytes,
        user_bytes: (info.total_chunklets as u64) * CHUNKLET_USER_BYTES,
        allocatable_bytes: (free as u64) * CHUNKLET_USER_BYTES,
        used_bytes: (used as u64) * CHUNKLET_USER_BYTES,
        spare_bytes: (spare as u64) * CHUNKLET_USER_BYTES,
        bad_bytes: (bad as u64) * CHUNKLET_USER_BYTES,
        migrating_bytes: (migrating as u64) * CHUNKLET_USER_BYTES,
        total_chunklets: info.total_chunklets,
        free_chunklets: free,
        used_chunklets: used,
        spare_chunklets: spare,
        bad_chunklets: bad,
        migrating_chunklets: migrating,
    }
}

fn build_ld_metrics(
    desc: &crate::ld::LdDescriptor,
    pd_health: &std::collections::BTreeMap<PdId, PdHealth>,
    draining: &std::collections::BTreeSet<PdId>,
    pd_flags: &std::collections::BTreeMap<PdId, u32>,
    pds: &std::collections::BTreeMap<PdId, std::sync::Arc<crate::pd::PhysicalDisk>>,
) -> ChunkletResult<LdMetrics> {
    let strip_size_bytes = match desc.raid_level {
        RaidLevel::Plain => crate::types::BLOCK_SIZE,
        _ => compute_strip_bytes(desc.strip_size_log2)?,
    };
    let usable_per_chunklet = (CHUNKLET_USER_BYTES / strip_size_bytes) * strip_size_bytes;
    let capacity_bytes = match desc.raid_level {
        RaidLevel::Plain => (desc.members.len() as u64) * CHUNKLET_USER_BYTES,
        RaidLevel::Mirror | RaidLevel::Raid0 => {
            (desc.row_size as u64) * (desc.num_rows as u64) * usable_per_chunklet
        }
        RaidLevel::Raid5 => {
            (desc.set_size.saturating_sub(1) as u64)
                * (desc.row_size as u64)
                * (desc.num_rows as u64)
                * usable_per_chunklet
        }
        RaidLevel::Raid6 => {
            (desc.set_size.saturating_sub(2) as u64)
                * (desc.row_size as u64)
                * (desc.num_rows as u64)
                * usable_per_chunklet
        }
    };

    let mut failed_members = 0usize;
    let mut draining_members = 0usize;
    let mut drained_members = 0usize;
    let mut bad_members = 0usize;
    for member in &desc.members {
        if pd_health.get(&member.pd) == Some(&PdHealth::Failed) {
            failed_members += 1;
        }
        if draining.contains(&member.pd) {
            draining_members += 1;
        }
        if pd_flags
            .get(&member.pd)
            .map(|flags| flags & pool_pd_flags::DRAINED != 0)
            .unwrap_or(false)
        {
            drained_members += 1;
        }
        if let Some(pd) = pds.get(&member.pd) {
            let (_, bitmap, _) = pd.snapshot();
            if bitmap.get(member.chunklet_index)? == ChunkletState::Bad {
                bad_members += 1;
            }
        }
    }
    Ok(LdMetrics {
        ld_id: desc.id,
        raid_level: desc.raid_level,
        set_size: desc.set_size,
        row_size: desc.row_size,
        num_rows: desc.num_rows,
        ha_domain: desc.ha_domain,
        strip_size_bytes,
        capacity_bytes,
        member_count: desc.members.len(),
        unavailable_members: failed_members + draining_members + drained_members + bad_members,
        bad_members,
        failed_members,
        draining_members,
        drained_members,
    })
}
