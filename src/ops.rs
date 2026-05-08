//! Operator-facing snapshots and recovery helpers.
//!
//! This module keeps dashboard/API DTOs separate from the internal pool
//! structs. The internal structs can evolve with locking and manifest details;
//! these snapshots stay string/number based and are easy to serialize.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde::Serialize;

use crate::error::{ChunkletError, ChunkletResult};
use crate::io::{AlignedBuf, RawDevice};
use crate::metrics::{PdOperationalState, PoolMetrics};
use crate::pool::{AutoRecoverReport, LdRecoverReport};
use crate::superblock::{SuperblockSlot, SLOT_BYTES};
use crate::types::{
    LdId, PdId, PoolId, RaidLevel, PD_RESERVED_BYTES, SUPERBLOCK_SLOT_A_OFFSET,
    SUPERBLOCK_SLOT_B_OFFSET,
};
use crate::Pool;

#[derive(Clone, Debug)]
pub struct RecoveryOptions {
    pub scrub_first: bool,
}

#[derive(Clone, Debug)]
pub struct RecoveryCycleOptions {
    pub scrub_first: bool,
    pub fail_on_recovery_error: bool,
}

#[derive(Clone, Debug, Serialize)]
pub struct DeviceProbe {
    pub path: String,
    pub opened: bool,
    pub pool_id: Option<String>,
    pub error: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct PoolSnapshot {
    pub pool_id: String,
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
    pub pds: Vec<PdSnapshot>,
    pub lds: Vec<LdSnapshot>,
}

#[derive(Clone, Debug, Serialize)]
pub struct PdSnapshot {
    pub pd_id: String,
    pub pd_seq: u32,
    pub state: String,
    pub drained: bool,
    pub draining: bool,
    pub path: Option<String>,
    pub backend: Option<&'static str>,
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

#[derive(Clone, Debug, Serialize)]
pub struct LdSnapshot {
    pub ld_id: String,
    pub raid_level: String,
    pub set_size: u8,
    pub row_size: u16,
    pub num_rows: u16,
    pub strip_size_bytes: u64,
    pub capacity_bytes: u64,
    pub member_count: usize,
    pub unavailable_members: usize,
    pub bad_members: usize,
    pub failed_members: usize,
    pub draining_members: usize,
    pub drained_members: usize,
}

#[derive(Clone, Debug, Serialize)]
pub struct AutoRecoverSnapshot {
    pub attempted: usize,
    pub recovered: usize,
    pub failed: usize,
    pub lds: Vec<LdRecoverSnapshot>,
}

#[derive(Clone, Debug, Serialize)]
pub struct LdRecoverSnapshot {
    pub ld_id: String,
    pub scrub_mismatches: usize,
    pub scrub_marked_bad: usize,
    pub rebuilt_members: usize,
    pub skipped_rebuild: bool,
    pub error: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct RecoveryCycleSnapshot {
    pub cycle: u64,
    pub probes: Vec<DeviceProbe>,
    pub selected_pool_id: Option<String>,
    pub opened_devices: usize,
    pub configured_devices: usize,
    pub recovery: Option<AutoRecoverSnapshot>,
    pub pool: Option<PoolSnapshot>,
    pub error: Option<String>,
}

pub fn probe_devices(paths: &[PathBuf]) -> Vec<DeviceProbe> {
    paths.iter().map(|path| probe_device(path)).collect()
}

pub fn open_available_pool_devices(
    paths: &[PathBuf],
) -> ChunkletResult<(Vec<RawDevice>, Vec<DeviceProbe>, PoolId)> {
    let mut probes = Vec::with_capacity(paths.len());
    let mut candidates = Vec::with_capacity(paths.len());
    let mut pool_counts: BTreeMap<PoolId, usize> = BTreeMap::new();
    for path in paths {
        match RawDevice::open(path) {
            Ok(raw) => match probe_pool_id(&raw)? {
                Some(pool_id) => {
                    probes.push(DeviceProbe {
                        path: path.display().to_string(),
                        opened: true,
                        pool_id: Some(pool_id.to_string()),
                        error: None,
                    });
                    *pool_counts.entry(pool_id).or_insert(0) += 1;
                    candidates.push((pool_id, raw));
                }
                None => probes.push(DeviceProbe {
                    path: path.display().to_string(),
                    opened: true,
                    pool_id: None,
                    error: Some("no valid pool superblock".into()),
                }),
            },
            Err(e) => probes.push(DeviceProbe {
                path: path.display().to_string(),
                opened: false,
                pool_id: None,
                error: Some(e.to_string()),
            }),
        }
    }

    let Some((pool_id, _)) = pool_counts.iter().max_by_key(|(_, count)| *count) else {
        return Err(ChunkletError::Config(
            "no pool devices could be opened".into(),
        ));
    };
    let pool_id = *pool_id;
    let raws = candidates
        .into_iter()
        .filter_map(|(candidate_pool_id, raw)| (candidate_pool_id == pool_id).then_some(raw))
        .collect::<Vec<_>>();
    if raws.is_empty() {
        return Err(ChunkletError::Config(
            "no pool devices could be opened".into(),
        ));
    }
    Ok((raws, probes, pool_id))
}

pub fn recover_pool_once(pool: &Pool, options: &RecoveryOptions) -> RecoveryCyclePayload {
    let recovery = pool.auto_recover(options.scrub_first);
    RecoveryCyclePayload {
        recovery,
        pool: pool.metrics(),
    }
}

pub fn run_recovery_cycle(
    cycle: u64,
    pool_paths: &[PathBuf],
    options: &RecoveryCycleOptions,
) -> ChunkletResult<RecoveryCycleSnapshot> {
    match open_available_pool_devices(pool_paths) {
        Ok((raws, probes, pool_id)) => {
            let opened_devices = raws.len();
            let pool = match Pool::open_with_missing(raws) {
                Ok(pool) => pool,
                Err(e) => {
                    return Ok(RecoveryCycleSnapshot {
                        cycle,
                        probes,
                        selected_pool_id: Some(pool_id.to_string()),
                        opened_devices,
                        configured_devices: pool_paths.len(),
                        recovery: None,
                        pool: None,
                        error: Some(e.to_string()),
                    });
                }
            };
            let payload = recover_pool_once(
                &pool,
                &RecoveryOptions {
                    scrub_first: options.scrub_first,
                },
            );
            let recovery = AutoRecoverSnapshot::from_report(&payload.recovery);
            if options.fail_on_recovery_error && recovery.failed > 0 {
                return Err(ChunkletError::Invariant(format!(
                    "recover-loop cycle {} failed on {} LDs",
                    cycle, recovery.failed
                )));
            }
            let pool = match payload.pool {
                Ok(metrics) => Some(PoolSnapshot::from_metrics(&metrics)),
                Err(e) => {
                    return Ok(RecoveryCycleSnapshot {
                        cycle,
                        probes,
                        selected_pool_id: Some(pool_id.to_string()),
                        opened_devices,
                        configured_devices: pool_paths.len(),
                        recovery: Some(recovery),
                        pool: None,
                        error: Some(e.to_string()),
                    });
                }
            };
            Ok(RecoveryCycleSnapshot {
                cycle,
                probes,
                selected_pool_id: Some(pool_id.to_string()),
                opened_devices,
                configured_devices: pool_paths.len(),
                recovery: Some(recovery),
                pool,
                error: None,
            })
        }
        Err(e) => Ok(RecoveryCycleSnapshot {
            cycle,
            probes: probe_devices(pool_paths),
            selected_pool_id: None,
            opened_devices: 0,
            configured_devices: pool_paths.len(),
            recovery: None,
            pool: None,
            error: Some(e.to_string()),
        }),
    }
}

pub struct RecoveryCyclePayload {
    pub recovery: AutoRecoverReport,
    pub pool: ChunkletResult<PoolMetrics>,
}

pub fn probe_pool_id(raw: &RawDevice) -> ChunkletResult<Option<PoolId>> {
    if raw.size() < PD_RESERVED_BYTES + SLOT_BYTES as u64 {
        return Ok(None);
    }
    let tail_base = raw.size() - PD_RESERVED_BYTES;
    for offset in [
        SUPERBLOCK_SLOT_A_OFFSET,
        SUPERBLOCK_SLOT_B_OFFSET,
        tail_base + SUPERBLOCK_SLOT_A_OFFSET,
        tail_base + SUPERBLOCK_SLOT_B_OFFSET,
    ] {
        let mut buf = AlignedBuf::new(SLOT_BYTES)?;
        if raw.read_at(buf.as_mut_slice(), offset).is_err() {
            continue;
        }
        if let Ok(slot) = SuperblockSlot::decode(buf.as_slice()) {
            return Ok(Some(slot.pool_id));
        }
    }
    Ok(None)
}

fn probe_device(path: &Path) -> DeviceProbe {
    match RawDevice::open(path) {
        Ok(raw) => match probe_pool_id(&raw) {
            Ok(pool_id) => DeviceProbe {
                path: path.display().to_string(),
                opened: true,
                pool_id: pool_id.map(|id| id.to_string()),
                error: pool_id.is_none().then(|| "no valid pool superblock".into()),
            },
            Err(e) => DeviceProbe {
                path: path.display().to_string(),
                opened: true,
                pool_id: None,
                error: Some(e.to_string()),
            },
        },
        Err(e) => DeviceProbe {
            path: path.display().to_string(),
            opened: false,
            pool_id: None,
            error: Some(e.to_string()),
        },
    }
}

impl PoolSnapshot {
    pub fn from_metrics(m: &PoolMetrics) -> Self {
        Self {
            pool_id: m.pool_id.to_string(),
            pd_count: m.pd_count,
            healthy_pds: m.healthy_pds,
            failed_pds: m.failed_pds,
            draining_pds: m.draining_pds,
            drained_pds: m.drained_pds,
            ld_count: m.ld_count,
            cpg_count: m.cpg_count,
            raw_bytes: m.raw_bytes,
            user_bytes: m.user_bytes,
            allocatable_bytes: m.allocatable_bytes,
            used_bytes: m.used_bytes,
            spare_bytes: m.spare_bytes,
            bad_bytes: m.bad_bytes,
            migrating_bytes: m.migrating_bytes,
            total_chunklets: m.total_chunklets,
            free_chunklets: m.free_chunklets,
            used_chunklets: m.used_chunklets,
            spare_chunklets: m.spare_chunklets,
            bad_chunklets: m.bad_chunklets,
            migrating_chunklets: m.migrating_chunklets,
            last_reconciliation_count: m.last_reconciliation_count,
            pds: m.pds.iter().map(PdSnapshot::from_metrics).collect(),
            lds: m.lds.iter().map(LdSnapshot::from_metrics).collect(),
        }
    }
}

impl PdSnapshot {
    fn from_metrics(m: &crate::metrics::PdMetrics) -> Self {
        Self {
            pd_id: m.pd_id.to_string(),
            pd_seq: m.pd_seq,
            state: pd_state_label(m.state).into(),
            drained: m.drained,
            draining: m.draining,
            path: m.path.as_ref().map(|p| p.display().to_string()),
            backend: m.backend,
            manifest_gen: m.manifest_gen,
            size_bytes: m.size_bytes,
            user_bytes: m.user_bytes,
            allocatable_bytes: m.allocatable_bytes,
            used_bytes: m.used_bytes,
            spare_bytes: m.spare_bytes,
            bad_bytes: m.bad_bytes,
            migrating_bytes: m.migrating_bytes,
            total_chunklets: m.total_chunklets,
            free_chunklets: m.free_chunklets,
            used_chunklets: m.used_chunklets,
            spare_chunklets: m.spare_chunklets,
            bad_chunklets: m.bad_chunklets,
            migrating_chunklets: m.migrating_chunklets,
        }
    }
}

impl LdSnapshot {
    fn from_metrics(m: &crate::metrics::LdMetrics) -> Self {
        Self {
            ld_id: m.ld_id.to_string(),
            raid_level: raid_label(m.raid_level).into(),
            set_size: m.set_size,
            row_size: m.row_size,
            num_rows: m.num_rows,
            strip_size_bytes: m.strip_size_bytes,
            capacity_bytes: m.capacity_bytes,
            member_count: m.member_count,
            unavailable_members: m.unavailable_members,
            bad_members: m.bad_members,
            failed_members: m.failed_members,
            draining_members: m.draining_members,
            drained_members: m.drained_members,
        }
    }
}

impl AutoRecoverSnapshot {
    pub fn from_report(report: &AutoRecoverReport) -> Self {
        Self {
            attempted: report.attempted,
            recovered: report.recovered,
            failed: report.failed,
            lds: report
                .lds
                .iter()
                .map(LdRecoverSnapshot::from_report)
                .collect(),
        }
    }
}

impl LdRecoverSnapshot {
    fn from_report(report: &LdRecoverReport) -> Self {
        Self {
            ld_id: report.ld_id.to_string(),
            scrub_mismatches: report.scrub_mismatches,
            scrub_marked_bad: report.scrub_marked_bad,
            rebuilt_members: report.rebuilt_members,
            skipped_rebuild: report.skipped_rebuild,
            error: report.error.clone(),
        }
    }
}

pub fn pd_state_label(state: PdOperationalState) -> &'static str {
    match state {
        PdOperationalState::Healthy => "healthy",
        PdOperationalState::Failed => "failed",
        PdOperationalState::Draining => "draining",
        PdOperationalState::Drained => "drained",
    }
}

pub fn raid_label(raid: RaidLevel) -> &'static str {
    match raid {
        RaidLevel::Plain => "plain",
        RaidLevel::Mirror => "mirror",
        RaidLevel::Raid0 => "raid0",
        RaidLevel::Raid5 => "raid5",
        RaidLevel::Raid6 => "raid6",
    }
}

pub fn parse_pd_id(s: &str) -> ChunkletResult<PdId> {
    let parsed =
        uuid::Uuid::parse_str(s).map_err(|e| ChunkletError::Config(format!("bad uuid: {}", e)))?;
    Ok(PdId::from_bytes(*parsed.as_bytes()))
}

pub fn parse_ld_id(s: &str) -> ChunkletResult<LdId> {
    let parsed =
        uuid::Uuid::parse_str(s).map_err(|e| ChunkletError::Config(format!("bad uuid: {}", e)))?;
    Ok(LdId::from_bytes(*parsed.as_bytes()))
}
