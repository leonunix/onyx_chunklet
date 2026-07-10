//! Inline-degrade accounting for RAID writes.
//!
//! When a member PD returns a runtime IO error mid-write, the surviving members
//! of a redundant LD have ALREADY written durably — every `IoBackend` submits
//! and waits on every leg before returning (see
//! [`crate::io::backend::submit_writes_detailed`]). This module decides, per
//! redundancy group (one stripe segment / set), whether the observed member
//! failures are within that group's tolerance. If so the write is a *degraded
//! success* (survivors hold the data; the missing copies are restored by
//! rebuild) and the failed members are reported as [`SuspectMember`]s for fast
//! isolation; if not, the error is surfaced exactly as the old all-or-nothing
//! path did (genuine data-loss risk).
//!
//! # Concurrency
//! A pure function over already-collected per-op results: no locks, no IO.

use std::collections::{BTreeMap, BTreeSet};

use crate::error::{ChunkletError, ChunkletResult};
use crate::io::backend::StripWrite;
use crate::types::PdId;

/// A member that failed a runtime write and should be isolated (`mark_pd_failed`)
/// so subsequent IO reopens degraded and a rebuild-to-spare restores redundancy.
/// Public because it crosses the crate boundary via [`crate::Pool::suspect_events`]
/// (onyx's isolation reactor consumes it).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SuspectMember {
    pub pd_id: PdId,
    pub chunklet_index: u32,
}

/// Decide whether a batch of per-op write results can be absorbed as a degraded
/// success. `results[i]` corresponds to `ops[i]`. `group_of[i]` is the COMPACT
/// redundancy-group index (`0..max_fail.len()`) of `ops[i]` — one group per
/// stripe segment / set. `max_fail[g]` is how many runtime member failures group
/// `g` tolerates, already net of members lost at open time (mirror =
/// `copies_in_group − 1`; R5 = `1 − open_failures`; R6 = `2 − open_failures`).
///
/// - `Ok(suspects)`: every group is within budget; the write is durable on the
///   survivors. `suspects` is the deduped set of failed PDs to isolate.
/// - `Err`: some group exceeded its budget (real data loss) — the caller must
///   surface the error just like the pre-inline-degrade path.
pub(crate) fn absorb_degraded(
    ops: &[StripWrite<'_>],
    results: &[ChunkletResult<()>],
    group_of: &[u32],
    max_fail: &[u32],
) -> ChunkletResult<Vec<SuspectMember>> {
    debug_assert_eq!(ops.len(), results.len(), "results must align with ops");
    debug_assert_eq!(ops.len(), group_of.len(), "group_of must align with ops");

    let mut fail_count: BTreeMap<u32, u32> = BTreeMap::new();
    let mut first_fail_idx: BTreeMap<u32, usize> = BTreeMap::new();
    let mut suspects: Vec<SuspectMember> = Vec::new();
    let mut seen: BTreeSet<PdId> = BTreeSet::new();

    for (i, res) in results.iter().enumerate() {
        if res.is_err() {
            let g = group_of[i];
            *fail_count.entry(g).or_insert(0) += 1;
            first_fail_idx.entry(g).or_insert(i);
            let pd_id = ops[i].pd.pd_id();
            if seen.insert(pd_id) {
                suspects.push(SuspectMember {
                    pd_id,
                    chunklet_index: ops[i].chunklet_index,
                });
            }
        }
    }

    for (g, count) in &fail_count {
        let budget = max_fail.get(*g as usize).copied().unwrap_or(0);
        if *count > budget {
            // Over budget in this group ⇒ not enough survivors to reconstruct
            // ⇒ surface the underlying error unchanged (no absorb).
            let idx = first_fail_idx[g];
            let underlying = match &results[idx] {
                Err(e) => e.to_string(),
                Ok(()) => unreachable!("only failing ops are counted"),
            };
            return Err(ChunkletError::Io(std::io::Error::other(format!(
                "write redundancy exceeded in group {}: {} member failure(s) > budget {} (first: {})",
                g, count, budget, underlying
            ))));
        }
    }

    Ok(suspects)
}

/// True for a runtime read error from a member the LD still believes healthy —
/// the read-side analog of a write EIO. The reconstruct-on-read path absorbs
/// these (rebuilds the strip from surviving redundancy within budget) and emits
/// a [`SuspectMember`] so a fault surfaced only by reads still triggers fast
/// isolation.
///
/// Absorb `{Device, Io}`: `Device` is what the default `SyncBackend` and every
/// serial direct-PD read surface (`RawDevice::read_loop` / `PhysicalDisk`
/// read-fault hook), `Io` is what `UringBackend` / the write fault harness
/// produce. NEVER absorb `Crc` (silent corruption MUST surface), nor structural
/// errors (`Invariant` / `Format` / `WriteRedundancyExceeded` / `NoSpace` /
/// `Unsupported` / `PoolLocked` / `PoolMismatch` / `NoValidSuperblock` /
/// `Config`) — those are bugs or capacity walls, not a recoverable member IO
/// fault, and reconstructing over them would mask the real defect.
pub(crate) fn is_runtime_read_fault(e: &ChunkletError) -> bool {
    matches!(e, ChunkletError::Device { .. } | ChunkletError::Io(_))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pd::PhysicalDisk;
    use crate::types::PoolId;
    use std::sync::Arc;
    use tempfile::TempDir;

    fn test_pd(dir: &TempDir, name: &str) -> Arc<PhysicalDisk> {
        let raw =
            crate::io::RawDevice::open_or_create(&dir.path().join(name), 4 * 1024 * 1024 * 1024)
                .unwrap();
        PhysicalDisk::init(
            raw,
            PoolId::new_v4(),
            PdId::new_v4(),
            0,
            1,
            vec![],
            0,
            vec![],
            vec![],
        )
        .unwrap()
    }

    fn sw<'a>(pd: &Arc<PhysicalDisk>, buf: &'a [u8]) -> StripWrite<'a> {
        StripWrite {
            pd: pd.clone(),
            chunklet_index: 0,
            in_chunklet_off: 0,
            data: buf,
        }
    }

    #[test]
    fn all_ok_yields_no_suspects() {
        let dir = TempDir::new().unwrap();
        let pd0 = test_pd(&dir, "pd0");
        let pd1 = test_pd(&dir, "pd1");
        let buf = [0u8; 4096];
        let ops = vec![sw(&pd0, &buf), sw(&pd1, &buf)];
        let results = vec![Ok(()), Ok(())];
        let suspects = absorb_degraded(&ops, &results, &[0, 0], &[1]).unwrap();
        assert!(suspects.is_empty());
    }

    #[test]
    fn mirror_one_leg_failed_is_absorbed_and_reports_suspect() {
        let dir = TempDir::new().unwrap();
        let pd0 = test_pd(&dir, "pd0");
        let pd1 = test_pd(&dir, "pd1");
        let buf = [0u8; 4096];
        let ops = vec![sw(&pd0, &buf), sw(&pd1, &buf)];
        // pd1's leg failed; the 2-copy group tolerates 1 failure.
        let results = vec![
            Ok(()),
            Err(ChunkletError::Io(std::io::Error::from_raw_os_error(5))),
        ];
        let suspects = absorb_degraded(&ops, &results, &[0, 0], &[1]).unwrap();
        assert_eq!(suspects.len(), 1);
        assert_eq!(suspects[0].pd_id, pd1.pd_id());
    }

    #[test]
    fn mirror_all_copies_failed_exceeds_budget() {
        let dir = TempDir::new().unwrap();
        let pd0 = test_pd(&dir, "pd0");
        let pd1 = test_pd(&dir, "pd1");
        let buf = [0u8; 4096];
        let ops = vec![sw(&pd0, &buf), sw(&pd1, &buf)];
        let results = vec![
            Err(ChunkletError::Io(std::io::Error::from_raw_os_error(5))),
            Err(ChunkletError::Io(std::io::Error::from_raw_os_error(5))),
        ];
        // 2 failures in a group that tolerates 1 ⇒ Err.
        assert!(absorb_degraded(&ops, &results, &[0, 0], &[1]).is_err());
    }

    #[test]
    fn suspects_dedup_across_groups_same_pd() {
        let dir = TempDir::new().unwrap();
        let pd0 = test_pd(&dir, "pd0");
        let pd1 = test_pd(&dir, "pd1");
        let buf = [0u8; 4096];
        // Two stripe segments (groups 0 and 1), each a 2-copy mirror on the same
        // two PDs. pd1 fails in BOTH segments — should surface as ONE suspect.
        let ops = vec![
            sw(&pd0, &buf),
            sw(&pd1, &buf),
            sw(&pd0, &buf),
            sw(&pd1, &buf),
        ];
        let results = vec![
            Ok(()),
            Err(ChunkletError::Io(std::io::Error::from_raw_os_error(5))),
            Ok(()),
            Err(ChunkletError::Io(std::io::Error::from_raw_os_error(5))),
        ];
        let suspects = absorb_degraded(&ops, &results, &[0, 0, 1, 1], &[1, 1]).unwrap();
        assert_eq!(
            suspects.len(),
            1,
            "same PD failing in two groups dedups to one suspect"
        );
        assert_eq!(suspects[0].pd_id, pd1.pd_id());
    }

    #[test]
    fn one_group_over_budget_fails_whole_batch() {
        let dir = TempDir::new().unwrap();
        let pd0 = test_pd(&dir, "pd0");
        let pd1 = test_pd(&dir, "pd1");
        let buf = [0u8; 4096];
        // group 0 within budget (1 fail), group 1 over budget (2 fails).
        let ops = vec![
            sw(&pd0, &buf),
            sw(&pd1, &buf),
            sw(&pd0, &buf),
            sw(&pd1, &buf),
        ];
        let results = vec![
            Ok(()),
            Err(ChunkletError::Io(std::io::Error::from_raw_os_error(5))),
            Err(ChunkletError::Io(std::io::Error::from_raw_os_error(5))),
            Err(ChunkletError::Io(std::io::Error::from_raw_os_error(5))),
        ];
        assert!(absorb_degraded(&ops, &results, &[0, 0, 1, 1], &[1, 1]).is_err());
    }
}
