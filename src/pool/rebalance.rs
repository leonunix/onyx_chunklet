//! Online data rebalance — migrate healthy members from over-full to under-full
//! PDs until per-PD used-skew converges, reusing the online-rebuild 3-phase
//! shadow machinery with a COPY backfill (the source member is healthy).
//!
//! # Why this reuses rebuild
//!
//! A rebalance move is a rebuild whose source is alive: instead of
//! reconstructing the target position from the set, Phase B copies the source
//! chunklet byte-for-byte into the shadow. The foreground `write_forward` path
//! is IDENTICAL — it mirrors the just-written strip of the moving position to
//! the shadow, and for both mirror and RAID5/6 those bytes equal what lands on
//! the source member (verified: full-stripe writes the buf slice / P / Q; RMW's
//! `new_strips[pos]` merges old+new so it equals the source's post-write
//! content). So only the backfill differs; the hot path is untouched.
//!
//! The `rebuild` cell doubles as the per-LD "migration in flight" mutex: both
//! `rebuild_ld_online` and this refuse to start if one is already live, so a
//! rebalance move never races a failover rebuild on the same LD.
//!
//! # Concurrency / crash-safety
//!
//! Same lock order as rebuild (`manifest_lock` → `io_lock` → ascending `PdId`
//! commits). Shadows are marked `Migrating`; a crash mid-move leaves them
//! reclaimed by `reclaim_orphan_migrating` at open with the descriptor still
//! naming the source → data intact. One move = one `commit_rebuild` = one epoch
//! bump (onyx's `with_stale_retry` absorbs it); moves run one at a time so a
//! single in-flight foreground IO can never approach the stale-refresh cap.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use crate::error::{ChunkletError, ChunkletResult};
use crate::ld::descriptor::LdList;
use crate::ld::{LdMirror, LdRaid5, LdRaid6, ReconstructEngine};
use crate::pd::PhysicalDisk;
use crate::pool::{PdHealth, Pool, RebuildProgress, SetRebuild, ShadowTarget};
use crate::types::{ChunkletState, LdId, LdMember, LdRole, PdId, RaidLevel};

/// Backfill window per stripe-lock acquisition — matches rebuild's
/// `REBUILD_BATCH_BYTES`: small on purpose so a batch holds the shared stripe
/// lock for ~one range-IO round-trip and never starves foreground writes.
const REBALANCE_BATCH_BYTES: u64 = 256 * 1024;

#[derive(Clone, Debug)]
pub struct RebalanceOptions {
    /// Stop once worst-case per-PD used skew (`max - min`) is within this
    /// percent of the mean healthy-PD used count.
    pub target_skew_pct: f64,
    /// Hard cap on committed moves per invocation (bounded work budget).
    pub max_moves: usize,
}

impl Default for RebalanceOptions {
    fn default() -> Self {
        Self {
            target_skew_pct: 20.0,
            max_moves: 256,
        }
    }
}

#[derive(Clone, Debug)]
pub struct RebalanceReport {
    pub moves_committed: usize,
    pub skew_before: u32,
    pub skew_after: u32,
    /// True ONLY if the planner declared the pool balanced (skew within target).
    /// False if it stopped for another reason (`max_moves` hit, or `stuck`).
    pub converged: bool,
    /// True if skew still exceeds target but NO legal skew-reducing move exists
    /// (e.g. every under-full PD has only Spare/no Free, or set-PD uniqueness /
    /// Plain-Raid0 blocks every candidate). Distinct from `converged` so an
    /// operator never reads a still-skewed pool as "balanced".
    pub stuck: bool,
}

#[derive(Clone, Copy, Debug)]
struct Move {
    ld_id: LdId,
    flat_index: usize,
    from_pd: PdId,
    to_pd: PdId,
    to_idx: u32,
}

/// Outcome of one planning step.
enum PlanOutcome {
    /// A concrete skew-reducing move to execute.
    Move(Move),
    /// Skew is within target (or the pool is too small to balance).
    Balanced,
    /// Skew exceeds target but no legal skew-reducing move exists.
    Stuck,
}

impl Pool {
    /// Rebalance data across PDs until per-PD used-skew converges below
    /// `opts.target_skew_pct` or `opts.max_moves` moves are committed. Online:
    /// each move keeps foreground IO flowing (write-forward), bounded by the
    /// 256 KiB backfill window and one-move-at-a-time commit.
    pub fn rebalance(&self, opts: RebalanceOptions) -> ChunkletResult<RebalanceReport> {
        let skew_before = self.metrics()?.used_skew_chunklets;
        let mut moves_committed = 0usize;
        let mut converged = false;
        let mut stuck = false;
        while moves_committed < opts.max_moves {
            // Re-plan from fresh per-PD counts each iteration so projections
            // never go stale (a concurrent failover could have changed health).
            match self.plan_one_move(opts.target_skew_pct) {
                PlanOutcome::Balanced => {
                    converged = true;
                    break;
                }
                PlanOutcome::Stuck => {
                    stuck = true;
                    break;
                }
                PlanOutcome::Move(mv) => {
                    self.migrate_one_member(mv)?;
                    moves_committed += 1;
                }
            }
        }
        let skew_after = self.metrics()?.used_skew_chunklets;
        Ok(RebalanceReport {
            moves_committed,
            skew_before,
            skew_after,
            converged,
            stuck,
        })
    }

    /// Pick one skew-reducing, set-safe move, or `None` when the pool is
    /// balanced / no legal move improves it. Reads only; the move is
    /// re-validated under lock in `migrate_one_member`.
    fn plan_one_move(&self, target_skew_pct: f64) -> PlanOutcome {
        let s = self.state.read();
        // Per-PD used + free over healthy, non-draining, present PDs only.
        let mut used: BTreeMap<PdId, u32> = BTreeMap::new();
        let mut first_free: BTreeMap<PdId, u32> = BTreeMap::new();
        for (pd_id, pd) in &s.pds {
            if s.pd_health.get(pd_id) != Some(&PdHealth::Healthy) {
                continue;
            }
            if s.draining.contains(pd_id) {
                continue;
            }
            let (_, bitmap, _) = pd.snapshot();
            used.insert(*pd_id, bitmap.count(ChunkletState::Used));
            for idx in 0..bitmap.len() {
                if matches!(bitmap.get(idx), Ok(ChunkletState::Free)) {
                    first_free.insert(*pd_id, idx);
                    break;
                }
            }
        }
        if used.len() < 2 {
            return PlanOutcome::Balanced;
        }
        let vals: Vec<u32> = used.values().copied().collect();
        let min = *vals.iter().min().unwrap();
        let max = *vals.iter().max().unwrap();
        let mean = vals.iter().map(|&u| u as f64).sum::<f64>() / vals.len() as f64;
        let skew_pct = if mean > 0.0 {
            (max - min) as f64 / mean * 100.0
        } else {
            0.0
        };
        // Balanced, or moving one chunklet can't help (diff < 2 would just
        // ping-pong the max/min between the two PDs).
        if skew_pct <= target_skew_pct || max - min < 2 {
            return PlanOutcome::Balanced;
        }
        // dst: least-used first, must have a free chunklet. src: most-used first.
        let mut dst_order: Vec<PdId> = used.keys().copied().collect();
        dst_order.sort_by_key(|pd| (used[pd], *pd));
        let mut src_order: Vec<PdId> = used.keys().copied().collect();
        src_order.sort_by_key(|pd| (std::cmp::Reverse(used[pd]), *pd));

        for dst in &dst_order {
            let Some(&to_idx) = first_free.get(dst) else {
                continue;
            };
            for src in &src_order {
                if src == dst || used[src] < used[dst] + 2 {
                    continue;
                }
                if let Some((ld_id, flat_index)) = find_movable_member(&s.ld_list, *src, *dst) {
                    return PlanOutcome::Move(Move {
                        ld_id,
                        flat_index,
                        from_pd: *src,
                        to_pd: *dst,
                        to_idx,
                    });
                }
            }
        }
        PlanOutcome::Stuck
    }

    /// Execute one member move via the rebuild 3-phase machinery with a copy
    /// backfill. All state is re-validated under `manifest_lock` (the plan is a
    /// hint; the pool may have changed since).
    fn migrate_one_member(&self, mv: Move) -> ChunkletResult<()> {
        // ---------------------------- Phase A ----------------------------
        let commit_a = self.manifest_lock.lock();
        let runtime = self
            .state
            .read()
            .ld_runtime
            .get(&mv.ld_id)
            .cloned()
            .ok_or_else(|| ChunkletError::Invariant(format!("LD {} runtime not found", mv.ld_id)))?;
        let io_a = runtime.io_lock.write();
        if runtime.rebuild.read().is_some() {
            return Err(ChunkletError::Invariant(format!(
                "LD {} already has an online migration (rebuild/rebalance) in flight",
                mv.ld_id
            )));
        }

        let desc = self
            .find_ld(mv.ld_id)
            .ok_or_else(|| ChunkletError::Invariant(format!("LD {} not found", mv.ld_id)))?;
        let pd_health = self.state.read().pd_health.clone();
        let pds_snapshot: BTreeMap<PdId, Arc<PhysicalDisk>> = self
            .state
            .read()
            .pds
            .iter()
            .filter(|(pd_id, _)| pd_health.get(pd_id) != Some(&PdHealth::Failed))
            .map(|(id, pd)| (*id, pd.clone()))
            .collect();

        let old_member = desc.members[mv.flat_index];
        if old_member.pd != mv.from_pd {
            return Err(ChunkletError::Invariant(
                "rebalance: member moved since planning".into(),
            ));
        }
        let src_pd = pds_snapshot.get(&mv.from_pd).cloned().ok_or_else(|| {
            ChunkletError::Invariant(format!("rebalance src PD {} not healthy", mv.from_pd))
        })?;
        let dst_pd = pds_snapshot.get(&mv.to_pd).cloned().ok_or_else(|| {
            ChunkletError::Invariant(format!("rebalance dst PD {} not healthy", mv.to_pd))
        })?;
        // A concurrent drain_pd could have marked the destination draining after
        // plan_one_move filtered it out. Never rebalance a live member ONTO a
        // draining PD (rebuild_ld_online strips draining PDs from its targets for
        // the same reason). Re-checked again in Phase C for a drain that starts
        // during the long Phase B copy.
        if self.state.read().draining.contains(&mv.to_pd) {
            return Err(ChunkletError::Invariant(format!(
                "rebalance dst PD {} is draining; abort move",
                mv.to_pd
            )));
        }

        let set_size = desc.set_size as usize;
        let set_idx = mv.flat_index / set_size;
        let base = set_idx * set_size;
        let pos_in_set = mv.flat_index % set_size;
        // Re-check set-PD uniqueness (no runtime assert enforces it; the
        // allocator only proves it by construction, so a hand-placed move must
        // verify it itself).
        for p in 0..set_size {
            if base + p != mv.flat_index && desc.members[base + p].pd == mv.to_pd {
                return Err(ChunkletError::Invariant(format!(
                    "rebalance of LD {} would break set-PD uniqueness (dst {} already in set {})",
                    mv.ld_id, mv.to_pd, set_idx
                )));
            }
        }
        // Confirm the planned dst chunklet is still Free.
        {
            let (_, bitmap, _) = dst_pd.snapshot();
            if !matches!(bitmap.get(mv.to_idx), Ok(ChunkletState::Free)) {
                return Err(ChunkletError::Invariant(format!(
                    "rebalance dst chunklet {}#{} no longer Free",
                    mv.to_pd, mv.to_idx
                )));
            }
        }

        let role = old_member.role;
        let new_gen = old_member.generation.wrapping_add(1);
        let mut new_desc = desc.clone();
        new_desc.members[mv.flat_index] = LdMember {
            pd: mv.to_pd,
            chunklet_index: mv.to_idx,
            role,
            generation: new_gen,
        };

        // Write the shadow header + mark it Migrating (invisible to the
        // allocator; reclaimed at open if we crash before Phase C).
        self.write_header(&dst_pd, mv.to_idx, new_desc.id, role, new_gen)?;
        {
            let to_idx = mv.to_idx;
            dst_pd.commit_manifest(move |_body, bitmap| {
                bitmap.set(to_idx, ChunkletState::Migrating)?;
                Ok(())
            })?;
        }

        let n_sets = (desc.row_size as usize) * (desc.num_rows as usize);
        let mut targets_by_set: Vec<Option<SetRebuild>> = (0..n_sets).map(|_| None).collect();
        targets_by_set[set_idx] = Some(SetRebuild {
            cursor: AtomicU64::new(0),
            shadows: vec![ShadowTarget {
                pos_in_set,
                pd: dst_pd.clone(),
                chunklet_index: mv.to_idx,
                copy_source: Some((src_pd.clone(), old_member.chunklet_index)),
            }],
        });
        let progress = Arc::new(RebuildProgress {
            targets_by_set,
            aborted: AtomicBool::new(false),
        });
        *runtime.rebuild.write() = Some(progress.clone());

        drop(io_a);
        drop(commit_a);

        // ---------------------------- Phase B ----------------------------
        // Copy the healthy source member into the shadow, batch by batch under
        // the SHARED stripe lock, advancing the per-set cursor. Foreground
        // writes below the cursor write-forward to the shadow (unchanged path).
        {
            let _io = runtime.io_lock.read();
            let engine: Box<dyn ReconstructEngine> = match desc.raid_level {
                RaidLevel::Mirror => {
                    Box::new(LdMirror::open_with_health(desc.clone(), &pds_snapshot, &pd_health)?)
                }
                RaidLevel::Raid5 => {
                    Box::new(LdRaid5::open_with_health(desc.clone(), &pds_snapshot, &pd_health)?)
                }
                RaidLevel::Raid6 => {
                    Box::new(LdRaid6::open_with_health(desc.clone(), &pds_snapshot, &pd_health)?)
                }
                other => {
                    *runtime.rebuild.write() = None;
                    return Err(ChunkletError::Unsupported(format!(
                        "rebalance not supported for {:?} (no redundancy / write-forward)",
                        other
                    )));
                }
            };
            let strip_bytes = engine.strip_bytes();
            let stripes = engine.stripes_per_chunklet();
            let batch_stripes = std::cmp::max(1, REBALANCE_BATCH_BYTES / strip_bytes);
            let mut buf = vec![0u8; (batch_stripes * strip_bytes) as usize];
            let sr = progress.targets_by_set[set_idx]
                .as_ref()
                .expect("set_idx target installed above");
            let shadow = &sr.shadows[0];
            // The shadow is self-describing: its copy_source names the healthy
            // source member to copy from (set in Phase A).
            let (copy_src_pd, copy_src_idx) = shadow
                .copy_source
                .as_ref()
                .expect("rebalance shadow must carry a copy_source");

            let mut s = 0u64;
            while s < stripes {
                if progress.aborted.load(Ordering::Relaxed) {
                    break;
                }
                let n = std::cmp::min(batch_stripes, stripes - s);
                let range_len = (n * strip_bytes) as usize;
                let base_off = s * strip_bytes;
                // SAME stripe-lock keys foreground writes take (rebuild uses the
                // identical `(set_idx<<32)|stripe` scheme), so the cursor / below-
                // cursor write-forward interlock is exactly as for a rebuild.
                let keys: Vec<u64> = (s..s + n).map(|st| ((set_idx as u64) << 32) | st).collect();
                let _guards = runtime.stripe_locks.write_keys(&keys);
                // Pure byte copy of the healthy source member's user region — no
                // parity math, no reconstruct, no full-set read.
                if let Err(e) =
                    copy_src_pd.read_chunklet_user(*copy_src_idx, base_off, &mut buf[..range_len])
                {
                    tracing::error!(
                        "rebalance: source read failed (ld {} set {}): {} — aborting",
                        mv.ld_id,
                        set_idx,
                        e
                    );
                    progress.aborted.store(true, Ordering::Relaxed);
                    break;
                }
                if let Err(e) =
                    shadow
                        .pd
                        .write_chunklet_user(shadow.chunklet_index, base_off, &buf[..range_len])
                {
                    tracing::error!(
                        "rebalance: shadow write failed (ld {} set {}): {} — aborting",
                        mv.ld_id,
                        set_idx,
                        e
                    );
                    progress.aborted.store(true, Ordering::Relaxed);
                    break;
                }
                sr.cursor.store(s + n, Ordering::Release);
                s += n;
            }
            if !progress.aborted.load(Ordering::Relaxed) {
                shadow.pd.sync()?;
            }
        }

        // ---------------------------- Phase C ----------------------------
        let _commit_c = self.manifest_lock.lock();
        let _io_c = runtime.io_lock.write();
        // Abort if the backfill failed OR the destination started draining during
        // Phase B — a concurrent drain must not end with a live member on a
        // draining PD. Reclaim the Migrating shadow and clear the cell.
        let dst_draining = self.state.read().draining.contains(&mv.to_pd);
        if progress.aborted.load(Ordering::Relaxed) || dst_draining {
            let to_idx = mv.to_idx;
            let _ = dst_pd.commit_manifest(move |_body, bitmap| {
                bitmap.set(to_idx, ChunkletState::Free)?;
                Ok(())
            });
            *runtime.rebuild.write() = None;
            let reason = if dst_draining {
                "destination PD started draining"
            } else {
                "copy/backfill IO failure"
            };
            return Err(ChunkletError::Invariant(format!(
                "rebalance of LD {} aborted: {}",
                mv.ld_id, reason
            )));
        }
        let mut new_alloc_by_pd: BTreeMap<PdId, Vec<(u32, LdRole, u8)>> = BTreeMap::new();
        new_alloc_by_pd.insert(mv.to_pd, vec![(mv.to_idx, role, new_gen)]);
        let mut freed_by_pd: BTreeMap<PdId, Vec<u32>> = BTreeMap::new();
        freed_by_pd.insert(mv.from_pd, vec![old_member.chunklet_index]);
        // Clear the cell whether or not the commit succeeds: a mid-loop cross-PD
        // commit failure otherwise returns via `?` BEFORE the clear, leaking the
        // cell and tripping the per-LD "migration in flight" gate forever. A torn
        // cross-PD commit is reconciled at next open (forward_reconcile_bitmaps +
        // reclaim_orphan_migrating reclaims the Migrating shadow).
        let committed = self.commit_rebuild(&new_desc, &new_alloc_by_pd, &freed_by_pd, &pds_snapshot);
        *runtime.rebuild.write() = None;
        committed?;
        runtime.bump();
        Ok(())
    }
}

/// Find a member on `src` in a redundant, striped LD whose set has no member on
/// `dst` (set-PD uniqueness must survive the move). Plain / Raid0 are skipped:
/// they have no `write_forward`, so an online copy could drift from concurrent
/// foreground writes.
fn find_movable_member(ld_list: &LdList, src: PdId, dst: PdId) -> Option<(LdId, usize)> {
    for ld in &ld_list.lds {
        if matches!(ld.raid_level, RaidLevel::Plain | RaidLevel::Raid0) {
            continue;
        }
        let set_size = ld.set_size as usize;
        for (flat_index, m) in ld.members.iter().enumerate() {
            if m.pd != src {
                continue;
            }
            let base = (flat_index / set_size) * set_size;
            let collides = (0..set_size).any(|p| ld.members[base + p].pd == dst);
            if !collides {
                return Some((ld.id, flat_index));
            }
        }
    }
    None
}
