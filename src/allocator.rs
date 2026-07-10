//! Cross-PD chunklet allocator.
//!
//! # Inputs / outputs
//!
//! Pure function: takes a snapshot of free chunklets per PD plus an
//! allocation request, returns a `Plan` (or an error). It does **not** mutate
//! pool state. Callers apply the plan via per-PD manifest commits.
//!
//! # Algorithm
//!
//! `HaDomain::Pd` preserves the original global balancing: every RAID set
//! picks `set_size` distinct PDs by descending free-count. `HaDomain::Numa`
//! first picks a NUMA node for the whole row, then allocates every set in that
//! row from PDs on that node. This keeps latency-sensitive stripes from
//! crossing sockets while still rotating rows across nodes as capacity allows.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use crate::error::{ChunkletError, ChunkletResult};
use crate::types::{HaDomain, LdMember, LdRole, PdId};

/// Snapshot of one PD's free chunklets, sorted ascending.
#[derive(Clone, Debug)]
pub struct PdFreeView {
    pub pd: PdId,
    pub numa_node: Option<u16>,
    pub free_indices: Vec<u32>,
}

#[derive(Clone, Debug)]
pub struct AllocRequest {
    pub set_size: u8,
    pub row_size: u16,
    pub num_rows: u16,
    /// Role for each member, in row-major / set-major order. Length must
    /// equal `set_size * row_size * num_rows`.
    pub role_assignments: Vec<LdRole>,
    pub ha_domain: HaDomain,
}

impl AllocRequest {
    pub fn total_members(&self) -> usize {
        (self.set_size as usize) * (self.row_size as usize) * (self.num_rows as usize)
    }

    fn validate(&self) -> ChunkletResult<()> {
        if self.set_size == 0 || self.row_size == 0 || self.num_rows == 0 {
            return Err(ChunkletError::Invariant(format!(
                "invalid AllocRequest dims: set={} row={} rows={}",
                self.set_size, self.row_size, self.num_rows
            )));
        }
        if self.role_assignments.len() != self.total_members() {
            return Err(ChunkletError::Invariant(format!(
                "role_assignments len {} != expected {}",
                self.role_assignments.len(),
                self.total_members()
            )));
        }
        if !self.ha_domain.is_supported() {
            return Err(ChunkletError::Unsupported(format!(
                "HA domain {:?}",
                self.ha_domain
            )));
        }
        Ok(())
    }
}

/// Result of `plan_alloc`: a list of members in the SAME order as
/// `request.role_assignments`. Caller maps these directly into an
/// `LdDescriptor.members` field.
#[derive(Clone, Debug)]
pub struct Plan {
    pub members: Vec<LdMember>,
}

pub fn plan_alloc(request: &AllocRequest, pd_views: Vec<PdFreeView>) -> ChunkletResult<Plan> {
    request.validate()?;

    let mut state = AllocState::new(pd_views);
    let total = request.total_members();
    let mut members = Vec::with_capacity(total);

    let total_free = state.total_free();
    if total_free < total {
        return Err(ChunkletError::Config(format!(
            "alloc: need {} chunklets, pool has only {} free",
            total, total_free
        )));
    }
    let usable_pds = state.usable_pds(None);
    if usable_pds < request.set_size as usize {
        return Err(ChunkletError::Config(format!(
            "alloc: set_size {} requires {} distinct PDs, pool has only {} usable",
            request.set_size, request.set_size, usable_pds
        )));
    }

    let mut role_iter = request.role_assignments.iter().copied();
    match request.ha_domain {
        HaDomain::Pd => {
            if request.set_size >= 2 {
                // Redundant / striped levels (mirror, raid5, raid6): band the
                // rows and rotate the position→PD map per band so the LD spreads
                // across ALL PDs (balanced wear, many-to-many rebuild) while the
                // descriptor stays O(N·positions), independent of num_rows. Plain
                // / Raid0 (set_size == 1) keep the row-major spread below.
                plan_pd_banded(&mut state, request, &mut role_iter, &mut members)?;
            } else {
                for _row in 0..request.num_rows {
                    for _set in 0..request.row_size {
                        members.extend(pick_set(
                            &mut state,
                            None,
                            request.set_size as usize,
                            &mut role_iter,
                        )?);
                    }
                }
            }
        }
        HaDomain::Numa => {
            if state.nodes_with_free().is_empty() {
                return Err(ChunkletError::Config(
                    "alloc: HaDomain::Numa requires PD NUMA node detection".into(),
                ));
            }
            let row_need = (request.set_size as usize) * (request.row_size as usize);
            for _row in 0..request.num_rows {
                let node = state.pick_numa_node(request.set_size as usize, row_need)?;
                for _set in 0..request.row_size {
                    members.extend(pick_set(
                        &mut state,
                        Some(node),
                        request.set_size as usize,
                        &mut role_iter,
                    )?);
                }
            }
        }
        HaDomain::PcieSwitch => unreachable!("validate rejects unsupported HA domains"),
    }

    Ok(Plan { members })
}

struct AllocState {
    free_by_pd: BTreeMap<PdId, VecDeque<u32>>,
    node_by_pd: BTreeMap<PdId, Option<u16>>,
}

impl AllocState {
    fn new(pd_views: Vec<PdFreeView>) -> Self {
        let mut free_by_pd = BTreeMap::new();
        let mut node_by_pd = BTreeMap::new();
        for view in pd_views {
            node_by_pd.insert(view.pd, view.numa_node);
            free_by_pd.insert(view.pd, view.free_indices.into_iter().collect());
        }
        Self {
            free_by_pd,
            node_by_pd,
        }
    }

    fn total_free(&self) -> usize {
        self.free_by_pd.values().map(|v| v.len()).sum()
    }

    fn usable_pds(&self, node: Option<u16>) -> usize {
        self.free_by_pd
            .iter()
            .filter(|(pd, free)| {
                !free.is_empty()
                    && node.map_or(true, |n| {
                        self.node_by_pd.get(pd).copied().flatten() == Some(n)
                    })
            })
            .count()
    }

    fn free_on_node(&self, node: u16) -> usize {
        self.free_by_pd
            .iter()
            .filter(|(pd, _)| self.node_by_pd.get(pd).copied().flatten() == Some(node))
            .map(|(_, free)| free.len())
            .sum()
    }

    fn nodes_with_free(&self) -> Vec<u16> {
        let mut nodes = BTreeSet::new();
        for (pd, free) in &self.free_by_pd {
            if !free.is_empty() {
                if let Some(node) = self.node_by_pd.get(pd).copied().flatten() {
                    nodes.insert(node);
                }
            }
        }
        nodes.into_iter().collect()
    }

    fn pick_numa_node(&self, set_size: usize, row_need: usize) -> ChunkletResult<u16> {
        self.nodes_with_free()
            .into_iter()
            .filter(|&node| {
                self.usable_pds(Some(node)) >= set_size && self.free_on_node(node) >= row_need
            })
            .max_by_key(|&node| (self.free_on_node(node), std::cmp::Reverse(node)))
            .ok_or_else(|| {
                ChunkletError::Config(format!(
                    "alloc: HaDomain::Numa cannot place a row locally: need {} chunklets and {} distinct PDs on one NUMA node",
                    row_need, set_size
                ))
            })
    }

    fn candidate_pds(&self, node: Option<u16>) -> impl Iterator<Item = (&PdId, &VecDeque<u32>)> {
        self.free_by_pd.iter().filter(move |(pd, free)| {
            !free.is_empty()
                && node.map_or(true, |n| {
                    self.node_by_pd.get(pd).copied().flatten() == Some(n)
                })
        })
    }

    fn pop_front(&mut self, pd: &PdId) -> Option<u32> {
        self.free_by_pd.get_mut(pd)?.pop_front()
    }
}

fn pick_set(
    state: &mut AllocState,
    node: Option<u16>,
    set_size: usize,
    role_iter: &mut impl Iterator<Item = LdRole>,
) -> ChunkletResult<Vec<LdMember>> {
    let mut picks = Vec::with_capacity(set_size);
    let mut used_pds: Vec<PdId> = Vec::with_capacity(set_size);
    for _ in 0..set_size {
        let chosen = state
            .candidate_pds(node)
            .filter(|(pd, _)| !used_pds.contains(pd))
            .max_by_key(|(pd, free)| (free.len(), std::cmp::Reverse(**pd)))
            .map(|(pd, _)| *pd)
            .ok_or_else(|| {
                let where_clause = node
                    .map(|n| format!(" on NUMA node {}", n))
                    .unwrap_or_default();
                ChunkletError::Config(format!(
                    "alloc: not enough distinct PDs for set_size {}{}",
                    set_size, where_clause
                ))
            })?;

        let chunklet_index = state
            .pop_front(&chosen)
            .expect("filter above guarantees at least one free index");
        let role = role_iter
            .next()
            .ok_or_else(|| ChunkletError::Invariant("role_assignments exhausted".into()))?;
        picks.push(LdMember {
            pd: chosen,
            chunklet_index,
            role,
            generation: 0,
        });
        used_pds.push(chosen);
    }
    Ok(picks)
}

/// Banded column-contiguous placement for redundant / striped levels
/// (`set_size >= 2`) — the chunklet-faithful spread.
///
/// The `num_rows` are split into `bands = min(N_pds, num_rows)` contiguous row
/// bands. In band `k` the member positions map to a PD window rotated by `k`
/// (`pd_order[(p + k) mod N]`), so across the bands every PD carries a roughly
/// equal share of the LD — full-width striping and balanced wear, exactly what
/// chunklet exists for, and the property that makes rebuild many-to-many (a
/// failed PD's chunklets scatter across many bands / rebuild targets).
///
/// Within a band a position sits on one PD with a contiguous index run, so the
/// descriptor's per-position RLE (`LdDescriptor::position_runs`) yields ~one run
/// per band → at most `N` runs per position, **independent of `num_rows`**. With
/// the 32 MiB manifest slot that stays a few KB, so full spread costs nothing —
/// no return to the ~13-row wall and no write amplification.
///
/// Per-set PD uniqueness holds by construction: a set's `set_size` positions are
/// consecutive, so their rotated PD indices are `set_size` consecutive values
/// mod `N` — distinct whenever `N >= set_size`.
///
/// Pushes members into `out` in the canonical row-major / set-major order that
/// `role_assignments` and `LdDescriptor::flat_index` expect.
fn plan_pd_banded(
    state: &mut AllocState,
    request: &AllocRequest,
    role_iter: &mut impl Iterator<Item = LdRole>,
    out: &mut Vec<LdMember>,
) -> ChunkletResult<()> {
    let set_size = request.set_size as usize;
    let num_rows = request.num_rows as usize;
    let p_count = set_size * request.row_size as usize;

    // Stable PD order (most-free first, tie by id) to index the rotation. All
    // PDs with any free space participate — that is the whole point.
    let mut pd_order: Vec<PdId> = state.candidate_pds(None).map(|(pd, _)| *pd).collect();
    pd_order.sort_by_key(|pd| {
        let free = state.free_by_pd.get(pd).map(|q| q.len()).unwrap_or(0);
        (std::cmp::Reverse(free), *pd)
    });
    let n = pd_order.len();
    if n < set_size {
        return Err(ChunkletError::Config(format!(
            "alloc: set_size {} requires {} distinct PDs, pool has only {} usable",
            set_size, set_size, n
        )));
    }

    // (pd, chunklet_index) for every (row, position), filled band by band.
    let mut placed: Vec<Option<(PdId, u32)>> = vec![None; p_count * num_rows];
    let bands = num_rows.min(n).max(1);
    for k in 0..bands {
        let r0 = k * num_rows / bands;
        let r1 = (k + 1) * num_rows / bands;
        for p in 0..p_count {
            let home = pd_order[(p + k) % n];
            // Pop `r1-r0` contiguous indices for this (band, position): on an
            // unfragmented PD they are sequential → one RLE run for the band.
            for r in r0..r1 {
                let idx = state.pop_front(&home).ok_or_else(|| {
                    ChunkletError::Config(format!(
                        "alloc: PD ran out of free chunklets placing a {}-row LD across {} PDs \
                         (pool too small or unbalanced); reduce num_rows or rebalance",
                        num_rows, n
                    ))
                })?;
                placed[r * p_count + p] = Some((home, idx));
            }
        }
    }

    // Emit row-major / set-major with roles: members[r*p_count + p] matches
    // LdDescriptor::flat_index(p, r).
    for r in 0..num_rows {
        for p in 0..p_count {
            let (pd, chunklet_index) = placed[r * p_count + p]
                .expect("every (row, position) placed across the bands above");
            let role = role_iter
                .next()
                .ok_or_else(|| ChunkletError::Invariant("role_assignments exhausted".into()))?;
            out.push(LdMember {
                pd,
                chunklet_index,
                role,
                generation: 0,
            });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pd(seed: u8) -> PdId {
        let mut bytes = [0u8; 16];
        bytes[0] = seed;
        PdId::from_bytes(bytes)
    }

    fn views(spec: &[(u8, &[u32])]) -> Vec<PdFreeView> {
        spec.iter()
            .map(|(seed, indices)| PdFreeView {
                pd: pd(*seed),
                numa_node: None,
                free_indices: indices.to_vec(),
            })
            .collect()
    }

    fn numa_views(spec: &[(u8, u16, &[u32])]) -> Vec<PdFreeView> {
        spec.iter()
            .map(|(seed, node, indices)| PdFreeView {
                pd: pd(*seed),
                numa_node: Some(*node),
                free_indices: indices.to_vec(),
            })
            .collect()
    }

    #[test]
    fn plain_lds_spread_across_pds() {
        let req = AllocRequest {
            set_size: 1,
            row_size: 1,
            num_rows: 4,
            role_assignments: vec![LdRole::Data; 4],
            ha_domain: HaDomain::Pd,
        };
        let v = views(&[(1, &[0, 1, 2, 3]), (2, &[0, 1, 2, 3]), (3, &[0, 1, 2, 3])]);
        let plan = plan_alloc(&req, v).unwrap();
        let mut counts: BTreeMap<PdId, u32> = BTreeMap::new();
        for m in &plan.members {
            *counts.entry(m.pd).or_insert(0) += 1;
        }
        assert!(counts.values().all(|&c| c <= 2));
        assert_eq!(plan.members.len(), 4);
    }

    #[test]
    fn raid5_set_members_distinct_per_set() {
        let req = AllocRequest {
            set_size: 4,
            row_size: 2,
            num_rows: 1,
            role_assignments: vec![
                LdRole::Data,
                LdRole::Data,
                LdRole::Data,
                LdRole::ParityP,
                LdRole::Data,
                LdRole::Data,
                LdRole::Data,
                LdRole::ParityP,
            ],
            ha_domain: HaDomain::Pd,
        };
        let v = views(&[
            (1, &[0, 1, 2]),
            (2, &[0, 1, 2]),
            (3, &[0, 1, 2]),
            (4, &[0, 1, 2]),
            (5, &[0, 1, 2]),
        ]);
        let plan = plan_alloc(&req, v).unwrap();
        for set in plan.members.chunks(4) {
            let mut seen: Vec<PdId> = set.iter().map(|m| m.pd).collect();
            seen.sort();
            seen.dedup();
            assert_eq!(seen.len(), 4, "set has duplicate PDs: {:?}", set);
        }
    }

    #[test]
    fn numa_alloc_keeps_each_row_on_one_node() {
        let req = AllocRequest {
            set_size: 2,
            row_size: 2,
            num_rows: 2,
            role_assignments: vec![LdRole::Data; 8],
            ha_domain: HaDomain::Numa,
        };
        let v = numa_views(&[
            (1, 0, &[0, 1, 2, 3]),
            (2, 0, &[0, 1, 2, 3]),
            (3, 1, &[0, 1, 2, 3]),
            (4, 1, &[0, 1, 2, 3]),
        ]);
        let plan = plan_alloc(&req, v).unwrap();
        let node_by_pd: BTreeMap<PdId, u16> = [(pd(1), 0), (pd(2), 0), (pd(3), 1), (pd(4), 1)]
            .into_iter()
            .collect();
        for row in plan.members.chunks(4) {
            let mut nodes: Vec<u16> = row.iter().map(|m| node_by_pd[&m.pd]).collect();
            nodes.sort_unstable();
            nodes.dedup();
            assert_eq!(nodes.len(), 1, "row crossed NUMA nodes: {:?}", row);
        }
    }

    #[test]
    fn numa_alloc_rejects_cross_node_sets() {
        let req = AllocRequest {
            set_size: 3,
            row_size: 1,
            num_rows: 1,
            role_assignments: vec![LdRole::Data; 3],
            ha_domain: HaDomain::Numa,
        };
        let v = numa_views(&[
            (1, 0, &[0, 1]),
            (2, 0, &[0, 1]),
            (3, 1, &[0, 1]),
            (4, 1, &[0, 1]),
        ]);
        let err = plan_alloc(&req, v).err().unwrap();
        assert!(matches!(err, ChunkletError::Config(_)));
    }

    #[test]
    fn rejects_when_set_size_exceeds_distinct_pds() {
        let req = AllocRequest {
            set_size: 4,
            row_size: 1,
            num_rows: 1,
            role_assignments: vec![LdRole::Data; 4],
            ha_domain: HaDomain::Pd,
        };
        let v = views(&[(1, &[0]), (2, &[0]), (3, &[0])]);
        let err = plan_alloc(&req, v).err().unwrap();
        assert!(matches!(err, ChunkletError::Config(_)));
    }

    #[test]
    fn rejects_when_pool_lacks_total_capacity() {
        let req = AllocRequest {
            set_size: 1,
            row_size: 1,
            num_rows: 5,
            role_assignments: vec![LdRole::Data; 5],
            ha_domain: HaDomain::Pd,
        };
        let v = views(&[(1, &[0, 1]), (2, &[0])]);
        let err = plan_alloc(&req, v).err().unwrap();
        assert!(matches!(err, ChunkletError::Config(_)));
    }

    #[test]
    fn rejects_unsupported_ha_domain() {
        let req = AllocRequest {
            set_size: 1,
            row_size: 1,
            num_rows: 1,
            role_assignments: vec![LdRole::Data],
            ha_domain: HaDomain::PcieSwitch,
        };
        let v = views(&[(1, &[0])]);
        let err = plan_alloc(&req, v).err().unwrap();
        assert!(matches!(err, ChunkletError::Unsupported(_)));
    }

    #[test]
    fn pd_banded_spreads_all_pds_balanced_and_compresses() {
        use crate::ld::descriptor::LdDescriptor;
        use crate::types::{LdId, RaidLevel};

        // 9 PDs (onyx LV3 shape), RAID6 6+2 (set_size 8, row_size 1), many rows.
        let num_rows: u16 = 300;
        let free: Vec<u32> = (0..1000).collect();
        let v: Vec<PdFreeView> = (1..=9u8)
            .map(|s| PdFreeView {
                pd: pd(s),
                numa_node: None,
                free_indices: free.clone(),
            })
            .collect();
        let mut roles = Vec::with_capacity(8 * num_rows as usize);
        for _ in 0..num_rows {
            for m in 0..8 {
                roles.push(match m {
                    6 => LdRole::ParityP,
                    7 => LdRole::ParityQ,
                    _ => LdRole::Data,
                });
            }
        }
        let req = AllocRequest {
            set_size: 8,
            row_size: 1,
            num_rows,
            role_assignments: roles,
            ha_domain: HaDomain::Pd,
        };
        let plan = plan_alloc(&req, v).unwrap();
        assert_eq!(plan.members.len(), 8 * num_rows as usize);

        // chunklet essence: EVERY PD carries a share (no idle disk), balanced.
        let mut per_pd: BTreeMap<PdId, u32> = BTreeMap::new();
        for m in &plan.members {
            *per_pd.entry(m.pd).or_insert(0) += 1;
        }
        assert_eq!(
            per_pd.len(),
            9,
            "all 9 PDs must be used, got {}",
            per_pd.len()
        );
        let total = (8 * num_rows as u32) as f64;
        let avg = total / 9.0;
        let (min, max) = (
            *per_pd.values().min().unwrap() as f64,
            *per_pd.values().max().unwrap() as f64,
        );
        assert!(
            min >= 0.7 * avg && max <= 1.3 * avg,
            "PD load imbalanced: min={min} max={max} avg={avg:.1}"
        );

        // Per-row set PD-uniqueness (RAID fault tolerance) holds every row.
        for r in 0..num_rows as usize {
            let mut pds: Vec<PdId> = (0..8).map(|p| plan.members[r * 8 + p].pd).collect();
            pds.sort();
            pds.dedup();
            assert_eq!(pds.len(), 8, "row {r} has duplicate PDs in its set");
        }

        // Descriptor stays O(N·positions), NOT O(rows): banding gives <= N runs
        // per position (here <= 9), so ~2 KB regardless of the 300 rows — vs the
        // ~65 KB / overflow a row-major layout would produce.
        let desc = LdDescriptor {
            id: LdId::new_v4(),
            raid_level: RaidLevel::Raid6,
            set_size: 8,
            row_size: 1,
            num_rows,
            strip_size_log2: 0,
            ha_domain: HaDomain::Pd,
            members: plan.members,
        };
        assert!(
            desc.encoded_len() < 2500,
            "expected <= N runs/position (~2 KB), got {}",
            desc.encoded_len()
        );
        // Round-trips losslessly through the codec.
        let bytes = desc.encode().unwrap();
        let (decoded, used) = LdDescriptor::decode_one(&bytes).unwrap();
        assert_eq!(used, bytes.len());
        assert_eq!(decoded, desc);
    }

    #[test]
    fn pd_column_positions_reuse_pds_across_sets_when_p_exceeds_n() {
        // set_size 2 (mirror), row_size 6 => P=12 positions on only 4 PDs:
        // positions must reuse PDs across sets while staying distinct WITHIN a
        // set, each still a contiguous column.
        let num_rows: u16 = 10;
        let free: Vec<u32> = (0..100).collect();
        let v: Vec<PdFreeView> = (1..=4u8)
            .map(|s| PdFreeView {
                pd: pd(s),
                numa_node: None,
                free_indices: free.clone(),
            })
            .collect();
        let req = AllocRequest {
            set_size: 2,
            row_size: 6,
            num_rows,
            role_assignments: vec![LdRole::Data; 2 * 6 * num_rows as usize],
            ha_domain: HaDomain::Pd,
        };
        let plan = plan_alloc(&req, v).unwrap();
        // Each of the 6 sets (2 members) must be on distinct PDs every row.
        for r in 0..num_rows as usize {
            for set in 0..6usize {
                let a = plan.members[r * 12 + set * 2].pd;
                let b = plan.members[r * 12 + set * 2 + 1].pd;
                assert_ne!(a, b, "row {r} set {set} mirror copies collide on one PD");
            }
        }
    }

    #[test]
    fn deterministic_for_same_input() {
        let req = AllocRequest {
            set_size: 2,
            row_size: 1,
            num_rows: 3,
            role_assignments: vec![LdRole::Data; 6],
            ha_domain: HaDomain::Pd,
        };
        let v1 = views(&[(1, &[0, 1, 2]), (2, &[0, 1, 2]), (3, &[0, 1, 2])]);
        let v2 = views(&[(1, &[0, 1, 2]), (2, &[0, 1, 2]), (3, &[0, 1, 2])]);
        let p1 = plan_alloc(&req, v1).unwrap();
        let p2 = plan_alloc(&req, v2).unwrap();
        assert_eq!(p1.members, p2.members);
    }
}
