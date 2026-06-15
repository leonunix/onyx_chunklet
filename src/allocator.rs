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
