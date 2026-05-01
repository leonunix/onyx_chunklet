//! Cross-PD chunklet allocator.
//!
//! # Inputs / outputs
//!
//! Pure function: takes a snapshot of free chunklets per PD plus an
//! allocation request, returns a `Plan` (or an error). It does **not**
//! mutate state. Callers (`Pool::create_ld`) apply the plan via per-PD
//! `commit_manifest`.
//!
//! # Algorithm
//!
//! For each "row" of the LD, for each "set" within the row:
//!   1. pick `set_size` distinct PDs by descending free-count;
//!   2. within each chosen PD, take the lowest-index free chunklet.
//!
//! "Distinct PDs per set" is the universal RAID invariant — two chunklets
//! in the same set must not live on the same PD. Across sets within a row
//! (and across rows) a PD can be reused.
//!
//! HA domain `Pd` is the only level wired in P1; `Numa` / `PcieSwitch`
//! return `Unsupported`.

use std::collections::BTreeMap;

use crate::error::{ChunkletError, ChunkletResult};
use crate::types::{HaDomain, LdMember, LdRole, PdId};

/// Snapshot of one PD's free chunklets, sorted ascending.
#[derive(Clone, Debug)]
pub struct PdFreeView {
    pub pd: PdId,
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

pub fn plan_alloc(
    request: &AllocRequest,
    pd_views: Vec<PdFreeView>,
) -> ChunkletResult<Plan> {
    request.validate()?;

    // Working state: per-PD free index list (mutable), index by PD.
    let mut state: BTreeMap<PdId, Vec<u32>> = pd_views
        .into_iter()
        .map(|v| (v.pd, v.free_indices))
        .collect();

    let total = request.total_members();
    let mut members = Vec::with_capacity(total);

    // Preflight: ensure we have enough total free chunklets *and* enough
    // distinct PDs to fill any single set.
    let total_free: usize = state.values().map(|v| v.len()).sum();
    if total_free < total {
        return Err(ChunkletError::Config(format!(
            "alloc: need {} chunklets, pool has only {} free",
            total, total_free
        )));
    }
    let usable_pds = state.values().filter(|v| !v.is_empty()).count();
    if (usable_pds as u8) < request.set_size {
        return Err(ChunkletError::Config(format!(
            "alloc: set_size {} requires {} distinct PDs, pool has only {} usable",
            request.set_size, request.set_size, usable_pds
        )));
    }

    let mut role_iter = request.role_assignments.iter().copied();
    for _row in 0..request.num_rows {
        for _set in 0..request.row_size {
            let set_members =
                pick_set(&mut state, request.set_size as usize, &mut role_iter)?;
            members.extend(set_members);
        }
    }

    Ok(Plan { members })
}

/// Pick one set: `set_size` distinct PDs by descending free-count, lowest
/// chunklet index from each.
fn pick_set(
    state: &mut BTreeMap<PdId, Vec<u32>>,
    set_size: usize,
    role_iter: &mut impl Iterator<Item = LdRole>,
) -> ChunkletResult<Vec<LdMember>> {
    let mut picks = Vec::with_capacity(set_size);
    let mut used_pds: Vec<PdId> = Vec::with_capacity(set_size);
    for _ in 0..set_size {
        // Find the PD with the most free chunklets that we haven't used in
        // this set. Tie-break by PdId for determinism.
        let chosen = state
            .iter()
            .filter(|(pd, free)| !free.is_empty() && !used_pds.contains(pd))
            .max_by_key(|(pd, free)| (free.len(), std::cmp::Reverse(*pd)))
            .map(|(pd, _)| *pd)
            .ok_or_else(|| {
                ChunkletError::Config(format!(
                    "alloc: not enough distinct PDs for set_size {}",
                    set_size
                ))
            })?;

        let chunklet_index = {
            let free = state.get_mut(&chosen).unwrap();
            // pop the lowest index (`free` is kept sorted ascending).
            free.remove(0)
        };
        let role = role_iter.next().ok_or_else(|| {
            ChunkletError::Invariant("role_assignments exhausted".into())
        })?;
        picks.push(LdMember {
            pd: chosen,
            chunklet_index,
            role,
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
                free_indices: indices.to_vec(),
            })
            .collect()
    }

    #[test]
    fn plain_lds_spread_across_pds() {
        // 3 PDs, plenty free, ask for 4 Plain members (set_size=1).
        let req = AllocRequest {
            set_size: 1,
            row_size: 1,
            num_rows: 4,
            role_assignments: vec![LdRole::Data; 4],
            ha_domain: HaDomain::Pd,
        };
        let v = views(&[(1, &[0, 1, 2, 3]), (2, &[0, 1, 2, 3]), (3, &[0, 1, 2, 3])]);
        let plan = plan_alloc(&req, v).unwrap();
        // 4 members: should be spread roundish across the 3 PDs.
        let mut counts: BTreeMap<PdId, u32> = BTreeMap::new();
        for m in &plan.members {
            *counts.entry(m.pd).or_insert(0) += 1;
        }
        // No single PD takes 4-of-4; tightest packing is (2, 1, 1).
        assert!(counts.values().all(|&c| c <= 2));
        assert_eq!(plan.members.len(), 4);
    }

    #[test]
    fn raid5_set_members_distinct_per_set() {
        // 5 PDs, RAID-5 set_size=4 (3+1), 1 row, 2 sets.
        let req = AllocRequest {
            set_size: 4,
            row_size: 2,
            num_rows: 1,
            role_assignments: vec![
                LdRole::Data, LdRole::Data, LdRole::Data, LdRole::ParityP,
                LdRole::Data, LdRole::Data, LdRole::Data, LdRole::ParityP,
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
        // Verify each set has 4 distinct PDs.
        for set in plan.members.chunks(4) {
            let mut seen: Vec<PdId> = set.iter().map(|m| m.pd).collect();
            seen.sort();
            seen.dedup();
            assert_eq!(seen.len(), 4, "set has duplicate PDs: {:?}", set);
        }
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
        // Only 3 PDs available — can't form set_size=4.
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
            ha_domain: HaDomain::Numa,
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
