//! Common Provisioning Group — declarative LD policy template.
//!
//! A CPG declares the RAID level / set size / row size / strip / HA domain
//! that future LDs in this pool should use. Once a CPG exists, the operator
//! creates LDs by reference (`Pool::create_ld_in_cpg(cpg, num_rows)`)
//! instead of spelling out an `LdSpec` each time. CPGs are persisted on
//! every PD's manifest in the same way as `LdList`.
//!
//! Phase 7 only carries the policy; **CPG-level capacity quotas, dynamic
//! LD growth, and per-CPG free-space accounting** are deferred. For now a
//! CPG is essentially a named bundle of `LdSpec` defaults.
//!
//! # Wire format (per descriptor)
//!
//! ```text
//! [0..2]    descriptor_size u16 LE
//! [2..3]    raid_level u8
//! [3..4]    set_size u8
//! [4..6]    row_size u16 LE
//! [6..7]    strip_size_log2 u8
//! [7..8]    ha_domain u8
//! [8..24]   cpg_id (16 bytes)
//! [24..28]  name_len u32 LE
//! [28..]    name_bytes (UTF-8)
//! ```
//!
//! # List format (in `SuperblockBody::cpg_list_bytes`)
//!
//! ```text
//! [0..4]    cpg_count u32 LE
//! [4..]     [CpgDescriptor; N]
//! ```

use std::convert::TryInto;
use std::sync::Arc;

use crate::error::{ChunkletError, ChunkletResult};
use crate::pool::ld_ops::LdSpec;
use crate::pool::Pool;
use crate::types::{CpgId, HaDomain, LdId, RaidLevel};

const CPG_HEADER_BYTES: usize = 28;

/// Caller-supplied CPG creation spec.
#[derive(Clone, Debug)]
pub struct CpgSpec {
    pub name: String,
    pub raid_level: RaidLevel,
    pub set_size: u8,
    pub row_size: u16,
    pub strip_size_log2: u8,
    pub ha_domain: HaDomain,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CpgDescriptor {
    pub id: CpgId,
    pub name: String,
    pub raid_level: RaidLevel,
    pub set_size: u8,
    pub row_size: u16,
    pub strip_size_log2: u8,
    pub ha_domain: HaDomain,
}

impl CpgDescriptor {
    pub fn encoded_len(&self) -> usize {
        CPG_HEADER_BYTES + self.name.as_bytes().len()
    }

    pub fn encode(&self) -> ChunkletResult<Vec<u8>> {
        let total = self.encoded_len();
        if total > u16::MAX as usize {
            return Err(ChunkletError::Format(format!(
                "CPG descriptor too large: {} bytes",
                total
            )));
        }
        let name_bytes = self.name.as_bytes();
        let mut out = vec![0u8; total];
        out[0..2].copy_from_slice(&(total as u16).to_le_bytes());
        out[2] = self.raid_level as u8;
        out[3] = self.set_size;
        out[4..6].copy_from_slice(&self.row_size.to_le_bytes());
        out[6] = self.strip_size_log2;
        out[7] = self.ha_domain as u8;
        out[8..24].copy_from_slice(&self.id.to_bytes());
        out[24..28].copy_from_slice(&(name_bytes.len() as u32).to_le_bytes());
        out[28..].copy_from_slice(name_bytes);
        Ok(out)
    }

    pub fn decode_one(bytes: &[u8]) -> ChunkletResult<(Self, usize)> {
        if bytes.len() < CPG_HEADER_BYTES {
            return Err(ChunkletError::Format(format!(
                "CPG descriptor truncated: {}",
                bytes.len()
            )));
        }
        let total = u16::from_le_bytes(bytes[0..2].try_into().unwrap()) as usize;
        if total < CPG_HEADER_BYTES || total > bytes.len() {
            return Err(ChunkletError::Format(format!(
                "CPG descriptor size {} out of range [{}, {}]",
                total,
                CPG_HEADER_BYTES,
                bytes.len()
            )));
        }
        let raid_level = RaidLevel::from_u8(bytes[2])?;
        let set_size = bytes[3];
        let row_size = u16::from_le_bytes(bytes[4..6].try_into().unwrap());
        let strip_size_log2 = bytes[6];
        let ha_domain = match bytes[7] {
            0 => HaDomain::Pd,
            1 => HaDomain::Numa,
            2 => HaDomain::PcieSwitch,
            other => {
                return Err(ChunkletError::Format(format!(
                    "unknown HaDomain byte: {}",
                    other
                )))
            }
        };
        let id = CpgId::from_bytes(bytes[8..24].try_into().unwrap());
        let name_len = u32::from_le_bytes(bytes[24..28].try_into().unwrap()) as usize;
        if CPG_HEADER_BYTES + name_len != total {
            return Err(ChunkletError::Format(format!(
                "CPG descriptor body size {} != header({}) + name_len({})",
                total, CPG_HEADER_BYTES, name_len
            )));
        }
        let name = std::str::from_utf8(&bytes[CPG_HEADER_BYTES..total])
            .map_err(|e| ChunkletError::Format(format!("CPG name not utf-8: {}", e)))?
            .to_string();
        Ok((
            Self {
                id,
                name,
                raid_level,
                set_size,
                row_size,
                strip_size_log2,
                ha_domain,
            },
            total,
        ))
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CpgList {
    pub cpgs: Vec<CpgDescriptor>,
}

impl CpgList {
    pub fn encode(&self) -> ChunkletResult<Vec<u8>> {
        let mut out =
            Vec::with_capacity(4 + self.cpgs.iter().map(|c| c.encoded_len()).sum::<usize>());
        out.extend_from_slice(&(self.cpgs.len() as u32).to_le_bytes());
        for c in &self.cpgs {
            out.extend_from_slice(&c.encode()?);
        }
        Ok(out)
    }

    pub fn decode(bytes: &[u8]) -> ChunkletResult<Self> {
        if bytes.is_empty() {
            return Ok(Self::default());
        }
        if bytes.len() < 4 {
            return Err(ChunkletError::Format(format!(
                "cpg_list bytes truncated: {}",
                bytes.len()
            )));
        }
        let count = u32::from_le_bytes(bytes[0..4].try_into().unwrap()) as usize;
        let mut cursor = 4;
        let mut cpgs = Vec::with_capacity(count);
        for _ in 0..count {
            let (c, used) = CpgDescriptor::decode_one(&bytes[cursor..])?;
            cursor += used;
            cpgs.push(c);
        }
        Ok(Self { cpgs })
    }

    pub fn find(&self, id: CpgId) -> Option<&CpgDescriptor> {
        self.cpgs.iter().find(|c| c.id == id)
    }

    pub fn find_by_name(&self, name: &str) -> Option<&CpgDescriptor> {
        self.cpgs.iter().find(|c| c.name == name)
    }

    pub fn upsert(&mut self, desc: CpgDescriptor) {
        if let Some(slot) = self.cpgs.iter_mut().find(|c| c.id == desc.id) {
            *slot = desc;
        } else {
            self.cpgs.push(desc);
        }
    }

    pub fn remove(&mut self, id: CpgId) -> Option<CpgDescriptor> {
        let idx = self.cpgs.iter().position(|c| c.id == id)?;
        Some(self.cpgs.remove(idx))
    }
}

impl Pool {
    pub fn list_cpgs(&self) -> Vec<CpgDescriptor> {
        self.state.read().cpg_list.cpgs.clone()
    }

    pub fn find_cpg(&self, id: CpgId) -> Option<CpgDescriptor> {
        self.state.read().cpg_list.find(id).cloned()
    }

    pub fn find_cpg_by_name(&self, name: &str) -> Option<CpgDescriptor> {
        self.state.read().cpg_list.find_by_name(name).cloned()
    }

    /// Create a new CPG with the given spec. The CPG persists on every PD's
    /// manifest. Returns the new `CpgId`.
    pub fn create_cpg(self: &Arc<Self>, spec: CpgSpec) -> ChunkletResult<CpgId> {
        // Reject HaDomain values that the allocator can't honor. Without this
        // a CPG with HaDomain::Numa / PcieSwitch persists fine, then every
        // create_ld_in_cpg fails downstream — confusing for operators because
        // the CPG list shows a config that "works" but never produces an LD.
        if !spec.ha_domain.is_supported() {
            return Err(ChunkletError::Unsupported(format!(
                "CPG HaDomain {:?} (only Pd is wired)",
                spec.ha_domain
            )));
        }
        let _commit = self.manifest_lock.lock();
        let new_id = CpgId::new_v4();
        let desc = CpgDescriptor {
            id: new_id,
            name: spec.name,
            raid_level: spec.raid_level,
            set_size: spec.set_size,
            row_size: spec.row_size,
            strip_size_log2: spec.strip_size_log2,
            ha_domain: spec.ha_domain,
        };
        let new_bytes = {
            let mut s = self.state.write();
            // Reject duplicate names — that's an admin foot-gun.
            if s.cpg_list.find_by_name(&desc.name).is_some() {
                return Err(ChunkletError::Config(format!(
                    "CPG name '{}' already in use",
                    desc.name
                )));
            }
            s.cpg_list.upsert(desc.clone());
            s.cpg_list.encode()?
        };
        let pds_snapshot = self.state.read().pds.clone();
        for (_pd_id, pd) in &pds_snapshot {
            let nb = new_bytes.clone();
            pd.commit_manifest(move |body, _bm| {
                body.cpg_list_bytes = nb;
                Ok(())
            })?;
        }
        Ok(new_id)
    }

    pub fn drop_cpg(&self, id: CpgId) -> ChunkletResult<()> {
        let _commit = self.manifest_lock.lock();
        let new_bytes = {
            let mut s = self.state.write();
            if s.cpg_list.remove(id).is_none() {
                return Err(ChunkletError::Invariant(format!("CPG {} not found", id)));
            }
            s.cpg_list.encode()?
        };
        let pds_snapshot = self.state.read().pds.clone();
        for (_pd_id, pd) in &pds_snapshot {
            let nb = new_bytes.clone();
            pd.commit_manifest(move |body, _bm| {
                body.cpg_list_bytes = nb;
                Ok(())
            })?;
        }
        Ok(())
    }

    /// Create an LD in the given CPG. The CPG provides the policy
    /// (raid_level, set_size, row_size, strip_size, HA domain); the caller
    /// specifies `num_rows` (capacity multiplier).
    pub fn create_ld_in_cpg(&self, cpg_id: CpgId, num_rows: u16) -> ChunkletResult<LdId> {
        let cpg = self
            .find_cpg(cpg_id)
            .ok_or_else(|| ChunkletError::Invariant(format!("CPG {} not found", cpg_id)))?;
        let spec = LdSpec {
            raid_level: cpg.raid_level,
            set_size: cpg.set_size,
            row_size: cpg.row_size,
            num_rows,
            strip_size_log2: cpg.strip_size_log2,
            ha_domain: cpg.ha_domain,
        };
        self.create_ld(spec)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(name: &str) -> CpgDescriptor {
        CpgDescriptor {
            id: CpgId::new_v4(),
            name: name.into(),
            raid_level: RaidLevel::Raid6,
            set_size: 5,
            row_size: 1,
            strip_size_log2: 0,
            ha_domain: HaDomain::Pd,
        }
    }

    #[test]
    fn descriptor_round_trip() {
        let d = sample("lv3-r6");
        let bytes = d.encode().unwrap();
        let (d2, used) = CpgDescriptor::decode_one(&bytes).unwrap();
        assert_eq!(used, bytes.len());
        assert_eq!(d2, d);
    }

    #[test]
    fn list_round_trip() {
        let list = CpgList {
            cpgs: vec![sample("a"), sample("b"), sample("c")],
        };
        let bytes = list.encode().unwrap();
        let list2 = CpgList::decode(&bytes).unwrap();
        assert_eq!(list2, list);
    }

    #[test]
    fn empty_round_trip() {
        let list = CpgList::default();
        let bytes = list.encode().unwrap();
        let list2 = CpgList::decode(&bytes).unwrap();
        assert_eq!(list2, list);
    }

    #[test]
    fn upsert_replaces_same_id() {
        let mut list = CpgList::default();
        let mut a = sample("a");
        let id = a.id;
        list.upsert(a.clone());
        a.name = "renamed".into();
        list.upsert(a.clone());
        assert_eq!(list.cpgs.len(), 1);
        assert_eq!(list.cpgs[0].name, "renamed");
        list.remove(id).unwrap();
        assert!(list.cpgs.is_empty());
    }
}
