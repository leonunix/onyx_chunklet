//! LD descriptor encoding (variable-length records embedded in
//! `SuperblockBody::ld_list_bytes`).
//!
//! # Wire format (per descriptor)
//!
//! ```text
//! [0..2]     descriptor_size u16 LE   (total bytes for this descriptor incl. header)
//! [2..3]     raid_level u8
//! [3..4]     set_size u8              (members per RAID set; 1 for Plain)
//! [4..6]     row_size u16 LE          (sets per stripe row)
//! [6..8]     num_rows u16 LE
//! [8..9]     strip_size_log2 u8       (0 = block-aligned, no striping inside set)
//! [9..12]    reserved
//! [12..28]   ld_uuid (16 bytes)
//! [28..32]   member_count u32 LE
//! [32..]     members[N], 24 bytes each:
//!              [0..16]  pd_id (16 bytes)
//!              [16..20] chunklet_index u32 LE
//!              [20..21] role u8
//!              [21..24] reserved
//! ```
//!
//! # List format (the bytes stored in `SuperblockBody::ld_list_bytes`)
//!
//! ```text
//! [0..4]     ld_count u32 LE
//! [4..]      [LdDescriptor; N]
//! ```

use std::convert::TryInto;

use crate::error::{ChunkletError, ChunkletResult};
use crate::types::{LdId, LdMember, LdRole, PdId, RaidLevel};

const DESC_HEADER_BYTES: usize = 32;
const MEMBER_BYTES: usize = 24;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LdDescriptor {
    pub id: LdId,
    pub raid_level: RaidLevel,
    pub set_size: u8,
    pub row_size: u16,
    pub num_rows: u16,
    pub strip_size_log2: u8,
    pub members: Vec<LdMember>,
}

impl LdDescriptor {
    pub fn encoded_len(&self) -> usize {
        DESC_HEADER_BYTES + self.members.len() * MEMBER_BYTES
    }

    pub fn encode(&self) -> ChunkletResult<Vec<u8>> {
        let total = self.encoded_len();
        if total > u16::MAX as usize {
            return Err(ChunkletError::Format(format!(
                "LD descriptor too large: {} bytes",
                total
            )));
        }
        let mut out = vec![0u8; total];
        out[0..2].copy_from_slice(&(total as u16).to_le_bytes());
        out[2] = self.raid_level as u8;
        out[3] = self.set_size;
        out[4..6].copy_from_slice(&self.row_size.to_le_bytes());
        out[6..8].copy_from_slice(&self.num_rows.to_le_bytes());
        out[8] = self.strip_size_log2;
        // [9..12] reserved.
        out[12..28].copy_from_slice(&self.id.to_bytes());
        out[28..32].copy_from_slice(&(self.members.len() as u32).to_le_bytes());
        for (i, m) in self.members.iter().enumerate() {
            let off = DESC_HEADER_BYTES + i * MEMBER_BYTES;
            out[off..off + 16].copy_from_slice(&m.pd.to_bytes());
            out[off + 16..off + 20].copy_from_slice(&m.chunklet_index.to_le_bytes());
            out[off + 20] = m.role as u8;
            // [21..24] reserved.
        }
        Ok(out)
    }

    /// Decode a single descriptor starting at `bytes[0..]`. Returns the
    /// descriptor and the number of bytes consumed.
    pub fn decode_one(bytes: &[u8]) -> ChunkletResult<(Self, usize)> {
        if bytes.len() < DESC_HEADER_BYTES {
            return Err(ChunkletError::Format(format!(
                "ld descriptor truncated: {} bytes",
                bytes.len()
            )));
        }
        let total = u16::from_le_bytes(bytes[0..2].try_into().unwrap()) as usize;
        if total < DESC_HEADER_BYTES || total > bytes.len() {
            return Err(ChunkletError::Format(format!(
                "ld descriptor size {} out of range [{}, {}]",
                total,
                DESC_HEADER_BYTES,
                bytes.len()
            )));
        }
        let raid_level = RaidLevel::from_u8(bytes[2])?;
        let set_size = bytes[3];
        let row_size = u16::from_le_bytes(bytes[4..6].try_into().unwrap());
        let num_rows = u16::from_le_bytes(bytes[6..8].try_into().unwrap());
        let strip_size_log2 = bytes[8];
        let id = LdId::from_bytes(bytes[12..28].try_into().unwrap());
        let member_count = u32::from_le_bytes(bytes[28..32].try_into().unwrap()) as usize;

        let body_bytes = total - DESC_HEADER_BYTES;
        if body_bytes != member_count * MEMBER_BYTES {
            return Err(ChunkletError::Format(format!(
                "ld descriptor body {} bytes != member_count {} * {}",
                body_bytes, member_count, MEMBER_BYTES
            )));
        }
        let mut members = Vec::with_capacity(member_count);
        for i in 0..member_count {
            let off = DESC_HEADER_BYTES + i * MEMBER_BYTES;
            members.push(LdMember {
                pd: PdId::from_bytes(bytes[off..off + 16].try_into().unwrap()),
                chunklet_index: u32::from_le_bytes(
                    bytes[off + 16..off + 20].try_into().unwrap(),
                ),
                role: LdRole::from_u8(bytes[off + 20])?,
            });
        }
        Ok((
            Self {
                id,
                raid_level,
                set_size,
                row_size,
                num_rows,
                strip_size_log2,
                members,
            },
            total,
        ))
    }
}

/// Top-level LD list, persisted in `SuperblockBody::ld_list_bytes`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct LdList {
    pub lds: Vec<LdDescriptor>,
}

impl LdList {
    pub fn encode(&self) -> ChunkletResult<Vec<u8>> {
        let mut out = Vec::with_capacity(4 + self.lds.iter().map(|d| d.encoded_len()).sum::<usize>());
        out.extend_from_slice(&(self.lds.len() as u32).to_le_bytes());
        for d in &self.lds {
            out.extend_from_slice(&d.encode()?);
        }
        Ok(out)
    }

    pub fn decode(bytes: &[u8]) -> ChunkletResult<Self> {
        if bytes.is_empty() {
            return Ok(Self::default());
        }
        if bytes.len() < 4 {
            return Err(ChunkletError::Format(format!(
                "ld_list bytes truncated: {}",
                bytes.len()
            )));
        }
        let count = u32::from_le_bytes(bytes[0..4].try_into().unwrap()) as usize;
        let mut cursor = 4;
        let mut lds = Vec::with_capacity(count);
        for _ in 0..count {
            let (d, used) = LdDescriptor::decode_one(&bytes[cursor..])?;
            cursor += used;
            lds.push(d);
        }
        Ok(Self { lds })
    }

    pub fn find(&self, id: LdId) -> Option<&LdDescriptor> {
        self.lds.iter().find(|d| d.id == id)
    }

    pub fn upsert(&mut self, desc: LdDescriptor) {
        if let Some(slot) = self.lds.iter_mut().find(|d| d.id == desc.id) {
            *slot = desc;
        } else {
            self.lds.push(desc);
        }
    }

    pub fn remove(&mut self, id: LdId) -> Option<LdDescriptor> {
        let idx = self.lds.iter().position(|d| d.id == id)?;
        Some(self.lds.remove(idx))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::PdId;

    fn sample(member_count: usize) -> LdDescriptor {
        LdDescriptor {
            id: LdId::new_v4(),
            raid_level: RaidLevel::Plain,
            set_size: 1,
            row_size: 1,
            num_rows: member_count as u16,
            strip_size_log2: 0,
            members: (0..member_count)
                .map(|i| LdMember {
                    pd: PdId::new_v4(),
                    chunklet_index: i as u32,
                    role: LdRole::Data,
                })
                .collect(),
        }
    }

    #[test]
    fn round_trip_single() {
        let d = sample(4);
        let bytes = d.encode().unwrap();
        let (decoded, used) = LdDescriptor::decode_one(&bytes).unwrap();
        assert_eq!(used, bytes.len());
        assert_eq!(decoded, d);
    }

    #[test]
    fn round_trip_list() {
        let lds = LdList {
            lds: vec![sample(2), sample(8), sample(1)],
        };
        let bytes = lds.encode().unwrap();
        let decoded = LdList::decode(&bytes).unwrap();
        assert_eq!(decoded, lds);
    }

    #[test]
    fn empty_list_round_trip() {
        let lds = LdList::default();
        let bytes = lds.encode().unwrap();
        let decoded = LdList::decode(&bytes).unwrap();
        assert_eq!(decoded, lds);
    }

    #[test]
    fn upsert_and_remove() {
        let mut lds = LdList::default();
        let d1 = sample(2);
        let id = d1.id;
        lds.upsert(d1);
        assert_eq!(lds.lds.len(), 1);
        // upsert with same id replaces.
        let mut d2 = sample(4);
        d2.id = id;
        lds.upsert(d2.clone());
        assert_eq!(lds.lds.len(), 1);
        assert_eq!(lds.lds[0], d2);
        assert!(lds.remove(id).is_some());
        assert!(lds.lds.is_empty());
    }

    #[test]
    fn rejects_truncated_descriptor() {
        let d = sample(3);
        let bytes = d.encode().unwrap();
        let truncated = &bytes[..bytes.len() - 1];
        assert!(LdDescriptor::decode_one(truncated).is_err());
    }
}
