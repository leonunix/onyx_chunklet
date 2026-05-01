//! Per-chunklet on-disk header.
//!
//! Each chunklet's first 4 KiB is reserved for a header that records who
//! owns it, what role it plays, and a generation counter used by rebuild
//! to distinguish stale data after a crash.
//!
//! The header is **advisory metadata for audit / rebuild** — the bitmap is
//! the authoritative record of allocation. Recovery trusts the bitmap when
//! they disagree; orphan headers (chunklet has header but bitmap says
//! Free) are simply ignored and will be overwritten on next allocation.
//!
//! # Wire format (4 KiB total, only first 64 bytes used; rest reserved)
//!
//! ```text
//! [0..8]   magic "ONYXCKHD"
//! [8..12]  version u32 LE = 1
//! [12..16] reserved
//! [16..32] owner_ld_uuid (16 bytes)
//! [32..36] chunklet_index u32 LE (sanity check vs. PD position)
//! [36..37] role u8 (LdRole)
//! [37..40] reserved
//! [40..48] generation u64 LE
//! [48..52] crc32c (covers [0..48])
//! [52..4096] reserved (zeros)
//! ```

use std::convert::TryInto;

use crate::error::{ChunkletError, ChunkletResult};
use crate::types::{LdId, LdRole, CHUNKLET_HEADER_BYTES};

const HEADER_MAGIC: &[u8; 8] = b"ONYXCKHD";
const HEADER_VERSION: u32 = 1;
const HEADER_USED_BYTES: usize = 52;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ChunkletHeader {
    pub owner_ld: LdId,
    pub chunklet_index: u32,
    pub role: LdRole,
    pub generation: u64,
}

impl ChunkletHeader {
    pub fn encode(&self) -> [u8; CHUNKLET_HEADER_BYTES as usize] {
        let mut out = [0u8; CHUNKLET_HEADER_BYTES as usize];
        out[0..8].copy_from_slice(HEADER_MAGIC);
        out[8..12].copy_from_slice(&HEADER_VERSION.to_le_bytes());
        // [12..16] reserved.
        out[16..32].copy_from_slice(&self.owner_ld.to_bytes());
        out[32..36].copy_from_slice(&self.chunklet_index.to_le_bytes());
        out[36] = self.role as u8;
        // [37..40] reserved.
        out[40..48].copy_from_slice(&self.generation.to_le_bytes());
        let crc = crc32c::crc32c(&out[..48]);
        out[48..52].copy_from_slice(&crc.to_le_bytes());
        out
    }

    pub fn decode(bytes: &[u8]) -> ChunkletResult<Self> {
        if bytes.len() < HEADER_USED_BYTES {
            return Err(ChunkletError::Format(format!(
                "chunklet header truncated: {} bytes",
                bytes.len()
            )));
        }
        if &bytes[0..8] != HEADER_MAGIC {
            return Err(ChunkletError::Format(format!(
                "bad chunklet header magic: {:x?}",
                &bytes[0..8]
            )));
        }
        let version = u32::from_le_bytes(bytes[8..12].try_into().unwrap());
        if version != HEADER_VERSION {
            return Err(ChunkletError::Format(format!(
                "unsupported chunklet header version: {}",
                version
            )));
        }
        let stored_crc = u32::from_le_bytes(bytes[48..52].try_into().unwrap());
        let computed = crc32c::crc32c(&bytes[..48]);
        if stored_crc != computed {
            return Err(ChunkletError::Crc {
                what: "chunklet header".into(),
                stored: stored_crc,
                computed,
            });
        }
        Ok(Self {
            owner_ld: LdId::from_bytes(bytes[16..32].try_into().unwrap()),
            chunklet_index: u32::from_le_bytes(bytes[32..36].try_into().unwrap()),
            role: LdRole::from_u8(bytes[36])?,
            generation: u64::from_le_bytes(bytes[40..48].try_into().unwrap()),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_round_trip() {
        let h = ChunkletHeader {
            owner_ld: LdId::new_v4(),
            chunklet_index: 7,
            role: LdRole::ParityP,
            generation: 42,
        };
        let bytes = h.encode();
        let decoded = ChunkletHeader::decode(&bytes).unwrap();
        assert_eq!(decoded, h);
    }

    #[test]
    fn rejects_corrupt_magic() {
        let h = ChunkletHeader {
            owner_ld: LdId::new_v4(),
            chunklet_index: 0,
            role: LdRole::Data,
            generation: 1,
        };
        let mut bytes = h.encode();
        bytes[0] = 0;
        assert!(ChunkletHeader::decode(&bytes).is_err());
    }

    #[test]
    fn rejects_bit_flip() {
        let h = ChunkletHeader {
            owner_ld: LdId::new_v4(),
            chunklet_index: 0,
            role: LdRole::Data,
            generation: 1,
        };
        let mut bytes = h.encode();
        bytes[20] ^= 0x01;
        let err = ChunkletHeader::decode(&bytes).err().unwrap();
        assert!(matches!(err, ChunkletError::Crc { .. }));
    }
}
