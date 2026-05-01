//! Per-PD superblock COW pair encoding.
//!
//! A PD reserves the first `PD_RESERVED_BYTES` (1 MiB) and the last
//! `PD_RESERVED_BYTES` (1 MiB tail mirror). Within each region:
//!
//! ```text
//! offset 0          : superblock slot A (4 KiB)
//! offset 4 KiB      : superblock slot B (4 KiB)
//! offset 8 KiB      : bitmap slot A (256 KiB)
//! offset 264 KiB    : bitmap slot B (256 KiB)
//! offset 520 KiB    : reserved
//! ```
//!
//! Both head and tail carry full mirrors. Recovery reads all 4 slots and picks
//! the highest `manifest_gen` with valid CRC.
//!
//! # Slot layout (4096 bytes)
//!
//! ```text
//! [0..8]     magic "ONYXCHK1"
//! [8..12]    version u32 LE
//! [12..16]   reserved
//! [16..32]   pool_id  (16 bytes UUID)
//! [32..48]   pd_id    (16 bytes UUID)
//! [48..56]   manifest_gen u64 LE
//! [56..60]   body_len u32 LE
//! [60..64]   reserved
//! [64..4092] body (up to 4028 bytes)
//! [4092..4096] crc32c u32 LE  (covers [0..4092])
//! ```
//!
//! # Body layout (header + variable lists)
//!
//! See `BodyHeader` below. Lists (PDs, LDs) are placed after the body header
//! at offsets recorded in the header. P0 only writes the PD list; LD list is
//! empty until P1.

use std::convert::TryInto;

use crate::error::{ChunkletError, ChunkletResult};
use crate::types::{
    PdId, PoolId, BLOCK_SIZE, CHUNKLET_SIZE_LOG2, SUPERBLOCK_MAGIC, SUPERBLOCK_VERSION,
};

/// Total slot size on disk.
pub const SLOT_BYTES: usize = BLOCK_SIZE as usize;

/// Slot header bytes preceding the body.
pub const SLOT_HEADER_BYTES: usize = 64;
/// Trailer bytes (CRC).
pub const SLOT_CRC_BYTES: usize = 4;
/// Maximum body bytes per slot.
pub const MAX_BODY_BYTES: usize = SLOT_BYTES - SLOT_HEADER_BYTES - SLOT_CRC_BYTES;

const PD_LIST_ENTRY_BYTES: usize = 24;

/// In-memory representation of a fully decoded slot.
#[derive(Clone, Debug)]
pub struct SuperblockSlot {
    pub pool_id: PoolId,
    pub pd_id: PdId,
    pub manifest_gen: u64,
    pub body: SuperblockBody,
}

/// Body fields (decoded). Lists are owned `Vec`s; `bitmap_crc32c` is the CRC of
/// the on-disk bitmap region (which lives outside the slot).
#[derive(Clone, Debug)]
pub struct SuperblockBody {
    pub chunklet_size_log2: u8,
    pub spare_pct: u8,
    pub pd_size_blocks: u64,
    pub reserved_head_blocks: u32,
    pub reserved_tail_blocks: u32,
    pub total_chunklets: u32,
    pub pd_seq_in_pool: u32,
    pub pool_pd_count: u32,
    pub bitmap_crc32c: u32,
    /// Which bitmap slot (0 = slot A, 1 = slot B) this superblock points to.
    /// Updated by every manifest commit so readers can locate the live bitmap.
    pub bitmap_slot_id: u8,
    pub pd_list: Vec<PoolPdEntry>,
    /// Reserved for P1+: opaque LD descriptor bytes.
    pub ld_list_bytes: Vec<u8>,
}

/// One entry per PD in the pool, recorded inside every PD's slot for
/// cross-checking on `Pool::scan`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PoolPdEntry {
    pub pd_id: PdId,
    pub pd_seq: u32,
    pub flags: u32,
}

/// `PoolPdEntry::flags` bit positions. Phase 7 will add drain / fail handling;
/// Phase 0 always writes 0.
pub mod pool_pd_flags {
    pub const DRAINED: u32 = 1 << 0;
    pub const FAILED: u32 = 1 << 1;
}

impl SuperblockSlot {
    pub fn encode(&self) -> ChunkletResult<[u8; SLOT_BYTES]> {
        let mut out = [0u8; SLOT_BYTES];
        // Header.
        out[0..8].copy_from_slice(SUPERBLOCK_MAGIC);
        out[8..12].copy_from_slice(&SUPERBLOCK_VERSION.to_le_bytes());
        // [12..16] reserved.
        out[16..32].copy_from_slice(&self.pool_id.to_bytes());
        out[32..48].copy_from_slice(&self.pd_id.to_bytes());
        out[48..56].copy_from_slice(&self.manifest_gen.to_le_bytes());

        // Body.
        let body_bytes = self.body.encode()?;
        if body_bytes.len() > MAX_BODY_BYTES {
            return Err(ChunkletError::Format(format!(
                "encoded body {} bytes exceeds slot capacity {}",
                body_bytes.len(),
                MAX_BODY_BYTES
            )));
        }
        out[56..60].copy_from_slice(&(body_bytes.len() as u32).to_le_bytes());
        // [60..64] reserved.
        let body_start = SLOT_HEADER_BYTES;
        out[body_start..body_start + body_bytes.len()].copy_from_slice(&body_bytes);

        // CRC over [0..4092].
        let crc = crc32c::crc32c(&out[..SLOT_BYTES - SLOT_CRC_BYTES]);
        out[SLOT_BYTES - SLOT_CRC_BYTES..].copy_from_slice(&crc.to_le_bytes());
        Ok(out)
    }

    pub fn decode(bytes: &[u8]) -> ChunkletResult<Self> {
        if bytes.len() < SLOT_BYTES {
            return Err(ChunkletError::Format(format!(
                "slot bytes len {} < {}",
                bytes.len(),
                SLOT_BYTES
            )));
        }
        let bytes = &bytes[..SLOT_BYTES];

        // Verify CRC first (cheap; rejects torn / corrupt slots).
        let stored = u32::from_le_bytes(bytes[SLOT_BYTES - SLOT_CRC_BYTES..].try_into().unwrap());
        let computed = crc32c::crc32c(&bytes[..SLOT_BYTES - SLOT_CRC_BYTES]);
        if stored != computed {
            return Err(ChunkletError::Crc {
                what: "superblock slot".into(),
                stored,
                computed,
            });
        }

        if &bytes[0..8] != SUPERBLOCK_MAGIC {
            return Err(ChunkletError::Format(format!(
                "bad superblock magic: {:x?}",
                &bytes[0..8]
            )));
        }
        let version = u32::from_le_bytes(bytes[8..12].try_into().unwrap());
        if version != SUPERBLOCK_VERSION {
            return Err(ChunkletError::Format(format!(
                "unsupported superblock version: {}",
                version
            )));
        }
        let pool_id = PoolId::from_bytes(bytes[16..32].try_into().unwrap());
        let pd_id = PdId::from_bytes(bytes[32..48].try_into().unwrap());
        let manifest_gen = u64::from_le_bytes(bytes[48..56].try_into().unwrap());
        let body_len = u32::from_le_bytes(bytes[56..60].try_into().unwrap()) as usize;
        if body_len > MAX_BODY_BYTES {
            return Err(ChunkletError::Format(format!(
                "body_len {} > MAX_BODY_BYTES {}",
                body_len, MAX_BODY_BYTES
            )));
        }
        let body_start = SLOT_HEADER_BYTES;
        let body =
            SuperblockBody::decode(&bytes[body_start..body_start + body_len])?;
        Ok(Self {
            pool_id,
            pd_id,
            manifest_gen,
            body,
        })
    }
}

impl SuperblockBody {
    pub fn new_empty(
        pd_size_blocks: u64,
        reserved_head_blocks: u32,
        reserved_tail_blocks: u32,
        total_chunklets: u32,
        pd_seq_in_pool: u32,
        pool_pd_count: u32,
        spare_pct: u8,
    ) -> Self {
        Self {
            chunklet_size_log2: CHUNKLET_SIZE_LOG2,
            spare_pct,
            pd_size_blocks,
            reserved_head_blocks,
            reserved_tail_blocks,
            total_chunklets,
            pd_seq_in_pool,
            pool_pd_count,
            bitmap_crc32c: 0,
            bitmap_slot_id: 0,
            pd_list: Vec::new(),
            ld_list_bytes: Vec::new(),
        }
    }

    pub fn encode(&self) -> ChunkletResult<Vec<u8>> {
        // Body header (64 bytes).
        let mut header = [0u8; 64];
        header[0] = self.chunklet_size_log2;
        header[1] = self.spare_pct;
        header[2] = self.bitmap_slot_id;
        // [3..4] reserved.
        header[4..12].copy_from_slice(&self.pd_size_blocks.to_le_bytes());
        header[12..16].copy_from_slice(&self.reserved_head_blocks.to_le_bytes());
        header[16..20].copy_from_slice(&self.reserved_tail_blocks.to_le_bytes());
        header[20..24].copy_from_slice(&self.total_chunklets.to_le_bytes());
        header[24..28].copy_from_slice(&self.pd_seq_in_pool.to_le_bytes());
        header[28..32].copy_from_slice(&self.pool_pd_count.to_le_bytes());
        header[32..36].copy_from_slice(&self.bitmap_crc32c.to_le_bytes());

        let pd_list_offset = 64u32;
        let pd_list_bytes = self.pd_list.len() * PD_LIST_ENTRY_BYTES;
        let ld_list_offset = pd_list_offset + pd_list_bytes as u32;
        header[36..40].copy_from_slice(&pd_list_offset.to_le_bytes());
        header[40..44].copy_from_slice(&(self.pd_list.len() as u32).to_le_bytes());
        header[44..48].copy_from_slice(&ld_list_offset.to_le_bytes());
        header[48..52].copy_from_slice(&(self.ld_list_bytes.len() as u32).to_le_bytes());
        // [52..64] reserved.

        let mut out = Vec::with_capacity(64 + pd_list_bytes + self.ld_list_bytes.len());
        out.extend_from_slice(&header);
        for entry in &self.pd_list {
            let mut buf = [0u8; PD_LIST_ENTRY_BYTES];
            buf[0..16].copy_from_slice(&entry.pd_id.to_bytes());
            buf[16..20].copy_from_slice(&entry.pd_seq.to_le_bytes());
            buf[20..24].copy_from_slice(&entry.flags.to_le_bytes());
            out.extend_from_slice(&buf);
        }
        out.extend_from_slice(&self.ld_list_bytes);
        Ok(out)
    }

    pub fn decode(bytes: &[u8]) -> ChunkletResult<Self> {
        if bytes.len() < 64 {
            return Err(ChunkletError::Format(format!(
                "body header truncated: {} bytes",
                bytes.len()
            )));
        }
        let chunklet_size_log2 = bytes[0];
        if chunklet_size_log2 != CHUNKLET_SIZE_LOG2 {
            return Err(ChunkletError::Format(format!(
                "unsupported chunklet_size_log2 {} (expected {})",
                chunklet_size_log2, CHUNKLET_SIZE_LOG2
            )));
        }
        let spare_pct = bytes[1];
        let bitmap_slot_id = bytes[2];
        if bitmap_slot_id > 1 {
            return Err(ChunkletError::Format(format!(
                "bitmap_slot_id out of range: {}",
                bitmap_slot_id
            )));
        }
        let pd_size_blocks = u64::from_le_bytes(bytes[4..12].try_into().unwrap());
        let reserved_head_blocks = u32::from_le_bytes(bytes[12..16].try_into().unwrap());
        let reserved_tail_blocks = u32::from_le_bytes(bytes[16..20].try_into().unwrap());
        let total_chunklets = u32::from_le_bytes(bytes[20..24].try_into().unwrap());
        let pd_seq_in_pool = u32::from_le_bytes(bytes[24..28].try_into().unwrap());
        let pool_pd_count = u32::from_le_bytes(bytes[28..32].try_into().unwrap());
        let bitmap_crc32c = u32::from_le_bytes(bytes[32..36].try_into().unwrap());

        let pd_list_offset = u32::from_le_bytes(bytes[36..40].try_into().unwrap()) as usize;
        let pd_list_len = u32::from_le_bytes(bytes[40..44].try_into().unwrap()) as usize;
        let ld_list_offset = u32::from_le_bytes(bytes[44..48].try_into().unwrap()) as usize;
        let ld_list_bytes_len = u32::from_le_bytes(bytes[48..52].try_into().unwrap()) as usize;

        let pd_list_end = pd_list_offset
            .checked_add(pd_list_len.checked_mul(PD_LIST_ENTRY_BYTES).ok_or_else(|| {
                ChunkletError::Format("pd_list size overflow".into())
            })?)
            .ok_or_else(|| ChunkletError::Format("pd_list end overflow".into()))?;
        if pd_list_end > bytes.len() {
            return Err(ChunkletError::Format(format!(
                "pd_list out of bounds: end={} body_len={}",
                pd_list_end,
                bytes.len()
            )));
        }
        let mut pd_list = Vec::with_capacity(pd_list_len);
        for i in 0..pd_list_len {
            let off = pd_list_offset + i * PD_LIST_ENTRY_BYTES;
            let entry = PoolPdEntry {
                pd_id: PdId::from_bytes(bytes[off..off + 16].try_into().unwrap()),
                pd_seq: u32::from_le_bytes(bytes[off + 16..off + 20].try_into().unwrap()),
                flags: u32::from_le_bytes(bytes[off + 20..off + 24].try_into().unwrap()),
            };
            pd_list.push(entry);
        }

        let ld_list_end = ld_list_offset.checked_add(ld_list_bytes_len).ok_or_else(|| {
            ChunkletError::Format("ld_list end overflow".into())
        })?;
        if ld_list_end > bytes.len() {
            return Err(ChunkletError::Format(format!(
                "ld_list out of bounds: end={} body_len={}",
                ld_list_end,
                bytes.len()
            )));
        }
        let ld_list_bytes = bytes[ld_list_offset..ld_list_end].to_vec();

        Ok(Self {
            chunklet_size_log2,
            spare_pct,
            bitmap_slot_id,
            pd_size_blocks,
            reserved_head_blocks,
            reserved_tail_blocks,
            total_chunklets,
            pd_seq_in_pool,
            pool_pd_count,
            bitmap_crc32c,
            pd_list,
            ld_list_bytes,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::PdId;

    fn sample_body() -> SuperblockBody {
        let mut body = SuperblockBody::new_empty(
            1024 * 1024,    // pd_size_blocks (4 GiB)
            256, 256,
            3,              // total_chunklets
            0,              // pd_seq_in_pool
            2,              // pool_pd_count
            5,              // spare_pct
        );
        body.bitmap_crc32c = 0xdead_beef;
        body.pd_list = vec![
            PoolPdEntry { pd_id: PdId::new_v4(), pd_seq: 0, flags: 0 },
            PoolPdEntry { pd_id: PdId::new_v4(), pd_seq: 1, flags: 0 },
        ];
        body
    }

    #[test]
    fn slot_round_trip() {
        let slot = SuperblockSlot {
            pool_id: PoolId::new_v4(),
            pd_id: PdId::new_v4(),
            manifest_gen: 42,
            body: sample_body(),
        };
        let bytes = slot.encode().unwrap();
        let decoded = SuperblockSlot::decode(&bytes).unwrap();
        assert_eq!(decoded.pool_id, slot.pool_id);
        assert_eq!(decoded.pd_id, slot.pd_id);
        assert_eq!(decoded.manifest_gen, slot.manifest_gen);
        assert_eq!(decoded.body.spare_pct, 5);
        assert_eq!(decoded.body.bitmap_crc32c, 0xdead_beef);
        assert_eq!(decoded.body.pd_list, slot.body.pd_list);
    }

    #[test]
    fn crc_detects_bit_flip() {
        let slot = SuperblockSlot {
            pool_id: PoolId::new_v4(),
            pd_id: PdId::new_v4(),
            manifest_gen: 1,
            body: sample_body(),
        };
        let mut bytes = slot.encode().unwrap();
        bytes[100] ^= 1;
        let err = SuperblockSlot::decode(&bytes).unwrap_err();
        assert!(matches!(err, ChunkletError::Crc { .. }));
    }

    #[test]
    fn rejects_bad_magic() {
        let slot = SuperblockSlot {
            pool_id: PoolId::new_v4(),
            pd_id: PdId::new_v4(),
            manifest_gen: 1,
            body: sample_body(),
        };
        let mut bytes = slot.encode().unwrap();
        bytes[0] = 0xff;
        // Need to recompute CRC so we hit the magic check, not the CRC check.
        let crc = crc32c::crc32c(&bytes[..SLOT_BYTES - SLOT_CRC_BYTES]);
        bytes[SLOT_BYTES - SLOT_CRC_BYTES..].copy_from_slice(&crc.to_le_bytes());
        let err = SuperblockSlot::decode(&bytes).unwrap_err();
        assert!(matches!(err, ChunkletError::Format(_)));
    }

    #[test]
    fn rejects_oversized_body() {
        // Push pd_list past slot capacity.
        let mut body = sample_body();
        body.pd_list = (0..200)
            .map(|i| PoolPdEntry {
                pd_id: PdId::new_v4(),
                pd_seq: i,
                flags: 0,
            })
            .collect();
        let slot = SuperblockSlot {
            pool_id: PoolId::new_v4(),
            pd_id: PdId::new_v4(),
            manifest_gen: 1,
            body,
        };
        assert!(slot.encode().is_err());
    }
}
