//! Per-PD chunklet bitmap.
//!
//! Encoded as 1 byte per chunklet (state enum). The on-disk region is sized
//! at `BITMAP_SLOT_BYTES` (256 KiB), padded with zeros (= Free state) for
//! unused entries. CRC32C of the on-disk bytes is stored in the superblock.
//!
//! Two slots A and B per PD (head + tail mirror = 4 total). Writes use the
//! same COW pattern as the superblock: write the inactive slot, fsync, then
//! commit by bumping `manifest_gen` in the superblock that references the new
//! slot's CRC.

use crate::error::ChunkletResult;
use crate::types::{ChunkletState, BITMAP_SLOT_BYTES, MAX_CHUNKLETS_PER_PD};

#[derive(Clone, Debug)]
pub struct Bitmap {
    /// One byte per chunklet (state enum).
    states: Vec<u8>,
}

impl Bitmap {
    pub fn new(total_chunklets: u32) -> ChunkletResult<Self> {
        if total_chunklets > MAX_CHUNKLETS_PER_PD {
            return Err(crate::ChunkletError::Config(format!(
                "{} chunklets > MAX_CHUNKLETS_PER_PD ({})",
                total_chunklets, MAX_CHUNKLETS_PER_PD
            )));
        }
        Ok(Self {
            states: vec![ChunkletState::Free as u8; total_chunklets as usize],
        })
    }

    pub fn len(&self) -> u32 {
        self.states.len() as u32
    }

    pub fn is_empty(&self) -> bool {
        self.states.is_empty()
    }

    pub fn get(&self, index: u32) -> ChunkletResult<ChunkletState> {
        let idx = self.bounded(index)?;
        ChunkletState::from_byte(self.states[idx])
    }

    pub fn set(&mut self, index: u32, state: ChunkletState) -> ChunkletResult<()> {
        let idx = self.bounded(index)?;
        self.states[idx] = state as u8;
        Ok(())
    }

    pub fn count(&self, state: ChunkletState) -> u32 {
        let needle = state as u8;
        self.states.iter().filter(|&&b| b == needle).count() as u32
    }

    /// Encode to the fixed 256 KiB on-disk slot bytes.
    pub fn encode(&self) -> [u8; BITMAP_SLOT_BYTES as usize] {
        let mut out = [0u8; BITMAP_SLOT_BYTES as usize];
        out[..self.states.len()].copy_from_slice(&self.states);
        out
    }

    /// Decode from on-disk slot bytes. Validates each byte is a known state.
    pub fn decode(slot_bytes: &[u8], total_chunklets: u32) -> ChunkletResult<Self> {
        if slot_bytes.len() < total_chunklets as usize {
            return Err(crate::ChunkletError::Format(format!(
                "bitmap slot len {} < total_chunklets {}",
                slot_bytes.len(),
                total_chunklets
            )));
        }
        let states = slot_bytes[..total_chunklets as usize].to_vec();
        // Validate each byte.
        for (i, &b) in states.iter().enumerate() {
            ChunkletState::from_byte(b).map_err(|e| {
                crate::ChunkletError::Format(format!("bitmap[{}]: {}", i, e))
            })?;
        }
        // Tail bytes (padding) must be zero.
        for (i, &b) in slot_bytes[total_chunklets as usize..]
            .iter()
            .enumerate()
            .take(64)
        {
            if b != 0 {
                return Err(crate::ChunkletError::Format(format!(
                    "non-zero padding byte at offset {}: {}",
                    total_chunklets as usize + i,
                    b
                )));
            }
        }
        Ok(Self { states })
    }

    /// CRC of the encoded slot (256 KiB region). Stored in the superblock.
    pub fn crc32c(&self) -> u32 {
        crc32c::crc32c(&self.encode())
    }

    fn bounded(&self, index: u32) -> ChunkletResult<usize> {
        if index >= self.states.len() as u32 {
            return Err(crate::ChunkletError::Invariant(format!(
                "bitmap index {} >= len {}",
                index,
                self.states.len()
            )));
        }
        Ok(index as usize)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fresh_bitmap_all_free() {
        let bm = Bitmap::new(100).unwrap();
        assert_eq!(bm.count(ChunkletState::Free), 100);
        assert_eq!(bm.count(ChunkletState::Used), 0);
    }

    #[test]
    fn set_and_get() {
        let mut bm = Bitmap::new(8).unwrap();
        bm.set(3, ChunkletState::Used).unwrap();
        bm.set(5, ChunkletState::Spare).unwrap();
        assert_eq!(bm.get(3).unwrap(), ChunkletState::Used);
        assert_eq!(bm.get(5).unwrap(), ChunkletState::Spare);
        assert_eq!(bm.count(ChunkletState::Free), 6);
    }

    #[test]
    fn encode_decode_round_trip() {
        let mut bm = Bitmap::new(50).unwrap();
        bm.set(0, ChunkletState::Used).unwrap();
        bm.set(1, ChunkletState::Spare).unwrap();
        bm.set(2, ChunkletState::Bad).unwrap();
        bm.set(49, ChunkletState::Migrating).unwrap();
        let bytes = bm.encode();
        let decoded = Bitmap::decode(&bytes, 50).unwrap();
        assert_eq!(decoded.get(0).unwrap(), ChunkletState::Used);
        assert_eq!(decoded.get(49).unwrap(), ChunkletState::Migrating);
        assert_eq!(decoded.count(ChunkletState::Free), 46);
    }

    #[test]
    fn out_of_bounds_index_rejected() {
        let bm = Bitmap::new(4).unwrap();
        assert!(bm.get(4).is_err());
    }

    #[test]
    fn rejects_unknown_state_byte() {
        let mut bytes = [0u8; BITMAP_SLOT_BYTES as usize];
        bytes[3] = 99; // not a valid state
        let err = Bitmap::decode(&bytes, 10).unwrap_err();
        assert!(matches!(err, crate::ChunkletError::Format(_)));
    }

    #[test]
    fn rejects_non_zero_padding() {
        let mut bytes = [0u8; BITMAP_SLOT_BYTES as usize];
        bytes[10] = 1; // beyond total_chunklets=4
        let err = Bitmap::decode(&bytes, 4).unwrap_err();
        assert!(matches!(err, crate::ChunkletError::Format(_)));
    }
}
