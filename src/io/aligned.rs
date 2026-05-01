use std::alloc::{self, Layout};

use crate::error::{ChunkletError, ChunkletResult};
use crate::types::BLOCK_SIZE;

/// 4 KiB-aligned buffer for O_DIRECT IO.
pub struct AlignedBuf {
    ptr: *mut u8,
    len: usize,
    layout: Layout,
}

// SAFETY: `AlignedBuf` owns its memory and never aliases.
unsafe impl Send for AlignedBuf {}
unsafe impl Sync for AlignedBuf {}

impl AlignedBuf {
    /// Allocate `size` bytes, rounded up to a multiple of `BLOCK_SIZE`.
    pub fn new(size: usize) -> ChunkletResult<Self> {
        let aligned_size = round_up(size, BLOCK_SIZE as usize);
        if aligned_size == 0 {
            return Err(ChunkletError::Config(
                "cannot allocate zero-size aligned buffer".into(),
            ));
        }
        let layout = Layout::from_size_align(aligned_size, BLOCK_SIZE as usize)
            .map_err(|e| ChunkletError::Config(format!("invalid layout: {}", e)))?;
        // SAFETY: layout is valid (size > 0, align is a power of two).
        let ptr = unsafe { alloc::alloc_zeroed(layout) };
        if ptr.is_null() {
            return Err(ChunkletError::Io(std::io::Error::from(
                std::io::ErrorKind::OutOfMemory,
            )));
        }
        Ok(Self {
            ptr,
            len: aligned_size,
            layout,
        })
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn as_slice(&self) -> &[u8] {
        // SAFETY: ptr is valid for self.len bytes.
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }

    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        // SAFETY: ptr is valid for self.len bytes; &mut self enforces uniqueness.
        unsafe { std::slice::from_raw_parts_mut(self.ptr, self.len) }
    }

    pub fn as_ptr(&self) -> *const u8 {
        self.ptr
    }

    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        self.ptr
    }
}

impl Drop for AlignedBuf {
    fn drop(&mut self) {
        // SAFETY: same layout used at allocation time.
        unsafe { alloc::dealloc(self.ptr, self.layout) }
    }
}

pub fn round_up(value: usize, align: usize) -> usize {
    debug_assert!(align.is_power_of_two());
    (value + align - 1) & !(align - 1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn aligned_buf_alloc_round_up() {
        let buf = AlignedBuf::new(1).unwrap();
        assert_eq!(buf.len(), BLOCK_SIZE as usize);
        assert_eq!(buf.as_ptr() as usize % BLOCK_SIZE as usize, 0);
    }

    #[test]
    fn aligned_buf_zero_init() {
        let buf = AlignedBuf::new(8192).unwrap();
        assert!(buf.as_slice().iter().all(|&b| b == 0));
    }

    #[test]
    fn aligned_buf_zero_size_rejected() {
        assert!(AlignedBuf::new(0).is_err());
    }
}
