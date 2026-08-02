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
    /// Memory is zero-initialized — use [`AlignedBuf::from_slice`] when
    /// you'll fully overwrite the buffer right away.
    pub fn new(size: usize) -> ChunkletResult<Self> {
        Self::alloc(size, /* zeroed */ true)
    }

    /// Allocate `size` bytes without the zero-fill, for a caller that fills the
    /// first `size` bytes itself. Only the round-up tail
    /// `[size..round_up(size, BLOCK_SIZE))` is zeroed, so nothing uninitialised
    /// can ever reach the device even when `size` is not a block multiple.
    ///
    /// Exists because the coalesced-write bounce buffer is overwritten byte for
    /// byte immediately after allocation: `new`'s zero-fill was pure waste that
    /// doubled the memory traffic of every merged group.
    pub fn uninit(size: usize) -> ChunkletResult<Self> {
        let mut buf = Self::alloc(size, /* zeroed */ false)?;
        buf.as_mut_slice()[size..].fill(0);
        Ok(buf)
    }

    /// Allocate an aligned buffer sized for `src` and copy `src` into it.
    /// Skips the zero-fill that `new` does, since the contents are
    /// overwritten by the copy. Used by `RawDevice::write_at` and
    /// `UringBackend` for the unaligned-to-aligned bounce path.
    pub fn from_slice(src: &[u8]) -> ChunkletResult<Self> {
        let mut buf = Self::alloc(src.len(), /* zeroed */ false)?;
        buf.as_mut_slice()[..src.len()].copy_from_slice(src);
        buf.as_mut_slice()[src.len()..].fill(0);
        Ok(buf)
    }

    fn alloc(size: usize, zeroed: bool) -> ChunkletResult<Self> {
        let aligned_size = round_up(size, BLOCK_SIZE as usize);
        if aligned_size == 0 {
            return Err(ChunkletError::Config(
                "cannot allocate zero-size aligned buffer".into(),
            ));
        }
        let layout = Layout::from_size_align(aligned_size, BLOCK_SIZE as usize)
            .map_err(|e| ChunkletError::Config(format!("invalid layout: {}", e)))?;
        // SAFETY: layout is valid (size > 0, align is a power of two).
        let ptr = unsafe {
            if zeroed {
                alloc::alloc_zeroed(layout)
            } else {
                alloc::alloc(layout)
            }
        };
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
        assert!(AlignedBuf::uninit(0).is_err());
    }

    /// `uninit` skips the body but MUST still zero the round-up tail: the whole
    /// `len()` is what gets written to the device, so a non-block-multiple
    /// request would otherwise leak allocator garbage to disk.
    #[test]
    fn uninit_zeroes_only_the_round_up_tail() {
        let size = BLOCK_SIZE as usize + 100;
        let mut buf = AlignedBuf::uninit(size).unwrap();
        assert_eq!(buf.len(), 2 * BLOCK_SIZE as usize);
        assert!(
            buf.as_slice()[size..].iter().all(|&b| b == 0),
            "round-up tail must be zeroed"
        );
        // The body is the caller's contract to fill.
        buf.as_mut_slice()[..size].fill(0xab);
        assert!(buf.as_slice()[size..].iter().all(|&b| b == 0));
    }
}
