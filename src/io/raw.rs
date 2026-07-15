use std::fs::{File, OpenOptions};
use std::os::fd::{AsRawFd, RawFd};
use std::os::unix::fs::{FileExt, OpenOptionsExt};
use std::path::{Path, PathBuf};

use crate::error::{ChunkletError, ChunkletResult};
use crate::io::aligned::AlignedBuf;
use crate::types::BLOCK_SIZE;

/// Raw block device or sparse-file handle for O_DIRECT IO.
///
/// Tries `O_DIRECT` first; falls back to buffered IO when the underlying fs
/// rejects it (regular files on overlayfs / tmpfs in tests). All public
/// `read_at` / `write_at` calls loop until the full buffer is transferred,
/// retry `EINTR`, and surface short transfers as errors.
pub struct RawDevice {
    file: File,
    size_bytes: u64,
    path: PathBuf,
    direct_io: bool,
    sync_required: bool,
}

impl RawDevice {
    /// Open a device or pre-existing file.
    pub fn open(path: &Path) -> ChunkletResult<Self> {
        let (file, direct_io) = Self::open_direct(path)?;
        let size_bytes = Self::query_size(&file, path)?;
        let sync_required = Self::detect_sync_required(&file, path);
        Ok(Self {
            file,
            size_bytes,
            path: path.to_path_buf(),
            direct_io,
            sync_required,
        })
    }

    /// Create a sparse file of `size_bytes` if it does not exist (test helper).
    pub fn open_or_create(path: &Path, size_bytes: u64) -> ChunkletResult<Self> {
        if !path.exists() {
            let f = File::create(path).map_err(|e| ChunkletError::Device {
                path: path.to_path_buf(),
                reason: format!("create: {}", e),
            })?;
            f.set_len(size_bytes).map_err(|e| ChunkletError::Device {
                path: path.to_path_buf(),
                reason: format!("set_len: {}", e),
            })?;
        }
        Self::open(path)
    }

    fn open_direct(path: &Path) -> ChunkletResult<(File, bool)> {
        match OpenOptions::new()
            .read(true)
            .write(true)
            .custom_flags(libc::O_DIRECT)
            .open(path)
        {
            Ok(f) => Ok((f, true)),
            Err(_) => {
                let f = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(path)
                    .map_err(|e| ChunkletError::Device {
                        path: path.to_path_buf(),
                        reason: format!("open: {}", e),
                    })?;
                Ok((f, false))
            }
        }
    }

    fn query_size(file: &File, path: &Path) -> ChunkletResult<u64> {
        let meta = file.metadata().map_err(|e| ChunkletError::Device {
            path: path.to_path_buf(),
            reason: format!("metadata: {}", e),
        })?;
        if meta.file_type().is_file() {
            return Ok(meta.len());
        }
        // Block device: ask the kernel for capacity via BLKGETSIZE64.
        let mut size: u64 = 0;
        // SAFETY: BLKGETSIZE64 takes *mut u64 and our fd is valid.
        let ret = unsafe {
            libc::ioctl(
                file.as_raw_fd(),
                0x8008_1272, /* BLKGETSIZE64 */
                &mut size,
            )
        };
        if ret != 0 {
            return Err(ChunkletError::Device {
                path: path.to_path_buf(),
                reason: format!("BLKGETSIZE64 failed: {}", std::io::Error::last_os_error()),
            });
        }
        Ok(size)
    }

    pub fn size(&self) -> u64 {
        self.size_bytes
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn is_direct_io(&self) -> bool {
        self.direct_io
    }

    /// Whether this device needs an explicit cache flush after completed
    /// O_DIRECT writes. Unknown modes remain conservative.
    pub fn sync_required(&self) -> bool {
        self.sync_required
    }

    #[cfg(test)]
    pub(crate) fn set_sync_required_for_test(&mut self, required: bool) {
        self.sync_required = required;
    }

    pub fn read_at(&self, buf: &mut [u8], offset: u64) -> ChunkletResult<()> {
        if buf.is_empty() {
            return Ok(());
        }
        self.bounds_check(offset, buf.len() as u64)?;
        if self.direct_io && self.unaligned(offset, buf.len(), buf.as_ptr() as usize) {
            let (aligned_offset, delta, aligned_len) = aligned_io_window(offset, buf.len() as u64)?;
            self.bounds_check(aligned_offset, aligned_len as u64)?;
            let mut aligned = AlignedBuf::new(aligned_len)?;
            self.read_loop(aligned.as_mut_slice(), aligned_offset)?;
            buf.copy_from_slice(&aligned.as_slice()[delta..delta + buf.len()]);
            return Ok(());
        }
        self.read_loop(buf, offset)
    }

    pub fn write_at(&self, buf: &[u8], offset: u64) -> ChunkletResult<()> {
        if buf.is_empty() {
            return Ok(());
        }
        self.bounds_check(offset, buf.len() as u64)?;
        if self.direct_io && self.unaligned(offset, buf.len(), buf.as_ptr() as usize) {
            let (aligned_offset, delta, aligned_len) = aligned_io_window(offset, buf.len() as u64)?;
            self.bounds_check(aligned_offset, aligned_len as u64)?;
            let mut aligned = AlignedBuf::new(aligned_len)?;
            self.read_loop(aligned.as_mut_slice(), aligned_offset)?;
            aligned.as_mut_slice()[delta..delta + buf.len()].copy_from_slice(buf);
            return self.write_loop(aligned.as_slice(), aligned_offset);
        }
        self.write_loop(buf, offset)
    }

    pub fn sync(&self) -> ChunkletResult<()> {
        if !self.sync_required {
            return Ok(());
        }
        loop {
            match self.file.sync_all() {
                Ok(()) => return Ok(()),
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(error) => {
                    return Err(ChunkletError::Device {
                        path: self.path.clone(),
                        reason: format!("sync_all: {error}"),
                    });
                }
            }
        }
    }

    fn detect_sync_required(file: &File, path: &Path) -> bool {
        use std::os::unix::fs::FileTypeExt;

        let Ok(metadata) = file.metadata() else {
            return true;
        };
        if !metadata.file_type().is_block_device() {
            return true;
        }
        let canonical = std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
        let Some(block_name) = canonical.file_name() else {
            return true;
        };
        let mode_path = Path::new("/sys/class/block")
            .join(block_name)
            .join("queue/write_cache");
        let Ok(mode) = std::fs::read_to_string(mode_path) else {
            return true;
        };
        let write_through = !Self::cache_mode_requires_sync(&mode);
        if write_through {
            tracing::info!(
                path = %path.display(),
                "block device is write-through; explicit cache flush is unnecessary"
            );
        }
        !write_through
    }

    fn cache_mode_requires_sync(mode: &str) -> bool {
        !mode.trim().eq_ignore_ascii_case("write through")
    }

    fn read_loop(&self, buf: &mut [u8], offset: u64) -> ChunkletResult<()> {
        let mut done = 0;
        while done < buf.len() {
            match self.file.read_at(&mut buf[done..], offset + done as u64) {
                Ok(0) => {
                    return Err(ChunkletError::Device {
                        path: self.path.clone(),
                        reason: format!(
                            "read_at offset={} short read after {}/{}",
                            offset,
                            done,
                            buf.len()
                        ),
                    });
                }
                Ok(n) => done += n,
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(e) => {
                    return Err(ChunkletError::Device {
                        path: self.path.clone(),
                        reason: format!("read_at offset={}: {}", offset, e),
                    });
                }
            }
        }
        Ok(())
    }

    fn write_loop(&self, buf: &[u8], offset: u64) -> ChunkletResult<()> {
        let mut done = 0;
        while done < buf.len() {
            match self.file.write_at(&buf[done..], offset + done as u64) {
                Ok(0) => {
                    return Err(ChunkletError::Device {
                        path: self.path.clone(),
                        reason: format!(
                            "write_at offset={} zero-length write after {}/{}",
                            offset,
                            done,
                            buf.len()
                        ),
                    });
                }
                Ok(n) => done += n,
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(e) => {
                    return Err(ChunkletError::Device {
                        path: self.path.clone(),
                        reason: format!("write_at offset={}: {}", offset, e),
                    });
                }
            }
        }
        Ok(())
    }

    fn bounds_check(&self, offset: u64, len: u64) -> ChunkletResult<()> {
        let end = offset
            .checked_add(len)
            .ok_or_else(|| ChunkletError::Device {
                path: self.path.clone(),
                reason: format!("offset overflow: offset={} len={}", offset, len),
            })?;
        if end > self.size_bytes {
            return Err(ChunkletError::Device {
                path: self.path.clone(),
                reason: format!(
                    "out-of-bounds IO: offset={} len={} size={}",
                    offset, len, self.size_bytes
                ),
            });
        }
        Ok(())
    }

    fn unaligned(&self, offset: u64, len: usize, ptr_addr: usize) -> bool {
        let bs = BLOCK_SIZE as usize;
        offset % BLOCK_SIZE != 0 || len % bs != 0 || ptr_addr % bs != 0
    }
}

fn aligned_io_window(offset: u64, len: u64) -> ChunkletResult<(u64, usize, usize)> {
    let bs = BLOCK_SIZE;
    let aligned_offset = offset / bs * bs;
    let end = offset
        .checked_add(len)
        .ok_or_else(|| ChunkletError::Io(std::io::Error::other("aligned IO offset overflow")))?;
    let aligned_end = end
        .checked_add(bs - 1)
        .ok_or_else(|| ChunkletError::Io(std::io::Error::other("aligned IO end overflow")))?
        / bs
        * bs;
    let aligned_len = usize::try_from(aligned_end - aligned_offset)
        .map_err(|_| ChunkletError::Io(std::io::Error::other("aligned IO length overflow")))?;
    let delta = usize::try_from(offset - aligned_offset)
        .map_err(|_| ChunkletError::Io(std::io::Error::other("aligned IO delta overflow")))?;
    Ok((aligned_offset, delta, aligned_len))
}

impl AsRawFd for RawDevice {
    fn as_raw_fd(&self) -> RawFd {
        self.file.as_raw_fd()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cache_mode_only_skips_explicit_write_through() {
        assert!(!RawDevice::cache_mode_requires_sync("write through\n"));
        assert!(!RawDevice::cache_mode_requires_sync("WRITE THROUGH"));
        assert!(RawDevice::cache_mode_requires_sync("write back\n"));
        assert!(RawDevice::cache_mode_requires_sync("unknown"));
    }

    #[test]
    fn regular_files_always_require_sync() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("pd");
        let raw = RawDevice::open_or_create(&path, 8 * 1024 * 1024).unwrap();
        assert!(raw.sync_required());
    }
}
