use std::fs::{File, OpenOptions};
use std::os::fd::AsRawFd;
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
}

impl RawDevice {
    /// Open a device or pre-existing file.
    pub fn open(path: &Path) -> ChunkletResult<Self> {
        let (file, direct_io) = Self::open_direct(path)?;
        let size_bytes = Self::query_size(&file, path)?;
        Ok(Self {
            file,
            size_bytes,
            path: path.to_path_buf(),
            direct_io,
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
        let ret = unsafe { libc::ioctl(file.as_raw_fd(), 0x8008_1272 /* BLKGETSIZE64 */, &mut size) };
        if ret != 0 {
            return Err(ChunkletError::Device {
                path: path.to_path_buf(),
                reason: format!(
                    "BLKGETSIZE64 failed: {}",
                    std::io::Error::last_os_error()
                ),
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

    pub fn read_at(&self, buf: &mut [u8], offset: u64) -> ChunkletResult<()> {
        self.bounds_check(offset, buf.len() as u64)?;
        if self.direct_io && self.unaligned(offset, buf.len(), buf.as_ptr() as usize) {
            // Bounce through an aligned buffer.
            let mut aligned = AlignedBuf::new(buf.len())?;
            self.read_loop(aligned.as_mut_slice(), offset)?;
            buf.copy_from_slice(&aligned.as_slice()[..buf.len()]);
            return Ok(());
        }
        self.read_loop(buf, offset)
    }

    pub fn write_at(&self, buf: &[u8], offset: u64) -> ChunkletResult<()> {
        self.bounds_check(offset, buf.len() as u64)?;
        if self.direct_io && self.unaligned(offset, buf.len(), buf.as_ptr() as usize) {
            let mut aligned = AlignedBuf::new(buf.len())?;
            aligned.as_mut_slice()[..buf.len()].copy_from_slice(buf);
            return self.write_loop(aligned.as_slice(), offset);
        }
        self.write_loop(buf, offset)
    }

    pub fn sync(&self) -> ChunkletResult<()> {
        self.file.sync_all().map_err(|e| ChunkletError::Device {
            path: self.path.clone(),
            reason: format!("sync_all: {}", e),
        })
    }

    fn read_loop(&self, buf: &mut [u8], offset: u64) -> ChunkletResult<()> {
        let mut done = 0;
        while done < buf.len() {
            match self
                .file
                .read_at(&mut buf[done..], offset + done as u64)
            {
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
        let end = offset.checked_add(len).ok_or_else(|| ChunkletError::Device {
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
