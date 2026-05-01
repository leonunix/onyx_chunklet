//! `UringBackend` — `io_uring` batched writes (Linux only).
//!
//! One thread-local `IoUring` instance per worker thread. `submit_writes`
//! bounces every `StripWrite` into an `AlignedBuf` (so the data buffer
//! satisfies O_DIRECT alignment when the underlying fd was opened with
//! it), pushes one `IORING_OP_WRITE` SQE per op, calls `submit_and_wait`
//! once, and drains CQEs to surface per-op errors.
//!
//! Two big wins versus `SyncBackend` for a K-strip fan-out:
//! - K thread spawns → 0 (everything runs on the calling thread, kernel
//!   parallelizes).
//! - K `pwrite` syscalls → 1 `io_uring_enter` submit + 1 `io_uring_enter`
//!   wait.
//!
//! Bounce-buffer cost: one `AlignedBuf` allocation (4 KiB-aligned heap
//! alloc) + one memcpy per op. For typical 4-64 KiB strips this is ~µs;
//! still net positive against the spawn savings. P8b-2 may add a fast
//! path that submits the user buffer directly when alignment is already
//! satisfied (typically true under O_DIRECT-capable allocators with
//! 4 KiB fragment alignment).

use std::cell::RefCell;

use io_uring::{opcode, types, IoUring};

use crate::error::{ChunkletError, ChunkletResult};
use crate::io::aligned::AlignedBuf;
use crate::io::backend::{IoBackend, StripWrite};

const URING_DEPTH: u32 = 64;

thread_local! {
    static URING: RefCell<Option<IoUring>> = const { RefCell::new(None) };
}

pub struct UringBackend;

impl UringBackend {
    /// Probe-initialize an `IoUring` instance to verify the kernel
    /// supports it. Returns `Err` on kernels without `io_uring` (pre-5.1)
    /// or with it disabled by sysctl.
    pub fn new() -> std::io::Result<Self> {
        let _probe = IoUring::new(URING_DEPTH)?;
        Ok(UringBackend)
    }
}

impl IoBackend for UringBackend {
    fn name(&self) -> &'static str {
        "uring"
    }

    fn submit_writes(&self, ops: &[StripWrite<'_>]) -> ChunkletResult<()> {
        if ops.is_empty() {
            return Ok(());
        }
        URING.with(|cell| -> ChunkletResult<()> {
            let mut slot = cell.borrow_mut();
            if slot.is_none() {
                *slot = Some(IoUring::new(URING_DEPTH).map_err(|e| {
                    ChunkletError::Io(std::io::Error::other(format!("io_uring init: {}", e)))
                })?);
            }
            let ring = slot.as_mut().expect("ring just initialized");
            for chunk in ops.chunks(URING_DEPTH as usize) {
                submit_chunk(ring, chunk)?;
            }
            Ok(())
        })
    }
}

fn submit_chunk(ring: &mut IoUring, ops: &[StripWrite<'_>]) -> ChunkletResult<()> {
    // SQEs reference these bounce buffers by raw pointer; the Vec is
    // held until the end of the function so the kernel sees them live
    // through `submit_and_wait`.
    let mut bounces: Vec<AlignedBuf> = Vec::with_capacity(ops.len());
    let mut ptrs: Vec<(*const u8, u32)> = Vec::with_capacity(ops.len());
    for op in ops {
        let buf = AlignedBuf::from_slice(op.data)?;
        ptrs.push((buf.as_slice().as_ptr(), op.data.len() as u32));
        bounces.push(buf);
    }

    // Resolve absolute offsets + fds while we still hold &op (PD borrow
    // outlives the submit because `ops` is borrowed for the function).
    let mut targets: Vec<(i32, u64)> = Vec::with_capacity(ops.len());
    for op in ops {
        let abs = op.pd.chunklet_user_abs_offset(
            op.chunklet_index,
            op.in_chunklet_off,
            op.data.len() as u64,
        )?;
        targets.push((op.pd.raw_fd(), abs));
    }

    {
        let mut sq = ring.submission();
        for (i, _op) in ops.iter().enumerate() {
            let (fd, abs) = targets[i];
            let (ptr, len) = ptrs[i];
            let entry = opcode::Write::new(types::Fd(fd), ptr, len)
                .offset(abs)
                .build()
                .user_data(i as u64);
            // SAFETY: bounce buffers in `bounces` outlive this function;
            // `submit_and_wait` blocks until the kernel has consumed
            // every SQE this loop pushed.
            unsafe {
                sq.push(&entry).map_err(|e| {
                    ChunkletError::Io(std::io::Error::other(format!(
                        "io_uring sq push: {}",
                        e
                    )))
                })?;
            }
        }
    }

    ring.submit_and_wait(ops.len()).map_err(|e| {
        ChunkletError::Io(std::io::Error::other(format!(
            "io_uring submit_and_wait: {}",
            e
        )))
    })?;

    let mut first_err: Option<ChunkletError> = None;
    let mut completed = 0;
    for cqe in ring.completion() {
        completed += 1;
        let idx = cqe.user_data() as usize;
        let res = cqe.result();
        if res < 0 {
            let errno = -res;
            if first_err.is_none() {
                first_err = Some(ChunkletError::Io(std::io::Error::from_raw_os_error(
                    errno,
                )));
            }
        } else if (res as u32) < ptrs[idx].1 {
            if first_err.is_none() {
                first_err = Some(ChunkletError::Io(std::io::Error::other(format!(
                    "io_uring short write op_idx={}: {} of {}",
                    idx, res, ptrs[idx].1
                ))));
            }
        }
    }
    if completed < ops.len() {
        return Err(ChunkletError::Io(std::io::Error::other(format!(
            "io_uring expected {} cqes, got {}",
            ops.len(),
            completed
        ))));
    }
    drop(bounces);
    first_err.map_or(Ok(()), Err)
}
