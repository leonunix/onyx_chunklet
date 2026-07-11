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
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use io_uring::{opcode, types, IoUring};

use crate::error::{ChunkletError, ChunkletResult};
use crate::io::aligned::AlignedBuf;
use crate::io::backend::{IoBackend, StripRead, StripWrite};
use crate::io::sync_backend::SyncBackend;
use crate::pd::PhysicalDisk;
use crate::types::BLOCK_SIZE;

const URING_DEPTH: u32 = 64;
const MAX_COALESCED_WRITE_BYTES: usize = 256 * 1024;

/// Per-thread ring state. A ring is created lazily on first submit and reused
/// for the thread's lifetime. If creation hits file-descriptor exhaustion
/// (EMFILE/ENFILE) or memory pressure (ENOMEM), the thread permanently
/// downgrades to the syscall path instead of hard-failing every IO — under a
/// large onyx thread fan-out the default `nofile` can be overrun, and a write
/// that fails is far worse than a write that runs a bit slower. (P8: this is
/// the runtime analog of the probe-time fallback in `backend::make_backend`.)
enum RingState {
    Uninit,
    Ready(IoUring),
    Disabled,
}

thread_local! {
    static URING: RefCell<RingState> = const { RefCell::new(RingState::Uninit) };
}

/// Process-wide latch so the EMFILE downgrade logs once, not once per thread.
static FD_EXHAUSTION_WARNED: AtomicBool = AtomicBool::new(false);

/// True when an `IoUring::new` error is fd/memory exhaustion the caller should
/// gracefully degrade around (vs a genuine "io_uring unsupported" error).
fn is_resource_exhaustion(e: &std::io::Error) -> bool {
    matches!(
        e.raw_os_error(),
        Some(libc::EMFILE) | Some(libc::ENFILE) | Some(libc::ENOMEM)
    )
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

/// Outcome of trying to get this thread's ring for one submit.
enum RingAccess<'a> {
    /// Use this ring.
    Ready(&'a mut IoUring),
    /// fd/memory exhaustion — caller must fall back to the syscall path.
    Degrade,
}

/// Borrow (lazily creating) this thread's ring, or report that the thread has
/// degraded to sync. A genuine non-exhaustion init error is returned as `Err`.
fn with_ring<R>(f: impl FnOnce(RingAccess<'_>) -> ChunkletResult<R>) -> ChunkletResult<R> {
    URING.with(|cell| {
        let mut slot = cell.borrow_mut();
        if matches!(&*slot, RingState::Uninit) {
            match IoUring::new(URING_DEPTH) {
                Ok(r) => *slot = RingState::Ready(r),
                Err(e) if is_resource_exhaustion(&e) => {
                    if !FD_EXHAUSTION_WARNED.swap(true, Ordering::Relaxed) {
                        tracing::warn!(
                            error = %e,
                            "io_uring init hit fd/memory exhaustion; this thread (and any \
                             other that hits it) falls back to the syscall backend — raise \
                             the process nofile limit (LimitNOFILE) to keep the io_uring path"
                        );
                    }
                    *slot = RingState::Disabled;
                }
                Err(e) => {
                    return Err(ChunkletError::Io(std::io::Error::other(format!(
                        "io_uring init: {}",
                        e
                    ))));
                }
            }
        }
        match &mut *slot {
            RingState::Ready(ring) => f(RingAccess::Ready(ring)),
            RingState::Disabled => f(RingAccess::Degrade),
            RingState::Uninit => unreachable!("ring state resolved above"),
        }
    })
}

impl IoBackend for UringBackend {
    fn name(&self) -> &'static str {
        "uring"
    }

    fn submit_reads(&self, ops: &mut [StripRead<'_>]) -> ChunkletResult<()> {
        if ops.is_empty() {
            return Ok(());
        }
        let degraded = with_ring(|access| match access {
            RingAccess::Degrade => Ok(true),
            RingAccess::Ready(ring) => {
                let mut start = 0usize;
                while start < ops.len() {
                    let end = (start + URING_DEPTH as usize).min(ops.len());
                    submit_read_chunk(ring, &mut ops[start..end])?;
                    start = end;
                }
                Ok(false)
            }
        })?;
        if degraded {
            // fd exhaustion → run this batch through the syscall fan-out.
            return SyncBackend.submit_reads(ops);
        }
        Ok(())
    }

    fn submit_writes_detailed(&self, ops: &[StripWrite<'_>]) -> Vec<ChunkletResult<()>> {
        if ops.is_empty() {
            return Vec::new();
        }
        let n = ops.len();
        let mut results: Vec<Option<ChunkletResult<()>>> = (0..n).map(|_| None).collect();
        let outcome = with_ring(|access| -> ChunkletResult<bool> {
            match access {
                RingAccess::Degrade => Ok(true),
                RingAccess::Ready(ring) => {
                    let mut start = 0usize;
                    while start < n {
                        let end = (start + URING_DEPTH as usize).min(n);
                        // Per-op results for this chunk, offset back into the
                        // full-batch positions (chunk uses chunk-local indices).
                        for (i, r) in submit_coalesced_chunk_detailed(ring, &ops[start..end])
                            .into_iter()
                            .enumerate()
                        {
                            results[start + i] = Some(r);
                        }
                        start = end;
                    }
                    Ok(false)
                }
            }
        });
        match outcome {
            // fd/memory exhaustion → run this batch through the syscall fan-out.
            Ok(true) => SyncBackend.submit_writes_detailed(ops),
            Ok(false) => results
                .into_iter()
                .map(|o| o.expect("every op filled by a chunk"))
                .collect(),
            // Genuine (non-exhaustion) ring-init error: cannot attribute to a
            // single op, so mark the whole batch failed and let the LD surface it.
            Err(e) => {
                let msg = e.to_string();
                (0..n)
                    .map(|_| Err(ChunkletError::Io(std::io::Error::other(msg.clone()))))
                    .collect()
            }
        }
    }

    fn submit_flushes(&self, pds: &[Arc<PhysicalDisk>]) -> ChunkletResult<()> {
        if pds.is_empty() {
            return Ok(());
        }
        let degraded = with_ring(|access| match access {
            RingAccess::Degrade => Ok(true),
            RingAccess::Ready(ring) => {
                for chunk in pds.chunks(URING_DEPTH as usize) {
                    submit_flush_chunk(ring, chunk)?;
                }
                Ok(false)
            }
        })?;
        if degraded {
            return SyncBackend.submit_flushes(pds);
        }
        Ok(())
    }
}

/// Submit one cache-flush command per PD in a single ring batch. The writes
/// guarded by this barrier have already completed before `LogicalDisk::flush`
/// is called, so the fsync SQEs do not need link ordering against write SQEs.
fn submit_flush_chunk(ring: &mut IoUring, pds: &[Arc<PhysicalDisk>]) -> ChunkletResult<()> {
    {
        let mut sq = ring.submission();
        for (idx, pd) in pds.iter().enumerate() {
            let entry = opcode::Fsync::new(types::Fd(pd.raw_fd()))
                .build()
                .user_data(idx as u64);
            // SAFETY: the fd remains owned by `pd`, and this function waits for
            // every submitted SQE before returning.
            unsafe {
                sq.push(&entry).map_err(|error| {
                    ChunkletError::Io(std::io::Error::other(format!(
                        "io_uring flush sq push: {error}"
                    )))
                })?;
            }
        }
    }

    ring.submit_and_wait(pds.len()).map_err(|error| {
        ChunkletError::Io(std::io::Error::other(format!(
            "io_uring flush submit_and_wait: {error}"
        )))
    })?;

    let mut completed = 0usize;
    let mut first_err = None;
    for cqe in ring.completion() {
        completed += 1;
        if cqe.result() < 0 && first_err.is_none() {
            first_err = Some(ChunkletError::Io(std::io::Error::from_raw_os_error(
                -cqe.result(),
            )));
        }
    }
    if completed != pds.len() {
        return Err(ChunkletError::Io(std::io::Error::other(format!(
            "io_uring expected {} flush cqes, got {completed}",
            pds.len()
        ))));
    }
    first_err.map_or(Ok(()), Err)
}

/// Collapse adjacent writes to one PD chunklet before submitting a depth-sized
/// batch. Mirror page writes arrive interleaved by copy (A0, B0, A1, B1, ...),
/// so issuing them verbatim turns a contiguous checkpoint into 4 KiB physical
/// IO. The temporary buffers here are bounded by `URING_DEPTH` and preserve a
/// result slot for every original op, including degraded-member failures.
fn submit_coalesced_chunk_detailed(
    ring: &mut IoUring,
    ops: &[StripWrite<'_>],
) -> Vec<ChunkletResult<()>> {
    let groups = coalesced_write_groups(ops);
    if groups.iter().all(|group| group.len() == 1) {
        return submit_chunk_detailed(ring, ops);
    }

    struct PlannedWrite {
        originals: Vec<usize>,
        buffer: Option<AlignedBuf>,
    }

    let mut planned = Vec::with_capacity(groups.len());
    for group in groups {
        if group.len() == 1 {
            planned.push(PlannedWrite {
                originals: group,
                buffer: None,
            });
            continue;
        }
        let total_bytes: usize = group.iter().map(|&idx| ops[idx].data.len()).sum();
        let mut buffer = match AlignedBuf::new(total_bytes) {
            Ok(buffer) => buffer,
            Err(error) => {
                let message = format!("io_uring coalesced write allocation: {error}");
                return (0..ops.len())
                    .map(|_| Err(ChunkletError::Io(std::io::Error::other(message.clone()))))
                    .collect();
            }
        };
        let mut cursor = 0;
        for &idx in &group {
            let data = ops[idx].data;
            buffer.as_mut_slice()[cursor..cursor + data.len()].copy_from_slice(data);
            cursor += data.len();
        }
        planned.push(PlannedWrite {
            originals: group,
            buffer: Some(buffer),
        });
    }

    let submitted: Vec<StripWrite<'_>> = planned
        .iter()
        .map(|plan| {
            let first = &ops[plan.originals[0]];
            StripWrite {
                pd: first.pd.clone(),
                chunklet_index: first.chunklet_index,
                in_chunklet_off: first.in_chunklet_off,
                data: plan
                    .buffer
                    .as_ref()
                    .map(AlignedBuf::as_slice)
                    .unwrap_or(first.data),
            }
        })
        .collect();
    let submitted_results = submit_chunk_detailed(ring, &submitted);
    let mut results: Vec<Option<ChunkletResult<()>>> = (0..ops.len()).map(|_| None).collect();
    for (plan, result) in planned.iter().zip(submitted_results) {
        if plan.originals.len() == 1 {
            results[plan.originals[0]] = Some(result);
            continue;
        }
        match result {
            Ok(()) => {
                for &idx in &plan.originals {
                    results[idx] = Some(Ok(()));
                }
            }
            Err(error) => {
                let message = format!("coalesced member write failed: {error}");
                for &idx in &plan.originals {
                    results[idx] = Some(Err(ChunkletError::Io(std::io::Error::other(
                        message.clone(),
                    ))));
                }
            }
        }
    }
    results
        .into_iter()
        .map(|result| result.expect("every coalesced source op has a result"))
        .collect()
}

fn coalesced_write_groups(ops: &[StripWrite<'_>]) -> Vec<Vec<usize>> {
    let mut by_location: BTreeMap<(crate::types::PdId, u32), Vec<usize>> = BTreeMap::new();
    for (idx, op) in ops.iter().enumerate() {
        by_location
            .entry((op.pd.pd_id(), op.chunklet_index))
            .or_default()
            .push(idx);
    }

    let mut groups = Vec::new();
    for (_, original_indices) in by_location {
        let mut indices = original_indices.clone();
        indices.sort_unstable_by_key(|&idx| ops[idx].in_chunklet_off);

        let overlaps = indices.windows(2).any(|pair| {
            let first = &ops[pair[0]];
            first.in_chunklet_off + first.data.len() as u64 > ops[pair[1]].in_chunklet_off
        });
        if overlaps {
            groups.extend(original_indices.into_iter().map(|idx| vec![idx]));
            continue;
        }

        let mut current: Vec<usize> = Vec::new();
        let mut current_end = 0u64;
        let mut current_bytes = 0usize;
        for idx in indices {
            let op = &ops[idx];
            let adjacent = !current.is_empty() && op.in_chunklet_off == current_end;
            if !current.is_empty()
                && (!adjacent || current_bytes + op.data.len() > MAX_COALESCED_WRITE_BYTES)
            {
                groups.push(std::mem::take(&mut current));
                current_bytes = 0;
            }
            current_end = op.in_chunklet_off + op.data.len() as u64;
            current_bytes += op.data.len();
            current.push(idx);
        }
        if !current.is_empty() {
            groups.push(current);
        }
    }
    groups
}

fn submit_read_chunk(ring: &mut IoUring, ops: &mut [StripRead<'_>]) -> ChunkletResult<()> {
    let mut targets: Vec<(i32, u64)> = Vec::with_capacity(ops.len());
    for op in ops.iter() {
        let abs = op.pd.chunklet_user_abs_offset(
            op.chunklet_index,
            op.in_chunklet_off,
            op.data.len() as u64,
        )?;
        targets.push((op.pd.raw_fd(), abs));
    }

    let mut bounces: Vec<Option<AlignedBuf>> = Vec::with_capacity(ops.len());
    let mut ptrs: Vec<(*mut u8, u32)> = Vec::with_capacity(ops.len());
    for (op, (_fd, abs)) in ops.iter_mut().zip(targets.iter()) {
        let ptr = op.data.as_mut_ptr();
        let len = op.data.len();
        if is_direct_aligned(*abs, len, ptr as usize) {
            ptrs.push((ptr, len as u32));
            bounces.push(None);
        } else {
            let mut buf = AlignedBuf::new(len)?;
            ptrs.push((buf.as_mut_slice().as_mut_ptr(), len as u32));
            bounces.push(Some(buf));
        }
    }

    {
        let mut sq = ring.submission();
        for i in 0..ops.len() {
            let (fd, abs) = targets[i];
            let (ptr, len) = ptrs[i];
            let entry = opcode::Read::new(types::Fd(fd), ptr, len)
                .offset(abs)
                .build()
                .user_data(i as u64);
            // SAFETY: direct buffers and bounce buffers outlive this function;
            // `submit_and_wait` waits for every submitted SQE.
            unsafe {
                sq.push(&entry).map_err(|e| {
                    ChunkletError::Io(std::io::Error::other(format!(
                        "io_uring read sq push: {}",
                        e
                    )))
                })?;
            }
        }
    }

    ring.submit_and_wait(ops.len()).map_err(|e| {
        ChunkletError::Io(std::io::Error::other(format!(
            "io_uring read submit_and_wait: {}",
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
                first_err = Some(ChunkletError::Io(std::io::Error::from_raw_os_error(errno)));
            }
        } else if (res as u32) < ptrs[idx].1 {
            if first_err.is_none() {
                first_err = Some(ChunkletError::Io(std::io::Error::other(format!(
                    "io_uring short read op_idx={}: {} of {}",
                    idx, res, ptrs[idx].1
                ))));
            }
        }
    }
    if completed < ops.len() {
        return Err(ChunkletError::Io(std::io::Error::other(format!(
            "io_uring expected {} read cqes, got {}",
            ops.len(),
            completed
        ))));
    }
    if let Some(err) = first_err {
        return Err(err);
    }

    for (op, bounce) in ops.iter_mut().zip(bounces.iter()) {
        if let Some(buf) = bounce {
            op.data.copy_from_slice(&buf.as_slice()[..op.data.len()]);
        }
    }
    Ok(())
}

/// Submit one NUMA-homogeneous chunk and return a per-op result in the chunk's
/// input order (`results[i]` ↔ `ops[i]`). A batch-level setup failure (offset
/// geometry, bounce alloc, SQE push, submit_and_wait) cannot be attributed to a
/// single op, so it marks every op in the chunk failed; otherwise each op's CQE
/// determines its own result. Surviving ops are durable when this returns.
fn submit_chunk_detailed(ring: &mut IoUring, ops: &[StripWrite<'_>]) -> Vec<ChunkletResult<()>> {
    let n = ops.len();
    let chunk_all_err = |msg: String| -> Vec<ChunkletResult<()>> {
        (0..n)
            .map(|_| Err(ChunkletError::Io(std::io::Error::other(msg.clone()))))
            .collect()
    };

    // Resolve absolute offsets + fds while we still hold &op.
    let mut targets: Vec<(i32, u64)> = Vec::with_capacity(n);
    for op in ops {
        match op.pd.chunklet_user_abs_offset(
            op.chunklet_index,
            op.in_chunklet_off,
            op.data.len() as u64,
        ) {
            Ok(abs) => targets.push((op.pd.raw_fd(), abs)),
            Err(e) => return chunk_all_err(format!("io_uring chunk offset setup: {}", e)),
        }
    }

    // SQEs reference either the caller's already O_DIRECT-safe buffer or
    // one of these bounce buffers. The Vec is held until every CQE arrives.
    let mut bounces: Vec<AlignedBuf> = Vec::with_capacity(n);
    let mut ptrs: Vec<(*const u8, u32)> = Vec::with_capacity(n);
    for (op, (_fd, abs)) in ops.iter().zip(targets.iter()) {
        let ptr = op.data.as_ptr();
        let len = op.data.len();
        if is_direct_aligned(*abs, len, ptr as usize) {
            ptrs.push((ptr, len as u32));
        } else {
            match AlignedBuf::from_slice(op.data) {
                Ok(buf) => {
                    ptrs.push((buf.as_slice().as_ptr(), len as u32));
                    bounces.push(buf);
                }
                Err(e) => return chunk_all_err(format!("io_uring chunk bounce alloc: {}", e)),
            }
        }
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
                if let Err(e) = sq.push(&entry) {
                    return chunk_all_err(format!("io_uring sq push: {}", e));
                }
            }
        }
    }

    if let Err(e) = ring.submit_and_wait(n) {
        return chunk_all_err(format!("io_uring submit_and_wait: {}", e));
    }

    let mut results: Vec<Option<ChunkletResult<()>>> = (0..n).map(|_| None).collect();
    let mut completed = 0;
    for cqe in ring.completion() {
        completed += 1;
        let idx = cqe.user_data() as usize;
        let res = cqe.result();
        let r = if res < 0 {
            Err(ChunkletError::Io(std::io::Error::from_raw_os_error(-res)))
        } else if (res as u32) < ptrs[idx].1 {
            Err(ChunkletError::Io(std::io::Error::other(format!(
                "io_uring short write op_idx={}: {} of {}",
                idx, res, ptrs[idx].1
            ))))
        } else {
            Ok(())
        };
        results[idx] = Some(r);
    }
    drop(bounces);
    // Any op without a CQE (should not happen after submit_and_wait(n)) is
    // reported as that op's own error, not a batch failure.
    results
        .into_iter()
        .map(|o| {
            o.unwrap_or_else(|| {
                Err(ChunkletError::Io(std::io::Error::other(format!(
                    "io_uring missing cqe (completed {} of {})",
                    completed, n
                ))))
            })
        })
        .collect()
}

fn is_direct_aligned(offset: u64, len: usize, ptr: usize) -> bool {
    let bs = BLOCK_SIZE as u64;
    offset % bs == 0 && (len as u64) % bs == 0 && (ptr as u64) % bs == 0
}

/// Force this thread's ring into the `Disabled` state, simulating an
/// `IoUring::new` EMFILE without actually exhausting the process fd table
/// (which would destabilise the rest of the suite). Lets a test drive the
/// runtime degrade-to-sync path deterministically.
#[cfg(test)]
fn force_ring_disabled_for_test() {
    URING.with(|cell| *cell.borrow_mut() = RingState::Disabled);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pd::PhysicalDisk;
    use crate::types::{PdId, PoolId};
    use std::sync::Arc;
    use tempfile::TempDir;

    #[test]
    fn resource_exhaustion_matches_fd_and_mem_errnos() {
        for errno in [libc::EMFILE, libc::ENFILE, libc::ENOMEM] {
            assert!(is_resource_exhaustion(&std::io::Error::from_raw_os_error(
                errno
            )));
        }
        for errno in [libc::EINVAL, libc::EIO, libc::EPERM] {
            assert!(!is_resource_exhaustion(&std::io::Error::from_raw_os_error(
                errno
            )));
        }
        // A non-OS error (no errno) is not exhaustion.
        assert!(!is_resource_exhaustion(&std::io::Error::other("nope")));
    }

    #[test]
    fn interleaved_adjacent_writes_are_grouped_per_physical_disk() {
        let dir = TempDir::new().unwrap();
        let raw =
            crate::io::RawDevice::open_or_create(&dir.path().join("pd0"), 4 * 1024 * 1024 * 1024)
                .unwrap();
        let pd: Arc<PhysicalDisk> = PhysicalDisk::init(
            raw,
            PoolId::new_v4(),
            PdId::new_v4(),
            0,
            1,
            vec![],
            0,
            vec![],
            vec![],
        )
        .unwrap();
        let page = [0x5a; 4096];
        let writes = vec![
            StripWrite {
                pd: pd.clone(),
                chunklet_index: 0,
                in_chunklet_off: 0,
                data: &page,
            },
            StripWrite {
                pd: pd.clone(),
                chunklet_index: 0,
                in_chunklet_off: 8192,
                data: &page,
            },
            StripWrite {
                pd,
                chunklet_index: 0,
                in_chunklet_off: 4096,
                data: &page,
            },
        ];

        assert_eq!(coalesced_write_groups(&writes), vec![vec![0, 2, 1]]);
    }

    /// When the thread's ring is disabled (the EMFILE outcome), `submit_writes`
    /// / `submit_reads` must transparently fall back to the syscall backend and
    /// still land the data correctly — a degraded write, never a failed one.
    #[test]
    fn degrades_to_sync_when_ring_disabled() {
        let dir = TempDir::new().unwrap();
        let raw =
            crate::io::RawDevice::open_or_create(&dir.path().join("pd0"), 4 * 1024 * 1024 * 1024)
                .unwrap();
        let pd: Arc<PhysicalDisk> = PhysicalDisk::init(
            raw,
            PoolId::new_v4(),
            PdId::new_v4(),
            0,
            1,
            vec![],
            0,
            vec![],
            vec![],
        )
        .unwrap();

        force_ring_disabled_for_test();

        let payload: Vec<u8> = (0..4096).map(|i| (i % 251) as u8).collect();
        let writes = vec![StripWrite {
            pd: pd.clone(),
            chunklet_index: 0,
            in_chunklet_off: 0,
            data: &payload,
        }];
        // Hard-fails today on a disabled ring; with the degrade fix it returns Ok
        // via SyncBackend.
        UringBackend.submit_writes(&writes).unwrap();

        let mut got = vec![0u8; 4096];
        {
            let mut reads = vec![StripRead {
                pd: pd.clone(),
                chunklet_index: 0,
                in_chunklet_off: 0,
                data: &mut got,
            }];
            UringBackend.submit_reads(&mut reads).unwrap();
        }
        assert_eq!(
            got, payload,
            "degraded read-back must match the degraded write"
        );
    }
}
