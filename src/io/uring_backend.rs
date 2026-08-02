//! `UringBackend` — `io_uring` batched writes (Linux only).
//!
//! One thread-local `IoUring` instance per execution thread. The default
//! constructor preserves caller-thread execution. The pooled constructor owns
//! independent persistent foreground/background Rayon pools, groups a batch by
//! physical disk, and reuses each worker's TLS ring across submits.
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

use std::any::Any;
use std::cell::RefCell;
use std::collections::BTreeSet;
use std::panic::{catch_unwind, resume_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

use io_uring::{opcode, squeue, types, IoUring};

use crate::error::{ChunkletError, ChunkletResult};
use crate::io::aligned::AlignedBuf;
use crate::io::backend::{
    submit_dispatched_blocking, IoBackend, IoExecutionSnapshot, StripRead, StripWrite,
    UringPoolConfig, WriteCompletionObserver, WriteDispatch,
};
use crate::io::scheduler::{current_io_class, IoClass};
use crate::pd::PhysicalDisk;
use crate::types::BLOCK_SIZE;
use crate::write_path as wp;

const URING_DEPTH: u32 = 256;
const MAX_COALESCED_WRITE_BYTES: usize = 256 * 1024;

mod batch;
mod execution;
mod stream;

use batch::{
    push_batch, wait_and_drain, wait_and_drain_observed, wait_and_drain_with_mode,
    ValidatedCompletion,
};
pub(crate) use execution::ExecutionPoolBackend;
use execution::ScopedWritePools;

/// Holds the first observer panic while terminal IO recovery continues. The
/// callback is disabled after its first panic and resumed only once every
/// already-submitted IO and required exact-IO retry has finished.
struct CompletionCallback<'a, F> {
    callback: &'a mut F,
    panic: Option<Box<dyn Any + Send>>,
}

impl<'a, F> CompletionCallback<'a, F>
where
    F: FnMut(&[ValidatedCompletion]),
{
    fn new(callback: &'a mut F) -> Self {
        Self {
            callback,
            panic: None,
        }
    }

    fn notify(&mut self, completions: &[ValidatedCompletion]) {
        if completions.is_empty() || self.panic.is_some() {
            return;
        }
        if let Err(payload) = catch_unwind(AssertUnwindSafe(|| (self.callback)(completions))) {
            self.panic = Some(payload);
        }
    }

    fn resume_if_panicked(self) {
        if let Some(payload) = self.panic {
            resume_unwind(payload);
        }
    }
}

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

/// Wait for a whole batch in one `io_uring_enter` instead of waking once per
/// CQE (see `batch::wait_and_drain`). Process-wide because the submit path runs
/// on thread-local rings with no backend handle in scope. Default OFF: the
/// per-CQE wake is the long-shipped behaviour, so this stays an explicit A/B
/// until the box run lands. Set once at pool-configure time, before any IO.
static COALESCED_WAIT: AtomicBool = AtomicBool::new(false);

/// Enable/disable the coalesced CQE wait. Applies process-wide to every
/// subsequent batch drain that has no completion observer.
pub fn set_coalesced_wait(enabled: bool) {
    COALESCED_WAIT.store(enabled, Ordering::Relaxed);
}

fn coalesced_wait_enabled() -> bool {
    COALESCED_WAIT.load(Ordering::Relaxed)
}

/// SQEs submitted per stop-and-wait wave on the batched submit paths. A logical
/// write fans out to hundreds of per-strip SQEs, and each wave must fully drain
/// before the next is pushed, so a small wave count means many drain-and-refill
/// barriers with the disks idle in between. Bounded by `URING_DEPTH` (the SQ
/// must hold a whole wave atomically). Default `DEFAULT_WRITE_CHUNK_OPS`
/// preserves the long-shipped wave size; raising it is the A/B.
static WRITE_CHUNK_OPS: AtomicUsize = AtomicUsize::new(DEFAULT_WRITE_CHUNK_OPS);

/// Historical wave size, kept as the default so the knob is opt-in.
const DEFAULT_WRITE_CHUNK_OPS: usize = 64;

/// Set the per-wave SQE count. Clamped to `1..=URING_DEPTH`; `0` restores the
/// default. Applies process-wide to subsequent batches.
pub fn set_write_chunk_ops(ops: usize) {
    let clamped = if ops == 0 {
        DEFAULT_WRITE_CHUNK_OPS
    } else {
        ops.clamp(1, URING_DEPTH as usize)
    };
    WRITE_CHUNK_OPS.store(clamped, Ordering::Relaxed);
}

fn write_chunk_ops() -> usize {
    WRITE_CHUNK_OPS.load(Ordering::Relaxed)
}

/// Submit an adjacency-merged group as one vectored write instead of copying its
/// strips into a bounce buffer. See [`UringPoolConfig::writev_coalesce`].
static WRITEV_COALESCE: AtomicBool = AtomicBool::new(false);

/// Enable/disable vectored submission of merged groups. Applies process-wide to
/// every subsequent batch.
pub fn set_writev_coalesce(enabled: bool) {
    WRITEV_COALESCE.store(enabled, Ordering::Relaxed);
}

/// Diagnostic override, same shape as onyx's `ONYX_ALLOCATOR_REGIONS`: it exists
/// so the WHOLE suite can be re-run against the vectored path
/// (`CHUNKLET_WRITEV_COALESCE=1 cargo test --release`) instead of only the
/// dedicated writev tests, which is the only way to find a mistake in a caller
/// nobody thought to writev-test. Overrides the config, so production must not
/// set it.
fn writev_coalesce_env_override() -> Option<bool> {
    static OVERRIDE: std::sync::OnceLock<Option<bool>> = std::sync::OnceLock::new();
    *OVERRIDE.get_or_init(|| {
        std::env::var("CHUNKLET_WRITEV_COALESCE")
            .ok()
            .map(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "on"))
    })
}

fn writev_coalesce_enabled() -> bool {
    writev_coalesce_env_override().unwrap_or_else(|| WRITEV_COALESCE.load(Ordering::Relaxed))
}

/// Upper bound on iovecs in one `writev`. `IOV_MAX` is 1024 on Linux; a group is
/// already capped at [`MAX_COALESCED_WRITE_BYTES`] / 4 KiB = 64 strips, so this
/// only guards a future strip size below 256 bytes.
const MAX_WRITEV_PARTS: usize = 1024;

/// True when an `IoUring::new` error is fd/memory exhaustion the caller should
/// gracefully degrade around (vs a genuine "io_uring unsupported" error).
fn is_resource_exhaustion(e: &std::io::Error) -> bool {
    matches!(
        e.raw_os_error(),
        Some(libc::EMFILE) | Some(libc::ENFILE) | Some(libc::ENOMEM)
    )
}

pub struct UringBackend {
    write_pools: ScopedWritePools,
}

impl UringBackend {
    /// Probe-initialize an `IoUring` instance to verify the kernel
    /// supports it. Returns `Err` on kernels without `io_uring` (pre-5.1)
    /// or with it disabled by sysctl.
    pub fn new() -> std::io::Result<Self> {
        Self::new_pooled(0, 0)
    }

    /// Build with optional persistent write pools. Zero workers keep the
    /// corresponding class on the historical caller-thread path.
    pub fn new_pooled(
        foreground_workers: usize,
        background_workers: usize,
    ) -> std::io::Result<Self> {
        Self::new_pooled_with_config(UringPoolConfig::new(foreground_workers, background_workers))
    }

    /// Build persistent write pools with explicit worker CPU sets.
    pub fn new_pooled_with_config(config: UringPoolConfig) -> std::io::Result<Self> {
        let _probe = IoUring::new(URING_DEPTH)?;
        set_coalesced_wait(config.coalesced_wait);
        set_write_chunk_ops(config.write_chunk_ops);
        set_writev_coalesce(config.writev_coalesce);
        Ok(Self {
            write_pools: ScopedWritePools::with_config(config)?,
        })
    }

    fn submit_writes_detailed_inline(ops: &[StripWrite<'_>]) -> Vec<ChunkletResult<()>> {
        if ops.is_empty() {
            return Vec::new();
        }
        let n = ops.len();
        // One `submit_calls` per LD batch, one `submit_waves` per stop-and-wait
        // barrier: their ratio IS the barrier count that `uring_write_chunk_ops`
        // controls, and it is only readable from here.
        let slot = wp::class_slot();
        wp::add(&wp::SUBMIT_CALLS[slot], 1);
        wp::add(&wp::SUBMIT_OPS[slot], n as u64);
        let mut results: Vec<Option<ChunkletResult<()>>> = (0..n).map(|_| None).collect();
        let outcome = with_ring(|access| -> ChunkletResult<bool> {
            match access {
                RingAccess::Degrade => Ok(true),
                RingAccess::Ready(ring) => {
                    let mut start = 0usize;
                    while start < n {
                        let end = (start + write_chunk_ops()).min(n);
                        wp::add(&wp::SUBMIT_WAVES[slot], 1);
                        for (i, result) in submit_coalesced_chunk_detailed(ring, &ops[start..end])
                            .into_iter()
                            .enumerate()
                        {
                            results[start + i] = Some(result);
                        }
                        start = end;
                    }
                    Ok(false)
                }
            }
        });
        match outcome {
            Ok(true) => submit_writes_serial(ops),
            Ok(false) => results
                .into_iter()
                .map(|result| result.expect("every op filled by a chunk"))
                .collect(),
            Err(error) => {
                let message = error.to_string();
                (0..n)
                    .map(|_| Err(ChunkletError::Io(std::io::Error::other(message.clone()))))
                    .collect()
            }
        }
    }

    fn submit_writes_detailed_inline_observed(
        ops: &[StripWrite<'_>],
        observer: &dyn WriteCompletionObserver,
        service_started: Instant,
    ) -> Vec<ChunkletResult<()>> {
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
                        let end = (start + write_chunk_ops()).min(n);
                        let chunk_results = submit_coalesced_chunk_detailed_observed(
                            ring,
                            &ops[start..end],
                            start,
                            observer,
                            service_started,
                        );
                        for (index, result) in chunk_results.into_iter().enumerate() {
                            results[start + index] = Some(result);
                        }
                        start = end;
                    }
                    Ok(false)
                }
            }
        });
        match outcome {
            Ok(true) => {
                let results = submit_writes_serial(ops);
                notify_all_completed(observer, n, service_started);
                results
            }
            Ok(false) => results
                .into_iter()
                .map(|result| result.expect("every op filled by a chunk"))
                .collect(),
            Err(error) => {
                notify_all_completed(observer, n, service_started);
                let message = error.to_string();
                (0..n)
                    .map(|_| Err(ChunkletError::Io(std::io::Error::other(message.clone()))))
                    .collect()
            }
        }
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
                    let end = (start + write_chunk_ops()).min(ops.len());
                    submit_read_chunk(ring, &mut ops[start..end])?;
                    start = end;
                }
                Ok(false)
            }
        })?;
        if degraded {
            return submit_reads_serial(ops);
        }
        Ok(())
    }

    fn submit_writes_detailed(&self, ops: &[StripWrite<'_>]) -> Vec<ChunkletResult<()>> {
        self.write_pools
            .submit(current_io_class(), ops, Self::submit_writes_detailed_inline)
    }

    fn submit_writes_detailed_with_class(
        &self,
        class: IoClass,
        ops: &[StripWrite<'_>],
    ) -> Vec<ChunkletResult<()>> {
        self.write_pools
            .submit(class, ops, Self::submit_writes_detailed_inline)
    }

    fn submit_writes_detailed_observed_with_class(
        &self,
        class: IoClass,
        ops: &[StripWrite<'_>],
        observer: &dyn WriteCompletionObserver,
    ) -> Vec<ChunkletResult<()>> {
        if self.write_pools.has_pool(class) {
            return self.write_pools.submit_observed(
                class,
                ops,
                observer,
                Self::submit_writes_detailed_inline_observed,
            );
        }
        Self::submit_writes_detailed_inline_observed(ops, observer, Instant::now())
    }

    fn submit_writes_dispatched_with_class<'a>(
        &self,
        class: IoClass,
        total_ops: usize,
        dispatch: &mut dyn WriteDispatch<'a>,
    ) -> Vec<ChunkletResult<()>> {
        if self.write_pools.has_pool(class) {
            return submit_dispatched_blocking(self, class, total_ops, dispatch);
        }
        let streamed = with_ring(|access| match access {
            RingAccess::Ready(ring) => {
                Ok(Some(stream::submit_dispatched(ring, total_ops, dispatch)))
            }
            RingAccess::Degrade => Ok(None),
        });
        match streamed {
            Ok(Some(results)) => results,
            Ok(None) => submit_dispatched_blocking(self, class, total_ops, dispatch),
            Err(error) => {
                let message = error.to_string();
                (0..total_ops)
                    .map(|_| Err(ChunkletError::Io(std::io::Error::other(message.clone()))))
                    .collect()
            }
        }
    }

    fn execution_snapshot(&self) -> Option<IoExecutionSnapshot> {
        Some(self.write_pools.snapshot())
    }

    fn submit_flushes(&self, pds: &[Arc<PhysicalDisk>]) -> ChunkletResult<()> {
        submit_required_flushes(pds, |required| {
            let degraded = with_ring(|access| match access {
                RingAccess::Degrade => Ok(true),
                RingAccess::Ready(ring) => {
                    for chunk in required.chunks(URING_DEPTH as usize) {
                        submit_flush_chunk(ring, chunk)?;
                    }
                    Ok(false)
                }
            })?;
            if degraded {
                return submit_flushes_serial(required);
            }
            Ok(())
        })
    }
}

/// Submit a barrier only for devices whose cache mode still requires one.
/// Write-through O_DIRECT completion is already durable, matching
/// `RawDevice::sync`; filtering here keeps the io_uring path from bypassing
/// that durability decision.
fn submit_required_flushes(
    pds: &[Arc<PhysicalDisk>],
    submit: impl FnOnce(&[Arc<PhysicalDisk>]) -> ChunkletResult<()>,
) -> ChunkletResult<()> {
    let mut required = Vec::new();
    for pd in pds {
        if pd.sync_required()
            && required
                .iter()
                .all(|selected: &Arc<PhysicalDisk>| selected.pd_id() != pd.pd_id())
        {
            required.push(pd.clone());
        }
    }
    if required.is_empty() {
        return Ok(());
    }
    submit(&required)
}

/// Submit one cache-flush command per PD in a single ring batch. The writes
/// guarded by this barrier have already completed before `LogicalDisk::flush`
/// is called, so the fsync SQEs do not need link ordering against write SQEs.
fn submit_flush_chunk(ring: &mut IoUring, pds: &[Arc<PhysicalDisk>]) -> ChunkletResult<()> {
    let entries: Vec<squeue::Entry> = pds
        .iter()
        .enumerate()
        .map(|(index, pd)| {
            opcode::Fsync::new(types::Fd(pd.raw_fd()))
                .build()
                .user_data(index as u64)
        })
        .collect();
    push_batch(ring, &entries, "io_uring flush")?;
    let completions = wait_and_drain(ring, pds.len(), "io_uring flush");
    if let Some(error) = completions.protocol_error {
        return Err(ChunkletError::Io(std::io::Error::other(format!(
            "io_uring flush completion protocol: {error}"
        ))));
    }
    let mut first_err = None;
    for result in completions.results.into_iter().flatten() {
        if result < 0 && first_err.is_none() {
            first_err = Some(ChunkletError::Io(std::io::Error::from_raw_os_error(
                -result,
            )));
        }
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
    // No observer: the callback is a no-op, so the batch may be waited for in
    // one `io_uring_enter` instead of one wake per completed 4 KiB strip.
    submit_coalesced_chunk_detailed_with_callback(ring, ops, false, |_| {})
}

fn submit_coalesced_chunk_detailed_observed(
    ring: &mut IoUring,
    ops: &[StripWrite<'_>],
    global_offset: usize,
    observer: &dyn WriteCompletionObserver,
    service_started: Instant,
) -> Vec<ChunkletResult<()>> {
    submit_coalesced_chunk_detailed_with_callback(ring, ops, true, |local_indices| {
        let global_indices: Vec<_> = local_indices
            .iter()
            .map(|index| global_offset.saturating_add(*index))
            .collect();
        observer.writes_completed(&global_indices, elapsed_ns(service_started));
    })
}

fn submit_coalesced_chunk_detailed_with_callback(
    ring: &mut IoUring,
    ops: &[StripWrite<'_>],
    stream_arrivals: bool,
    mut on_completed: impl FnMut(&[usize]),
) -> Vec<ChunkletResult<()>> {
    let slot = wp::class_slot();
    let group_started = Instant::now();
    let groups = coalesced_write_groups(ops);
    let copy_started = wp::record_since(&wp::SUBMIT_GROUP_NS[slot], group_started);
    if groups.all_single() {
        // Nothing merged: every strip is its own SQE. On the dense-PBA flusher
        // path this means the batch handed in strips that are not adjacent on
        // any PD, so the wave is as wide as the op count.
        wp::add(&wp::SUBMIT_SQES[slot], ops.len() as u64);
        let parts: Vec<&[u8]> = ops.iter().map(|op| op.data).collect();
        let submitted: Vec<PhysWrite<'_>> = ops
            .iter()
            .enumerate()
            .map(|(i, op)| PhysWrite {
                pd: &op.pd,
                chunklet_index: op.chunklet_index,
                in_chunklet_off: op.in_chunklet_off,
                parts: &parts[i..i + 1],
            })
            .collect();
        let wait_started = Instant::now();
        let results =
            submit_chunk_detailed_with_callback(ring, &submitted, stream_arrivals, |completions| {
                let indices: Vec<_> = completions
                    .iter()
                    .map(|completion| completion.index)
                    .collect();
                on_completed(&indices);
            });
        wp::record_since(&wp::SUBMIT_WAIT_NS[slot], wait_started);
        return results;
    }

    // Per group: either a bounce buffer holding its strips concatenated, or
    // `None` meaning "submit the strips as they lie" — one iovec each when the
    // group merged and `writev_coalesce` is on, or the single caller slice.
    let vectored = writev_coalesce_enabled();
    let mut buffers: Vec<Option<AlignedBuf>> = Vec::with_capacity(groups.len());
    for group in groups.iter() {
        if group.len() == 1 || (vectored && group_is_writev_safe(ops, group)) {
            buffers.push(None);
            continue;
        }
        let total_bytes: usize = group.iter().map(|&idx| ops[idx].data.len()).sum();
        // `uninit`, not `new`: the copy below fills every byte, so `new`'s
        // zero-fill was pure waste that doubled this stage's memory traffic.
        let mut buffer = match AlignedBuf::uninit(total_bytes) {
            Ok(buffer) => buffer,
            Err(error) => {
                let message = format!("io_uring coalesced write allocation: {error}");
                let indices: Vec<_> = (0..ops.len()).collect();
                on_completed(&indices);
                return (0..ops.len())
                    .map(|_| Err(ChunkletError::Io(std::io::Error::other(message.clone()))))
                    .collect();
            }
        };
        let mut cursor = 0;
        for &idx in group {
            let data = ops[idx].data;
            buffer.as_mut_slice()[cursor..cursor + data.len()].copy_from_slice(data);
            cursor += data.len();
        }
        wp::add(&wp::SUBMIT_BOUNCE_BYTES[slot], total_bytes as u64);
        wp::add(&wp::SUBMIT_BOUNCE_ALLOCS[slot], 1);
        buffers.push(Some(buffer));
    }
    let build_started = wp::record_since(&wp::SUBMIT_COPY_NS[slot], copy_started);

    // One shared part table for the whole wave: a bounced group contributes its
    // single buffer slice, a vectored group contributes each strip.
    let mut parts: Vec<&[u8]> = Vec::with_capacity(ops.len());
    let mut part_spans: Vec<(usize, usize)> = Vec::with_capacity(groups.len());
    for (group, buffer) in groups.iter().zip(&buffers) {
        let start = parts.len();
        match buffer {
            Some(buffer) => parts.push(buffer.as_slice()),
            None => parts.extend(group.iter().map(|&idx| ops[idx].data)),
        }
        part_spans.push((start, parts.len() - start));
    }
    let submitted: Vec<PhysWrite<'_>> = groups
        .iter()
        .zip(&part_spans)
        .map(|(group, &(start, count))| {
            let first = &ops[group[0]];
            PhysWrite {
                pd: &first.pd,
                chunklet_index: first.chunklet_index,
                in_chunklet_off: first.in_chunklet_off,
                parts: &parts[start..start + count],
            }
        })
        .collect();
    wp::add(&wp::SUBMIT_SQES[slot], submitted.len() as u64);
    wp::record_since(&wp::SUBMIT_BUILD_NS[slot], build_started);
    let wait_started = Instant::now();
    let submitted_results =
        submit_chunk_detailed_with_callback(ring, &submitted, stream_arrivals, |completions| {
        let indices = map_physical_completions(&groups, completions, 0);
        on_completed(&indices);
    });
    wp::record_since(&wp::SUBMIT_WAIT_NS[slot], wait_started);
    let mut results: Vec<Option<ChunkletResult<()>>> = (0..ops.len()).map(|_| None).collect();
    for (index, result) in submitted_results.into_iter().enumerate() {
        let group = groups.group(index);
        if group.len() == 1 {
            results[group[0]] = Some(result);
            continue;
        }
        match result {
            Ok(()) => {
                for &idx in group {
                    results[idx] = Some(Ok(()));
                }
            }
            Err(error) => {
                let message = format!("coalesced member write failed: {error}");
                for &idx in group {
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

fn map_physical_completions(
    groups: &WriteGroups,
    completions: &[ValidatedCompletion],
    global_offset: usize,
) -> Vec<usize> {
    completions
        .iter()
        .filter(|completion| completion.index < groups.len())
        .flat_map(|completion| groups.group(completion.index).iter().copied())
        .map(|index| global_offset.saturating_add(index))
        .collect()
}

/// Adjacency groups over one wave's ops, in TWO allocations total rather than
/// two per group.
///
/// The previous shape was `Vec<Vec<usize>>` built through a
/// `BTreeMap<(PdId, u32), Vec<usize>>`: a `Vec` per PD/chunklet key, a clone of
/// it, and a fresh `Vec` per emitted group — then the planner cloned every group
/// again. At the 567 strip ops per call the LV3 flusher hands in, that is ~1600
/// small allocations per submit, and the whole pre-submit stage measured
/// 505 µs/call on the box while merging almost nothing (merge 1.04x). `order`
/// holds every op index once, arranged so each group is a contiguous slice.
struct WriteGroups {
    /// Op indices, group by group.
    order: Vec<usize>,
    /// `(start, len)` into `order`, one entry per physical write.
    spans: Vec<(u32, u32)>,
}

impl WriteGroups {
    fn len(&self) -> usize {
        self.spans.len()
    }

    fn group(&self, index: usize) -> &[usize] {
        let (start, len) = self.spans[index];
        &self.order[start as usize..start as usize + len as usize]
    }

    fn all_single(&self) -> bool {
        self.spans.iter().all(|&(_, len)| len == 1)
    }

    fn iter(&self) -> impl Iterator<Item = &[usize]> {
        (0..self.len()).map(move |index| self.group(index))
    }
}

/// Whether a merged group can be submitted as iovecs instead of being copied
/// into one buffer. Every strip must be block aligned in its own right (O_DIRECT
/// checks each iovec), and the group must fit `IOV_MAX`. Parity strips are plain
/// `Vec<u8>` allocations, so alignment is not something the LD guarantees — a
/// group that fails here falls back to the bounce path unchanged.
fn group_is_writev_safe(ops: &[StripWrite<'_>], group: &[usize]) -> bool {
    group.len() <= MAX_WRITEV_PARTS
        && group.iter().all(|&idx| {
            let data = ops[idx].data;
            is_direct_aligned(0, data.len(), data.as_ptr() as usize)
        })
}

/// Collapse writes that are adjacent within one PD chunklet into one physical
/// write each.
///
/// One sort by `(pd, chunklet, offset)` replaces the per-key map + per-key sort:
/// equal keys land in one run, so a single linear pass can both detect overlap
/// within a key and cut the runs into groups. Selection is unchanged — same
/// partition, same members, and a group is still capped at
/// [`MAX_COALESCED_WRITE_BYTES`]. `write_groups_match_the_reference_grouping`
/// pins the equivalence against the original implementation.
fn coalesced_write_groups(ops: &[StripWrite<'_>]) -> WriteGroups {
    let mut order: Vec<usize> = (0..ops.len()).collect();
    order.sort_unstable_by_key(|&idx| {
        let op = &ops[idx];
        (op.pd.pd_id(), op.chunklet_index, op.in_chunklet_off)
    });

    let mut spans: Vec<(u32, u32)> = Vec::with_capacity(ops.len());
    let mut key_start = 0usize;
    while key_start < order.len() {
        let key = {
            let op = &ops[order[key_start]];
            (op.pd.pd_id(), op.chunklet_index)
        };
        let mut key_end = key_start + 1;
        while key_end < order.len() {
            let op = &ops[order[key_end]];
            if (op.pd.pd_id(), op.chunklet_index) != key {
                break;
            }
            key_end += 1;
        }
        // Byte overlap inside one key means the ops are not a disjoint cover of
        // the range, so no merge is safe: degrade the WHOLE key to single-op
        // writes, exactly as before. Restore the caller's op order for those —
        // overlapping writes are the one shape where submission order is worth
        // preserving, and it keeps this byte-identical to the reference.
        let overlaps = order[key_start..key_end].windows(2).any(|pair| {
            let first = &ops[pair[0]];
            first.in_chunklet_off + first.data.len() as u64 > ops[pair[1]].in_chunklet_off
        });
        if overlaps {
            order[key_start..key_end].sort_unstable();
            spans.extend((key_start..key_end).map(|at| (at as u32, 1)));
            key_start = key_end;
            continue;
        }

        let run = &order[key_start..key_end];
        let mut group_start = key_start;
        let mut current_end = 0u64;
        let mut current_bytes = 0usize;
        for (at, &idx) in run.iter().enumerate().map(|(i, idx)| (key_start + i, idx)) {
            let op = &ops[idx];
            let adjacent = at > group_start && op.in_chunklet_off == current_end;
            if at > group_start
                && (!adjacent || current_bytes + op.data.len() > MAX_COALESCED_WRITE_BYTES)
            {
                spans.push((group_start as u32, (at - group_start) as u32));
                group_start = at;
                current_bytes = 0;
            }
            current_end = op.in_chunklet_off + op.data.len() as u64;
            current_bytes += op.data.len();
        }
        spans.push((group_start as u32, (key_end - group_start) as u32));
        key_start = key_end;
    }
    WriteGroups { order, spans }
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

    let entries: Vec<squeue::Entry> = (0..ops.len())
        .map(|i| {
            let (fd, abs) = targets[i];
            let (ptr, len) = ptrs[i];
            opcode::Read::new(types::Fd(fd), ptr, len)
                .offset(abs)
                .build()
                .user_data(i as u64)
        })
        .collect();
    push_batch(ring, &entries, "io_uring read")?;
    let completions = wait_and_drain(ring, ops.len(), "io_uring read");
    let mut first_err = completions.protocol_error.map(|error| {
        ChunkletError::Io(std::io::Error::other(format!(
            "io_uring read completion protocol: {error}"
        )))
    });
    let mut successful = vec![false; ops.len()];
    let mut short_indices = Vec::new();
    for (index, result) in completions.results.iter().enumerate() {
        let Some(res) = *result else {
            continue;
        };
        if res < 0 {
            let errno = -res;
            if first_err.is_none() {
                first_err = Some(ChunkletError::Io(std::io::Error::from_raw_os_error(errno)));
            }
        } else if res as u32 == ptrs[index].1 {
            successful[index] = true;
        } else if is_positive_short(res, ptrs[index].1) {
            short_indices.push(index);
        } else if first_err.is_none() {
            let reason = if res == 0 { "zero-length" } else { "oversized" };
            first_err = Some(ChunkletError::Io(std::io::Error::other(format!(
                "io_uring {reason} read completion op_idx={index}: {res} for {} bytes",
                ptrs[index].1
            ))));
        }
    }

    let recovered = attempt_all_indices(
        &short_indices,
        |index| {
            let op = &mut ops[index];
            let pd = op.pd.clone();
            match bounces[index].as_mut() {
                Some(buffer) => pd.read_chunklet_user_unbound(
                    op.chunklet_index,
                    op.in_chunklet_off,
                    buffer.as_mut_slice(),
                ),
                None => {
                    pd.read_chunklet_user_unbound(op.chunklet_index, op.in_chunklet_off, op.data)
                }
            }
        },
        |_, _| {},
    );
    for (index, result) in recovered {
        match result {
            Ok(()) => successful[index] = true,
            Err(error) if first_err.is_none() => {
                first_err = Some(ChunkletError::Io(std::io::Error::other(format!(
                    "io_uring short read exact retry op_idx={index} failed: {error}"
                ))));
            }
            Err(_) => {}
        }
    }

    for (index, (op, bounce)) in ops.iter_mut().zip(bounces.iter()).enumerate() {
        if !successful[index] {
            continue;
        }
        if let Some(buf) = bounce {
            op.data.copy_from_slice(&buf.as_slice()[..op.data.len()]);
        }
    }
    if let Some(err) = first_err {
        Err(err)
    } else {
        Ok(())
    }
}

/// One physical write handed to the ring.
///
/// `parts` are in DEVICE order and adjacent on the device by construction, but
/// need not be adjacent in memory: per-PD strip data is strided (24 KiB apart for
/// a 6+2 RAID6 stripe), so a merged group is inherently scattered. One part → an
/// `IORING_OP_WRITE`; several → one `IORING_OP_WRITEV`, which is what lets a
/// merge happen without copying the strips together.
struct PhysWrite<'a> {
    pd: &'a Arc<PhysicalDisk>,
    chunklet_index: u32,
    in_chunklet_off: u64,
    parts: &'a [&'a [u8]],
}

impl PhysWrite<'_> {
    fn total_len(&self) -> usize {
        self.parts.iter().map(|part| part.len()).sum()
    }

    /// O_DIRECT needs the device offset AND every iovec (base + length) block
    /// aligned — checking only the first part would submit an EINVAL.
    fn direct_aligned(&self, abs: u64) -> bool {
        abs.is_multiple_of(BLOCK_SIZE)
            && self
                .parts
                .iter()
                .all(|part| is_direct_aligned(0, part.len(), part.as_ptr() as usize))
    }

    /// Write every part at its own device offset through the caller-thread
    /// fallback. Used for exact recovery of a short vectored write, where there
    /// is no single contiguous buffer to re-issue.
    fn write_parts_exact(&self) -> ChunkletResult<()> {
        let mut off = self.in_chunklet_off;
        for part in self.parts {
            self.pd
                .write_chunklet_user_unbound(self.chunklet_index, off, part)?;
            off += part.len() as u64;
        }
        Ok(())
    }
}

/// Concatenate a scattered payload into one aligned buffer — the fallback when a
/// vectored group cannot be submitted as iovecs (a part or the device offset is
/// not block aligned, or there are more parts than `IOV_MAX`).
fn bounce_parts(parts: &[&[u8]]) -> ChunkletResult<AlignedBuf> {
    let total: usize = parts.iter().map(|part| part.len()).sum();
    let mut buffer = AlignedBuf::uninit(total)?;
    let mut cursor = 0;
    for part in parts {
        buffer.as_mut_slice()[cursor..cursor + part.len()].copy_from_slice(part);
        cursor += part.len();
    }
    Ok(buffer)
}

/// Submit one NUMA-homogeneous chunk and return a per-op result in the chunk's
/// input order (`results[i]` ↔ `ops[i]`). A pre-publication setup failure
/// (offset geometry, bounce allocation, atomic SQ push) marks the chunk failed.
/// After publication, the safety helper drains every CQE before this can return;
/// each CQE then determines its own result.
fn submit_chunk_detailed_with_callback(
    ring: &mut IoUring,
    ops: &[PhysWrite<'_>],
    stream_arrivals: bool,
    mut on_completed: impl FnMut(&[ValidatedCompletion]),
) -> Vec<ChunkletResult<()>> {
    let n = ops.len();

    // Resolve absolute offsets + fds while we still hold &op.
    let mut targets: Vec<(i32, u64)> = Vec::with_capacity(n);
    for op in ops {
        match op.pd.chunklet_user_abs_offset(
            op.chunklet_index,
            op.in_chunklet_off,
            op.total_len() as u64,
        ) {
            Ok(abs) => targets.push((op.pd.raw_fd(), abs)),
            Err(e) => {
                return failed_chunk_results(
                    n,
                    format!("io_uring chunk offset setup: {}", e),
                    &mut on_completed,
                )
            }
        }
    }

    // SQEs reference either the caller's already O_DIRECT-safe buffers or one of
    // these bounce buffers. Both Vecs are held until every CQE arrives.
    let mut bounces: Vec<AlignedBuf> = Vec::new();
    let mut iovecs: Vec<libc::iovec> = Vec::new();
    // Per op: either one contiguous (ptr, len) or an iovec range. `len` is the
    // total in both cases — it is what a completion must report.
    let mut ptrs: Vec<(*const u8, u32)> = Vec::with_capacity(n);
    let mut vectored: Vec<Option<(usize, usize)>> = Vec::with_capacity(n);
    for (op, (_fd, abs)) in ops.iter().zip(targets.iter()) {
        let total = op.total_len();
        if !op.direct_aligned(*abs) || op.parts.len() > MAX_WRITEV_PARTS {
            match bounce_parts(op.parts) {
                Ok(buf) => {
                    ptrs.push((buf.as_slice().as_ptr(), total as u32));
                    vectored.push(None);
                    bounces.push(buf);
                }
                Err(e) => {
                    return failed_chunk_results(
                        n,
                        format!("io_uring chunk bounce alloc: {}", e),
                        &mut on_completed,
                    )
                }
            }
            continue;
        }
        if op.parts.len() == 1 {
            ptrs.push((op.parts[0].as_ptr(), total as u32));
            vectored.push(None);
            continue;
        }
        let start = iovecs.len();
        iovecs.extend(op.parts.iter().map(|part| libc::iovec {
            iov_base: part.as_ptr() as *mut libc::c_void,
            iov_len: part.len(),
        }));
        ptrs.push((op.parts[0].as_ptr(), total as u32));
        vectored.push(Some((start, op.parts.len())));
    }

    let entries: Vec<squeue::Entry> = (0..n)
        .map(|i| {
            let (fd, abs) = targets[i];
            match vectored[i] {
                // SAFETY-adjacent invariant: `iovecs` is not resized after this
                // point and outlives the CQE drain below, so the kernel's view of
                // the array stays valid for the whole IO.
                Some((start, count)) => {
                    opcode::Writev::new(types::Fd(fd), iovecs[start..].as_ptr(), count as u32)
                        .offset(abs)
                        .build()
                        .user_data(i as u64)
                }
                None => {
                    let (ptr, len) = ptrs[i];
                    opcode::Write::new(types::Fd(fd), ptr, len)
                        .offset(abs)
                        .build()
                        .user_data(i as u64)
                }
            }
        })
        .collect();
    if let Err(error) = push_batch(ring, &entries, "io_uring write") {
        return failed_chunk_results(n, error.to_string(), &mut on_completed);
    }
    let mut callback = CompletionCallback::new(&mut on_completed);
    let mut short_indices = Vec::new();
    // Short-write detection and the terminal-completion forwarding below only
    // ACCUMULATE, so they are indifferent to whether arrivals land one at a time
    // or in one bulk drain. Streaming callers (scheduler credit release) still
    // opt out via `stream_arrivals`.
    let coalesce = !stream_arrivals && coalesced_wait_enabled();
    let completions =
        wait_and_drain_with_mode(ring, n, "io_uring write", coalesce, &mut |arrivals| {
            let terminal = partition_write_completions(arrivals, &ptrs, &mut short_indices);
            callback.notify(&terminal);
        });

    let mut retry_results: Vec<Option<ChunkletResult<()>>> = (0..n).map(|_| None).collect();
    let recovered = attempt_all_indices(
        &short_indices,
        // A vectored write reports only the TOTAL bytes transferred, so a short
        // one cannot be resumed from an offset — re-issue every part at its own
        // device offset instead. That is exact and idempotent, and strictly more
        // precise than re-writing a whole merged group as one buffer.
        |index| ops[index].write_parts_exact(),
        |index, result| {
            let completion = ValidatedCompletion {
                index,
                result: if result.is_ok() {
                    ptrs[index].1 as i32
                } else {
                    -libc::EIO
                },
            };
            callback.notify(std::slice::from_ref(&completion));
        },
    );
    for (index, result) in recovered {
        retry_results[index] = Some(result.map_err(|error| {
            ChunkletError::Io(std::io::Error::other(format!(
                "io_uring short write exact retry op_idx={index} failed: {error}"
            )))
        }));
    }

    if let Some(error) = completions.protocol_error {
        let missing: Vec<_> = completions
            .results
            .iter()
            .enumerate()
            .filter_map(|(index, result)| {
                result.is_none().then_some(ValidatedCompletion {
                    index,
                    result: -libc::EPROTO,
                })
            })
            .collect();
        callback.notify(&missing);
        callback.resume_if_panicked();
        return write_error_results(n, format!("io_uring write completion protocol: {error}"));
    }

    let results = completions
        .results
        .into_iter()
        .enumerate()
        .map(|(index, result)| {
            let res = result.expect("validated completion batch has every write result");
            if let Some(retry_result) = retry_results[index].take() {
                retry_result
            } else if res < 0 {
                Err(ChunkletError::Io(std::io::Error::from_raw_os_error(-res)))
            } else if res as u32 == ptrs[index].1 {
                Ok(())
            } else if res == 0 {
                Err(ChunkletError::Io(std::io::Error::other(format!(
                    "io_uring zero-length write completion op_idx={index} for {} bytes",
                    ptrs[index].1
                ))))
            } else {
                Err(ChunkletError::Io(std::io::Error::other(format!(
                    "io_uring oversized write completion op_idx={index}: {res} for {} bytes",
                    ptrs[index].1
                ))))
            }
        })
        .collect();
    callback.resume_if_panicked();
    results
}

fn is_positive_short(result: i32, expected: u32) -> bool {
    result > 0 && (result as u32) < expected
}

fn partition_write_completions(
    arrivals: &[ValidatedCompletion],
    ptrs: &[(*const u8, u32)],
    short_indices: &mut Vec<usize>,
) -> Vec<ValidatedCompletion> {
    let mut terminal = Vec::with_capacity(arrivals.len());
    for &completion in arrivals {
        if is_positive_short(completion.result, ptrs[completion.index].1) {
            short_indices.push(completion.index);
        } else {
            terminal.push(completion);
        }
    }
    terminal
}

/// Run every exact-IO recovery even if an earlier one fails. `after` runs only
/// after its corresponding operation has reached a terminal result, allowing
/// write observers to delay credit release until recovery really completed.
fn attempt_all_indices(
    indices: &[usize],
    mut operation: impl FnMut(usize) -> ChunkletResult<()>,
    mut after: impl FnMut(usize, &ChunkletResult<()>),
) -> Vec<(usize, ChunkletResult<()>)> {
    indices
        .iter()
        .map(|&index| {
            let result = operation(index);
            after(index, &result);
            (index, result)
        })
        .collect()
}

fn failed_chunk_results(
    count: usize,
    message: String,
    on_completed: &mut impl FnMut(&[ValidatedCompletion]),
) -> Vec<ChunkletResult<()>> {
    let completed: Vec<_> = (0..count)
        .map(|index| ValidatedCompletion {
            index,
            result: -libc::EIO,
        })
        .collect();
    if !completed.is_empty() {
        on_completed(&completed);
    }
    write_error_results(count, message)
}

fn write_error_results(count: usize, message: String) -> Vec<ChunkletResult<()>> {
    (0..count)
        .map(|_| Err(ChunkletError::Io(std::io::Error::other(message.clone()))))
        .collect()
}

fn submit_reads_serial(ops: &mut [StripRead<'_>]) -> ChunkletResult<()> {
    submit_reads_serial_with(ops, |op| {
        op.pd
            .read_chunklet_user_unbound(op.chunklet_index, op.in_chunklet_off, op.data)
    })
}

fn submit_reads_serial_with(
    ops: &mut [StripRead<'_>],
    mut read: impl FnMut(&mut StripRead<'_>) -> ChunkletResult<()>,
) -> ChunkletResult<()> {
    let mut first_error = None;
    for op in ops {
        if let Err(error) = read(op) {
            if first_error.is_none() {
                first_error = Some(error);
            }
        }
    }
    first_error.map_or(Ok(()), Err)
}

fn submit_writes_serial(ops: &[StripWrite<'_>]) -> Vec<ChunkletResult<()>> {
    ops.iter()
        .map(|op| {
            op.pd
                .write_chunklet_user_unbound(op.chunklet_index, op.in_chunklet_off, op.data)
        })
        .collect()
}

fn submit_flushes_serial(pds: &[Arc<PhysicalDisk>]) -> ChunkletResult<()> {
    submit_flushes_serial_with(pds, |pd| pd.sync())
}

fn submit_flushes_serial_with(
    pds: &[Arc<PhysicalDisk>],
    mut sync: impl FnMut(&Arc<PhysicalDisk>) -> ChunkletResult<()>,
) -> ChunkletResult<()> {
    let mut seen = BTreeSet::new();
    let mut first_error = None;
    for pd in pds {
        if !seen.insert(pd.pd_id()) {
            continue;
        }
        if let Err(error) = sync(pd) {
            if first_error.is_none() {
                first_error = Some(error);
            }
        }
    }
    first_error.map_or(Ok(()), Err)
}

fn notify_all_completed(
    observer: &dyn WriteCompletionObserver,
    count: usize,
    service_started: Instant,
) {
    if count == 0 {
        return;
    }
    let indices: Vec<_> = (0..count).collect();
    observer.writes_completed(&indices, elapsed_ns(service_started));
}

fn elapsed_ns(started: Instant) -> u64 {
    started.elapsed().as_nanos().min(u64::MAX as u128) as u64
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
    use parking_lot::Mutex;
    use std::cell::{Cell, RefCell};
    // Only the `reference_write_groups` shadow implementation needs a map now.
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use tempfile::TempDir;

    #[derive(Default)]
    struct RecordingObserver {
        completions: Mutex<Vec<(Vec<usize>, u64)>>,
    }

    impl WriteCompletionObserver for RecordingObserver {
        fn writes_completed(&self, op_indices: &[usize], service_ns: u64) {
            self.completions
                .lock()
                .push((op_indices.to_vec(), service_ns));
        }
    }

    fn test_pd(dir: &TempDir, name: &str, pd_seq: u32, sync_required: bool) -> Arc<PhysicalDisk> {
        let mut raw =
            crate::io::RawDevice::open_or_create(&dir.path().join(name), 4 * 1024 * 1024 * 1024)
                .unwrap();
        raw.set_sync_required_for_test(sync_required);
        PhysicalDisk::init(
            raw,
            PoolId::new_v4(),
            PdId::new_v4(),
            pd_seq,
            1,
            vec![],
            0,
            vec![],
            vec![],
        )
        .unwrap()
    }

    #[test]
    fn write_through_flushes_are_all_skipped() {
        let dir = TempDir::new().unwrap();
        let pds = vec![
            test_pd(&dir, "pd0", 0, false),
            test_pd(&dir, "pd1", 1, false),
        ];
        submit_required_flushes(&pds, |_| panic!("write-through PDs must not be submitted"))
            .unwrap();
    }

    #[test]
    fn mixed_flushes_submit_only_required_devices() {
        let dir = TempDir::new().unwrap();
        let skipped = test_pd(&dir, "pd0", 0, false);
        let required_a = test_pd(&dir, "pd1", 1, true);
        let required_b = test_pd(&dir, "pd2", 2, true);
        let expected = vec![required_a.pd_id(), required_b.pd_id()];
        let pds = vec![skipped, required_a.clone(), required_b, required_a];

        submit_required_flushes(&pds, |submitted| {
            assert_eq!(
                submitted.iter().map(|pd| pd.pd_id()).collect::<Vec<_>>(),
                expected
            );
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn required_flush_error_is_propagated() {
        let dir = TempDir::new().unwrap();
        let pds = vec![test_pd(&dir, "pd0", 0, true)];
        let error = submit_required_flushes(&pds, |_| {
            Err(ChunkletError::Io(std::io::Error::from_raw_os_error(
                libc::EIO,
            )))
        })
        .unwrap_err();

        assert!(error.to_string().contains("Input/output error"));
    }

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

        assert_eq!(group_vecs(&coalesced_write_groups(&writes)), vec![vec![0, 2, 1]]);
    }

    /// The pre-2026-08-02 grouping, kept verbatim as the reference the flat
    /// [`WriteGroups`] builder is checked against. Same partition, same member
    /// order, same emission order — the rewrite is an allocation change only, so
    /// any divergence here is a bug in the rewrite and not a policy choice.
    fn reference_write_groups(ops: &[StripWrite<'_>]) -> Vec<Vec<usize>> {
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

    fn group_vecs(groups: &WriteGroups) -> Vec<Vec<usize>> {
        groups.iter().map(|group| group.to_vec()).collect()
    }

    /// Build a [`WriteGroups`] from an explicit partition, for tests that only
    /// care about the group → original-index expansion.
    fn groups_from(partition: &[&[usize]]) -> WriteGroups {
        let mut order = Vec::new();
        let mut spans = Vec::new();
        for group in partition {
            spans.push((order.len() as u32, group.len() as u32));
            order.extend_from_slice(group);
        }
        WriteGroups { order, spans }
    }

    /// Randomised equivalence against [`reference_write_groups`] over shapes that
    /// exercise every rule: several PDs and chunklets, adjacency runs, holes,
    /// byte overlap (whole-key degrade), and groups long enough to hit
    /// [`MAX_COALESCED_WRITE_BYTES`].
    #[test]
    fn write_groups_match_the_reference_grouping() {
        use rand::rngs::StdRng;
        use rand::{Rng, SeedableRng};

        let dir = TempDir::new().unwrap();
        let pds: Vec<Arc<PhysicalDisk>> = (0..3)
            .map(|i| test_pd(&dir, &format!("pd{i}"), i, false))
            .collect();
        // One page per distinct length so `data.len()` can vary independently.
        let page4k = vec![0u8; 4096];
        let page8k = vec![0u8; 8192];
        let mut rng = StdRng::seed_from_u64(0xC0A1E5CE);

        for round in 0..400 {
            let n = rng.gen_range(1..40);
            let mut ops: Vec<StripWrite<'_>> = Vec::with_capacity(n);
            for _ in 0..n {
                let data: &[u8] = if rng.gen_bool(0.75) { &page4k } else { &page8k };
                // A small offset lattice makes adjacency AND overlap both common.
                let slot: u64 = rng.gen_range(0..8);
                ops.push(StripWrite {
                    pd: pds[rng.gen_range(0..pds.len())].clone(),
                    chunklet_index: rng.gen_range(0..2),
                    in_chunklet_off: slot * 4096,
                    data,
                });
            }
            assert_eq!(
                group_vecs(&coalesced_write_groups(&ops)),
                reference_write_groups(&ops),
                "round {round} diverged for {} ops",
                ops.len()
            );
        }
    }

    /// The byte cap must still cut a long adjacency run, and at the same place.
    #[test]
    fn write_groups_respect_the_coalesce_byte_cap() {
        let dir = TempDir::new().unwrap();
        let pd = test_pd(&dir, "pd0", 0, false);
        let page = vec![0u8; 4096];
        let per_group = MAX_COALESCED_WRITE_BYTES / page.len();
        let n = per_group + 3;
        let ops: Vec<StripWrite<'_>> = (0..n)
            .map(|i| StripWrite {
                pd: pd.clone(),
                chunklet_index: 0,
                in_chunklet_off: i as u64 * 4096,
                data: &page,
            })
            .collect();
        let groups = coalesced_write_groups(&ops);
        assert_eq!(group_vecs(&groups), reference_write_groups(&ops));
        assert_eq!(groups.len(), 2);
        assert_eq!(groups.group(0).len(), per_group);
        assert_eq!(groups.group(1).len(), 3);
    }

    /// The whole point of the rewrite: no allocation per group. `order` is one
    /// vec sized by the op count and `spans` one vec of `(start, len)`, so a
    /// 567-op LV3 wave allocates twice rather than ~1600 times.
    #[test]
    fn write_groups_allocate_twice_regardless_of_group_count() {
        let dir = TempDir::new().unwrap();
        let pd = test_pd(&dir, "pd0", 0, false);
        let page = vec![0u8; 4096];
        // Stride 2 pages ⇒ nothing adjacent ⇒ one group per op, the worst case.
        let ops: Vec<StripWrite<'_>> = (0..64)
            .map(|i| StripWrite {
                pd: pd.clone(),
                chunklet_index: 0,
                in_chunklet_off: i as u64 * 8192,
                data: &page,
            })
            .collect();
        let groups = coalesced_write_groups(&ops);
        assert_eq!(groups.len(), 64);
        assert!(groups.all_single());
        assert_eq!(groups.order.len(), ops.len());
        assert!(groups.order.capacity() <= ops.len());
        assert!(groups.spans.capacity() <= ops.len());
    }

    /// Strip slices carved out of one aligned buffer, which is what an LD hands
    /// in on the full-stripe path. Plain arrays/`Vec`s are NOT reliably 4 KiB
    /// aligned, and O_DIRECT checks every iovec, so a fixture built from those
    /// would silently test the bounce fallback instead of the vectored path.
    fn aligned_pages(count: usize) -> AlignedBuf {
        let mut buf = AlignedBuf::new(count * 4096).unwrap();
        for (i, byte) in buf.as_mut_slice().iter_mut().enumerate() {
            *byte = ((i / 4096 + 1) * 7 + i % 251) as u8;
        }
        buf
    }

    #[test]
    fn writev_safety_requires_every_part_to_be_block_aligned() {
        let dir = TempDir::new().unwrap();
        let pd = test_pd(&dir, "pd0", 0, false);
        let pages = aligned_pages(3);
        let aligned: Vec<StripWrite<'_>> = (0..3)
            .map(|i| StripWrite {
                pd: pd.clone(),
                chunklet_index: 0,
                in_chunklet_off: i as u64 * 4096,
                data: &pages.as_slice()[i * 4096..(i + 1) * 4096],
            })
            .collect();
        assert!(group_is_writev_safe(&aligned, &[0, 1, 2]));

        // One part shifted off a block boundary in MEMORY (its device offset is
        // still fine) must sink the whole group to the bounce path.
        let skewed_page = &pages.as_slice()[512..512 + 4096];
        let mut skewed = aligned.clone();
        skewed[1] = StripWrite {
            pd: pd.clone(),
            chunklet_index: 0,
            in_chunklet_off: 4096,
            data: skewed_page,
        };
        assert!(!group_is_writev_safe(&skewed, &[0, 1, 2]));

        // A short (non-block-multiple) part is equally unsubmittable.
        let mut stubby = aligned.clone();
        stubby[2] = StripWrite {
            pd,
            chunklet_index: 0,
            in_chunklet_off: 8192,
            data: &pages.as_slice()[8192..8192 + 2048],
        };
        assert!(!group_is_writev_safe(&stubby, &[0, 1, 2]));
    }

    #[test]
    fn bounce_parts_concatenates_in_order_and_zeroes_the_tail() {
        let a = [0xaa; 1000];
        let b = [0xbb; 2000];
        let buf = bounce_parts(&[&a, &b]).unwrap();
        assert_eq!(buf.len(), 4096, "rounded up to one block");
        assert!(buf.as_slice()[..1000].iter().all(|&x| x == 0xaa));
        assert!(buf.as_slice()[1000..3000].iter().all(|&x| x == 0xbb));
        assert!(
            buf.as_slice()[3000..].iter().all(|&x| x == 0),
            "the pad must be zeroed, not allocator garbage"
        );
    }

    /// The new short-write recovery for a vectored group: there is no contiguous
    /// buffer to re-issue, so each part must be rewritten at ITS OWN device
    /// offset. Getting the offset arithmetic wrong here would silently duplicate
    /// one strip over another.
    #[test]
    fn write_parts_exact_lands_every_part_at_its_own_offset() {
        let dir = TempDir::new().unwrap();
        let pd = test_pd(&dir, "pd0", 0, false);
        let pages = aligned_pages(3);
        let parts: Vec<&[u8]> = (0..3)
            .map(|i| &pages.as_slice()[i * 4096..(i + 1) * 4096])
            .collect();
        let write = PhysWrite {
            pd: &pd,
            chunklet_index: 0,
            in_chunklet_off: 4096,
            parts: &parts,
        };
        write.write_parts_exact().unwrap();

        let mut got = vec![0u8; 3 * 4096];
        pd.read_chunklet_user(0, 4096, &mut got).unwrap();
        assert_eq!(got, pages.as_slice(), "parts must be contiguous on device");
        // ...and nothing landed at the offset the group did NOT cover.
        let mut before = vec![0xffu8; 4096];
        pd.read_chunklet_user(0, 0, &mut before).unwrap();
        assert!(before.iter().all(|&b| b == 0), "wrote below the group start");
    }

    /// End-to-end: a merged group submitted as iovecs must land byte-identical to
    /// the same strips written separately. Correctness is the gate here; the
    /// `CHUNKLET_WRITEV_COALESCE=1` suite sweep is what proves every OTHER caller
    /// still works when this path is taken.
    #[test]
    fn vectored_merged_group_round_trips_like_separate_writes() {
        let dir = TempDir::new().unwrap();
        let pd = test_pd(&dir, "pd0", 0, false);
        let backend = match UringBackend::new() {
            Ok(backend) => backend,
            Err(error) => {
                eprintln!("io_uring unavailable; skipping vectored submit: {error}");
                return;
            }
        };
        let pages = aligned_pages(6);
        let writes: Vec<StripWrite<'_>> = (0..6)
            .map(|i| StripWrite {
                pd: pd.clone(),
                chunklet_index: 0,
                // Interleaved arrival order, adjacent on the device: exactly the
                // shape `coalesced_write_groups` exists to merge.
                in_chunklet_off: [0u64, 8192, 4096, 20480, 12288, 16384][i],
                data: &pages.as_slice()[i * 4096..(i + 1) * 4096],
            })
            .collect();
        let groups = coalesced_write_groups(&writes);
        assert_eq!(groups.len(), 1, "the fixture must merge into one group");
        assert!(group_is_writev_safe(&writes, groups.group(0)));

        let previous = writev_coalesce_enabled();
        set_writev_coalesce(true);
        let results = backend.submit_writes_detailed_with_class(IoClass::DrainData, &writes);
        set_writev_coalesce(previous);
        assert!(results.iter().all(Result::is_ok), "{results:?}");

        // Device order is by offset, so read back and compare against the strips
        // re-sorted the same way.
        let mut got = vec![0u8; 6 * 4096];
        pd.read_chunklet_user(0, 0, &mut got).unwrap();
        let mut expected = vec![0u8; 6 * 4096];
        for write in &writes {
            let at = write.in_chunklet_off as usize;
            expected[at..at + 4096].copy_from_slice(write.data);
        }
        assert_eq!(got, expected);
    }

    #[test]
    fn physical_completions_expand_to_coalesced_originals_with_global_offset() {
        let groups = groups_from(&[&[2, 0], &[1], &[3, 4]]);
        let completions = vec![
            ValidatedCompletion {
                index: 2,
                result: 8192,
            },
            ValidatedCompletion {
                index: 0,
                result: 8192,
            },
        ];

        assert_eq!(
            map_physical_completions(&groups, &completions, 64),
            vec![67, 68, 66, 64]
        );
    }

    #[test]
    fn short_write_callback_waits_for_exact_recovery_then_expands_coalesced_indices() {
        let ptrs = vec![(std::ptr::null(), 4096)];
        let arrivals = [ValidatedCompletion {
            index: 0,
            result: 2048,
        }];
        let mut short_indices = Vec::new();
        let terminal = partition_write_completions(&arrivals, &ptrs, &mut short_indices);
        assert!(terminal.is_empty());
        assert_eq!(short_indices, vec![0]);

        let recovered = Cell::new(false);
        let published = RefCell::new(Vec::new());
        let groups = groups_from(&[&[0, 2]]);
        let mut observer = |completions: &[ValidatedCompletion]| {
            assert!(
                recovered.get(),
                "short completion escaped before exact retry"
            );
            published
                .borrow_mut()
                .extend(map_physical_completions(&groups, completions, 64));
        };
        let mut callback = CompletionCallback::new(&mut observer);
        callback.notify(&terminal);
        assert!(published.borrow().is_empty());

        let results = attempt_all_indices(
            &short_indices,
            |_| {
                recovered.set(true);
                Ok(())
            },
            |index, result| {
                callback.notify(&[ValidatedCompletion {
                    index,
                    result: if result.is_ok() { 4096 } else { -libc::EIO },
                }]);
            },
        );
        callback.resume_if_panicked();

        assert!(results[0].1.is_ok());
        assert_eq!(*published.borrow(), vec![64, 66]);
    }

    #[test]
    fn short_recovery_attempts_every_index_after_an_early_error() {
        let attempted = RefCell::new(Vec::new());
        let terminal = RefCell::new(Vec::new());
        let results = attempt_all_indices(
            &[2, 0, 1],
            |index| {
                attempted.borrow_mut().push(index);
                if index == 2 {
                    Err(ChunkletError::Invariant("first recovery error".into()))
                } else {
                    Ok(())
                }
            },
            |index, _| terminal.borrow_mut().push(index),
        );

        assert_eq!(*attempted.borrow(), vec![2, 0, 1]);
        assert_eq!(*terminal.borrow(), vec![2, 0, 1]);
        assert!(results[0].1.is_err());
        assert!(results[1].1.is_ok());
        assert!(results[2].1.is_ok());
    }

    #[test]
    fn observer_panic_during_short_recovery_waits_for_every_retry() {
        let attempts = Cell::new(0usize);
        let callbacks = Cell::new(0usize);
        let unwind = catch_unwind(AssertUnwindSafe(|| {
            let mut observer = |_completions: &[ValidatedCompletion]| {
                callbacks.set(callbacks.get() + 1);
                panic!("recovery observer panic");
            };
            let mut callback = CompletionCallback::new(&mut observer);
            let _ = attempt_all_indices(
                &[0, 1],
                |_| {
                    attempts.set(attempts.get() + 1);
                    Ok(())
                },
                |index, result| {
                    callback.notify(&[ValidatedCompletion {
                        index,
                        result: if result.is_ok() { 4096 } else { -libc::EIO },
                    }]);
                },
            );
            callback.resume_if_panicked();
        }));

        assert!(unwind.is_err());
        assert_eq!(attempts.get(), 2);
        assert_eq!(callbacks.get(), 1);
    }

    #[test]
    fn zero_and_oversized_completions_are_terminal_not_retryable() {
        assert!(is_positive_short(2048, 4096));
        assert!(!is_positive_short(0, 4096));
        assert!(!is_positive_short(4096, 4096));
        assert!(!is_positive_short(8192, 4096));
        assert!(!is_positive_short(-libc::EIO, 4096));
    }

    #[test]
    fn serial_read_fallback_attempts_all_ops_and_returns_first_error() {
        let dir = TempDir::new().unwrap();
        let first = test_pd(&dir, "pd0", 0, false);
        let second = test_pd(&dir, "pd1", 1, false);
        let mut first_data = [0_u8; 4096];
        let mut second_data = [0_u8; 4096];
        let mut third_data = [0_u8; 4096];
        let mut ops = vec![
            StripRead {
                pd: first.clone(),
                chunklet_index: 0,
                in_chunklet_off: 0,
                data: &mut first_data,
            },
            StripRead {
                pd: second,
                chunklet_index: 0,
                in_chunklet_off: 0,
                data: &mut second_data,
            },
            StripRead {
                pd: first,
                chunklet_index: 0,
                in_chunklet_off: 4096,
                data: &mut third_data,
            },
        ];
        let attempts = RefCell::new(Vec::new());

        let error = submit_reads_serial_with(&mut ops, |op| {
            let attempt = attempts.borrow().len();
            attempts.borrow_mut().push(op.pd.pd_id());
            Err(ChunkletError::Invariant(format!("read-error-{attempt}")))
        })
        .unwrap_err();

        assert_eq!(attempts.borrow().len(), 3);
        assert!(error.to_string().contains("read-error-0"));
    }

    #[test]
    fn serial_flush_fallback_attempts_each_distinct_pd_and_returns_first_error() {
        let dir = TempDir::new().unwrap();
        let first = test_pd(&dir, "pd0", 0, true);
        let second = test_pd(&dir, "pd1", 1, true);
        let third = test_pd(&dir, "pd2", 2, true);
        let pds = vec![first.clone(), second, first, third];
        let attempts = RefCell::new(Vec::new());

        let error = submit_flushes_serial_with(&pds, |pd| {
            let attempt = attempts.borrow().len();
            attempts.borrow_mut().push(pd.pd_id());
            if attempt == 1 {
                Ok(())
            } else {
                Err(ChunkletError::Invariant(format!("flush-error-{attempt}")))
            }
        })
        .unwrap_err();

        assert_eq!(attempts.borrow().len(), 3);
        assert!(error.to_string().contains("flush-error-0"));
    }

    #[test]
    fn caller_ring_observer_reports_each_coalesced_original_once() {
        let backend = match UringBackend::new() {
            Ok(backend) => backend,
            Err(error) => {
                eprintln!("io_uring unavailable; skipping observed caller-ring test: {error}");
                return;
            }
        };
        let dir = TempDir::new().unwrap();
        let first = test_pd(&dir, "pd0", 0, false);
        let second = test_pd(&dir, "pd1", 1, false);
        let pages = [[0x31; 4096], [0x42; 4096], [0x53; 4096]];
        let writes = vec![
            StripWrite {
                pd: first.clone(),
                chunklet_index: 0,
                in_chunklet_off: 0,
                data: &pages[0],
            },
            StripWrite {
                pd: second,
                chunklet_index: 0,
                in_chunklet_off: 0,
                data: &pages[1],
            },
            StripWrite {
                pd: first,
                chunklet_index: 0,
                in_chunklet_off: 4096,
                data: &pages[2],
            },
        ];
        let observer = RecordingObserver::default();

        let results = backend.submit_writes_detailed_observed_with_class(
            IoClass::DrainData,
            &writes,
            &observer,
        );
        assert!(results.iter().all(Result::is_ok));
        let completions = observer.completions.lock();
        assert!(completions.iter().all(|(_, service_ns)| *service_ns > 0));
        let mut indices: Vec<_> = completions
            .iter()
            .flat_map(|(indices, _)| indices.iter().copied())
            .collect();
        indices.sort_unstable();
        assert_eq!(indices, vec![0, 1, 2]);
    }

    /// When the thread's ring is disabled (the EMFILE outcome), reads and
    /// writes run serially on this same thread without rebinding its affinity.
    #[test]
    fn disabled_ring_uses_same_thread_serial_fallback() {
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
        let backend = UringBackend::new().unwrap();
        backend.submit_writes(&writes).unwrap();

        let mut got = vec![0u8; 4096];
        {
            let mut reads = vec![StripRead {
                pd: pd.clone(),
                chunklet_index: 0,
                in_chunklet_off: 0,
                data: &mut got,
            }];
            backend.submit_reads(&mut reads).unwrap();
        }
        assert_eq!(
            got, payload,
            "degraded read-back must match the degraded write"
        );
    }

    #[test]
    fn pooled_backend_writes_on_pd_groups_and_reports_class_metrics() {
        let dir = TempDir::new().unwrap();
        let first = test_pd(&dir, "pd0", 0, false);
        let second = test_pd(&dir, "pd1", 1, false);
        let backend = match UringBackend::new_pooled(2, 1) {
            Ok(backend) => backend,
            Err(error) => {
                eprintln!("io_uring unavailable; skipping pooled ring exercise: {error}");
                return;
            }
        };
        let first_page = [0x71; 4096];
        let second_page = [0x82; 4096];
        let writes = vec![
            StripWrite {
                pd: first.clone(),
                chunklet_index: 0,
                in_chunklet_off: 0,
                data: &first_page,
            },
            StripWrite {
                pd: second.clone(),
                chunklet_index: 0,
                in_chunklet_off: 0,
                data: &second_page,
            },
        ];

        let results = backend.submit_writes_detailed_with_class(IoClass::DrainData, &writes);
        assert!(results.iter().all(Result::is_ok));
        let mut first_readback = [0u8; 4096];
        let mut second_readback = [0u8; 4096];
        first.read_chunklet_user(0, 0, &mut first_readback).unwrap();
        second
            .read_chunklet_user(0, 0, &mut second_readback)
            .unwrap();
        assert_eq!(first_readback, first_page);
        assert_eq!(second_readback, second_page);

        let snapshot = backend.execution_snapshot().unwrap();
        assert!(snapshot.enabled);
        assert_eq!(snapshot.foreground_workers, 2);
        assert_eq!(snapshot.background_workers, 1);
        let drain_data = snapshot
            .classes
            .iter()
            .find(|class| class.class == IoClass::DrainData)
            .unwrap();
        assert_eq!(drain_data.batches, 1);
        assert_eq!(drain_data.groups, 2);
        assert_eq!(drain_data.ops, 2);
    }
}
