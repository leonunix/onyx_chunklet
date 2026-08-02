//! `IoBackend` trait + cross-PD batched-write op type.
//!
//! All chunklet IOs that fan out across multiple PDs (full-stripe / RW
//! / PDW dispatchers in `LdRaid5` and `LdRaid6`) build strip-level ops
//! and hand them to `IoBackend`. The trait gives us two
//! interchangeable implementations:
//!
//! - **`SyncBackend`** (`src/io/sync_backend.rs`): one `std::thread::scope`
//!   spawn per write, each thread calls the standard `pwrite` loop on the
//!   PD. Always available, no kernel feature requirements. Default.
//! - **`UringBackend`** (`src/io/uring_backend.rs`, Linux only): a
//!   thread-local `io_uring` instance batches every write in `ops` into a
//!   single submit; the calling thread blocks on `submit_and_wait` until
//!   all completions land. Saves K spawns per stripe write and collapses
//!   K pwrite syscalls into one submit + one wait.
//!
//! The choice is made at `Pool` construction via `PoolConfig::io_backend`
//! and stamped onto every PD in the pool. `submit_writes` is invoked
//! through the backend that lives on `ops[0].pd` — every PD in a single
//! batch belongs to the same pool, so the backend pointer is uniform.

use std::sync::Arc;

use crate::error::ChunkletResult;
use crate::io::scheduler::{
    current_io_class, with_io_class, IoClass, ScheduledBackend, SchedulerConfig, SchedulerSnapshot,
};
use crate::pd::PhysicalDisk;
use crate::types::PdId;

/// Persistent write-execution metrics for one scheduling class. Queue wait is
/// measured from scoped task creation until a worker starts the PD group;
/// execute time covers the group's backend submit through all completions.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IoExecutionClassSnapshot {
    pub class: IoClass,
    pub batches: u64,
    pub groups: u64,
    pub ops: u64,
    pub queue_wait_ns: u64,
    pub queue_wait_max_ns: u64,
    pub execute_ns: u64,
    pub execute_max_ns: u64,
}

/// Immutable state for an optional persistent write-execution pool.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IoExecutionSnapshot {
    pub enabled: bool,
    pub foreground_workers: usize,
    pub background_workers: usize,
    pub foreground_cpus: Vec<usize>,
    pub background_cpus: Vec<usize>,
    pub cpu_sets_disjoint: bool,
    pub classes: Vec<IoExecutionClassSnapshot>,
}

/// Optional persistent io_uring execution-pool layout. Empty CPU vectors let
/// workers inherit the creating thread's affinity; non-empty vectors bind each
/// pool's workers when they start.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct UringPoolConfig {
    pub foreground_workers: usize,
    pub background_workers: usize,
    pub foreground_cpus: Vec<usize>,
    pub background_cpus: Vec<usize>,
    /// Wait for a whole batch in one `io_uring_enter` rather than waking once
    /// per CQE on the no-observer drain path. Default `false` (legacy wake).
    pub coalesced_wait: bool,
    /// SQEs per stop-and-wait wave on the batched submit paths. `0` keeps the
    /// historical 64-op wave; larger values cut the number of drain-and-refill
    /// barriers a many-strip write must cross (capped by the ring depth).
    pub write_chunk_ops: usize,
    /// Submit an adjacency-merged group as one `IORING_OP_WRITEV` with an iovec
    /// per strip, instead of copying the strips into one bounce buffer. Default
    /// `false` (bounce, the long-shipped path).
    ///
    /// Per-PD strip data is strided in memory (24 KiB apart for a 6+2 RAID6
    /// stripe), so a merged group can never be contiguous in the caller's buffer
    /// — the copy is inherent to merging, and it is what made merging a net loss
    /// on the box: raising the LV3 merge cut 0.29 ms/call of SQE wait but added
    /// 0.94 ms/call to the materialise stage.
    pub writev_coalesce: bool,
}

impl UringPoolConfig {
    pub fn new(foreground_workers: usize, background_workers: usize) -> Self {
        Self {
            foreground_workers,
            background_workers,
            foreground_cpus: Vec::new(),
            background_cpus: Vec::new(),
            coalesced_wait: false,
            write_chunk_ops: 0,
            writev_coalesce: false,
        }
    }
}

/// One PD-level write prepared by an LD. The lifetime ties `data` to
/// whatever buffer the caller owns; backends are required to issue + wait
/// for the IO before returning, so the borrow only needs to outlive a
/// single `submit_writes` call.
///
/// `Clone` is cheap (an `Arc` bump + `Copy` fields + a shared-slice copy) and
/// is used by [`submit_strip_writes_detailed`] to reorder ops by NUMA node
/// while keeping the caller's original ordering for the returned per-op results.
#[derive(Clone)]
pub struct StripWrite<'a> {
    pub pd: Arc<PhysicalDisk>,
    pub chunklet_index: u32,
    pub in_chunklet_off: u64,
    pub data: &'a [u8],
}

/// One PD-level read prepared by an LD. Backends issue + wait before
/// returning, so the mutable borrow only needs to live for one call.
pub struct StripRead<'a> {
    pub pd: Arc<PhysicalDisk>,
    pub chunklet_index: u32,
    pub in_chunklet_off: u64,
    pub data: &'a mut [u8],
}

/// Completion sink for synchronous batched writes. Implementations may call
/// this concurrently and in any order, but exactly once per completed input
/// index. `service_ns` is elapsed time from the start of backend execution to
/// this completion. An execution pool must exclude its worker-queue residence
/// and report that separately, so scheduler service time remains comparable to
/// the caller-thread path. Implementations must stop calling this before the
/// submit method returns.
pub trait WriteCompletionObserver: Send + Sync {
    fn writes_completed(&self, op_indices: &[usize], service_ns: u64);
}

struct IgnoreWriteCompletions;

impl WriteCompletionObserver for IgnoreWriteCompletions {
    fn writes_completed(&self, _op_indices: &[usize], _service_ns: u64) {}
}

/// One scheduler-admitted write. `index` always refers to the original input
/// position, while `write` keeps borrowing the caller-owned payload for no
/// longer than the surrounding synchronous backend call.
#[derive(Clone)]
pub struct DispatchedWrite<'a> {
    pub index: usize,
    pub write: StripWrite<'a>,
}

/// Terminal status reported by a driven backend after an admitted write no
/// longer has any kernel or retry IO referencing its payload.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DispatchedCompletion {
    pub index: usize,
    pub failed: bool,
}

/// Result of polling a synchronous write source. Backends must not call
/// `wait_ready` while they still own in-flight writes: doing so could wait on
/// scheduler credit while the same thread is responsible for harvesting the
/// CQEs which release that credit.
pub enum WriteDispatchStatus<'a> {
    Ready(Vec<DispatchedWrite<'a>>),
    Pending,
    Complete,
}

/// Scoped producer used by admission wrappers to refill a caller-owned backend
/// as individual PD completions arrive. Implementations return borrowed writes,
/// but the backend method itself remains synchronous, so no `StripWrite` can
/// escape the caller stack.
pub trait WriteDispatch<'a> {
    fn poll_ready(&mut self, max_ops: usize) -> WriteDispatchStatus<'a>;

    fn wait_ready(&mut self, max_ops: usize) -> WriteDispatchStatus<'a>;

    fn writes_completed(&mut self, completions: &[DispatchedCompletion], service_ns: u64);
}

pub(crate) fn submit_dispatched_blocking<'a, B: IoBackend + ?Sized>(
    backend: &B,
    class: IoClass,
    total_ops: usize,
    dispatch: &mut dyn WriteDispatch<'a>,
) -> Vec<ChunkletResult<()>> {
    let mut output: Vec<Option<ChunkletResult<()>>> =
        std::iter::repeat_with(|| None).take(total_ops).collect();
    loop {
        match dispatch.wait_ready(usize::MAX) {
            WriteDispatchStatus::Ready(admitted) => {
                if admitted.is_empty() {
                    return output
                        .into_iter()
                        .map(|result| {
                            result.unwrap_or_else(|| {
                                Err(crate::error::ChunkletError::Invariant(
                                    "write dispatch returned an empty ready set".into(),
                                ))
                            })
                        })
                        .collect();
                }
                let writes: Vec<_> = admitted
                    .iter()
                    .map(|admitted| admitted.write.clone())
                    .collect();
                let service_started = std::time::Instant::now();
                let mut results = backend
                    .submit_writes_detailed_observed_with_class(
                        class,
                        &writes,
                        &IgnoreWriteCompletions,
                    )
                    .into_iter();
                let service_ns = service_started.elapsed().as_nanos().min(u64::MAX as u128) as u64;
                let mut completions = Vec::with_capacity(admitted.len());
                for admitted in admitted {
                    let result = results.next().unwrap_or_else(|| {
                        Err(crate::error::ChunkletError::Invariant(
                            "driven IO backend returned too few write results".into(),
                        ))
                    });
                    let failed = result.is_err();
                    if let Some(slot) = output.get_mut(admitted.index) {
                        *slot = Some(result);
                    }
                    completions.push(DispatchedCompletion {
                        index: admitted.index,
                        failed,
                    });
                }
                dispatch.writes_completed(&completions, service_ns);
            }
            WriteDispatchStatus::Pending => {
                return output
                    .into_iter()
                    .map(|result| {
                        result.unwrap_or_else(|| {
                            Err(crate::error::ChunkletError::Invariant(
                                "write dispatch wait returned without ready work".into(),
                            ))
                        })
                    })
                    .collect();
            }
            WriteDispatchStatus::Complete => break,
        }
    }
    output
        .into_iter()
        .map(|result| {
            result.unwrap_or_else(|| {
                Err(crate::error::ChunkletError::Invariant(
                    "driven IO backend omitted a write result".into(),
                ))
            })
        })
        .collect()
}

/// Cross-PD batched IO backend. Backends MUST block until every write in
/// `ops` is durable on its respective PD (or short-circuit on the first
/// error and report it).
///
/// Backends are stored as `Arc<dyn IoBackend>` on each `PhysicalDisk`;
/// see `PhysicalDisk::backend`.
pub trait IoBackend: Send + Sync {
    /// Register a pool member for scheduler observability. Plain backends do
    /// not retain per-PD state and therefore inherit this no-op.
    fn register_pd(&self, pd_id: PdId) {
        let _ = pd_id;
    }

    /// Forget a pool member after lifecycle code has made it unreachable to
    /// new IO. Stateful wrappers reject removal while the PD still owns queued
    /// or active work; plain backends inherit this no-op.
    fn unregister_pd(&self, pd_id: PdId) -> ChunkletResult<()> {
        let _ = pd_id;
        Ok(())
    }

    /// Scheduler metrics when this backend is an admission wrapper.
    fn scheduler_snapshot(&self) -> Option<SchedulerSnapshot> {
        None
    }

    /// Persistent write-execution metrics when this backend owns worker pools.
    fn execution_snapshot(&self) -> Option<IoExecutionSnapshot> {
        None
    }

    /// Issue every read in `ops`, blocking until all complete. Returns
    /// the first error seen.
    fn submit_reads(&self, ops: &mut [StripRead<'_>]) -> ChunkletResult<()>;

    /// Issue every write in `ops`, blocking until all complete, and return a
    /// **per-op** result aligned to `ops` order: `results[i]` is `Ok` iff
    /// `ops[i]` landed durably on its PD. Every op is issued and waited even
    /// when a sibling fails — so on return the surviving ops ARE durable, and
    /// the LD layer decides (per its redundancy budget) whether a subset of
    /// `Err`s is tolerable and can be absorbed into a degraded success (see
    /// [`crate::ld::degrade::absorb_degraded`]). This is the primitive that
    /// lets a RAID write ride through a single member's runtime EIO instead of
    /// discarding the surviving legs' success. Implementations should
    /// short-circuit on `len <= 1` to avoid backend-specific ceremony.
    fn submit_writes_detailed(&self, ops: &[StripWrite<'_>]) -> Vec<ChunkletResult<()>>;

    /// Class-aware detailed write entry point. Existing backends inherit the
    /// historical implementation; scheduling wrappers override this method.
    fn submit_writes_detailed_with_class(
        &self,
        class: IoClass,
        ops: &[StripWrite<'_>],
    ) -> Vec<ChunkletResult<()>> {
        with_io_class(class, || self.submit_writes_detailed(ops))
    }

    /// Optional streaming-completion entry point. The default keeps every
    /// existing backend compatible by notifying after its synchronous submit
    /// returns. CQE-aware backends can override this and notify earlier while
    /// retaining the ordered return-vector contract.
    fn submit_writes_detailed_observed_with_class(
        &self,
        class: IoClass,
        ops: &[StripWrite<'_>],
        observer: &dyn WriteCompletionObserver,
    ) -> Vec<ChunkletResult<()>> {
        let service_started = std::time::Instant::now();
        let results = self.submit_writes_detailed_with_class(class, ops);
        if !ops.is_empty() {
            let completed: Vec<_> = (0..ops.len()).collect();
            observer.writes_completed(
                &completed,
                service_started.elapsed().as_nanos().min(u64::MAX as u128) as u64,
            );
        }
        results
    }

    /// Completion-driven synchronous write entry point. The default adapter is
    /// intentionally conservative: it drains each ready set before requesting
    /// another. CQE-aware backends override this to keep independent PD lanes
    /// populated from one caller-local completion loop.
    fn submit_writes_dispatched_with_class<'a>(
        &self,
        class: IoClass,
        total_ops: usize,
        dispatch: &mut dyn WriteDispatch<'a>,
    ) -> Vec<ChunkletResult<()>> {
        submit_dispatched_blocking(self, class, total_ops, dispatch)
    }

    /// First-error convenience over [`Self::submit_writes_detailed`]: issue and
    /// wait on every op, then return the first `Err` (others dropped). Preserves
    /// the historical all-or-nothing contract for callers that do not perform
    /// inline-degrade accounting (reads path stays first-error via `submit_reads`).
    fn submit_writes(&self, ops: &[StripWrite<'_>]) -> ChunkletResult<()> {
        for r in self.submit_writes_detailed(ops) {
            r?;
        }
        Ok(())
    }

    /// Class-aware first-error convenience over
    /// [`Self::submit_writes_detailed_with_class`].
    fn submit_writes_with_class(
        &self,
        class: IoClass,
        ops: &[StripWrite<'_>],
    ) -> ChunkletResult<()> {
        for result in self.submit_writes_detailed_with_class(class, ops) {
            result?;
        }
        Ok(())
    }

    /// Flush every distinct PD in `pds`, blocking until all completions have
    /// been observed. Implementations may issue the device cache flushes in
    /// parallel; the default preserves compatibility for test backends.
    fn submit_flushes(&self, pds: &[Arc<PhysicalDisk>]) -> ChunkletResult<()> {
        let mut first_err = None;
        for pd in pds {
            if let Err(error) = pd.sync() {
                if first_err.is_none() {
                    first_err = Some(error);
                }
            }
        }
        first_err.map_or(Ok(()), Err)
    }

    /// Human-readable backend label for logs / metrics.
    fn name(&self) -> &'static str;
}

/// LD-side entry point for a durability barrier across distinct PDs. Every PD
/// in one LD belongs to the same pool and therefore shares one backend.
pub(crate) fn submit_pd_flushes(pds: &[Arc<PhysicalDisk>]) -> ChunkletResult<()> {
    if pds.is_empty() {
        return Ok(());
    }
    pds[0].backend().submit_flushes(pds)
}

/// Pool-wide backend selector. Stored on `PoolConfig` and resolved to a
/// concrete `Arc<dyn IoBackend>` when the pool boots.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IoBackendKind {
    /// `std::thread::scope` fan-out (always available).
    Sync,
    /// `io_uring` batched submit (Linux only — falls back to Sync on
    /// other platforms).
    Uring,
}

impl Default for IoBackendKind {
    fn default() -> Self {
        IoBackendKind::Sync
    }
}

/// LD-side entry point for batched writes. Picks the backend off the
/// first op's PD (every PD in a single batch belongs to the same pool
/// and shares one backend Arc) and forwards. Empty batches short-circuit
/// without touching the backend. Takes `Vec` by value so call sites can
/// move the assembled batch without an extra borrow dance.
pub fn submit_strip_writes(ops: Vec<StripWrite<'_>>) -> ChunkletResult<()> {
    if ops.is_empty() {
        return Ok(());
    }
    let backend = ops[0].pd.backend();
    backend.submit_writes_with_class(current_io_class(), &ops)
}

/// LD-side entry point for batched writes that returns a **per-op** result in
/// the caller's INPUT order. The LD maps `results[i]` back to its
/// `(segment, member)` by position, so the ordering is load-bearing. One batch
/// deliberately spans NUMA nodes: controllers on both PCIe roots should run in
/// parallel rather than being separated by `submit_and_wait` barriers.
///
/// Empty batches return `[]`. The returned Vec always has `ops.len()` entries.
/// Borrows `ops` so the caller can map results back to `(segment, member)` and
/// look up failed members for [`crate::ld::degrade::absorb_degraded`].
pub fn submit_strip_writes_detailed(ops: &[StripWrite<'_>]) -> Vec<ChunkletResult<()>> {
    let n = ops.len();
    if n == 0 {
        return Vec::new();
    }
    let backend = ops[0].pd.backend();
    backend.submit_writes_detailed_with_class(current_io_class(), ops)
}

/// LD-side entry point for batched reads. Picks the backend off the
/// first op's PD, matching `submit_strip_writes`.
pub fn submit_strip_reads(ops: &mut [StripRead<'_>]) -> ChunkletResult<()> {
    if ops.is_empty() {
        return Ok(());
    }
    let backend = ops[0].pd.backend();
    backend.submit_reads(ops)
}

/// Build the concrete backend that matches `kind`. On non-Linux, a
/// `Uring` request silently downgrades to `Sync` because `io_uring` only
/// links on Linux targets.
pub fn make_backend(kind: IoBackendKind) -> Arc<dyn IoBackend> {
    make_backend_with_uring_workers(kind, 0, 0)
}

/// Build a backend with optional persistent io_uring execution pools. Zero
/// workers preserve the historical caller-thread/TLS-ring behavior. On
/// non-Linux targets, or when io_uring/pool initialization fails, this retains
/// the existing graceful fallback to `SyncBackend`.
pub fn make_backend_with_uring_workers(
    kind: IoBackendKind,
    foreground_workers: usize,
    background_workers: usize,
) -> Arc<dyn IoBackend> {
    make_backend_with_uring_pool_config(
        kind,
        UringPoolConfig::new(foreground_workers, background_workers),
    )
}

/// Build a backend with explicit persistent worker counts and CPU affinity.
pub fn make_backend_with_uring_pool_config(
    kind: IoBackendKind,
    config: UringPoolConfig,
) -> Arc<dyn IoBackend> {
    match kind {
        IoBackendKind::Sync => Arc::new(crate::io::sync_backend::SyncBackend),
        #[cfg(target_os = "linux")]
        IoBackendKind::Uring => {
            match crate::io::uring_backend::UringBackend::new_pooled_with_config(config) {
                Ok(b) => Arc::new(b),
                Err(e) => {
                    tracing::warn!("io_uring init failed ({}); falling back to SyncBackend", e);
                    Arc::new(crate::io::sync_backend::SyncBackend)
                }
            }
        }
        #[cfg(not(target_os = "linux"))]
        IoBackendKind::Uring => {
            let _ = config;
            Arc::new(crate::io::sync_backend::SyncBackend)
        }
    }
}

/// Compose per-PD admission with optional persistent execution workers.
///
/// The wrapper order is deliberately `execution -> scheduler -> device`.
/// Each execution worker dequeues one PD-homogeneous group before asking the
/// scheduler for credit, so worker queue wait is not counted as active device
/// work and a blocked PD does not create an all-or-none multi-PD admission.
pub fn make_scheduled_backend_with_uring_pool_config(
    kind: IoBackendKind,
    scheduler: SchedulerConfig,
    uring: UringPoolConfig,
) -> ChunkletResult<Arc<dyn IoBackend>> {
    let workers_enabled = uring.foreground_workers > 0 || uring.background_workers > 0;
    let mut inline_config = uring.clone();
    inline_config.foreground_workers = 0;
    inline_config.background_workers = 0;
    let inner: Arc<dyn IoBackend> = if kind == IoBackendKind::Uring && workers_enabled {
        #[cfg(target_os = "linux")]
        {
            Arc::new(crate::io::uring_backend::UringBackend::new_pooled_with_config(inline_config)?)
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = inline_config;
            return Err(crate::error::ChunkletError::Unsupported(
                "persistent io_uring workers require Linux".into(),
            ));
        }
    } else {
        make_backend_with_uring_pool_config(kind, inline_config)
    };
    let scheduled: Arc<dyn IoBackend> = Arc::new(ScheduledBackend::new(inner, scheduler)?);

    if kind != IoBackendKind::Uring || !workers_enabled {
        return Ok(scheduled);
    }

    #[cfg(target_os = "linux")]
    {
        let backend = crate::io::uring_backend::ExecutionPoolBackend::new(scheduled, uring)?;
        return Ok(Arc::new(backend));
    }

    #[cfg(not(target_os = "linux"))]
    {
        let _ = uring;
        Ok(scheduled)
    }
}
