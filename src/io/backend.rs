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
use crate::pd::PhysicalDisk;

/// One PD-level write prepared by an LD. The lifetime ties `data` to
/// whatever buffer the caller owns; backends are required to issue + wait
/// for the IO before returning, so the borrow only needs to outlive a
/// single `submit_writes` call.
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

/// Cross-PD batched IO backend. Backends MUST block until every write in
/// `ops` is durable on its respective PD (or short-circuit on the first
/// error and report it).
///
/// Backends are stored as `Arc<dyn IoBackend>` on each `PhysicalDisk`;
/// see `PhysicalDisk::backend`.
pub trait IoBackend: Send + Sync {
    /// Issue every read in `ops`, blocking until all complete. Returns
    /// the first error seen.
    fn submit_reads(&self, ops: &mut [StripRead<'_>]) -> ChunkletResult<()>;

    /// Issue every write in `ops`, blocking until all complete. Returns
    /// the first error seen (others are dropped). Implementations should
    /// short-circuit on `len <= 1` to avoid backend-specific ceremony.
    fn submit_writes(&self, ops: &[StripWrite<'_>]) -> ChunkletResult<()>;

    /// Human-readable backend label for logs / metrics.
    fn name(&self) -> &'static str;
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
    backend.submit_writes(&ops)
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
    match kind {
        IoBackendKind::Sync => Arc::new(crate::io::sync_backend::SyncBackend),
        #[cfg(target_os = "linux")]
        IoBackendKind::Uring => match crate::io::uring_backend::UringBackend::new() {
            Ok(b) => Arc::new(b),
            Err(e) => {
                tracing::warn!("io_uring init failed ({}); falling back to SyncBackend", e);
                Arc::new(crate::io::sync_backend::SyncBackend)
            }
        },
        #[cfg(not(target_os = "linux"))]
        IoBackendKind::Uring => Arc::new(crate::io::sync_backend::SyncBackend),
    }
}
