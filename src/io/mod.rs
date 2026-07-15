//! O_DIRECT IO primitives + cross-PD batched-write backends.
//!
//! `AlignedBuf` provides 4 KiB-aligned page memory; `RawDevice` wraps a
//! block device or sparse file with a complete `read_at` / `write_at`
//! loop and falls back to buffered IO when O_DIRECT is unsupported
//! (typical for tests on tmpfs / overlayfs).
//!
//! `IoBackend` is the trait that fan-out write paths (R5/R6 stripe
//! writes) submit through. Two implementations: `SyncBackend` (always
//! available, `std::thread::scope` fan-out) and `UringBackend` (Linux
//! only, batched `io_uring` submit). Selected per-pool via
//! `PoolConfig::io_backend`.

pub mod aligned;
pub mod backend;
pub mod raw;
pub mod scheduler;
pub mod sync_backend;

#[cfg(target_os = "linux")]
pub mod uring_backend;

pub use aligned::{round_up, AlignedBuf};
pub use backend::{
    make_backend, make_backend_with_uring_pool_config, make_backend_with_uring_workers,
    make_scheduled_backend_with_uring_pool_config, IoBackend, IoBackendKind,
    IoExecutionClassSnapshot, IoExecutionSnapshot, StripRead, StripWrite, UringPoolConfig,
    WriteCompletionObserver,
};
pub use raw::RawDevice;
pub use scheduler::{
    current_io_class, with_io_class, IoClass, IoClassSnapshot, PdSchedulerSnapshot,
    ScheduledBackend, SchedulerConfig, SchedulerSnapshot,
};
