//! O_DIRECT IO primitives.
//!
//! `AlignedBuf` provides 4 KiB-aligned page memory; `RawDevice` wraps a block
//! device or sparse file with a complete `read_at` / `write_at` loop and falls
//! back to buffered IO when O_DIRECT is unsupported (typical for tests on
//! tmpfs / overlayfs).

pub mod aligned;
pub mod raw;

pub use aligned::{round_up, AlignedBuf};
pub use raw::RawDevice;
