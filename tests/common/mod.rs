//! Shared scaffolding for integration tests that need a Pool over sparse
//! files plus the ability to simulate PD failures by reopening the pool
//! with a subset of devices. Lives at `tests/common/mod.rs` so cargo
//! treats it as a private module rather than its own integration binary.

#![allow(dead_code)]

pub mod fault;

use std::path::PathBuf;
use std::sync::Arc;

use onyx_chunklet::io::{IoBackendKind, RawDevice};
use onyx_chunklet::{Pool, PoolConfig};
use tempfile::TempDir;

const PD_SIZE: u64 = 4 * 1024 * 1024 * 1024;

/// Create a fresh pool of `n` sparse-backed PDs (default Sync backend).
/// Returns the pool plus the device paths so callers can drop members
/// later via `open_subset`.
pub fn make_pool(dir: &TempDir, n: usize) -> (Arc<Pool>, Vec<PathBuf>) {
    make_pool_with(dir, n, IoBackendKind::Sync)
}

/// Same as `make_pool` but with an explicit IO backend choice.
pub fn make_pool_with(
    dir: &TempDir,
    n: usize,
    backend: IoBackendKind,
) -> (Arc<Pool>, Vec<PathBuf>) {
    let mut raws = Vec::new();
    let mut paths = Vec::new();
    for i in 0..n {
        let p = dir.path().join(format!("pd{}", i));
        raws.push(RawDevice::open_or_create(&p, PD_SIZE).unwrap());
        paths.push(p);
    }
    let pool = Pool::create(
        raws,
        PoolConfig {
            spare_pct: 0,
            io_backend: backend,
        },
    )
    .unwrap();
    (pool, paths)
}

/// Deterministic per-byte splitmix pattern. Seeded by `tag`, addressed
/// by absolute byte offset, so writes done in arbitrary chunks and reads
/// at arbitrary offsets produce matching bytes.
pub fn pattern(tag: u64, len: usize, base: u64) -> Vec<u8> {
    (0..len)
        .map(|i| {
            let mut x = tag.wrapping_add((base + i as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15));
            x ^= x >> 33;
            x = x.wrapping_mul(0xff51_afd7_ed55_8ccd);
            x ^= x >> 33;
            (x >> 56) as u8
        })
        .collect()
}

/// Reopen the pool excluding the PDs at the given path indices. Quorum
/// (`floor(N/2)+1`) must still hold across the surviving PDs.
pub fn open_subset(paths: &[PathBuf], drop_idx: &[usize]) -> Arc<Pool> {
    let raws: Vec<_> = paths
        .iter()
        .enumerate()
        .filter(|(i, _)| !drop_idx.contains(i))
        .map(|(_, p)| RawDevice::open(p).unwrap())
        .collect();
    Pool::open_with_missing(raws).unwrap()
}

/// Reopen the pool with every PD present.
pub fn open_full(paths: &[PathBuf]) -> Arc<Pool> {
    let raws: Vec<_> = paths.iter().map(|p| RawDevice::open(p).unwrap()).collect();
    Pool::open(raws).unwrap()
}

/// Locate which path index holds the LD member at `member_idx` (assumes a
/// single LD in the pool). Lets tests target a specific failure (e.g. the
/// parity slot, or data position 1) without caring how the allocator laid
/// chunklets out across PDs.
pub fn path_for_member(pool: &Pool, paths: &[PathBuf], member_idx: usize) -> usize {
    let desc = pool.list_lds().into_iter().next().unwrap();
    let pd_id = desc.members[member_idx].pd;
    paths
        .iter()
        .position(|p| pool.pd(pd_id).unwrap().path() == p)
        .unwrap()
}
