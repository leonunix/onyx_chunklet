//! `LogicalDisk::flush` durability-barrier tests across every RAID level.
//!
//! `flush` fans `PhysicalDisk::sync()` out over an LD's distinct member PDs.
//! These tests assert it (a) returns `Ok` for every LD variant — proving the
//! trait method is wired into all six `impl LogicalDisk` sites — and (b) is a
//! real persistence barrier: bytes written then flushed survive a pool reopen.

use std::path::PathBuf;
use std::sync::Arc;

use onyx_chunklet::io::RawDevice;
use onyx_chunklet::pool::LdSpec;
use onyx_chunklet::{Pool, PoolConfig};
use tempfile::TempDir;

const PD_SIZE: u64 = 4 * 1024 * 1024 * 1024;

fn make_pool(dir: &TempDir, n_pds: usize) -> (Arc<Pool>, Vec<PathBuf>) {
    let mut raws = Vec::new();
    let mut paths = Vec::new();
    for i in 0..n_pds {
        let p = dir.path().join(format!("pd{i}"));
        raws.push(RawDevice::open_or_create(&p, PD_SIZE).unwrap());
        paths.push(p);
    }
    let pool = Pool::create(
        raws,
        PoolConfig {
            spare_pct: 0,
            ..Default::default()
        },
    )
    .unwrap();
    (pool, paths)
}

fn open_pool(paths: &[PathBuf]) -> Arc<Pool> {
    let raws: Vec<_> = paths.iter().map(|p| RawDevice::open(p).unwrap()).collect();
    Pool::open(raws).unwrap()
}

/// Write `payload` at offset 0, `flush()`, then drop and reopen the pool and
/// confirm the bytes are intact. Exercises one LD variant end to end.
fn flush_roundtrip(label: &str, n_pds: usize, spec: LdSpec) {
    let dir = TempDir::new().unwrap();
    let (pool, paths) = make_pool(&dir, n_pds);
    let ld_id = pool.create_ld(spec).unwrap();
    let ld = pool.open_ld(ld_id).unwrap();

    // 256 KiB spans several strips on the striped/parity variants.
    let payload: Vec<u8> = (0..(256usize << 10))
        .map(|i| ((i * 131 + 17) % 251) as u8)
        .collect();
    ld.write_at(0, &payload).unwrap();
    ld.flush()
        .unwrap_or_else(|e| panic!("{label}: flush failed: {e}"));

    drop((ld, pool));
    let pool2 = open_pool(&paths);
    let ld2 = pool2.open_ld(ld_id).unwrap();
    let mut readback = vec![0u8; payload.len()];
    ld2.read_at(0, &mut readback).unwrap();
    assert_eq!(readback, payload, "{label}: data lost across flush + reopen");
}

#[test]
fn flush_plain() {
    flush_roundtrip("plain", 3, LdSpec::plain(3));
}

#[test]
fn flush_raid0() {
    flush_roundtrip("raid0", 3, LdSpec::raid0(3, 1, 0));
}

#[test]
fn flush_raid1() {
    flush_roundtrip("raid1", 2, LdSpec::mirror(2, 1, 1, 0));
}

#[test]
fn flush_raid10() {
    // row_size = 2 → two mirror columns striped → RAID-10 over 4 PDs.
    flush_roundtrip("raid10", 4, LdSpec::mirror(2, 2, 1, 0));
}

#[test]
fn flush_raid5() {
    flush_roundtrip("raid5", 4, LdSpec::raid5(3, 1, 1, 0));
}

#[test]
fn flush_raid6() {
    flush_roundtrip("raid6", 5, LdSpec::raid6(3, 1, 1, 0));
}

/// `flush` on an LD whose chunklets share a backing PD must sync that PD once,
/// not error — a plain LD with more chunklets than PDs forces the dedup path.
#[test]
fn flush_dedups_shared_pd() {
    let dir = TempDir::new().unwrap();
    // 2 PDs but a 4-chunklet plain LD → some PD owns 2 of the LD's chunklets.
    let (pool, _paths) = make_pool(&dir, 2);
    let ld_id = pool.create_ld(LdSpec::plain(4)).unwrap();
    let ld = pool.open_ld(ld_id).unwrap();
    let payload = vec![0xa5u8; 64 << 10];
    ld.write_at(0, &payload).unwrap();
    ld.flush().unwrap();
    let mut readback = vec![0u8; payload.len()];
    ld.read_at(0, &mut readback).unwrap();
    assert_eq!(readback, payload);
}
