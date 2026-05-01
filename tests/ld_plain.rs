//! LdPlain end-to-end integration tests.

use std::path::PathBuf;
use std::sync::Arc;

use onyx_chunklet::io::RawDevice;
use onyx_chunklet::ld::LogicalDisk;
use onyx_chunklet::pool::LdSpec;
use onyx_chunklet::types::{ChunkletState, RaidLevel, BLOCK_SIZE, CHUNKLET_HEADER_BYTES, CHUNKLET_SIZE};
use onyx_chunklet::{HaDomain, Pool, PoolConfig};
use tempfile::TempDir;

/// 4 GiB sparse files: ~2 chunklets each (4 GiB - 2 MiB reserved / 1 GiB ≈ 3, but
/// rounding gives us 2-3 depending on exact size). We use 5 PDs so any plain LD
/// can have up to ~10 chunklets.
const PD_SIZE: u64 = 4 * 1024 * 1024 * 1024;

fn make_pool(dir: &TempDir, names: &[&str]) -> (Arc<Pool>, Vec<PathBuf>) {
    let mut raws = Vec::new();
    let mut paths = Vec::new();
    for n in names {
        let p = dir.path().join(n);
        raws.push(RawDevice::open_or_create(&p, PD_SIZE).unwrap());
        paths.push(p);
    }
    let pool = Pool::create(raws, PoolConfig { spare_pct: 0 }).unwrap();
    (pool, paths)
}

fn open_pool(paths: &[PathBuf]) -> Arc<Pool> {
    let raws: Vec<_> = paths.iter().map(|p| RawDevice::open(p).unwrap()).collect();
    Pool::open(raws).unwrap()
}

#[test]
fn create_open_read_write_round_trip() {
    let dir = TempDir::new().unwrap();
    let (pool, _paths) = make_pool(&dir, &["pd0", "pd1", "pd2"]);

    let ld_id = pool.create_ld(LdSpec::plain(2)).unwrap();
    let ld = pool.open_ld(ld_id).unwrap();
    assert_eq!(ld.id(), ld_id);
    assert!(ld.capacity_bytes() >= 2 * (CHUNKLET_SIZE - CHUNKLET_HEADER_BYTES));
    assert_eq!(ld.block_size(), BLOCK_SIZE as usize);

    // Write & read 1 MiB at offset 0.
    let payload: Vec<u8> = (0..(1 << 20)).map(|i| ((i * 31 + 7) % 251) as u8).collect();
    ld.write_at(0, &payload).unwrap();
    let mut readback = vec![0u8; payload.len()];
    ld.read_at(0, &mut readback).unwrap();
    assert_eq!(readback, payload);
}

#[test]
fn cross_chunklet_io_splits_correctly() {
    // Write at an offset that straddles a chunklet boundary.
    let dir = TempDir::new().unwrap();
    let (pool, _paths) = make_pool(&dir, &["pd0", "pd1"]);
    let ld_id = pool.create_ld(LdSpec::plain(2)).unwrap();
    let ld = pool.open_ld(ld_id).unwrap();

    let chunklet_user = CHUNKLET_SIZE - CHUNKLET_HEADER_BYTES;
    // Start 4 KiB before a chunklet boundary, write 8 KiB. The write spans
    // the last 4 KiB of chunklet 0 and the first 4 KiB of chunklet 1.
    let offset = chunklet_user - BLOCK_SIZE;
    let payload: Vec<u8> = (0..(2 * BLOCK_SIZE as usize))
        .map(|i| ((i * 17 + 3) % 199) as u8)
        .collect();
    ld.write_at(offset, &payload).unwrap();
    let mut readback = vec![0u8; payload.len()];
    ld.read_at(offset, &mut readback).unwrap();
    assert_eq!(readback, payload);
}

#[test]
fn ld_persists_across_pool_reopen() {
    let dir = TempDir::new().unwrap();
    let (pool, paths) = make_pool(&dir, &["pd0", "pd1", "pd2"]);
    let ld_id = pool.create_ld(LdSpec::plain(3)).unwrap();
    let ld = pool.open_ld(ld_id).unwrap();
    let payload: Vec<u8> = (0..(64 << 10)).map(|i| (i % 211) as u8).collect();
    ld.write_at(0, &payload).unwrap();
    drop((ld, pool));

    let pool2 = open_pool(&paths);
    let lds = pool2.list_lds();
    assert_eq!(lds.len(), 1);
    assert_eq!(lds[0].id, ld_id);
    let ld2 = pool2.open_ld(ld_id).unwrap();
    let mut readback = vec![0u8; payload.len()];
    ld2.read_at(0, &mut readback).unwrap();
    assert_eq!(readback, payload);
}

#[test]
fn drop_ld_frees_chunklets() {
    let dir = TempDir::new().unwrap();
    let (pool, _paths) = make_pool(&dir, &["pd0", "pd1", "pd2"]);
    let ld_id = pool.create_ld(LdSpec::plain(3)).unwrap();
    // Each PD should have 1 chunklet marked Used.
    for info in pool.list_pds() {
        let pd = pool.pd(info.pd_id).unwrap();
        let (_, bitmap, _) = pd.snapshot();
        assert_eq!(
            bitmap.count(ChunkletState::Used),
            1,
            "PD {} should own 1 chunklet for the LD",
            info.pd_id
        );
    }
    pool.drop_ld(ld_id).unwrap();
    assert!(pool.find_ld(ld_id).is_none());
    for info in pool.list_pds() {
        let pd = pool.pd(info.pd_id).unwrap();
        let (_, bitmap, _) = pd.snapshot();
        assert_eq!(bitmap.count(ChunkletState::Used), 0);
    }
}

#[test]
fn rejects_non_plain_levels() {
    let dir = TempDir::new().unwrap();
    let (pool, _paths) = make_pool(&dir, &["pd0", "pd1", "pd2"]);
    let spec = LdSpec {
        raid_level: RaidLevel::Mirror,
        set_size: 2,
        row_size: 1,
        num_rows: 1,
        strip_size_log2: 0,
        ha_domain: HaDomain::Pd,
    };
    assert!(pool.create_ld(spec).is_err());
}

#[test]
fn rejects_when_pool_too_small() {
    let dir = TempDir::new().unwrap();
    // 2 PDs at 4 GiB each = ~6 chunklets total. Asking for 100 should fail.
    let (pool, _paths) = make_pool(&dir, &["pd0", "pd1"]);
    let err = pool.create_ld(LdSpec::plain(100)).err().unwrap();
    let s = format!("{}", err);
    assert!(s.contains("free"), "expected NoSpace-like error, got: {}", s);
}

#[test]
fn create_two_lds_and_observe_separate_chunklets() {
    let dir = TempDir::new().unwrap();
    let (pool, _paths) = make_pool(&dir, &["pd0", "pd1", "pd2", "pd3"]);
    let ld1 = pool.create_ld(LdSpec::plain(2)).unwrap();
    let ld2 = pool.create_ld(LdSpec::plain(2)).unwrap();
    assert_ne!(ld1, ld2);
    let lds = pool.list_lds();
    assert_eq!(lds.len(), 2);

    let h1 = pool.open_ld(ld1).unwrap();
    let h2 = pool.open_ld(ld2).unwrap();
    let pat1: Vec<u8> = std::iter::repeat(0xaa).take(64 * 1024).collect();
    let pat2: Vec<u8> = std::iter::repeat(0x55).take(64 * 1024).collect();
    h1.write_at(0, &pat1).unwrap();
    h2.write_at(0, &pat2).unwrap();

    let mut buf1 = vec![0u8; pat1.len()];
    let mut buf2 = vec![0u8; pat2.len()];
    h1.read_at(0, &mut buf1).unwrap();
    h2.read_at(0, &mut buf2).unwrap();
    assert_eq!(buf1, pat1);
    assert_eq!(buf2, pat2);
}
