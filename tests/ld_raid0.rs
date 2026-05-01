//! LdRaid0 integration tests: striping correctness, capacity, and that the
//! IO actually fans out across PDs (vs LdPlain which keeps it on one PD).

use std::path::PathBuf;
use std::sync::Arc;

use onyx_chunklet::io::RawDevice;
use onyx_chunklet::ld::LogicalDisk;
use onyx_chunklet::pool::LdSpec;
use onyx_chunklet::types::{ChunkletState, BLOCK_SIZE, CHUNKLET_HEADER_BYTES, CHUNKLET_SIZE};
use onyx_chunklet::{Pool, PoolConfig};
use tempfile::TempDir;

const PD_SIZE: u64 = 4 * 1024 * 1024 * 1024;

fn make_pool(dir: &TempDir, n_pds: usize) -> (Arc<Pool>, Vec<PathBuf>) {
    let mut raws = Vec::new();
    let mut paths = Vec::new();
    for i in 0..n_pds {
        let p = dir.path().join(format!("pd{}", i));
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
fn raid0_round_trip_2_chunklets() {
    let dir = TempDir::new().unwrap();
    let (pool, _) = make_pool(&dir, 2);
    // 2-chunklet stripe.
    let id = pool.create_ld(LdSpec::raid0(2, 1, 0)).unwrap();
    let ld = pool.open_ld(id).unwrap();
    assert_eq!(ld.capacity_bytes(), 2 * (CHUNKLET_SIZE - CHUNKLET_HEADER_BYTES));

    let payload: Vec<u8> = (0..(64 << 10)).map(|i| ((i * 19 + 5) % 251) as u8).collect();
    ld.write_at(0, &payload).unwrap();
    let mut readback = vec![0u8; payload.len()];
    ld.read_at(0, &mut readback).unwrap();
    assert_eq!(readback, payload);
}

#[test]
fn raid0_stripe_actually_distributes_blocks() {
    // Verify that consecutive 4 KiB blocks land on different PDs (the
    // defining property of RAID-0 vs LdPlain's concat).
    let dir = TempDir::new().unwrap();
    let (pool, _) = make_pool(&dir, 3);
    let id = pool.create_ld(LdSpec::raid0(3, 1, 0)).unwrap();
    let ld = pool.open_ld(id).unwrap();

    // Write 3 distinct strips so we can identify them on disk.
    let strip = BLOCK_SIZE as usize;
    let mut payload = vec![0u8; 3 * strip];
    payload[0..strip].fill(0xa1);
    payload[strip..2 * strip].fill(0xb2);
    payload[2 * strip..3 * strip].fill(0xc3);
    ld.write_at(0, &payload).unwrap();

    // Each member's chunklet at in_chunklet_off=0 should hold exactly one of
    // the three patterns. Read each member directly and assert.
    let desc = pool.find_ld(id).unwrap();
    assert_eq!(desc.members.len(), 3);
    let expected_patterns = [0xa1u8, 0xb2u8, 0xc3u8];
    for (i, m) in desc.members.iter().enumerate() {
        let pd = pool.pd(m.pd).unwrap();
        let mut buf = vec![0u8; strip];
        pd.read_chunklet_user(m.chunklet_index, 0, &mut buf).unwrap();
        assert!(
            buf.iter().all(|&b| b == expected_patterns[i]),
            "member {}: expected all {:02x}, got first byte {:02x}",
            i,
            expected_patterns[i],
            buf[0]
        );
    }
}

#[test]
fn raid0_persists_across_reopen() {
    let dir = TempDir::new().unwrap();
    let (pool, paths) = make_pool(&dir, 4);
    let id = pool.create_ld(LdSpec::raid0(4, 1, 0)).unwrap();
    let ld = pool.open_ld(id).unwrap();
    let payload: Vec<u8> = (0..(128 << 10)).map(|i| (i % 211) as u8).collect();
    ld.write_at(0, &payload).unwrap();
    drop((ld, pool));

    let pool2 = open_pool(&paths);
    let ld2 = pool2.open_ld(id).unwrap();
    let mut readback = vec![0u8; payload.len()];
    ld2.read_at(0, &mut readback).unwrap();
    assert_eq!(readback, payload);
}

#[test]
fn raid0_drop_frees_chunklets() {
    let dir = TempDir::new().unwrap();
    let (pool, _) = make_pool(&dir, 4);
    let id = pool.create_ld(LdSpec::raid0(4, 1, 0)).unwrap();
    pool.drop_ld(id).unwrap();
    for info in pool.list_pds() {
        let pd = pool.pd(info.pd_id).unwrap();
        let (_, bm, _) = pd.snapshot();
        assert_eq!(bm.count(ChunkletState::Used), 0);
    }
}

#[test]
fn raid0_rejects_stripe_width_one() {
    // RAID-0 with stripe_width=1 is just LdPlain — reject so the user picks
    // the right primitive.
    let dir = TempDir::new().unwrap();
    let (pool, _) = make_pool(&dir, 2);
    let err = pool.create_ld(LdSpec::raid0(1, 1, 0)).err().unwrap();
    let s = format!("{}", err);
    assert!(s.contains("row_size") || s.contains("Plain"), "{}", s);
}

#[test]
fn raid0_strip_size_log2_works_for_larger_strips() {
    // 64 KiB strip across 2 PDs.
    let dir = TempDir::new().unwrap();
    let (pool, _) = make_pool(&dir, 2);
    let id = pool.create_ld(LdSpec::raid0(2, 1, 16)).unwrap(); // 1 << 16 = 64 KiB
    let ld = pool.open_ld(id).unwrap();
    assert_eq!(ld.strip_size(), 64 * 1024);

    let payload: Vec<u8> = (0..(192 << 10)).map(|i| (i % 191) as u8).collect();
    ld.write_at(0, &payload).unwrap();
    let mut readback = vec![0u8; payload.len()];
    ld.read_at(0, &mut readback).unwrap();
    assert_eq!(readback, payload);
}
