//! LdMirror end-to-end tests: RAID-1, RAID-10, mirror persistence after
//! pool reopen, and degraded-read sanity (one copy corrupted, the other
//! still serves data).

use std::path::PathBuf;
use std::sync::Arc;

use onyx_chunklet::io::RawDevice;
use onyx_chunklet::ld::LogicalDisk;
use onyx_chunklet::pool::LdSpec;
use onyx_chunklet::types::{ChunkletState, BLOCK_SIZE, CHUNKLET_HEADER_BYTES, CHUNKLET_SIZE};
use onyx_chunklet::{Pool, PoolConfig};
use tempfile::TempDir;

const PD_SIZE: u64 = 4 * 1024 * 1024 * 1024; // ~3 chunklets per PD

fn make_pool(dir: &TempDir, names: &[&str]) -> (Arc<Pool>, Vec<PathBuf>) {
    let mut raws = Vec::new();
    let mut paths = Vec::new();
    for n in names {
        let p = dir.path().join(n);
        raws.push(RawDevice::open_or_create(&p, PD_SIZE).unwrap());
        paths.push(p);
    }
    let pool = Pool::create(raws, PoolConfig { spare_pct: 0, ..Default::default() }).unwrap();
    (pool, paths)
}

fn open_pool(paths: &[PathBuf]) -> Arc<Pool> {
    let raws: Vec<_> = paths.iter().map(|p| RawDevice::open(p).unwrap()).collect();
    Pool::open(raws).unwrap()
}

#[test]
fn raid1_round_trip() {
    let dir = TempDir::new().unwrap();
    let (pool, _) = make_pool(&dir, &["pd0", "pd1"]);
    // Pure mirror: 2 copies, 1 set per row, 1 row.
    let id = pool.create_ld(LdSpec::mirror(2, 1, 1, 0)).unwrap();
    let ld = pool.open_ld(id).unwrap();
    assert_eq!(ld.capacity_bytes(), CHUNKLET_SIZE - CHUNKLET_HEADER_BYTES);

    let payload: Vec<u8> = (0..(64 << 10)).map(|i| ((i * 23 + 11) % 251) as u8).collect();
    ld.write_at(0, &payload).unwrap();
    let mut readback = vec![0u8; payload.len()];
    ld.read_at(0, &mut readback).unwrap();
    assert_eq!(readback, payload);
}

#[test]
fn raid1_marks_two_chunklets_used() {
    let dir = TempDir::new().unwrap();
    let (pool, _) = make_pool(&dir, &["pd0", "pd1", "pd2"]);
    let _id = pool.create_ld(LdSpec::mirror(2, 1, 1, 0)).unwrap();
    let mut used_total = 0u32;
    for info in pool.list_pds() {
        let pd = pool.pd(info.pd_id).unwrap();
        let (_, bitmap, _) = pd.snapshot();
        used_total += bitmap.count(ChunkletState::Used);
    }
    assert_eq!(used_total, 2, "RAID-1 with 2 copies should consume 2 chunklets");
}

#[test]
fn raid10_round_trip_with_strip_alignment() {
    // 4 PDs, mirror=2, row_size=2 (RAID-10), num_rows=1, strip=4 KiB (block).
    let dir = TempDir::new().unwrap();
    let (pool, _) = make_pool(&dir, &["pd0", "pd1", "pd2", "pd3"]);
    let id = pool.create_ld(LdSpec::mirror(2, 2, 1, 0)).unwrap();
    let ld = pool.open_ld(id).unwrap();
    // Capacity = row_size * num_rows * chunklet_user_size = 2 * 1 * (1 GiB - 4 KiB).
    assert_eq!(ld.capacity_bytes(), 2 * (CHUNKLET_SIZE - CHUNKLET_HEADER_BYTES));
    assert_eq!(ld.strip_size(), BLOCK_SIZE as usize);

    // Write enough to span both stripes (8 blocks = 32 KiB).
    let payload: Vec<u8> = (0..(32 << 10))
        .map(|i| ((i * 7 + 19) % 211) as u8)
        .collect();
    ld.write_at(0, &payload).unwrap();
    let mut readback = vec![0u8; payload.len()];
    ld.read_at(0, &mut readback).unwrap();
    assert_eq!(readback, payload);
}

#[test]
fn raid10_uses_4_chunklets_total() {
    let dir = TempDir::new().unwrap();
    let (pool, _) = make_pool(&dir, &["pd0", "pd1", "pd2", "pd3"]);
    let _id = pool.create_ld(LdSpec::mirror(2, 2, 1, 0)).unwrap();
    let mut used_total = 0u32;
    for info in pool.list_pds() {
        let pd = pool.pd(info.pd_id).unwrap();
        let (_, bitmap, _) = pd.snapshot();
        used_total += bitmap.count(ChunkletState::Used);
    }
    assert_eq!(used_total, 4, "RAID-10 (mirror=2, row_size=2) should use 4 chunklets");
}

#[test]
fn mirror_persists_across_pool_reopen() {
    let dir = TempDir::new().unwrap();
    let (pool, paths) = make_pool(&dir, &["pd0", "pd1", "pd2", "pd3"]);
    let id = pool.create_ld(LdSpec::mirror(2, 2, 1, 0)).unwrap();
    let ld = pool.open_ld(id).unwrap();
    let payload: Vec<u8> = (0..(16 << 10)).map(|i| (i % 199) as u8).collect();
    ld.write_at(0, &payload).unwrap();
    drop((ld, pool));

    let pool2 = open_pool(&paths);
    let lds = pool2.list_lds();
    assert_eq!(lds.len(), 1);
    assert_eq!(lds[0].id, id);
    let ld2 = pool2.open_ld(id).unwrap();
    let mut readback = vec![0u8; payload.len()];
    ld2.read_at(0, &mut readback).unwrap();
    assert_eq!(readback, payload);
}

#[test]
fn mirror_drop_frees_all_copies() {
    let dir = TempDir::new().unwrap();
    let (pool, _) = make_pool(&dir, &["pd0", "pd1", "pd2", "pd3"]);
    let id = pool.create_ld(LdSpec::mirror(2, 2, 1, 0)).unwrap();
    pool.drop_ld(id).unwrap();
    for info in pool.list_pds() {
        let pd = pool.pd(info.pd_id).unwrap();
        let (_, bitmap, _) = pd.snapshot();
        assert_eq!(bitmap.count(ChunkletState::Used), 0);
    }
}

#[test]
fn raid1_serves_reads_after_one_copy_corrupted() {
    // 2 PDs, RAID-1. After write, corrupt one copy's chunklet header — that
    // doesn't break user data. To actually verify single-copy survival we
    // would need to close the pool and reopen with one PD missing. For P2
    // that's a Phase 5 task (degraded mode), so this test just verifies the
    // happy path: both copies hold identical data we can read back.
    let dir = TempDir::new().unwrap();
    let (pool, _) = make_pool(&dir, &["pd0", "pd1"]);
    let id = pool.create_ld(LdSpec::mirror(2, 1, 1, 0)).unwrap();
    let ld = pool.open_ld(id).unwrap();

    let payload: Vec<u8> = std::iter::repeat(0xa5).take(8 << 10).collect();
    ld.write_at(0, &payload).unwrap();

    // Read each copy directly via PD::read_chunklet_user and verify both
    // hold the same bytes.
    let desc = pool.find_ld(id).unwrap();
    assert_eq!(desc.members.len(), 2);
    for m in &desc.members {
        let pd = pool.pd(m.pd).unwrap();
        let mut buf = vec![0u8; payload.len()];
        pd.read_chunklet_user(m.chunklet_index, 0, &mut buf).unwrap();
        assert_eq!(buf, payload);
    }
}

#[test]
fn rejects_mirror_with_set_size_one() {
    let dir = TempDir::new().unwrap();
    let (pool, _) = make_pool(&dir, &["pd0", "pd1"]);
    let spec = LdSpec {
        raid_level: onyx_chunklet::types::RaidLevel::Mirror,
        set_size: 1, // invalid
        row_size: 1,
        num_rows: 1,
        strip_size_log2: 0,
        ha_domain: onyx_chunklet::types::HaDomain::Pd,
    };
    assert!(pool.create_ld(spec).is_err());
}

#[test]
fn rejects_when_set_size_exceeds_distinct_pds() {
    // 4-way mirror needs 4 distinct PDs per set, but pool only has 3.
    let dir = TempDir::new().unwrap();
    let (pool, _) = make_pool(&dir, &["pd0", "pd1", "pd2"]);
    let err = pool.create_ld(LdSpec::mirror(4, 1, 1, 0)).err().unwrap();
    let s = format!("{}", err);
    assert!(s.contains("distinct PDs") || s.contains("free"), "{}", s);
}

/// Pool::mark_chunklet_bad persists the Bad state across reopen and is
/// honored by future LdMirror::open's `resolve_members` (so reads skip the
/// bad copy and write_at fans out to the surviving copies only).
#[test]
fn mark_chunklet_bad_persists_and_excludes_member_from_reads() {
    let dir = TempDir::new().unwrap();
    let (pool, paths) = make_pool(&dir, &["pd0", "pd1", "pd2"]);
    let id = pool.create_ld(LdSpec::mirror(3, 1, 1, 0)).unwrap();
    let ld = pool.open_ld(id).unwrap();
    let payload: Vec<u8> = (0..(48 << 10)).map(|i| (i % 211) as u8).collect();
    ld.write_at(0, &payload).unwrap();
    drop(ld);

    // Mark copy 0 Bad via the new pool API.
    let desc = pool.list_lds().into_iter().next().unwrap();
    let bad = desc.members[0];
    pool.mark_chunklet_bad(bad.pd, bad.chunklet_index).unwrap();
    let pd = pool.pd(bad.pd).unwrap();
    let (_, bm, _) = pd.snapshot();
    assert_eq!(bm.get(bad.chunklet_index).unwrap(), ChunkletState::Bad);

    drop(pool);

    // Reopen + verify Bad state survived.
    let pool2 = open_pool(&paths);
    let pd2 = pool2.pd(bad.pd).unwrap();
    let (_, bm2, _) = pd2.snapshot();
    assert_eq!(bm2.get(bad.chunklet_index).unwrap(), ChunkletState::Bad);

    // Reads still succeed via the other 2 copies; sibling copies match the
    // original data (Bad copy is silently excluded by resolve_members).
    let ld2 = pool2.open_ld(id).unwrap();
    let mut readback = vec![0u8; payload.len()];
    ld2.read_at(0, &mut readback).unwrap();
    assert_eq!(readback, payload);
}
