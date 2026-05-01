//! P5 end-to-end: simulate PD failure (reopen pool with missing devices),
//! verify degraded reads succeed via reconstruct, then `Pool::rebuild_ld`
//! restores full redundancy and post-rebuild reads still match.

use std::path::PathBuf;
use std::sync::Arc;

use onyx_chunklet::io::RawDevice;
use onyx_chunklet::pool::LdSpec;
use onyx_chunklet::types::ChunkletState;
use onyx_chunklet::{Pool, PoolConfig};
use tempfile::TempDir;

const PD_SIZE: u64 = 4 * 1024 * 1024 * 1024;

fn make_pool(dir: &TempDir, n: usize) -> (Arc<Pool>, Vec<PathBuf>) {
    let mut raws = Vec::new();
    let mut paths = Vec::new();
    for i in 0..n {
        let p = dir.path().join(format!("pd{}", i));
        raws.push(RawDevice::open_or_create(&p, PD_SIZE).unwrap());
        paths.push(p);
    }
    let pool = Pool::create(raws, PoolConfig { spare_pct: 0 }).unwrap();
    (pool, paths)
}

fn open_subset(paths: &[PathBuf], drop_idx: &[usize]) -> Arc<Pool> {
    let raws: Vec<_> = paths
        .iter()
        .enumerate()
        .filter(|(i, _)| !drop_idx.contains(i))
        .map(|(_, p)| RawDevice::open(p).unwrap())
        .collect();
    Pool::open_with_missing(raws).unwrap()
}

fn open_full(paths: &[PathBuf]) -> Arc<Pool> {
    let raws: Vec<_> = paths.iter().map(|p| RawDevice::open(p).unwrap()).collect();
    Pool::open(raws).unwrap()
}

// ------ Mirror RAID-1 ------------------------------------------------------

#[test]
fn mirror_degraded_read_then_rebuild() {
    let dir = TempDir::new().unwrap();
    // 3 PDs so rebuild has somewhere to place the new copy.
    let (pool, paths) = make_pool(&dir, 3);
    let ld_id = pool.create_ld(LdSpec::mirror(2, 1, 1, 0)).unwrap();
    let ld = pool.open_ld(ld_id).unwrap();
    let payload: Vec<u8> = (0..(64 << 10)).map(|i| (i % 211) as u8).collect();
    ld.write_at(0, &payload).unwrap();
    drop((ld, pool));

    // Find which PD holds member 0 of the mirror, then drop it.
    let pool_full = open_full(&paths);
    let desc = pool_full.list_lds().into_iter().next().unwrap();
    let m0_pd = desc.members[0].pd;
    let drop_idx = paths
        .iter()
        .position(|p| pool_full.pd(m0_pd).unwrap().path() == p)
        .unwrap();
    drop(pool_full);

    let pool_deg = open_subset(&paths, &[drop_idx]);
    assert_eq!(pool_deg.failed_pds().len(), 1);
    let ld_deg = pool_deg.open_ld(ld_id).unwrap();
    let mut readback = vec![0u8; payload.len()];
    ld_deg.read_at(0, &mut readback).unwrap();
    assert_eq!(readback, payload, "degraded mirror read mismatch");
    drop(ld_deg);

    // Rebuild.
    let report = pool_deg.rebuild_ld(ld_id).unwrap();
    assert_eq!(report.rebuilt_members, 1);
    let ld_post = pool_deg.open_ld(ld_id).unwrap();
    let mut readback2 = vec![0u8; payload.len()];
    ld_post.read_at(0, &mut readback2).unwrap();
    assert_eq!(readback2, payload, "post-rebuild mirror read mismatch");

    // Both copies should now live on the 2 healthy PDs (not the dropped one).
    let new_desc = pool_deg.find_ld(ld_id).unwrap();
    for m in &new_desc.members {
        assert_ne!(m.pd, m0_pd, "rebuilt member still on failed PD");
    }
}

// ------ RAID-5 -------------------------------------------------------------

#[test]
fn raid5_degraded_read_then_rebuild() {
    let dir = TempDir::new().unwrap();
    // 5 PDs: RAID-5 3+1 uses 4, leaves 1 PD as rebuild target.
    let (pool, paths) = make_pool(&dir, 5);
    let ld_id = pool.create_ld(LdSpec::raid5(3, 1, 1, 0)).unwrap();
    let ld = pool.open_ld(ld_id).unwrap();
    let payload: Vec<u8> = (0..(48 << 10)).map(|i| ((i * 13 + 5) % 251) as u8).collect();
    ld.write_at(0, &payload).unwrap();
    drop((ld, pool));

    // Drop one of the data PDs (member 0).
    let pool_full = open_full(&paths);
    let desc = pool_full.list_lds().into_iter().next().unwrap();
    let drop_pd = desc.members[0].pd;
    let drop_idx = paths
        .iter()
        .position(|p| pool_full.pd(drop_pd).unwrap().path() == p)
        .unwrap();
    drop(pool_full);

    let pool_deg = open_subset(&paths, &[drop_idx]);
    let ld_deg = pool_deg.open_ld(ld_id).unwrap();
    let mut readback = vec![0u8; payload.len()];
    ld_deg.read_at(0, &mut readback).unwrap();
    assert_eq!(readback, payload, "raid5 degraded read mismatch");
    drop(ld_deg);

    let report = pool_deg.rebuild_ld(ld_id).unwrap();
    assert_eq!(report.rebuilt_members, 1);
    let ld_post = pool_deg.open_ld(ld_id).unwrap();
    let mut readback2 = vec![0u8; payload.len()];
    ld_post.read_at(0, &mut readback2).unwrap();
    assert_eq!(readback2, payload, "raid5 post-rebuild read mismatch");
}

#[test]
fn raid5_rebuild_parity_failure() {
    let dir = TempDir::new().unwrap();
    let (pool, paths) = make_pool(&dir, 5);
    let ld_id = pool.create_ld(LdSpec::raid5(3, 1, 1, 0)).unwrap();
    let ld = pool.open_ld(ld_id).unwrap();
    let payload: Vec<u8> = (0..(36 << 10)).map(|i| (i % 199) as u8).collect();
    ld.write_at(0, &payload).unwrap();
    drop((ld, pool));

    // Drop the parity PD (member 3 = last in 3+1).
    let pool_full = open_full(&paths);
    let desc = pool_full.list_lds().into_iter().next().unwrap();
    let drop_pd = desc.members[3].pd;
    let drop_idx = paths
        .iter()
        .position(|p| pool_full.pd(drop_pd).unwrap().path() == p)
        .unwrap();
    drop(pool_full);

    let pool_deg = open_subset(&paths, &[drop_idx]);
    // Reads still work (data is alive).
    let ld_deg = pool_deg.open_ld(ld_id).unwrap();
    let mut readback = vec![0u8; payload.len()];
    ld_deg.read_at(0, &mut readback).unwrap();
    assert_eq!(readback, payload);
    drop(ld_deg);

    pool_deg.rebuild_ld(ld_id).unwrap();
    // Now write should succeed too.
    let ld_post = pool_deg.open_ld(ld_id).unwrap();
    let new_payload = vec![0xee_u8; payload.len()];
    ld_post.write_at(0, &new_payload).unwrap();
    let mut readback2 = vec![0u8; payload.len()];
    ld_post.read_at(0, &mut readback2).unwrap();
    assert_eq!(readback2, new_payload);
}

// ------ RAID-6 -------------------------------------------------------------

#[test]
fn raid6_degraded_read_one_failure_then_rebuild() {
    let dir = TempDir::new().unwrap();
    // RAID-6 3+2 needs 5 distinct PDs; 6 PDs → 1 spare for rebuild.
    let (pool, paths) = make_pool(&dir, 6);
    let ld_id = pool.create_ld(LdSpec::raid6(3, 1, 1, 0)).unwrap();
    let ld = pool.open_ld(ld_id).unwrap();
    let payload: Vec<u8> = (0..(48 << 10)).map(|i| ((i * 17 + 3) % 251) as u8).collect();
    ld.write_at(0, &payload).unwrap();
    drop((ld, pool));

    let pool_full = open_full(&paths);
    let desc = pool_full.list_lds().into_iter().next().unwrap();
    let drop_pd = desc.members[1].pd; // a data position
    let drop_idx = paths
        .iter()
        .position(|p| pool_full.pd(drop_pd).unwrap().path() == p)
        .unwrap();
    drop(pool_full);

    let pool_deg = open_subset(&paths, &[drop_idx]);
    let ld_deg = pool_deg.open_ld(ld_id).unwrap();
    let mut readback = vec![0u8; payload.len()];
    ld_deg.read_at(0, &mut readback).unwrap();
    assert_eq!(readback, payload, "raid6 1-failure degraded read mismatch");
    drop(ld_deg);

    pool_deg.rebuild_ld(ld_id).unwrap();
    let ld_post = pool_deg.open_ld(ld_id).unwrap();
    let mut readback2 = vec![0u8; payload.len()];
    ld_post.read_at(0, &mut readback2).unwrap();
    assert_eq!(readback2, payload);
}

#[test]
fn raid6_degraded_read_two_data_failures_then_rebuild() {
    let dir = TempDir::new().unwrap();
    // 7 PDs: RAID-6 3+2 uses 5, leaves 2 spares; we drop 2 data PDs.
    let (pool, paths) = make_pool(&dir, 7);
    let ld_id = pool.create_ld(LdSpec::raid6(3, 1, 1, 0)).unwrap();
    let ld = pool.open_ld(ld_id).unwrap();
    let payload: Vec<u8> = (0..(36 << 10)).map(|i| ((i * 23 + 7) % 251) as u8).collect();
    ld.write_at(0, &payload).unwrap();
    drop((ld, pool));

    let pool_full = open_full(&paths);
    let desc = pool_full.list_lds().into_iter().next().unwrap();
    // Drop members 0 and 2 (both data positions).
    let drop_pds = [desc.members[0].pd, desc.members[2].pd];
    let drop_idxs: Vec<usize> = drop_pds
        .iter()
        .map(|pd| {
            paths
                .iter()
                .position(|p| pool_full.pd(*pd).unwrap().path() == p)
                .unwrap()
        })
        .collect();
    drop(pool_full);

    let pool_deg = open_subset(&paths, &drop_idxs);
    assert_eq!(pool_deg.failed_pds().len(), 2);
    let ld_deg = pool_deg.open_ld(ld_id).unwrap();
    let mut readback = vec![0u8; payload.len()];
    ld_deg.read_at(0, &mut readback).unwrap();
    assert_eq!(readback, payload, "raid6 2-data-failure degraded read mismatch");
    drop(ld_deg);

    let report = pool_deg.rebuild_ld(ld_id).unwrap();
    assert_eq!(report.rebuilt_members, 2);
    let ld_post = pool_deg.open_ld(ld_id).unwrap();
    let mut readback2 = vec![0u8; payload.len()];
    ld_post.read_at(0, &mut readback2).unwrap();
    assert_eq!(readback2, payload);
}

// ------ No-redundancy LDs --------------------------------------------------

#[test]
fn rebuild_ld_unsupported_for_plain_and_raid0() {
    // Healthy Plain / Raid0 rebuild = skipped no-op. With a failure, rebuild
    // explicitly errors with the no-redundancy reason.
    let dir = TempDir::new().unwrap();
    let (pool, paths) = make_pool(&dir, 3);
    let plain_id = pool.create_ld(LdSpec::plain(2)).unwrap();
    let raid0_id = pool.create_ld(LdSpec::raid0(2, 1, 0)).unwrap();
    drop(pool);

    let pool_full = open_full(&paths);
    // Find a PD that's used by the Plain LD's first member, then drop it.
    let plain_desc = pool_full.find_ld(plain_id).unwrap();
    let drop_pd = plain_desc.members[0].pd;
    let drop_idx = paths
        .iter()
        .position(|p| pool_full.pd(drop_pd).unwrap().path() == p)
        .unwrap();
    drop(pool_full);

    let pool_deg = open_subset(&paths, &[drop_idx]);
    let err1 = pool_deg.rebuild_ld(plain_id).err().unwrap();
    assert!(format!("{}", err1).contains("no redundancy"));
    // raid0_id may or may not have a member on the dropped PD; either way the
    // attempt should be a clean no-redundancy error if a member is missing,
    // or a clean skipped if not.
    match pool_deg.rebuild_ld(raid0_id) {
        Err(e) => assert!(format!("{}", e).contains("no redundancy")),
        Ok(report) => assert!(report.skipped),
    }
}

#[test]
fn rebuild_no_op_when_nothing_failed() {
    let dir = TempDir::new().unwrap();
    let (pool, _paths) = make_pool(&dir, 3);
    let id = pool.create_ld(LdSpec::mirror(2, 1, 1, 0)).unwrap();
    let report = pool.rebuild_ld(id).unwrap();
    assert!(report.skipped);
    assert_eq!(report.rebuilt_members, 0);
}

// ------ Spare bitmap state -------------------------------------------------

#[test]
fn rebuild_marks_target_chunklets_used() {
    let dir = TempDir::new().unwrap();
    let (pool, paths) = make_pool(&dir, 3);
    let ld_id = pool.create_ld(LdSpec::mirror(2, 1, 1, 0)).unwrap();
    let ld = pool.open_ld(ld_id).unwrap();
    let payload = vec![0xa5_u8; 8 << 10];
    ld.write_at(0, &payload).unwrap();
    drop((ld, pool));

    let pool_full = open_full(&paths);
    let desc = pool_full.list_lds().into_iter().next().unwrap();
    let drop_pd = desc.members[0].pd;
    let drop_idx = paths.iter().position(|p| pool_full.pd(drop_pd).unwrap().path() == p).unwrap();
    drop(pool_full);

    let pool_deg = open_subset(&paths, &[drop_idx]);
    pool_deg.rebuild_ld(ld_id).unwrap();
    let new_desc = pool_deg.find_ld(ld_id).unwrap();
    let new_member_pd = new_desc.members[0].pd;
    let pd = pool_deg.pd(new_member_pd).unwrap();
    let (_, bitmap, _) = pd.snapshot();
    assert_eq!(
        bitmap.get(new_desc.members[0].chunklet_index).unwrap(),
        ChunkletState::Used
    );
}
