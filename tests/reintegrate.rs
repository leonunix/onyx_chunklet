//! Returned-disk reintegration (Wipe strategy) + failed-PD retirement.
//!
//! Exercises the full lifecycle tail the fault→isolate→rebuild chain left open:
//! a physically-returned disk rejoins by being wiped and re-admitted into the
//! failed tombstone's pool slot (`reintegrate_wipe`, `pool_pd_count` invariant),
//! and a gone-for-good disk is dropped with the surviving seqs re-densed
//! (`retire_failed_pd`, `pool_pd_count` shrinks). The safety gate must migrate a
//! still-referenced member off the disk BEFORE wiping — never destroy live data.

mod common;

use common::{make_pool, open_full, open_subset, path_for_member, pattern};
use onyx_chunklet::io::RawDevice;
use onyx_chunklet::pool::{LdSpec, RebalanceOptions};
use onyx_chunklet::{LdId, Pool, PoolConfig};
use std::path::PathBuf;

fn make_raid6(pool: &Pool, num_rows: u16) -> LdId {
    // 6 data + P + Q = set_size 8; 4 KiB strips.
    pool.create_ld(LdSpec::raid6(6, 1, num_rows, 12)).unwrap()
}

fn write_pattern(pool: &Pool, ld: LdId, tag: u64, len: usize) -> Vec<u8> {
    let data = pattern(tag, len, 0);
    let h = pool.open_ld(ld).unwrap();
    h.write_at(0, &data).unwrap();
    h.flush().unwrap();
    data
}

fn read_back(pool: &Pool, ld: LdId, len: usize) -> Vec<u8> {
    let mut got = vec![0u8; len];
    pool.open_ld(ld).unwrap().read_at(0, &mut got).unwrap();
    got
}

/// Paths minus one index (the "returned" device stays out of the reopen so the
/// pool doesn't hold its flock — mirrors a pulled disk).
fn paths_without(paths: &[PathBuf], drop_idx: usize) -> Vec<PathBuf> {
    paths
        .iter()
        .enumerate()
        .filter(|(i, _)| *i != drop_idx)
        .map(|(_, p)| p.clone())
        .collect()
}

/// Happy path: a disk that was already rebuilt away (unreferenced tombstone)
/// returns, gets wiped, and rejoins at the SAME pool slot. `pool_pd_count` is
/// unchanged, no LD needs rebuilding, live data survives, and the pool reopens
/// strict afterward (the cross-PD pd_list swap is durable + consistent).
#[test]
fn reintegrate_replace_in_place_keeps_count_and_data() {
    let dir = tempfile::tempdir().unwrap();
    let (pool, paths) = make_pool(&dir, 10);
    let ld = make_raid6(&pool, 2);
    let data = write_pattern(&pool, ld, 0xAB, 96 * 1024);

    // Fail + rebuild member[3] away so the victim PD is an unreferenced tombstone.
    let victim_idx = path_for_member(&pool, &paths, 3);
    let victim_pd = pool.list_lds()[0].members[3].pd;
    let victim_seq = pool
        .list_pds()
        .into_iter()
        .find(|p| p.pd_id == victim_pd)
        .unwrap()
        .pd_seq_in_pool;
    pool.mark_pd_failed(victim_pd).unwrap();
    pool.rebuild_ld(ld).unwrap();
    drop(pool);

    // Disk "pulled": reopen degraded without it (missing tombstone), then it
    // "returns" as a fresh raw device.
    let pool2 = open_subset(&paths, &[victim_idx]);
    assert_eq!(pool2.pd_count(), 9, "victim missing → 9 live PDs");

    let returned = RawDevice::open(&paths[victim_idx]).unwrap();
    let report = pool2.reintegrate_wipe(returned).unwrap();

    assert_eq!(report.replaced_pd_id, victim_pd, "reused the victim's slot");
    assert_eq!(report.reused_seq, victim_seq, "same pd_seq");
    assert_ne!(report.new_pd_id, victim_pd, "fresh identity");
    assert!(
        report.rebuilt_lds.is_empty(),
        "already unreferenced → no safety-gate rebuild"
    );
    assert_eq!(report.referenced_members_blocking, 0);
    assert_eq!(pool2.pd_count(), 10, "replace-in-place → count restored to 10");

    // The reintegrated PD is fresh/near-empty (the rebalance opportunity).
    let new_used = pool2
        .metrics()
        .unwrap()
        .pds
        .into_iter()
        .find(|p| p.pd_id == report.new_pd_id)
        .map(|p| p.used_chunklets)
        .unwrap();
    assert_eq!(new_used, 0, "wiped disk rejoins empty");

    assert_eq!(read_back(&pool2, ld, data.len()), data, "data survives reintegrate");
    drop(pool2);

    // Strict reopen with ALL devices: the swapped pd_list (new PdId at the reused
    // seq, count 10, victim gone) is durable + consistent across every PD.
    let pool3 = open_full(&paths);
    assert_eq!(pool3.pd_count(), 10);
    assert!(
        pool3.list_pds().iter().all(|p| p.pd_id != victim_pd),
        "old tombstone is gone from the pd_list"
    );
    assert_eq!(read_back(&pool3, ld, data.len()), data, "data survives reopen");
}

/// Safety gate: a returned disk whose member is STILL referenced (auto-failover
/// hadn't rebuilt it) must be rebuilt away BEFORE the wipe — reintegrate does it
/// itself and never destroys the live member.
#[test]
fn reintegrate_safety_gate_rebuilds_before_wipe() {
    let dir = tempfile::tempdir().unwrap();
    let (pool, paths) = make_pool(&dir, 10);
    let ld = make_raid6(&pool, 2);
    let data = write_pattern(&pool, ld, 0xCD, 96 * 1024);

    let victim_idx = path_for_member(&pool, &paths, 3);
    let victim_pd = pool.list_lds()[0].members[3].pd;
    drop(pool);

    // Pulled WITHOUT a prior rebuild → member[3] still references the victim
    // (served degraded from RAID6 redundancy).
    let pool2 = open_subset(&paths, &[victim_idx]);
    assert!(
        pool2.list_lds()[0].members.iter().any(|m| m.pd == victim_pd),
        "member still references the victim before reintegrate"
    );
    assert_eq!(
        read_back(&pool2, ld, data.len()),
        data,
        "degraded read reconstructs the missing member"
    );

    let returned = RawDevice::open(&paths[victim_idx]).unwrap();
    let report = pool2.reintegrate_wipe(returned).unwrap();

    assert!(
        report.referenced_members_blocking >= 1,
        "the safety gate saw a still-referenced member"
    );
    assert!(
        report.rebuilt_lds.contains(&ld),
        "the LD was rebuilt before the wipe"
    );
    assert_eq!(pool2.pd_count(), 10);
    assert!(
        pool2.list_lds()[0].members.iter().all(|m| m.pd != victim_pd),
        "no member references the old PD after reintegrate"
    );
    assert_eq!(read_back(&pool2, ld, data.len()), data, "live data preserved");
}

/// A device from a DIFFERENT pool is never auto-reintegrated.
#[test]
fn reintegrate_rejects_foreign_pool() {
    let dir = tempfile::tempdir().unwrap();
    // Pool A (target) + pool B (foreign). Drop B so its device isn't flocked.
    let (pool_a, _a_paths) = make_pool(&dir, 5);
    let b_dir = tempfile::tempdir().unwrap();
    let mut b_raws = Vec::new();
    let mut b_paths = Vec::new();
    for i in 0..5 {
        let p = b_dir.path().join(format!("pdb{}", i));
        b_raws.push(RawDevice::open_or_create(&p, 4 * 1024 * 1024 * 1024).unwrap());
        b_paths.push(p);
    }
    let pool_b = Pool::create(b_raws, PoolConfig::default()).unwrap();
    drop(pool_b);

    let foreign = RawDevice::open(&b_paths[0]).unwrap();
    let err = pool_a.reintegrate_wipe(foreign).unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("belongs to pool") || msg.contains("pool"),
        "foreign device must be rejected, got: {}",
        msg
    );
}

/// Capstone: the full returned-disk lifecycle end to end on one pool —
/// write → pull → reintegrate (safety-gate rebuild + wipe + slot reuse) →
/// rebalance the now-empty disk back to balanced → strict reopen — with a CRC
/// check at every stage. This is the sparse-file mirror of the nvme-box
/// acceptance flow (the *concurrent-IO* aspect is covered by the `#[ignore]`
/// online-rebalance cases in fault_injection.rs).
#[test]
fn full_lifecycle_pull_reintegrate_rebalance_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let (pool, paths) = make_pool(&dir, 10);
    let ld = make_raid6(&pool, 3);
    let data = write_pattern(&pool, ld, 0x77, 160 * 1024);

    let victim_idx = path_for_member(&pool, &paths, 4);
    let victim_pd = pool.list_lds()[0].members[4].pd;
    drop(pool);

    // 1) Pull the disk → degraded pool still serves data (reconstruct).
    let pool = open_subset(&paths, &[victim_idx]);
    assert_eq!(read_back(&pool, ld, data.len()), data, "degraded read ok");

    // 2) Disk returns → reintegrate (safety gate rebuilds member[4] off it first,
    //    then wipes + rejoins at the same slot; count restored to 10).
    let returned = RawDevice::open(&paths[victim_idx]).unwrap();
    let rep = pool.reintegrate_wipe(returned).unwrap();
    assert_eq!(rep.replaced_pd_id, victim_pd);
    assert_eq!(pool.pd_count(), 10);
    assert_eq!(read_back(&pool, ld, data.len()), data, "data ok post-reintegrate");

    // 3) The reintegrated disk is empty → skew is high; rebalance converges it
    //    back down without losing data or breaking set-PD uniqueness.
    let before = pool.metrics().unwrap().used_skew_chunklets;
    let r = pool
        .rebalance(RebalanceOptions {
            target_skew_pct: 20.0,
            max_moves: 64,
        })
        .unwrap();
    assert!(!r.stuck, "rebalance must not be stuck: {:?}", r);
    assert!(
        r.skew_after <= before,
        "skew must not grow: {} -> {}",
        before,
        r.skew_after
    );
    assert_eq!(read_back(&pool, ld, data.len()), data, "data ok post-rebalance");
    // Set-PD uniqueness holds across every set after the moves.
    let desc = pool.list_lds().into_iter().next().unwrap();
    let set_size = desc.set_size as usize;
    for set in desc.members.chunks(set_size) {
        let mut pds: Vec<_> = set.iter().map(|m| m.pd).collect();
        pds.sort();
        let n = pds.len();
        pds.dedup();
        assert_eq!(pds.len(), n, "set-PD uniqueness preserved");
    }
    drop(pool);

    // 4) Strict reopen with all devices: every manifest change is durable.
    let pool = open_full(&paths);
    assert_eq!(pool.pd_count(), 10);
    assert_eq!(read_back(&pool, ld, data.len()), data, "data survives reopen");
}

/// A gone-for-good disk is retired: its tombstone drops, surviving seqs re-dense
/// to `[0, count-1)`, `pool_pd_count` shrinks, and the smaller pool reopens
/// strict (the re-densed per-PD seq is durable, not just in-memory).
#[test]
fn retire_failed_pd_redenses_and_reopens_smaller() {
    let dir = tempfile::tempdir().unwrap();
    let (pool, paths) = make_pool(&dir, 10);
    let ld = make_raid6(&pool, 2);
    let data = write_pattern(&pool, ld, 0xEF, 96 * 1024);

    let victim_idx = path_for_member(&pool, &paths, 3);
    let victim_pd = pool.list_lds()[0].members[3].pd;
    pool.mark_pd_failed(victim_pd).unwrap();
    pool.rebuild_ld(ld).unwrap();
    drop(pool);

    // Pulled for good → reopen degraded, then retire the tombstone.
    let pool2 = open_subset(&paths, &[victim_idx]);
    assert_eq!(pool2.pd_count(), 9);
    pool2.retire_failed_pd(victim_pd).unwrap();
    assert_eq!(pool2.pd_count(), 9, "retire doesn't add/remove live handles");

    // Surviving seqs are dense [0, 9) with no gap, victim gone.
    let mut seqs: Vec<u32> = pool2.list_pds().iter().map(|p| p.pd_seq_in_pool).collect();
    seqs.sort_unstable();
    assert_eq!(seqs, (0..9).collect::<Vec<_>>(), "re-densed to [0,9)");
    assert!(pool2.list_pds().iter().all(|p| p.pd_id != victim_pd));
    assert_eq!(read_back(&pool2, ld, data.len()), data);
    drop(pool2);

    // Strict reopen with exactly the 9 survivors: pool_pd_count=9 + dense seqs
    // are durable, so the smaller pool opens cleanly and data survives.
    let survivor_paths = paths_without(&paths, victim_idx);
    let pool3 = open_full(&survivor_paths);
    assert_eq!(pool3.pd_count(), 9, "reopens as a 9-PD pool");
    let mut seqs3: Vec<u32> = pool3.list_pds().iter().map(|p| p.pd_seq_in_pool).collect();
    seqs3.sort_unstable();
    assert_eq!(seqs3, (0..9).collect::<Vec<_>>());
    assert_eq!(read_back(&pool3, ld, data.len()), data, "data survives retire+reopen");
}
