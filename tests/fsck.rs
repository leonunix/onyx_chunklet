//! Reverse-reconcile / fsck: reclaim Used-but-unreferenced chunklets.
//!
//! After a member is rebuilt off a Failed-but-still-present PD, that PD's old
//! chunklet stays `Used` on its bitmap (rebuild's `freed_by_pd` skips Failed
//! PDs) yet is referenced by no live descriptor — a capacity leak. These tests
//! pin the online `Pool::fsck`, the open-time reverse-reconcile hook, the
//! missing-PD safety gate, and non-interference with a healthy pool.

mod common;

use common::{make_pool, open_full, open_subset, path_for_member, pattern};
use onyx_chunklet::pool::LdSpec;
use onyx_chunklet::{LdId, PdId, Pool};

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

fn used_on(pool: &Pool, pd_id: PdId) -> u32 {
    pool.metrics()
        .unwrap()
        .pds
        .into_iter()
        .find(|p| p.pd_id == pd_id)
        .map(|p| p.used_chunklets)
        .unwrap_or(0)
}

/// Rebuild a member off a Failed (but present) PD, clear the flag so the pool
/// is complete again, then `fsck` reclaims the orphaned Used chunklet.
#[test]
fn fsck_reclaims_orphan_after_rebuild() {
    let dir = tempfile::tempdir().unwrap();
    let (pool, _paths) = make_pool(&dir, 10);
    let ld = make_raid6(&pool, 2);
    let data = write_pattern(&pool, ld, 0xAB, 64 * 1024);

    let (victim_pd, victim_idx) = {
        let desc = pool.list_lds().into_iter().next().unwrap();
        (desc.members[3].pd, desc.members[3].chunklet_index)
    };
    let used_before = used_on(&pool, victim_pd);

    pool.mark_pd_failed(victim_pd).unwrap();
    pool.rebuild_ld(ld).unwrap();
    // Descriptor no longer references (victim_pd, victim_idx); its bitmap still
    // marks it Used → orphan.
    let desc_after = pool.list_lds().into_iter().next().unwrap();
    assert!(
        !desc_after
            .members
            .iter()
            .any(|m| m.pd == victim_pd && m.chunklet_index == victim_idx),
        "rebuilt member should have moved off the victim chunklet"
    );

    // Physical disk is still present — clear the flag so the pool is complete.
    pool.clear_pd_failed(victim_pd).unwrap();

    let report = pool.fsck().unwrap();
    assert!(!report.skipped_incomplete, "complete pool must not skip");
    assert!(
        report.total_reclaimed >= 1,
        "expected >=1 reclaimed, got {}",
        report.total_reclaimed
    );
    assert!(
        report.reclaimed_by_pd.get(&victim_pd).copied().unwrap_or(0) >= 1,
        "victim PD's orphan should be reclaimed"
    );
    assert!(
        used_on(&pool, victim_pd) <= used_before,
        "victim used_chunklets should not have grown after reclaim"
    );

    // Live data is untouched by the reclaim.
    assert_eq!(
        read_back(&pool, ld, data.len()),
        data,
        "data must survive fsck"
    );
}

/// The open-time reverse-reconcile hook (Pool::open only) reclaims the stale
/// Used chunklet on a returned disk that still carries its FAILED flag.
#[test]
fn open_reverse_reconciles_stale_used() {
    let dir = tempfile::tempdir().unwrap();
    let (pool, paths) = make_pool(&dir, 10);
    let ld = make_raid6(&pool, 2);
    write_pattern(&pool, ld, 0xCD, 64 * 1024);

    let victim_pd = pool.list_lds()[0].members[5].pd;
    pool.mark_pd_failed(victim_pd).unwrap();
    pool.rebuild_ld(ld).unwrap();
    drop(pool);

    // Reopen with every PD present (strict open) → open hook reverse-reconciles.
    let pool2 = open_full(&paths);
    let m = pool2.metrics().unwrap();
    assert!(
        m.last_fsck_reclaimed >= 1,
        "open should have reclaimed the stale orphan, got {}",
        m.last_fsck_reclaimed
    );
}

/// With a PD missing, the in-memory descriptor set is not authoritative, so
/// fsck must skip and reclaim nothing.
#[test]
fn fsck_skips_incomplete_pool() {
    let dir = tempfile::tempdir().unwrap();
    let (pool, paths) = make_pool(&dir, 10);
    let ld = make_raid6(&pool, 2);
    write_pattern(&pool, ld, 0xEF, 64 * 1024);

    let victim_path_idx = path_for_member(&pool, &paths, 5);
    let victim_pd = pool.list_lds()[0].members[5].pd;
    pool.mark_pd_failed(victim_pd).unwrap();
    pool.rebuild_ld(ld).unwrap();
    drop(pool);

    // Reopen WITHOUT the victim device → open_with_missing → has_missing.
    let pool2 = open_subset(&paths, &[victim_path_idx]);
    let report = pool2.fsck().unwrap();
    assert!(report.skipped_incomplete, "missing PD must force a skip");
    assert_eq!(report.total_reclaimed, 0, "skip reclaims nothing");
}

/// A healthy pool has no orphans; fsck reclaims nothing and never touches live
/// data.
#[test]
fn fsck_leaves_healthy_pool_untouched() {
    let dir = tempfile::tempdir().unwrap();
    let (pool, _paths) = make_pool(&dir, 10);
    let ld = make_raid6(&pool, 3);
    let data = write_pattern(&pool, ld, 0x42, 128 * 1024);

    let report = pool.fsck().unwrap();
    assert!(!report.skipped_incomplete);
    assert_eq!(
        report.total_reclaimed, 0,
        "healthy pool must have no Used orphans"
    );
    assert_eq!(read_back(&pool, ld, data.len()), data);
}
