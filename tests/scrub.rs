//! P6 scrub tests: detect parity / mirror divergence and quarantine
//! culprit chunklets via the bitmap Bad state.

use std::path::PathBuf;
use std::sync::Arc;

use onyx_chunklet::io::RawDevice;
use onyx_chunklet::pool::{LdSpec, ScrubMismatchKind};
use onyx_chunklet::types::ChunkletState;
use onyx_chunklet::{Pool, PoolConfig};
use tempfile::TempDir;

fn open_subset(paths: &[PathBuf], drop_idx: &[usize]) -> Arc<Pool> {
    let raws: Vec<_> = paths
        .iter()
        .enumerate()
        .filter(|(i, _)| !drop_idx.contains(i))
        .map(|(_, p)| RawDevice::open(p).unwrap())
        .collect();
    Pool::open_with_missing(raws).unwrap()
}

const PD_SIZE: u64 = 4 * 1024 * 1024 * 1024;

fn make_pool(dir: &TempDir, n: usize) -> (Arc<Pool>, Vec<PathBuf>) {
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
            ..Default::default()
        },
    )
    .unwrap();
    (pool, paths)
}

fn corrupt_chunklet_user_byte0(pool: &Arc<Pool>, pd_id: onyx_chunklet::PdId, chunklet_idx: u32) {
    let pd = pool.pd(pd_id).unwrap();
    let mut buf = vec![0u8; 4096];
    pd.read_chunklet_user(chunklet_idx, 0, &mut buf).unwrap();
    buf[0] ^= 0xff;
    pd.write_chunklet_user(chunklet_idx, 0, &buf).unwrap();
    pd.sync().unwrap();
}

#[test]
fn raid5_scrub_clean_after_full_stripe_write() {
    let dir = TempDir::new().unwrap();
    let (pool, _) = make_pool(&dir, 4);
    let id = pool.create_ld(LdSpec::raid5(3, 1, 1, 0)).unwrap();
    let ld = pool.open_ld(id).unwrap();
    let payload = vec![0xa5_u8; 12 * 4096];
    ld.write_at(0, &payload).unwrap();
    drop(ld);
    let report = pool.scrub_ld(id).unwrap();
    assert!(report.mismatches.is_empty(), "{:?}", report.mismatches);
    assert_eq!(report.marked_bad, 0);
}

#[test]
fn raid5_scrub_detects_corrupt_parity() {
    let dir = TempDir::new().unwrap();
    let (pool, _) = make_pool(&dir, 4);
    let id = pool.create_ld(LdSpec::raid5(3, 1, 1, 0)).unwrap();
    let ld = pool.open_ld(id).unwrap();
    let payload = vec![0x5a_u8; 16 * 4096];
    ld.write_at(0, &payload).unwrap();
    drop(ld);
    let desc = pool.find_ld(id).unwrap();
    let parity_member = &desc.members[3];
    corrupt_chunklet_user_byte0(&pool, parity_member.pd, parity_member.chunklet_index);

    let report = pool.scrub_ld(id).unwrap();
    assert_eq!(report.mismatches.len(), 1);
    assert!(matches!(
        report.mismatches[0].kind,
        ScrubMismatchKind::Raid5ParityMismatch
    ));
    assert_eq!(report.marked_bad, 0);
    let pd = pool.pd(parity_member.pd).unwrap();
    let (_, bitmap, _) = pd.snapshot();
    assert_eq!(
        bitmap.get(parity_member.chunklet_index).unwrap(),
        ChunkletState::Used
    );
}

#[test]
fn raid5_scrub_reports_ambiguous_mismatch_without_rebuild() {
    let dir = TempDir::new().unwrap();
    let (pool, _) = make_pool(&dir, 5); // extra PD as rebuild target
    let id = pool.create_ld(LdSpec::raid5(3, 1, 1, 0)).unwrap();
    let ld = pool.open_ld(id).unwrap();
    let payload: Vec<u8> = (0..(12 * 4096)).map(|i| (i % 199) as u8).collect();
    ld.write_at(0, &payload).unwrap();
    drop(ld);
    let desc = pool.find_ld(id).unwrap();
    let parity_member = desc.members[3];
    corrupt_chunklet_user_byte0(&pool, parity_member.pd, parity_member.chunklet_index);
    let scrub_report = pool.scrub_ld(id).unwrap();
    assert_eq!(scrub_report.mismatches.len(), 1);
    assert_eq!(scrub_report.marked_bad, 0);

    let new_desc = pool.find_ld(id).unwrap();
    assert_eq!(new_desc.members[3], parity_member);
}

#[test]
fn raid6_scrub_detects_corrupt_p() {
    let dir = TempDir::new().unwrap();
    let (pool, _) = make_pool(&dir, 5);
    let id = pool.create_ld(LdSpec::raid6(3, 1, 1, 0)).unwrap();
    let ld = pool.open_ld(id).unwrap();
    let payload = vec![0xc3_u8; 16 * 4096];
    ld.write_at(0, &payload).unwrap();
    drop(ld);
    let desc = pool.find_ld(id).unwrap();
    let p_member = desc.members[3];
    corrupt_chunklet_user_byte0(&pool, p_member.pd, p_member.chunklet_index);
    let report = pool.scrub_ld(id).unwrap();
    assert_eq!(report.mismatches.len(), 1);
    assert_eq!(report.mismatches[0].kind, ScrubMismatchKind::Raid6P);
    assert_eq!(report.marked_bad, 1);
}

#[test]
fn raid6_scrub_detects_corrupt_q() {
    let dir = TempDir::new().unwrap();
    let (pool, _) = make_pool(&dir, 5);
    let id = pool.create_ld(LdSpec::raid6(3, 1, 1, 0)).unwrap();
    let ld = pool.open_ld(id).unwrap();
    let payload = vec![0xc3_u8; 16 * 4096];
    ld.write_at(0, &payload).unwrap();
    drop(ld);
    let desc = pool.find_ld(id).unwrap();
    let q_member = desc.members[4];
    corrupt_chunklet_user_byte0(&pool, q_member.pd, q_member.chunklet_index);
    let report = pool.scrub_ld(id).unwrap();
    assert_eq!(report.mismatches.len(), 1);
    assert_eq!(report.mismatches[0].kind, ScrubMismatchKind::Raid6Q);
    assert_eq!(report.marked_bad, 1);
}

#[test]
fn mirror_3way_scrub_majority_marks_minority_bad() {
    let dir = TempDir::new().unwrap();
    let (pool, _) = make_pool(&dir, 4);
    // 3-way mirror.
    let id = pool.create_ld(LdSpec::mirror(3, 1, 1, 0)).unwrap();
    let ld = pool.open_ld(id).unwrap();
    let payload = vec![0x42_u8; 8 * 4096];
    ld.write_at(0, &payload).unwrap();
    drop(ld);
    let desc = pool.find_ld(id).unwrap();
    // Corrupt copy 0 — copies 1 and 2 form the majority.
    let target = desc.members[0];
    corrupt_chunklet_user_byte0(&pool, target.pd, target.chunklet_index);
    let report = pool.scrub_ld(id).unwrap();
    assert!(!report.mismatches.is_empty(), "expected mismatch");
    assert!(report.marked_bad >= 1);
    let pd = pool.pd(target.pd).unwrap();
    let (_, bitmap, _) = pd.snapshot();
    assert_eq!(
        bitmap.get(target.chunklet_index).unwrap(),
        ChunkletState::Bad
    );
}

#[test]
fn mirror_2way_scrub_does_not_mark_when_ambiguous() {
    let dir = TempDir::new().unwrap();
    let (pool, _) = make_pool(&dir, 3);
    let id = pool.create_ld(LdSpec::mirror(2, 1, 1, 0)).unwrap();
    let ld = pool.open_ld(id).unwrap();
    let payload = vec![0x33_u8; 4 * 4096];
    ld.write_at(0, &payload).unwrap();
    drop(ld);
    let desc = pool.find_ld(id).unwrap();
    let target = desc.members[0];
    corrupt_chunklet_user_byte0(&pool, target.pd, target.chunklet_index);
    let report = pool.scrub_ld(id).unwrap();
    // Should detect the divergence but not mark Bad (can't tell which is right).
    assert!(!report.mismatches.is_empty());
    assert_eq!(report.marked_bad, 0);
}

#[test]
fn plain_scrub_no_op() {
    let dir = TempDir::new().unwrap();
    let (pool, _) = make_pool(&dir, 3);
    let id = pool.create_ld(LdSpec::plain(2)).unwrap();
    let report = pool.scrub_ld(id).unwrap();
    assert!(report.mismatches.is_empty());
    assert_eq!(report.marked_bad, 0);
}

/// Regression: scrub used to silently `continue` past sets with a missing
/// member, returning report.mismatches=[] which read like "all clean".
/// `sets_skipped_degraded` now counts those skips so the operator can see
/// the LD is degraded and the scrub didn't cover it.
#[test]
fn raid5_scrub_reports_skipped_when_data_member_missing() {
    let dir = TempDir::new().unwrap();
    let (pool, paths) = make_pool(&dir, 4);
    let id = pool.create_ld(LdSpec::raid5(3, 1, 1, 0)).unwrap();
    let ld = pool.open_ld(id).unwrap();
    ld.write_at(0, &vec![0xc3_u8; 12 * 4096]).unwrap();
    drop(ld);

    let desc = pool.find_ld(id).unwrap();
    // Drop a data PD (member 0).
    let drop_pd = desc.members[0].pd;
    let drop_idx = paths
        .iter()
        .position(|p| pool.pd(drop_pd).unwrap().path() == p)
        .unwrap();
    drop(pool);

    let pool_deg = open_subset(&paths, &[drop_idx]);
    let report = pool_deg.scrub_ld(id).unwrap();
    assert!(report.mismatches.is_empty(), "no actual mismatch expected");
    assert!(
        report.sets_skipped_degraded > 0,
        "scrub must surface degraded sets that it skipped"
    );
}

#[test]
fn raid6_scrub_reports_skipped_when_p_missing() {
    let dir = TempDir::new().unwrap();
    let (pool, paths) = make_pool(&dir, 5);
    let id = pool.create_ld(LdSpec::raid6(3, 1, 1, 0)).unwrap();
    let ld = pool.open_ld(id).unwrap();
    ld.write_at(0, &vec![0xa1_u8; 12 * 4096]).unwrap();
    drop(ld);

    let desc = pool.find_ld(id).unwrap();
    let p_member = desc.members[3]; // P slot
    let drop_idx = paths
        .iter()
        .position(|p| pool.pd(p_member.pd).unwrap().path() == p)
        .unwrap();
    drop(pool);

    let pool_deg = open_subset(&paths, &[drop_idx]);
    let report = pool_deg.scrub_ld(id).unwrap();
    assert!(report.mismatches.is_empty());
    assert!(report.sets_skipped_degraded > 0);
}

#[test]
fn raid5_scrub_clean_set_does_not_skip() {
    let dir = TempDir::new().unwrap();
    let (pool, _) = make_pool(&dir, 4);
    let id = pool.create_ld(LdSpec::raid5(3, 1, 1, 0)).unwrap();
    let ld = pool.open_ld(id).unwrap();
    ld.write_at(0, &vec![0x44_u8; 12 * 4096]).unwrap();
    drop(ld);
    let report = pool.scrub_ld(id).unwrap();
    assert_eq!(report.sets_skipped_degraded, 0);
    assert!(report.mismatches.is_empty());
}

/// Scrub now releases manifest_lock during its read-only batch IO and only
/// re-acquires it for commit_bad_marks at the end. This smoke test runs
/// scrub + a concurrent admin op (mark_chunklet_bad on a different LD's
/// member) and verifies both complete without deadlock or error.
#[test]
fn scrub_and_concurrent_admin_op_both_succeed() {
    use std::thread;
    let dir = TempDir::new().unwrap();
    let (pool, _) = make_pool(&dir, 8);
    let scrub_id = pool.create_ld(LdSpec::raid5(3, 1, 1, 0)).unwrap();
    let other_id = pool.create_ld(LdSpec::raid5(3, 1, 1, 0)).unwrap();
    let ld = pool.open_ld(scrub_id).unwrap();
    ld.write_at(0, &vec![0x99_u8; 12 * 4096]).unwrap();
    drop(ld);

    let other_target = pool.find_ld(other_id).unwrap().members[0];
    let pool_a = Arc::clone(&pool);
    let pool_b = Arc::clone(&pool);
    let h1 = thread::spawn(move || pool_a.scrub_ld(scrub_id));
    let h2 = thread::spawn(move || {
        pool_b.mark_chunklet_bad(other_target.pd, other_target.chunklet_index)
    });
    let r1 = h1.join().unwrap().unwrap();
    let r2 = h2.join().unwrap();
    assert!(r1.mismatches.is_empty());
    r2.unwrap();
    let pd = pool.pd(other_target.pd).unwrap();
    let (_, bm, _) = pd.snapshot();
    assert_eq!(
        bm.get(other_target.chunklet_index).unwrap(),
        ChunkletState::Bad
    );
}
