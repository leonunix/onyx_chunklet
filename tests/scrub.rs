//! P6 scrub tests: detect parity / mirror divergence and quarantine
//! culprit chunklets via the bitmap Bad state.

use std::path::PathBuf;
use std::sync::Arc;

use onyx_chunklet::io::RawDevice;
use onyx_chunklet::pool::{LdSpec, ScrubMismatchKind};
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
    assert_eq!(report.marked_bad, 1);
    let pd = pool.pd(parity_member.pd).unwrap();
    let (_, bitmap, _) = pd.snapshot();
    assert_eq!(
        bitmap.get(parity_member.chunklet_index).unwrap(),
        ChunkletState::Bad
    );
}

#[test]
fn raid5_scrub_then_rebuild_restores_parity() {
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
    assert_eq!(scrub_report.marked_bad, 1);

    pool.rebuild_ld(id).unwrap();
    let new_desc = pool.find_ld(id).unwrap();
    // Parity member should now point to a different (chunklet, pd) pair.
    let new_parity = new_desc.members[3];
    assert!(new_parity.pd != parity_member.pd
        || new_parity.chunklet_index != parity_member.chunklet_index);

    // After rebuild, scrub should be clean.
    let scrub2 = pool.scrub_ld(id).unwrap();
    assert!(scrub2.mismatches.is_empty(), "{:?}", scrub2.mismatches);
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
