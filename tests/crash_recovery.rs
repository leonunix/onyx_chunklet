//! Crash-recovery integration tests for the superblock COW pair.
//!
//! Each test creates a pool of sparse files, corrupts one or more on-disk
//! slots, then reopens the pool and asserts that the surviving slot is
//! used.
//!
//! These tests exercise the public Pool / PD APIs only; the corruption is
//! done by overwriting raw file bytes via `RawDevice`, which is what a
//! real torn write or media error would look like to us.

use std::path::PathBuf;

use onyx_chunklet::io::{AlignedBuf, RawDevice};
use onyx_chunklet::superblock::SLOT_BYTES;
use onyx_chunklet::types::{
    BITMAP_SLOT_A_OFFSET, BITMAP_SLOT_B_OFFSET, BITMAP_SLOT_BYTES,
    PD_RESERVED_BYTES, SUPERBLOCK_SLOT_A_OFFSET, SUPERBLOCK_SLOT_B_OFFSET,
};
use onyx_chunklet::{Pool, PoolConfig};
use tempfile::TempDir;

const PD_SIZE: u64 = 4 * 1024 * 1024 * 1024;

fn make_pool(dir: &TempDir, names: &[&str]) -> Vec<PathBuf> {
    let mut raws = Vec::new();
    let mut paths = Vec::new();
    for n in names {
        let p = dir.path().join(n);
        raws.push(RawDevice::open_or_create(&p, PD_SIZE).unwrap());
        paths.push(p);
    }
    let _pool = Pool::create(raws, PoolConfig::default()).unwrap();
    paths
}

fn corrupt(path: &PathBuf, offset: u64, len: usize) {
    let raw = RawDevice::open(path).unwrap();
    let buf = vec![0xa5u8; len];
    raw.write_at(&buf, offset).unwrap();
    raw.sync().unwrap();
}

fn open_paths(paths: &[PathBuf]) -> Vec<RawDevice> {
    paths.iter().map(|p| RawDevice::open(p).unwrap()).collect()
}

#[test]
fn survives_head_slot_a_corruption() {
    let dir = TempDir::new().unwrap();
    let paths = make_pool(&dir, &["pd0", "pd1", "pd2"]);
    // Trash superblock head slot A on pd0.
    corrupt(&paths[0], SUPERBLOCK_SLOT_A_OFFSET, SLOT_BYTES);
    // Reopen — should fall back to head B / tail A / tail B.
    let pool = Pool::open(open_paths(&paths)).unwrap();
    assert_eq!(pool.pd_count(), 3);
}

#[test]
fn survives_head_slot_b_corruption() {
    let dir = TempDir::new().unwrap();
    let paths = make_pool(&dir, &["pd0", "pd1"]);
    corrupt(&paths[1], SUPERBLOCK_SLOT_B_OFFSET, SLOT_BYTES);
    let pool = Pool::open(open_paths(&paths)).unwrap();
    assert_eq!(pool.pd_count(), 2);
}

#[test]
fn survives_both_head_slots_via_tail_mirror() {
    let dir = TempDir::new().unwrap();
    let paths = make_pool(&dir, &["pd0", "pd1"]);
    corrupt(&paths[0], SUPERBLOCK_SLOT_A_OFFSET, SLOT_BYTES);
    corrupt(&paths[0], SUPERBLOCK_SLOT_B_OFFSET, SLOT_BYTES);
    let pool = Pool::open(open_paths(&paths)).unwrap();
    assert_eq!(pool.pd_count(), 2);
}

#[test]
fn fails_when_all_four_slots_dead() {
    let dir = TempDir::new().unwrap();
    let paths = make_pool(&dir, &["pd0"]);
    let pd_size = PD_SIZE;
    let tail_base = pd_size - PD_RESERVED_BYTES;
    corrupt(&paths[0], SUPERBLOCK_SLOT_A_OFFSET, SLOT_BYTES);
    corrupt(&paths[0], SUPERBLOCK_SLOT_B_OFFSET, SLOT_BYTES);
    corrupt(&paths[0], tail_base + SUPERBLOCK_SLOT_A_OFFSET, SLOT_BYTES);
    corrupt(&paths[0], tail_base + SUPERBLOCK_SLOT_B_OFFSET, SLOT_BYTES);
    let err = Pool::open(open_paths(&paths))
        .err()
        .expect("expected open to fail");
    assert!(format!("{}", err).contains("no valid superblock"));
}

#[test]
fn survives_bitmap_head_slot_corruption_via_tail() {
    let dir = TempDir::new().unwrap();
    let paths = make_pool(&dir, &["pd0", "pd1"]);
    // Trash both bitmap slots on the head of pd0 — must fall back to tail.
    corrupt(
        &paths[0],
        BITMAP_SLOT_A_OFFSET,
        BITMAP_SLOT_BYTES as usize,
    );
    corrupt(
        &paths[0],
        BITMAP_SLOT_B_OFFSET,
        BITMAP_SLOT_BYTES as usize,
    );
    let pool = Pool::open(open_paths(&paths)).unwrap();
    assert_eq!(pool.pd_count(), 2);
}

#[test]
fn commit_rotates_to_inactive_slot() {
    // After init, gen=1 lives in slot A. Commit once: gen=2 should land in
    // slot B without touching slot A. Verify by reading slot A directly.
    let dir = TempDir::new().unwrap();
    let paths = make_pool(&dir, &["pd0"]);

    // Read & remember the slot A bytes before commit.
    let raw_pre = RawDevice::open(&paths[0]).unwrap();
    let mut a_pre = AlignedBuf::new(SLOT_BYTES).unwrap();
    raw_pre
        .read_at(a_pre.as_mut_slice(), SUPERBLOCK_SLOT_A_OFFSET)
        .unwrap();
    let a_pre_bytes = a_pre.as_slice().to_vec();
    drop(raw_pre);

    // Reopen as a Pool, do one manifest commit on the only PD, then re-read.
    let pool = Pool::open(open_paths(&paths)).unwrap();
    let info = pool.list_pds()[0].clone();
    let pd = pool.pd(info.pd_id).unwrap();
    pd.commit_manifest(|_, _| Ok(())).unwrap();
    drop(pool);

    let raw_post = RawDevice::open(&paths[0]).unwrap();
    let mut a_post = AlignedBuf::new(SLOT_BYTES).unwrap();
    raw_post
        .read_at(a_post.as_mut_slice(), SUPERBLOCK_SLOT_A_OFFSET)
        .unwrap();
    assert_eq!(
        a_post.as_slice(),
        &a_pre_bytes[..],
        "commit must not touch the active slot"
    );

    // Reopen and verify gen=2 is what's loaded.
    let pool2 = Pool::open(open_paths(&paths)).unwrap();
    let info2 = pool2.list_pds()[0].clone();
    assert_eq!(info2.manifest_gen, 2);
}
