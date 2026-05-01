//! P8a end-to-end: degraded write + reconstruct-write paths.
//!
//! These tests bake out the new R5/R6 write logic that lets writes proceed
//! even when up to F failed members exist (1 for R5, 2 for R6), and the
//! reconstruct-write (RW) path that beats RMW when a high fraction of the
//! stripe is being modified. Validation strategy: after a write happens
//! against a degraded set, read every byte back through the LD trait —
//! reconstruct paths surface any parity mismatch.

use std::path::PathBuf;
use std::sync::Arc;

use onyx_chunklet::io::RawDevice;
use onyx_chunklet::pool::LdSpec;
use onyx_chunklet::{Pool, PoolConfig};
use tempfile::TempDir;

const PD_SIZE: u64 = 4 * 1024 * 1024 * 1024;
const STRIP: usize = 4096;

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

/// Locate the path holding the LD member at `member_idx`.
fn path_for_member(pool: &Pool, paths: &[PathBuf], member_idx: usize) -> usize {
    let desc = pool.list_lds().into_iter().next().unwrap();
    let pd_id = desc.members[member_idx].pd;
    paths
        .iter()
        .position(|p| pool.pd(pd_id).unwrap().path() == p)
        .unwrap()
}

/// Deterministic per-byte pattern keyed by `tag` so different fills produce
/// distinct byte sequences across a single LD.
fn pattern(tag: u64, len: usize, base: u64) -> Vec<u8> {
    (0..len)
        .map(|i| {
            let mut x = tag.wrapping_add((base + i as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15));
            x ^= x >> 33;
            x = x.wrapping_mul(0xff51_afd7_ed55_8ccd);
            x ^= x >> 33;
            (x >> 56) as u8
        })
        .collect()
}

// -------- RAID-5 ------------------------------------------------------------

/// R5 K=3 + 1 parity (set_size=4). Drop the parity PD, write to data, read
/// back. Without parity there's nothing to reconstruct from, but the data
/// PDs themselves should hold the bytes we wrote — degraded write must skip
/// the parity write rather than fail.
#[test]
fn r5_degraded_write_parity_failed_round_trip() {
    let dir = TempDir::new().unwrap();
    let (pool, paths) = make_pool(&dir, 4);
    let ld_id = pool.create_ld(LdSpec::raid5(3, 1, 1, 0)).unwrap();
    drop(pool);

    let pool_full = open_full(&paths);
    // R5 with K=3: members = [D0, D1, D2, P]; parity = idx 3.
    let drop_idx = path_for_member(&pool_full, &paths, 3);
    drop(pool_full);

    let pool = open_subset(&paths, &[drop_idx]);
    let ld = pool.open_ld(ld_id).unwrap();
    let payload = pattern(0xa1, 3 * STRIP, 0);
    ld.write_at(0, &payload).unwrap();
    let mut readback = vec![0u8; payload.len()];
    ld.read_at(0, &mut readback).unwrap();
    assert_eq!(readback, payload, "data lost on parity-failed degraded write");
}

/// R5: drop one data PD, do a partial write that touches the failed
/// position. RW must reconstruct old data via parity, compute new parity
/// against the union of old + new, and skip the physical write to the
/// failed PD. Read-back via reconstruct must return the new value.
#[test]
fn r5_degraded_write_modified_data_failed_round_trip() {
    let dir = TempDir::new().unwrap();
    let (pool, paths) = make_pool(&dir, 4);
    let ld_id = pool.create_ld(LdSpec::raid5(3, 1, 1, 0)).unwrap();
    let ld = pool.open_ld(ld_id).unwrap();

    // Healthy fill: every LBA gets initial pattern.
    let initial = pattern(0xb2, 3 * STRIP, 0);
    ld.write_at(0, &initial).unwrap();
    drop((ld, pool));

    // Drop the PD holding data position 1 (member idx 1).
    let pool_full = open_full(&paths);
    let drop_idx = path_for_member(&pool_full, &paths, 1);
    drop(pool_full);

    let pool = open_subset(&paths, &[drop_idx]);
    let ld = pool.open_ld(ld_id).unwrap();
    // Partial write covering data_pos 0 and 1 (the failed one).
    let new_chunk = pattern(0xc3, 2 * STRIP, 0);
    ld.write_at(0, &new_chunk).unwrap();

    // Verify: positions 0 and 1 → new_chunk. Position 2 → initial.
    let mut readback = vec![0u8; 3 * STRIP];
    ld.read_at(0, &mut readback).unwrap();
    assert_eq!(&readback[0..2 * STRIP], &new_chunk[..]);
    assert_eq!(&readback[2 * STRIP..], &initial[2 * STRIP..]);
}

/// R5: drop one data PD, write to *unmodified* positions only (failed PD's
/// position is not touched). Verify the failed position still reconstructs
/// correctly post-write.
#[test]
fn r5_degraded_write_unmodified_data_failed_round_trip() {
    let dir = TempDir::new().unwrap();
    let (pool, paths) = make_pool(&dir, 4);
    let ld_id = pool.create_ld(LdSpec::raid5(3, 1, 1, 0)).unwrap();
    let ld = pool.open_ld(ld_id).unwrap();
    let initial = pattern(0xd4, 3 * STRIP, 0);
    ld.write_at(0, &initial).unwrap();
    drop((ld, pool));

    // Drop data position 2; we'll write only to positions 0 + 1.
    let pool_full = open_full(&paths);
    let drop_idx = path_for_member(&pool_full, &paths, 2);
    drop(pool_full);

    let pool = open_subset(&paths, &[drop_idx]);
    let ld = pool.open_ld(ld_id).unwrap();
    let new_chunk = pattern(0xe5, 2 * STRIP, 0);
    ld.write_at(0, &new_chunk).unwrap(); // covers data_pos 0 + 1

    let mut readback = vec![0u8; 3 * STRIP];
    ld.read_at(0, &mut readback).unwrap();
    assert_eq!(&readback[0..2 * STRIP], &new_chunk[..]);
    // data_pos 2 reconstructed from new parity + new D0 + new D1 must
    // recover the OLD initial bytes there.
    assert_eq!(&readback[2 * STRIP..], &initial[2 * STRIP..]);
}

/// R5: F=2 must reject the write outright. Pool needs 6 PDs so quorum
/// (floor(N/2)+1 = 4) still holds after we drop two LD-member PDs.
#[test]
fn r5_two_failures_reject_write() {
    let dir = TempDir::new().unwrap();
    let (pool, paths) = make_pool(&dir, 6);
    let ld_id = pool.create_ld(LdSpec::raid5(3, 1, 1, 0)).unwrap();
    drop(pool);

    let pool_full = open_full(&paths);
    let d0_idx = path_for_member(&pool_full, &paths, 0);
    let d1_idx = path_for_member(&pool_full, &paths, 1);
    drop(pool_full);

    let pool = open_subset(&paths, &[d0_idx, d1_idx]);
    let ld = pool.open_ld(ld_id).unwrap();
    let buf = vec![0u8; STRIP];
    let err = ld.write_at(0, &buf).err().expect("write should fail with F=2");
    let msg = format!("{}", err);
    assert!(msg.contains("max 1"), "wrong error: {}", msg);
}

/// R5 healthy: RW path (M=2 of K=3, full-strip) must produce parity
/// consistent with read-back via reconstruct after dropping one data PD.
#[test]
fn r5_rw_path_parity_correct() {
    let dir = TempDir::new().unwrap();
    let (pool, paths) = make_pool(&dir, 4);
    let ld_id = pool.create_ld(LdSpec::raid5(3, 1, 1, 0)).unwrap();
    let ld = pool.open_ld(ld_id).unwrap();
    let initial = pattern(0xf6, 3 * STRIP, 0);
    ld.write_at(0, &initial).unwrap(); // full-stripe write

    // Now do a partial write that hits 2 of 3 positions (M=2 of K=3) at
    // full-strip granularity. Threshold (K-M)=1 < (M+1)=3 → RW path.
    let new_chunk = pattern(0x07, 2 * STRIP, 0);
    ld.write_at(0, &new_chunk).unwrap();
    drop((ld, pool));

    // Drop data position 0 — read it back via reconstruct.
    let pool_full = open_full(&paths);
    let drop_idx = path_for_member(&pool_full, &paths, 0);
    drop(pool_full);

    let pool = open_subset(&paths, &[drop_idx]);
    let ld = pool.open_ld(ld_id).unwrap();
    let mut readback = vec![0u8; 3 * STRIP];
    ld.read_at(0, &mut readback).unwrap();
    assert_eq!(&readback[0..STRIP], &new_chunk[0..STRIP]);
    assert_eq!(&readback[STRIP..2 * STRIP], &new_chunk[STRIP..]);
    assert_eq!(&readback[2 * STRIP..], &initial[2 * STRIP..]);
}

// -------- RAID-6 ------------------------------------------------------------

/// R6 K=3 + P + Q (set_size=5). Drop both parity PDs. Write to data, read
/// back. Degraded write must skip both parities, not fail.
#[test]
fn r6_degraded_write_both_parities_failed_round_trip() {
    let dir = TempDir::new().unwrap();
    let (pool, paths) = make_pool(&dir, 5);
    let ld_id = pool.create_ld(LdSpec::raid6(3, 1, 1, 0)).unwrap();
    drop(pool);

    let pool_full = open_full(&paths);
    // members = [D0, D1, D2, P, Q]; drop P (idx 3) and Q (idx 4).
    let p_idx = path_for_member(&pool_full, &paths, 3);
    let q_idx = path_for_member(&pool_full, &paths, 4);
    drop(pool_full);

    let pool = open_subset(&paths, &[p_idx, q_idx]);
    let ld = pool.open_ld(ld_id).unwrap();
    let payload = pattern(0x18, 3 * STRIP, 0);
    ld.write_at(0, &payload).unwrap();
    let mut readback = vec![0u8; payload.len()];
    ld.read_at(0, &mut readback).unwrap();
    assert_eq!(readback, payload);
}

/// R6: drop P only (Q healthy). Write must compute Q (and skip P), read
/// back via reconstruct_one_data_via_q after also dropping a data position
/// must succeed.
#[test]
fn r6_degraded_write_p_failed_then_q_reconstruct() {
    let dir = TempDir::new().unwrap();
    let (pool, paths) = make_pool(&dir, 5);
    let ld_id = pool.create_ld(LdSpec::raid6(3, 1, 1, 0)).unwrap();
    let ld = pool.open_ld(ld_id).unwrap();
    let initial = pattern(0x29, 3 * STRIP, 0);
    ld.write_at(0, &initial).unwrap();
    drop((ld, pool));

    // Drop P only. Partial write triggers RW path which recomputes Q.
    let pool_full = open_full(&paths);
    let p_idx = path_for_member(&pool_full, &paths, 3);
    drop(pool_full);

    let pool = open_subset(&paths, &[p_idx]);
    let ld = pool.open_ld(ld_id).unwrap();
    let new_chunk = pattern(0x3a, 2 * STRIP, 0);
    ld.write_at(0, &new_chunk).unwrap(); // partial: positions 0, 1
    drop((ld, pool));

    // Now also drop data position 2 → F=2 (P + D2). Reading data_pos 2
    // must use reconstruct_one_data_via_q (P is gone, Q computed during
    // the degraded write must be correct).
    let pool_full2 = open_subset(&paths, &[p_idx]);
    let d2_idx = path_for_member(&pool_full2, &paths, 2);
    drop(pool_full2);

    let pool = open_subset(&paths, &[p_idx, d2_idx]);
    let ld = pool.open_ld(ld_id).unwrap();
    let mut readback = vec![0u8; 3 * STRIP];
    ld.read_at(0, &mut readback).unwrap();
    assert_eq!(&readback[0..STRIP], &new_chunk[0..STRIP]);
    assert_eq!(&readback[STRIP..2 * STRIP], &new_chunk[STRIP..]);
    // data_pos 2 unmodified — must equal initial bytes there.
    assert_eq!(&readback[2 * STRIP..], &initial[2 * STRIP..]);
}

/// R6: drop one data PD + Q. Partial write on the failed data position
/// must reconstruct old via P, compute new P from the full updated stripe,
/// skip writes to D and Q.
#[test]
fn r6_degraded_write_one_data_plus_q_failed() {
    let dir = TempDir::new().unwrap();
    let (pool, paths) = make_pool(&dir, 5);
    let ld_id = pool.create_ld(LdSpec::raid6(3, 1, 1, 0)).unwrap();
    let ld = pool.open_ld(ld_id).unwrap();
    let initial = pattern(0x4b, 3 * STRIP, 0);
    ld.write_at(0, &initial).unwrap();
    drop((ld, pool));

    let pool_full = open_full(&paths);
    let d1_idx = path_for_member(&pool_full, &paths, 1);
    let q_idx = path_for_member(&pool_full, &paths, 4);
    drop(pool_full);

    let pool = open_subset(&paths, &[d1_idx, q_idx]);
    let ld = pool.open_ld(ld_id).unwrap();
    let new_chunk = pattern(0x5c, 2 * STRIP, 0);
    ld.write_at(0, &new_chunk).unwrap(); // covers D0 + D1 (D1 failed)

    let mut readback = vec![0u8; 3 * STRIP];
    ld.read_at(0, &mut readback).unwrap();
    assert_eq!(&readback[0..2 * STRIP], &new_chunk[..]);
    assert_eq!(&readback[2 * STRIP..], &initial[2 * STRIP..]);
}

/// R6: drop two data PDs (P + Q both healthy). Partial write that hits one
/// of the failed positions must use reconstruct_two_data on the
/// unmodified-failed one, then write everything correctly.
#[test]
fn r6_degraded_write_two_data_failed() {
    let dir = TempDir::new().unwrap();
    let (pool, paths) = make_pool(&dir, 5);
    let ld_id = pool.create_ld(LdSpec::raid6(3, 1, 1, 0)).unwrap();
    let ld = pool.open_ld(ld_id).unwrap();
    let initial = pattern(0x6d, 3 * STRIP, 0);
    ld.write_at(0, &initial).unwrap();
    drop((ld, pool));

    let pool_full = open_full(&paths);
    let d0_idx = path_for_member(&pool_full, &paths, 0);
    let d2_idx = path_for_member(&pool_full, &paths, 2);
    drop(pool_full);

    let pool = open_subset(&paths, &[d0_idx, d2_idx]);
    let ld = pool.open_ld(ld_id).unwrap();
    // Write only D0 (failed). D2 (failed) is unmodified — must be
    // reconstructed via PQ system using its old initial value.
    let new_chunk = pattern(0x7e, STRIP, 0);
    ld.write_at(0, &new_chunk).unwrap();

    let mut readback = vec![0u8; 3 * STRIP];
    ld.read_at(0, &mut readback).unwrap();
    assert_eq!(&readback[0..STRIP], &new_chunk[..]);
    assert_eq!(&readback[STRIP..2 * STRIP], &initial[STRIP..2 * STRIP]);
    assert_eq!(&readback[2 * STRIP..], &initial[2 * STRIP..]);
}

/// R6: F=3 must reject the write outright. Pool needs 8 PDs so quorum
/// (5) still holds after we drop three LD-member PDs.
#[test]
fn r6_three_failures_reject_write() {
    let dir = TempDir::new().unwrap();
    let (pool, paths) = make_pool(&dir, 8);
    let ld_id = pool.create_ld(LdSpec::raid6(3, 1, 1, 0)).unwrap();
    drop(pool);

    let pool_full = open_full(&paths);
    let d0 = path_for_member(&pool_full, &paths, 0);
    let p = path_for_member(&pool_full, &paths, 3);
    let q = path_for_member(&pool_full, &paths, 4);
    drop(pool_full);

    let pool = open_subset(&paths, &[d0, p, q]);
    let ld = pool.open_ld(ld_id).unwrap();
    let buf = vec![0u8; STRIP];
    let err = ld.write_at(0, &buf).err().expect("write should fail with F=3");
    let msg = format!("{}", err);
    assert!(msg.contains("max 2"), "wrong error: {}", msg);
}

/// R6 healthy RW path: M=2 of K=3, full-strip → (K-M)=1 < (M+2)=4 → RW.
/// After the write, drop one data PD and verify reconstruct returns
/// correct bytes (i.e., RW computed parity correctly).
#[test]
fn r6_rw_path_parity_correct() {
    let dir = TempDir::new().unwrap();
    let (pool, paths) = make_pool(&dir, 5);
    let ld_id = pool.create_ld(LdSpec::raid6(3, 1, 1, 0)).unwrap();
    let ld = pool.open_ld(ld_id).unwrap();
    let initial = pattern(0x8f, 3 * STRIP, 0);
    ld.write_at(0, &initial).unwrap();
    let new_chunk = pattern(0x90, 2 * STRIP, 0); // RW path
    ld.write_at(0, &new_chunk).unwrap();
    drop((ld, pool));

    let pool_full = open_full(&paths);
    let drop_idx = path_for_member(&pool_full, &paths, 0);
    drop(pool_full);

    let pool = open_subset(&paths, &[drop_idx]);
    let ld = pool.open_ld(ld_id).unwrap();
    let mut readback = vec![0u8; 3 * STRIP];
    ld.read_at(0, &mut readback).unwrap();
    assert_eq!(&readback[0..STRIP], &new_chunk[0..STRIP]);
    assert_eq!(&readback[STRIP..2 * STRIP], &new_chunk[STRIP..]);
    assert_eq!(&readback[2 * STRIP..], &initial[2 * STRIP..]);
}

/// R6 reconstruct_one_data_via_q correctness: drop P, ensure read-back via
/// the Q-based formula matches the originally written bytes.
#[test]
fn r6_reconstruct_via_q_when_p_dropped() {
    let dir = TempDir::new().unwrap();
    let (pool, paths) = make_pool(&dir, 5);
    let ld_id = pool.create_ld(LdSpec::raid6(3, 1, 1, 0)).unwrap();
    let ld = pool.open_ld(ld_id).unwrap();
    let payload = pattern(0xa1, 3 * STRIP, 0);
    ld.write_at(0, &payload).unwrap();
    drop((ld, pool));

    let pool_full = open_full(&paths);
    // Drop P (idx 3) AND a data position to force read via Q.
    let p_idx = path_for_member(&pool_full, &paths, 3);
    let d1_idx = path_for_member(&pool_full, &paths, 1);
    drop(pool_full);

    let pool = open_subset(&paths, &[p_idx, d1_idx]);
    let ld = pool.open_ld(ld_id).unwrap();
    let mut readback = vec![0u8; 3 * STRIP];
    ld.read_at(0, &mut readback).unwrap();
    // D1's strip must be reconstructed via Q (P is gone).
    assert_eq!(readback, payload);
}
