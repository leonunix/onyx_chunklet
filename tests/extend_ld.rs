//! Pool::extend_ld integration tests.
//!
//! Cover the cheap row-append primitive across every RAID level. Anchor
//! test is mirror because it exercises set_size > 1 + multi-PD allocator
//! paths AND has the simplest non-trivial capacity formula. Plain / Raid0
//! tests are thin because their layouts are linear concat already proven
//! by `create_ld`. R5 / R6 tests are load-bearing — they prove the
//! row-major / sets-independent invariant that lets parity rows survive a
//! row-append untouched.

mod common;

use std::path::PathBuf;
use std::sync::Arc;

use onyx_chunklet::io::RawDevice;
use onyx_chunklet::pool::LdSpec;
use onyx_chunklet::types::{ChunkletState, BLOCK_SIZE, CHUNKLET_HEADER_BYTES, CHUNKLET_SIZE};
use onyx_chunklet::{ChunkletError, Pool, PoolConfig};
use tempfile::TempDir;

const CHUNKLET_USER: u64 = CHUNKLET_SIZE - CHUNKLET_HEADER_BYTES;

/// Extend tests need more chunklets per PD than the shared 4 GiB common
/// helper provides. 8 GiB sparse files give ~7 usable chunklets per PD,
/// enough to grow an LD a few times before exhausting the pool.
const EXTEND_PD_SIZE: u64 = 8 * 1024 * 1024 * 1024;

fn make_extend_pool(dir: &TempDir, n_pds: usize) -> (Arc<Pool>, Vec<PathBuf>) {
    let mut raws = Vec::new();
    let mut paths = Vec::new();
    for i in 0..n_pds {
        let p = dir.path().join(format!("pd{}", i));
        raws.push(RawDevice::open_or_create(&p, EXTEND_PD_SIZE).unwrap());
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

fn open_extend_pool(paths: &[PathBuf]) -> Arc<Pool> {
    let raws: Vec<_> = paths.iter().map(|p| RawDevice::open(p).unwrap()).collect();
    Pool::open(raws).unwrap()
}

#[test]
fn extend_mirror_doubles_capacity_and_persists() {
    // R10: 2-way mirror, 2 sets striped per row, 1 row → 4 chunklets,
    // capacity = 2 * CHUNKLET_USER. Extend by 1 row → 8 chunklets,
    // capacity = 4 * CHUNKLET_USER (doubled).
    let dir = TempDir::new().unwrap();
    let (pool, paths) = make_extend_pool(&dir, 4);
    let id = pool.create_ld(LdSpec::mirror(2, 2, 1, 0)).unwrap();
    let ld = pool.open_ld(id).unwrap();
    let strip = BLOCK_SIZE as usize;
    assert_eq!(ld.capacity_bytes(), 2 * CHUNKLET_USER);

    // Write a known pattern to the very start of the old range — easy to
    // verify after extend + reopen.
    let old_payload: Vec<u8> = (0..(8 * strip))
        .map(|i| ((i * 7 + 3) % 251) as u8)
        .collect();
    ld.write_at(0, &old_payload).unwrap();
    drop(ld);

    let new_capacity = pool.extend_ld(id, 1).unwrap();
    assert_eq!(new_capacity, 4 * CHUNKLET_USER);

    // Fresh handle reflects the new capacity. Old offset still reads back
    // the original payload; new offset (just past the original capacity) is
    // newly addressable.
    let ld2 = pool.open_ld(id).unwrap();
    assert_eq!(ld2.capacity_bytes(), 4 * CHUNKLET_USER);
    let mut readback = vec![0u8; old_payload.len()];
    ld2.read_at(0, &mut readback).unwrap();
    assert_eq!(readback, old_payload);

    let new_payload: Vec<u8> = (0..(8 * strip))
        .map(|i| ((i * 11 + 17) % 251) as u8)
        .collect();
    ld2.write_at(2 * CHUNKLET_USER, &new_payload).unwrap();
    drop((ld2, pool));

    // Reopen: descriptor must persist with extended num_rows, and both old
    // + new range data must survive the round-trip.
    let pool2 = open_extend_pool(&paths);
    let ld3 = pool2.open_ld(id).unwrap();
    assert_eq!(ld3.capacity_bytes(), 4 * CHUNKLET_USER);
    let desc = pool2.find_ld(id).unwrap();
    assert_eq!(desc.num_rows, 2);
    assert_eq!(desc.members.len(), 2 * 2 * 2);

    let mut readback = vec![0u8; old_payload.len()];
    ld3.read_at(0, &mut readback).unwrap();
    assert_eq!(readback, old_payload);
    let mut readback = vec![0u8; new_payload.len()];
    ld3.read_at(2 * CHUNKLET_USER, &mut readback).unwrap();
    assert_eq!(readback, new_payload);

    // Bitmap accounting: 8 chunklets Used across the pool (4 + 4 after
    // extend), nothing leaked.
    let mut total_used = 0u32;
    for info in pool2.list_pds() {
        let pd = pool2.pd(info.pd_id).unwrap();
        let (_, bm, _) = pd.snapshot();
        total_used += bm.count(ChunkletState::Used);
    }
    assert_eq!(total_used, 8);
}

#[test]
fn extend_zero_rows_is_noop() {
    let dir = TempDir::new().unwrap();
    let (pool, _) = make_extend_pool(&dir, 4);
    let id = pool.create_ld(LdSpec::mirror(2, 1, 2, 0)).unwrap();
    let before = pool.find_ld(id).unwrap();
    let cap_before = pool.open_ld(id).unwrap().capacity_bytes();

    let cap_after = pool.extend_ld(id, 0).unwrap();
    assert_eq!(cap_after, cap_before);

    let after = pool.find_ld(id).unwrap();
    assert_eq!(
        before, after,
        "descriptor must not change on zero-row extend"
    );
}

#[test]
fn extend_existing_handle_keeps_old_capacity() {
    // extend is additive: handles opened BEFORE the extend keep their
    // original capacity_bytes, and IO to old offsets keeps working. Only a
    // fresh open_ld sees the new capacity. This pins the no-epoch-bump
    // semantics so future refactors don't quietly invalidate live handles.
    let dir = TempDir::new().unwrap();
    let (pool, _) = make_extend_pool(&dir, 4);
    let id = pool.create_ld(LdSpec::mirror(2, 1, 1, 0)).unwrap();
    let ld_before = pool.open_ld(id).unwrap();
    let old_cap = ld_before.capacity_bytes();
    let strip = BLOCK_SIZE as usize;

    let payload = vec![0xa7u8; 4 * strip];
    ld_before.write_at(0, &payload).unwrap();

    pool.extend_ld(id, 1).unwrap();

    // Pre-existing handle: capacity still old, IO inside old range still
    // works, IO past the old boundary fails (out-of-range, not corruption).
    assert_eq!(ld_before.capacity_bytes(), old_cap);
    let mut readback = vec![0u8; payload.len()];
    ld_before.read_at(0, &mut readback).unwrap();
    assert_eq!(readback, payload);
    let mut overflow = vec![0u8; strip];
    let err = ld_before.read_at(old_cap, &mut overflow).err().unwrap();
    assert!(format!("{}", err).contains("out of range"));

    // Fresh handle sees the new capacity and can address the new range.
    let ld_after = pool.open_ld(id).unwrap();
    assert!(ld_after.capacity_bytes() > old_cap);
    let new_payload = vec![0xb3u8; 4 * strip];
    ld_after.write_at(old_cap, &new_payload).unwrap();
    let mut readback = vec![0u8; new_payload.len()];
    ld_after.read_at(old_cap, &mut readback).unwrap();
    assert_eq!(readback, new_payload);
}

#[test]
fn extend_plain_appends() {
    let dir = TempDir::new().unwrap();
    let (pool, _) = make_extend_pool(&dir, 4);
    let id = pool.create_ld(LdSpec::plain(2)).unwrap();
    let cap_before = pool.open_ld(id).unwrap().capacity_bytes();
    assert_eq!(cap_before, 2 * CHUNKLET_USER);

    let cap_after = pool.extend_ld(id, 3).unwrap();
    assert_eq!(cap_after, 5 * CHUNKLET_USER);

    let desc = pool.find_ld(id).unwrap();
    assert_eq!(desc.num_rows, 5);
    assert_eq!(desc.members.len(), 5);
}

#[test]
fn extend_raid0_appends() {
    // Raid0 with stripe_width=2, num_rows=1 → 2 chunklets, capacity = 2 *
    // CHUNKLET_USER. Extend by 2 rows → 6 chunklets, capacity = 6 * CHUNKLET_USER.
    let dir = TempDir::new().unwrap();
    let (pool, _) = make_extend_pool(&dir, 4);
    let id = pool.create_ld(LdSpec::raid0(2, 1, 0)).unwrap();
    let ld = pool.open_ld(id).unwrap();
    assert_eq!(ld.capacity_bytes(), 2 * CHUNKLET_USER);

    let strip = BLOCK_SIZE as usize;
    let old_payload: Vec<u8> = (0..(4 * strip))
        .map(|i| ((i * 5 + 9) % 251) as u8)
        .collect();
    ld.write_at(0, &old_payload).unwrap();
    drop(ld);

    let new_capacity = pool.extend_ld(id, 2).unwrap();
    assert_eq!(new_capacity, 6 * CHUNKLET_USER);

    let ld2 = pool.open_ld(id).unwrap();
    let mut readback = vec![0u8; old_payload.len()];
    ld2.read_at(0, &mut readback).unwrap();
    assert_eq!(readback, old_payload);
}

#[test]
fn extend_raid5_preserves_old_parity() {
    // R5 K=3 (4 PDs total), 1 row initial. Write a known pattern, extend
    // by 1 row, then mark one chunklet of the OLD row Bad — reconstruct
    // read must still produce the original payload, proving extend didn't
    // touch old parity.
    let dir = TempDir::new().unwrap();
    let (pool, _) = make_extend_pool(&dir, 4);
    let id = pool.create_ld(LdSpec::raid5(3, 1, 1, 0)).unwrap();
    let ld = pool.open_ld(id).unwrap();
    let strip = BLOCK_SIZE as usize;
    let stripe = 3 * strip;

    let mut payload = vec![0u8; 4 * stripe];
    for (i, byte) in payload.iter_mut().enumerate() {
        *byte = ((i * 17 + 3) % 251) as u8;
    }
    ld.write_at(0, &payload).unwrap();
    drop(ld);

    pool.extend_ld(id, 1).unwrap();
    let desc = pool.find_ld(id).unwrap();
    assert_eq!(desc.num_rows, 2);
    assert_eq!(desc.members.len(), 4 * 2);

    // Pick a data position from the OLD row (member 0 = row 0, set 0,
    // pos 0) and quarantine it. R5 reconstruct via parity must serve the
    // read.
    let victim = &desc.members[0];
    pool.mark_chunklet_bad(victim.pd, victim.chunklet_index)
        .unwrap();

    let ld2 = pool.open_ld(id).unwrap();
    let mut readback = vec![0u8; payload.len()];
    ld2.read_at(0, &mut readback).unwrap();
    assert_eq!(
        readback, payload,
        "extend changed old-row parity / data; reconstruct returned wrong bytes"
    );
}

#[test]
fn extend_raid6_preserves_old_parity() {
    // R6 K=2 (4 PDs total: 2 data + P + Q), 1 row initial. Extend by 1
    // row, then mark TWO chunklets of the OLD row Bad — R6 must
    // reconstruct from the surviving data + parities.
    let dir = TempDir::new().unwrap();
    let (pool, _) = make_extend_pool(&dir, 4);
    let id = pool.create_ld(LdSpec::raid6(2, 1, 1, 0)).unwrap();
    let ld = pool.open_ld(id).unwrap();
    let strip = BLOCK_SIZE as usize;
    let stripe = 2 * strip;

    let mut payload = vec![0u8; 4 * stripe];
    for (i, byte) in payload.iter_mut().enumerate() {
        *byte = ((i * 23 + 5) % 251) as u8;
    }
    ld.write_at(0, &payload).unwrap();
    drop(ld);

    pool.extend_ld(id, 1).unwrap();
    let desc = pool.find_ld(id).unwrap();
    assert_eq!(desc.num_rows, 2);
    assert_eq!(desc.members.len(), 4 * 2);

    // Knock out the two data chunklets of the OLD row (members 0, 1) — P
    // + Q on the old row must reconstruct them.
    for victim_idx in 0..2 {
        let victim = &desc.members[victim_idx];
        pool.mark_chunklet_bad(victim.pd, victim.chunklet_index)
            .unwrap();
    }

    let ld2 = pool.open_ld(id).unwrap();
    let mut readback = vec![0u8; payload.len()];
    ld2.read_at(0, &mut readback).unwrap();
    assert_eq!(readback, payload);
}

#[test]
fn extend_pool_full_returns_config_error() {
    // 4 PDs × 8 GiB ≈ 7 chunklets per PD = 28 total. Mirror set_size=2,
    // row_size=4 means 8 chunklets per row spread distinctly across 4 PDs
    // (each set of 2 needs 2 PDs) — so each PD takes 2 chunklets per row.
    // 3 rows fits (24 chunklets, 6 per PD), 4 rows would need 8 per PD
    // → fails distinct-PD allocator preflight.
    let dir = TempDir::new().unwrap();
    let (pool, _) = make_extend_pool(&dir, 4);
    let id = pool.create_ld(LdSpec::mirror(2, 4, 3, 0)).unwrap();
    let err = pool.extend_ld(id, 5).err().unwrap();
    assert!(
        matches!(err, ChunkletError::Config(_)),
        "expected pool-full ChunkletError::Config, got: {:?}",
        err
    );
}
