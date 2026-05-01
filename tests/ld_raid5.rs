//! LdRaid5 integration tests: full-stripe + partial RMW + degraded read
//! reconstruction.
//!
//! Tests are organized so that the EASIEST RAID-5 invariant is checked
//! first (write → read same data), then we explicitly verify the parity is
//! correct by reading the parity chunklet and recomputing it from the data
//! chunklets, then we exercise the degraded-read code path by simulating
//! "this PD is gone" via direct chunklet IO.

use std::path::PathBuf;
use std::sync::Arc;

use onyx_chunklet::io::RawDevice;
use onyx_chunklet::ld::raid5::LdRaid5;
use onyx_chunklet::ld::LogicalDisk;
use onyx_chunklet::pool::LdSpec;
use onyx_chunklet::types::{ChunkletState, BLOCK_SIZE};
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

fn open_pool(paths: &[PathBuf]) -> Arc<Pool> {
    let raws: Vec<_> = paths.iter().map(|p| RawDevice::open(p).unwrap()).collect();
    Pool::open(raws).unwrap()
}

#[test]
fn raid5_full_stripe_round_trip() {
    // 4 PDs, RAID-5 3+1.
    let dir = TempDir::new().unwrap();
    let (pool, _) = make_pool(&dir, 4);
    let id = pool.create_ld(LdSpec::raid5(3, 1, 1, 0)).unwrap();
    let ld = pool.open_ld(id).unwrap();
    // Capacity = 3 * (1 GiB - 4 KiB).
    let chunklet_user =
        onyx_chunklet::types::CHUNKLET_SIZE - onyx_chunklet::types::CHUNKLET_HEADER_BYTES;
    assert_eq!(ld.capacity_bytes(), 3 * chunklet_user);
    // Strip size = full stripe = 3 * 4 KiB.
    assert_eq!(ld.strip_size(), 3 * BLOCK_SIZE as usize);

    // Write multiple full stripes.
    let payload: Vec<u8> = (0..(3 * 16 * BLOCK_SIZE as usize))
        .map(|i| ((i * 31 + 7) % 251) as u8)
        .collect();
    ld.write_at(0, &payload).unwrap();
    let mut readback = vec![0u8; payload.len()];
    ld.read_at(0, &mut readback).unwrap();
    assert_eq!(readback, payload);
}

#[test]
fn raid5_parity_matches_data_xor() {
    // After a full-stripe write we can verify the parity chunklet equals the
    // XOR of the data chunklets at the same in_chunklet_off.
    let dir = TempDir::new().unwrap();
    let (pool, _) = make_pool(&dir, 4);
    let id = pool.create_ld(LdSpec::raid5(3, 1, 1, 0)).unwrap();
    let ld = pool.open_ld(id).unwrap();

    // Write one full stripe of distinct patterns.
    let strip = BLOCK_SIZE as usize;
    let mut payload = vec![0u8; 3 * strip];
    payload[0..strip].iter_mut().for_each(|b| *b = 0xa5);
    payload[strip..2 * strip].iter_mut().for_each(|b| *b = 0x5a);
    payload[2 * strip..3 * strip]
        .iter_mut()
        .for_each(|b| *b = 0xc3);
    ld.write_at(0, &payload).unwrap();

    // Read parity = D0 ^ D1 ^ D2 = 0xa5 ^ 0x5a ^ 0xc3 = 0x3c.
    let desc = pool.find_ld(id).unwrap();
    assert_eq!(desc.members.len(), 4);
    let parity_member = &desc.members[3];
    let parity_pd = pool.pd(parity_member.pd).unwrap();
    let mut parity_buf = vec![0u8; strip];
    parity_pd
        .read_chunklet_user(parity_member.chunklet_index, 0, &mut parity_buf)
        .unwrap();
    assert!(parity_buf.iter().all(|&b| b == 0x3c));
}

#[test]
fn raid5_partial_rmw_preserves_other_positions() {
    let dir = TempDir::new().unwrap();
    let (pool, _) = make_pool(&dir, 4);
    let id = pool.create_ld(LdSpec::raid5(3, 1, 1, 0)).unwrap();
    let ld = pool.open_ld(id).unwrap();

    // Initialize with a full-stripe pattern.
    let strip = BLOCK_SIZE as usize;
    let mut payload = vec![0u8; 3 * strip];
    payload[0..strip].fill(0x11);
    payload[strip..2 * strip].fill(0x22);
    payload[2 * strip..3 * strip].fill(0x33);
    ld.write_at(0, &payload).unwrap();

    // Now overwrite ONLY position 1 (data_pos=1) with 0x99, leaving 0 and 2
    // alone. This exercises the partial RMW path.
    let mut new_d1 = vec![0x99u8; strip];
    ld.write_at(strip as u64, &new_d1).unwrap();

    // Read full stripe and check.
    let mut readback = vec![0u8; 3 * strip];
    ld.read_at(0, &mut readback).unwrap();
    assert!(readback[0..strip].iter().all(|&b| b == 0x11));
    assert!(readback[strip..2 * strip].iter().all(|&b| b == 0x99));
    assert!(readback[2 * strip..3 * strip].iter().all(|&b| b == 0x33));

    // Parity should now equal 0x11 ^ 0x99 ^ 0x33 = 0xbb.
    let desc = pool.find_ld(id).unwrap();
    let parity_member = &desc.members[3];
    let parity_pd = pool.pd(parity_member.pd).unwrap();
    parity_pd
        .read_chunklet_user(parity_member.chunklet_index, 0, &mut new_d1)
        .unwrap();
    assert!(new_d1.iter().all(|&b| b == 0xbb));
}

#[test]
fn raid5_reconstruct_data_via_parity_xor() {
    // The reconstruct helper used by Phase 5 degraded reads.
    let dir = TempDir::new().unwrap();
    let (pool, _) = make_pool(&dir, 4);
    let id = pool.create_ld(LdSpec::raid5(3, 1, 1, 0)).unwrap();
    let ld = pool.open_ld(id).unwrap();
    let strip = BLOCK_SIZE as usize;
    let mut payload = vec![0u8; 3 * strip];
    for (i, byte) in payload.iter_mut().enumerate() {
        *byte = ((i * 17) % 251) as u8;
    }
    ld.write_at(0, &payload).unwrap();
    drop(ld);

    // Re-open as concrete LdRaid5 to call reconstruct_data.
    let desc = pool.find_ld(id).unwrap();
    let pds_map = {
        let mut m = std::collections::BTreeMap::new();
        for info in pool.list_pds() {
            m.insert(info.pd_id, pool.pd(info.pd_id).unwrap());
        }
        m
    };
    let r5 = LdRaid5::open(desc, &pds_map).unwrap();

    // Reconstruct each data position from the surviving members + parity,
    // and assert the result matches the original payload.
    for pos in 0..3 {
        let mut out = vec![0u8; strip];
        r5.reconstruct_data(0, pos, 0, &mut out).unwrap();
        let expected = &payload[pos * strip..(pos + 1) * strip];
        assert_eq!(out, expected, "reconstruct of data_pos {} mismatched", pos);
    }
}

#[test]
fn raid5_persists_across_reopen() {
    let dir = TempDir::new().unwrap();
    let (pool, paths) = make_pool(&dir, 4);
    let id = pool.create_ld(LdSpec::raid5(3, 1, 1, 0)).unwrap();
    let ld = pool.open_ld(id).unwrap();
    let payload: Vec<u8> = (0..(64 << 10))
        .map(|i| ((i * 13 + 5) % 251) as u8)
        .collect();
    ld.write_at(0, &payload).unwrap();
    drop((ld, pool));

    let pool2 = open_pool(&paths);
    let lds = pool2.list_lds();
    assert_eq!(lds.len(), 1);
    let ld2 = pool2.open_ld(id).unwrap();
    let mut readback = vec![0u8; payload.len()];
    ld2.read_at(0, &mut readback).unwrap();
    assert_eq!(readback, payload);
}

#[test]
fn raid5_drops_4_chunklets() {
    let dir = TempDir::new().unwrap();
    let (pool, _) = make_pool(&dir, 4);
    let id = pool.create_ld(LdSpec::raid5(3, 1, 1, 0)).unwrap();
    let mut total_used = 0u32;
    for info in pool.list_pds() {
        let pd = pool.pd(info.pd_id).unwrap();
        let (_, bm, _) = pd.snapshot();
        total_used += bm.count(ChunkletState::Used);
    }
    assert_eq!(total_used, 4);
    pool.drop_ld(id).unwrap();
    let mut total_after = 0u32;
    for info in pool.list_pds() {
        let pd = pool.pd(info.pd_id).unwrap();
        let (_, bm, _) = pd.snapshot();
        total_after += bm.count(ChunkletState::Used);
    }
    assert_eq!(total_after, 0);
}

#[test]
fn raid5_rejects_when_too_few_pds() {
    let dir = TempDir::new().unwrap();
    // Only 3 PDs available; RAID-5 3+1 needs 4 distinct.
    let (pool, _) = make_pool(&dir, 3);
    let err = pool.create_ld(LdSpec::raid5(3, 1, 1, 0)).err().unwrap();
    let s = format!("{}", err);
    assert!(s.contains("distinct PDs") || s.contains("free"), "{}", s);
}

#[test]
fn raid5_rejects_invalid_strip_size_log2() {
    let dir = TempDir::new().unwrap();
    let (pool, _) = make_pool(&dir, 4);
    for bad in [1, 11, 63, 64] {
        let err = pool.create_ld(LdSpec::raid5(3, 1, 1, bad)).err().unwrap();
        assert!(format!("{}", err).contains("strip_size_log2"), "{}", err);
    }
}

#[test]
fn raid5_unaligned_partial_within_strip() {
    // Subblock writes that span less than a full stripe + don't start at 0.
    let dir = TempDir::new().unwrap();
    let (pool, _) = make_pool(&dir, 4);
    let id = pool.create_ld(LdSpec::raid5(3, 1, 1, 0)).unwrap();
    let ld = pool.open_ld(id).unwrap();

    let bs = BLOCK_SIZE as usize;
    // Initialize three full stripes with a known pattern.
    let mut payload = vec![0xa0u8; 9 * bs]; // 3 stripes of 3 strips
    ld.write_at(0, &payload).unwrap();
    // Overwrite blocks 4 and 5 only (offset 4*bs, length 2*bs).
    // These map to data_pos=1 of stripe 1 and data_pos=2 of stripe 1.
    payload[4 * bs..6 * bs].fill(0x55);
    let new_segment = vec![0x55u8; 2 * bs];
    ld.write_at(4 * bs as u64, &new_segment).unwrap();
    let mut readback = vec![0u8; 9 * bs];
    ld.read_at(0, &mut readback).unwrap();
    assert_eq!(readback, payload);
}

/// Regression for the partial-write offset bug: when `strip_size > BLOCK_SIZE`
/// AND a write spans `>= 2` data positions starting at a sub-strip-aligned
/// offset, all helpers (`write_partial_stripe_rmw` / `_rw` / `write_data_only`)
/// previously used `start.in_chunklet_off` (which embeds `start.in_strip_off`)
/// for every position. pos[1+] therefore landed at the wrong chunklet offset
/// and the parity strip was read/written straddling two stripes.
#[test]
fn raid5_partial_rmw_strip_gt_block_spans_positions() {
    let dir = TempDir::new().unwrap();
    let (pool, _) = make_pool(&dir, 4);
    // K=3, strip_size_log2=16 -> 64 KiB strip, full stripe = 192 KiB.
    let id = pool.create_ld(LdSpec::raid5(3, 1, 1, 16)).unwrap();
    let ld = pool.open_ld(id).unwrap();

    let bs = BLOCK_SIZE as usize;
    let strip = 1usize << 16;
    let full_stripe = 3 * strip;

    // Initialize first full stripe with a known pattern (each block has a
    // distinct byte).
    let mut payload = vec![0u8; full_stripe];
    for i in 0..(full_stripe / bs) {
        let v = ((i as u32) & 0xff) as u8;
        payload[i * bs..(i + 1) * bs].fill(v);
    }
    ld.write_at(0, &payload).unwrap();

    // Now do a sub-strip-aligned write that spans pos[0] -> pos[1]: write
    // 8 KiB starting at LD offset (strip - bs) = 60 KiB. The first 4 KiB
    // lands in pos[0]'s last block, the next 4 KiB lands in pos[1]'s first
    // block. start.in_strip_off = 60 KiB, so the buggy code uses chunklet
    // offset 60 KiB for both reads/writes, which corrupts pos[1] (writes
    // 4 KiB at offset 60 KiB into pos[1]'s chunklet instead of offset 0).
    let new = vec![0xa5u8; 2 * bs];
    let off = (strip - bs) as u64;
    payload[off as usize..off as usize + 2 * bs].copy_from_slice(&new);
    ld.write_at(off, &new).unwrap();

    // Read back the entire first stripe and verify byte-for-byte.
    let mut readback = vec![0u8; full_stripe];
    ld.read_at(0, &mut readback).unwrap();
    assert_eq!(
        readback, payload,
        "partial RMW spanning pos[0]->pos[1] at sub-strip offset corrupts data"
    );

    // Verify parity == XOR of all data strips (proves parity wasn't read or
    // written at the wrong chunklet offset).
    let desc = pool.list_lds().into_iter().next().unwrap();
    let pds: Vec<_> = (0..4)
        .map(|i| pool.pd(desc.members[i].pd).unwrap())
        .collect();
    let mut data = vec![vec![0u8; strip]; 3];
    for i in 0..3 {
        pds[i]
            .read_chunklet_user(desc.members[i].chunklet_index, 0, &mut data[i])
            .unwrap();
    }
    let mut expected_parity = data[0].clone();
    for i in 1..3 {
        for j in 0..strip {
            expected_parity[j] ^= data[i][j];
        }
    }
    let mut parity = vec![0u8; strip];
    pds[3]
        .read_chunklet_user(desc.members[3].chunklet_index, 0, &mut parity)
        .unwrap();
    assert_eq!(parity, expected_parity, "parity drifted after partial RMW");
}

/// Regression: write that starts at a sub-strip offset and spans pos[1]->pos[2]
/// (skipping pos[0] entirely), `strip_size > BLOCK_SIZE`. Exercises the same
/// helpers as the spans_positions test but proves the bug isn't specific to
/// pos[0] being the start position.
#[test]
fn raid5_partial_rmw_starts_at_pos1_sub_strip() {
    let dir = TempDir::new().unwrap();
    let (pool, _) = make_pool(&dir, 4);
    let id = pool.create_ld(LdSpec::raid5(3, 1, 1, 16)).unwrap();
    let ld = pool.open_ld(id).unwrap();

    let bs = BLOCK_SIZE as usize;
    let strip = 1usize << 16;
    let full_stripe = 3 * strip;

    let mut payload = vec![0u8; full_stripe];
    for i in 0..(full_stripe / bs) {
        payload[i * bs..(i + 1) * bs].fill((i as u32 & 0xff) as u8);
    }
    ld.write_at(0, &payload).unwrap();

    // Write 8 KiB at LD offset (strip + strip - bs) = 124 KiB. This lands
    // in pos[1]'s last 4 KiB and pos[2]'s first 4 KiB. start.data_pos = 1,
    // start.in_strip_off = 60 KiB.
    let new = vec![0x77u8; 2 * bs];
    let off = (strip + strip - bs) as u64;
    payload[off as usize..off as usize + 2 * bs].copy_from_slice(&new);
    ld.write_at(off, &new).unwrap();

    let mut readback = vec![0u8; full_stripe];
    ld.read_at(0, &mut readback).unwrap();
    assert_eq!(
        readback, payload,
        "partial RMW spanning pos[1]->pos[2] at sub-strip offset corrupts data"
    );

    // Parity must still equal XOR of all data strips.
    let desc = pool.list_lds().into_iter().next().unwrap();
    let pds: Vec<_> = (0..4)
        .map(|i| pool.pd(desc.members[i].pd).unwrap())
        .collect();
    let mut data = vec![vec![0u8; strip]; 3];
    for i in 0..3 {
        pds[i]
            .read_chunklet_user(desc.members[i].chunklet_index, 0, &mut data[i])
            .unwrap();
    }
    let mut expected_parity = data[0].clone();
    for i in 1..3 {
        for j in 0..strip {
            expected_parity[j] ^= data[i][j];
        }
    }
    let mut parity = vec![0u8; strip];
    pds[3]
        .read_chunklet_user(desc.members[3].chunklet_index, 0, &mut parity)
        .unwrap();
    assert_eq!(parity, expected_parity, "parity drifted after partial RMW");
}
