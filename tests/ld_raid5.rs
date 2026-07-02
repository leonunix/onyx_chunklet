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
use std::thread;

use onyx_chunklet::io::RawDevice;
use onyx_chunklet::ld::raid5::LdRaid5;
use onyx_chunklet::LogicalDisk;
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

fn pds_map(
    pool: &Arc<Pool>,
) -> std::collections::BTreeMap<onyx_chunklet::PdId, Arc<onyx_chunklet::PhysicalDisk>> {
    let mut m = std::collections::BTreeMap::new();
    for info in pool.list_pds() {
        m.insert(info.pd_id, pool.pd(info.pd_id).unwrap());
    }
    m
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
fn raid5_concurrent_disjoint_full_stripe_writes() {
    let dir = TempDir::new().unwrap();
    let (pool, _) = make_pool(&dir, 4);
    let id = pool.create_ld(LdSpec::raid5(3, 1, 1, 0)).unwrap();
    let stripe = 3 * BLOCK_SIZE as usize;

    let handles: Vec<_> = (0..4)
        .map(|i| {
            let ld = pool.open_ld(id).unwrap();
            thread::spawn(move || {
                let offset = (i * stripe) as u64;
                for round in 0..64 {
                    let fill = (0x31 + i as u8).wrapping_add(round as u8);
                    let payload = vec![fill; stripe];
                    ld.write_at(offset, &payload).unwrap();
                }
            })
        })
        .collect();
    for handle in handles {
        handle.join().unwrap();
    }

    let ld = pool.open_ld(id).unwrap();
    for i in 0..4 {
        let fill = (0x31 + i as u8).wrapping_add(63);
        let mut readback = vec![0u8; stripe];
        ld.read_at((i * stripe) as u64, &mut readback).unwrap();
        assert!(readback.iter().all(|&b| b == fill), "stripe {}", i);
    }
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

// ---- batched write_many_at (the flusher hot path) --------------------------
//
// write_many_at now collapses every op's stripe segments into ONE batched
// read submit + ONE batched write submit (healthy disjoint stripes) instead
// of the trait-default serial per-op loop. These assert the batched path
// recomputes parity correctly across Full / RMW / RW segments, and bails to
// the serial path for degraded sets + intra-batch stripe collisions.

/// Read the raw D0..D(K-1) + parity strips of one healthy set stripe and
/// assert on-disk parity == XOR of on-disk data.
fn assert_r5_stripe_parity(
    pool: &Arc<Pool>,
    id: onyx_chunklet::LdId,
    in_chunklet_off: u64,
    strip: usize,
) {
    let desc = pool.find_ld(id).unwrap();
    let k = (desc.set_size - 1) as usize;
    let mut exp = vec![0u8; strip];
    for pos in 0..k {
        let mm = &desc.members[pos];
        let mut d = vec![0u8; strip];
        pool.pd(mm.pd)
            .unwrap()
            .read_chunklet_user(mm.chunklet_index, in_chunklet_off, &mut d)
            .unwrap();
        for i in 0..strip {
            exp[i] ^= d[i];
        }
    }
    let pm = &desc.members[k];
    let mut p_buf = vec![0u8; strip];
    pool.pd(pm.pd)
        .unwrap()
        .read_chunklet_user(pm.chunklet_index, in_chunklet_off, &mut p_buf)
        .unwrap();
    assert_eq!(p_buf, exp, "parity mismatch at off {}", in_chunklet_off);
}

#[test]
fn raid5_write_many_batched_full_and_partial() {
    let dir = TempDir::new().unwrap();
    let (pool, _) = make_pool(&dir, 4);
    let id = pool.create_ld(LdSpec::raid5(3, 1, 1, 0)).unwrap();
    let ld = pool.open_ld(id).unwrap();
    let strip = BLOCK_SIZE as usize;
    let fs = 3 * strip;

    let full: Vec<u8> = (0..fs).map(|i| ((i * 7 + 1) % 251) as u8).collect(); // stripe 0, Full
    let one: Vec<u8> = vec![0xA5u8; strip]; // stripe 4 / D1 only → RMW
    let two: Vec<u8> = (0..2 * strip).map(|i| ((i * 13 + 5) % 251) as u8).collect(); // stripe 7 / D0+D1 → RW

    let off_full = 0u64;
    let off_one = (4 * fs + strip) as u64;
    let off_two = (7 * fs) as u64;

    ld.write_many_at(&[
        (off_full, full.as_slice()),
        (off_one, one.as_slice()),
        (off_two, two.as_slice()),
    ])
    .unwrap();

    let mut rb = vec![0u8; fs];
    ld.read_at(off_full, &mut rb).unwrap();
    assert_eq!(rb, full);
    let mut rb1 = vec![0u8; strip];
    ld.read_at(off_one, &mut rb1).unwrap();
    assert_eq!(rb1, one);
    let mut rb2 = vec![0u8; 2 * strip];
    ld.read_at(off_two, &mut rb2).unwrap();
    assert_eq!(rb2, two);

    drop(ld);
    assert_r5_stripe_parity(&pool, id, 0, strip);
    assert_r5_stripe_parity(&pool, id, 4 * strip as u64, strip);
    assert_r5_stripe_parity(&pool, id, 7 * strip as u64, strip);
}

#[test]
fn raid5_write_many_batched_rmw_substrip() {
    let dir = TempDir::new().unwrap();
    let (pool, _) = make_pool(&dir, 4);
    let id = pool.create_ld(LdSpec::raid5(3, 1, 1, 13)).unwrap(); // strip = 2 blocks
    let ld = pool.open_ld(id).unwrap();
    let strip = 2 * BLOCK_SIZE as usize;
    let fs = 3 * strip;

    let seed: Vec<u8> = (0..fs).map(|i| ((i * 11 + 2) % 251) as u8).collect();
    ld.write_at((2 * fs) as u64, &seed).unwrap();

    // Sub-strip overwrite (len < strip) → RMW.
    let sub: Vec<u8> = vec![0x5Au8; BLOCK_SIZE as usize];
    ld.write_many_at(&[((2 * fs) as u64, sub.as_slice())]).unwrap();

    let mut expect = seed.clone();
    expect[0..BLOCK_SIZE as usize].copy_from_slice(&sub);
    let mut rb = vec![0u8; fs];
    ld.read_at((2 * fs) as u64, &mut rb).unwrap();
    assert_eq!(rb, expect);

    drop(ld);
    assert_r5_stripe_parity(&pool, id, 2 * strip as u64, strip);
}

#[test]
fn raid5_write_many_batched_duplicate_stripe_serializes() {
    let dir = TempDir::new().unwrap();
    let (pool, _) = make_pool(&dir, 4);
    let id = pool.create_ld(LdSpec::raid5(3, 1, 1, 0)).unwrap();
    let ld = pool.open_ld(id).unwrap();
    let strip = BLOCK_SIZE as usize;
    let fs = 3 * strip;

    let a = vec![0x11u8; fs];
    let b = vec![0x22u8; fs];
    ld.write_many_at(&[(0u64, a.as_slice()), (0u64, b.as_slice())])
        .unwrap();

    let mut rb = vec![0u8; fs];
    ld.read_at(0, &mut rb).unwrap();
    assert_eq!(rb, b);
    drop(ld);
    assert_r5_stripe_parity(&pool, id, 0, strip);
}

#[test]
fn raid5_write_many_batched_degraded_falls_back() {
    let dir = TempDir::new().unwrap();
    let (pool, _) = make_pool(&dir, 4);
    let id = pool.create_ld(LdSpec::raid5(3, 1, 1, 0)).unwrap();
    let ld = pool.open_ld(id).unwrap();
    let strip = BLOCK_SIZE as usize;
    let fs = 3 * strip;
    let seed: Vec<u8> = (0..2 * fs).map(|i| ((i * 7 + 9) % 251) as u8).collect();
    ld.write_at(0, &seed).unwrap();
    drop(ld);

    let desc = pool.find_ld(id).unwrap();
    let d0_pd = desc.members[0].pd;
    let mut pds = pds_map(&pool);
    pds.remove(&d0_pd);
    let r5 = LdRaid5::open(pool.find_ld(id).unwrap(), &pds).unwrap();

    let w0 = vec![0x33u8; fs];
    let w1 = vec![0x44u8; strip];
    r5.write_many_at(&[(0u64, w0.as_slice()), ((fs + strip) as u64, w1.as_slice())])
        .unwrap();

    let mut rb0 = vec![0u8; fs];
    r5.read_at(0, &mut rb0).unwrap();
    assert_eq!(rb0, w0);
    let mut rb1 = vec![0u8; strip];
    r5.read_at((fs + strip) as u64, &mut rb1).unwrap();
    assert_eq!(rb1, w1);
}
