//! LdRaid6 integration tests: full-stripe + partial RMW + 1- and 2-failure
//! reconstruction (Anvin convention).

use std::path::PathBuf;
use std::sync::Arc;

use onyx_chunklet::io::RawDevice;
use onyx_chunklet::ld::raid6::LdRaid6;
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
    let pool = Pool::create(raws, PoolConfig { spare_pct: 0 }).unwrap();
    (pool, paths)
}

fn open_pool(paths: &[PathBuf]) -> Arc<Pool> {
    let raws: Vec<_> = paths.iter().map(|p| RawDevice::open(p).unwrap()).collect();
    Pool::open(raws).unwrap()
}

fn pds_map(pool: &Arc<Pool>) -> std::collections::BTreeMap<onyx_chunklet::PdId, Arc<onyx_chunklet::PhysicalDisk>> {
    let mut m = std::collections::BTreeMap::new();
    for info in pool.list_pds() {
        m.insert(info.pd_id, pool.pd(info.pd_id).unwrap());
    }
    m
}

#[test]
fn raid6_full_stripe_round_trip() {
    let dir = TempDir::new().unwrap();
    let (pool, _) = make_pool(&dir, 5);
    let id = pool.create_ld(LdSpec::raid6(3, 1, 1, 0)).unwrap();
    let ld = pool.open_ld(id).unwrap();
    let chunklet_user = onyx_chunklet::types::CHUNKLET_SIZE
        - onyx_chunklet::types::CHUNKLET_HEADER_BYTES;
    assert_eq!(ld.capacity_bytes(), 3 * (chunklet_user / BLOCK_SIZE) * BLOCK_SIZE);
    assert_eq!(ld.strip_size(), 3 * BLOCK_SIZE as usize);

    let payload: Vec<u8> = (0..(3 * 16 * BLOCK_SIZE as usize))
        .map(|i| ((i * 31 + 7) % 251) as u8)
        .collect();
    ld.write_at(0, &payload).unwrap();
    let mut readback = vec![0u8; payload.len()];
    ld.read_at(0, &mut readback).unwrap();
    assert_eq!(readback, payload);
}

#[test]
fn raid6_full_stripe_pq_match_anvin_formulas() {
    // Verify P = XOR of data and Q = sum g^i * D_i.
    let dir = TempDir::new().unwrap();
    let (pool, _) = make_pool(&dir, 5);
    let id = pool.create_ld(LdSpec::raid6(3, 1, 1, 0)).unwrap();
    let ld = pool.open_ld(id).unwrap();

    let strip = BLOCK_SIZE as usize;
    let mut payload = vec![0u8; 3 * strip];
    payload[0..strip].fill(0xa5); // D_0
    payload[strip..2 * strip].fill(0x5a); // D_1
    payload[2 * strip..3 * strip].fill(0xc3); // D_2
    ld.write_at(0, &payload).unwrap();

    let desc = pool.find_ld(id).unwrap();
    assert_eq!(desc.members.len(), 5);
    // members[0..3] = data, [3] = P, [4] = Q.
    let p_member = &desc.members[3];
    let q_member = &desc.members[4];
    let p_pd = pool.pd(p_member.pd).unwrap();
    let q_pd = pool.pd(q_member.pd).unwrap();
    let mut p_buf = vec![0u8; strip];
    let mut q_buf = vec![0u8; strip];
    p_pd.read_chunklet_user(p_member.chunklet_index, 0, &mut p_buf).unwrap();
    q_pd.read_chunklet_user(q_member.chunklet_index, 0, &mut q_buf).unwrap();

    let expected_p = 0xa5u8 ^ 0x5a ^ 0xc3; // = 0x3c
    assert!(p_buf.iter().all(|&b| b == expected_p));
    // Q = g^0*0xa5 ^ g^1*0x5a ^ g^2*0x5a -- compute by hand.
    use onyx_chunklet::ld::gf256::{g_pow, mul};
    let expected_q = mul(g_pow(0), 0xa5) ^ mul(g_pow(1), 0x5a) ^ mul(g_pow(2), 0xc3);
    assert!(
        q_buf.iter().all(|&b| b == expected_q),
        "Q expected {:02x}, got first {:02x}",
        expected_q,
        q_buf[0]
    );
}

#[test]
fn raid6_partial_rmw_preserves_other_data_and_parity() {
    let dir = TempDir::new().unwrap();
    let (pool, _) = make_pool(&dir, 5);
    let id = pool.create_ld(LdSpec::raid6(3, 1, 1, 0)).unwrap();
    let ld = pool.open_ld(id).unwrap();

    let strip = BLOCK_SIZE as usize;
    let mut payload = vec![0u8; 3 * strip];
    payload[0..strip].fill(0x11);
    payload[strip..2 * strip].fill(0x22);
    payload[2 * strip..3 * strip].fill(0x33);
    ld.write_at(0, &payload).unwrap();

    // Overwrite only D_1 with 0x99.
    let new_d1 = vec![0x99u8; strip];
    ld.write_at(strip as u64, &new_d1).unwrap();

    let mut readback = vec![0u8; 3 * strip];
    ld.read_at(0, &mut readback).unwrap();
    assert!(readback[0..strip].iter().all(|&b| b == 0x11));
    assert!(readback[strip..2 * strip].iter().all(|&b| b == 0x99));
    assert!(readback[2 * strip..3 * strip].iter().all(|&b| b == 0x33));

    // Parity should now reflect new data.
    let desc = pool.find_ld(id).unwrap();
    let p_member = &desc.members[3];
    let q_member = &desc.members[4];
    let p_pd = pool.pd(p_member.pd).unwrap();
    let q_pd = pool.pd(q_member.pd).unwrap();
    let mut p_buf = vec![0u8; strip];
    let mut q_buf = vec![0u8; strip];
    p_pd.read_chunklet_user(p_member.chunklet_index, 0, &mut p_buf).unwrap();
    q_pd.read_chunklet_user(q_member.chunklet_index, 0, &mut q_buf).unwrap();

    let expected_p = 0x11u8 ^ 0x99 ^ 0x33;
    use onyx_chunklet::ld::gf256::{g_pow, mul};
    let expected_q = mul(g_pow(0), 0x11) ^ mul(g_pow(1), 0x99) ^ mul(g_pow(2), 0x33);
    assert!(p_buf.iter().all(|&b| b == expected_p));
    assert!(q_buf.iter().all(|&b| b == expected_q));
}

#[test]
fn raid6_reconstruct_one_data_via_p() {
    let dir = TempDir::new().unwrap();
    let (pool, _) = make_pool(&dir, 5);
    let id = pool.create_ld(LdSpec::raid6(3, 1, 1, 0)).unwrap();
    let ld = pool.open_ld(id).unwrap();
    let strip = BLOCK_SIZE as usize;
    let mut payload = vec![0u8; 3 * strip];
    for (i, b) in payload.iter_mut().enumerate() {
        *b = ((i * 17 + 3) % 251) as u8;
    }
    ld.write_at(0, &payload).unwrap();
    drop(ld);

    let desc = pool.find_ld(id).unwrap();
    let r6 = LdRaid6::open(desc, &pds_map(&pool)).unwrap();
    for pos in 0..3 {
        let mut out = vec![0u8; strip];
        r6.reconstruct_one_data(0, pos, 0, &mut out).unwrap();
        assert_eq!(out, &payload[pos * strip..(pos + 1) * strip]);
    }
}

#[test]
fn raid6_reconstruct_two_data_via_pq() {
    let dir = TempDir::new().unwrap();
    let (pool, _) = make_pool(&dir, 5);
    let id = pool.create_ld(LdSpec::raid6(3, 1, 1, 0)).unwrap();
    let ld = pool.open_ld(id).unwrap();
    let strip = BLOCK_SIZE as usize;
    let mut payload = vec![0u8; 3 * strip];
    for (i, b) in payload.iter_mut().enumerate() {
        *b = ((i * 23 + 5) % 251) as u8;
    }
    ld.write_at(0, &payload).unwrap();
    drop(ld);

    let desc = pool.find_ld(id).unwrap();
    let r6 = LdRaid6::open(desc, &pds_map(&pool)).unwrap();
    // Try every (x, y) pair with x < y < K.
    for x in 0..3 {
        for y in (x + 1)..3 {
            let (dx, dy) = r6.reconstruct_two_data(0, x, y, 0, strip).unwrap();
            assert_eq!(dx, &payload[x * strip..(x + 1) * strip], "D_{} mismatch", x);
            assert_eq!(dy, &payload[y * strip..(y + 1) * strip], "D_{} mismatch", y);
        }
    }
}

#[test]
fn raid6_persists_across_reopen() {
    let dir = TempDir::new().unwrap();
    let (pool, paths) = make_pool(&dir, 5);
    let id = pool.create_ld(LdSpec::raid6(3, 1, 1, 0)).unwrap();
    let ld = pool.open_ld(id).unwrap();
    let payload: Vec<u8> = (0..(3 * 4 * BLOCK_SIZE as usize)) // 4 stripes
        .map(|i| (i % 199) as u8)
        .collect();
    ld.write_at(0, &payload).unwrap();
    drop((ld, pool));

    let pool2 = open_pool(&paths);
    let ld2 = pool2.open_ld(id).unwrap();
    let mut readback = vec![0u8; payload.len()];
    ld2.read_at(0, &mut readback).unwrap();
    assert_eq!(readback, payload);
}

#[test]
fn raid6_drops_5_chunklets() {
    let dir = TempDir::new().unwrap();
    let (pool, _) = make_pool(&dir, 5);
    let id = pool.create_ld(LdSpec::raid6(3, 1, 1, 0)).unwrap();
    let mut total = 0u32;
    for info in pool.list_pds() {
        total += pool.pd(info.pd_id).unwrap().snapshot().1.count(ChunkletState::Used);
    }
    assert_eq!(total, 5);
    pool.drop_ld(id).unwrap();
    let mut total2 = 0u32;
    for info in pool.list_pds() {
        total2 += pool.pd(info.pd_id).unwrap().snapshot().1.count(ChunkletState::Used);
    }
    assert_eq!(total2, 0);
}

#[test]
fn raid6_rejects_when_too_few_pds() {
    let dir = TempDir::new().unwrap();
    let (pool, _) = make_pool(&dir, 3);
    // RAID-6 3+2 needs 5 distinct PDs.
    let err = pool.create_ld(LdSpec::raid6(3, 1, 1, 0)).err().unwrap();
    let s = format!("{}", err);
    assert!(s.contains("distinct PDs") || s.contains("free"), "{}", s);
}
