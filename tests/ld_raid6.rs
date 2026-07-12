//! LdRaid6 integration tests: full-stripe + partial RMW + 1- and 2-failure
//! reconstruction (Anvin convention).

use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;

use onyx_chunklet::error::ChunkletResult;
use onyx_chunklet::io::{IoBackend, RawDevice, StripRead, StripWrite};
use onyx_chunklet::ld::raid6::LdRaid6;
use onyx_chunklet::pool::LdSpec;
use onyx_chunklet::types::{ChunkletState, BLOCK_SIZE};
use onyx_chunklet::LogicalDisk;
use onyx_chunklet::{Pool, PoolConfig};
use tempfile::TempDir;

const PD_SIZE: u64 = 4 * 1024 * 1024 * 1024;

struct CountingReadBackend {
    inner: Arc<dyn IoBackend>,
    submits: AtomicUsize,
    ops: AtomicUsize,
}

impl CountingReadBackend {
    fn new(inner: Arc<dyn IoBackend>) -> Self {
        Self {
            inner,
            submits: AtomicUsize::new(0),
            ops: AtomicUsize::new(0),
        }
    }
}

impl IoBackend for CountingReadBackend {
    fn name(&self) -> &'static str {
        "counting-read"
    }

    fn submit_reads(&self, ops: &mut [StripRead<'_>]) -> ChunkletResult<()> {
        self.submits.fetch_add(1, Ordering::Relaxed);
        self.ops.fetch_add(ops.len(), Ordering::Relaxed);
        self.inner.submit_reads(ops)
    }

    fn submit_writes_detailed(&self, ops: &[StripWrite<'_>]) -> Vec<ChunkletResult<()>> {
        self.inner.submit_writes_detailed(ops)
    }
}

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
fn raid6_full_stripe_round_trip() {
    let dir = TempDir::new().unwrap();
    let (pool, _) = make_pool(&dir, 5);
    let id = pool.create_ld(LdSpec::raid6(3, 1, 1, 0)).unwrap();
    let ld = pool.open_ld(id).unwrap();
    let chunklet_user =
        onyx_chunklet::types::CHUNKLET_SIZE - onyx_chunklet::types::CHUNKLET_HEADER_BYTES;
    assert_eq!(
        ld.capacity_bytes(),
        3 * (chunklet_user / BLOCK_SIZE) * BLOCK_SIZE
    );
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
fn raid6_read_many_batches_non_crossing_substrip_ranges() {
    let dir = TempDir::new().unwrap();
    let (pool, _) = make_pool(&dir, 5);
    let id = pool.create_ld(LdSpec::raid6(3, 1, 1, 16)).unwrap();
    let ld = pool.open_ld(id).unwrap();
    let block = BLOCK_SIZE as usize;
    let strip = 16 * block;
    let full_stripe = 3 * strip;
    let payload: Vec<u8> = (0..full_stripe)
        .map(|i| ((i * 31 + 7) % 251) as u8)
        .collect();
    ld.write_at(0, &payload).unwrap();

    let inner = pool.pd(pool.list_pds()[0].pd_id).unwrap().backend();
    let counting = Arc::new(CountingReadBackend::new(inner));
    for info in pool.list_pds() {
        pool.pd(info.pd_id).unwrap().set_backend(counting.clone());
    }

    let offsets = [
        block as u64,
        (strip + 2 * block) as u64,
        (3 * strip - block) as u64,
    ];
    let mut bufs = [vec![0u8; block], vec![0u8; block], vec![0u8; block]];
    let mut ops: Vec<(u64, &mut [u8])> = offsets
        .iter()
        .copied()
        .zip(bufs.iter_mut().map(Vec::as_mut_slice))
        .collect();
    ld.read_many_at(&mut ops).unwrap();
    drop(ops);

    for (&offset, got) in offsets.iter().zip(&bufs) {
        assert_eq!(got, &payload[offset as usize..offset as usize + block]);
    }
    assert_eq!(counting.submits.load(Ordering::Relaxed), 1);
    assert_eq!(counting.ops.load(Ordering::Relaxed), offsets.len());
}

#[test]
fn raid6_read_many_substrip_runtime_fault_reconstructs_range() {
    let dir = TempDir::new().unwrap();
    let (pool, _) = make_pool(&dir, 5);
    let id = pool.create_ld(LdSpec::raid6(3, 1, 1, 16)).unwrap();
    let ld = pool.open_ld(id).unwrap();
    let block = BLOCK_SIZE as usize;
    let strip = 16 * block;
    let payload: Vec<u8> = (0..3 * strip)
        .map(|i| ((i * 17 + 11) % 251) as u8)
        .collect();
    ld.write_at(0, &payload).unwrap();
    drop(ld);

    let desc = pool.find_ld(id).unwrap();
    pool.pd(desc.members[0].pd).unwrap().set_read_faulting(true);
    let r6 = LdRaid6::open(desc, &pds_map(&pool)).unwrap();

    let offsets = [2 * block as u64, (strip + 5 * block) as u64];
    let mut bufs = [vec![0u8; block], vec![0u8; block]];
    let mut ops: Vec<(u64, &mut [u8])> = offsets
        .iter()
        .copied()
        .zip(bufs.iter_mut().map(Vec::as_mut_slice))
        .collect();
    r6.read_many_at(&mut ops).unwrap();
    drop(ops);

    for (&offset, got) in offsets.iter().zip(&bufs) {
        assert_eq!(got, &payload[offset as usize..offset as usize + block]);
    }
}

#[test]
fn raid6_read_many_substrip_two_data_failures_reconstruct_via_pq() {
    let dir = TempDir::new().unwrap();
    let (pool, _) = make_pool(&dir, 5);
    let id = pool.create_ld(LdSpec::raid6(3, 1, 1, 16)).unwrap();
    let ld = pool.open_ld(id).unwrap();
    let block = BLOCK_SIZE as usize;
    let strip = 16 * block;
    let payload: Vec<u8> = (0..3 * strip)
        .map(|i| ((i * 19 + 13) % 251) as u8)
        .collect();
    ld.write_at(0, &payload).unwrap();
    drop(ld);

    let desc = pool.find_ld(id).unwrap();
    let mut pds = pds_map(&pool);
    pds.remove(&desc.members[0].pd);
    pool.pd(desc.members[1].pd).unwrap().set_read_faulting(true);
    let r6 = LdRaid6::open(desc, &pds).unwrap();

    let offset = (strip + 7 * block) as u64;
    let mut got = vec![0u8; block];
    let mut ops = [(offset, got.as_mut_slice())];
    r6.read_many_at(&mut ops).unwrap();
    assert_eq!(got, payload[offset as usize..offset as usize + block]);
}

#[test]
fn raid6_concurrent_disjoint_full_stripe_writes() {
    let dir = TempDir::new().unwrap();
    let (pool, _) = make_pool(&dir, 5);
    let id = pool.create_ld(LdSpec::raid6(3, 1, 1, 0)).unwrap();
    let stripe = 3 * BLOCK_SIZE as usize;

    let handles: Vec<_> = (0..4)
        .map(|i| {
            let ld = pool.open_ld(id).unwrap();
            thread::spawn(move || {
                let offset = (i * stripe) as u64;
                for round in 0..64 {
                    let fill = (0x51 + i as u8).wrapping_add(round as u8);
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
        let fill = (0x51 + i as u8).wrapping_add(63);
        let mut readback = vec![0u8; stripe];
        ld.read_at((i * stripe) as u64, &mut readback).unwrap();
        assert!(readback.iter().all(|&b| b == fill), "stripe {}", i);
    }
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
    p_pd.read_chunklet_user(p_member.chunklet_index, 0, &mut p_buf)
        .unwrap();
    q_pd.read_chunklet_user(q_member.chunklet_index, 0, &mut q_buf)
        .unwrap();

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
    p_pd.read_chunklet_user(p_member.chunklet_index, 0, &mut p_buf)
        .unwrap();
    q_pd.read_chunklet_user(q_member.chunklet_index, 0, &mut q_buf)
        .unwrap();

    let expected_p = 0x11u8 ^ 0x99 ^ 0x33;
    use onyx_chunklet::ld::gf256::{g_pow, mul};
    let expected_q = mul(g_pow(0), 0x11) ^ mul(g_pow(1), 0x99) ^ mul(g_pow(2), 0x33);
    assert!(p_buf.iter().all(|&b| b == expected_p));
    assert!(q_buf.iter().all(|&b| b == expected_q));
}

#[test]
fn raid6_wide_partial_uses_parity_delta_and_updates_pq() {
    let dir = TempDir::new().unwrap();
    let (pool, _) = make_pool(&dir, 8);
    let id = pool.create_ld(LdSpec::raid6(6, 1, 1, 0)).unwrap();
    let ld = pool.open_ld(id).unwrap();

    let strip = BLOCK_SIZE as usize;
    let k = 6usize;
    let mut payload = vec![0u8; k * strip];
    for pos in 0..k {
        payload[pos * strip..(pos + 1) * strip].fill(0x10 + pos as u8);
    }
    ld.write_at(0, &payload).unwrap();

    let new_d4 = vec![0xebu8; strip];
    ld.write_at((4 * strip) as u64, &new_d4).unwrap();

    let mut readback = vec![0u8; k * strip];
    ld.read_at(0, &mut readback).unwrap();
    for pos in 0..k {
        let expected = if pos == 4 { 0xeb } else { 0x10 + pos as u8 };
        assert!(
            readback[pos * strip..(pos + 1) * strip]
                .iter()
                .all(|&b| b == expected),
            "data position {} drifted",
            pos
        );
    }

    let desc = pool.find_ld(id).unwrap();
    let p_member = &desc.members[k];
    let q_member = &desc.members[k + 1];
    let p_pd = pool.pd(p_member.pd).unwrap();
    let q_pd = pool.pd(q_member.pd).unwrap();
    let mut p_buf = vec![0u8; strip];
    let mut q_buf = vec![0u8; strip];
    p_pd.read_chunklet_user(p_member.chunklet_index, 0, &mut p_buf)
        .unwrap();
    q_pd.read_chunklet_user(q_member.chunklet_index, 0, &mut q_buf)
        .unwrap();

    use onyx_chunklet::ld::gf256::{g_pow, mul};
    let mut expected_p = 0u8;
    let mut expected_q = 0u8;
    for pos in 0..k {
        let data = if pos == 4 { 0xeb } else { 0x10 + pos as u8 };
        expected_p ^= data;
        expected_q ^= mul(g_pow(pos), data);
    }
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

/// Reproduce onyx's EXACT LV3 geometry (raid6 6 data + 2 parity, row_size=1,
/// num_rows=12, 4 KiB strip) and verify a full write -> drop pool -> reopen ->
/// read round-trips across the WHOLE linear space. `raid6_persists_across_reopen`
/// only covers num_rows=1; onyx runs num_rows=12. If the descriptor encode/decode
/// does not preserve the per-row (PD, chunklet_index) mapping, a post-reopen read
/// at a given LD offset resolves to the WRONG physical chunklet and returns stale
/// bytes -- exactly the "clean before restart, corrupt after restart" signature.
#[test]
fn raid6_multirow_reopen_onyx_geometry() {
    let dir = TempDir::new().unwrap();
    // 9 PDs matching onyx's LV3 pool. num_rows=12 needs 96 chunklets (96 GiB
    // raw), so the default 4 GiB test PDs are too small -> use 16 GiB each
    // (9 * 16 = 144 GiB, > 96 chunklets with margin). Sparse files: only the
    // ~72 MiB actually written allocates.
    const PD_BIG: u64 = 16 * 1024 * 1024 * 1024;
    let mut raws = Vec::new();
    let mut paths = Vec::new();
    for i in 0..9 {
        let p = dir.path().join(format!("pd{}", i));
        raws.push(RawDevice::open_or_create(&p, PD_BIG).unwrap());
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
    // strip_size_log2 = 12 -> 4 KiB strip == block size, like onyx.
    let id = pool.create_ld(LdSpec::raid6(6, 1, 12, 12)).unwrap();
    let ld = pool.open_ld(id).unwrap();
    let bs = ld.block_size();
    let cap = ld.capacity_bytes();
    let stripe = 6 * bs; // full-stripe data = 6 data strips * 4 KiB = 24 KiB
    let n_stripes = cap / stripe as u64;
    assert!(
        n_stripes > 12,
        "expected many stripes across 12 rows, got {n_stripes}"
    );

    // Unique per-stripe pattern so a misplaced read is detectable.
    let pat = |s: u64| -> Vec<u8> {
        let mut b = vec![0u8; stripe];
        let seed = s.wrapping_mul(0x9E37_79B9_7F4A_7C15).wrapping_add(1);
        for (i, x) in b.iter_mut().enumerate() {
            *x = ((seed >> ((i % 8) * 8)) as u8) ^ (i as u8) ^ (s as u8);
        }
        b
    };

    // Sample stripes evenly across the entire capacity (covers all 12 rows).
    // Full 72 GiB on sparse files is too heavy; a stride sample still catches
    // any per-row mapping shift.
    let n_samples: u64 = 3000;
    let step = (n_stripes / n_samples).max(1);
    let samples: Vec<u64> = (0..n_stripes).step_by(step as usize).collect();

    for &s in &samples {
        ld.write_at(s * stripe as u64, &pat(s)).unwrap();
    }
    drop((ld, pool));

    let pool2 = open_pool(&paths);
    let ld2 = pool2.open_ld(id).unwrap();
    let mut mismatches = 0u64;
    let mut first_bad: Option<u64> = None;
    for &s in &samples {
        let mut rb = vec![0u8; stripe];
        ld2.read_at(s * stripe as u64, &mut rb).unwrap();
        if rb != pat(s) {
            mismatches += 1;
            if first_bad.is_none() {
                first_bad = Some(s);
            }
        }
    }
    assert_eq!(
        mismatches,
        0,
        "{}/{} sampled stripes mismatched after reopen (first bad stripe={:?}); \
         descriptor round-trip corrupts the offset->chunklet mapping at num_rows=12",
        mismatches,
        samples.len(),
        first_bad
    );
}

/// Like `raid6_multirow_reopen_onyx_geometry` but adds onyx's real write mix:
/// full-stripe writes FOLLOWED by partial-stripe RMW overwrites of a single
/// data strip (the hybrid-pack "multiple sub-PBAs share one stripe, some later
/// overwritten" case), then reopen and verify EVERY strip -- overwritten strips
/// read the new bytes, untouched neighbors in the same stripe read the old
/// full-stripe bytes. A partial-RMW/neighbor/parity bug that only surfaces after
/// reopen would show here.
#[test]
fn raid6_multirow_reopen_partial_overwrite() {
    let dir = TempDir::new().unwrap();
    const PD_BIG: u64 = 16 * 1024 * 1024 * 1024;
    let mut raws = Vec::new();
    let mut paths = Vec::new();
    for i in 0..9 {
        let p = dir.path().join(format!("pd{}", i));
        raws.push(RawDevice::open_or_create(&p, PD_BIG).unwrap());
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
    let id = pool.create_ld(LdSpec::raid6(6, 1, 12, 12)).unwrap();
    let ld = pool.open_ld(id).unwrap();
    let bs = ld.block_size();
    let stripe = 6 * bs;
    let n_stripes = ld.capacity_bytes() / stripe as u64;

    // Per-(stripe, strip) base pattern.
    let base = |s: u64, j: usize| -> Vec<u8> {
        let mut b = vec![0u8; bs];
        let seed = s.wrapping_mul(0x100_0000_01B3).wrapping_add(j as u64 + 1);
        for (i, x) in b.iter_mut().enumerate() {
            *x = ((seed >> ((i % 8) * 8)) as u8) ^ (i as u8) ^ (j as u8);
        }
        b
    };
    // Overwrite pattern for the middle data strip (strip 3).
    let over = |s: u64| -> Vec<u8> {
        let mut b = vec![0u8; bs];
        let seed = s.wrapping_mul(0xDEAD_BEEF_1234_5678).wrapping_add(7);
        for (i, x) in b.iter_mut().enumerate() {
            *x = ((seed >> ((i % 8) * 8)) as u8) ^ (0xA5 ^ i as u8);
        }
        b
    };

    let n_samples: u64 = 3000;
    let step = (n_stripes / n_samples).max(1);
    let samples: Vec<u64> = (0..n_stripes).step_by(step as usize).collect();
    const OVER_STRIP: usize = 3;

    // Phase 1: full-stripe writes.
    for &s in &samples {
        let mut full = vec![0u8; stripe];
        for j in 0..6 {
            full[j * bs..(j + 1) * bs].copy_from_slice(&base(s, j));
        }
        ld.write_at(s * stripe as u64, &full).unwrap();
    }
    // Phase 2: partial-RMW overwrite of strip 3 on every other sample.
    for (idx, &s) in samples.iter().enumerate() {
        if idx % 2 == 0 {
            ld.write_at(s * stripe as u64 + (OVER_STRIP * bs) as u64, &over(s))
                .unwrap();
        }
    }
    drop((ld, pool));

    let pool2 = open_pool(&paths);
    let ld2 = pool2.open_ld(id).unwrap();
    let mut mism = 0u64;
    let mut first_bad: Option<(u64, usize)> = None;
    for (idx, &s) in samples.iter().enumerate() {
        let overwritten = idx % 2 == 0;
        for j in 0..6usize {
            let mut rb = vec![0u8; bs];
            ld2.read_at(s * stripe as u64 + (j * bs) as u64, &mut rb)
                .unwrap();
            let expect = if overwritten && j == OVER_STRIP {
                over(s)
            } else {
                base(s, j)
            };
            if rb != expect {
                mism += 1;
                if first_bad.is_none() {
                    first_bad = Some((s, j));
                }
            }
        }
    }
    assert_eq!(
        mism, 0,
        "{} strip reads mismatched after reopen (first bad (stripe,strip)={:?}); \
         partial-RMW overwrite + reopen corrupts data at num_rows=12",
        mism, first_bad
    );
}

#[test]
fn raid6_drops_5_chunklets() {
    let dir = TempDir::new().unwrap();
    let (pool, _) = make_pool(&dir, 5);
    let id = pool.create_ld(LdSpec::raid6(3, 1, 1, 0)).unwrap();
    let mut total = 0u32;
    for info in pool.list_pds() {
        total += pool
            .pd(info.pd_id)
            .unwrap()
            .snapshot()
            .1
            .count(ChunkletState::Used);
    }
    assert_eq!(total, 5);
    pool.drop_ld(id).unwrap();
    let mut total2 = 0u32;
    for info in pool.list_pds() {
        total2 += pool
            .pd(info.pd_id)
            .unwrap()
            .snapshot()
            .1
            .count(ChunkletState::Used);
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

#[test]
fn raid6_rejects_invalid_strip_size_log2() {
    let dir = TempDir::new().unwrap();
    let (pool, _) = make_pool(&dir, 5);
    for bad in [1, 11, 63, 64] {
        let err = pool.create_ld(LdSpec::raid6(3, 1, 1, bad)).err().unwrap();
        assert!(format!("{}", err).contains("strip_size_log2"), "{}", err);
    }
}

/// Regression for the partial-write offset bug: when `strip_size > BLOCK_SIZE`
/// AND a write spans `>= 2` data positions starting at a sub-strip-aligned
/// offset, the buggy code uses one `in_chunklet_off` for every position and
/// for both parities, corrupting pos[1+] data and the parity strips.
#[test]
fn raid6_partial_rmw_strip_gt_block_spans_positions() {
    let dir = TempDir::new().unwrap();
    let (pool, _) = make_pool(&dir, 5);
    // K=3, strip_size_log2=16 -> 64 KiB strip, full stripe = 192 KiB.
    let id = pool.create_ld(LdSpec::raid6(3, 1, 1, 16)).unwrap();
    let ld = pool.open_ld(id).unwrap();

    let bs = BLOCK_SIZE as usize;
    let strip = 1usize << 16;
    let full_stripe = 3 * strip;

    let mut payload = vec![0u8; full_stripe];
    for i in 0..(full_stripe / bs) {
        payload[i * bs..(i + 1) * bs].fill((i as u32 & 0xff) as u8);
    }
    ld.write_at(0, &payload).unwrap();

    // Write 8 KiB at LD offset (strip - bs) = 60 KiB. Spans pos[0]'s last
    // block + pos[1]'s first block.
    let new = vec![0xa5u8; 2 * bs];
    let off = (strip - bs) as u64;
    payload[off as usize..off as usize + 2 * bs].copy_from_slice(&new);
    ld.write_at(off, &new).unwrap();

    let mut readback = vec![0u8; full_stripe];
    ld.read_at(0, &mut readback).unwrap();
    assert_eq!(
        readback, payload,
        "R6 partial RMW spanning pos[0]->pos[1] at sub-strip offset corrupts data"
    );

    // Verify P = XOR(D_i) and Q = sum(g^i * D_i) are correct.
    use onyx_chunklet::ld::gf256;
    let desc = pool.list_lds().into_iter().next().unwrap();
    let pds: Vec<_> = (0..5)
        .map(|i| pool.pd(desc.members[i].pd).unwrap())
        .collect();
    let mut data = vec![vec![0u8; strip]; 3];
    for i in 0..3 {
        pds[i]
            .read_chunklet_user(desc.members[i].chunklet_index, 0, &mut data[i])
            .unwrap();
    }
    let mut expected_p = vec![0u8; strip];
    let mut expected_q = vec![0u8; strip];
    for i in 0..3 {
        for j in 0..strip {
            expected_p[j] ^= data[i][j];
        }
        gf256::mul_xor_into(&mut expected_q, &data[i], gf256::g_pow(i));
    }
    let mut p = vec![0u8; strip];
    let mut q = vec![0u8; strip];
    pds[3]
        .read_chunklet_user(desc.members[3].chunklet_index, 0, &mut p)
        .unwrap();
    pds[4]
        .read_chunklet_user(desc.members[4].chunklet_index, 0, &mut q)
        .unwrap();
    assert_eq!(p, expected_p, "P parity drifted after R6 partial RMW");
    assert_eq!(q, expected_q, "Q parity drifted after R6 partial RMW");
}

/// R6 sibling of `raid5_partial_rmw_starts_at_pos1_sub_strip`: write spanning
/// pos[1]->pos[2] at sub-strip offset.
#[test]
fn raid6_partial_rmw_starts_at_pos1_sub_strip() {
    let dir = TempDir::new().unwrap();
    let (pool, _) = make_pool(&dir, 5);
    let id = pool.create_ld(LdSpec::raid6(3, 1, 1, 16)).unwrap();
    let ld = pool.open_ld(id).unwrap();

    let bs = BLOCK_SIZE as usize;
    let strip = 1usize << 16;
    let full_stripe = 3 * strip;

    let mut payload = vec![0u8; full_stripe];
    for i in 0..(full_stripe / bs) {
        payload[i * bs..(i + 1) * bs].fill((i as u32 & 0xff) as u8);
    }
    ld.write_at(0, &payload).unwrap();

    let new = vec![0x77u8; 2 * bs];
    let off = (strip + strip - bs) as u64;
    payload[off as usize..off as usize + 2 * bs].copy_from_slice(&new);
    ld.write_at(off, &new).unwrap();

    let mut readback = vec![0u8; full_stripe];
    ld.read_at(0, &mut readback).unwrap();
    assert_eq!(
        readback, payload,
        "R6 partial RMW spanning pos[1]->pos[2] at sub-strip offset corrupts data"
    );
}

// ---- batched write_many_at (the flusher hot path) --------------------------
//
// write_many_at now collapses every op's stripe segments into ONE batched
// read submit + ONE batched write submit (healthy disjoint stripes), instead
// of the trait-default serial per-op loop. These assert the batched path
// recomputes P/Q correctly across Full / PDW / RW segments, and that it bails
// to the serial path for degraded sets + intra-batch stripe collisions.

/// Read the raw D0..D(K-1), P, Q strips of one healthy set stripe and assert
/// the on-disk parity matches the Anvin formula over the on-disk data.
fn assert_r6_stripe_parity(
    pool: &Arc<Pool>,
    id: onyx_chunklet::LdId,
    in_chunklet_off: u64,
    strip: usize,
) {
    use onyx_chunklet::ld::gf256::{g_pow, mul};
    let desc = pool.find_ld(id).unwrap();
    let k = (desc.set_size - 2) as usize;
    let mut data = vec![vec![0u8; strip]; k];
    for pos in 0..k {
        let mm = &desc.members[pos];
        pool.pd(mm.pd)
            .unwrap()
            .read_chunklet_user(mm.chunklet_index, in_chunklet_off, &mut data[pos])
            .unwrap();
    }
    let pm = &desc.members[k];
    let qm = &desc.members[k + 1];
    let mut p_buf = vec![0u8; strip];
    let mut q_buf = vec![0u8; strip];
    pool.pd(pm.pd)
        .unwrap()
        .read_chunklet_user(pm.chunklet_index, in_chunklet_off, &mut p_buf)
        .unwrap();
    pool.pd(qm.pd)
        .unwrap()
        .read_chunklet_user(qm.chunklet_index, in_chunklet_off, &mut q_buf)
        .unwrap();
    let mut exp_p = vec![0u8; strip];
    let mut exp_q = vec![0u8; strip];
    for pos in 0..k {
        for i in 0..strip {
            exp_p[i] ^= data[pos][i];
            exp_q[i] ^= mul(g_pow(pos), data[pos][i]);
        }
    }
    assert_eq!(p_buf, exp_p, "P mismatch at off {}", in_chunklet_off);
    assert_eq!(q_buf, exp_q, "Q mismatch at off {}", in_chunklet_off);
}

#[test]
fn raid6_write_many_batched_full_and_partial() {
    let dir = TempDir::new().unwrap();
    let (pool, _) = make_pool(&dir, 5);
    let id = pool.create_ld(LdSpec::raid6(3, 1, 1, 0)).unwrap();
    let ld = pool.open_ld(id).unwrap();
    let strip = BLOCK_SIZE as usize;
    let fs = 3 * strip;

    let full: Vec<u8> = (0..fs).map(|i| ((i * 7 + 1) % 251) as u8).collect(); // stripe 0, Full
    let one: Vec<u8> = vec![0xA5u8; strip]; // stripe 4 / D1 only (M=1 full-strip → RW)
    let two: Vec<u8> = (0..2 * strip).map(|i| ((i * 13 + 5) % 251) as u8).collect(); // stripe 7 / D0+D1 (RW)

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
    assert_r6_stripe_parity(&pool, id, 0, strip);
    assert_r6_stripe_parity(&pool, id, 4 * strip as u64, strip);
    assert_r6_stripe_parity(&pool, id, 7 * strip as u64, strip);
}

#[test]
fn raid6_write_many_batched_pdw_substrip() {
    let dir = TempDir::new().unwrap();
    let (pool, _) = make_pool(&dir, 5);
    let id = pool.create_ld(LdSpec::raid6(3, 1, 1, 13)).unwrap(); // strip = 2 blocks
    let ld = pool.open_ld(id).unwrap();
    let strip = 2 * BLOCK_SIZE as usize;
    let fs = 3 * strip;

    // Seed stripe 2 full so the sub-strip overwrite has real old data + parity.
    let seed: Vec<u8> = (0..fs).map(|i| ((i * 11 + 2) % 251) as u8).collect();
    ld.write_at((2 * fs) as u64, &seed).unwrap();

    // Sub-strip overwrite of D0's first block (len < strip) → PDW path.
    let sub: Vec<u8> = vec![0x5Au8; BLOCK_SIZE as usize];
    ld.write_many_at(&[((2 * fs) as u64, sub.as_slice())])
        .unwrap();

    let mut expect = seed.clone();
    expect[0..BLOCK_SIZE as usize].copy_from_slice(&sub);
    let mut rb = vec![0u8; fs];
    ld.read_at((2 * fs) as u64, &mut rb).unwrap();
    assert_eq!(rb, expect);

    drop(ld);
    assert_r6_stripe_parity(&pool, id, 2 * strip as u64, strip);
}

#[test]
fn raid6_write_many_batched_duplicate_stripe_serializes() {
    let dir = TempDir::new().unwrap();
    let (pool, _) = make_pool(&dir, 5);
    let id = pool.create_ld(LdSpec::raid6(3, 1, 1, 0)).unwrap();
    let ld = pool.open_ld(id).unwrap();
    let strip = BLOCK_SIZE as usize;
    let fs = 3 * strip;

    // Two ops on the SAME stripe → has_duplicate_keys → serial fallback, last wins.
    let a = vec![0x11u8; fs];
    let b = vec![0x22u8; fs];
    ld.write_many_at(&[(0u64, a.as_slice()), (0u64, b.as_slice())])
        .unwrap();

    let mut rb = vec![0u8; fs];
    ld.read_at(0, &mut rb).unwrap();
    assert_eq!(rb, b);
    drop(ld);
    assert_r6_stripe_parity(&pool, id, 0, strip);
}

#[test]
fn raid6_write_many_batched_degraded_falls_back() {
    let dir = TempDir::new().unwrap();
    let (pool, _) = make_pool(&dir, 5);
    let id = pool.create_ld(LdSpec::raid6(3, 1, 1, 0)).unwrap();
    let ld = pool.open_ld(id).unwrap();
    let strip = BLOCK_SIZE as usize;
    let fs = 3 * strip;
    let seed: Vec<u8> = (0..2 * fs).map(|i| ((i * 7 + 9) % 251) as u8).collect();
    ld.write_at(0, &seed).unwrap();
    drop(ld);

    // Reopen with D0's PD absent → degraded set; batched writes must bail to
    // the serial reconstruct-write path.
    let desc = pool.find_ld(id).unwrap();
    let d0_pd = desc.members[0].pd;
    let mut pds = pds_map(&pool);
    pds.remove(&d0_pd);
    let r6 = LdRaid6::open(pool.find_ld(id).unwrap(), &pds).unwrap();

    let w0 = vec![0x33u8; fs]; // full stripe 0
    let w1 = vec![0x44u8; strip]; // stripe 1 / D1
    r6.write_many_at(&[(0u64, w0.as_slice()), ((fs + strip) as u64, w1.as_slice())])
        .unwrap();

    // Degraded reads reconstruct D0 via P/Q — proves parity was written right.
    let mut rb0 = vec![0u8; fs];
    r6.read_at(0, &mut rb0).unwrap();
    assert_eq!(rb0, w0);
    let mut rb1 = vec![0u8; strip];
    r6.read_at((fs + strip) as u64, &mut rb1).unwrap();
    assert_eq!(rb1, w1);
}

#[test]
fn raid6_write_many_batched_same_stripe_merge_to_full() {
    // Two DISJOINT ops that together fill stripe 0 (D0+D1 from op A, D2 from op
    // B) collide on one stripe within a single write_many_at. They must MERGE
    // into a zero-RMW full-stripe write, not bail to serial.
    let dir = TempDir::new().unwrap();
    let (pool, _) = make_pool(&dir, 5);
    let id = pool.create_ld(LdSpec::raid6(3, 1, 1, 0)).unwrap();
    let ld = pool.open_ld(id).unwrap();
    let strip = BLOCK_SIZE as usize;

    let a: Vec<u8> = (0..2 * strip).map(|i| ((i * 7 + 1) % 251) as u8).collect();
    let b = vec![0xC7u8; strip];
    ld.write_many_at(&[(0u64, a.as_slice()), ((2 * strip) as u64, b.as_slice())])
        .unwrap();

    let mut ra = vec![0u8; 2 * strip];
    ld.read_at(0, &mut ra).unwrap();
    assert_eq!(ra, a);
    let mut rb = vec![0u8; strip];
    ld.read_at((2 * strip) as u64, &mut rb).unwrap();
    assert_eq!(rb, b);

    drop(ld);
    assert_r6_stripe_parity(&pool, id, 0, strip);
}

#[test]
fn raid6_write_many_batched_same_stripe_merge_partial_preserves_untouched() {
    // Two ops touch D0 and D2 of stripe 0 in one write_many_at, leaving D1.
    // The merge must stay partial (read + preserve D1), never promote to full.
    let dir = TempDir::new().unwrap();
    let (pool, _) = make_pool(&dir, 5);
    let id = pool.create_ld(LdSpec::raid6(3, 1, 1, 0)).unwrap();
    let ld = pool.open_ld(id).unwrap();
    let strip = BLOCK_SIZE as usize;

    let seed: Vec<u8> = (0..3 * strip).map(|i| ((i * 5 + 3) % 251) as u8).collect();
    ld.write_at(0, &seed).unwrap();

    let d0 = vec![0x11u8; strip];
    let d2 = vec![0x33u8; strip];
    ld.write_many_at(&[(0u64, d0.as_slice()), ((2 * strip) as u64, d2.as_slice())])
        .unwrap();

    let mut rb = vec![0u8; 3 * strip];
    ld.read_at(0, &mut rb).unwrap();
    assert!(rb[0..strip].iter().all(|&x| x == 0x11), "D0 overwritten");
    assert_eq!(
        &rb[strip..2 * strip],
        &seed[strip..2 * strip],
        "D1 preserved"
    );
    assert!(
        rb[2 * strip..3 * strip].iter().all(|&x| x == 0x33),
        "D2 overwritten"
    );

    drop(ld);
    assert_r6_stripe_parity(&pool, id, 0, strip);
}
