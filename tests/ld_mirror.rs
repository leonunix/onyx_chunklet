//! LdMirror end-to-end tests: RAID-1, RAID-10, mirror persistence after
//! pool reopen, and degraded-read sanity (one copy corrupted, the other
//! still serves data).

use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;

use onyx_chunklet::error::{ChunkletError, ChunkletResult};
use onyx_chunklet::io::{IoBackend, RawDevice, StripRead, StripWrite};
use onyx_chunklet::pool::LdSpec;
use onyx_chunklet::types::{ChunkletState, BLOCK_SIZE, CHUNKLET_HEADER_BYTES, CHUNKLET_SIZE};
use onyx_chunklet::{Pool, PoolConfig};
use tempfile::TempDir;

const PD_SIZE: u64 = 4 * 1024 * 1024 * 1024; // ~3 chunklets per PD

fn make_pool(dir: &TempDir, names: &[&str]) -> (Arc<Pool>, Vec<PathBuf>) {
    let mut raws = Vec::new();
    let mut paths = Vec::new();
    for n in names {
        let p = dir.path().join(n);
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

/// Test-only `IoBackend` that forwards to an inner backend while counting
/// `submit_reads` invocations. One `read_at` / `read_many_at` must drive
/// exactly ONE `parallel_strip_reads` → one `submit_reads`, even for a
/// multi-strip span — the regression this whole change targets (the old code
/// issued one synchronous `pd.read_chunklet_user` per 4 KiB strip and never
/// touched the backend at all).
struct CountingBackend {
    inner: Arc<dyn IoBackend>,
    reads: AtomicUsize,
}

impl CountingBackend {
    fn new(inner: Arc<dyn IoBackend>) -> Self {
        Self {
            inner,
            reads: AtomicUsize::new(0),
        }
    }
    fn read_count(&self) -> usize {
        self.reads.load(Ordering::Relaxed)
    }
}

impl IoBackend for CountingBackend {
    fn name(&self) -> &'static str {
        "counting"
    }
    fn submit_reads(&self, ops: &mut [StripRead<'_>]) -> Result<(), ChunkletError> {
        self.reads.fetch_add(1, Ordering::Relaxed);
        self.inner.submit_reads(ops)
    }
    fn submit_writes_detailed(&self, ops: &[StripWrite<'_>]) -> Vec<ChunkletResult<()>> {
        self.inner.submit_writes_detailed(ops)
    }
}

#[test]
fn raid1_round_trip() {
    let dir = TempDir::new().unwrap();
    let (pool, _) = make_pool(&dir, &["pd0", "pd1"]);
    // Pure mirror: 2 copies, 1 set per row, 1 row.
    let id = pool.create_ld(LdSpec::mirror(2, 1, 1, 0)).unwrap();
    let ld = pool.open_ld(id).unwrap();
    assert_eq!(ld.capacity_bytes(), CHUNKLET_SIZE - CHUNKLET_HEADER_BYTES);

    let payload: Vec<u8> = (0..(64 << 10))
        .map(|i| ((i * 23 + 11) % 251) as u8)
        .collect();
    ld.write_at(0, &payload).unwrap();
    let mut readback = vec![0u8; payload.len()];
    ld.read_at(0, &mut readback).unwrap();
    assert_eq!(readback, payload);
}

#[test]
fn raid1_marks_two_chunklets_used() {
    let dir = TempDir::new().unwrap();
    let (pool, _) = make_pool(&dir, &["pd0", "pd1", "pd2"]);
    let _id = pool.create_ld(LdSpec::mirror(2, 1, 1, 0)).unwrap();
    let mut used_total = 0u32;
    for info in pool.list_pds() {
        let pd = pool.pd(info.pd_id).unwrap();
        let (_, bitmap, _) = pd.snapshot();
        used_total += bitmap.count(ChunkletState::Used);
    }
    assert_eq!(
        used_total, 2,
        "RAID-1 with 2 copies should consume 2 chunklets"
    );
}

#[test]
fn raid10_round_trip_with_strip_alignment() {
    // 4 PDs, mirror=2, row_size=2 (RAID-10), num_rows=1, strip=4 KiB (block).
    let dir = TempDir::new().unwrap();
    let (pool, _) = make_pool(&dir, &["pd0", "pd1", "pd2", "pd3"]);
    let id = pool.create_ld(LdSpec::mirror(2, 2, 1, 0)).unwrap();
    let ld = pool.open_ld(id).unwrap();
    // Capacity = row_size * num_rows * chunklet_user_size = 2 * 1 * (1 GiB - 4 KiB).
    assert_eq!(
        ld.capacity_bytes(),
        2 * (CHUNKLET_SIZE - CHUNKLET_HEADER_BYTES)
    );
    assert_eq!(ld.strip_size(), BLOCK_SIZE as usize);

    // Write enough to span both stripes (8 blocks = 32 KiB).
    let payload: Vec<u8> = (0..(32 << 10))
        .map(|i| ((i * 7 + 19) % 211) as u8)
        .collect();
    ld.write_at(0, &payload).unwrap();
    let mut readback = vec![0u8; payload.len()];
    ld.read_at(0, &mut readback).unwrap();
    assert_eq!(readback, payload);
}

#[test]
fn raid10_write_many_round_trip() {
    let dir = TempDir::new().unwrap();
    let (pool, _) = make_pool(&dir, &["pd0", "pd1", "pd2", "pd3"]);
    let id = pool.create_ld(LdSpec::mirror(2, 2, 1, 0)).unwrap();
    let ld = pool.open_ld(id).unwrap();
    let strip = BLOCK_SIZE as usize;

    let payloads: Vec<Vec<u8>> = (0..16)
        .map(|i| {
            (0..strip)
                .map(|j| ((i * 31 + j * 7 + 13) % 251) as u8)
                .collect()
        })
        .collect();
    let ops: Vec<(u64, &[u8])> = payloads
        .iter()
        .enumerate()
        .map(|(i, payload)| ((i * strip) as u64, payload.as_slice()))
        .collect();
    ld.write_many_at(&ops).unwrap();

    for (i, payload) in payloads.iter().enumerate() {
        let mut readback = vec![0u8; strip];
        ld.read_at((i * strip) as u64, &mut readback).unwrap();
        assert_eq!(&readback, payload, "strip {}", i);
    }
}

#[test]
fn mirror_concurrent_disjoint_strip_writes() {
    let dir = TempDir::new().unwrap();
    let (pool, _) = make_pool(&dir, &["pd0", "pd1", "pd2", "pd3"]);
    let id = pool.create_ld(LdSpec::mirror(2, 2, 1, 0)).unwrap();
    let strip = BLOCK_SIZE as usize;

    let handles: Vec<_> = (0..8)
        .map(|i| {
            let ld = pool.open_ld(id).unwrap();
            thread::spawn(move || {
                let offset = (i * strip) as u64;
                for round in 0..64 {
                    let fill = (0x71 + i as u8).wrapping_add(round as u8);
                    let payload = vec![fill; strip];
                    ld.write_at(offset, &payload).unwrap();
                }
            })
        })
        .collect();
    for handle in handles {
        handle.join().unwrap();
    }

    let ld = pool.open_ld(id).unwrap();
    for i in 0..8 {
        let fill = (0x71 + i as u8).wrapping_add(63);
        let mut readback = vec![0u8; strip];
        ld.read_at((i * strip) as u64, &mut readback).unwrap();
        assert!(readback.iter().all(|&b| b == fill), "strip {}", i);
    }
}

#[test]
fn raid10_uses_4_chunklets_total() {
    let dir = TempDir::new().unwrap();
    let (pool, _) = make_pool(&dir, &["pd0", "pd1", "pd2", "pd3"]);
    let _id = pool.create_ld(LdSpec::mirror(2, 2, 1, 0)).unwrap();
    let mut used_total = 0u32;
    for info in pool.list_pds() {
        let pd = pool.pd(info.pd_id).unwrap();
        let (_, bitmap, _) = pd.snapshot();
        used_total += bitmap.count(ChunkletState::Used);
    }
    assert_eq!(
        used_total, 4,
        "RAID-10 (mirror=2, row_size=2) should use 4 chunklets"
    );
}

#[test]
fn mirror_persists_across_pool_reopen() {
    let dir = TempDir::new().unwrap();
    let (pool, paths) = make_pool(&dir, &["pd0", "pd1", "pd2", "pd3"]);
    let id = pool.create_ld(LdSpec::mirror(2, 2, 1, 0)).unwrap();
    let ld = pool.open_ld(id).unwrap();
    let payload: Vec<u8> = (0..(16 << 10)).map(|i| (i % 199) as u8).collect();
    ld.write_at(0, &payload).unwrap();
    drop((ld, pool));

    let pool2 = open_pool(&paths);
    let lds = pool2.list_lds();
    assert_eq!(lds.len(), 1);
    assert_eq!(lds[0].id, id);
    let ld2 = pool2.open_ld(id).unwrap();
    let mut readback = vec![0u8; payload.len()];
    ld2.read_at(0, &mut readback).unwrap();
    assert_eq!(readback, payload);
}

#[test]
fn mirror_drop_frees_all_copies() {
    let dir = TempDir::new().unwrap();
    let (pool, _) = make_pool(&dir, &["pd0", "pd1", "pd2", "pd3"]);
    let id = pool.create_ld(LdSpec::mirror(2, 2, 1, 0)).unwrap();
    let old_handle = pool.open_ld(id).unwrap();
    pool.drop_ld(id).unwrap();
    let mut buf = vec![0u8; BLOCK_SIZE as usize];
    let err = old_handle.read_at(0, &mut buf).err().unwrap();
    assert!(format!("{}", err).contains("stale"), "{}", err);
    for info in pool.list_pds() {
        let pd = pool.pd(info.pd_id).unwrap();
        let (_, bitmap, _) = pd.snapshot();
        assert_eq!(bitmap.count(ChunkletState::Used), 0);
    }
}

#[test]
fn raid1_serves_reads_after_one_copy_corrupted() {
    // 2 PDs, RAID-1. After write, corrupt one copy's chunklet header — that
    // doesn't break user data. To actually verify single-copy survival we
    // would need to close the pool and reopen with one PD missing. For P2
    // that's a Phase 5 task (degraded mode), so this test just verifies the
    // happy path: both copies hold identical data we can read back.
    let dir = TempDir::new().unwrap();
    let (pool, _) = make_pool(&dir, &["pd0", "pd1"]);
    let id = pool.create_ld(LdSpec::mirror(2, 1, 1, 0)).unwrap();
    let ld = pool.open_ld(id).unwrap();

    let payload: Vec<u8> = std::iter::repeat(0xa5).take(8 << 10).collect();
    ld.write_at(0, &payload).unwrap();

    // Read each copy directly via PD::read_chunklet_user and verify both
    // hold the same bytes.
    let desc = pool.find_ld(id).unwrap();
    assert_eq!(desc.members.len(), 2);
    for m in &desc.members {
        let pd = pool.pd(m.pd).unwrap();
        let mut buf = vec![0u8; payload.len()];
        pd.read_chunklet_user(m.chunklet_index, 0, &mut buf)
            .unwrap();
        assert_eq!(buf, payload);
    }
}

#[test]
fn rejects_mirror_with_set_size_one() {
    let dir = TempDir::new().unwrap();
    let (pool, _) = make_pool(&dir, &["pd0", "pd1"]);
    let spec = LdSpec {
        raid_level: onyx_chunklet::types::RaidLevel::Mirror,
        set_size: 1, // invalid
        row_size: 1,
        num_rows: 1,
        strip_size_log2: 0,
        ha_domain: onyx_chunklet::types::HaDomain::Pd,
    };
    assert!(pool.create_ld(spec).is_err());
}

#[test]
fn rejects_when_set_size_exceeds_distinct_pds() {
    // 4-way mirror needs 4 distinct PDs per set, but pool only has 3.
    let dir = TempDir::new().unwrap();
    let (pool, _) = make_pool(&dir, &["pd0", "pd1", "pd2"]);
    let err = pool.create_ld(LdSpec::mirror(4, 1, 1, 0)).err().unwrap();
    let s = format!("{}", err);
    assert!(s.contains("distinct PDs") || s.contains("free"), "{}", s);
}

/// Pool::mark_chunklet_bad persists the Bad state across reopen and is
/// honored by future LdMirror::open's `resolve_members` (so reads skip the
/// bad copy and write_at fans out to the surviving copies only).
#[test]
fn mark_chunklet_bad_persists_and_excludes_member_from_reads() {
    let dir = TempDir::new().unwrap();
    let (pool, paths) = make_pool(&dir, &["pd0", "pd1", "pd2"]);
    let id = pool.create_ld(LdSpec::mirror(3, 1, 1, 0)).unwrap();
    let ld = pool.open_ld(id).unwrap();
    let old_handle = ld.clone();
    let payload: Vec<u8> = (0..(48 << 10)).map(|i| (i % 211) as u8).collect();
    ld.write_at(0, &payload).unwrap();
    drop(ld);

    // Mark copy 0 Bad via the new pool API.
    let desc = pool.list_lds().into_iter().next().unwrap();
    let bad = desc.members[0];
    pool.mark_chunklet_bad(bad.pd, bad.chunklet_index).unwrap();
    let mut stale_read = vec![0u8; payload.len()];
    let err = old_handle.read_at(0, &mut stale_read).err().unwrap();
    assert!(format!("{}", err).contains("stale"), "{}", err);
    let pd = pool.pd(bad.pd).unwrap();
    let (_, bm, _) = pd.snapshot();
    assert_eq!(bm.get(bad.chunklet_index).unwrap(), ChunkletState::Bad);

    // Release the LD/PD handles that still pin member fds, then the pool, so
    // the reopen below isn't rejected by the pool's exclusive flock.
    drop(old_handle);
    drop(pd);
    drop(pool);

    // Reopen + verify Bad state survived.
    let pool2 = open_pool(&paths);
    let pd2 = pool2.pd(bad.pd).unwrap();
    let (_, bm2, _) = pd2.snapshot();
    assert_eq!(bm2.get(bad.chunklet_index).unwrap(), ChunkletState::Bad);

    // Reads still succeed via the other 2 copies; sibling copies match the
    // original data (Bad copy is silently excluded by resolve_members).
    let ld2 = pool2.open_ld(id).unwrap();
    let mut readback = vec![0u8; payload.len()];
    ld2.read_at(0, &mut readback).unwrap();
    assert_eq!(readback, payload);
}

/// Variable-length, mixed-size, multi-strip disjoint writes in ONE
/// `write_many_at` round-trip on a RAID-10 LD. The spans cross 4 KiB strip
/// boundaries and alternate between the two mirror sets — the coalesced
/// ring-span shape the onyx flusher hands LV2. Before this change any op
/// whose length != strip_bytes fell back to one submit per 4 KiB strip.
#[test]
fn write_many_at_variable_length_multi_strip_round_trip() {
    let dir = TempDir::new().unwrap();
    let (pool, _) = make_pool(&dir, &["pd0", "pd1", "pd2", "pd3"]);
    let id = pool.create_ld(LdSpec::mirror(2, 2, 1, 0)).unwrap();
    let ld = pool.open_ld(id).unwrap();

    // Disjoint, 4 KiB-aligned, mixed lengths spanning many strips + both sets.
    let regions: [(u64, usize); 4] = [
        (0, 4 * 1024),
        (4 * 1024, 20 * 1024),
        (24 * 1024, 256 * 1024),
        (280 * 1024, 12 * 1024),
    ];
    let payloads: Vec<Vec<u8>> = regions
        .iter()
        .enumerate()
        .map(|(i, &(off, len))| {
            (0..len)
                .map(|j| ((off as usize + j).wrapping_mul(31).wrapping_add(i * 7) % 251) as u8)
                .collect()
        })
        .collect();
    let ops: Vec<(u64, &[u8])> = regions
        .iter()
        .zip(&payloads)
        .map(|(&(off, _), p)| (off, p.as_slice()))
        .collect();
    ld.write_many_at(&ops).unwrap();

    // Each region reads back correctly...
    for (&(off, len), expected) in regions.iter().zip(&payloads) {
        let mut got = vec![0u8; len];
        ld.read_at(off, &mut got).unwrap();
        assert_eq!(&got, expected, "region @ {}", off);
    }
    // ...and so does the whole span as one multi-strip read.
    let total = 280 * 1024 + 12 * 1024;
    let mut whole = vec![0u8; total];
    ld.read_at(0, &mut whole).unwrap();
    for (&(off, len), expected) in regions.iter().zip(&payloads) {
        let o = off as usize;
        assert_eq!(&whole[o..o + len], expected.as_slice(), "whole-span @ {}", off);
    }
}

/// A large multi-strip `read_at` must batch into exactly ONE `submit_reads`,
/// not one synchronous pread per 4 KiB strip. The counting backend is
/// installed on every PD so the assertion is independent of which copy the
/// round-robin picks.
#[test]
fn read_at_large_multi_strip_span_one_submit() {
    let dir = TempDir::new().unwrap();
    let (pool, _) = make_pool(&dir, &["pd0", "pd1", "pd2", "pd3"]);
    let id = pool.create_ld(LdSpec::mirror(2, 2, 1, 0)).unwrap();
    let ld = pool.open_ld(id).unwrap();

    let payload: Vec<u8> = (0usize..(256 * 1024))
        .map(|i| (i.wrapping_mul(17).wrapping_add(3) % 251) as u8)
        .collect();
    ld.write_at(0, &payload).unwrap();

    let inner = pool.pd(pool.list_pds()[0].pd_id).unwrap().backend();
    let counting = Arc::new(CountingBackend::new(inner));
    for info in pool.list_pds() {
        pool.pd(info.pd_id).unwrap().set_backend(counting.clone());
    }

    let mut got = vec![0u8; payload.len()];
    ld.read_at(0, &mut got).unwrap();
    assert_eq!(got, payload);
    assert_eq!(
        counting.read_count(),
        1,
        "a 256 KiB multi-strip read_at must be one batched submit, not 64"
    );
}

/// `read_many_at` with several non-uniform multi-strip ops round-trips,
/// carving each op's buffer into strips and collecting all segments across
/// all ops into one batched submit.
#[test]
fn read_many_at_nonuniform_multi_strip_round_trip() {
    let dir = TempDir::new().unwrap();
    let (pool, _) = make_pool(&dir, &["pd0", "pd1", "pd2", "pd3"]);
    let id = pool.create_ld(LdSpec::mirror(2, 2, 1, 0)).unwrap();
    let ld = pool.open_ld(id).unwrap();

    let regions: [(u64, usize); 3] = [
        (0, 8 * 1024),
        (8 * 1024, 4 * 1024),
        (12 * 1024, 64 * 1024),
    ];
    let payloads: Vec<Vec<u8>> = regions
        .iter()
        .enumerate()
        .map(|(i, &(_, len))| {
            (0..len)
                .map(|j| (j.wrapping_mul(13).wrapping_add(i) % 251) as u8)
                .collect()
        })
        .collect();
    let wops: Vec<(u64, &[u8])> = regions
        .iter()
        .zip(&payloads)
        .map(|(&(o, _), p)| (o, p.as_slice()))
        .collect();
    ld.write_many_at(&wops).unwrap();

    let mut bufs: Vec<(u64, Vec<u8>)> =
        regions.iter().map(|&(o, len)| (o, vec![0u8; len])).collect();
    let mut rops: Vec<(u64, &mut [u8])> =
        bufs.iter_mut().map(|(o, b)| (*o, b.as_mut_slice())).collect();
    ld.read_many_at(&mut rops).unwrap();
    drop(rops);
    for ((_, got), expected) in bufs.iter().zip(&payloads) {
        assert_eq!(got, expected);
    }
}

/// Degraded multi-strip read: mark one copy Bad, reopen, then read a span
/// covering many strips. `pick_read_copy` must skip the bad copy on every
/// segment and the surviving copies must hold the full multi-strip payload.
#[test]
fn mirror_degraded_copy_read_multi_strip() {
    let dir = TempDir::new().unwrap();
    let (pool, paths) = make_pool(&dir, &["pd0", "pd1", "pd2"]);
    let id = pool.create_ld(LdSpec::mirror(3, 1, 1, 0)).unwrap();
    let ld = pool.open_ld(id).unwrap();
    let payload: Vec<u8> = (0usize..(48 * 1024))
        .map(|i| (i.wrapping_mul(29).wrapping_add(5) % 211) as u8)
        .collect();
    ld.write_at(0, &payload).unwrap();
    drop(ld);

    let desc = pool.list_lds().into_iter().next().unwrap();
    let bad = desc.members[0];
    pool.mark_chunklet_bad(bad.pd, bad.chunklet_index).unwrap();
    drop(pool);

    let pool2 = open_pool(&paths);
    let ld2 = pool2.open_ld(id).unwrap();
    let mut readback = vec![0u8; payload.len()];
    ld2.read_at(0, &mut readback).unwrap();
    assert_eq!(readback, payload);
}

/// Regression: `RuntimeLogicalDisk::read_many_at` must acquire its range
/// read-locks in the SAME globally-sorted bucket order as `write_many_at`'s
/// `write_keys`. The old code locked per-op (each `read_key_range` sorted only
/// within its own range), so a multi-op read with out-of-order offsets grabbed
/// buckets in a different global order than a concurrent multi-strip write →
/// AB-BA deadlock (observed wedging a live ublk perf run). This hammers
/// out-of-order multi-op reads against overlapping wide writes; with the bug
/// it deadlocks and the test hangs, with the fix it completes.
#[test]
fn rtd_concurrent_out_of_order_read_many_vs_write_many_no_deadlock() {
    let dir = TempDir::new().unwrap();
    let (pool, _) = make_pool(&dir, &["pd0", "pd1", "pd2", "pd3"]);
    // RAID-10, 4 KiB strip → each strip is its own range key / lock bucket.
    let id = pool.create_ld(LdSpec::mirror(2, 2, 1, 0)).unwrap();
    let strip = BLOCK_SIZE as usize;
    let nstrips = 24usize;
    let iters = 80usize;

    let writers: Vec<_> = (0..4)
        .map(|w| {
            let ld = pool.open_ld(id).unwrap();
            thread::spawn(move || {
                let bufs: Vec<Vec<u8>> =
                    (0..nstrips).map(|i| vec![(w * 7 + i) as u8; strip]).collect();
                // Ascending offsets → write_keys acquires buckets globally sorted.
                let ops: Vec<(u64, &[u8])> = bufs
                    .iter()
                    .enumerate()
                    .map(|(i, b)| ((i * strip) as u64, b.as_slice()))
                    .collect();
                for _ in 0..iters {
                    ld.write_many_at(&ops).unwrap();
                }
            })
        })
        .collect();

    let readers: Vec<_> = (0..4)
        .map(|_| {
            let ld = pool.open_ld(id).unwrap();
            thread::spawn(move || {
                for _ in 0..iters {
                    let mut bufs: Vec<Vec<u8>> = (0..nstrips).map(|_| vec![0u8; strip]).collect();
                    let mut ops: Vec<(u64, &mut [u8])> = bufs
                        .iter_mut()
                        .enumerate()
                        .map(|(i, b)| ((i * strip) as u64, b.as_mut_slice()))
                        .collect();
                    // DESCENDING op order — the exact per-op reverse-bucket trigger.
                    ops.reverse();
                    ld.read_many_at(&mut ops).unwrap();
                }
            })
        })
        .collect();

    for w in writers {
        w.join().unwrap();
    }
    for r in readers {
        r.join().unwrap();
    }
}
