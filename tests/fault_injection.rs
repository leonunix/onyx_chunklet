//! `--ignored` fault-injection tests. These exercise partial-failure /
//! crash-mid-write code paths that are otherwise unreachable on a healthy
//! sparse-file harness. CLAUDE.md treats these as a phase gate rather
//! than optional — run with:
//!
//! ```bash
//! cargo test --release -- --ignored
//! ```

mod common;

use std::sync::Arc;

use onyx_chunklet::pool::LdSpec;
use onyx_chunklet::types::ChunkletState;
use tempfile::TempDir;

use common::fault::FaultInjectingBackend;
use common::{make_pool, open_subset, path_for_member, pattern};

/// Mirror write where one of N copies fails mid-batch must (post inline-degrade):
/// - RIDE THROUGH on the surviving copies (≥1 survivor → the write is durable),
///   returning `Ok` instead of surfacing the error — a single member EIO must not
///   fail a redundant write (this is the fix for the box META_FENCE bug).
/// - Report the failed copy as a suspect for fast isolation.
/// - Leave the surviving copies with the new data, the failed copy with the old
///   data (real torn state) — repaired later by scrub / rebuild.
///
/// 3-way mirror so the test can demonstrate post-failure recovery via
/// scrub_ld (which majority-votes) — the operator's recommended
/// remediation per `LdMirror` module doc.
#[test]
#[ignore = "fault-injection: installs a test backend on a live PD"]
fn mirror_partial_write_torn_state_then_scrub_recovers() {
    let dir = TempDir::new().unwrap();
    let (pool, _) = make_pool(&dir, 3);
    let id = pool.create_ld(LdSpec::mirror(3, 1, 1, 0)).unwrap();
    let ld = pool.open_ld(id).unwrap();

    // Initialize with a known payload (no fault yet).
    let initial = vec![0xa0u8; 4096];
    ld.write_at(0, &initial).unwrap();
    drop(ld);

    // Install a fault injector targeting copy 1's PD. succeed_first=0 →
    // every write to that PD now fails.
    let desc = pool.find_ld(id).unwrap();
    let target_pd_id = desc.members[1].pd;
    let target_pd = pool.pd(target_pd_id).unwrap();
    let inner = target_pd.backend();
    let injector = Arc::new(FaultInjectingBackend::new(inner, target_pd_id, 0));
    // The mirror write fan-out goes through whichever PD's backend is
    // attached to ops[0]; install on every PD so coverage is uniform.
    for info in pool.list_pds() {
        pool.pd(info.pd_id).unwrap().set_backend(injector.clone());
    }

    let new_payload = vec![0xc7u8; 4096];
    let ld = pool.open_ld(id).unwrap();
    ld.write_at(0, &new_payload)
        .expect("inline-degrade: mirror write rides through one failed copy (2 survivors)");
    // The failed copy is reported as a suspect for fast isolation.
    let ev = pool
        .suspect_events()
        .try_recv()
        .expect("a suspect member must be reported for the failed copy");
    assert_eq!(ev.pd_id, target_pd_id, "suspect is the faulted PD");

    // copy 0 + copy 2 should be on healthy PDs and have new data.
    let pd0 = pool.pd(desc.members[0].pd).unwrap();
    let pd2 = pool.pd(desc.members[2].pd).unwrap();
    let mut buf0 = vec![0u8; 4096];
    let mut buf2 = vec![0u8; 4096];
    pd0.read_chunklet_user(desc.members[0].chunklet_index, 0, &mut buf0)
        .unwrap();
    pd2.read_chunklet_user(desc.members[2].chunklet_index, 0, &mut buf2)
        .unwrap();
    assert_eq!(buf0, new_payload, "surviving copy 0 has new data");
    assert_eq!(buf2, new_payload, "surviving copy 2 has new data");

    // copy 1 retains old data (write was rejected).
    let mut buf1 = vec![0u8; 4096];
    target_pd
        .read_chunklet_user(desc.members[1].chunklet_index, 0, &mut buf1)
        .unwrap();
    assert_eq!(buf1, initial, "failed copy 1 keeps old data (torn state)");

    assert!(
        injector.failed_count() >= 1,
        "fault should have been triggered at least once"
    );

    // Restore the healthy backend before scrub (scrub does its own writes
    // for commit_bad_marks; we don't want those failing).
    let healthy = onyx_chunklet::io::make_backend(onyx_chunklet::io::IoBackendKind::Sync);
    for info in pool.list_pds() {
        pool.pd(info.pd_id).unwrap().set_backend(healthy.clone());
    }

    // Scrub on a 3-way mirror majority-votes; the divergent copy 1 gets
    // marked Bad in its bitmap.
    let report = pool.scrub_ld(id).unwrap();
    assert!(
        !report.mismatches.is_empty(),
        "scrub must detect the divergence"
    );
    assert_eq!(
        report.marked_bad, 1,
        "majority-vote should mark exactly one copy bad"
    );
    let (_, bm, _) = target_pd.snapshot();
    assert_eq!(
        bm.get(desc.members[1].chunklet_index).unwrap(),
        ChunkletState::Bad,
        "divergent copy must be quarantined post-scrub"
    );
}

/// Same torn-state + scrub contract as above, but the failing write is a
/// MULTI-STRIP variable-length span that now fans every strip × copy through
/// one batched `submit_writes`. Confirms the batched submit still produces a
/// clean K-1-of-K torn state (survivors fully new, failed copy fully old)
/// across all strips, and scrub majority-votes the divergent copy Bad.
#[test]
#[ignore = "fault-injection: installs a test backend on a live PD"]
fn mirror_partial_write_torn_state_multi_strip_then_scrub() {
    let dir = TempDir::new().unwrap();
    let (pool, _) = make_pool(&dir, 3);
    let id = pool.create_ld(LdSpec::mirror(3, 1, 1, 0)).unwrap();
    let ld = pool.open_ld(id).unwrap();

    // 16 KiB = 4 strips, so the batched submit carries many strips per copy.
    let span = 16 * 1024;
    let initial = vec![0xa0u8; span];
    ld.write_at(0, &initial).unwrap();
    drop(ld);

    let desc = pool.find_ld(id).unwrap();
    let target_pd_id = desc.members[1].pd;
    let target_pd = pool.pd(target_pd_id).unwrap();
    let inner = target_pd.backend();
    let injector = Arc::new(FaultInjectingBackend::new(inner, target_pd_id, 0));
    for info in pool.list_pds() {
        pool.pd(info.pd_id).unwrap().set_backend(injector.clone());
    }

    let new_payload: Vec<u8> = (0..span).map(|i| (i % 251) as u8).collect();
    let ld = pool.open_ld(id).unwrap();
    ld.write_at(0, &new_payload).expect(
        "inline-degrade: multi-strip mirror write rides through one failed copy (2 survivors)",
    );
    // One suspect for the failed copy (deduped across all 4 strips' segments).
    let ev = pool
        .suspect_events()
        .try_recv()
        .expect("a suspect member must be reported for the failed copy");
    assert_eq!(ev.pd_id, target_pd_id, "suspect is the faulted PD");

    // Survivors hold the FULL new multi-strip payload; failed copy keeps old.
    let pd0 = pool.pd(desc.members[0].pd).unwrap();
    let pd2 = pool.pd(desc.members[2].pd).unwrap();
    let mut buf0 = vec![0u8; span];
    let mut buf2 = vec![0u8; span];
    pd0.read_chunklet_user(desc.members[0].chunklet_index, 0, &mut buf0)
        .unwrap();
    pd2.read_chunklet_user(desc.members[2].chunklet_index, 0, &mut buf2)
        .unwrap();
    assert_eq!(buf0, new_payload, "surviving copy 0 has new multi-strip data");
    assert_eq!(buf2, new_payload, "surviving copy 2 has new multi-strip data");
    let mut buf1 = vec![0u8; span];
    target_pd
        .read_chunklet_user(desc.members[1].chunklet_index, 0, &mut buf1)
        .unwrap();
    assert_eq!(buf1, initial, "failed copy 1 keeps old data (torn state)");
    assert!(injector.failed_count() >= 1);

    // Heal the backend, then scrub: 3-way majority-vote marks copy 1 Bad.
    let healthy = onyx_chunklet::io::make_backend(onyx_chunklet::io::IoBackendKind::Sync);
    for info in pool.list_pds() {
        pool.pd(info.pd_id).unwrap().set_backend(healthy.clone());
    }
    let report = pool.scrub_ld(id).unwrap();
    assert!(
        !report.mismatches.is_empty(),
        "scrub must detect the multi-strip divergence"
    );
    assert_eq!(report.marked_bad, 1);
    let (_, bm, _) = target_pd.snapshot();
    assert_eq!(
        bm.get(desc.members[1].chunklet_index).unwrap(),
        ChunkletState::Bad
    );
}

/// R5 full-stripe write where the parity-PD's submit fails (post inline-degrade):
/// - RIDES THROUGH: the K data strips landed and parity is reconstructible, so
///   within the R5 budget of 1 the write returns `Ok` (not an error).
/// - Reports the parity PD as a suspect for fast isolation.
/// - Parity strip on the failed PD didn't land — a subsequent scrub still
///   detects the parity mismatch until rebuild restores it.
#[test]
#[ignore = "fault-injection: installs a test backend on a live PD"]
fn raid5_full_stripe_fault_on_parity_pd_detected_by_scrub() {
    let dir = TempDir::new().unwrap();
    let (pool, _) = make_pool(&dir, 4);
    let id = pool.create_ld(LdSpec::raid5(3, 1, 1, 0)).unwrap();

    // Pre-fault initialization to set a known parity baseline.
    let ld = pool.open_ld(id).unwrap();
    ld.write_at(0, &vec![0x00u8; 12 * 4096]).unwrap();
    drop(ld);

    let desc = pool.find_ld(id).unwrap();
    let parity_pd = desc.members[3].pd;
    let parity_pd_handle = pool.pd(parity_pd).unwrap();
    let inner = parity_pd_handle.backend();
    let injector = Arc::new(FaultInjectingBackend::new(inner, parity_pd, 0));
    for info in pool.list_pds() {
        pool.pd(info.pd_id).unwrap().set_backend(injector.clone());
    }

    let ld = pool.open_ld(id).unwrap();
    let payload = vec![0x88u8; 12 * 4096]; // a full-stripe full write
    ld.write_at(0, &payload)
        .expect("inline-degrade: R5 full-stripe rides through a failed parity write (budget 1)");
    let ev = pool
        .suspect_events()
        .try_recv()
        .expect("a suspect member must be reported for the failed parity PD");
    assert_eq!(ev.pd_id, parity_pd, "suspect is the faulted parity PD");
    assert!(injector.failed_count() >= 1);

    // Restore healthy backend for scrub.
    let healthy = onyx_chunklet::io::make_backend(onyx_chunklet::io::IoBackendKind::Sync);
    for info in pool.list_pds() {
        pool.pd(info.pd_id).unwrap().set_backend(healthy.clone());
    }

    let report = pool.scrub_ld(id).unwrap();
    assert!(
        !report.mismatches.is_empty(),
        "scrub must detect parity mismatch after parity-write fault"
    );
}

/// R6 full-stripe write where the Q-parity PD's submit fails. The Anvin
/// incremental Q is a distinct code path from P (`g^i` GF(2⁸) weighting), so a
/// fault landing on Q — the LAST member of a raid6 set — must (post
/// inline-degrade) RIDE THROUGH on the surviving data + P (R6 budget 2), report
/// the Q PD as a suspect, and only be caught by scrub until rebuild restores Q.
#[test]
#[ignore = "fault-injection: installs a test backend on a live PD"]
fn raid6_full_stripe_fault_on_q_parity_detected_by_scrub() {
    let dir = TempDir::new().unwrap();
    let (pool, _) = make_pool(&dir, 5); // raid6(3) = 3 data + P + Q = 5 members
    let id = pool.create_ld(LdSpec::raid6(3, 1, 1, 0)).unwrap();

    // Known baseline (4 full stripes; raid6(3) full stripe = 3 * 4096).
    let ld = pool.open_ld(id).unwrap();
    ld.write_at(0, &vec![0x00u8; 12 * 4096]).unwrap();
    drop(ld);

    // members = [data0, data1, data2, ParityP, ParityQ]; Q is the last.
    let desc = pool.find_ld(id).unwrap();
    let q_pd = desc.members[4].pd;
    let q_pd_handle = pool.pd(q_pd).unwrap();
    let injector = Arc::new(FaultInjectingBackend::new(q_pd_handle.backend(), q_pd, 0));
    for info in pool.list_pds() {
        pool.pd(info.pd_id).unwrap().set_backend(injector.clone());
    }

    let ld = pool.open_ld(id).unwrap();
    ld.write_at(0, &vec![0x88u8; 12 * 4096])
        .expect("inline-degrade: R6 full-stripe rides through a failed Q write (budget 2)");
    let ev = pool
        .suspect_events()
        .try_recv()
        .expect("a suspect member must be reported for the failed Q PD");
    assert_eq!(ev.pd_id, q_pd, "suspect is the faulted Q PD");
    assert!(injector.failed_count() >= 1);

    // Heal, then scrub: stale Q vs recomputed Q is a mismatch.
    let healthy = onyx_chunklet::io::make_backend(onyx_chunklet::io::IoBackendKind::Sync);
    for info in pool.list_pds() {
        pool.pd(info.pd_id).unwrap().set_backend(healthy.clone());
    }
    let report = pool.scrub_ld(id).unwrap();
    assert!(
        !report.mismatches.is_empty(),
        "scrub must detect Q-parity mismatch after Q-write fault"
    );
}

/// R6 double-PD failure → `rebuild_ld` reconstructs BOTH lost members onto
/// spares and restores the data bit-for-bit. This is the headline R6 recovery
/// path — 2-missing PQ reconstruction driven through the rebuild orchestration
/// (allocate replacements outside the surviving set → reconstruct → commit),
/// not just the in-place degraded-read reconstruct that `ld_raid6.rs` covers.
///
/// 8 PDs so the 6-member set leaves 2 live non-member PDs to rebuild onto after
/// 2 member PDs are lost (6 survivors ≥ quorum 5).
#[test]
#[ignore = "fault-injection: rebuild reconstructs a 1 GiB chunklet per member"]
fn raid6_double_pd_fail_rebuild_to_spare_restores_data() {
    let dir = TempDir::new().unwrap();
    let (pool, paths) = make_pool(&dir, 8);
    let id = pool.create_ld(LdSpec::raid6(4, 1, 1, 0)).unwrap();

    // 4 full stripes (raid6(4) full stripe = 4 data strips * 4096 = 16 KiB).
    let span = 4 * 4 * 4096;
    let expected = pattern(0x6a, span, 0);
    let ld = pool.open_ld(id).unwrap();
    ld.write_at(0, &expected).unwrap();
    drop(ld);

    // Kill two DATA member PDs (positions 0 and 1) by reopening without them.
    let drop0 = path_for_member(&pool, &paths, 0);
    let drop1 = path_for_member(&pool, &paths, 1);
    assert_ne!(drop0, drop1);
    drop(pool);
    let pool = open_subset(&paths, &[drop0, drop1]);

    // Both lost members reconstructed onto fresh chunklets on live non-set PDs.
    let report = pool.rebuild_ld(id).unwrap();
    assert!(!report.skipped, "rebuild must run with two failed members");
    assert_eq!(report.rebuilt_members, 2, "both R6 members rebuilt");

    // The LD is whole again (all 6 members on live PDs) → healthy read returns
    // the original bytes, proving the PQ reconstruction was correct.
    let ld = pool.open_ld(id).unwrap();
    let mut got = vec![0u8; span];
    ld.read_at(0, &mut got).unwrap();
    assert_eq!(got, expected, "rebuilt R6 data matches the original");
}

/// The headline ONLINE-rebuild test: foreground writes run CONCURRENTLY with a
/// RAID6 rebuild and every write survives the descriptor swap. Proves the
/// write-forward + per-set cursor keep the shadow spare consistent with live
/// writes across all interleavings — the property that lets `rebuild_ld` drop
/// the whole-op `io_lock.write()` and stop hard-blocking foreground IO.
///
/// One writer thread hammers a fixed set of blocks with monotonically-changing
/// values (recording the last value written to each) while another thread runs
/// the rebuild; after both finish, a fresh handle must read back the last value
/// written to every block. A stale shadow (broken write-forward) would surface
/// as a mismatch on blocks whose last write landed below the cursor mid-rebuild.
#[test]
#[ignore = "fault-injection: online rebuild backfills a full 1 GiB chunklet (~60s)"]
fn raid6_online_rebuild_concurrent_writes_preserve_data() {
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicBool, Ordering};

    let dir = TempDir::new().unwrap();
    let (pool, paths) = make_pool(&dir, 8);
    let id = pool.create_ld(LdSpec::raid6(4, 1, 1, 0)).unwrap();

    // 96 distinct 4 KiB blocks spread across the LD's capacity.
    let ld = pool.open_ld(id).unwrap();
    let cap = ld.capacity_bytes();
    let block = 4096u64;
    let n_off = 96usize;
    let stride = ((cap / block / n_off as u64).max(1)) * block;
    let offsets: Vec<u64> = (0..n_off as u64).map(|i| i * stride).collect();
    // Seed a baseline so degraded reads/reconstructs have data.
    for &off in &offsets {
        ld.write_at(off, &pattern(0, block as usize, off)).unwrap();
    }
    ld.flush().unwrap();
    drop(ld);

    // Fail one data-member PD, reopen degraded (7 of 8 PDs, quorum holds).
    let drop0 = path_for_member(&pool, &paths, 0);
    drop(pool);
    let pool = open_subset(&paths, &[drop0]);

    let rebuild_done = Arc::new(AtomicBool::new(false));

    // Rebuild on its own thread (Phase B holds only io_lock.read).
    let pool_r = pool.clone();
    let done_r = rebuild_done.clone();
    let rebuild_thread = std::thread::spawn(move || {
        let report = pool_r.rebuild_ld(id).unwrap();
        done_r.store(true, Ordering::Release);
        report
    });

    // Concurrent writer: hammer every offset with a fresh value until the
    // rebuild finishes, recording the last value written to each. Reopen the
    // handle when Phase C bumps the epoch (mirrors onyx's stale-handle retry).
    let mut expected: HashMap<u64, Vec<u8>> = HashMap::new();
    let mut ld = pool.open_ld(id).unwrap();
    let mut counter: u64 = 1;
    while !rebuild_done.load(Ordering::Acquire) {
        for &off in &offsets {
            let val = pattern(counter, block as usize, off);
            counter += 1;
            match ld.write_at(off, &val) {
                Ok(()) => {
                    expected.insert(off, val);
                }
                Err(_) => {
                    // Stale handle (epoch bumped by Phase C) → reopen + retry.
                    ld = pool.open_ld(id).unwrap();
                    ld.write_at(off, &val).unwrap();
                    expected.insert(off, val);
                }
            }
        }
    }
    let report = rebuild_thread.join().unwrap();
    assert!(!report.skipped, "rebuild ran");
    assert_eq!(report.rebuilt_members, 1, "one failed data member rebuilt");

    // Fresh handle (post-swap: reads hit the rebuilt spare). Every block must
    // read back the last value the writer recorded for it.
    let ld = pool.open_ld(id).unwrap();
    for &off in &offsets {
        let mut got = vec![0u8; block as usize];
        ld.read_at(off, &mut got).unwrap();
        assert_eq!(
            got, expected[&off],
            "block at offset {} lost its last concurrent write across the online rebuild",
            off
        );
    }
}

/// Shared driver: fail the PD holding member `fail_member_idx`, run an online
/// rebuild on one thread while another hammers a fixed block set with changing
/// values, then assert every block reads back its last concurrent write. Proves
/// the write-forward + cursor for whichever RAID level `spec` selects.
fn run_online_rebuild_concurrent(
    n_pds: usize,
    spec: LdSpec,
    fail_member_idx: usize,
    expect_rebuilt: usize,
) {
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicBool, Ordering};

    let dir = TempDir::new().unwrap();
    let (pool, paths) = make_pool(&dir, n_pds);
    let id = pool.create_ld(spec).unwrap();

    let ld = pool.open_ld(id).unwrap();
    let cap = ld.capacity_bytes();
    let block = 4096u64;
    let n_off = 96usize;
    let stride = ((cap / block / n_off as u64).max(1)) * block;
    let offsets: Vec<u64> = (0..n_off as u64).map(|i| i * stride).collect();
    for &off in &offsets {
        ld.write_at(off, &pattern(0, block as usize, off)).unwrap();
    }
    ld.flush().unwrap();
    drop(ld);

    let drop0 = path_for_member(&pool, &paths, fail_member_idx);
    drop(pool);
    let pool = open_subset(&paths, &[drop0]);

    let rebuild_done = Arc::new(AtomicBool::new(false));
    let pool_r = pool.clone();
    let done_r = rebuild_done.clone();
    let rebuild_thread = std::thread::spawn(move || {
        let report = pool_r.rebuild_ld(id).unwrap();
        done_r.store(true, Ordering::Release);
        report
    });

    let mut expected: HashMap<u64, Vec<u8>> = HashMap::new();
    let mut ld = pool.open_ld(id).unwrap();
    let mut counter: u64 = 1;
    while !rebuild_done.load(Ordering::Acquire) {
        for &off in &offsets {
            let val = pattern(counter, block as usize, off);
            counter += 1;
            match ld.write_at(off, &val) {
                Ok(()) => {
                    expected.insert(off, val);
                }
                Err(_) => {
                    ld = pool.open_ld(id).unwrap();
                    ld.write_at(off, &val).unwrap();
                    expected.insert(off, val);
                }
            }
        }
    }
    let report = rebuild_thread.join().unwrap();
    assert!(!report.skipped, "rebuild ran");
    assert_eq!(report.rebuilt_members, expect_rebuilt, "rebuilt member count");

    let ld = pool.open_ld(id).unwrap();
    for &off in &offsets {
        let mut got = vec![0u8; block as usize];
        ld.read_at(off, &mut got).unwrap();
        assert_eq!(
            got, expected[&off],
            "block at offset {} lost its last concurrent write across the online rebuild",
            off
        );
    }
}

/// Mirror (RAID10) online rebuild under concurrent writes — exercises the
/// write-forward added to `LdMirror::collect_strip_writes`. LV2 + metadb are
/// RAID10, so this is the level that matters for onyx's failover.
#[test]
#[ignore = "fault-injection: online rebuild backfills a full 1 GiB chunklet"]
fn mirror_online_rebuild_concurrent_writes_preserve_data() {
    // 3-way mirror, single set; fail one copy, rebuild onto a live outside PD.
    run_online_rebuild_concurrent(8, LdSpec::mirror(3, 1, 1, 0), 0, 1);
}

/// RAID5 online rebuild under concurrent writes — exercises the R5 write-forward
/// + the RW-path failed-parity fix (rebuilding a data member keeps the set on
/// the RW path, which now tolerates + write-forwards correctly).
#[test]
#[ignore = "fault-injection: online rebuild backfills a full 1 GiB chunklet"]
fn raid5_online_rebuild_concurrent_writes_preserve_data() {
    // raid5(3) = 3 data + P; fail one data member, rebuild onto an outside PD.
    run_online_rebuild_concurrent(8, LdSpec::raid5(3, 1, 1, 0), 0, 1);
}

/// Budget boundary: when EVERY copy of a mirror segment fails, there is no
/// survivor to hold the data, so inline-degrade must NOT absorb — the write
/// surfaces the error exactly like the pre-fix all-or-nothing path.
#[test]
#[ignore = "fault-injection: installs a test backend on a live PD"]
fn mirror_all_copies_fail_surfaces_error() {
    let dir = TempDir::new().unwrap();
    let (pool, _) = make_pool(&dir, 3);
    let id = pool.create_ld(LdSpec::mirror(3, 1, 1, 0)).unwrap();
    let desc = pool.find_ld(id).unwrap();

    // Fail ALL three copies' PDs.
    let targets: std::collections::BTreeSet<_> = desc.members.iter().map(|m| m.pd).collect();
    let any_pd = pool.pd(desc.members[0].pd).unwrap();
    let injector = Arc::new(FaultInjectingBackend::new_multi(any_pd.backend(), targets, 0));
    for info in pool.list_pds() {
        pool.pd(info.pd_id).unwrap().set_backend(injector.clone());
    }

    let ld = pool.open_ld(id).unwrap();
    let err = ld.write_at(0, &vec![0xc7u8; 4096]).err();
    assert!(
        err.is_some(),
        "all copies failed ⇒ over budget ⇒ write must surface the error"
    );
}

/// R6 F=2: fail one DATA member and the Q parity in one write. R6's budget is 2,
/// so the write is absorbed on the surviving data + P; after isolating the two
/// failed members the degraded read must reconstruct the NEW data via P
/// (proving the parity that landed reflects the new stripe).
#[test]
#[ignore = "fault-injection: installs a test backend on a live PD"]
fn raid6_two_member_fault_absorbed_then_reconstructs_new_data() {
    let dir = TempDir::new().unwrap();
    let (pool, _) = make_pool(&dir, 5); // raid6(3) = 3 data + P + Q
    let id = pool.create_ld(LdSpec::raid6(3, 1, 1, 0)).unwrap();

    let ld = pool.open_ld(id).unwrap();
    ld.write_at(0, &vec![0x00u8; 12 * 4096]).unwrap(); // baseline
    drop(ld);

    let desc = pool.find_ld(id).unwrap();
    let data0_pd = desc.members[0].pd; // a data member
    let q_pd = desc.members[4].pd; // Q parity
    let targets: std::collections::BTreeSet<_> = [data0_pd, q_pd].into_iter().collect();
    let injector =
        Arc::new(FaultInjectingBackend::new_multi(pool.pd(data0_pd).unwrap().backend(), targets, 0));
    for info in pool.list_pds() {
        pool.pd(info.pd_id).unwrap().set_backend(injector.clone());
    }

    let new_payload = pattern(0x99, 12 * 4096, 0);
    let ld = pool.open_ld(id).unwrap();
    ld.write_at(0, &new_payload)
        .expect("inline-degrade: R6 rides through F=2 (one data + Q) on survivors + P");
    // Both failed members are reported (the serial write_at loop emits per
    // stripe, so the same two PDs recur across the 4 stripes — the reactor
    // dedups idempotently via mark_pd_failed; here we assert the UNIQUE set).
    let suspects: std::collections::BTreeSet<_> =
        std::iter::from_fn(|| pool.suspect_events().try_recv().ok())
            .map(|s| s.pd_id)
            .collect();
    let want: std::collections::BTreeSet<_> = [data0_pd, q_pd].into_iter().collect();
    assert_eq!(suspects, want, "both failed members reported as suspects");

    // Heal the backend, isolate the two failed members, reopen degraded, and
    // read: data0 must reconstruct to the NEW value from the P that landed.
    let healthy = onyx_chunklet::io::make_backend(onyx_chunklet::io::IoBackendKind::Sync);
    for info in pool.list_pds() {
        pool.pd(info.pd_id).unwrap().set_backend(healthy.clone());
    }
    pool.mark_pd_failed(data0_pd).unwrap();
    pool.mark_pd_failed(q_pd).unwrap();
    let ld = pool.open_ld(id).unwrap();
    let mut got = vec![0u8; 12 * 4096];
    ld.read_at(0, &mut got).unwrap();
    assert_eq!(got, new_payload, "degraded read reconstructs the new data after F=2 absorb");
}

/// Budget boundary: R6 tolerates 2, so failing 3 members in one write exceeds
/// the budget and must surface the error (no silent data loss).
#[test]
#[ignore = "fault-injection: installs a test backend on a live PD"]
fn raid6_three_member_fault_exceeds_budget() {
    let dir = TempDir::new().unwrap();
    let (pool, _) = make_pool(&dir, 5);
    let id = pool.create_ld(LdSpec::raid6(3, 1, 1, 0)).unwrap();
    let desc = pool.find_ld(id).unwrap();

    // Fail 3 of the 5 members (two data + Q) in one write.
    let targets: std::collections::BTreeSet<_> =
        [desc.members[0].pd, desc.members[1].pd, desc.members[4].pd]
            .into_iter()
            .collect();
    let injector = Arc::new(FaultInjectingBackend::new_multi(
        pool.pd(desc.members[0].pd).unwrap().backend(),
        targets,
        0,
    ));
    for info in pool.list_pds() {
        pool.pd(info.pd_id).unwrap().set_backend(injector.clone());
    }

    let ld = pool.open_ld(id).unwrap();
    let err = ld.write_at(0, &vec![0x99u8; 12 * 4096]).err();
    assert!(
        err.is_some(),
        "3 failed members > R6 budget 2 ⇒ write must surface the error"
    );
}

// ---------------------------------------------------------------------------
// Reconstruct-on-READ (a member faults on the READ side while the LD still
// believes it healthy — the detection window before isolation). These use the
// per-PD `set_read_faulting` hook (uniform across the fast read, sibling reads,
// and RAID5/6 parity survivor reads, all of which funnel through
// `read_chunklet_user`). They assert the read is TRANSPARENT (correct bytes,
// no upper-layer replay), that the faulting member is reported as a suspect so
// reads also drive fast isolation, and that an over-budget read surfaces an
// error (never silently reconstructs wrong data).
// ---------------------------------------------------------------------------

/// 3-way mirror: the round-robin-chosen copy's reads fault → read reconstructs
/// from a surviving sibling transparently, and the faulting copy is a suspect.
#[test]
#[ignore = "fault-injection: installs a per-PD read fault on a live PD"]
fn mirror_read_eio_reconstructs_from_sibling_and_suspects() {
    let dir = TempDir::new().unwrap();
    let (pool, _) = make_pool(&dir, 3);
    let id = pool.create_ld(LdSpec::mirror(3, 1, 1, 0)).unwrap();
    let ld = pool.open_ld(id).unwrap();
    let payload = pattern(1, 4096, 0);
    ld.write_at(0, &payload).unwrap();
    drop(ld);

    let desc = pool.find_ld(id).unwrap();
    let target = desc.members[0].pd; // a fresh handle's first read picks copy 0
    pool.pd(target).unwrap().set_read_faulting(true);

    let ld = pool.open_ld(id).unwrap();
    let mut buf = vec![0u8; 4096];
    ld.read_at(0, &mut buf)
        .expect("mirror read reconstructs from a live sibling transparently");
    assert_eq!(buf, payload, "reconstruct-on-read returns the correct bytes");

    let ev = pool
        .suspect_events()
        .try_recv()
        .expect("the faulting copy must be reported as a suspect");
    assert_eq!(ev.pd_id, target, "suspect is the faulting copy's PD");
}

/// 2-way mirror with BOTH copies' reads faulting → no live copy → the read
/// surfaces an error (never fabricates data), and both are suspected.
#[test]
#[ignore = "fault-injection: installs a per-PD read fault on a live PD"]
fn mirror_all_live_copies_fail_read_surfaces_error() {
    let dir = TempDir::new().unwrap();
    let (pool, _) = make_pool(&dir, 2);
    let id = pool.create_ld(LdSpec::mirror(2, 1, 1, 0)).unwrap();
    let ld = pool.open_ld(id).unwrap();
    ld.write_at(0, &vec![0x5au8; 4096]).unwrap();
    drop(ld);

    let desc = pool.find_ld(id).unwrap();
    pool.pd(desc.members[0].pd).unwrap().set_read_faulting(true);
    pool.pd(desc.members[1].pd).unwrap().set_read_faulting(true);

    let ld = pool.open_ld(id).unwrap();
    let mut buf = vec![0u8; 4096];
    assert!(
        ld.read_at(0, &mut buf).is_err(),
        "every live copy faulting ⇒ read must surface an error"
    );
    // Both faulting copies are reported (order-independent).
    let mut seen = std::collections::BTreeSet::new();
    while let Ok(ev) = pool.suspect_events().try_recv() {
        seen.insert(ev.pd_id);
    }
    assert!(
        seen.contains(&desc.members[0].pd) && seen.contains(&desc.members[1].pd),
        "both faulting copies reported as suspects, got {:?}",
        seen
    );
}

/// RAID5: a data member's reads fault → read reconstructs via parity + the
/// surviving data, transparently, and the faulting member is a suspect.
#[test]
#[ignore = "fault-injection: installs a per-PD read fault on a live PD"]
fn raid5_read_eio_reconstructs_via_parity_and_suspects() {
    let dir = TempDir::new().unwrap();
    let (pool, _) = make_pool(&dir, 3);
    let id = pool.create_ld(LdSpec::raid5(2, 1, 1, 0)).unwrap(); // 2 data + 1 parity
    let ld = pool.open_ld(id).unwrap();
    let payload = pattern(7, 4096, 0);
    ld.write_at(0, &payload).unwrap();
    drop(ld);

    let desc = pool.find_ld(id).unwrap();
    let target = desc.members[0].pd; // data position 0
    pool.pd(target).unwrap().set_read_faulting(true);

    let ld = pool.open_ld(id).unwrap();
    let mut buf = vec![0u8; 4096];
    ld.read_at(0, &mut buf)
        .expect("R5 read reconstructs the faulting data strip from parity");
    assert_eq!(buf, payload, "reconstruct-on-read returns the correct bytes");

    let ev = pool
        .suspect_events()
        .try_recv()
        .expect("the faulting data member must be reported as a suspect");
    assert_eq!(ev.pd_id, target);
}

/// RAID5 over budget: a data read fault PLUS parity reads faulting = 2 faults
/// in a set that tolerates 1 → the read surfaces an error, never wrong data.
#[test]
#[ignore = "fault-injection: installs a per-PD read fault on a live PD"]
fn raid5_read_eio_second_failure_over_budget_errors() {
    let dir = TempDir::new().unwrap();
    let (pool, _) = make_pool(&dir, 3);
    let id = pool.create_ld(LdSpec::raid5(2, 1, 1, 0)).unwrap();
    let ld = pool.open_ld(id).unwrap();
    ld.write_at(0, &pattern(8, 4096, 0)).unwrap();
    drop(ld);

    let desc = pool.find_ld(id).unwrap();
    pool.pd(desc.members[0].pd).unwrap().set_read_faulting(true); // data 0
    pool.pd(desc.members[2].pd).unwrap().set_read_faulting(true); // parity

    let ld = pool.open_ld(id).unwrap();
    let mut buf = vec![0u8; 4096];
    assert!(
        ld.read_at(0, &mut buf).is_err(),
        "data + parity faulting = over R5 budget 1 ⇒ read must error"
    );
}

/// RAID6: one data member's reads fault → read reconstructs via P + surviving
/// data (effective failed set of size 1), transparently, and it is suspected.
#[test]
#[ignore = "fault-injection: installs a per-PD read fault on a live PD"]
fn raid6_read_eio_one_data_reconstructs_and_suspects() {
    let dir = TempDir::new().unwrap();
    let (pool, _) = make_pool(&dir, 4);
    let id = pool.create_ld(LdSpec::raid6(2, 1, 1, 0)).unwrap(); // 2 data + P + Q
    let ld = pool.open_ld(id).unwrap();
    let payload = pattern(9, 4096, 0);
    ld.write_at(0, &payload).unwrap();
    drop(ld);

    let desc = pool.find_ld(id).unwrap();
    let target = desc.members[0].pd; // data position 0
    pool.pd(target).unwrap().set_read_faulting(true);

    let ld = pool.open_ld(id).unwrap();
    let mut buf = vec![0u8; 4096];
    ld.read_at(0, &mut buf)
        .expect("R6 read reconstructs the faulting data strip via parity");
    assert_eq!(buf, payload, "reconstruct-on-read returns the correct bytes");

    let ev = pool
        .suspect_events()
        .try_recv()
        .expect("the faulting data member must be reported as a suspect");
    assert_eq!(ev.pd_id, target);
}

/// RAID6 over budget: reading data 0 whose member faults, while a SECOND data
/// member also faults its reads, exceeds what a single-position reconstruct can
/// serve (the reconstruct read of the other data strip faults) → the read
/// surfaces an error rather than returning wrong data.
#[test]
#[ignore = "fault-injection: installs a per-PD read fault on a live PD"]
fn raid6_read_eio_two_data_faults_over_budget_errors() {
    let dir = TempDir::new().unwrap();
    let (pool, _) = make_pool(&dir, 4);
    let id = pool.create_ld(LdSpec::raid6(2, 1, 1, 0)).unwrap();
    let ld = pool.open_ld(id).unwrap();
    ld.write_at(0, &pattern(10, 8192, 0)).unwrap(); // fill both data strips
    drop(ld);

    let desc = pool.find_ld(id).unwrap();
    pool.pd(desc.members[0].pd).unwrap().set_read_faulting(true); // data 0
    pool.pd(desc.members[1].pd).unwrap().set_read_faulting(true); // data 1

    let ld = pool.open_ld(id).unwrap();
    let mut buf = vec![0u8; 4096];
    assert!(
        ld.read_at(0, &mut buf).is_err(),
        "two data members faulting reads ⇒ single-position reconstruct can't \
         serve it ⇒ read must error (no wrong data)"
    );
}
