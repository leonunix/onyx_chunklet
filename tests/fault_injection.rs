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

/// Mirror write where one of N copies fails mid-batch must:
/// - Surface the error to the caller (so the upper layer knows to retry
///   or scrub).
/// - Leave the surviving copies with the new data.
/// - Leave the failed copy with the old data (real torn state).
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
    let err = ld.write_at(0, &new_payload).err();
    assert!(
        err.is_some(),
        "mirror write must surface the injected fault"
    );

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
    let err = ld.write_at(0, &new_payload).err();
    assert!(
        err.is_some(),
        "multi-strip mirror write must surface the injected fault"
    );

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

/// R5 full-stripe write where the parity-PD's submit fails:
/// - Caller sees the error.
/// - Data strips that succeeded are durable (written before the fault
///   batch returned).
/// - Parity strip on the failed PD didn't land — subsequent scrub will
///   detect parity mismatch.
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
    let err = ld.write_at(0, &payload).err();
    assert!(err.is_some(), "R5 full-stripe write must surface fault");

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
/// fault landing on Q — the LAST member of a raid6 set — must be surfaced to
/// the caller and later caught by scrub, exactly like the R5 P-parity case.
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
    let err = ld.write_at(0, &vec![0x88u8; 12 * 4096]).err();
    assert!(err.is_some(), "R6 full-stripe write must surface the Q fault");
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
