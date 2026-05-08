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
use common::make_pool;

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
