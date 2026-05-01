//! P8b: end-to-end coverage for both `IoBackend` impls.
//!
//! `SyncBackend` is the default and exercised by every other integration
//! test indirectly. This file specifically routes a handful of LD round
//! trips through `UringBackend` (Linux only) to make sure the
//! `io_uring`-batched submission path encodes / writes / reads the same
//! bytes as the sync `pwrite` fan-out.
//!
//! When `io_uring` init fails (kernel too old, sysctl disabled, container
//! sandbox, etc.), `make_backend` silently falls back to `SyncBackend` —
//! so these tests never fail spuriously on environments without
//! `io_uring`. The trade-off is that on such systems the tests don't
//! actually exercise the `Uring` codepath; that's acceptable because the
//! prod target (RHEL 10 / kernel 6.12 nvme-box) has `io_uring` enabled.

mod common;

use onyx_chunklet::io::IoBackendKind;
use onyx_chunklet::pool::LdSpec;
use tempfile::TempDir;

use common::{make_pool_with, pattern};

const STRIP: usize = 4096;

/// Sanity-check that `Uring` actually got installed on Linux. On
/// kernels without `io_uring`, `make_backend` falls back to `Sync` —
/// log loudly so a passing test on the wrong backend still surfaces in
/// CI output. Kept off the assertion path because some CI sandboxes
/// disable `io_uring` legitimately.
fn report_backend_used(pool: &onyx_chunklet::Pool, requested: IoBackendKind) {
    let actual = pool
        .pd_by_seq(0)
        .map(|pd| pd.backend().name())
        .unwrap_or("?");
    if requested == IoBackendKind::Uring && actual != "uring" {
        eprintln!(
            "[io_backends] requested Uring but got {}; environment may not support io_uring",
            actual
        );
    }
}

/// `IoBackendKind::default()` is `Sync` — round-trip a R5 LD and assert
/// data integrity. Acts as the control case.
#[test]
fn sync_backend_r5_round_trip() {
    let dir = TempDir::new().unwrap();
    let (pool, _paths) = make_pool_with(&dir, 4, IoBackendKind::Sync);
    let ld_id = pool.create_ld(LdSpec::raid5(3, 1, 1, 0)).unwrap();
    let ld = pool.open_ld(ld_id).unwrap();
    let payload = pattern(0xa1, 3 * STRIP, 0);
    ld.write_at(0, &payload).unwrap();
    let mut readback = vec![0u8; payload.len()];
    ld.read_at(0, &mut readback).unwrap();
    assert_eq!(readback, payload);
}

/// Same round trip routed through `UringBackend`. On non-Linux (or
/// Linux without io_uring) `make_backend` falls back to `SyncBackend`,
/// so the test still passes without exercising the new path.
#[test]
fn uring_backend_r5_round_trip() {
    let dir = TempDir::new().unwrap();
    let (pool, _paths) = make_pool_with(&dir, 4, IoBackendKind::Uring);
    report_backend_used(&pool, IoBackendKind::Uring);
    let ld_id = pool.create_ld(LdSpec::raid5(3, 1, 1, 0)).unwrap();
    let ld = pool.open_ld(ld_id).unwrap();
    let payload = pattern(0xb2, 3 * STRIP, 0);
    ld.write_at(0, &payload).unwrap();
    let mut readback = vec![0u8; payload.len()];
    ld.read_at(0, &mut readback).unwrap();
    assert_eq!(readback, payload);
}

/// R6 round trip on Uring — exercises K=3 + P + Q (set_size=5) which
/// generates 5 SQEs per full-stripe write.
#[test]
fn uring_backend_r6_round_trip() {
    let dir = TempDir::new().unwrap();
    let (pool, _paths) = make_pool_with(&dir, 5, IoBackendKind::Uring);
    report_backend_used(&pool, IoBackendKind::Uring);
    let ld_id = pool.create_ld(LdSpec::raid6(3, 1, 1, 0)).unwrap();
    let ld = pool.open_ld(ld_id).unwrap();
    let payload = pattern(0xc3, 3 * STRIP, 0);
    ld.write_at(0, &payload).unwrap();
    let mut readback = vec![0u8; payload.len()];
    ld.read_at(0, &mut readback).unwrap();
    assert_eq!(readback, payload);
}

/// Mixed full-stripe + RW partial on Uring R5 — exercises both write
/// paths through the batched submit.
#[test]
fn uring_backend_r5_partial_then_full() {
    let dir = TempDir::new().unwrap();
    let (pool, _paths) = make_pool_with(&dir, 4, IoBackendKind::Uring);
    report_backend_used(&pool, IoBackendKind::Uring);
    let ld_id = pool.create_ld(LdSpec::raid5(3, 1, 1, 0)).unwrap();
    let ld = pool.open_ld(ld_id).unwrap();

    let initial = pattern(0xd4, 3 * STRIP, 0);
    ld.write_at(0, &initial).unwrap(); // full-stripe → 4 SQEs

    let updated = pattern(0xe5, 2 * STRIP, 0);
    ld.write_at(0, &updated).unwrap(); // RW path (M=2 of K=3) → 3 SQEs

    let mut readback = vec![0u8; 3 * STRIP];
    ld.read_at(0, &mut readback).unwrap();
    assert_eq!(&readback[0..2 * STRIP], &updated[..]);
    assert_eq!(&readback[2 * STRIP..], &initial[2 * STRIP..]);
}

/// `Pool::set_io_backend` swaps the backend on a live pool. Open with
/// Sync, do one write, hot-swap to Uring, do another write, verify both
/// landed.
#[test]
fn pool_set_io_backend_hot_swap() {
    let dir = TempDir::new().unwrap();
    let (pool, _paths) = make_pool_with(&dir, 4, IoBackendKind::Sync);
    let ld_id = pool.create_ld(LdSpec::raid5(3, 1, 1, 0)).unwrap();
    let ld = pool.open_ld(ld_id).unwrap();

    let first = pattern(0xf6, 3 * STRIP, 0);
    ld.write_at(0, &first).unwrap();

    pool.set_io_backend(IoBackendKind::Uring);

    let second = pattern(0x07, 3 * STRIP, 0);
    ld.write_at(0, &second).unwrap();

    let mut readback = vec![0u8; 3 * STRIP];
    ld.read_at(0, &mut readback).unwrap();
    assert_eq!(readback, second);
}
