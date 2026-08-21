//! Windowed io_uring submission must be byte-for-byte equivalent to the
//! stop-and-wait barrier it replaces.
//!
//! `uring_write_window_sqes` changes only WHEN SQEs reach the kernel, never what
//! they point at, so any observable difference is a bug in the window loop's
//! bookkeeping (a slice pushed twice, a completion attributed to the wrong op, a
//! batch that returns before the kernel is done with a borrowed buffer). That
//! last one is the dangerous class and it is silent, which is why this file also
//! reads the data back through a REOPENED pool: a use-after-free would more
//! likely show up as wrong bytes on the media than as a failed result.
//!
//! Its own integration binary on purpose: the knob is process-global, so a test
//! in a file with parallel siblings could not say which arm actually ran. The
//! broader gate is re-running the whole suite under
//! `CHUNKLET_WRITE_WINDOW_SQES=8`, which is the only way to catch a caller
//! nobody thought to window-test.

mod common;

use onyx_chunklet::io::uring_backend::{
    set_write_window_sqes, write_window_sqes, URING_DEPTH,
};
use onyx_chunklet::io::IoBackendKind;
use onyx_chunklet::pool::LdSpec;
use tempfile::TempDir;

use common::{make_pool_with, pattern};

/// `(pushes, waves)` summed over every `IoClass`. `pushes > waves` is the only
/// direct evidence that the window loop refilled the ring while earlier SQEs
/// were still in flight; without it this file would pass even if windowing were
/// silently disabled.
fn push_and_wave_totals() -> (u64, u64) {
    let stats = onyx_chunklet::write_path_stats();
    stats.submit.iter().fold((0, 0), |(pushes, waves), class| {
        (pushes + class.pushes, waves + class.waves)
    })
}

/// Two blocks, so a sub-strip write below is genuinely partial (a 4 KiB strip
/// makes every block-aligned write a whole number of strips, which would only
/// ever exercise the zero-read full-stripe arm).
const STRIP_LOG2: u8 = 13;
const STRIP: usize = 1 << STRIP_LOG2;
/// 3 data + P + Q, so a full stripe is 5 SQEs and the batch below is 80 --
/// comfortably more than the window, which is the only interesting case.
const DATA: usize = 3;
const STRIPES: usize = 16;

fn write_batch_and_read_back(dir: &TempDir, window: usize, tag: u64) -> (Vec<u8>, u64, u64) {
    set_write_window_sqes(window);
    // The setter clamps to the SQ depth, so compare against the clamp: a window
    // wider than the ring would find no room at top-up and silently collapse
    // back into a barrier.
    assert_eq!(
        write_window_sqes(),
        window.min(URING_DEPTH as usize),
        "window knob did not take"
    );

    let (pool, _paths) = make_pool_with(dir, 5, IoBackendKind::Uring);
    let ld_id = pool.create_ld(LdSpec::raid6(DATA as u8, 1, 1, STRIP_LOG2)).unwrap();
    let ld = pool.open_ld(ld_id).unwrap();

    let full_stripe = DATA * STRIP;
    let total = STRIPES * full_stripe;
    let payload = pattern(tag, total, 0);

    // One `write_many_at` per run so the whole thing is a single submit.
    // Contiguous stripes MERGE on every member (16 x 8 KiB = 128 KiB, under the
    // 256 KiB coalesce cap), so this arm covers windowed submission of vectored
    // groups -- and is a reminder that op count is NOT SQE count.
    let ops: Vec<(u64, &[u8])> = (0..STRIPES)
        .map(|i| {
            let start = i * full_stripe;
            ((start as u64), &payload[start..start + full_stripe])
        })
        .collect();
    ld.write_many_at(&ops).unwrap();

    // The same stripes written again with a gap between them, so no two strips
    // are adjacent on any member and the batch stays 80 unmerged SQEs. This is
    // the arm that is guaranteed wider than the window.
    let strided: Vec<(u64, &[u8])> = (0..STRIPES)
        .step_by(2)
        .map(|i| {
            let start = i * full_stripe;
            ((start as u64), &payload[start..start + full_stripe])
        })
        .collect();
    ld.write_many_at(&strided).unwrap();

    // A sub-strip overwrite, so the batch's RMW arms (old-data + old P/Q reads,
    // then new P/Q writes) also go through the window rather than only the
    // zero-read full-stripe arm.
    let partial: Vec<(u64, &[u8])> = (0..STRIPES)
        .step_by(2)
        .map(|i| {
            let start = i * full_stripe + STRIP;
            ((start as u64), &payload[start..start + STRIP / 2])
        })
        .collect();
    ld.write_many_at(&partial).unwrap();

    let mut readback = vec![0u8; total];
    ld.read_at(0, &mut readback).unwrap();
    assert_eq!(
        readback, payload,
        "window={window} readback differs from what was written"
    );
    let (pushes, waves) = push_and_wave_totals();
    (readback, pushes, waves)
}

/// The barrier arm and two window widths must produce identical media contents.
/// `window >= batch` is deliberately included: it runs the window code path but
/// degenerates to one push, so it proves the disabled value is not a second
/// implementation.
#[test]
fn windowed_submit_matches_the_barrier_byte_for_byte() {
    // This is the one test that drives the knob itself, and the env override
    // deliberately outranks the setter, so under a suite-wide
    // `CHUNKLET_WRITE_WINDOW_SQES` run there is no barrier arm to compare
    // against. That run's job is to put every OTHER test through the window.
    if std::env::var_os("CHUNKLET_WRITE_WINDOW_SQES").is_some() {
        eprintln!(
            "[uring_write_window] skipped: CHUNKLET_WRITE_WINDOW_SQES pins the knob, so the \
             barrier arm is unreachable"
        );
        return;
    }
    let mut results = Vec::new();
    let mut before = push_and_wave_totals();
    for window in [0usize, 8, 4096] {
        let dir = TempDir::new().unwrap();
        let (bytes, pushes, waves) = write_batch_and_read_back(&dir, window, 0x5e);
        let delta = (pushes - before.0, waves - before.1);
        before = (pushes, waves);
        results.push((window, bytes, delta));
    }
    set_write_window_sqes(0);

    let (_, baseline, (barrier_pushes, _)) = &results[0];
    // The barrier arm goes through `push_batch`, which this counter does not
    // instrument, so it must contribute no windowed push at all.
    assert_eq!(
        *barrier_pushes, 0,
        "the barrier arm must not report a windowed push"
    );

    for (window, got, (pushes, waves)) in &results[1..] {
        assert_eq!(
            got, baseline,
            "window={window} produced different bytes than the barrier"
        );
        assert!(
            pushes >= waves,
            "window={window} pushed {pushes} times for {waves} waves"
        );
    }

    // 80 unmerged SQEs through an 8-deep window HAS to refill; a window wider
    // than every wave must not.
    let (_, _, (narrow_pushes, narrow_waves)) = &results[1];
    assert!(
        narrow_pushes > narrow_waves,
        "window=8 never refilled the ring: {narrow_pushes} pushes for {narrow_waves} waves"
    );
    let (_, _, (wide_pushes, wide_waves)) = &results[2];
    assert_eq!(
        wide_pushes, wide_waves,
        "a window wider than the batch must be one push per wave"
    );
}
