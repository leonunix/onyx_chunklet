//! The pipelined RAID6 batched writer must be byte-for-byte equivalent to the
//! two-phase one.
//!
//! `raid6_pipeline_window_stripes` changes only WHEN a segment's syndromes are
//! computed relative to its siblings' submission — never what is written. The
//! interesting failure modes are all bookkeeping: a segment's strips handed out
//! twice, a result attributed to the wrong op (which would make
//! `absorb_degraded` blame the wrong member), or a segment mutated while its
//! strips are still in flight. The last is a use-after-free and is silent, so
//! this file reads the data back and compares media bytes rather than trusting
//! the returned results.
//!
//! Its own integration binary because the knob is process-global.

mod common;

use onyx_chunklet::io::IoBackendKind;
use onyx_chunklet::ld::raid6::{pipeline_window_stripes, set_pipeline_window_stripes};
use onyx_chunklet::pool::LdSpec;
use tempfile::TempDir;

use common::{make_pool_with, pattern};

/// Two blocks, so a sub-strip write is genuinely partial.
const STRIP_LOG2: u8 = 13;
const STRIP: usize = 1 << STRIP_LOG2;
const DATA: usize = 3;
/// More than one stripe per call is the whole premise: with a single segment
/// there is nothing to overlap and the writer keeps the two-phase path.
const STRIPES: usize = 12;

/// `(pipeline_ns, write_ns)` — non-zero `pipeline_ns` is the only direct
/// evidence the interleaved writer actually ran, so without this the test would
/// pass even if the knob were ignored.
fn pipeline_and_write_ns() -> (u64, u64) {
    let stats = onyx_chunklet::write_path_stats();
    (stats.r6_pipeline_ns, stats.r6_write_ns)
}

fn write_batches_and_read_back(dir: &TempDir, window: usize) -> Vec<u8> {
    set_pipeline_window_stripes(window);
    assert_eq!(pipeline_window_stripes(), window, "pipeline knob did not take");

    let (pool, _paths) = make_pool_with(dir, 5, IoBackendKind::Uring);
    let ld_id = pool.create_ld(LdSpec::raid6(DATA as u8, 1, 1, STRIP_LOG2)).unwrap();
    let ld = pool.open_ld(ld_id).unwrap();

    let full_stripe = DATA * STRIP;
    let total = STRIPES * full_stripe;
    let payload = pattern(0x7c, total, 0);

    // Full-stripe arm: every segment is zero-RMW, so the pipeline hands out
    // K+2 strips per segment with no read phase in front of it.
    let full: Vec<(u64, &[u8])> = (0..STRIPES)
        .map(|i| {
            let start = i * full_stripe;
            ((start as u64), &payload[start..start + full_stripe])
        })
        .collect();
    ld.write_many_at(&full).unwrap();

    // Sub-strip arm: each segment now carries old-data + old P/Q reads in
    // Phase 1 and a delta-folded P/Q in the interleaved compute.
    let partial: Vec<(u64, &[u8])> = (0..STRIPES)
        .map(|i| {
            let start = i * full_stripe + STRIP;
            ((start as u64), &payload[start..start + STRIP / 2])
        })
        .collect();
    ld.write_many_at(&partial).unwrap();

    // Strided, so no two segments are stripe-adjacent — the shape that keeps
    // the streaming submit's groups singleton.
    let strided: Vec<(u64, &[u8])> = (0..STRIPES)
        .step_by(2)
        .map(|i| {
            let start = i * full_stripe;
            ((start as u64), &payload[start..start + full_stripe])
        })
        .collect();
    ld.write_many_at(&strided).unwrap();

    let mut readback = vec![0u8; total];
    ld.read_at(0, &mut readback).unwrap();
    assert_eq!(
        readback, payload,
        "window={window} readback differs from what was written"
    );
    readback
}

#[test]
fn pipelined_writer_matches_the_two_phase_writer_byte_for_byte() {
    if std::env::var_os("CHUNKLET_R6_PIPELINE_STRIPES").is_some() {
        eprintln!(
            "[raid6_pipelined_writer] skipped: CHUNKLET_R6_PIPELINE_STRIPES pins the knob, so \
             the two-phase arm is unreachable"
        );
        return;
    }

    let mut results = Vec::new();
    let mut before = pipeline_and_write_ns();
    for window in [0usize, 1, 4] {
        let dir = TempDir::new().unwrap();
        let bytes = write_batches_and_read_back(&dir, window);
        let now = pipeline_and_write_ns();
        results.push((window, bytes, now.0 - before.0, now.1 - before.1));
        before = now;
    }
    set_pipeline_window_stripes(0);

    let (_, baseline, two_phase_pipeline_ns, two_phase_write_ns) = &results[0];
    assert_eq!(
        *two_phase_pipeline_ns, 0,
        "the two-phase arm must not report any pipelined time"
    );
    assert!(
        *two_phase_write_ns > 0,
        "the two-phase arm must still report a submit leg"
    );

    for (window, got, pipeline_ns, write_ns) in &results[1..] {
        assert_eq!(
            got, baseline,
            "window={window} produced different bytes than the two-phase writer"
        );
        assert!(
            *pipeline_ns > 0,
            "window={window} did not take the pipelined path"
        );
        // The pipelined leg IS the submit leg, so it must account for it.
        assert!(
            *pipeline_ns <= *write_ns,
            "window={window} pipeline_ns {pipeline_ns} exceeds write_ns {write_ns}"
        );
    }
}
