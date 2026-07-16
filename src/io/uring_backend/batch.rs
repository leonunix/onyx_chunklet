//! Safety-critical io_uring batch submission and completion collection.
//!
//! Once SQEs containing borrowed pointers are published, unwinding is not a
//! valid error path: the kernel may still dereference those pointers. This
//! module therefore retries interruptible waits, drains every CQE in the
//! batch, and fail-stops on a non-recoverable wait error.

use std::any::Any;
use std::fmt;
use std::panic::{catch_unwind, resume_unwind, AssertUnwindSafe};

use io_uring::{squeue, IoUring};

use crate::error::{ChunkletError, ChunkletResult};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct RawCompletion {
    user_data: u64,
    result: i32,
}

/// One in-range, first-seen completion from the current CQ drain. Invalid or
/// duplicate `user_data` is retained as a protocol error but never published
/// to an observer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct ValidatedCompletion {
    pub(super) index: usize,
    pub(super) result: i32,
}

#[derive(Debug)]
pub(super) struct BatchCompletions {
    pub(super) results: Vec<Option<i32>>,
    pub(super) protocol_error: Option<String>,
}

struct DrivenBatch {
    completions: BatchCompletions,
    observer_panic: Option<Box<dyn Any + Send>>,
}

impl fmt::Debug for DrivenBatch {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DrivenBatch")
            .field("completions", &self.completions)
            .field("observer_panicked", &self.observer_panic.is_some())
            .finish()
    }
}

/// Publish an entire batch atomically. `push_multiple` guarantees that a
/// capacity failure leaves the SQ unchanged, so callers may safely return
/// while their borrowed buffers are still owned by the caller.
pub(super) fn push_batch(
    ring: &mut IoUring,
    entries: &[squeue::Entry],
    context: &str,
) -> ChunkletResult<()> {
    if entries.is_empty() {
        return Ok(());
    }

    assert_ring_clean(ring, context);
    let mut sq = ring.submission();
    let capacity = sq.capacity();
    let queued = sq.len();
    ensure_atomic_push_capacity(capacity, queued, entries.len(), context)?;
    // SAFETY: the caller retains every fd and referenced buffer until
    // `wait_and_drain` has collected this entire batch.
    unsafe {
        sq.push_multiple(entries).map_err(|error| {
            ChunkletError::Io(std::io::Error::other(format!(
                "{context} atomic SQ push failed despite reserved capacity: {error}"
            )))
        })?;
    }
    Ok(())
}

/// Wait for and collect every completion belonging to the just-pushed batch.
/// A fatal `io_uring_enter` error is ambiguous: some SQEs may already be in
/// flight. Returning would release borrowed/bounce buffers and cause UAF, so
/// the only safe response is an explicit process fail-stop.
pub(super) fn wait_and_drain(
    ring: &mut IoUring,
    expected: usize,
    context: &str,
) -> BatchCompletions {
    wait_and_drain_observed(ring, expected, context, |_| {})
}

/// Variant of [`wait_and_drain`] that publishes each CQ drain after validating
/// `user_data`. Observer panics are captured while borrowed buffers may still
/// be in flight, all remaining CQEs are harvested, and the panic resumes only
/// after the ring-clean invariant has been checked.
pub(super) fn wait_and_drain_observed(
    ring: &mut IoUring,
    expected: usize,
    context: &str,
    mut on_drain: impl FnMut(&[ValidatedCompletion]),
) -> BatchCompletions {
    match drive_completions_observed(
        expected,
        || {
            ring.submit_and_wait(1)?;
            let completions = ring
                .completion()
                .map(|cqe| RawCompletion {
                    user_data: cqe.user_data(),
                    result: cqe.result(),
                })
                .collect();
            Ok(completions)
        },
        &mut on_drain,
    ) {
        Ok(driven) => {
            assert_ring_clean(ring, context);
            if let Some(error) = &driven.completions.protocol_error {
                // A duplicate/out-of-range CQE means the raw completion count
                // can no longer prove that every SQE referencing a borrowed
                // buffer reached a terminal state. Returning here could free a
                // buffer that the kernel still owns, so preserve the same
                // fail-stop contract as an ambiguous wait failure.
                fatal_protocol(context, error);
            }
            if let Some(payload) = driven.observer_panic {
                resume_unwind(payload);
            }
            driven.completions
        }
        Err(error) => fatal_wait(context, &error),
    }
}

fn ensure_atomic_push_capacity(
    capacity: usize,
    queued: usize,
    requested: usize,
    context: &str,
) -> ChunkletResult<()> {
    let available = capacity.saturating_sub(queued);
    if requested > available {
        return Err(ChunkletError::Io(std::io::Error::other(format!(
            "{context} batch has {requested} SQEs but only {available}/{capacity} slots are free"
        ))));
    }
    Ok(())
}

pub(super) fn assert_ring_clean(ring: &mut IoUring, context: &str) {
    let pending_cqes = ring.completion().len();
    let pending_sqes = ring.submission().len();
    if pending_cqes != 0 || pending_sqes != 0 {
        fatal_protocol(
            context,
            &format!(
                "ring reused with {pending_sqes} pending SQEs and {pending_cqes} unharvested CQEs"
            ),
        );
    }
}

#[cfg(test)]
fn drive_completions(
    expected: usize,
    wait_once: impl FnMut() -> std::io::Result<Vec<RawCompletion>>,
) -> std::io::Result<BatchCompletions> {
    Ok(drive_completions_observed(expected, wait_once, &mut |_| {})?.completions)
}

fn drive_completions_observed(
    expected: usize,
    mut wait_once: impl FnMut() -> std::io::Result<Vec<RawCompletion>>,
    on_drain: &mut impl FnMut(&[ValidatedCompletion]),
) -> std::io::Result<DrivenBatch> {
    let mut results = vec![None; expected];
    let mut completed = 0usize;
    let mut protocol_errors = Vec::new();
    let mut observer_panic = None;

    while completed < expected {
        let completions = match wait_once() {
            Ok(completions) => completions,
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error),
        };
        if completions.is_empty() {
            return Err(std::io::Error::other(
                "io_uring wait reported success without a CQE",
            ));
        }

        let mut validated = Vec::with_capacity(completions.len());
        for completion in completions {
            completed = completed.saturating_add(1);
            let Ok(index) = usize::try_from(completion.user_data) else {
                protocol_errors.push(format!(
                    "CQE user_data {} does not fit usize",
                    completion.user_data
                ));
                continue;
            };
            if index >= expected {
                protocol_errors.push(format!(
                    "CQE user_data {index} outside batch range 0..{expected}"
                ));
                continue;
            }
            if results[index].is_some() {
                protocol_errors.push(format!("duplicate CQE user_data {index}"));
                continue;
            }
            results[index] = Some(completion.result);
            validated.push(ValidatedCompletion {
                index,
                result: completion.result,
            });
        }
        if !validated.is_empty() && observer_panic.is_none() {
            if let Err(payload) = catch_unwind(AssertUnwindSafe(|| on_drain(&validated))) {
                observer_panic = Some(payload);
            }
        }
    }

    if completed != expected {
        protocol_errors.push(format!(
            "received {completed} CQEs for a batch of {expected}"
        ));
    }
    let missing: Vec<_> = results
        .iter()
        .enumerate()
        .filter_map(|(index, result)| result.is_none().then_some(index))
        .collect();
    if !missing.is_empty() {
        protocol_errors.push(format!("missing CQEs for indices {missing:?}"));
    }

    Ok(DrivenBatch {
        completions: BatchCompletions {
            results,
            protocol_error: (!protocol_errors.is_empty()).then(|| protocol_errors.join("; ")),
        },
        observer_panic,
    })
}

pub(super) fn fatal_wait(context: &str, error: &std::io::Error) -> ! {
    tracing::error!(%error, context, "fatal io_uring wait with possibly in-flight borrowed buffers");
    eprintln!(
        "fatal: {context}: io_uring wait failed with possibly in-flight borrowed buffers: {error}"
    );
    std::process::abort()
}

pub(super) fn fatal_protocol(context: &str, reason: &str) -> ! {
    tracing::error!(context, reason, "fatal dirty io_uring reuse");
    eprintln!("fatal: {context}: dirty io_uring reuse: {reason}");
    std::process::abort()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;
    use std::collections::VecDeque;
    use std::panic::{catch_unwind, AssertUnwindSafe};

    #[test]
    fn interrupted_wait_retries_and_partial_progress_is_collected() {
        let mut steps = VecDeque::from([
            Err(std::io::Error::from(std::io::ErrorKind::Interrupted)),
            Ok(vec![RawCompletion {
                user_data: 1,
                result: 4096,
            }]),
            Ok(vec![
                RawCompletion {
                    user_data: 0,
                    result: 4096,
                },
                RawCompletion {
                    user_data: 2,
                    result: -libc::EIO,
                },
            ]),
        ]);
        let mut calls = 0;
        let batch = drive_completions(3, || {
            calls += 1;
            steps.pop_front().expect("injected wait step")
        })
        .unwrap();

        assert_eq!(calls, 3);
        assert_eq!(
            batch.results,
            vec![Some(4096), Some(4096), Some(-libc::EIO)]
        );
        assert_eq!(batch.protocol_error, None);
    }

    #[test]
    fn multiple_cq_drains_publish_validated_results_in_arrival_waves() {
        let mut steps = VecDeque::from([
            Ok(vec![RawCompletion {
                user_data: 2,
                result: -libc::EIO,
            }]),
            Ok(vec![
                RawCompletion {
                    user_data: 0,
                    result: 4096,
                },
                RawCompletion {
                    user_data: 1,
                    result: 8192,
                },
            ]),
        ]);
        let mut waves = Vec::new();
        let driven = drive_completions_observed(
            3,
            || steps.pop_front().expect("injected wait step"),
            &mut |completions| waves.push(completions.to_vec()),
        )
        .unwrap();

        assert!(driven.observer_panic.is_none());
        assert_eq!(
            waves,
            vec![
                vec![ValidatedCompletion {
                    index: 2,
                    result: -libc::EIO,
                }],
                vec![
                    ValidatedCompletion {
                        index: 0,
                        result: 4096,
                    },
                    ValidatedCompletion {
                        index: 1,
                        result: 8192,
                    },
                ],
            ]
        );
    }

    #[test]
    fn duplicate_and_invalid_cqes_are_not_published_twice() {
        let mut steps = VecDeque::from([
            Ok(vec![RawCompletion {
                user_data: 0,
                result: 4096,
            }]),
            Ok(vec![
                RawCompletion {
                    user_data: 0,
                    result: -libc::EIO,
                },
                RawCompletion {
                    user_data: u64::MAX,
                    result: 4096,
                },
                RawCompletion {
                    user_data: 1,
                    result: 4096,
                },
            ]),
        ]);
        let mut published = Vec::new();
        let driven = drive_completions_observed(
            4,
            || steps.pop_front().expect("injected wait step"),
            &mut |completions| {
                published.extend(completions.iter().map(|completion| completion.index))
            },
        )
        .unwrap();

        assert_eq!(published, vec![0, 1]);
        assert_eq!(driven.completions.results[0], Some(4096));
        assert_eq!(driven.completions.results[1], Some(4096));
        let error = driven.completions.protocol_error.unwrap();
        assert!(error.contains("duplicate"));
        assert!(error.contains("user_data"));
        assert!(error.contains("missing CQEs"));
    }

    #[test]
    fn observer_panic_is_held_until_all_cqes_are_drained() {
        let waits = Cell::new(0usize);
        let callbacks = Cell::new(0usize);
        let unwind = catch_unwind(AssertUnwindSafe(|| {
            let driven = drive_completions_observed(
                2,
                || {
                    let index = waits.get();
                    waits.set(index + 1);
                    Ok(vec![RawCompletion {
                        user_data: index as u64,
                        result: 4096,
                    }])
                },
                &mut |_| {
                    callbacks.set(callbacks.get() + 1);
                    panic!("observer panic");
                },
            )
            .unwrap();
            assert_eq!(waits.get(), 2, "panic escaped before the final CQE");
            resume_unwind(driven.observer_panic.expect("observer panic retained"));
        }));

        assert!(unwind.is_err());
        assert_eq!(waits.get(), 2);
        assert_eq!(
            callbacks.get(),
            1,
            "observer must stay disabled after panic"
        );
    }

    #[test]
    fn protocol_error_is_retained_alongside_observer_panic() {
        let mut steps = VecDeque::from([
            Ok(vec![RawCompletion {
                user_data: 0,
                result: 4096,
            }]),
            Ok(vec![RawCompletion {
                user_data: 0,
                result: 4096,
            }]),
        ]);
        let driven = drive_completions_observed(
            2,
            || steps.pop_front().expect("injected wait step"),
            &mut |_| panic!("observer panic"),
        )
        .unwrap();

        assert!(driven.observer_panic.is_some());
        let error = driven.completions.protocol_error.unwrap();
        assert!(error.contains("duplicate"));
        assert!(error.contains("missing CQEs"));
    }

    #[test]
    fn bad_user_data_is_bounded_and_all_completions_are_drained() {
        let mut calls = 0;
        let batch = drive_completions(2, || {
            calls += 1;
            Ok(vec![
                RawCompletion {
                    user_data: u64::MAX,
                    result: 4096,
                },
                RawCompletion {
                    user_data: 0,
                    result: 4096,
                },
            ])
        })
        .unwrap();

        assert_eq!(calls, 1);
        assert_eq!(batch.results, vec![Some(4096), None]);
        let error = batch.protocol_error.unwrap();
        assert!(error.contains("user_data"));
        assert!(error.contains("missing CQEs"));
    }

    #[test]
    fn atomic_capacity_check_prevents_partial_sq_push() {
        assert!(ensure_atomic_push_capacity(64, 0, 64, "test").is_ok());
        let error = ensure_atomic_push_capacity(64, 1, 64, "test").unwrap_err();
        assert!(error.to_string().contains("only 63/64 slots are free"));
    }

    #[test]
    fn non_interrupted_wait_error_is_reported_for_fail_stop_policy() {
        let error =
            drive_completions(1, || Err(std::io::Error::from_raw_os_error(libc::EIO))).unwrap_err();
        assert_eq!(error.raw_os_error(), Some(libc::EIO));
    }
}
