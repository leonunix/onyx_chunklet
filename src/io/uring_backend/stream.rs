use std::any::Any;
use std::collections::VecDeque;
use std::panic::{catch_unwind, resume_unwind, AssertUnwindSafe};
use std::sync::Arc;
use std::time::Instant;

use io_uring::{opcode, types, IoUring};

use super::batch::{assert_ring_clean, fatal_wait};
use super::{coalesced_write_groups, elapsed_ns, is_direct_aligned, AlignedBuf};
use crate::error::{ChunkletError, ChunkletResult};
use crate::io::backend::{
    DispatchedCompletion, DispatchedWrite, StripWrite, WriteDispatch, WriteDispatchStatus,
};
use crate::pd::PhysicalDisk;

const STREAM_DEPTH: usize = super::URING_DEPTH as usize;
const SLOT_BITS: u32 = 8;
const SLOT_MASK: u64 = (1_u64 << SLOT_BITS) - 1;
const GENERATION_MASK: u64 = u64::MAX >> SLOT_BITS;
const _: () = assert!(STREAM_DEPTH == 1_usize << SLOT_BITS);

enum Payload<'a> {
    Borrowed(&'a [u8]),
    Owned(AlignedBuf),
}

impl Payload<'_> {
    fn as_slice(&self) -> &[u8] {
        match self {
            Self::Borrowed(data) => data,
            Self::Owned(data) => data.as_slice(),
        }
    }
}

struct PreparedWrite<'a> {
    originals: Vec<usize>,
    pd: Arc<PhysicalDisk>,
    chunklet_index: u32,
    in_chunklet_off: u64,
    fd: i32,
    abs: u64,
    payload: Payload<'a>,
}

struct InFlightWrite<'a> {
    generation: u64,
    originals: Vec<usize>,
    pd: Arc<PhysicalDisk>,
    chunklet_index: u32,
    in_chunklet_off: u64,
    payload: Payload<'a>,
    expected: u32,
    submitted_at: Instant,
}

struct DispatchFailure {
    message: String,
}

struct DispatchState {
    source_complete: bool,
    panic: Option<Box<dyn Any + Send>>,
    failure: Option<DispatchFailure>,
    completion_protocol_error: Option<String>,
}

impl DispatchState {
    fn new() -> Self {
        Self {
            source_complete: false,
            panic: None,
            failure: None,
            completion_protocol_error: None,
        }
    }

    fn stopped(&self) -> bool {
        self.panic.is_some() || self.failure.is_some() || self.completion_protocol_error.is_some()
    }

    fn fail(&mut self, message: impl Into<String>) {
        if self.failure.is_none() {
            self.failure = Some(DispatchFailure {
                message: message.into(),
            });
        }
    }

    fn fail_completion_protocol(&mut self, message: impl Into<String>) {
        let message = message.into();
        tracing::error!(
            reason = %message,
            "io_uring completion protocol violation; draining owned slots before failing batch"
        );
        if self.completion_protocol_error.is_none() {
            self.completion_protocol_error = Some(message);
        }
    }
}

pub(super) fn submit_dispatched<'a>(
    ring: &mut IoUring,
    total_ops: usize,
    dispatch: &mut dyn WriteDispatch<'a>,
) -> Vec<ChunkletResult<()>> {
    assert_ring_clean(ring, "io_uring dispatched write");
    let mut output: Vec<Option<ChunkletResult<()>>> =
        std::iter::repeat_with(|| None).take(total_ops).collect();
    let mut slots: Vec<Option<InFlightWrite<'a>>> =
        std::iter::repeat_with(|| None).take(STREAM_DEPTH).collect();
    let mut generations = vec![0_u64; STREAM_DEPTH];
    let mut logical_state = vec![0_u8; total_ops];
    let mut harvested = VecDeque::with_capacity(STREAM_DEPTH);
    let mut active = 0usize;
    let mut state = DispatchState::new();

    let run = catch_unwind(AssertUnwindSafe(|| loop {
        if !state.stopped() && ring.submission().is_empty() {
            fill_ready(
                ring,
                dispatch,
                false,
                total_ops,
                &mut output,
                &mut slots,
                &mut generations,
                &mut logical_state,
                &mut active,
                &mut state,
            );
        }

        if active == 0 {
            if state.source_complete || state.stopped() {
                break;
            }
            fill_ready(
                ring,
                dispatch,
                true,
                total_ops,
                &mut output,
                &mut slots,
                &mut generations,
                &mut logical_state,
                &mut active,
                &mut state,
            );
            if active == 0 {
                if state.source_complete || state.stopped() {
                    break;
                }
                state.fail("blocking write dispatch returned no work");
                break;
            }
        }

        let waited = wait_for_completions(ring);
        if let Some(message) = waited.protocol_error {
            state.fail_completion_protocol(message);
        }
        harvested.extend(waited.completions);
        let mut terminal = Vec::new();
        let mut service_ns = 0_u64;
        while let Some((user_data, result)) = harvested.pop_front() {
            // Do not consume ownership until both slot and generation match.
            // A malformed CQE is not proof that the current slot's borrowed
            // payload reached a terminal state, so leave it installed and wait
            // for its exact CQE before returning the poisoned batch.
            let slot_index = match validate_completion_slot(user_data, slots.len(), |index| {
                slots[index].as_ref().map(|inflight| inflight.generation)
            }) {
                Ok(slot_index) => slot_index,
                Err(message) => {
                    state.fail_completion_protocol(message);
                    continue;
                }
            };
            let inflight = slots[slot_index]
                .take()
                .expect("validated completion slot remains active");
            let tracked_active = slots.iter().filter(|slot| slot.is_some()).count();
            if active != tracked_active.saturating_add(1) {
                state.fail_completion_protocol(format!(
                    "tracked {active} in-flight writes before CQE, but slot table held {}",
                    tracked_active.saturating_add(1)
                ));
            }
            active = tracked_active;
            service_ns = service_ns.max(elapsed_ns(inflight.submitted_at));
            let processed = catch_unwind(AssertUnwindSafe(|| {
                let result = terminal_result(&inflight, result);
                let failed = result.is_err();
                let message = result.err().map(|error| error.to_string());
                let mut completed = Vec::with_capacity(inflight.originals.len());
                for &index in &inflight.originals {
                    if index >= total_ops {
                        return Err(format!(
                            "dispatched completion index {index} outside result range 0..{total_ops}"
                        ));
                    }
                    if output[index].is_some() {
                        return Err(format!("duplicate dispatched result index {index}"));
                    }
                    logical_state[index] = 2;
                    output[index] = Some(match &message {
                        Some(message) => {
                            Err(ChunkletError::Io(std::io::Error::other(message.clone())))
                        }
                        None => Ok(()),
                    });
                    completed.push(DispatchedCompletion { index, failed });
                }
                Ok(completed)
            }));
            match processed {
                Ok(Ok(completed)) => terminal.extend(completed),
                Ok(Err(message)) => state.fail(message),
                Err(payload) => {
                    if state.panic.is_none() {
                        state.panic = Some(payload);
                    }
                }
            }
        }
        notify_completed(dispatch, &terminal, service_ns, &mut state);
    }));
    if let Err(payload) = run {
        if state.panic.is_none() {
            state.panic = Some(payload);
        }
        drain_after_unwind(ring, &mut slots, &mut harvested);
    }

    assert_ring_clean(ring, "io_uring dispatched write");
    if let Some(payload) = state.panic.take() {
        resume_unwind(payload);
    }
    finish_output(output, state)
}

fn finish_output(
    output: Vec<Option<ChunkletResult<()>>>,
    state: DispatchState,
) -> Vec<ChunkletResult<()>> {
    if let Some(message) = state.completion_protocol_error {
        return output
            .into_iter()
            .map(|_| {
                Err(ChunkletError::Invariant(format!(
                    "io_uring completion protocol failed: {message}"
                )))
            })
            .collect();
    }
    let failure = state.failure.map(|failure| failure.message);
    output
        .into_iter()
        .map(|result| {
            result.unwrap_or_else(|| {
                Err(ChunkletError::Invariant(failure.clone().unwrap_or_else(
                    || "io_uring dispatch omitted a write result".into(),
                )))
            })
        })
        .collect()
}

fn validate_completion_slot(
    user_data: u64,
    depth: usize,
    generation_at: impl FnOnce(usize) -> Option<u64>,
) -> Result<usize, String> {
    let slot_index = (user_data & SLOT_MASK) as usize;
    let generation = user_data >> SLOT_BITS;
    if slot_index >= depth {
        return Err(format!("CQE slot {slot_index} outside depth {depth}"));
    }
    let Some(expected_generation) = generation_at(slot_index) else {
        return Err(format!("duplicate or inactive CQE slot {slot_index}"));
    };
    if expected_generation != generation {
        return Err(format!(
            "stale CQE slot {slot_index} generation {generation}, expected {expected_generation}"
        ));
    }
    Ok(slot_index)
}

#[allow(clippy::too_many_arguments)]
fn fill_ready<'a>(
    ring: &mut IoUring,
    dispatch: &mut dyn WriteDispatch<'a>,
    may_wait: bool,
    total_ops: usize,
    output: &mut [Option<ChunkletResult<()>>],
    slots: &mut [Option<InFlightWrite<'a>>],
    generations: &mut [u64],
    logical_state: &mut [u8],
    active: &mut usize,
    state: &mut DispatchState,
) {
    while *active < STREAM_DEPTH && !state.stopped() && !state.source_complete {
        let available = STREAM_DEPTH - *active;
        let status = match catch_unwind(AssertUnwindSafe(|| {
            if may_wait && *active == 0 {
                dispatch.wait_ready(available)
            } else {
                dispatch.poll_ready(available)
            }
        })) {
            Ok(status) => status,
            Err(payload) => {
                state.panic = Some(payload);
                return;
            }
        };
        let admitted = match status {
            WriteDispatchStatus::Ready(admitted) if admitted.is_empty() => {
                state.fail("write dispatch returned an empty ready set");
                return;
            }
            WriteDispatchStatus::Ready(admitted) => admitted,
            WriteDispatchStatus::Pending => {
                if may_wait && *active == 0 {
                    state.fail("blocking write dispatch returned pending");
                }
                return;
            }
            WriteDispatchStatus::Complete => {
                state.source_complete = true;
                return;
            }
        };
        if admitted.len() > available {
            state.fail(format!(
                "write dispatch returned {} ops for {available} available slots",
                admitted.len()
            ));
            return;
        }
        if let Err(message) = validate_admitted(&admitted, total_ops, logical_state) {
            state.fail(message);
            return;
        }
        for admitted in &admitted {
            logical_state[admitted.index] = 1;
        }

        let (prepared, setup_failures) = prepare_writes(admitted);
        for (indices, message) in setup_failures {
            let mut completions = Vec::with_capacity(indices.len());
            for index in indices {
                logical_state[index] = 2;
                output[index] = Some(Err(ChunkletError::Io(std::io::Error::other(
                    message.clone(),
                ))));
                completions.push(DispatchedCompletion {
                    index,
                    failed: true,
                });
            }
            notify_completed(dispatch, &completions, 0, state);
            if state.stopped() {
                return;
            }
        }
        if prepared.is_empty() {
            continue;
        }
        if let Err((indices, message)) = queue_prepared(ring, prepared, slots, generations, active)
        {
            let mut completions = Vec::with_capacity(indices.len());
            for index in indices {
                logical_state[index] = 2;
                output[index] = Some(Err(ChunkletError::Io(std::io::Error::other(
                    message.clone(),
                ))));
                completions.push(DispatchedCompletion {
                    index,
                    failed: true,
                });
            }
            notify_completed(dispatch, &completions, 0, state);
            if state.stopped() {
                return;
            }
        }
    }
}

fn validate_admitted(
    admitted: &[DispatchedWrite<'_>],
    total_ops: usize,
    logical_state: &[u8],
) -> Result<(), String> {
    let mut seen = std::collections::BTreeSet::new();
    for op in admitted {
        if op.index >= total_ops {
            return Err(format!(
                "write dispatch index {} outside result range 0..{total_ops}",
                op.index
            ));
        }
        if logical_state[op.index] != 0 || !seen.insert(op.index) {
            return Err(format!("write dispatch repeated index {}", op.index));
        }
    }
    Ok(())
}

fn prepare_writes<'a>(
    admitted: Vec<DispatchedWrite<'a>>,
) -> (Vec<PreparedWrite<'a>>, Vec<(Vec<usize>, String)>) {
    let writes: Vec<StripWrite<'a>> = admitted
        .iter()
        .map(|admitted| admitted.write.clone())
        .collect();
    let groups = coalesced_write_groups(&writes);
    let mut prepared = Vec::with_capacity(groups.len());
    let mut failures = Vec::new();
    for group in groups {
        let originals: Vec<_> = group.iter().map(|&index| admitted[index].index).collect();
        match prepare_group(&writes, &group, originals.clone()) {
            Ok(write) => prepared.push(write),
            Err(message) => failures.push((originals, message)),
        }
    }
    (prepared, failures)
}

fn prepare_group<'a>(
    writes: &[StripWrite<'a>],
    group: &[usize],
    originals: Vec<usize>,
) -> Result<PreparedWrite<'a>, String> {
    let first = &writes[group[0]];
    let payload = if group.len() == 1 {
        let abs = first
            .pd
            .chunklet_user_abs_offset(
                first.chunklet_index,
                first.in_chunklet_off,
                first.data.len() as u64,
            )
            .map_err(|error| format!("io_uring dispatched offset setup: {error}"))?;
        if is_direct_aligned(abs, first.data.len(), first.data.as_ptr() as usize) {
            Payload::Borrowed(first.data)
        } else {
            Payload::Owned(
                AlignedBuf::from_slice(first.data)
                    .map_err(|error| format!("io_uring dispatched bounce alloc: {error}"))?,
            )
        }
    } else {
        let total_bytes = group.iter().try_fold(0usize, |total, &index| {
            total.checked_add(writes[index].data.len())
        });
        let total_bytes = total_bytes
            .ok_or_else(|| "io_uring dispatched coalesced write length overflow".to_string())?;
        let mut buffer = AlignedBuf::new(total_bytes)
            .map_err(|error| format!("io_uring dispatched coalesced alloc: {error}"))?;
        let mut cursor = 0usize;
        for &index in group {
            let data = writes[index].data;
            buffer.as_mut_slice()[cursor..cursor + data.len()].copy_from_slice(data);
            cursor += data.len();
        }
        Payload::Owned(buffer)
    };
    let len = payload.as_slice().len();
    u32::try_from(len).map_err(|_| "io_uring dispatched write exceeds u32 length".to_string())?;
    let abs = first
        .pd
        .chunklet_user_abs_offset(first.chunklet_index, first.in_chunklet_off, len as u64)
        .map_err(|error| format!("io_uring dispatched offset setup: {error}"))?;
    Ok(PreparedWrite {
        originals,
        pd: first.pd.clone(),
        chunklet_index: first.chunklet_index,
        in_chunklet_off: first.in_chunklet_off,
        fd: first.pd.raw_fd(),
        abs,
        payload,
    })
}

fn queue_prepared<'a>(
    ring: &mut IoUring,
    prepared: Vec<PreparedWrite<'a>>,
    slots: &mut [Option<InFlightWrite<'a>>],
    generations: &mut [u64],
    active: &mut usize,
) -> Result<(), (Vec<usize>, String)> {
    let originals: Vec<_> = prepared
        .iter()
        .flat_map(|prepared| prepared.originals.iter().copied())
        .collect();
    let free_slots: Vec<_> = slots
        .iter()
        .enumerate()
        .filter_map(|(index, slot)| slot.is_none().then_some(index))
        .take(prepared.len())
        .collect();
    if free_slots.len() != prepared.len() {
        return Err((
            originals,
            "io_uring dispatched write exhausted in-flight slots".into(),
        ));
    }

    let mut entries = Vec::with_capacity(prepared.len());
    let mut installed_slots = Vec::with_capacity(prepared.len());
    for (slot_index, prepared) in free_slots.into_iter().zip(prepared) {
        generations[slot_index] = generations[slot_index].wrapping_add(1) & GENERATION_MASK;
        let generation = generations[slot_index];
        let expected = prepared.payload.as_slice().len() as u32;
        let ptr = prepared.payload.as_slice().as_ptr();
        let user_data = (generation << SLOT_BITS) | slot_index as u64;
        entries.push(
            opcode::Write::new(types::Fd(prepared.fd), ptr, expected)
                .offset(prepared.abs)
                .build()
                .user_data(user_data),
        );
        slots[slot_index] = Some(InFlightWrite {
            generation,
            originals: prepared.originals,
            pd: prepared.pd,
            chunklet_index: prepared.chunklet_index,
            in_chunklet_off: prepared.in_chunklet_off,
            payload: prepared.payload,
            expected,
            submitted_at: Instant::now(),
        });
        installed_slots.push(slot_index);
    }

    let push_result = {
        let mut submission = ring.submission();
        let capacity = submission.capacity();
        let available = capacity.saturating_sub(submission.len());
        if entries.len() > available {
            Err(format!(
                "io_uring dispatched write has {} SQEs but only {available}/{capacity} slots",
                entries.len()
            ))
        } else {
            // SAFETY: every entry points at an `InFlightWrite` slot retained
            // until its validated terminal CQE (and exact retry, if needed).
            unsafe {
                submission
                    .push_multiple(&entries)
                    .map_err(|error| format!("io_uring dispatched SQ push failed: {error}"))
            }
        }
    };
    if let Err(message) = push_result {
        // `push_multiple` is atomic, so no SQE references these slots on error.
        for slot_index in installed_slots {
            slots[slot_index] = None;
        }
        return Err((originals, message));
    }
    *active += entries.len();
    Ok(())
}

struct WaitedCompletions {
    completions: Vec<(u64, i32)>,
    protocol_error: Option<String>,
}

fn wait_for_completions(ring: &mut IoUring) -> WaitedCompletions {
    let mut empty_waits = 0_u64;
    loop {
        match ring.submit_and_wait(1) {
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(error) => fatal_wait("io_uring dispatched write", &error),
        }
        let completions: Vec<_> = ring
            .completion()
            .map(|completion| (completion.user_data(), completion.result()))
            .collect();
        if completions.is_empty() {
            // Returning here could release payloads whose SQEs are still in
            // flight. Keep their slots owned and retry the wait; if no exact
            // completion ever arrives this caller remains isolated here.
            empty_waits = empty_waits.saturating_add(1);
            continue;
        }
        return WaitedCompletions {
            completions,
            protocol_error: (empty_waits != 0).then(|| {
                format!("submit_and_wait reported success without a CQE {empty_waits} time(s)")
            }),
        };
    }
}

fn drain_after_unwind<'a>(
    ring: &mut IoUring,
    slots: &mut [Option<InFlightWrite<'a>>],
    harvested: &mut VecDeque<(u64, i32)>,
) {
    while slots.iter().any(Option::is_some) {
        if harvested.is_empty() {
            let waited = wait_for_completions(ring);
            if let Some(message) = waited.protocol_error {
                tracing::error!(
                    reason = %message,
                    "io_uring completion protocol violation while draining after unwind"
                );
            }
            harvested.extend(waited.completions);
        }
        while let Some((user_data, result)) = harvested.pop_front() {
            let slot_index = match validate_completion_slot(user_data, slots.len(), |index| {
                slots[index].as_ref().map(|inflight| inflight.generation)
            }) {
                Ok(slot_index) => slot_index,
                Err(message) => {
                    tracing::error!(
                        reason = %message,
                        "io_uring completion protocol violation while draining after unwind"
                    );
                    continue;
                }
            };
            let inflight = slots[slot_index]
                .take()
                .expect("validated completion slot remains active");
            // A positive short CQE is not terminal until the exact fallback no
            // longer references the payload. Ignore any second panic here; the
            // original unwind is resumed only after every slot is terminal.
            let _ = catch_unwind(AssertUnwindSafe(|| terminal_result(&inflight, result)));
        }
    }
}

fn terminal_result(inflight: &InFlightWrite<'_>, result: i32) -> ChunkletResult<()> {
    if result > 0 && (result as u32) < inflight.expected {
        return inflight
            .pd
            .write_chunklet_user_unbound(
                inflight.chunklet_index,
                inflight.in_chunklet_off,
                inflight.payload.as_slice(),
            )
            .map_err(|error| {
                ChunkletError::Io(std::io::Error::other(format!(
                    "io_uring dispatched short write exact retry failed: {error}"
                )))
            });
    }
    if result < 0 {
        return Err(ChunkletError::Io(std::io::Error::from_raw_os_error(
            -result,
        )));
    }
    if result as u32 == inflight.expected {
        return Ok(());
    }
    let reason = if result == 0 {
        "zero-length"
    } else {
        "oversized"
    };
    Err(ChunkletError::Io(std::io::Error::other(format!(
        "io_uring dispatched {reason} write: {result} for {} bytes",
        inflight.expected
    ))))
}

fn notify_completed(
    dispatch: &mut dyn WriteDispatch<'_>,
    completions: &[DispatchedCompletion],
    service_ns: u64,
    state: &mut DispatchState,
) {
    // A malformed CQE stops refill and poisons the returned batch, but exact
    // slot+generation matches still release scheduler credits while we drain.
    if completions.is_empty() || state.panic.is_some() || state.failure.is_some() {
        return;
    }
    if let Err(payload) = catch_unwind(AssertUnwindSafe(|| {
        dispatch.writes_completed(completions, service_ns)
    })) {
        state.panic = Some(payload);
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeSet, VecDeque};

    use super::*;
    use crate::io::{IoBackendKind, RawDevice};
    use crate::{Pool, PoolConfig};

    struct TestDispatch<'a> {
        ready: VecDeque<DispatchedWrite<'a>>,
        completions: Vec<DispatchedCompletion>,
        panic_on_completion: bool,
    }

    impl<'a> TestDispatch<'a> {
        fn new(ready: Vec<DispatchedWrite<'a>>) -> Self {
            Self {
                ready: ready.into(),
                completions: Vec::new(),
                panic_on_completion: false,
            }
        }

        fn next(&mut self, max_ops: usize) -> WriteDispatchStatus<'a> {
            if self.ready.is_empty() {
                return WriteDispatchStatus::Complete;
            }
            let count = max_ops.min(self.ready.len());
            WriteDispatchStatus::Ready(self.ready.drain(..count).collect())
        }
    }

    impl<'a> WriteDispatch<'a> for TestDispatch<'a> {
        fn poll_ready(&mut self, max_ops: usize) -> WriteDispatchStatus<'a> {
            self.next(max_ops)
        }

        fn wait_ready(&mut self, max_ops: usize) -> WriteDispatchStatus<'a> {
            self.next(max_ops)
        }

        fn writes_completed(&mut self, completions: &[DispatchedCompletion], _service_ns: u64) {
            if self.panic_on_completion {
                panic!("dispatched completion panic");
            }
            self.completions.extend_from_slice(completions);
        }
    }

    fn make_test_pds(count: usize) -> (tempfile::TempDir, Vec<Arc<PhysicalDisk>>) {
        let dir = tempfile::tempdir().unwrap();
        let devices: Vec<_> = (0..count)
            .map(|index| {
                RawDevice::open_or_create(
                    &dir.path().join(format!("pd-{index}")),
                    4 * 1024 * 1024 * 1024,
                )
                .unwrap()
            })
            .collect();
        let pool = Pool::create(
            devices,
            PoolConfig {
                spare_pct: 0,
                io_backend: IoBackendKind::Sync,
            },
        )
        .unwrap();
        let pds = pool
            .list_pds()
            .into_iter()
            .map(|info| pool.pd(info.pd_id).unwrap())
            .collect();
        (dir, pds)
    }

    #[test]
    fn prepare_preserves_global_indices_when_adjacent_writes_coalesce() {
        let (_dir, pds) = make_test_pds(1);
        let data = [
            vec![1_u8; crate::types::BLOCK_SIZE as usize],
            vec![2_u8; crate::types::BLOCK_SIZE as usize],
        ];
        let admitted = vec![
            DispatchedWrite {
                index: 9,
                write: StripWrite {
                    pd: pds[0].clone(),
                    chunklet_index: 0,
                    in_chunklet_off: 0,
                    data: &data[0],
                },
            },
            DispatchedWrite {
                index: 3,
                write: StripWrite {
                    pd: pds[0].clone(),
                    chunklet_index: 0,
                    in_chunklet_off: crate::types::BLOCK_SIZE,
                    data: &data[1],
                },
            },
        ];

        let (prepared, failures) = prepare_writes(admitted);
        assert!(failures.is_empty());
        assert_eq!(prepared.len(), 1);
        assert_eq!(prepared[0].originals, vec![9, 3]);
        assert_eq!(
            prepared[0].payload.as_slice().len(),
            2 * crate::types::BLOCK_SIZE as usize
        );
    }

    #[test]
    fn invalid_cqe_does_not_consume_active_generation() {
        let generations = [Some(7_u64), None];
        let stale = (6_u64 << SLOT_BITS) | 0;
        let error = validate_completion_slot(stale, generations.len(), |index| generations[index])
            .unwrap_err();
        assert!(error.contains("stale CQE slot 0"));

        let inactive = (7_u64 << SLOT_BITS) | 1;
        let error =
            validate_completion_slot(inactive, generations.len(), |index| generations[index])
                .unwrap_err();
        assert!(error.contains("inactive CQE slot 1"));

        let valid = (7_u64 << SLOT_BITS) | 0;
        assert_eq!(
            validate_completion_slot(valid, generations.len(), |index| generations[index]),
            Ok(0)
        );
    }

    #[test]
    fn completion_protocol_error_poisons_every_result() {
        let mut state = DispatchState::new();
        state.fail_completion_protocol("duplicate or inactive CQE slot 3");
        assert!(state.stopped());

        let results = finish_output(vec![Some(Ok(())), None], state);
        assert_eq!(results.len(), 2);
        assert!(results.iter().all(|result| {
            matches!(
                result,
                Err(ChunkletError::Invariant(message))
                    if message.contains("completion protocol failed")
                        && message.contains("inactive CQE slot 3")
            )
        }));
    }

    #[test]
    fn dynamic_ring_returns_original_order_and_reuses_clean_ring() {
        let Ok(mut ring) = IoUring::new(super::super::URING_DEPTH) else {
            return;
        };
        let (_dir, pds) = make_test_pds(2);
        let data: Vec<_> = (0..96)
            .map(|index| vec![(index + 1) as u8; crate::types::BLOCK_SIZE as usize])
            .collect();
        let writes: Vec<_> = (0..96)
            .map(|index| DispatchedWrite {
                index,
                write: StripWrite {
                    pd: pds[index % 2].clone(),
                    chunklet_index: 0,
                    in_chunklet_off: ((index / 2) as u64) * 2 * crate::types::BLOCK_SIZE,
                    data: &data[index],
                },
            })
            .collect();
        let mut dispatch = TestDispatch::new(writes);

        let results = submit_dispatched(&mut ring, data.len(), &mut dispatch);
        assert!(results.into_iter().all(|result| result.is_ok()));
        assert_eq!(dispatch.completions.len(), data.len());
        assert_eq!(
            dispatch
                .completions
                .iter()
                .map(|completion| completion.index)
                .collect::<BTreeSet<_>>(),
            (0..data.len()).collect()
        );

        let retry_data = vec![0x5a; crate::types::BLOCK_SIZE as usize];
        let mut retry = TestDispatch::new(vec![DispatchedWrite {
            index: 0,
            write: StripWrite {
                pd: pds[0].clone(),
                chunklet_index: 0,
                in_chunklet_off: 64 * crate::types::BLOCK_SIZE,
                data: &retry_data,
            },
        }]);
        assert!(submit_dispatched(&mut ring, 1, &mut retry)[0].is_ok());
    }

    #[test]
    fn completion_panic_drains_ring_before_unwind() {
        let Ok(mut ring) = IoUring::new(super::super::URING_DEPTH) else {
            return;
        };
        let (_dir, pds) = make_test_pds(1);
        let data: Vec<_> = (0..80)
            .map(|index| vec![(index + 1) as u8; crate::types::BLOCK_SIZE as usize])
            .collect();
        let writes: Vec<_> = (0..80)
            .map(|index| DispatchedWrite {
                index,
                write: StripWrite {
                    pd: pds[0].clone(),
                    chunklet_index: 0,
                    in_chunklet_off: (index as u64) * 2 * crate::types::BLOCK_SIZE,
                    data: &data[index],
                },
            })
            .collect();
        let mut dispatch = TestDispatch::new(writes);
        dispatch.panic_on_completion = true;
        let unwind = catch_unwind(AssertUnwindSafe(|| {
            submit_dispatched(&mut ring, data.len(), &mut dispatch)
        }));
        assert!(unwind.is_err());
        assert_ring_clean(&mut ring, "post-dispatch-panic test");

        let retry_data = vec![0x7c; crate::types::BLOCK_SIZE as usize];
        let mut retry = TestDispatch::new(vec![DispatchedWrite {
            index: 0,
            write: StripWrite {
                pd: pds[0].clone(),
                chunklet_index: 0,
                in_chunklet_off: 128 * crate::types::BLOCK_SIZE,
                data: &retry_data,
            },
        }]);
        assert!(submit_dispatched(&mut ring, 1, &mut retry)[0].is_ok());
    }
}
