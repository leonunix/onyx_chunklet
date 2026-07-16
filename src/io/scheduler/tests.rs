use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use super::*;
use crate::io::{
    DispatchedCompletion, IoBackendKind, RawDevice, WriteCompletionObserver, WriteDispatch,
    WriteDispatchStatus,
};
use crate::{Pool, PoolConfig};

struct CountingBackend {
    calls: AtomicUsize,
    max_ops: AtomicUsize,
}

#[derive(Default)]
struct OneAtATimeDispatchBackend {
    pds: Mutex<Vec<PdId>>,
}

struct DuplicateDispatchCompletionBackend;

struct StreamingGateBackend {
    first_pd_done: mpsc::Sender<()>,
    release_second_pd: Mutex<mpsc::Receiver<()>>,
    followup_done: mpsc::Sender<()>,
}

impl IoBackend for StreamingGateBackend {
    fn submit_reads(&self, _ops: &mut [StripRead<'_>]) -> ChunkletResult<()> {
        Ok(())
    }

    fn submit_writes_detailed(&self, ops: &[StripWrite<'_>]) -> Vec<ChunkletResult<()>> {
        (0..ops.len()).map(|_| Ok(())).collect()
    }

    fn submit_writes_detailed_observed_with_class(
        &self,
        _class: IoClass,
        ops: &[StripWrite<'_>],
        observer: &dyn WriteCompletionObserver,
    ) -> Vec<ChunkletResult<()>> {
        if ops.len() == 2 {
            observer.writes_completed(&[0], 10);
            self.first_pd_done.send(()).unwrap();
            self.release_second_pd.lock().recv().unwrap();
            observer.writes_completed(&[1], 80);
            vec![Err(ChunkletError::Invariant("first result".into())), Ok(())]
        } else {
            assert_eq!(ops.len(), 1);
            observer.writes_completed(&[0], 5);
            self.followup_done.send(()).unwrap();
            vec![Ok(())]
        }
    }

    fn submit_writes_dispatched_with_class<'a>(
        &self,
        _class: IoClass,
        total_ops: usize,
        dispatch: &mut dyn WriteDispatch<'a>,
    ) -> Vec<ChunkletResult<()>> {
        let mut output: Vec<Option<ChunkletResult<()>>> =
            std::iter::repeat_with(|| None).take(total_ops).collect();
        let initial = match dispatch.wait_ready(total_ops.max(1)) {
            WriteDispatchStatus::Ready(initial) => initial,
            WriteDispatchStatus::Pending => panic!("initial dispatch unexpectedly pending"),
            WriteDispatchStatus::Complete => return Vec::new(),
        };
        if initial.len() == 1 {
            let index = initial[0].index;
            output[index] = Some(Ok(()));
            dispatch.writes_completed(
                &[DispatchedCompletion {
                    index,
                    failed: false,
                }],
                5,
            );
            self.followup_done.send(()).unwrap();
        } else {
            assert_eq!(initial.len(), 2);
            let fast_position = initial
                .iter()
                .position(|admitted| admitted.write.data.first() == Some(&1))
                .expect("streaming gate fast write missing");
            let slow_position = 1 - fast_position;
            let fast_index = initial[fast_position].index;
            let slow_index = initial[slow_position].index;
            output[fast_index] = Some(Err(ChunkletError::Invariant("first result".into())));
            dispatch.writes_completed(
                &[DispatchedCompletion {
                    index: fast_index,
                    failed: true,
                }],
                10,
            );
            self.first_pd_done.send(()).unwrap();

            if let WriteDispatchStatus::Ready(refill) = dispatch.poll_ready(total_ops.max(1)) {
                for admitted in refill {
                    output[admitted.index] = Some(Ok(()));
                    dispatch.writes_completed(
                        &[DispatchedCompletion {
                            index: admitted.index,
                            failed: false,
                        }],
                        5,
                    );
                    self.followup_done.send(()).unwrap();
                }
            }

            self.release_second_pd.lock().recv().unwrap();
            output[slow_index] = Some(Ok(()));
            dispatch.writes_completed(
                &[DispatchedCompletion {
                    index: slow_index,
                    failed: false,
                }],
                80,
            );
        }
        output
            .into_iter()
            .map(|result| {
                result.unwrap_or_else(|| {
                    Err(ChunkletError::Invariant(
                        "streaming gate omitted dispatched result".into(),
                    ))
                })
            })
            .collect()
    }

    fn name(&self) -> &'static str {
        "streaming-gate"
    }
}

struct PartialCompletionPanicBackend;

struct RecordingBackend {
    calls: mpsc::Sender<Vec<PdId>>,
}

#[derive(Default)]
struct MarkerResultBackend {
    calls: Mutex<Vec<Vec<u8>>>,
}

impl IoBackend for RecordingBackend {
    fn submit_reads(&self, _ops: &mut [StripRead<'_>]) -> ChunkletResult<()> {
        Ok(())
    }

    fn submit_writes_detailed(&self, _ops: &[StripWrite<'_>]) -> Vec<ChunkletResult<()>> {
        panic!("non-observed path must not be used")
    }

    fn submit_writes_detailed_observed_with_class(
        &self,
        _class: IoClass,
        ops: &[StripWrite<'_>],
        observer: &dyn WriteCompletionObserver,
    ) -> Vec<ChunkletResult<()>> {
        self.calls
            .send(ops.iter().map(|op| op.pd.pd_id()).collect())
            .unwrap();
        observer.writes_completed(&(0..ops.len()).collect::<Vec<_>>(), 1);
        (0..ops.len()).map(|_| Ok(())).collect()
    }

    fn name(&self) -> &'static str {
        "recording"
    }
}

impl IoBackend for MarkerResultBackend {
    fn submit_reads(&self, _ops: &mut [StripRead<'_>]) -> ChunkletResult<()> {
        Ok(())
    }

    fn submit_writes_detailed(&self, _ops: &[StripWrite<'_>]) -> Vec<ChunkletResult<()>> {
        panic!("non-observed path must not be used")
    }

    fn submit_writes_detailed_observed_with_class(
        &self,
        _class: IoClass,
        ops: &[StripWrite<'_>],
        observer: &dyn WriteCompletionObserver,
    ) -> Vec<ChunkletResult<()>> {
        let markers: Vec<_> = ops
            .iter()
            .map(|op| op.data.first().copied().unwrap_or_default())
            .collect();
        self.calls.lock().push(markers.clone());
        observer.writes_completed(&(0..ops.len()).collect::<Vec<_>>(), 1);
        markers
            .into_iter()
            .map(|marker| {
                if marker % 2 == 0 {
                    Ok(())
                } else {
                    Err(ChunkletError::Invariant(format!("marker-{marker}")))
                }
            })
            .collect()
    }

    fn name(&self) -> &'static str {
        "marker-result"
    }
}

impl IoBackend for PartialCompletionPanicBackend {
    fn submit_reads(&self, _ops: &mut [StripRead<'_>]) -> ChunkletResult<()> {
        Ok(())
    }

    fn submit_writes_detailed(&self, _ops: &[StripWrite<'_>]) -> Vec<ChunkletResult<()>> {
        panic!("non-observed path must not be used")
    }

    fn submit_writes_detailed_observed_with_class(
        &self,
        _class: IoClass,
        _ops: &[StripWrite<'_>],
        observer: &dyn WriteCompletionObserver,
    ) -> Vec<ChunkletResult<()>> {
        observer.writes_completed(&[0], 10);
        panic!("simulated streaming backend panic")
    }

    fn name(&self) -> &'static str {
        "partial-completion-panic"
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

impl CountingBackend {
    fn new() -> Self {
        Self {
            calls: AtomicUsize::new(0),
            max_ops: AtomicUsize::new(0),
        }
    }
}

impl IoBackend for CountingBackend {
    fn submit_reads(&self, _ops: &mut [StripRead<'_>]) -> ChunkletResult<()> {
        Ok(())
    }

    fn submit_writes_detailed(&self, ops: &[StripWrite<'_>]) -> Vec<ChunkletResult<()>> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        self.max_ops.fetch_max(ops.len(), Ordering::Relaxed);
        (0..ops.len()).map(|_| Ok(())).collect()
    }

    fn name(&self) -> &'static str {
        "counting"
    }
}

impl IoBackend for OneAtATimeDispatchBackend {
    fn submit_reads(&self, _ops: &mut [StripRead<'_>]) -> ChunkletResult<()> {
        Ok(())
    }

    fn submit_writes_detailed(&self, ops: &[StripWrite<'_>]) -> Vec<ChunkletResult<()>> {
        (0..ops.len()).map(|_| Ok(())).collect()
    }

    fn submit_writes_dispatched_with_class<'a>(
        &self,
        _class: IoClass,
        total_ops: usize,
        dispatch: &mut dyn WriteDispatch<'a>,
    ) -> Vec<ChunkletResult<()>> {
        let mut output: Vec<Option<ChunkletResult<()>>> =
            std::iter::repeat_with(|| None).take(total_ops).collect();
        loop {
            let admitted = match dispatch.wait_ready(1) {
                WriteDispatchStatus::Ready(admitted) => admitted,
                WriteDispatchStatus::Pending => {
                    panic!("one-at-a-time dispatch unexpectedly pending")
                }
                WriteDispatchStatus::Complete => break,
            };
            assert_eq!(admitted.len(), 1);
            let admitted = &admitted[0];
            self.pds.lock().push(admitted.write.pd.pd_id());
            output[admitted.index] = Some(Ok(()));
            dispatch.writes_completed(
                &[DispatchedCompletion {
                    index: admitted.index,
                    failed: false,
                }],
                1,
            );
        }
        output
            .into_iter()
            .map(|result| result.expect("one-at-a-time backend omitted result"))
            .collect()
    }

    fn name(&self) -> &'static str {
        "one-at-a-time-dispatch"
    }
}

impl IoBackend for DuplicateDispatchCompletionBackend {
    fn submit_reads(&self, _ops: &mut [StripRead<'_>]) -> ChunkletResult<()> {
        Ok(())
    }

    fn submit_writes_detailed(&self, ops: &[StripWrite<'_>]) -> Vec<ChunkletResult<()>> {
        (0..ops.len()).map(|_| Ok(())).collect()
    }

    fn submit_writes_dispatched_with_class<'a>(
        &self,
        _class: IoClass,
        total_ops: usize,
        dispatch: &mut dyn WriteDispatch<'a>,
    ) -> Vec<ChunkletResult<()>> {
        let admitted = match dispatch.wait_ready(total_ops.max(1)) {
            WriteDispatchStatus::Ready(admitted) => admitted,
            WriteDispatchStatus::Pending => panic!("duplicate backend unexpectedly pending"),
            WriteDispatchStatus::Complete => return Vec::new(),
        };
        let index = admitted[0].index;
        let completion = DispatchedCompletion {
            index,
            failed: false,
        };
        dispatch.writes_completed(&[completion, completion], 1);
        (0..total_ops).map(|_| Ok(())).collect()
    }

    fn name(&self) -> &'static str {
        "duplicate-dispatch-completion"
    }
}

fn controller(config: SchedulerConfig) -> Arc<AdmissionController> {
    AdmissionController::new(config).unwrap()
}

fn demand(pd_id: PdId, blocks: u64) -> Demand {
    Demand {
        pd_id,
        blocks,
        requested_blocks: blocks,
        exclusive: false,
    }
}

fn class_snapshot(
    controller: &AdmissionController,
    pd_id: PdId,
    class: IoClass,
) -> IoClassSnapshot {
    controller
        .snapshot()
        .pds
        .into_iter()
        .find(|pd| pd.pd_id == pd_id)
        .unwrap()
        .classes
        .into_iter()
        .find(|current| current.class == class)
        .unwrap()
}

fn wait_until(mut predicate: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(2);
    while !predicate() {
        assert!(
            Instant::now() < deadline,
            "timed out waiting for scheduler state"
        );
        thread::sleep(Duration::from_millis(1));
    }
}

#[test]
fn accounts_ceil_blocks_and_aggregates_each_pd() {
    let first = PdId::new_v4();
    let second = PdId::new_v4();
    assert_eq!(blocks_for_len(0).unwrap(), 0);
    assert_eq!(blocks_for_len(1).unwrap(), 1);
    assert_eq!(blocks_for_len(4096).unwrap(), 1);
    assert_eq!(blocks_for_len(4097).unwrap(), 2);

    let lanes = plan_work(&[(first, 1), (second, 2), (first, 1), (first, 0)], 8, 8).unwrap();
    assert_eq!(lanes[&first][0].indices, vec![0, 2, 3]);
    assert_eq!(lanes[&first][0].demand.blocks, 2);
    assert_eq!(lanes[&second][0].indices, vec![1]);
    assert_eq!(lanes[&second][0].demand.blocks, 2);
}

#[test]
fn different_pds_have_independent_credits() {
    let scheduler = controller(SchedulerConfig::new(4));
    let first = PdId::new_v4();
    let second = PdId::new_v4();
    let first_permit = scheduler
        .admit(IoClass::Foreground, vec![demand(first, 4)])
        .unwrap();
    let second_permit = scheduler
        .admit(IoClass::Foreground, vec![demand(second, 4)])
        .unwrap();
    assert_eq!(
        class_snapshot(&scheduler, first, IoClass::Foreground).active_blocks,
        4
    );
    assert_eq!(
        class_snapshot(&scheduler, second, IoClass::Foreground).active_blocks,
        4
    );
    drop((first_permit, second_permit));
}

#[test]
fn multi_pd_queue_admits_ready_pd_without_waiting_for_busy_pd() {
    let scheduler = controller(SchedulerConfig::new(4));
    let busy_pd = PdId::new_v4();
    let ready_pd = PdId::new_v4();
    let busy = scheduler
        .admit(IoClass::Foreground, vec![demand(busy_pd, 4)])
        .unwrap();
    let (ready_tx, ready_rx) = mpsc::channel();
    let (release_ready_tx, release_ready_rx) = mpsc::channel();
    let (busy_tx, busy_rx) = mpsc::channel();
    let waiter_scheduler = scheduler.clone();
    let waiter = thread::spawn(move || {
        let mut pending = waiter_scheduler
            .queue(
                IoClass::Foreground,
                vec![demand(busy_pd, 4), demand(ready_pd, 4)],
            )
            .unwrap();
        let admitted = pending.admit_ready();
        assert_eq!(admitted.pd_ids, vec![ready_pd]);
        ready_tx.send(()).unwrap();
        release_ready_rx.recv().unwrap();
        drop(admitted.permit);

        let admitted = pending.admit_ready();
        assert_eq!(admitted.pd_ids, vec![busy_pd]);
        busy_tx.send(()).unwrap();
        drop(admitted.permit);
    });
    wait_until(|| {
        scheduler.snapshot().pds.into_iter().any(|pd| {
            pd.pd_id == busy_pd
                && pd
                    .classes
                    .into_iter()
                    .any(|class| class.class == IoClass::Foreground && class.queued_blocks == 4)
        })
    });
    ready_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    assert_eq!(
        class_snapshot(&scheduler, ready_pd, IoClass::Foreground).active_blocks,
        4
    );

    release_ready_tx.send(()).unwrap();
    drop(busy);
    busy_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    waiter.join().unwrap();
    assert_eq!(
        class_snapshot(&scheduler, busy_pd, IoClass::Foreground).active_blocks,
        0
    );
    assert_eq!(
        class_snapshot(&scheduler, ready_pd, IoClass::Foreground).active_blocks,
        0
    );
}

#[test]
fn background_reservation_deficit_precedes_older_foreground() {
    let scheduler =
        controller(SchedulerConfig::new(4).with_min_active_blocks(IoClass::DrainData, 2));
    let pd_id = PdId::new_v4();
    let busy = scheduler
        .admit(IoClass::Foreground, vec![demand(pd_id, 4)])
        .unwrap();
    let (order_tx, order_rx) = mpsc::channel();
    let (fg_release_tx, fg_release_rx) = mpsc::channel();
    let foreground_scheduler = scheduler.clone();
    let foreground_tx = order_tx.clone();
    let foreground = thread::spawn(move || {
        let permit = foreground_scheduler
            .admit(IoClass::Foreground, vec![demand(pd_id, 3)])
            .unwrap();
        foreground_tx.send(IoClass::Foreground).unwrap();
        fg_release_rx.recv().unwrap();
        drop(permit);
    });
    wait_until(|| class_snapshot(&scheduler, pd_id, IoClass::Foreground).queued_blocks == 3);

    let (bg_release_tx, bg_release_rx) = mpsc::channel();
    let background_scheduler = scheduler.clone();
    let background = thread::spawn(move || {
        let permit = background_scheduler
            .admit(IoClass::DrainData, vec![demand(pd_id, 2)])
            .unwrap();
        order_tx.send(IoClass::DrainData).unwrap();
        bg_release_rx.recv().unwrap();
        drop(permit);
    });
    wait_until(|| class_snapshot(&scheduler, pd_id, IoClass::DrainData).queued_blocks == 2);
    drop(busy);

    assert_eq!(
        order_rx.recv_timeout(Duration::from_secs(2)).unwrap(),
        IoClass::DrainData
    );
    assert!(order_rx.try_recv().is_err());
    bg_release_tx.send(()).unwrap();
    assert_eq!(
        order_rx.recv_timeout(Duration::from_secs(2)).unwrap(),
        IoClass::Foreground
    );
    fg_release_tx.send(()).unwrap();
    foreground.join().unwrap();
    background.join().unwrap();
}

#[test]
fn safe_wave_caps_prevent_foreground_background_reservation_deadlock() {
    let config = SchedulerConfig::new(4)
        .with_min_active_blocks(IoClass::Foreground, 1)
        .with_min_active_blocks(IoClass::DrainData, 1);
    assert_eq!(config.wave_cap(IoClass::Foreground), 3);
    assert_eq!(config.wave_cap(IoClass::DrainData), 3);
    let scheduler = controller(config);
    let pd_id = PdId::new_v4();
    let holder = scheduler
        .admit(IoClass::Maintenance, vec![demand(pd_id, 4)])
        .unwrap();

    let (order_tx, order_rx) = mpsc::channel();
    let (fg_release_tx, fg_release_rx) = mpsc::channel();
    let fg_scheduler = scheduler.clone();
    let fg_tx = order_tx.clone();
    let foreground = thread::spawn(move || {
        let permit = fg_scheduler
            .admit(IoClass::Foreground, vec![demand(pd_id, 3)])
            .unwrap();
        fg_tx.send(IoClass::Foreground).unwrap();
        fg_release_rx.recv().unwrap();
        drop(permit);
    });
    wait_until(|| class_snapshot(&scheduler, pd_id, IoClass::Foreground).queued_blocks == 3);

    let (data_release_tx, data_release_rx) = mpsc::channel();
    let data_scheduler = scheduler.clone();
    let data = thread::spawn(move || {
        let permit = data_scheduler
            .admit(IoClass::DrainData, vec![demand(pd_id, 3)])
            .unwrap();
        order_tx.send(IoClass::DrainData).unwrap();
        data_release_rx.recv().unwrap();
        drop(permit);
    });
    wait_until(|| class_snapshot(&scheduler, pd_id, IoClass::DrainData).queued_blocks == 3);
    drop(holder);

    assert_eq!(
        order_rx.recv_timeout(Duration::from_secs(2)).unwrap(),
        IoClass::DrainData
    );
    assert!(order_rx.try_recv().is_err());
    data_release_tx.send(()).unwrap();
    assert_eq!(
        order_rx.recv_timeout(Duration::from_secs(2)).unwrap(),
        IoClass::Foreground
    );
    fg_release_tx.send(()).unwrap();
    data.join().unwrap();
    foreground.join().unwrap();
}

#[test]
fn background_deficit_accumulates_capacity_instead_of_starving() {
    let scheduler =
        controller(SchedulerConfig::new(4).with_min_active_blocks(IoClass::DrainData, 1));
    let pd_id = PdId::new_v4();
    let active: Vec<_> = (0..4)
        .map(|_| {
            scheduler
                .admit(IoClass::Foreground, vec![demand(pd_id, 1)])
                .unwrap()
        })
        .collect();

    let (background_tx, background_rx) = mpsc::channel();
    let (background_release_tx, background_release_rx) = mpsc::channel();
    let background_scheduler = scheduler.clone();
    let background = thread::spawn(move || {
        let permit = background_scheduler
            .admit(IoClass::DrainData, vec![demand(pd_id, 4)])
            .unwrap();
        background_tx.send(()).unwrap();
        background_release_rx.recv().unwrap();
        drop(permit);
    });
    wait_until(|| class_snapshot(&scheduler, pd_id, IoClass::DrainData).queued_blocks == 4);

    let (foreground_tx, foreground_rx) = mpsc::channel();
    let foreground_scheduler = scheduler.clone();
    let foreground = thread::spawn(move || {
        let permit = foreground_scheduler
            .admit(IoClass::Foreground, vec![demand(pd_id, 1)])
            .unwrap();
        foreground_tx.send(()).unwrap();
        drop(permit);
    });
    wait_until(|| class_snapshot(&scheduler, pd_id, IoClass::Foreground).queued_blocks == 1);

    let mut active = active.into_iter();
    drop(active.next());
    thread::sleep(Duration::from_millis(10));
    assert!(foreground_rx.try_recv().is_err());
    assert!(background_rx.try_recv().is_err());
    drop(active);

    background_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    assert!(foreground_rx.try_recv().is_err());
    background_release_tx.send(()).unwrap();
    foreground_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    background.join().unwrap();
    foreground.join().unwrap();
}

#[test]
fn later_small_same_class_request_cannot_bypass_overlapping_large_waiter() {
    let scheduler = controller(SchedulerConfig::new(4));
    let pd_id = PdId::new_v4();
    let mut active: Vec<_> = (0..4)
        .map(|_| {
            scheduler
                .admit(IoClass::Foreground, vec![demand(pd_id, 1)])
                .unwrap()
        })
        .collect();

    let (large_tx, large_rx) = mpsc::channel();
    let (large_release_tx, large_release_rx) = mpsc::channel();
    let large_scheduler = scheduler.clone();
    let large = thread::spawn(move || {
        let permit = large_scheduler
            .admit(IoClass::Foreground, vec![demand(pd_id, 4)])
            .unwrap();
        large_tx.send(()).unwrap();
        large_release_rx.recv().unwrap();
        drop(permit);
    });
    wait_until(|| class_snapshot(&scheduler, pd_id, IoClass::Foreground).queued_blocks == 4);

    drop(active.pop());
    let (small_tx, small_rx) = mpsc::channel();
    let small_scheduler = scheduler.clone();
    let small = thread::spawn(move || {
        let permit = small_scheduler
            .admit(IoClass::Foreground, vec![demand(pd_id, 1)])
            .unwrap();
        small_tx.send(()).unwrap();
        drop(permit);
    });
    wait_until(|| class_snapshot(&scheduler, pd_id, IoClass::Foreground).queued_blocks == 5);
    thread::sleep(Duration::from_millis(10));
    assert!(small_rx.try_recv().is_err());

    drop(active);
    large_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    assert!(small_rx.try_recv().is_err());
    large_release_tx.send(()).unwrap();
    small_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    large.join().unwrap();
    small.join().unwrap();
}

#[test]
fn foreground_borrows_idle_reservation_and_completion_reclaims() {
    let scheduler =
        controller(SchedulerConfig::new(4).with_min_active_blocks(IoClass::DrainData, 2));
    let pd_id = PdId::new_v4();
    let permit = scheduler
        .admit(IoClass::Foreground, vec![demand(pd_id, 4)])
        .unwrap();
    let active = class_snapshot(&scheduler, pd_id, IoClass::Foreground);
    assert_eq!(active.active_blocks, 4);
    assert_eq!(active.borrowed_blocks, 2);
    assert_eq!(active.borrowed_blocks_total, 2);
    assert_eq!(active.borrow_events, 1);

    drop(permit);
    let reclaimed = class_snapshot(&scheduler, pd_id, IoClass::Foreground);
    assert_eq!(reclaimed.active_blocks, 0);
    assert_eq!(reclaimed.borrowed_blocks, 0);
    assert_eq!(reclaimed.reclaim_events, 1);
    assert_eq!(reclaimed.reclaimed_blocks, 4);
}

#[test]
fn error_and_unwind_drop_release_every_credit() {
    let scheduler = controller(SchedulerConfig::new(8));
    let pd_id = PdId::new_v4();
    let result: Result<(), ()> = (|| {
        let _permit = scheduler
            .admit(IoClass::DrainMeta, vec![demand(pd_id, 6)])
            .unwrap();
        Err(())
    })();
    assert!(result.is_err());
    assert_eq!(
        class_snapshot(&scheduler, pd_id, IoClass::DrainMeta).active_blocks,
        0
    );

    let unwind = catch_unwind(AssertUnwindSafe({
        let scheduler = scheduler.clone();
        move || {
            let _permit = scheduler
                .admit(IoClass::DrainMeta, vec![demand(pd_id, 8)])
                .unwrap();
            panic!("simulated backend panic");
        }
    }));
    assert!(unwind.is_err());
    let snapshot = class_snapshot(&scheduler, pd_id, IoClass::DrainMeta);
    assert_eq!(snapshot.active_blocks, 0);
    assert_eq!(snapshot.reclaim_events, 2);
    assert_eq!(snapshot.reclaimed_blocks, 14);
}

#[test]
fn plans_bounded_waves_in_original_order_and_caps_oversized_op() {
    let first = PdId::new_v4();
    let second = PdId::new_v4();
    let lanes = plan_work(
        &[(first, 3), (second, 2), (first, 3), (second, 1), (first, 9)],
        4,
        4,
    )
    .unwrap();
    assert_eq!(lanes[&first].len(), 3);
    assert_eq!(lanes[&first][0].indices, vec![0]);
    assert_eq!(lanes[&first][1].indices, vec![2]);
    assert_eq!(lanes[&first][2].indices, vec![4]);
    assert_eq!(lanes[&first][2].demand.blocks, 4);
    assert_eq!(lanes[&first][2].demand.requested_blocks, 9);
    assert!(lanes[&first][2].demand.exclusive);
    assert_eq!(lanes[&second].len(), 1);
    assert_eq!(lanes[&second][0].indices, vec![1, 3]);
    assert_eq!(lanes[&second][0].demand.blocks, 3);
}

#[test]
fn idle_background_plan_uses_full_max_instead_of_min_sized_waves() {
    let pd_id = PdId::new_v4();
    let lanes = plan_work(&[(pd_id, 1), (pd_id, 1), (pd_id, 1), (pd_id, 3)], 8, 8).unwrap();
    assert_eq!(lanes[&pd_id].len(), 1);
    assert_eq!(lanes[&pd_id][0].indices, vec![0, 1, 2, 3]);
    assert_eq!(lanes[&pd_id][0].demand.blocks, 6);
}

#[test]
fn idle_background_backend_borrows_to_max_in_one_inner_call() {
    let dir = tempfile::tempdir().unwrap();
    let raw = RawDevice::open_or_create(&dir.path().join("pd"), 4 * 1024 * 1024 * 1024).unwrap();
    let pool = Pool::create(
        vec![raw],
        PoolConfig {
            spare_pct: 0,
            io_backend: IoBackendKind::Sync,
        },
    )
    .unwrap();
    let pd = pool.pd(pool.list_pds()[0].pd_id).unwrap();
    let inner = Arc::new(CountingBackend::new());
    let scheduled = ScheduledBackend::new(
        inner.clone(),
        SchedulerConfig::new(4).with_min_active_blocks(IoClass::DrainData, 1),
    )
    .unwrap();
    let data = vec![vec![0_u8; 4096]; 4];
    let ops: Vec<_> = data
        .iter()
        .map(|data| StripWrite {
            pd: pd.clone(),
            chunklet_index: 0,
            in_chunklet_off: 0,
            data,
        })
        .collect();

    let results = scheduled.submit_writes_detailed_with_class(IoClass::DrainData, &ops);
    assert!(results.into_iter().all(|result| result.is_ok()));
    assert_eq!(inner.calls.load(Ordering::Relaxed), 1);
    assert_eq!(inner.max_ops.load(Ordering::Relaxed), 4);
    let snapshot = scheduled.snapshot();
    let drain = snapshot.pds[0]
        .classes
        .iter()
        .find(|class| class.class == IoClass::DrainData)
        .unwrap();
    assert_eq!(drain.active_blocks_max, 4);
    assert_eq!(drain.completed_blocks, 4);
}

#[test]
fn flush_fence_waits_for_active_and_blocks_new_admissions() {
    let scheduler = controller(SchedulerConfig::new(4));
    let pd_id = PdId::new_v4();
    let active = scheduler
        .admit(IoClass::DrainData, vec![demand(pd_id, 4)])
        .unwrap();

    let (fenced_tx, fenced_rx) = mpsc::channel();
    let (flush_release_tx, flush_release_rx) = mpsc::channel();
    let flush_scheduler = scheduler.clone();
    let flush = thread::spawn(move || {
        let fence = flush_scheduler.fence(vec![pd_id]);
        fenced_tx.send(()).unwrap();
        flush_release_rx.recv().unwrap();
        drop(fence);
    });
    wait_until(|| scheduler.snapshot().pds[0].flush_waiters == 1);

    let (write_tx, write_rx) = mpsc::channel();
    let write_scheduler = scheduler.clone();
    let writer = thread::spawn(move || {
        let permit = write_scheduler
            .admit(IoClass::Foreground, vec![demand(pd_id, 1)])
            .unwrap();
        write_tx.send(()).unwrap();
        drop(permit);
    });
    wait_until(|| class_snapshot(&scheduler, pd_id, IoClass::Foreground).queued_blocks == 1);
    drop(active);

    fenced_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    assert!(scheduler.snapshot().pds[0].flush_fenced);
    assert!(write_rx.try_recv().is_err());
    flush_release_tx.send(()).unwrap();
    write_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    flush.join().unwrap();
    writer.join().unwrap();
    assert!(!scheduler.snapshot().pds[0].flush_fenced);
}

#[test]
fn older_pending_write_precedes_later_flush_fence() {
    let scheduler = controller(SchedulerConfig::new(1));
    let pd_id = PdId::new_v4();
    let active = scheduler
        .admit(IoClass::Foreground, vec![demand(pd_id, 1)])
        .unwrap();

    let (write_tx, write_rx) = mpsc::channel();
    let (write_release_tx, write_release_rx) = mpsc::channel();
    let write_scheduler = scheduler.clone();
    let writer = thread::spawn(move || {
        let permit = write_scheduler
            .admit(IoClass::DrainMeta, vec![demand(pd_id, 1)])
            .unwrap();
        write_tx.send(()).unwrap();
        write_release_rx.recv().unwrap();
        drop(permit);
    });
    wait_until(|| class_snapshot(&scheduler, pd_id, IoClass::DrainMeta).queued_blocks == 1);

    let (flush_tx, flush_rx) = mpsc::channel();
    let flush_scheduler = scheduler.clone();
    let flush = thread::spawn(move || {
        let _fence = flush_scheduler.fence(vec![pd_id]);
        flush_tx.send(()).unwrap();
    });
    wait_until(|| scheduler.snapshot().pds[0].flush_waiters == 1);
    drop(active);

    write_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    assert!(flush_rx.try_recv().is_err());
    write_release_tx.send(()).unwrap();
    flush_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    writer.join().unwrap();
    flush.join().unwrap();
}

#[test]
fn multi_pd_flush_waits_for_ready_and_blocked_older_writes() {
    let scheduler = controller(SchedulerConfig::new(1));
    let ready_pd = PdId::new_v4();
    let blocked_pd = PdId::new_v4();
    let blocked = scheduler
        .admit(IoClass::Foreground, vec![demand(blocked_pd, 1)])
        .unwrap();
    let mut pending = scheduler
        .queue(
            IoClass::DrainData,
            vec![demand(ready_pd, 1), demand(blocked_pd, 1)],
        )
        .unwrap();
    let ready = pending.admit_ready();
    assert_eq!(ready.pd_ids, vec![ready_pd]);

    let (fenced_tx, fenced_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let flush_scheduler = scheduler.clone();
    let flush = thread::spawn(move || {
        let fence = flush_scheduler.fence(vec![ready_pd, blocked_pd]);
        fenced_tx.send(()).unwrap();
        release_rx.recv().unwrap();
        drop(fence);
    });
    wait_until(|| {
        let snapshot = scheduler.snapshot();
        [ready_pd, blocked_pd].iter().all(|pd_id| {
            snapshot
                .pds
                .iter()
                .find(|pd| pd.pd_id == *pd_id)
                .is_some_and(|pd| pd.flush_waiters == 1)
        })
    });

    drop(ready.permit);
    assert!(fenced_rx.try_recv().is_err());
    drop(blocked);
    let blocked_ready = pending.admit_ready();
    assert_eq!(blocked_ready.pd_ids, vec![blocked_pd]);
    assert!(fenced_rx.try_recv().is_err());

    drop(blocked_ready.permit);
    fenced_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    let fenced = scheduler.snapshot();
    assert!([ready_pd, blocked_pd].iter().all(|pd_id| {
        fenced
            .pds
            .iter()
            .find(|pd| pd.pd_id == *pd_id)
            .is_some_and(|pd| pd.flush_fenced)
    }));
    release_tx.send(()).unwrap();
    flush.join().unwrap();
    assert!(scheduler
        .snapshot()
        .pds
        .iter()
        .all(|pd| !pd.flush_fenced && pd.total_active_blocks == 0));
}

#[test]
fn registration_and_snapshot_include_idle_pd_limits_and_totals() {
    let scheduler =
        controller(SchedulerConfig::new(8).with_min_active_blocks(IoClass::DrainMeta, 2));
    let pd_id = PdId::new_v4();
    scheduler.register_pd(pd_id);
    let snapshot = scheduler.snapshot();
    assert_eq!(snapshot.pds.len(), 1);
    let pd = &snapshot.pds[0];
    assert_eq!(pd.pd_id, pd_id);
    assert_eq!(pd.total_queued_blocks, 0);
    assert_eq!(pd.total_active_blocks, 0);
    assert_eq!(pd.total_active_blocks_max, 0);
    let drain_meta = pd
        .classes
        .iter()
        .find(|class| class.class == IoClass::DrainMeta)
        .unwrap();
    assert_eq!(drain_meta.configured_min_blocks, 2);

    scheduler.record_completion(
        IoClass::DrainMeta,
        &[(pd_id, 3, false), (pd_id, 2, true)],
        100,
    );
    scheduler.record_completion(IoClass::DrainMeta, &[(pd_id, 1, false)], 40);
    let completed = class_snapshot(&scheduler, pd_id, IoClass::DrainMeta);
    assert_eq!(completed.completed_blocks, 4);
    assert_eq!(completed.error_blocks, 2);
    assert_eq!(completed.service_ns, 140);
    assert_eq!(completed.service_max_ns, 100);
}

#[test]
fn tls_class_restores_after_nested_scope_and_panic() {
    assert_eq!(current_io_class(), IoClass::Foreground);
    with_io_class(IoClass::DrainData, || {
        assert_eq!(IoClass::current(), IoClass::DrainData);
        with_io_class(IoClass::DrainMeta, || {
            assert_eq!(current_io_class(), IoClass::DrainMeta);
        });
        assert_eq!(current_io_class(), IoClass::DrainData);
    });
    assert_eq!(current_io_class(), IoClass::Foreground);

    let unwind = catch_unwind(|| {
        with_io_class(IoClass::Maintenance, || panic!("restore TLS"));
    });
    assert!(unwind.is_err());
    assert_eq!(current_io_class(), IoClass::Foreground);
}

#[test]
fn ready_pd_advances_each_lane_wave_while_another_pd_is_blocked() {
    let (_dir, pds) = make_test_pds(2);
    let blocked_pd = pds[0].clone();
    let ready_pd = pds[1].clone();
    let (calls_tx, calls_rx) = mpsc::channel();
    let scheduled = Arc::new(
        ScheduledBackend::new(
            Arc::new(RecordingBackend { calls: calls_tx }),
            SchedulerConfig::new(1),
        )
        .unwrap(),
    );
    let blocked = scheduled
        .admission
        .admit(IoClass::Foreground, vec![demand(blocked_pd.pd_id(), 1)])
        .unwrap();

    let submitter = scheduled.clone();
    let blocked_submit_pd = blocked_pd.clone();
    let ready_submit_pd = ready_pd.clone();
    let submit = thread::spawn(move || {
        let blocked_data = vec![1_u8; BLOCK_SIZE as usize];
        let first_ready_data = vec![2_u8; BLOCK_SIZE as usize];
        let second_ready_data = vec![3_u8; BLOCK_SIZE as usize];
        let ops = [
            StripWrite {
                pd: blocked_submit_pd,
                chunklet_index: 0,
                in_chunklet_off: 0,
                data: &blocked_data,
            },
            StripWrite {
                pd: ready_submit_pd.clone(),
                chunklet_index: 0,
                in_chunklet_off: 0,
                data: &first_ready_data,
            },
            StripWrite {
                pd: ready_submit_pd,
                chunklet_index: 0,
                in_chunklet_off: BLOCK_SIZE,
                data: &second_ready_data,
            },
        ];
        submitter.submit_writes_detailed_with_class(IoClass::DrainData, &ops)
    });

    assert_eq!(
        calls_rx.recv_timeout(Duration::from_secs(2)).unwrap(),
        vec![ready_pd.pd_id()]
    );
    assert_eq!(
        calls_rx.recv_timeout(Duration::from_secs(2)).unwrap(),
        vec![ready_pd.pd_id()]
    );
    assert!(calls_rx.try_recv().is_err());

    drop(blocked);
    assert_eq!(
        calls_rx.recv_timeout(Duration::from_secs(2)).unwrap(),
        vec![blocked_pd.pd_id()]
    );
    assert!(submit
        .join()
        .unwrap()
        .into_iter()
        .all(|result| result.is_ok()));
    assert!(scheduled
        .snapshot()
        .pds
        .iter()
        .all(|pd| { pd.total_active_blocks == 0 && pd.total_queued_blocks == 0 }));
}

#[test]
fn multi_pd_multiwave_errors_attempt_every_wave_and_preserve_result_order() {
    let (_dir, pds) = make_test_pds(2);
    let first_pd = pds[0].clone();
    let second_pd = pds[1].clone();
    let inner = Arc::new(MarkerResultBackend::default());
    let scheduled = ScheduledBackend::new(inner.clone(), SchedulerConfig::new(1)).unwrap();
    let data: Vec<_> = (1_u8..=6)
        .map(|marker| vec![marker; BLOCK_SIZE as usize])
        .collect();
    let ops = [
        StripWrite {
            pd: first_pd.clone(),
            chunklet_index: 0,
            in_chunklet_off: 0,
            data: &data[0],
        },
        StripWrite {
            pd: second_pd.clone(),
            chunklet_index: 0,
            in_chunklet_off: 0,
            data: &data[1],
        },
        StripWrite {
            pd: first_pd.clone(),
            chunklet_index: 0,
            in_chunklet_off: BLOCK_SIZE,
            data: &data[2],
        },
        StripWrite {
            pd: second_pd.clone(),
            chunklet_index: 0,
            in_chunklet_off: BLOCK_SIZE,
            data: &data[3],
        },
        StripWrite {
            pd: first_pd.clone(),
            chunklet_index: 0,
            in_chunklet_off: 2 * BLOCK_SIZE,
            data: &data[4],
        },
        StripWrite {
            pd: second_pd.clone(),
            chunklet_index: 0,
            in_chunklet_off: 2 * BLOCK_SIZE,
            data: &data[5],
        },
    ];

    let results = scheduled.submit_writes_detailed_with_class(IoClass::DrainData, &ops);
    assert_eq!(
        inner.calls.lock().as_slice(),
        &[vec![1, 2], vec![3, 4], vec![5, 6]]
    );
    for (result, marker) in results.iter().zip(1_u8..=6) {
        if marker % 2 == 0 {
            assert!(result.is_ok(), "marker {marker} should succeed");
        } else {
            match result {
                Err(ChunkletError::Invariant(message)) => {
                    assert_eq!(message, &format!("marker-{marker}"));
                }
                other => panic!("marker {marker} returned {other:?}"),
            }
        }
    }
    let snapshot = scheduled.snapshot();
    let first = snapshot
        .pds
        .iter()
        .find(|pd| pd.pd_id == first_pd.pd_id())
        .unwrap();
    let second = snapshot
        .pds
        .iter()
        .find(|pd| pd.pd_id == second_pd.pd_id())
        .unwrap();
    let first_class = first
        .classes
        .iter()
        .find(|class| class.class == IoClass::DrainData)
        .unwrap();
    let second_class = second
        .classes
        .iter()
        .find(|class| class.class == IoClass::DrainData)
        .unwrap();
    assert_eq!(first_class.error_blocks, 3);
    assert_eq!(first_class.completed_blocks, 0);
    assert_eq!(second_class.completed_blocks, 3);
    assert_eq!(second_class.error_blocks, 0);
    assert!(snapshot
        .pds
        .iter()
        .all(|pd| pd.total_active_blocks == 0 && pd.total_queued_blocks == 0));
}

#[test]
fn oversized_exclusive_lane_does_not_block_ready_pd_and_metrics_keep_actual_work() {
    let (_dir, pds) = make_test_pds(2);
    let oversized_pd = pds[0].clone();
    let ready_pd = pds[1].clone();
    let (calls_tx, calls_rx) = mpsc::channel();
    let scheduled = Arc::new(
        ScheduledBackend::new(
            Arc::new(RecordingBackend { calls: calls_tx }),
            SchedulerConfig::new(2),
        )
        .unwrap(),
    );
    let blocked = scheduled
        .admission
        .admit(IoClass::Foreground, vec![demand(oversized_pd.pd_id(), 1)])
        .unwrap();

    let submitter = scheduled.clone();
    let submit_oversized_pd = oversized_pd.clone();
    let submit_ready_pd = ready_pd.clone();
    let submit = thread::spawn(move || {
        let oversized_data = vec![1_u8; (3 * BLOCK_SIZE) as usize];
        let ready_data = vec![2_u8; BLOCK_SIZE as usize];
        let ops = [
            StripWrite {
                pd: submit_oversized_pd,
                chunklet_index: 0,
                in_chunklet_off: 0,
                data: &oversized_data,
            },
            StripWrite {
                pd: submit_ready_pd,
                chunklet_index: 0,
                in_chunklet_off: 0,
                data: &ready_data,
            },
        ];
        submitter.submit_writes_detailed_with_class(IoClass::DrainData, &ops)
    });

    assert_eq!(
        calls_rx.recv_timeout(Duration::from_secs(2)).unwrap(),
        vec![ready_pd.pd_id()]
    );
    wait_until(|| {
        class_snapshot(&scheduled.admission, ready_pd.pd_id(), IoClass::DrainData).completed_blocks
            == 1
    });
    let waiting = scheduled.snapshot();
    let oversized_waiting = waiting
        .pds
        .iter()
        .find(|pd| pd.pd_id == oversized_pd.pd_id())
        .unwrap()
        .classes
        .iter()
        .find(|class| class.class == IoClass::DrainData)
        .unwrap();
    let ready_done = waiting
        .pds
        .iter()
        .find(|pd| pd.pd_id == ready_pd.pd_id())
        .unwrap()
        .classes
        .iter()
        .find(|class| class.class == IoClass::DrainData)
        .unwrap();
    assert_eq!(oversized_waiting.queued_blocks, 2);
    assert_eq!(oversized_waiting.admitted_blocks, 0);
    assert_eq!(ready_done.active_blocks_max, 1);
    assert_eq!(ready_done.admitted_blocks, 1);
    assert_eq!(ready_done.completed_blocks, 1);

    drop(blocked);
    assert_eq!(
        calls_rx.recv_timeout(Duration::from_secs(2)).unwrap(),
        vec![oversized_pd.pd_id()]
    );
    assert!(submit
        .join()
        .unwrap()
        .into_iter()
        .all(|result| result.is_ok()));
    let completed = scheduled.snapshot();
    let oversized_done = completed
        .pds
        .iter()
        .find(|pd| pd.pd_id == oversized_pd.pd_id())
        .unwrap()
        .classes
        .iter()
        .find(|class| class.class == IoClass::DrainData)
        .unwrap();
    assert_eq!(oversized_done.active_blocks_max, 2);
    assert_eq!(oversized_done.admitted_blocks, 3);
    assert_eq!(oversized_done.completed_blocks, 3);
    assert_eq!(oversized_done.reclaimed_blocks, 2);
    assert!(completed
        .pds
        .iter()
        .all(|pd| pd.total_active_blocks == 0 && pd.total_queued_blocks == 0));
}

#[test]
fn panic_cancels_not_yet_admitted_pd_lane() {
    let (_dir, pds) = make_test_pds(2);
    let ready_pd = pds[0].clone();
    let blocked_pd = pds[1].clone();
    let scheduled = ScheduledBackend::new(
        Arc::new(PartialCompletionPanicBackend),
        SchedulerConfig::new(1),
    )
    .unwrap();
    let blocked = scheduled
        .admission
        .admit(IoClass::Foreground, vec![demand(blocked_pd.pd_id(), 1)])
        .unwrap();
    let ready_data = vec![1_u8; BLOCK_SIZE as usize];
    let blocked_data = vec![2_u8; BLOCK_SIZE as usize];
    let ops = [
        StripWrite {
            pd: ready_pd,
            chunklet_index: 0,
            in_chunklet_off: 0,
            data: &ready_data,
        },
        StripWrite {
            pd: blocked_pd,
            chunklet_index: 0,
            in_chunklet_off: 0,
            data: &blocked_data,
        },
    ];

    let unwind = catch_unwind(AssertUnwindSafe(|| {
        scheduled.submit_writes_detailed_with_class(IoClass::DrainMeta, &ops)
    }));
    assert!(unwind.is_err());
    let snapshot = scheduled.snapshot();
    assert!(snapshot.pds.iter().all(|pd| {
        let drain_meta = pd
            .classes
            .iter()
            .find(|class| class.class == IoClass::DrainMeta)
            .unwrap();
        drain_meta.active_blocks == 0 && drain_meta.queued_blocks == 0
    }));
    assert_eq!(
        snapshot
            .pds
            .iter()
            .flat_map(|pd| &pd.classes)
            .filter(|class| class.class == IoClass::DrainMeta)
            .map(|class| class.wait_events)
            .sum::<u64>(),
        1
    );
    assert!(
        snapshot
            .pds
            .iter()
            .flat_map(|pd| &pd.classes)
            .filter(|class| class.class == IoClass::DrainMeta)
            .map(|class| class.wait_ns)
            .sum::<u64>()
            > 0
    );
    drop(blocked);
    assert!(scheduled
        .snapshot()
        .pds
        .iter()
        .all(|pd| pd.total_active_blocks == 0 && pd.total_queued_blocks == 0));
}

#[test]
fn streaming_completion_reclaims_fast_pd_before_slow_pd_returns() {
    let (_dir, pds) = make_test_pds(2);
    let fast_pd = pds[0].clone();
    let slow_pd = pds[1].clone();
    let (first_pd_done_tx, first_pd_done_rx) = mpsc::channel();
    let (release_second_pd_tx, release_second_pd_rx) = mpsc::channel();
    let (followup_done_tx, followup_done_rx) = mpsc::channel();
    let inner = Arc::new(StreamingGateBackend {
        first_pd_done: first_pd_done_tx,
        release_second_pd: Mutex::new(release_second_pd_rx),
        followup_done: followup_done_tx,
    });
    let scheduled = Arc::new(ScheduledBackend::new(inner, SchedulerConfig::new(1)).unwrap());

    let first_scheduled = scheduled.clone();
    let first_fast_pd = fast_pd.clone();
    let first = thread::spawn(move || {
        let fast_data = vec![1_u8; BLOCK_SIZE as usize];
        let slow_data = vec![2_u8; BLOCK_SIZE as usize];
        let ops = [
            StripWrite {
                pd: first_fast_pd,
                chunklet_index: 0,
                in_chunklet_off: 0,
                data: &fast_data,
            },
            StripWrite {
                pd: slow_pd,
                chunklet_index: 0,
                in_chunklet_off: 0,
                data: &slow_data,
            },
        ];
        first_scheduled.submit_writes_detailed_with_class(IoClass::DrainData, &ops)
    });
    first_pd_done_rx
        .recv_timeout(Duration::from_secs(2))
        .unwrap();

    let followup_scheduled = scheduled.clone();
    let followup = thread::spawn(move || {
        let data = vec![3_u8; BLOCK_SIZE as usize];
        let ops = [StripWrite {
            pd: fast_pd,
            chunklet_index: 0,
            in_chunklet_off: BLOCK_SIZE,
            data: &data,
        }];
        followup_scheduled.submit_writes_detailed_with_class(IoClass::Foreground, &ops)
    });
    followup_done_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("fast-PD credit stayed coupled to the slow PD");

    let active = scheduled.snapshot();
    let fast = active
        .pds
        .iter()
        .find(|pd| pd.pd_id == pds[0].pd_id())
        .unwrap();
    let slow = active
        .pds
        .iter()
        .find(|pd| pd.pd_id == pds[1].pd_id())
        .unwrap();
    assert_eq!(fast.total_active_blocks, 0);
    assert_eq!(slow.total_active_blocks, 1);

    release_second_pd_tx.send(()).unwrap();
    let first_results = first.join().unwrap();
    let followup_results = followup.join().unwrap();
    assert!(first_results[0].is_err());
    assert!(first_results[1].is_ok());
    assert!(followup_results[0].is_ok());
    assert!(scheduled
        .snapshot()
        .pds
        .iter()
        .all(|pd| pd.total_active_blocks == 0));
    let completed = scheduled.snapshot();
    let fast_service = completed
        .pds
        .iter()
        .find(|pd| pd.pd_id == pds[0].pd_id())
        .unwrap()
        .classes
        .iter()
        .find(|class| class.class == IoClass::DrainData)
        .unwrap()
        .service_ns;
    let slow_service = completed
        .pds
        .iter()
        .find(|pd| pd.pd_id == pds[1].pd_id())
        .unwrap()
        .classes
        .iter()
        .find(|class| class.class == IoClass::DrainData)
        .unwrap()
        .service_ns;
    assert_eq!(fast_service, 10);
    assert_eq!(slow_service, 80);
}

#[test]
fn streaming_completion_refills_same_call_fast_pd_before_slow_pd_returns() {
    let (_dir, pds) = make_test_pds(2);
    let fast_pd = pds[0].clone();
    let slow_pd = pds[1].clone();
    let (first_pd_done_tx, first_pd_done_rx) = mpsc::channel();
    let (release_second_pd_tx, release_second_pd_rx) = mpsc::channel();
    let (followup_done_tx, followup_done_rx) = mpsc::channel();
    let inner = Arc::new(StreamingGateBackend {
        first_pd_done: first_pd_done_tx,
        release_second_pd: Mutex::new(release_second_pd_rx),
        followup_done: followup_done_tx,
    });
    let scheduled = Arc::new(ScheduledBackend::new(inner, SchedulerConfig::new(1)).unwrap());

    let submitter = scheduled.clone();
    let submit_fast_pd = fast_pd.clone();
    let submit = thread::spawn(move || {
        let first_fast_data = vec![1_u8; BLOCK_SIZE as usize];
        let slow_data = vec![2_u8; BLOCK_SIZE as usize];
        let second_fast_data = vec![3_u8; BLOCK_SIZE as usize];
        let ops = [
            StripWrite {
                pd: submit_fast_pd.clone(),
                chunklet_index: 0,
                in_chunklet_off: 0,
                data: &first_fast_data,
            },
            StripWrite {
                pd: slow_pd,
                chunklet_index: 0,
                in_chunklet_off: 0,
                data: &slow_data,
            },
            StripWrite {
                pd: submit_fast_pd,
                chunklet_index: 0,
                in_chunklet_off: BLOCK_SIZE,
                data: &second_fast_data,
            },
        ];
        submitter.submit_writes_detailed_with_class(IoClass::DrainData, &ops)
    });

    first_pd_done_rx
        .recv_timeout(Duration::from_secs(2))
        .unwrap();
    let refilled_before_slow = followup_done_rx.recv_timeout(Duration::from_millis(100));
    release_second_pd_tx.send(()).unwrap();
    let results = submit.join().unwrap();

    assert!(
        refilled_before_slow.is_ok(),
        "same-call fast-PD lane stayed coupled to the slow PD"
    );
    assert!(results[0].is_err());
    assert!(results[1].is_ok());
    assert!(results[2].is_ok());
    assert!(scheduled
        .snapshot()
        .pds
        .iter()
        .all(|pd| pd.total_active_blocks == 0 && pd.total_queued_blocks == 0));
}

#[test]
fn streaming_completion_panic_reclaims_unreported_pd_credit() {
    let (_dir, pds) = make_test_pds(2);
    let scheduled = ScheduledBackend::new(
        Arc::new(PartialCompletionPanicBackend),
        SchedulerConfig::new(1),
    )
    .unwrap();
    let first_data = vec![1_u8; BLOCK_SIZE as usize];
    let second_data = vec![2_u8; BLOCK_SIZE as usize];
    let ops = [
        StripWrite {
            pd: pds[0].clone(),
            chunklet_index: 0,
            in_chunklet_off: 0,
            data: &first_data,
        },
        StripWrite {
            pd: pds[1].clone(),
            chunklet_index: 0,
            in_chunklet_off: 0,
            data: &second_data,
        },
    ];

    let unwind = catch_unwind(AssertUnwindSafe(|| {
        scheduled.submit_writes_detailed_with_class(IoClass::DrainMeta, &ops)
    }));
    assert!(unwind.is_err());
    let snapshot = scheduled.snapshot();
    assert!(snapshot.pds.iter().all(|pd| pd.total_active_blocks == 0));
    assert_eq!(
        snapshot
            .pds
            .iter()
            .flat_map(|pd| &pd.classes)
            .filter(|class| class.class == IoClass::DrainMeta)
            .map(|class| class.reclaimed_blocks)
            .sum::<u64>(),
        2
    );
}

#[test]
fn one_slot_refill_rotates_across_ready_pds() {
    let (_dir, pds) = make_test_pds(3);
    let inner = Arc::new(OneAtATimeDispatchBackend::default());
    let scheduled = ScheduledBackend::new(inner.clone(), SchedulerConfig::new(2)).unwrap();
    let data: Vec<_> = (0..6)
        .map(|marker| vec![marker; BLOCK_SIZE as usize])
        .collect();
    let mut ops = Vec::new();
    for round in 0..2 {
        for (pd_index, pd) in pds.iter().enumerate() {
            let index = round * pds.len() + pd_index;
            ops.push(StripWrite {
                pd: pd.clone(),
                chunklet_index: 0,
                in_chunklet_off: (round as u64) * BLOCK_SIZE,
                data: &data[index],
            });
        }
    }

    assert!(scheduled
        .submit_writes_detailed_with_class(IoClass::DrainData, &ops)
        .into_iter()
        .all(|result| result.is_ok()));
    let issued = inner.pds.lock();
    assert_eq!(issued.len(), 6);
    assert_eq!(
        issued[..3].iter().copied().collect::<BTreeSet<_>>().len(),
        3
    );
    assert_eq!(issued[0], issued[3]);
    assert_eq!(issued[1], issued[4]);
    assert_eq!(issued[2], issued[5]);
}

#[test]
fn duplicate_dispatch_completion_returns_protocol_error_and_releases_credit() {
    let (_dir, pds) = make_test_pds(1);
    let scheduled = ScheduledBackend::new(
        Arc::new(DuplicateDispatchCompletionBackend),
        SchedulerConfig::new(1),
    )
    .unwrap();
    let data = vec![1_u8; BLOCK_SIZE as usize];
    let ops = [StripWrite {
        pd: pds[0].clone(),
        chunklet_index: 0,
        in_chunklet_off: 0,
        data: &data,
    }];

    let results = scheduled.submit_writes_detailed_with_class(IoClass::DrainData, &ops);
    assert!(matches!(
        &results[0],
        Err(ChunkletError::Invariant(message)) if message.contains("protocol failed")
    ));
    let snapshot = scheduled.snapshot();
    assert!(snapshot
        .pds
        .iter()
        .all(|pd| pd.total_active_blocks == 0 && pd.total_queued_blocks == 0));
}
