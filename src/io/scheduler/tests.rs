use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use super::*;
use crate::io::{IoBackendKind, RawDevice};
use crate::{Pool, PoolConfig};

struct CountingBackend {
    calls: AtomicUsize,
    max_ops: AtomicUsize,
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

    let waves = plan_work(&[(first, 1), (second, 2), (first, 1), (first, 0)], 8, 8).unwrap();
    let by_pd: BTreeMap<_, _> = waves[0]
        .demands
        .iter()
        .into_iter()
        .map(|demand| (demand.pd_id, demand.blocks))
        .collect();
    assert_eq!(by_pd[&first], 2);
    assert_eq!(by_pd[&second], 2);
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
fn multi_pd_batch_never_holds_partial_credit() {
    let scheduler = controller(SchedulerConfig::new(4));
    let first = PdId::new_v4();
    let second = PdId::new_v4();
    let first_busy = scheduler
        .admit(IoClass::Foreground, vec![demand(first, 4)])
        .unwrap();
    let (admitted_tx, admitted_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let waiter_scheduler = scheduler.clone();
    let waiter = thread::spawn(move || {
        let permit = waiter_scheduler
            .admit(
                IoClass::Foreground,
                vec![demand(first, 4), demand(second, 4)],
            )
            .unwrap();
        admitted_tx.send(()).unwrap();
        release_rx.recv().unwrap();
        drop(permit);
    });
    wait_until(|| {
        scheduler.snapshot().pds.into_iter().any(|pd| {
            pd.pd_id == second
                && pd
                    .classes
                    .into_iter()
                    .any(|class| class.class == IoClass::Foreground && class.queued_blocks == 4)
        })
    });
    assert_eq!(
        class_snapshot(&scheduler, second, IoClass::Foreground).active_blocks,
        0
    );

    drop(first_busy);
    admitted_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    release_tx.send(()).unwrap();
    waiter.join().unwrap();
    assert_eq!(
        class_snapshot(&scheduler, first, IoClass::Foreground).active_blocks,
        0
    );
    assert_eq!(
        class_snapshot(&scheduler, second, IoClass::Foreground).active_blocks,
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
    let waves = plan_work(
        &[(first, 3), (second, 2), (first, 3), (second, 1), (first, 9)],
        4,
        4,
    )
    .unwrap();
    assert_eq!(waves.len(), 3);
    assert_eq!(waves[0].indices, vec![0, 1]);
    assert_eq!(waves[1].indices, vec![2, 3]);
    assert_eq!(waves[2].indices, vec![4]);
    for wave in &waves[..2] {
        assert!(wave.demands.iter().all(|demand| demand.blocks <= 4));
    }
    assert_eq!(waves[2].demands[0].blocks, 4);
    assert_eq!(waves[2].demands[0].requested_blocks, 9);
}

#[test]
fn idle_background_plan_uses_full_max_instead_of_min_sized_waves() {
    let pd_id = PdId::new_v4();
    let waves = plan_work(&[(pd_id, 1), (pd_id, 1), (pd_id, 1), (pd_id, 3)], 8, 8).unwrap();
    assert_eq!(waves.len(), 1);
    assert_eq!(waves[0].indices, vec![0, 1, 2, 3]);
    assert_eq!(waves[0].demands[0].blocks, 6);
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
