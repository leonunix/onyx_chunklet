//! Persistent scoped execution for PD-homogeneous io_uring write groups.
//!
//! Rayon owns fixed foreground and background worker sets. A submit borrows
//! caller buffers only for the duration of `ThreadPool::in_place_scope`, while each
//! persistent worker reuses the `IoUring` stored in its thread-local state.

use std::any::Any;
use std::collections::{BTreeMap, VecDeque};
use std::panic::{catch_unwind, resume_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use crossbeam_channel::unbounded;
use parking_lot::{Condvar, Mutex as ParkingMutex};
use rayon::{ThreadPool, ThreadPoolBuilder};

use crate::error::{ChunkletError, ChunkletResult};
use crate::io::backend::{
    IoBackend, IoExecutionClassSnapshot, IoExecutionSnapshot, StripRead, StripWrite,
    UringPoolConfig, WriteCompletionObserver,
};
use crate::io::scheduler::{current_io_class, with_io_class, IoClass, SchedulerSnapshot};
use crate::pd::PhysicalDisk;
use crate::types::PdId;

struct ClassStats {
    batches: AtomicU64,
    groups: AtomicU64,
    ops: AtomicU64,
    queue_wait_ns: AtomicU64,
    queue_wait_max_ns: AtomicU64,
    execute_ns: AtomicU64,
    execute_max_ns: AtomicU64,
}

impl ClassStats {
    fn new() -> Self {
        Self {
            batches: AtomicU64::new(0),
            groups: AtomicU64::new(0),
            ops: AtomicU64::new(0),
            queue_wait_ns: AtomicU64::new(0),
            queue_wait_max_ns: AtomicU64::new(0),
            execute_ns: AtomicU64::new(0),
            execute_max_ns: AtomicU64::new(0),
        }
    }

    fn record_batch(&self, groups: usize, ops: usize) {
        self.batches.fetch_add(1, Ordering::Relaxed);
        self.groups
            .fetch_add(saturating_u64(groups), Ordering::Relaxed);
        self.ops.fetch_add(saturating_u64(ops), Ordering::Relaxed);
    }

    fn record_group(&self, queue_wait_ns: u64, execute_ns: u64) {
        self.queue_wait_ns
            .fetch_add(queue_wait_ns, Ordering::Relaxed);
        update_max(&self.queue_wait_max_ns, queue_wait_ns);
        self.execute_ns.fetch_add(execute_ns, Ordering::Relaxed);
        update_max(&self.execute_max_ns, execute_ns);
    }

    fn snapshot(&self, class: IoClass) -> IoExecutionClassSnapshot {
        IoExecutionClassSnapshot {
            class,
            batches: self.batches.load(Ordering::Relaxed),
            groups: self.groups.load(Ordering::Relaxed),
            ops: self.ops.load(Ordering::Relaxed),
            queue_wait_ns: self.queue_wait_ns.load(Ordering::Relaxed),
            queue_wait_max_ns: self.queue_wait_max_ns.load(Ordering::Relaxed),
            execute_ns: self.execute_ns.load(Ordering::Relaxed),
            execute_max_ns: self.execute_max_ns.load(Ordering::Relaxed),
        }
    }
}

struct ExecutionStats {
    classes: [ClassStats; 4],
}

impl ExecutionStats {
    fn new() -> Self {
        Self {
            classes: std::array::from_fn(|_| ClassStats::new()),
        }
    }

    fn class(&self, class: IoClass) -> &ClassStats {
        &self.classes[class_index(class)]
    }
}

#[derive(Default)]
struct PdLaneState {
    cap: usize,
    active: usize,
    waiters: VecDeque<u64>,
}

#[derive(Default)]
struct LaneState {
    pds: BTreeMap<PdId, PdLaneState>,
    total_active: usize,
    next_ticket: u64,
}

/// Admission in front of one shared Rayon pool. Foreground owns one instance;
/// all background classes share another, so neither side consumes the other's
/// worker lanes.
struct PdLaneAdmission {
    workers: usize,
    state: ParkingMutex<LaneState>,
    changed: Condvar,
}

impl PdLaneAdmission {
    fn new(workers: usize) -> Self {
        Self {
            workers,
            state: ParkingMutex::new(LaneState::default()),
            changed: Condvar::new(),
        }
    }

    fn register_pd(&self, pd_id: PdId) {
        let mut state = self.state.lock();
        if state.pds.contains_key(&pd_id) {
            return;
        }
        state.pds.insert(pd_id, PdLaneState::default());
        self.rebalance(&mut state);
        drop(state);
        self.changed.notify_all();
    }

    fn enqueue_groups(self: &Arc<Self>, groups: &mut [PdGroup]) {
        let mut state = self.state.lock();
        let mut registered = false;
        for group in groups.iter() {
            if let std::collections::btree_map::Entry::Vacant(entry) = state.pds.entry(group.pd_id)
            {
                entry.insert(PdLaneState::default());
                registered = true;
            }
        }
        if registered {
            self.rebalance(&mut state);
        }

        for group in groups {
            debug_assert!(group.waiter.is_none());
            let ticket = state.next_ticket;
            state.next_ticket = state.next_ticket.wrapping_add(1);
            state
                .pds
                .get_mut(&group.pd_id)
                .expect("queued execution PD was not registered")
                .waiters
                .push_back(ticket);
            group.waiter = Some(PdLaneWaiter {
                admission: self.clone(),
                pd_id: group.pd_id,
                ticket,
                queued: true,
            });
        }
        drop(state);
        self.changed.notify_all();
    }

    fn acquire_any(self: &Arc<Self>, pending: &mut [PdGroup]) -> (usize, PdLanePermit) {
        assert!(
            self.workers > 0,
            "cannot acquire a zero-worker execution lane"
        );
        assert!(
            !pending.is_empty(),
            "cannot acquire from an empty PD group set"
        );

        let mut state = self.state.lock();
        loop {
            if state.total_active < self.workers {
                if let Some((pending_index, pd_id)) =
                    pending.iter().enumerate().find_map(|(index, group)| {
                        let lane = state
                            .pds
                            .get(&group.pd_id)
                            .expect("pending execution PD was not registered");
                        let ticket = group
                            .waiter
                            .as_ref()
                            .expect("pending execution group has no lane ticket")
                            .ticket;
                        (lane.active < lane.cap && lane.waiters.front() == Some(&ticket))
                            .then_some((index, group.pd_id))
                    })
                {
                    let lane = state
                        .pds
                        .get_mut(&pd_id)
                        .expect("selected execution PD was not registered");
                    lane.waiters.pop_front();
                    lane.active += 1;
                    state.total_active += 1;
                    let mut waiter = pending[pending_index]
                        .waiter
                        .take()
                        .expect("selected execution group has no lane ticket");
                    waiter.queued = false;
                    return (
                        pending_index,
                        PdLanePermit {
                            admission: self.clone(),
                            pd_id,
                        },
                    );
                }
            }
            self.changed.wait(&mut state);
        }
    }

    fn cancel_waiter(&self, pd_id: PdId, ticket: u64) {
        let mut state = self.state.lock();
        let lane = state
            .pds
            .get_mut(&pd_id)
            .expect("queued execution PD was unregistered");
        if let Some(index) = lane.waiters.iter().position(|&queued| queued == ticket) {
            lane.waiters.remove(index);
        }
        drop(state);
        self.changed.notify_all();
    }

    fn rebalance(&self, state: &mut LaneState) {
        let pd_count = state.pds.len();
        if pd_count == 0 {
            return;
        }

        if self.workers >= pd_count {
            let base = self.workers / pd_count;
            let remainder = self.workers % pd_count;
            for (index, lane) in state.pds.values_mut().enumerate() {
                lane.cap = base + usize::from(index < remainder);
            }
        } else {
            // No static reservation can cover every PD when workers < PDs.
            // A one-lane cap keeps every PD runnable; total_active still caps
            // actual concurrency at the worker count.
            for lane in state.pds.values_mut() {
                lane.cap = usize::from(self.workers > 0);
            }
        }
    }

    #[cfg(test)]
    fn snapshot(&self) -> Vec<(PdId, usize, usize)> {
        self.state
            .lock()
            .pds
            .iter()
            .map(|(&pd_id, lane)| (pd_id, lane.cap, lane.active))
            .collect()
    }

    #[cfg(test)]
    fn waiter_count(&self, pd_id: PdId) -> usize {
        self.state
            .lock()
            .pds
            .get(&pd_id)
            .map_or(0, |lane| lane.waiters.len())
    }
}

struct PdLanePermit {
    admission: Arc<PdLaneAdmission>,
    pd_id: PdId,
}

struct PdLaneWaiter {
    admission: Arc<PdLaneAdmission>,
    pd_id: PdId,
    ticket: u64,
    queued: bool,
}

impl Drop for PdLaneWaiter {
    fn drop(&mut self) {
        if self.queued {
            self.admission.cancel_waiter(self.pd_id, self.ticket);
        }
    }
}

impl Drop for PdLanePermit {
    fn drop(&mut self) {
        let mut state = self.admission.state.lock();
        debug_assert!(state.total_active > 0);
        {
            let lane = state
                .pds
                .get_mut(&self.pd_id)
                .expect("active execution PD was unregistered");
            debug_assert!(lane.active > 0);
            lane.active -= 1;
        }
        state.total_active -= 1;
        drop(state);
        self.admission.changed.notify_all();
    }
}

pub(super) struct ScopedWritePools {
    foreground: Option<ThreadPool>,
    background: Option<ThreadPool>,
    foreground_lanes: Arc<PdLaneAdmission>,
    background_lanes: Arc<PdLaneAdmission>,
    foreground_workers: usize,
    background_workers: usize,
    foreground_cpus: Vec<usize>,
    background_cpus: Vec<usize>,
    stats: ExecutionStats,
}

/// Persistent PD-homogeneous execution outside admission control.
///
/// Keeping this wrapper outside `ScheduledBackend` is load-bearing: worker
/// queue residence is execution backlog, not device-active work. A worker
/// acquires scheduler credit only after it dequeues one PD group, so a blocked
/// member cannot hold credit for, or prevent dispatch to, an unrelated PD.
pub(crate) struct ExecutionPoolBackend {
    inner: Arc<dyn IoBackend>,
    write_pools: ScopedWritePools,
    fence: ExecutionFence,
}

impl ExecutionPoolBackend {
    pub(crate) fn new(inner: Arc<dyn IoBackend>, config: UringPoolConfig) -> std::io::Result<Self> {
        let write_pools = ScopedWritePools::with_config(config)?;
        if let Some(snapshot) = inner.scheduler_snapshot() {
            for pd in snapshot.pds {
                write_pools.register_pd(pd.pd_id);
            }
        }
        Ok(Self {
            inner,
            write_pools,
            fence: ExecutionFence::new(),
        })
    }
}

impl IoBackend for ExecutionPoolBackend {
    fn register_pd(&self, pd_id: PdId) {
        self.inner.register_pd(pd_id);
        self.write_pools.register_pd(pd_id);
    }

    fn scheduler_snapshot(&self) -> Option<SchedulerSnapshot> {
        self.inner.scheduler_snapshot()
    }

    fn execution_snapshot(&self) -> Option<IoExecutionSnapshot> {
        Some(self.write_pools.snapshot())
    }

    fn submit_reads(&self, ops: &mut [StripRead<'_>]) -> ChunkletResult<()> {
        self.inner.submit_reads(ops)
    }

    fn submit_writes_detailed(&self, ops: &[StripWrite<'_>]) -> Vec<ChunkletResult<()>> {
        self.submit_writes_detailed_with_class(current_io_class(), ops)
    }

    fn submit_writes_detailed_with_class(
        &self,
        class: IoClass,
        ops: &[StripWrite<'_>],
    ) -> Vec<ChunkletResult<()>> {
        let _write = (!ops.is_empty()).then(|| self.fence.enter_write());
        self.write_pools.submit(class, ops, |group| {
            self.inner.submit_writes_detailed_with_class(class, group)
        })
    }

    fn submit_writes_detailed_observed_with_class(
        &self,
        class: IoClass,
        ops: &[StripWrite<'_>],
        observer: &dyn WriteCompletionObserver,
    ) -> Vec<ChunkletResult<()>> {
        let _write = (!ops.is_empty()).then(|| self.fence.enter_write());
        self.write_pools.submit_observed(
            class,
            ops,
            observer,
            |group, group_observer, _service_started| {
                self.inner
                    .submit_writes_detailed_observed_with_class(class, group, group_observer)
            },
        )
    }

    fn submit_flushes(&self, pds: &[Arc<PhysicalDisk>]) -> ChunkletResult<()> {
        if pds.iter().all(|pd| !pd.sync_required()) {
            return self.inner.submit_flushes(pds);
        }
        let _flush = self.fence.enter_flush();
        self.inner.submit_flushes(pds)
    }

    fn name(&self) -> &'static str {
        "execution-pool"
    }
}

#[derive(Default)]
struct ExecutionFenceState {
    active_writes: u64,
    flush_waiters: u64,
    flushing: bool,
}

struct ExecutionFence {
    state: ParkingMutex<ExecutionFenceState>,
    changed: Condvar,
}

impl ExecutionFence {
    fn new() -> Self {
        Self {
            state: ParkingMutex::new(ExecutionFenceState::default()),
            changed: Condvar::new(),
        }
    }

    fn enter_write(&self) -> ExecutionWriteGuard<'_> {
        let mut state = self.state.lock();
        while state.flushing || state.flush_waiters > 0 {
            self.changed.wait(&mut state);
        }
        state.active_writes = state.active_writes.saturating_add(1);
        ExecutionWriteGuard { fence: self }
    }

    fn enter_flush(&self) -> ExecutionFlushGuard<'_> {
        let mut state = self.state.lock();
        state.flush_waiters = state.flush_waiters.saturating_add(1);
        while state.flushing || state.active_writes > 0 {
            self.changed.wait(&mut state);
        }
        state.flush_waiters -= 1;
        state.flushing = true;
        ExecutionFlushGuard { fence: self }
    }
}

struct ExecutionWriteGuard<'a> {
    fence: &'a ExecutionFence,
}

impl Drop for ExecutionWriteGuard<'_> {
    fn drop(&mut self) {
        let mut state = self.fence.state.lock();
        state.active_writes -= 1;
        if state.active_writes == 0 {
            self.fence.changed.notify_all();
        }
    }
}

struct ExecutionFlushGuard<'a> {
    fence: &'a ExecutionFence,
}

impl Drop for ExecutionFlushGuard<'_> {
    fn drop(&mut self) {
        let mut state = self.fence.state.lock();
        debug_assert!(state.flushing);
        state.flushing = false;
        self.fence.changed.notify_all();
    }
}

impl ScopedWritePools {
    pub(super) fn with_config(config: UringPoolConfig) -> std::io::Result<Self> {
        let foreground_cpus = config.foreground_cpus;
        let background_cpus = config.background_cpus;
        Ok(Self {
            foreground: build_pool("ckuring-fg", config.foreground_workers, &foreground_cpus)?,
            background: build_pool("ckuring-bg", config.background_workers, &background_cpus)?,
            foreground_lanes: Arc::new(PdLaneAdmission::new(config.foreground_workers)),
            background_lanes: Arc::new(PdLaneAdmission::new(config.background_workers)),
            foreground_workers: config.foreground_workers,
            background_workers: config.background_workers,
            foreground_cpus,
            background_cpus,
            stats: ExecutionStats::new(),
        })
    }

    fn register_pd(&self, pd_id: PdId) {
        self.foreground_lanes.register_pd(pd_id);
        self.background_lanes.register_pd(pd_id);
    }

    pub(super) fn submit<'a, F>(
        &self,
        class: IoClass,
        ops: &[StripWrite<'a>],
        submit_group: F,
    ) -> Vec<ChunkletResult<()>>
    where
        F: Fn(&[StripWrite<'a>]) -> Vec<ChunkletResult<()>> + Sync,
    {
        self.submit_inner(class, ops, None, submit_group)
    }

    pub(super) fn submit_observed<'a, F>(
        &self,
        class: IoClass,
        ops: &[StripWrite<'a>],
        observer: &dyn WriteCompletionObserver,
        submit_group: F,
    ) -> Vec<ChunkletResult<()>>
    where
        F: Fn(&[StripWrite<'a>], &dyn WriteCompletionObserver, Instant) -> Vec<ChunkletResult<()>>
            + Sync,
    {
        let submit_started = Instant::now();
        let Some(pool) = self.pool(class) else {
            let mut results = with_io_class(class, || submit_group(ops, observer, submit_started));
            normalize_result_count(&mut results, ops.len());
            return results;
        };
        if ops.is_empty() {
            return Vec::new();
        }

        let mut groups = group_indices_by_pd(ops);
        let lanes = self.lanes(class);
        lanes.enqueue_groups(&mut groups);
        self.stats
            .class(class)
            .record_batch(groups.len(), ops.len());
        let (sender, receiver) = unbounded::<(Vec<usize>, Vec<ChunkletResult<()>>)>();
        let observer_panic = ParkingMutex::new(None::<Box<dyn Any + Send>>);
        let batch_queued_at = submit_started;

        assert!(
            pool.current_thread_index().is_none(),
            "persistent write execution pool cannot re-enter itself"
        );
        pool.in_place_scope(|scope| {
            while !groups.is_empty() {
                let (pending_index, permit) = lanes.acquire_any(&mut groups);
                let PdGroup { indices, .. } = groups.remove(pending_index);
                let sender = sender.clone();
                let stats = self.stats.class(class);
                let queued_at = batch_queued_at;
                let submit_group = &submit_group;
                let observer_panic = &observer_panic;
                scope.spawn(move |_| {
                    let _permit = permit;
                    let queue_wait_ns = elapsed_ns(queued_at);
                    let group_ops: Vec<_> =
                        indices.iter().map(|&index| ops[index].clone()).collect();
                    let indexed_observer = IndexedObserver {
                        global_indices: &indices,
                        outer: observer,
                        panic: observer_panic,
                    };
                    let execute_started = Instant::now();
                    let mut results = with_io_class(class, || {
                        submit_group(&group_ops, &indexed_observer, execute_started)
                    });
                    let execute_ns = elapsed_ns(execute_started);
                    stats.record_group(queue_wait_ns, execute_ns);
                    normalize_result_count(&mut results, indices.len());
                    sender
                        .send((indices, results))
                        .expect("scoped write result receiver remains alive");
                });
            }
        });
        drop(sender);
        let observer_panic = observer_panic.into_inner();
        let output = collect_group_results(receiver, ops.len());
        if let Some(payload) = observer_panic {
            resume_unwind(payload);
        }
        output
    }

    fn submit_inner<'a, F>(
        &self,
        class: IoClass,
        ops: &[StripWrite<'a>],
        observer: Option<&dyn WriteCompletionObserver>,
        submit_group: F,
    ) -> Vec<ChunkletResult<()>>
    where
        F: Fn(&[StripWrite<'a>]) -> Vec<ChunkletResult<()>> + Sync,
    {
        let submit_started = Instant::now();
        let Some(pool) = self.pool(class) else {
            let mut results = with_io_class(class, || submit_group(ops));
            normalize_result_count(&mut results, ops.len());
            if let Some(observer) = observer {
                let indices: Vec<_> = (0..ops.len()).collect();
                observer.writes_completed(&indices, elapsed_ns(submit_started));
            }
            return results;
        };
        if ops.is_empty() {
            return Vec::new();
        }

        let mut groups = group_indices_by_pd(ops);
        let lanes = self.lanes(class);
        lanes.enqueue_groups(&mut groups);
        self.stats
            .class(class)
            .record_batch(groups.len(), ops.len());
        let (sender, receiver) = unbounded::<(Vec<usize>, Vec<ChunkletResult<()>>)>();
        let observer_panic = ParkingMutex::new(None::<Box<dyn Any + Send>>);
        let batch_queued_at = submit_started;

        assert!(
            pool.current_thread_index().is_none(),
            "persistent write execution pool cannot re-enter itself"
        );
        pool.in_place_scope(|scope| {
            while !groups.is_empty() {
                let (pending_index, permit) = lanes.acquire_any(&mut groups);
                let PdGroup { indices, .. } = groups.remove(pending_index);
                let sender = sender.clone();
                let stats = self.stats.class(class);
                let queued_at = batch_queued_at;
                let submit_group = &submit_group;
                let observer_panic = &observer_panic;
                scope.spawn(move |_| {
                    let _permit = permit;
                    let queue_wait_ns = elapsed_ns(queued_at);
                    let group_ops: Vec<_> =
                        indices.iter().map(|&index| ops[index].clone()).collect();
                    let execute_started = Instant::now();
                    let mut results = with_io_class(class, || submit_group(&group_ops));
                    let execute_ns = elapsed_ns(execute_started);
                    stats.record_group(queue_wait_ns, execute_ns);
                    normalize_result_count(&mut results, indices.len());
                    if let Some(observer) = observer {
                        let mut panic = observer_panic.lock();
                        if panic.is_none() {
                            if let Err(payload) = catch_unwind(AssertUnwindSafe(|| {
                                observer.writes_completed(&indices, elapsed_ns(submit_started));
                            })) {
                                *panic = Some(payload);
                            }
                        }
                    }
                    sender
                        .send((indices, results))
                        .expect("scoped write result receiver remains alive");
                });
            }
        });
        drop(sender);
        let observer_panic = observer_panic.into_inner();

        let mut output: Vec<Option<ChunkletResult<()>>> =
            std::iter::repeat_with(|| None).take(ops.len()).collect();
        for (indices, results) in receiver {
            for (index, result) in indices.into_iter().zip(results) {
                output[index] = Some(result);
            }
        }
        let output = output
            .into_iter()
            .map(|result| {
                result.unwrap_or_else(|| {
                    Err(ChunkletError::Invariant(
                        "pooled io_uring execution omitted a write result".into(),
                    ))
                })
            })
            .collect();
        if let Some(payload) = observer_panic {
            resume_unwind(payload);
        }
        output
    }

    pub(super) fn snapshot(&self) -> IoExecutionSnapshot {
        IoExecutionSnapshot {
            enabled: self.foreground.is_some() || self.background.is_some(),
            foreground_workers: self.foreground_workers,
            background_workers: self.background_workers,
            foreground_cpus: self.foreground_cpus.clone(),
            background_cpus: self.background_cpus.clone(),
            cpu_sets_disjoint: cpu_sets_disjoint(&self.foreground_cpus, &self.background_cpus),
            classes: IoClass::ALL
                .iter()
                .map(|&class| self.stats.class(class).snapshot(class))
                .collect(),
        }
    }

    fn pool(&self, class: IoClass) -> Option<&ThreadPool> {
        match class {
            IoClass::Foreground => self.foreground.as_ref(),
            IoClass::DrainData | IoClass::DrainMeta | IoClass::Maintenance => {
                self.background.as_ref()
            }
        }
    }

    fn lanes(&self, class: IoClass) -> &Arc<PdLaneAdmission> {
        match class {
            IoClass::Foreground => &self.foreground_lanes,
            IoClass::DrainData | IoClass::DrainMeta | IoClass::Maintenance => {
                &self.background_lanes
            }
        }
    }

    pub(super) fn has_pool(&self, class: IoClass) -> bool {
        self.pool(class).is_some()
    }
}

struct IndexedObserver<'a> {
    global_indices: &'a [usize],
    outer: &'a dyn WriteCompletionObserver,
    panic: &'a ParkingMutex<Option<Box<dyn Any + Send>>>,
}

impl WriteCompletionObserver for IndexedObserver<'_> {
    fn writes_completed(&self, op_indices: &[usize], service_ns: u64) {
        let indices: Vec<_> = op_indices
            .iter()
            .filter_map(|&index| self.global_indices.get(index).copied())
            .collect();
        if indices.is_empty() {
            return;
        }

        let mut panic = self.panic.lock();
        if panic.is_some() {
            return;
        }
        if let Err(payload) = catch_unwind(AssertUnwindSafe(|| {
            self.outer.writes_completed(&indices, service_ns);
        })) {
            *panic = Some(payload);
        }
    }
}

fn collect_group_results(
    receiver: crossbeam_channel::Receiver<(Vec<usize>, Vec<ChunkletResult<()>>)>,
    op_count: usize,
) -> Vec<ChunkletResult<()>> {
    let mut output: Vec<Option<ChunkletResult<()>>> =
        std::iter::repeat_with(|| None).take(op_count).collect();
    for (indices, results) in receiver {
        for (index, result) in indices.into_iter().zip(results) {
            output[index] = Some(result);
        }
    }
    output
        .into_iter()
        .map(|result| {
            result.unwrap_or_else(|| {
                Err(ChunkletError::Invariant(
                    "pooled io_uring execution omitted a write result".into(),
                ))
            })
        })
        .collect()
}

fn cpu_sets_disjoint(foreground: &[usize], background: &[usize]) -> bool {
    !foreground.is_empty()
        && !background.is_empty()
        && foreground.iter().all(|cpu| !background.contains(cpu))
}

fn build_pool(
    name: &'static str,
    workers: usize,
    cpus: &[usize],
) -> std::io::Result<Option<ThreadPool>> {
    if workers == 0 {
        return Ok(None);
    }
    let mut builder = ThreadPoolBuilder::new()
        .num_threads(workers)
        .thread_name(move |index| format!("{name}-{index}"));
    if !cpus.is_empty() {
        let cpus = cpus.to_vec();
        builder = builder.start_handler(move |worker| {
            if let Err(error) = crate::numa::bind_current_to_cpus(&cpus) {
                tracing::warn!(
                    pool = name,
                    worker,
                    error = %error,
                    "failed to bind persistent io_uring worker"
                );
            }
        });
    }
    builder
        .build()
        .map(Some)
        .map_err(|error| std::io::Error::other(format!("build {name} pool: {error}")))
}

struct PdGroup {
    pd_id: PdId,
    indices: Vec<usize>,
    waiter: Option<PdLaneWaiter>,
}

fn group_indices_by_pd(ops: &[StripWrite<'_>]) -> Vec<PdGroup> {
    let mut groups = BTreeMap::new();
    for (index, op) in ops.iter().enumerate() {
        groups
            .entry(op.pd.pd_id())
            .or_insert_with(Vec::new)
            .push(index);
    }
    groups
        .into_iter()
        .map(|(pd_id, indices)| PdGroup {
            pd_id,
            indices,
            waiter: None,
        })
        .collect()
}

fn normalize_result_count(results: &mut Vec<ChunkletResult<()>>, expected: usize) {
    if results.len() == expected {
        return;
    }
    let actual = results.len();
    *results = (0..expected)
        .map(|_| {
            Err(ChunkletError::Invariant(format!(
                "pooled io_uring group returned {actual} results for {expected} writes"
            )))
        })
        .collect();
}

fn class_index(class: IoClass) -> usize {
    match class {
        IoClass::Foreground => 0,
        IoClass::DrainData => 1,
        IoClass::DrainMeta => 2,
        IoClass::Maintenance => 3,
    }
}

fn elapsed_ns(started: Instant) -> u64 {
    started.elapsed().as_nanos().min(u64::MAX as u128) as u64
}

fn saturating_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

fn update_max(target: &AtomicU64, value: u64) {
    let mut current = target.load(Ordering::Relaxed);
    while value > current {
        match target.compare_exchange_weak(current, value, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => break,
            Err(observed) => current = observed,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::panic::{catch_unwind, AssertUnwindSafe};
    use std::sync::atomic::AtomicUsize;
    use std::sync::mpsc;
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    use parking_lot::{Condvar, Mutex};
    use tempfile::TempDir;

    use crate::io::sync_backend::SyncBackend;
    use crate::io::{RawDevice, ScheduledBackend, SchedulerConfig};
    use crate::pd::PhysicalDisk;
    use crate::types::{PdId, PoolId};

    struct Rendezvous {
        arrived: Mutex<usize>,
        changed: Condvar,
        target: usize,
    }

    #[derive(Default)]
    struct OrderedGateState {
        slow_started: bool,
        release_slow: bool,
    }

    struct OrderedGate {
        state: Mutex<OrderedGateState>,
        changed: Condvar,
    }

    #[derive(Default)]
    struct LaneOrderState {
        started: Vec<usize>,
        released_through: usize,
    }

    struct LaneOrderGate {
        state: Mutex<LaneOrderState>,
        changed: Condvar,
    }

    impl LaneOrderGate {
        fn new() -> Self {
            Self {
                state: Mutex::new(LaneOrderState::default()),
                changed: Condvar::new(),
            }
        }

        fn run_slow(&self, marker: usize) {
            let mut state = self.state.lock();
            state.started.push(marker);
            self.changed.notify_all();
            let deadline = Instant::now() + Duration::from_secs(2);
            while state.released_through < marker {
                let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
                    panic!("timed out waiting to release slow lane task {marker}");
                };
                if self.changed.wait_for(&mut state, remaining).timed_out()
                    && state.released_through < marker
                {
                    panic!("timed out waiting to release slow lane task {marker}");
                }
            }
        }

        fn wait_for_started(&self, count: usize) -> Vec<usize> {
            let mut state = self.state.lock();
            let deadline = Instant::now() + Duration::from_secs(2);
            while state.started.len() < count {
                let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
                    panic!("timed out waiting for {count} slow lane tasks");
                };
                if self.changed.wait_for(&mut state, remaining).timed_out()
                    && state.started.len() < count
                {
                    panic!("timed out waiting for {count} slow lane tasks");
                }
            }
            state.started.clone()
        }

        fn release(&self, marker: usize) {
            let mut state = self.state.lock();
            state.released_through = state.released_through.max(marker);
            self.changed.notify_all();
        }
    }

    impl OrderedGate {
        fn new() -> Self {
            Self {
                state: Mutex::new(OrderedGateState::default()),
                changed: Condvar::new(),
            }
        }

        fn start_slow_and_wait(&self) -> bool {
            let mut state = self.state.lock();
            state.slow_started = true;
            self.changed.notify_all();
            let deadline = Instant::now() + Duration::from_secs(2);
            while !state.release_slow {
                let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
                    return false;
                };
                if self.changed.wait_for(&mut state, remaining).timed_out() && !state.release_slow {
                    return false;
                }
            }
            true
        }

        fn wait_for_slow(&self) -> bool {
            let mut state = self.state.lock();
            let deadline = Instant::now() + Duration::from_secs(2);
            while !state.slow_started {
                let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
                    return false;
                };
                if self.changed.wait_for(&mut state, remaining).timed_out() && !state.slow_started {
                    return false;
                }
            }
            true
        }

        fn release_slow(&self) {
            let mut state = self.state.lock();
            state.release_slow = true;
            self.changed.notify_all();
        }
    }

    struct RecordingObserver {
        calls: Mutex<Vec<(Vec<usize>, u64)>>,
        gate: Arc<OrderedGate>,
    }

    struct PanicObserver {
        calls: AtomicUsize,
    }

    struct FirstSlowGateBackend {
        slow_pd: PdId,
        slow_calls: AtomicUsize,
        read_calls: AtomicUsize,
        first_slow_started: mpsc::Sender<()>,
        release_first_slow: Mutex<mpsc::Receiver<()>>,
        fast_submitted: mpsc::Sender<()>,
        flush_submitted: Option<mpsc::Sender<()>>,
    }

    impl IoBackend for FirstSlowGateBackend {
        fn submit_reads(&self, _ops: &mut [StripRead<'_>]) -> ChunkletResult<()> {
            self.read_calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        fn submit_writes_detailed(&self, _ops: &[StripWrite<'_>]) -> Vec<ChunkletResult<()>> {
            panic!("scheduled test backend must use the observed path")
        }

        fn submit_writes_detailed_observed_with_class(
            &self,
            _class: IoClass,
            ops: &[StripWrite<'_>],
            observer: &dyn WriteCompletionObserver,
        ) -> Vec<ChunkletResult<()>> {
            assert!(!ops.is_empty());
            let pd_id = ops[0].pd.pd_id();
            assert!(ops.iter().all(|op| op.pd.pd_id() == pd_id));
            if pd_id == self.slow_pd && self.slow_calls.fetch_add(1, Ordering::SeqCst) == 0 {
                self.first_slow_started.send(()).unwrap();
                self.release_first_slow.lock().recv().unwrap();
            } else if pd_id != self.slow_pd {
                self.fast_submitted.send(()).unwrap();
            }
            let indices: Vec<_> = (0..ops.len()).collect();
            observer.writes_completed(&indices, 1);
            ops.iter().map(|_| Ok(())).collect()
        }

        fn submit_flushes(&self, _pds: &[Arc<PhysicalDisk>]) -> ChunkletResult<()> {
            if let Some(submitted) = &self.flush_submitted {
                submitted.send(()).unwrap();
            }
            Ok(())
        }

        fn name(&self) -> &'static str {
            "first-slow-gate"
        }
    }

    impl WriteCompletionObserver for PanicObserver {
        fn writes_completed(&self, _op_indices: &[usize], _service_ns: u64) {
            self.calls.fetch_add(1, Ordering::Relaxed);
            panic!("observer panic");
        }
    }

    impl WriteCompletionObserver for RecordingObserver {
        fn writes_completed(&self, op_indices: &[usize], service_ns: u64) {
            self.calls.lock().push((op_indices.to_vec(), service_ns));
            self.gate.release_slow();
        }
    }

    impl Rendezvous {
        fn new(target: usize) -> Self {
            Self {
                arrived: Mutex::new(0),
                changed: Condvar::new(),
                target,
            }
        }

        fn meet(&self) -> bool {
            let mut arrived = self.arrived.lock();
            *arrived += 1;
            self.changed.notify_all();
            let deadline = Instant::now() + Duration::from_secs(2);
            while *arrived < self.target {
                let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
                    return false;
                };
                if self.changed.wait_for(&mut arrived, remaining).timed_out()
                    && *arrived < self.target
                {
                    return false;
                }
            }
            true
        }
    }

    fn test_pd_with_sync(
        dir: &TempDir,
        name: &str,
        pd_seq: u32,
        pool_id: PoolId,
        sync_required: bool,
    ) -> Arc<PhysicalDisk> {
        let mut raw =
            RawDevice::open_or_create(&dir.path().join(name), 4 * 1024 * 1024 * 1024).unwrap();
        raw.set_sync_required_for_test(sync_required);
        PhysicalDisk::init(
            raw,
            pool_id,
            PdId::new_v4(),
            pd_seq,
            1,
            vec![],
            0,
            vec![],
            vec![],
        )
        .unwrap()
    }

    fn test_pd(dir: &TempDir, name: &str, pd_seq: u32, pool_id: PoolId) -> Arc<PhysicalDisk> {
        test_pd_with_sync(dir, name, pd_seq, pool_id, true)
    }

    fn wait_until(mut predicate: impl FnMut() -> bool) {
        let deadline = Instant::now() + Duration::from_secs(2);
        while !predicate() {
            assert!(Instant::now() < deadline, "timed out waiting for state");
            thread::sleep(Duration::from_millis(1));
        }
    }

    fn pd_totals(backend: &ScheduledBackend, pd_id: PdId) -> (u64, u64) {
        backend
            .snapshot()
            .pds
            .into_iter()
            .find(|pd| pd.pd_id == pd_id)
            .map(|pd| (pd.total_queued_blocks, pd.total_active_blocks))
            .unwrap_or((0, 0))
    }

    fn four_interleaved_ops<'a>(
        first: &Arc<PhysicalDisk>,
        second: &Arc<PhysicalDisk>,
        pages: &'a [[u8; 4096]; 4],
    ) -> Vec<StripWrite<'a>> {
        vec![
            StripWrite {
                pd: first.clone(),
                chunklet_index: 0,
                in_chunklet_off: 0,
                data: &pages[0],
            },
            StripWrite {
                pd: second.clone(),
                chunklet_index: 0,
                in_chunklet_off: 4096,
                data: &pages[1],
            },
            StripWrite {
                pd: first.clone(),
                chunklet_index: 0,
                in_chunklet_off: 8192,
                data: &pages[2],
            },
            StripWrite {
                pd: second.clone(),
                chunklet_index: 0,
                in_chunklet_off: 12288,
                data: &pages[3],
            },
        ]
    }

    fn lane_group(pd_id: PdId) -> PdGroup {
        PdGroup {
            pd_id,
            indices: vec![0],
            waiter: None,
        }
    }

    fn spawn_slow_lane_submit(
        pools: Arc<ScopedWritePools>,
        pd: Arc<PhysicalDisk>,
        gate: Arc<LaneOrderGate>,
        marker: usize,
    ) -> thread::JoinHandle<Vec<ChunkletResult<()>>> {
        thread::spawn(move || {
            let page = [marker as u8; 4096];
            pools.submit(
                IoClass::Foreground,
                &[StripWrite {
                    pd,
                    chunklet_index: 0,
                    in_chunklet_off: (marker as u64) * 4096,
                    data: &page,
                }],
                move |group| {
                    gate.run_slow(marker);
                    group.iter().map(|_| Ok(())).collect()
                },
            )
        })
    }

    #[test]
    fn registered_scheduler_pds_seed_and_rebalance_execution_lane_caps() {
        let first = PdId::new_v4();
        let second = PdId::new_v4();
        let third = PdId::new_v4();
        let scheduled = Arc::new(
            ScheduledBackend::new(Arc::new(SyncBackend), SchedulerConfig::new(1)).unwrap(),
        );
        scheduled.register_pd(first);
        scheduled.register_pd(second);

        let execution = ExecutionPoolBackend::new(scheduled, UringPoolConfig::new(5, 3)).unwrap();
        let mut foreground_caps: Vec<_> = execution
            .write_pools
            .foreground_lanes
            .snapshot()
            .into_iter()
            .map(|(_, cap, _)| cap)
            .collect();
        foreground_caps.sort_unstable();
        assert_eq!(foreground_caps, vec![2, 3]);
        assert_eq!(foreground_caps.iter().sum::<usize>(), 5);

        execution.register_pd(third);
        let foreground = execution.write_pools.foreground_lanes.snapshot();
        let background = execution.write_pools.background_lanes.snapshot();
        assert_eq!(foreground.len(), 3);
        assert_eq!(foreground.iter().map(|(_, cap, _)| cap).sum::<usize>(), 5);
        assert_eq!(background.len(), 3);
        assert_eq!(background.iter().map(|(_, cap, _)| cap).sum::<usize>(), 3);
    }

    #[test]
    fn fewer_workers_than_pds_keep_every_lane_runnable_under_global_cap() {
        let lanes = Arc::new(PdLaneAdmission::new(2));
        let mut pending = vec![
            lane_group(PdId::new_v4()),
            lane_group(PdId::new_v4()),
            lane_group(PdId::new_v4()),
        ];
        lanes.enqueue_groups(&mut pending);
        assert!(lanes.snapshot().iter().all(|(_, cap, _)| *cap == 1));

        let (first_index, first) = lanes.acquire_any(&mut pending);
        pending.remove(first_index);
        let (second_index, second) = lanes.acquire_any(&mut pending);
        pending.remove(second_index);
        assert_eq!(
            lanes
                .snapshot()
                .iter()
                .map(|(_, _, active)| active)
                .sum::<usize>(),
            2
        );

        drop(first);
        let (third_index, third) = lanes.acquire_any(&mut pending);
        pending.remove(third_index);
        drop(second);
        drop(third);
        assert!(lanes.snapshot().iter().all(|(_, _, active)| *active == 0));
    }

    #[test]
    fn foreground_and_background_lanes_are_independent_and_background_is_shared() {
        let pools = ScopedWritePools::with_config(UringPoolConfig::new(1, 1)).unwrap();
        let pd_id = PdId::new_v4();
        pools.register_pd(pd_id);
        assert!(!Arc::ptr_eq(
            pools.lanes(IoClass::Foreground),
            pools.lanes(IoClass::DrainData)
        ));
        assert!(Arc::ptr_eq(
            pools.lanes(IoClass::DrainData),
            pools.lanes(IoClass::DrainMeta)
        ));
        assert!(Arc::ptr_eq(
            pools.lanes(IoClass::DrainMeta),
            pools.lanes(IoClass::Maintenance)
        ));

        let mut foreground = vec![lane_group(pd_id)];
        let mut background = vec![lane_group(pd_id)];
        pools.foreground_lanes.enqueue_groups(&mut foreground);
        pools.background_lanes.enqueue_groups(&mut background);
        let (foreground_index, foreground_permit) =
            pools.foreground_lanes.acquire_any(&mut foreground);
        foreground.remove(foreground_index);
        let (background_index, background_permit) =
            pools.background_lanes.acquire_any(&mut background);
        background.remove(background_index);
        assert_eq!(pools.foreground_lanes.snapshot()[0].2, 1);
        assert_eq!(pools.background_lanes.snapshot()[0].2, 1);
        drop(foreground_permit);
        drop(background_permit);
    }

    #[test]
    fn panicking_lane_task_releases_permit_and_wakes_fifo_waiter() {
        let lanes = Arc::new(PdLaneAdmission::new(1));
        let pd_id = PdId::new_v4();
        let mut first_group = vec![lane_group(pd_id)];
        lanes.enqueue_groups(&mut first_group);
        let (first_index, first_permit) = lanes.acquire_any(&mut first_group);
        first_group.remove(first_index);

        let (queued_tx, queued_rx) = mpsc::channel();
        let (acquired_tx, acquired_rx) = mpsc::channel();
        let waiter_lanes = lanes.clone();
        let waiter = thread::spawn(move || {
            let mut pending = vec![lane_group(pd_id)];
            waiter_lanes.enqueue_groups(&mut pending);
            queued_tx.send(()).unwrap();
            let (index, permit) = waiter_lanes.acquire_any(&mut pending);
            pending.remove(index);
            acquired_tx.send(()).unwrap();
            drop(permit);
        });
        queued_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        wait_until(|| lanes.waiter_count(pd_id) == 1);

        let unwind = catch_unwind(AssertUnwindSafe(move || {
            let _permit = first_permit;
            panic!("simulated execution task panic");
        }));
        assert!(unwind.is_err());
        acquired_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("panic did not release and notify the next PD waiter");
        waiter.join().unwrap();
        assert_eq!(lanes.snapshot()[0].2, 0);
    }

    #[test]
    fn same_pd_waiters_do_not_occupy_fast_pd_worker_and_admit_fifo() {
        let dir = TempDir::new().unwrap();
        let pool_id = PoolId::new_v4();
        let slow_pd = test_pd(&dir, "slow-lane", 0, pool_id);
        let fast_pd = test_pd(&dir, "fast-lane", 1, pool_id);
        let pools = Arc::new(ScopedWritePools::with_config(UringPoolConfig::new(2, 0)).unwrap());
        pools.register_pd(slow_pd.pd_id());
        pools.register_pd(fast_pd.pd_id());
        let gate = Arc::new(LaneOrderGate::new());

        let first = spawn_slow_lane_submit(pools.clone(), slow_pd.clone(), gate.clone(), 1);
        assert_eq!(gate.wait_for_started(1), vec![1]);
        let second = spawn_slow_lane_submit(pools.clone(), slow_pd.clone(), gate.clone(), 2);
        wait_until(|| pools.foreground_lanes.waiter_count(slow_pd.pd_id()) == 1);
        let third = spawn_slow_lane_submit(pools.clone(), slow_pd.clone(), gate.clone(), 3);
        wait_until(|| pools.foreground_lanes.waiter_count(slow_pd.pd_id()) == 2);
        thread::sleep(Duration::from_millis(20));

        let (fast_started_tx, fast_started_rx) = mpsc::channel();
        let fast_pools = pools.clone();
        let fast = thread::spawn(move || {
            let page = [0xf1; 4096];
            fast_pools.submit(
                IoClass::Foreground,
                &[StripWrite {
                    pd: fast_pd,
                    chunklet_index: 0,
                    in_chunklet_off: 0,
                    data: &page,
                }],
                move |group| {
                    fast_started_tx.send(()).unwrap();
                    group.iter().map(|_| Ok(())).collect()
                },
            )
        });
        fast_started_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("same-PD waiters consumed the fast PD's worker lane");
        assert!(fast.join().unwrap().iter().all(Result::is_ok));
        assert_eq!(gate.wait_for_started(1), vec![1]);

        gate.release(1);
        assert_eq!(gate.wait_for_started(2), vec![1, 2]);
        gate.release(2);
        assert_eq!(gate.wait_for_started(3), vec![1, 2, 3]);
        gate.release(3);
        assert!(first.join().unwrap().iter().all(Result::is_ok));
        assert!(second.join().unwrap().iter().all(Result::is_ok));
        assert!(third.join().unwrap().iter().all(Result::is_ok));
        assert!(pools.snapshot().classes[0].queue_wait_max_ns >= 10_000_000);
    }

    #[test]
    fn execution_pool_reentry_panics_without_leaking_lane_waiter() {
        let dir = TempDir::new().unwrap();
        let pool_id = PoolId::new_v4();
        let pd = test_pd(&dir, "reentry", 0, pool_id);
        let pd_id = pd.pd_id();
        let pools = ScopedWritePools::with_config(UringPoolConfig::new(1, 0)).unwrap();
        pools.register_pd(pd_id);
        let page = [0xc1; 4096];

        let unwind = catch_unwind(AssertUnwindSafe(|| {
            pools.foreground.as_ref().unwrap().install(|| {
                pools.submit(
                    IoClass::Foreground,
                    &[StripWrite {
                        pd,
                        chunklet_index: 0,
                        in_chunklet_off: 0,
                        data: &page,
                    }],
                    |group| group.iter().map(|_| Ok(())).collect(),
                )
            })
        }));

        assert!(unwind.is_err());
        assert_eq!(pools.foreground_lanes.waiter_count(pd_id), 0);
        assert_eq!(pools.foreground_lanes.snapshot()[0].2, 0);
    }

    #[test]
    fn pd_groups_run_concurrently_and_results_return_in_input_order() {
        let dir = TempDir::new().unwrap();
        let pool_id = PoolId::new_v4();
        let first = test_pd(&dir, "pd0", 0, pool_id);
        let second = test_pd(&dir, "pd1", 1, pool_id);
        let pages = [[0x11; 4096], [0x22; 4096], [0x33; 4096], [0x44; 4096]];
        let ops = four_interleaved_ops(&first, &second, &pages);
        let pools = ScopedWritePools::with_config(UringPoolConfig::new(2, 0)).unwrap();
        let rendezvous = Arc::new(Rendezvous::new(2));

        let results = pools.submit(IoClass::Foreground, &ops, {
            let rendezvous = rendezvous.clone();
            move |group| {
                let concurrent = rendezvous.meet();
                group
                    .iter()
                    .map(|op| {
                        if !concurrent {
                            return Err(ChunkletError::Invariant(
                                "PD groups did not overlap".into(),
                            ));
                        }
                        if matches!(op.in_chunklet_off, 4096 | 12288) {
                            Err(ChunkletError::Invariant(format!(
                                "marker-{}",
                                op.in_chunklet_off
                            )))
                        } else {
                            Ok(())
                        }
                    })
                    .collect()
            }
        });

        assert!(results[0].is_ok());
        assert_eq!(
            results[1].as_ref().unwrap_err().to_string(),
            "invariant violated: marker-4096"
        );
        assert!(results[2].is_ok());
        assert_eq!(
            results[3].as_ref().unwrap_err().to_string(),
            "invariant violated: marker-12288"
        );
        let foreground = &pools.snapshot().classes[0];
        assert_eq!(foreground.batches, 1);
        assert_eq!(foreground.groups, 2);
        assert_eq!(foreground.ops, 4);
        assert!(foreground.execute_ns > 0);
    }

    #[test]
    fn observed_submit_maps_each_pd_cqe_before_batch_return() {
        let dir = TempDir::new().unwrap();
        let pool_id = PoolId::new_v4();
        let fast_pd = test_pd(&dir, "pd0", 0, pool_id);
        let slow_pd = test_pd(&dir, "pd1", 1, pool_id);
        let pages = [[0x11; 4096], [0x22; 4096], [0x33; 4096], [0x44; 4096]];
        let ops = four_interleaved_ops(&fast_pd, &slow_pd, &pages);
        let pools = ScopedWritePools::with_config(UringPoolConfig::new(2, 0)).unwrap();
        let gate = Arc::new(OrderedGate::new());
        let observer = RecordingObserver {
            calls: Mutex::new(Vec::new()),
            gate: gate.clone(),
        };
        let slow_id = slow_pd.pd_id();

        let results = pools.submit_observed(
            IoClass::Foreground,
            &ops,
            &observer,
            move |group, completion, started| {
                let overlapped = if group[0].pd.pd_id() == slow_id {
                    let ready = gate.start_slow_and_wait();
                    completion.writes_completed(&[0, 1], elapsed_ns(started));
                    ready
                } else {
                    let ready = gate.wait_for_slow();
                    completion.writes_completed(&[0], elapsed_ns(started));
                    completion.writes_completed(&[1], elapsed_ns(started));
                    ready
                };
                group
                    .iter()
                    .map(|op| {
                        if overlapped {
                            Ok(())
                        } else {
                            Err(ChunkletError::Invariant(format!(
                                "group timed out at {}",
                                op.in_chunklet_off
                            )))
                        }
                    })
                    .collect()
            },
        );

        assert!(results.iter().all(Result::is_ok));
        let calls = observer.calls.lock();
        assert_eq!(calls[0].0, vec![0]);
        assert!(calls.iter().any(|call| call.0 == vec![2]));
        assert!(calls.iter().any(|call| call.0 == vec![1, 3]));
        let mut completed: Vec<_> = calls
            .iter()
            .flat_map(|(indices, _)| indices.iter().copied())
            .collect();
        completed.sort_unstable();
        assert_eq!(completed, vec![0, 1, 2, 3]);
        assert!(calls.iter().all(|call| call.1 > 0));
    }

    #[test]
    fn blocked_pd_child_does_not_hold_back_fast_pd_dispatch() {
        let dir = TempDir::new().unwrap();
        let pool_id = PoolId::new_v4();
        let slow_pd = test_pd(&dir, "slow", 0, pool_id);
        let fast_pd = test_pd(&dir, "fast", 1, pool_id);
        let (slow_started_tx, slow_started_rx) = mpsc::channel();
        let (release_slow_tx, release_slow_rx) = mpsc::channel();
        let (fast_submitted_tx, fast_submitted_rx) = mpsc::channel();
        let device = Arc::new(FirstSlowGateBackend {
            slow_pd: slow_pd.pd_id(),
            slow_calls: AtomicUsize::new(0),
            read_calls: AtomicUsize::new(0),
            first_slow_started: slow_started_tx,
            release_first_slow: Mutex::new(release_slow_rx),
            fast_submitted: fast_submitted_tx,
            flush_submitted: None,
        });
        let scheduled = Arc::new(ScheduledBackend::new(device, SchedulerConfig::new(1)).unwrap());
        scheduled.register_pd(slow_pd.pd_id());
        scheduled.register_pd(fast_pd.pd_id());
        let execution = Arc::new(
            ExecutionPoolBackend::new(scheduled.clone(), UringPoolConfig::new(2, 0)).unwrap(),
        );

        let holder_backend = scheduled.clone();
        let holder_pd = slow_pd.clone();
        let holder = thread::spawn(move || {
            let page = [0x31; 4096];
            holder_backend.submit_writes_detailed_with_class(
                IoClass::Foreground,
                &[StripWrite {
                    pd: holder_pd,
                    chunklet_index: 0,
                    in_chunklet_off: 0,
                    data: &page,
                }],
            )
        });
        slow_started_rx
            .recv_timeout(Duration::from_secs(2))
            .unwrap();

        let child_backend = execution.clone();
        let child_slow_pd = slow_pd.clone();
        let child_fast_pd = fast_pd.clone();
        let children = thread::spawn(move || {
            let slow_page = [0x41; 4096];
            let fast_page = [0x51; 4096];
            child_backend.submit_writes_detailed_with_class(
                IoClass::Foreground,
                &[
                    StripWrite {
                        pd: child_slow_pd,
                        chunklet_index: 0,
                        in_chunklet_off: 4096,
                        data: &slow_page,
                    },
                    StripWrite {
                        pd: child_fast_pd,
                        chunklet_index: 0,
                        in_chunklet_off: 4096,
                        data: &fast_page,
                    },
                ],
            )
        });

        wait_until(|| pd_totals(&scheduled, slow_pd.pd_id()) == (1, 1));
        fast_submitted_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("fast PD child was coupled to blocked slow-PD admission");
        assert_eq!(pd_totals(&scheduled, fast_pd.pd_id()), (0, 0));

        release_slow_tx.send(()).unwrap();
        assert!(holder.join().unwrap().iter().all(Result::is_ok));
        assert!(children.join().unwrap().iter().all(Result::is_ok));
        assert_eq!(pd_totals(&scheduled, slow_pd.pd_id()), (0, 0));
        let execution_snapshot = execution.execution_snapshot().unwrap();
        assert_eq!(execution_snapshot.classes[0].groups, 2);
    }

    #[test]
    fn worker_queue_wait_is_not_counted_as_scheduler_active() {
        let dir = TempDir::new().unwrap();
        let pool_id = PoolId::new_v4();
        let slow_pd = test_pd(&dir, "slow", 0, pool_id);
        let fast_pd = test_pd(&dir, "fast", 1, pool_id);
        let (slow_started_tx, slow_started_rx) = mpsc::channel();
        let (release_slow_tx, release_slow_rx) = mpsc::channel();
        let (fast_submitted_tx, fast_submitted_rx) = mpsc::channel();
        let device = Arc::new(FirstSlowGateBackend {
            slow_pd: slow_pd.pd_id(),
            slow_calls: AtomicUsize::new(0),
            read_calls: AtomicUsize::new(0),
            first_slow_started: slow_started_tx,
            release_first_slow: Mutex::new(release_slow_rx),
            fast_submitted: fast_submitted_tx,
            flush_submitted: None,
        });
        let scheduled = Arc::new(ScheduledBackend::new(device, SchedulerConfig::new(1)).unwrap());
        scheduled.register_pd(slow_pd.pd_id());
        scheduled.register_pd(fast_pd.pd_id());
        let execution = Arc::new(
            ExecutionPoolBackend::new(scheduled.clone(), UringPoolConfig::new(1, 0)).unwrap(),
        );

        let slow_backend = execution.clone();
        let submitted_slow_pd = slow_pd.clone();
        let slow = thread::spawn(move || {
            let page = [0x61; 4096];
            slow_backend.submit_writes_detailed_with_class(
                IoClass::Foreground,
                &[StripWrite {
                    pd: submitted_slow_pd,
                    chunklet_index: 0,
                    in_chunklet_off: 0,
                    data: &page,
                }],
            )
        });
        slow_started_rx
            .recv_timeout(Duration::from_secs(2))
            .unwrap();

        let fast_backend = execution.clone();
        let submitted_fast_pd = fast_pd.clone();
        let fast = thread::spawn(move || {
            let page = [0x71; 4096];
            fast_backend.submit_writes_detailed_with_class(
                IoClass::Foreground,
                &[StripWrite {
                    pd: submitted_fast_pd,
                    chunklet_index: 0,
                    in_chunklet_off: 0,
                    data: &page,
                }],
            )
        });
        wait_until(|| execution.execution_snapshot().unwrap().classes[0].batches == 2);
        thread::sleep(Duration::from_millis(20));
        assert_eq!(pd_totals(&scheduled, slow_pd.pd_id()), (0, 1));
        assert_eq!(pd_totals(&scheduled, fast_pd.pd_id()), (0, 0));
        assert!(fast_submitted_rx.try_recv().is_err());

        release_slow_tx.send(()).unwrap();
        fast_submitted_rx
            .recv_timeout(Duration::from_secs(2))
            .unwrap();
        assert!(slow.join().unwrap().iter().all(Result::is_ok));
        assert!(fast.join().unwrap().iter().all(Result::is_ok));
        let foreground = &execution.execution_snapshot().unwrap().classes[0];
        assert!(foreground.queue_wait_max_ns >= 10_000_000);
        assert_eq!(pd_totals(&scheduled, fast_pd.pd_id()), (0, 0));
    }

    #[test]
    fn flush_waits_for_outer_queue_and_reads_delegate() {
        let dir = TempDir::new().unwrap();
        let pool_id = PoolId::new_v4();
        let slow_pd = test_pd(&dir, "slow", 0, pool_id);
        let fast_pd = test_pd(&dir, "fast", 1, pool_id);
        let (slow_started_tx, slow_started_rx) = mpsc::channel();
        let (release_slow_tx, release_slow_rx) = mpsc::channel();
        let (fast_submitted_tx, fast_submitted_rx) = mpsc::channel();
        let (flush_submitted_tx, flush_submitted_rx) = mpsc::channel();
        let device = Arc::new(FirstSlowGateBackend {
            slow_pd: slow_pd.pd_id(),
            slow_calls: AtomicUsize::new(0),
            read_calls: AtomicUsize::new(0),
            first_slow_started: slow_started_tx,
            release_first_slow: Mutex::new(release_slow_rx),
            fast_submitted: fast_submitted_tx,
            flush_submitted: Some(flush_submitted_tx),
        });
        let scheduled =
            Arc::new(ScheduledBackend::new(device.clone(), SchedulerConfig::new(1)).unwrap());
        scheduled.register_pd(slow_pd.pd_id());
        scheduled.register_pd(fast_pd.pd_id());
        let execution =
            Arc::new(ExecutionPoolBackend::new(scheduled, UringPoolConfig::new(1, 0)).unwrap());

        let mut read_page = [0_u8; 4096];
        execution
            .submit_reads(&mut [StripRead {
                pd: fast_pd.clone(),
                chunklet_index: 0,
                in_chunklet_off: 0,
                data: &mut read_page,
            }])
            .unwrap();
        assert_eq!(device.read_calls.load(Ordering::SeqCst), 1);

        let slow_backend = execution.clone();
        let submitted_slow_pd = slow_pd.clone();
        let slow = thread::spawn(move || {
            let page = [0x81; 4096];
            slow_backend.submit_writes_detailed_with_class(
                IoClass::Foreground,
                &[StripWrite {
                    pd: submitted_slow_pd,
                    chunklet_index: 0,
                    in_chunklet_off: 0,
                    data: &page,
                }],
            )
        });
        slow_started_rx
            .recv_timeout(Duration::from_secs(2))
            .unwrap();

        let fast_backend = execution.clone();
        let submitted_fast_pd = fast_pd.clone();
        let fast = thread::spawn(move || {
            let page = [0x91; 4096];
            fast_backend.submit_writes_detailed_with_class(
                IoClass::Foreground,
                &[StripWrite {
                    pd: submitted_fast_pd,
                    chunklet_index: 0,
                    in_chunklet_off: 0,
                    data: &page,
                }],
            )
        });
        wait_until(|| execution.execution_snapshot().unwrap().classes[0].batches == 2);

        let flush_backend = execution.clone();
        let flush_pds = vec![slow_pd, fast_pd];
        let flush = thread::spawn(move || flush_backend.submit_flushes(&flush_pds));
        assert!(flush_submitted_rx
            .recv_timeout(Duration::from_millis(20))
            .is_err());

        release_slow_tx.send(()).unwrap();
        fast_submitted_rx
            .recv_timeout(Duration::from_secs(2))
            .unwrap();
        flush_submitted_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("flush passed queued execution work");
        assert!(slow.join().unwrap().iter().all(Result::is_ok));
        assert!(fast.join().unwrap().iter().all(Result::is_ok));
        flush.join().unwrap().unwrap();
    }

    #[test]
    fn write_through_flush_does_not_wait_for_outer_queue() {
        let dir = TempDir::new().unwrap();
        let pool_id = PoolId::new_v4();
        let slow_pd = test_pd_with_sync(&dir, "slow", 0, pool_id, false);
        let fast_pd = test_pd_with_sync(&dir, "fast", 1, pool_id, false);
        let (slow_started_tx, slow_started_rx) = mpsc::channel();
        let (release_slow_tx, release_slow_rx) = mpsc::channel();
        let (fast_submitted_tx, fast_submitted_rx) = mpsc::channel();
        let (flush_submitted_tx, flush_submitted_rx) = mpsc::channel();
        let device = Arc::new(FirstSlowGateBackend {
            slow_pd: slow_pd.pd_id(),
            slow_calls: AtomicUsize::new(0),
            read_calls: AtomicUsize::new(0),
            first_slow_started: slow_started_tx,
            release_first_slow: Mutex::new(release_slow_rx),
            fast_submitted: fast_submitted_tx,
            flush_submitted: Some(flush_submitted_tx),
        });
        let scheduled = Arc::new(ScheduledBackend::new(device, SchedulerConfig::new(1)).unwrap());
        scheduled.register_pd(slow_pd.pd_id());
        scheduled.register_pd(fast_pd.pd_id());
        let execution =
            Arc::new(ExecutionPoolBackend::new(scheduled, UringPoolConfig::new(1, 0)).unwrap());

        let slow_backend = execution.clone();
        let submitted_slow_pd = slow_pd.clone();
        let slow = thread::spawn(move || {
            let page = [0xa1; 4096];
            slow_backend.submit_writes_detailed_with_class(
                IoClass::Foreground,
                &[StripWrite {
                    pd: submitted_slow_pd,
                    chunklet_index: 0,
                    in_chunklet_off: 0,
                    data: &page,
                }],
            )
        });
        slow_started_rx
            .recv_timeout(Duration::from_secs(2))
            .unwrap();

        let fast_backend = execution.clone();
        let submitted_fast_pd = fast_pd.clone();
        let fast = thread::spawn(move || {
            let page = [0xb1; 4096];
            fast_backend.submit_writes_detailed_with_class(
                IoClass::Foreground,
                &[StripWrite {
                    pd: submitted_fast_pd,
                    chunklet_index: 0,
                    in_chunklet_off: 0,
                    data: &page,
                }],
            )
        });
        wait_until(|| execution.execution_snapshot().unwrap().classes[0].batches == 2);

        let flush_backend = execution.clone();
        let flush_pds = vec![slow_pd, fast_pd];
        let flush = thread::spawn(move || flush_backend.submit_flushes(&flush_pds));
        flush_submitted_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("write-through no-op flush waited behind the execution queue");
        flush.join().unwrap().unwrap();
        assert!(fast_submitted_rx.try_recv().is_err());

        release_slow_tx.send(()).unwrap();
        fast_submitted_rx
            .recv_timeout(Duration::from_secs(2))
            .unwrap();
        assert!(slow.join().unwrap().iter().all(Result::is_ok));
        assert!(fast.join().unwrap().iter().all(Result::is_ok));
    }

    #[test]
    fn observer_panic_waits_for_every_pooled_group_and_stops_notifications() {
        let dir = TempDir::new().unwrap();
        let pool_id = PoolId::new_v4();
        let first = test_pd(&dir, "pd0", 0, pool_id);
        let second = test_pd(&dir, "pd1", 1, pool_id);
        let pages = [[0x11; 4096], [0x22; 4096], [0x33; 4096], [0x44; 4096]];
        let ops = four_interleaved_ops(&first, &second, &pages);
        let pools = ScopedWritePools::with_config(UringPoolConfig::new(2, 0)).unwrap();
        let rendezvous = Arc::new(Rendezvous::new(2));
        let completed_groups = Arc::new(AtomicUsize::new(0));
        let observer = PanicObserver {
            calls: AtomicUsize::new(0),
        };

        let unwind = catch_unwind(AssertUnwindSafe(|| {
            pools.submit_observed(IoClass::Foreground, &ops, &observer, {
                let rendezvous = rendezvous.clone();
                let completed_groups = completed_groups.clone();
                move |group, completion, started| {
                    assert!(rendezvous.meet());
                    completed_groups.fetch_add(1, Ordering::Relaxed);
                    completion.writes_completed(&[0, 1], elapsed_ns(started));
                    group.iter().map(|_| Ok(())).collect()
                }
            })
        }));

        assert!(unwind.is_err());
        assert_eq!(completed_groups.load(Ordering::Relaxed), 2);
        assert_eq!(observer.calls.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn foreground_and_background_use_independent_worker_sets() {
        let dir = TempDir::new().unwrap();
        let pool_id = PoolId::new_v4();
        let pd = test_pd(&dir, "pd0", 0, pool_id);
        let foreground_page = [0x51; 4096];
        let background_page = [0x61; 4096];
        let foreground_ops = vec![StripWrite {
            pd: pd.clone(),
            chunklet_index: 0,
            in_chunklet_off: 0,
            data: &foreground_page,
        }];
        let background_ops = vec![StripWrite {
            pd,
            chunklet_index: 0,
            in_chunklet_off: 4096,
            data: &background_page,
        }];
        let pools = ScopedWritePools::with_config(UringPoolConfig::new(1, 1)).unwrap();
        let rendezvous = Arc::new(Rendezvous::new(2));
        let worker_names = Arc::new(Mutex::new(Vec::new()));

        std::thread::scope(|scope| {
            let foreground = scope.spawn(|| {
                pools.submit(IoClass::Foreground, &foreground_ops, {
                    let rendezvous = rendezvous.clone();
                    let worker_names = worker_names.clone();
                    move |group| {
                        worker_names.lock().push(
                            std::thread::current()
                                .name()
                                .unwrap_or("unnamed")
                                .to_string(),
                        );
                        let concurrent = rendezvous.meet();
                        group
                            .iter()
                            .map(|_| {
                                concurrent.then_some(()).ok_or_else(|| {
                                    ChunkletError::Invariant(
                                        "foreground/background pools did not overlap".into(),
                                    )
                                })
                            })
                            .collect()
                    }
                })
            });
            let background = scope.spawn(|| {
                pools.submit(IoClass::DrainMeta, &background_ops, {
                    let rendezvous = rendezvous.clone();
                    let worker_names = worker_names.clone();
                    move |group| {
                        worker_names.lock().push(
                            std::thread::current()
                                .name()
                                .unwrap_or("unnamed")
                                .to_string(),
                        );
                        let concurrent = rendezvous.meet();
                        group
                            .iter()
                            .map(|_| {
                                concurrent.then_some(()).ok_or_else(|| {
                                    ChunkletError::Invariant(
                                        "foreground/background pools did not overlap".into(),
                                    )
                                })
                            })
                            .collect()
                    }
                })
            });
            assert!(foreground.join().unwrap().iter().all(Result::is_ok));
            assert!(background.join().unwrap().iter().all(Result::is_ok));
        });

        let names = worker_names.lock();
        assert!(names.iter().any(|name| name.starts_with("ckuring-fg-")));
        assert!(names.iter().any(|name| name.starts_with("ckuring-bg-")));
        let snapshot = pools.snapshot();
        assert_eq!(snapshot.classes[0].batches, 1);
        assert_eq!(snapshot.classes[2].batches, 1);
    }

    #[test]
    fn zero_workers_preserve_single_caller_thread_submit() {
        let dir = TempDir::new().unwrap();
        let pool_id = PoolId::new_v4();
        let first = test_pd(&dir, "pd0", 0, pool_id);
        let second = test_pd(&dir, "pd1", 1, pool_id);
        let pages = [[0x11; 4096], [0x22; 4096], [0x33; 4096], [0x44; 4096]];
        let ops = four_interleaved_ops(&first, &second, &pages);
        let pools = ScopedWritePools::with_config(UringPoolConfig::new(0, 0)).unwrap();
        let calls = AtomicU64::new(0);

        let results = pools.submit(IoClass::DrainData, &ops, |submitted| {
            calls.fetch_add(1, Ordering::Relaxed);
            submitted.iter().map(|_| Ok(())).collect()
        });

        assert!(results.iter().all(Result::is_ok));
        assert_eq!(calls.load(Ordering::Relaxed), 1);
        let snapshot = pools.snapshot();
        assert!(!snapshot.enabled);
        assert_eq!(snapshot.foreground_workers, 0);
        assert_eq!(snapshot.background_workers, 0);
        assert!(snapshot.classes.iter().all(|class| class.batches == 0));
    }

    #[test]
    fn execution_snapshot_reports_cpu_set_independence() {
        assert!(cpu_sets_disjoint(&[0, 2, 4], &[1, 3, 5]));
        assert!(!cpu_sets_disjoint(&[0, 2, 4], &[4, 6]));
        assert!(!cpu_sets_disjoint(&[], &[1, 3, 5]));

        let pools = ScopedWritePools::with_config(UringPoolConfig {
            foreground_workers: 0,
            background_workers: 0,
            foreground_cpus: vec![0, 2, 4],
            background_cpus: vec![1, 3, 5],
        })
        .unwrap();
        let snapshot = pools.snapshot();
        assert_eq!(snapshot.foreground_cpus, vec![0, 2, 4]);
        assert_eq!(snapshot.background_cpus, vec![1, 3, 5]);
        assert!(snapshot.cpu_sets_disjoint);
    }
}
