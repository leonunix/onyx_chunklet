//! Persistent scoped execution for PD-homogeneous io_uring write groups.
//!
//! Rayon owns fixed foreground and background worker sets. A submit borrows
//! caller buffers only for the duration of `ThreadPool::scope`, while each
//! persistent worker reuses the `IoUring` stored in its thread-local state.

use std::any::Any;
use std::collections::BTreeMap;
use std::panic::{catch_unwind, resume_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use crossbeam_channel::unbounded;
use parking_lot::Mutex as ParkingMutex;
use rayon::{ThreadPool, ThreadPoolBuilder};

use crate::error::{ChunkletError, ChunkletResult};
use crate::io::backend::{
    IoExecutionClassSnapshot, IoExecutionSnapshot, StripWrite, UringPoolConfig,
    WriteCompletionObserver,
};
use crate::io::scheduler::{with_io_class, IoClass};

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

pub(super) struct ScopedWritePools {
    foreground: Option<ThreadPool>,
    background: Option<ThreadPool>,
    foreground_workers: usize,
    background_workers: usize,
    foreground_cpus: Vec<usize>,
    background_cpus: Vec<usize>,
    stats: ExecutionStats,
}

impl ScopedWritePools {
    pub(super) fn with_config(config: UringPoolConfig) -> std::io::Result<Self> {
        let foreground_cpus = config.foreground_cpus;
        let background_cpus = config.background_cpus;
        Ok(Self {
            foreground: build_pool("ckuring-fg", config.foreground_workers, &foreground_cpus)?,
            background: build_pool("ckuring-bg", config.background_workers, &background_cpus)?,
            foreground_workers: config.foreground_workers,
            background_workers: config.background_workers,
            foreground_cpus,
            background_cpus,
            stats: ExecutionStats::new(),
        })
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

        let groups = group_indices_by_pd(ops);
        self.stats
            .class(class)
            .record_batch(groups.len(), ops.len());
        let (sender, receiver) = unbounded::<(Vec<usize>, Vec<ChunkletResult<()>>)>();
        let observer_panic = ParkingMutex::new(None::<Box<dyn Any + Send>>);
        let scope_queued_at = Instant::now();

        pool.scope(|scope| {
            for indices in groups {
                let sender = sender.clone();
                let stats = self.stats.class(class);
                let queued_at = scope_queued_at;
                let submit_group = &submit_group;
                let observer_panic = &observer_panic;
                scope.spawn(move |_| {
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

        let groups = group_indices_by_pd(ops);
        self.stats
            .class(class)
            .record_batch(groups.len(), ops.len());
        let (sender, receiver) = unbounded::<(Vec<usize>, Vec<ChunkletResult<()>>)>();
        let observer_panic = ParkingMutex::new(None::<Box<dyn Any + Send>>);
        let scope_queued_at = Instant::now();

        pool.scope(|scope| {
            for indices in groups {
                let sender = sender.clone();
                let stats = self.stats.class(class);
                let queued_at = scope_queued_at;
                let submit_group = &submit_group;
                let observer_panic = &observer_panic;
                scope.spawn(move |_| {
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

fn group_indices_by_pd(ops: &[StripWrite<'_>]) -> Vec<Vec<usize>> {
    let mut groups = BTreeMap::new();
    for (index, op) in ops.iter().enumerate() {
        groups
            .entry(op.pd.pd_id())
            .or_insert_with(Vec::new)
            .push(index);
    }
    groups.into_values().collect()
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
    use std::sync::Arc;
    use std::time::Duration;

    use parking_lot::{Condvar, Mutex};
    use tempfile::TempDir;

    use crate::io::RawDevice;
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

    fn test_pd(dir: &TempDir, name: &str, pd_seq: u32, pool_id: PoolId) -> Arc<PhysicalDisk> {
        let raw =
            RawDevice::open_or_create(&dir.path().join(name), 4 * 1024 * 1024 * 1024).unwrap();
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
