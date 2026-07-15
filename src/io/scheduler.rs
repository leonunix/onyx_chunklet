//! Per-PD write admission using the physical 4 KiB work emitted by LDs.
//!
//! Callers keep ownership of borrowed write buffers while waiting. The
//! scheduler queues only `(PdId, blocks)` metadata, admits every PD in a batch
//! atomically under one mutex, invokes the backend synchronously, and releases credits on unwind.

use std::cell::Cell;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Arc;
use std::time::Instant;

use parking_lot::{Condvar, Mutex};

use crate::error::{ChunkletError, ChunkletResult};
use crate::io::backend::{IoBackend, StripRead, StripWrite};
use crate::pd::PhysicalDisk;
use crate::types::{PdId, BLOCK_SIZE};

/// Scheduling class carried from the logical caller to physical-disk writes.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
#[repr(u8)]
pub enum IoClass {
    Foreground = 0,
    DrainData = 1,
    DrainMeta = 2,
    Maintenance = 3,
}

impl IoClass {
    pub const ALL: [Self; 4] = [
        Self::Foreground,
        Self::DrainData,
        Self::DrainMeta,
        Self::Maintenance,
    ];

    const COUNT: usize = Self::ALL.len();

    const fn index(self) -> usize {
        self as usize
    }

    const fn is_background(self) -> bool {
        !matches!(self, Self::Foreground)
    }

    /// Class active on the current thread.
    pub fn current() -> Self {
        current_io_class()
    }
}

thread_local! {
    static CURRENT_IO_CLASS: Cell<IoClass> = const { Cell::new(IoClass::Foreground) };
}

/// Return the IO class active on this thread. New threads start as Foreground.
pub fn current_io_class() -> IoClass {
    CURRENT_IO_CLASS.with(Cell::get)
}

/// Run `f` with `class` installed, restoring the previous class on unwind.
pub fn with_io_class<R>(class: IoClass, f: impl FnOnce() -> R) -> R {
    struct Restore(IoClass);

    impl Drop for Restore {
        fn drop(&mut self) {
            CURRENT_IO_CLASS.with(|current| current.set(self.0));
        }
    }

    let previous = CURRENT_IO_CLASS.with(|current| current.replace(class));
    let _restore = Restore(previous);
    f()
}

/// Per-PD block limits. Reservations are protected only while their owner has
/// queued demand, so otherwise-idle credits remain work-conserving.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SchedulerConfig {
    pub max_active_blocks_per_pd: u64,
    pub foreground_min_blocks: u64,
    pub drain_data_min_blocks: u64,
    pub drain_meta_min_blocks: u64,
    pub maintenance_min_blocks: u64,
}

impl SchedulerConfig {
    pub fn new(max_active_blocks_per_pd: u64) -> Self {
        Self {
            max_active_blocks_per_pd,
            foreground_min_blocks: 0,
            drain_data_min_blocks: 0,
            drain_meta_min_blocks: 0,
            maintenance_min_blocks: 0,
        }
    }

    pub fn with_min_active_blocks(mut self, class: IoClass, blocks: u64) -> Self {
        match class {
            IoClass::Foreground => self.foreground_min_blocks = blocks,
            IoClass::DrainData => self.drain_data_min_blocks = blocks,
            IoClass::DrainMeta => self.drain_meta_min_blocks = blocks,
            IoClass::Maintenance => self.maintenance_min_blocks = blocks,
        }
        self
    }

    pub fn min_active_blocks(&self, class: IoClass) -> u64 {
        match class {
            IoClass::Foreground => self.foreground_min_blocks,
            IoClass::DrainData => self.drain_data_min_blocks,
            IoClass::DrainMeta => self.drain_meta_min_blocks,
            IoClass::Maintenance => self.maintenance_min_blocks,
        }
    }

    fn wave_cap(&self, class: IoClass) -> u64 {
        let reserved_for_others = IoClass::ALL
            .iter()
            .filter(|&&other| other != class)
            .fold(0_u64, |total, &other| {
                total.saturating_add(self.min_active_blocks(other))
            });
        self.max_active_blocks_per_pd
            .saturating_sub(reserved_for_others)
            .max(1)
            .min(self.max_active_blocks_per_pd)
    }

    fn validate(&self) -> Result<(), String> {
        if self.max_active_blocks_per_pd == 0 {
            return Err("scheduler max_active_blocks_per_pd must be non-zero".into());
        }
        let reserved = IoClass::ALL.iter().try_fold(0_u64, |total, &class| {
            total.checked_add(self.min_active_blocks(class))
        });
        match reserved {
            Some(total) if total <= self.max_active_blocks_per_pd => Ok(()),
            Some(total) => Err(format!(
                "scheduler class reservations ({total}) exceed per-PD maximum ({})",
                self.max_active_blocks_per_pd
            )),
            None => Err("scheduler class reservations overflow u64".into()),
        }
    }
}

/// Immutable metrics for one class on one PD.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IoClassSnapshot {
    pub class: IoClass,
    pub configured_min_blocks: u64,
    pub queued_blocks: u64,
    pub queued_blocks_max: u64,
    pub active_blocks: u64,
    pub active_blocks_max: u64,
    pub wait_events: u64,
    pub wait_ns: u64,
    pub wait_max_ns: u64,
    pub admission_events: u64,
    pub admitted_blocks: u64,
    pub borrow_events: u64,
    pub borrowed_blocks: u64,
    pub borrowed_blocks_max: u64,
    pub borrowed_blocks_total: u64,
    pub reclaim_events: u64,
    pub reclaimed_blocks: u64,
    pub completed_blocks: u64,
    pub error_blocks: u64,
    pub service_ns: u64,
    pub service_max_ns: u64,
}

/// Immutable scheduler state for one physical disk.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PdSchedulerSnapshot {
    pub pd_id: PdId,
    pub max_active_blocks: u64,
    pub total_queued_blocks: u64,
    pub total_queued_blocks_max: u64,
    pub total_active_blocks: u64,
    pub total_active_blocks_max: u64,
    pub flush_waiters: u64,
    pub flush_fenced: bool,
    pub classes: Vec<IoClassSnapshot>,
}

/// Point-in-time scheduler metrics, sorted by `PdId` and then `IoClass`.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SchedulerSnapshot {
    pub pds: Vec<PdSchedulerSnapshot>,
}

#[derive(Clone, Debug)]
struct Demand {
    pd_id: PdId,
    blocks: u64,
    requested_blocks: u64,
    exclusive: bool,
}

#[derive(Debug)]
struct Waiter {
    ticket: u64,
    class: IoClass,
    demands: Vec<Demand>,
}

#[derive(Debug)]
struct FlushWaiter {
    ticket: u64,
    pd_ids: Vec<PdId>,
}

#[derive(Debug, Default)]
struct ClassState {
    queued_blocks: u64,
    queued_blocks_max: u64,
    active_blocks: u64,
    active_blocks_max: u64,
    wait_events: u64,
    wait_ns: u64,
    wait_max_ns: u64,
    admission_events: u64,
    admitted_blocks: u64,
    borrow_events: u64,
    borrowed_blocks: u64,
    borrowed_blocks_max: u64,
    borrowed_blocks_total: u64,
    reclaim_events: u64,
    reclaimed_blocks: u64,
    completed_blocks: u64,
    error_blocks: u64,
    service_ns: u64,
    service_max_ns: u64,
}

#[derive(Debug)]
struct PdState {
    classes: [ClassState; IoClass::COUNT],
    total_queued_blocks_max: u64,
    total_active_blocks_max: u64,
    flush_waiters: u64,
    flush_fenced: bool,
}

impl Default for PdState {
    fn default() -> Self {
        Self {
            classes: std::array::from_fn(|_| ClassState::default()),
            total_queued_blocks_max: 0,
            total_active_blocks_max: 0,
            flush_waiters: 0,
            flush_fenced: false,
        }
    }
}

#[derive(Debug, Default)]
struct State {
    pds: BTreeMap<PdId, PdState>,
    waiters: VecDeque<Waiter>,
    flush_waiters: VecDeque<FlushWaiter>,
    next_ticket: u64,
}

#[derive(Clone, Debug)]
struct Allocation {
    pd_id: PdId,
    class: IoClass,
    blocks: u64,
    borrowed_blocks: u64,
}

#[derive(Debug)]
struct PlannedWave {
    indices: Vec<usize>,
    demands: Vec<Demand>,
}

struct AdmissionController {
    config: SchedulerConfig,
    state: Mutex<State>,
    changed: Condvar,
}

impl AdmissionController {
    fn new(config: SchedulerConfig) -> Result<Arc<Self>, String> {
        config.validate()?;
        Ok(Arc::new(Self {
            config,
            state: Mutex::new(State::default()),
            changed: Condvar::new(),
        }))
    }

    fn admit(
        self: &Arc<Self>,
        class: IoClass,
        demands: Vec<Demand>,
    ) -> Result<AdmissionPermit, String> {
        if demands.is_empty() {
            return Ok(AdmissionPermit {
                controller: self.clone(),
                allocations: Vec::new(),
            });
        }

        let enqueued_at = Instant::now();
        let mut state = self.state.lock();
        let ticket = state.next_ticket;
        state.next_ticket = state.next_ticket.wrapping_add(1);
        for demand in &demands {
            let queued = state
                .pds
                .get(&demand.pd_id)
                .map_or(0, |pd| pd.classes[class.index()].queued_blocks);
            queued
                .checked_add(demand.blocks)
                .ok_or_else(|| "scheduler queued block accounting overflow".to_string())?;
        }
        for demand in &demands {
            let pd = state.pds.entry(demand.pd_id).or_default();
            {
                let class_state = &mut pd.classes[class.index()];
                class_state.queued_blocks += demand.blocks;
                class_state.queued_blocks_max =
                    class_state.queued_blocks_max.max(class_state.queued_blocks);
            }
            pd.total_queued_blocks_max = pd.total_queued_blocks_max.max(total_queued_blocks(pd));
        }
        state.waiters.push_back(Waiter {
            ticket,
            class,
            demands,
        });
        self.changed.notify_all();

        let mut recorded_wait = false;
        loop {
            if self.next_admissible_ticket(&state) == Some(ticket) {
                let position = state
                    .waiters
                    .iter()
                    .position(|waiter| waiter.ticket == ticket)
                    .expect("admission ticket disappeared");
                let waiter = state
                    .waiters
                    .remove(position)
                    .expect("admission ticket disappeared");
                let wait_ns = if recorded_wait {
                    elapsed_ns(enqueued_at)
                } else {
                    0
                };
                let allocations = self.activate(&mut state, waiter, wait_ns);
                drop(state);
                self.changed.notify_all();
                return Ok(AdmissionPermit {
                    controller: self.clone(),
                    allocations,
                });
            }

            if !recorded_wait {
                let waiter = state
                    .waiters
                    .iter()
                    .find(|waiter| waiter.ticket == ticket)
                    .expect("admission ticket disappeared");
                let class_index = waiter.class.index();
                let touched: Vec<_> = waiter.demands.iter().map(|demand| demand.pd_id).collect();
                for pd_id in touched {
                    let class_state = &mut state
                        .pds
                        .get_mut(&pd_id)
                        .expect("queued PD missing")
                        .classes[class_index];
                    class_state.wait_events = class_state.wait_events.saturating_add(1);
                }
                recorded_wait = true;
            }
            self.changed.notify_all();
            self.changed.wait(&mut state);
        }
    }

    fn next_admissible_ticket(&self, state: &State) -> Option<u64> {
        let mut first = None;
        for waiter in &state.waiters {
            if !self.can_admit(state, waiter) {
                continue;
            }
            if first.is_none() {
                first = Some(waiter.ticket);
            }
            if self.has_background_deficit(state, waiter) {
                return Some(waiter.ticket);
            }
        }
        first
    }

    fn has_background_deficit(&self, state: &State, waiter: &Waiter) -> bool {
        waiter.class.is_background()
            && waiter.demands.iter().any(|demand| {
                state.pds[&demand.pd_id].classes[waiter.class.index()].active_blocks
                    < self.config.min_active_blocks(waiter.class)
            })
    }

    fn can_admit(&self, state: &State, waiter: &Waiter) -> bool {
        if self.must_yield_to_older_same_class(state, waiter)
            || self.must_yield_to_background_deficit(state, waiter)
        {
            return false;
        }
        waiter.demands.iter().all(|demand| {
            let pd = &state.pds[&demand.pd_id];
            if pd.flush_fenced
                || state.flush_waiters.iter().any(|flush| {
                    flush.ticket < waiter.ticket && flush.pd_ids.contains(&demand.pd_id)
                })
            {
                return false;
            }
            let active_total = pd.classes.iter().fold(0_u64, |total, class| {
                total.saturating_add(class.active_blocks)
            });
            if demand.exclusive {
                return active_total == 0;
            }
            if demand.blocks
                > self
                    .config
                    .max_active_blocks_per_pd
                    .saturating_sub(active_total)
            {
                return false;
            }

            let protected_other = IoClass::ALL
                .iter()
                .filter(|&&other| other != waiter.class)
                .fold(0_u64, |total, &other| {
                    let other_state = &pd.classes[other.index()];
                    if other_state.queued_blocks == 0 {
                        return total;
                    }
                    let deficit = self
                        .config
                        .min_active_blocks(other)
                        .saturating_sub(other_state.active_blocks)
                        .min(other_state.queued_blocks);
                    total.saturating_add(deficit)
                });
            demand.blocks
                <= self
                    .config
                    .max_active_blocks_per_pd
                    .saturating_sub(active_total)
                    .saturating_sub(protected_other)
        })
    }

    fn must_yield_to_older_same_class(&self, state: &State, waiter: &Waiter) -> bool {
        state.waiters.iter().any(|older| {
            older.ticket < waiter.ticket
                && older.class == waiter.class
                && demands_overlap(&older.demands, &waiter.demands)
        })
    }

    fn must_yield_to_background_deficit(&self, state: &State, waiter: &Waiter) -> bool {
        state.waiters.iter().any(|background| {
            background.ticket != waiter.ticket
                && self.has_background_deficit(state, background)
                && (waiter.class == IoClass::Foreground || background.ticket < waiter.ticket)
                && demands_overlap(&background.demands, &waiter.demands)
        })
    }

    fn activate(&self, state: &mut State, waiter: Waiter, wait_ns: u64) -> Vec<Allocation> {
        let mut allocations = Vec::with_capacity(waiter.demands.len());
        for demand in waiter.demands {
            let pd = state.pds.get_mut(&demand.pd_id).expect("queued PD missing");
            let active_total = pd.classes.iter().fold(0_u64, |total, class| {
                total.saturating_add(class.active_blocks)
            });
            let other_reservations = IoClass::ALL
                .iter()
                .filter(|&&other| other != waiter.class)
                .fold(0_u64, |total, &other| {
                    total.saturating_add(
                        self.config
                            .min_active_blocks(other)
                            .saturating_sub(pd.classes[other.index()].active_blocks),
                    )
                });
            let non_borrowed_available = self
                .config
                .max_active_blocks_per_pd
                .saturating_sub(active_total)
                .saturating_sub(other_reservations);
            let borrowed_blocks = demand.blocks.saturating_sub(non_borrowed_available);

            let class_state = &mut pd.classes[waiter.class.index()];
            class_state.queued_blocks -= demand.blocks;
            class_state.active_blocks += demand.blocks;
            class_state.active_blocks_max =
                class_state.active_blocks_max.max(class_state.active_blocks);
            class_state.wait_ns = class_state.wait_ns.saturating_add(wait_ns);
            class_state.wait_max_ns = class_state.wait_max_ns.max(wait_ns);
            class_state.admission_events = class_state.admission_events.saturating_add(1);
            class_state.admitted_blocks = class_state
                .admitted_blocks
                .saturating_add(demand.requested_blocks);
            if borrowed_blocks > 0 {
                class_state.borrow_events = class_state.borrow_events.saturating_add(1);
                class_state.borrowed_blocks += borrowed_blocks;
                class_state.borrowed_blocks_max = class_state
                    .borrowed_blocks_max
                    .max(class_state.borrowed_blocks);
                class_state.borrowed_blocks_total = class_state
                    .borrowed_blocks_total
                    .saturating_add(borrowed_blocks);
            }
            allocations.push(Allocation {
                pd_id: demand.pd_id,
                class: waiter.class,
                blocks: demand.blocks,
                borrowed_blocks,
            });
            pd.total_active_blocks_max = pd.total_active_blocks_max.max(total_active_blocks(pd));
        }
        allocations
    }

    fn release(&self, allocations: &[Allocation]) {
        if allocations.is_empty() {
            return;
        }
        let mut state = self.state.lock();
        for allocation in allocations {
            let class_state = &mut state
                .pds
                .get_mut(&allocation.pd_id)
                .expect("active PD missing")
                .classes[allocation.class.index()];
            debug_assert!(class_state.active_blocks >= allocation.blocks);
            debug_assert!(class_state.borrowed_blocks >= allocation.borrowed_blocks);
            class_state.active_blocks -= allocation.blocks;
            class_state.borrowed_blocks -= allocation.borrowed_blocks;
            class_state.reclaim_events = class_state.reclaim_events.saturating_add(1);
            class_state.reclaimed_blocks = class_state
                .reclaimed_blocks
                .saturating_add(allocation.blocks);
        }
        drop(state);
        self.changed.notify_all();
    }

    fn fence(self: &Arc<Self>, pd_ids: Vec<PdId>) -> FlushPermit {
        let pd_ids: Vec<_> = pd_ids
            .into_iter()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        if pd_ids.is_empty() {
            return FlushPermit {
                controller: self.clone(),
                pd_ids,
            };
        }

        let mut state = self.state.lock();
        let ticket = state.next_ticket;
        state.next_ticket = state.next_ticket.wrapping_add(1);
        for &pd_id in &pd_ids {
            let pd = state.pds.entry(pd_id).or_default();
            pd.flush_waiters = pd.flush_waiters.saturating_add(1);
        }
        state.flush_waiters.push_back(FlushWaiter {
            ticket,
            pd_ids: pd_ids.clone(),
        });
        self.changed.notify_all();

        loop {
            let waiter = state
                .flush_waiters
                .iter()
                .find(|waiter| waiter.ticket == ticket)
                .expect("flush ticket disappeared");
            if self.can_activate_fence(&state, waiter) {
                let position = state
                    .flush_waiters
                    .iter()
                    .position(|waiter| waiter.ticket == ticket)
                    .expect("flush ticket disappeared");
                let waiter = state
                    .flush_waiters
                    .remove(position)
                    .expect("flush ticket disappeared");
                for &pd_id in &waiter.pd_ids {
                    let pd = state.pds.get_mut(&pd_id).expect("flush PD missing");
                    pd.flush_waiters -= 1;
                    debug_assert!(!pd.flush_fenced);
                    pd.flush_fenced = true;
                }
                drop(state);
                self.changed.notify_all();
                return FlushPermit {
                    controller: self.clone(),
                    pd_ids: waiter.pd_ids,
                };
            }
            self.changed.notify_all();
            self.changed.wait(&mut state);
        }
    }

    fn can_activate_fence(&self, state: &State, flush: &FlushWaiter) -> bool {
        if flush.pd_ids.iter().any(|pd_id| {
            let pd = &state.pds[pd_id];
            pd.flush_fenced || pd.classes.iter().any(|class| class.active_blocks > 0)
        }) {
            return false;
        }
        if state.waiters.iter().any(|waiter| {
            waiter.ticket < flush.ticket
                && waiter
                    .demands
                    .iter()
                    .any(|demand| flush.pd_ids.contains(&demand.pd_id))
        }) {
            return false;
        }
        !state.flush_waiters.iter().any(|older| {
            older.ticket < flush.ticket
                && older
                    .pd_ids
                    .iter()
                    .any(|pd_id| flush.pd_ids.contains(pd_id))
        })
    }

    fn release_fence(&self, pd_ids: &[PdId]) {
        if pd_ids.is_empty() {
            return;
        }
        let mut state = self.state.lock();
        for pd_id in pd_ids {
            let pd = state.pds.get_mut(pd_id).expect("fenced PD missing");
            debug_assert!(pd.flush_fenced);
            pd.flush_fenced = false;
        }
        drop(state);
        self.changed.notify_all();
    }

    fn snapshot(&self) -> SchedulerSnapshot {
        let state = self.state.lock();
        SchedulerSnapshot {
            pds: state
                .pds
                .iter()
                .map(|(&pd_id, pd)| PdSchedulerSnapshot {
                    pd_id,
                    max_active_blocks: self.config.max_active_blocks_per_pd,
                    total_queued_blocks: total_queued_blocks(pd),
                    total_queued_blocks_max: pd.total_queued_blocks_max,
                    total_active_blocks: total_active_blocks(pd),
                    total_active_blocks_max: pd.total_active_blocks_max,
                    flush_waiters: pd.flush_waiters,
                    flush_fenced: pd.flush_fenced,
                    classes: IoClass::ALL
                        .iter()
                        .map(|&class| {
                            let current = &pd.classes[class.index()];
                            IoClassSnapshot {
                                class,
                                configured_min_blocks: self.config.min_active_blocks(class),
                                queued_blocks: current.queued_blocks,
                                queued_blocks_max: current.queued_blocks_max,
                                active_blocks: current.active_blocks,
                                active_blocks_max: current.active_blocks_max,
                                wait_events: current.wait_events,
                                wait_ns: current.wait_ns,
                                wait_max_ns: current.wait_max_ns,
                                admission_events: current.admission_events,
                                admitted_blocks: current.admitted_blocks,
                                borrow_events: current.borrow_events,
                                borrowed_blocks: current.borrowed_blocks,
                                borrowed_blocks_max: current.borrowed_blocks_max,
                                borrowed_blocks_total: current.borrowed_blocks_total,
                                reclaim_events: current.reclaim_events,
                                reclaimed_blocks: current.reclaimed_blocks,
                                completed_blocks: current.completed_blocks,
                                error_blocks: current.error_blocks,
                                service_ns: current.service_ns,
                                service_max_ns: current.service_max_ns,
                            }
                        })
                        .collect(),
                })
                .collect(),
        }
    }

    fn register_pd(&self, pd_id: PdId) {
        self.state.lock().pds.entry(pd_id).or_default();
    }

    fn record_completion(
        &self,
        class: IoClass,
        completions: &[(PdId, u64, bool)],
        service_ns: u64,
    ) {
        let mut state = self.state.lock();
        let mut serviced_pds = BTreeSet::new();
        for &(pd_id, blocks, failed) in completions {
            let current = &mut state.pds.entry(pd_id).or_default().classes[class.index()];
            if failed {
                current.error_blocks = current.error_blocks.saturating_add(blocks);
            } else {
                current.completed_blocks = current.completed_blocks.saturating_add(blocks);
            }
            serviced_pds.insert(pd_id);
        }
        for pd_id in serviced_pds {
            let current = &mut state
                .pds
                .get_mut(&pd_id)
                .expect("serviced PD missing")
                .classes[class.index()];
            current.service_ns = current.service_ns.saturating_add(service_ns);
            current.service_max_ns = current.service_max_ns.max(service_ns);
        }
    }
}

struct AdmissionPermit {
    controller: Arc<AdmissionController>,
    allocations: Vec<Allocation>,
}

impl Drop for AdmissionPermit {
    fn drop(&mut self) {
        self.controller.release(&self.allocations);
    }
}

struct FlushPermit {
    controller: Arc<AdmissionController>,
    pd_ids: Vec<PdId>,
}

impl Drop for FlushPermit {
    fn drop(&mut self) {
        self.controller.release_fence(&self.pd_ids);
    }
}

fn total_queued_blocks(pd: &PdState) -> u64 {
    pd.classes.iter().fold(0_u64, |total, class| {
        total.saturating_add(class.queued_blocks)
    })
}

fn total_active_blocks(pd: &PdState) -> u64 {
    pd.classes.iter().fold(0_u64, |total, class| {
        total.saturating_add(class.active_blocks)
    })
}

fn demands_overlap(left: &[Demand], right: &[Demand]) -> bool {
    left.iter()
        .any(|left| right.iter().any(|right| left.pd_id == right.pd_id))
}

/// Work-conserving per-PD admission around an existing synchronous backend.
pub struct ScheduledBackend {
    inner: Arc<dyn IoBackend>,
    admission: Arc<AdmissionController>,
}

impl ScheduledBackend {
    pub fn new(inner: Arc<dyn IoBackend>, config: SchedulerConfig) -> ChunkletResult<Self> {
        Ok(Self {
            inner,
            admission: AdmissionController::new(config).map_err(ChunkletError::Config)?,
        })
    }

    pub fn snapshot(&self) -> SchedulerSnapshot {
        self.admission.snapshot()
    }

    pub fn inner_name(&self) -> &'static str {
        self.inner.name()
    }

    fn submit_scheduled(&self, class: IoClass, ops: &[StripWrite<'_>]) -> Vec<ChunkletResult<()>> {
        let max_blocks = self.admission.config.max_active_blocks_per_pd;
        let waves = match plan_ops(ops, self.admission.config.wave_cap(class), max_blocks) {
            Ok(waves) => waves,
            Err(message) => return admission_errors(ops.len(), message),
        };
        let mut output: Vec<Option<ChunkletResult<()>>> =
            std::iter::repeat_with(|| None).take(ops.len()).collect();
        for wave in waves {
            let wave_ops: Vec<_> = wave
                .indices
                .iter()
                .map(|&index| ops[index].clone())
                .collect();
            let permit = match self.admission.admit(class, wave.demands) {
                Ok(permit) => permit,
                Err(message) => {
                    for index in wave.indices {
                        output[index] = Some(Err(ChunkletError::Config(message.clone())));
                    }
                    continue;
                }
            };
            let service_started = Instant::now();
            let mut results = self
                .inner
                .submit_writes_detailed_with_class(class, &wave_ops)
                .into_iter();
            let service_ns = elapsed_ns(service_started);
            let mut completions = Vec::with_capacity(wave.indices.len());
            for (&index, op) in wave.indices.iter().zip(&wave_ops) {
                let result = results.next().unwrap_or_else(|| {
                    Err(ChunkletError::Invariant(
                        "wrapped IO backend returned too few write results".into(),
                    ))
                });
                completions.push((
                    op.pd.pd_id(),
                    blocks_for_len(op.data.len()).expect("planned write length became invalid"),
                    result.is_err(),
                ));
                output[index] = Some(result);
            }
            self.admission
                .record_completion(class, &completions, service_ns);
            drop(permit);
        }
        output
            .into_iter()
            .map(|result| {
                result.unwrap_or_else(|| {
                    Err(ChunkletError::Invariant(
                        "scheduler omitted a write result".into(),
                    ))
                })
            })
            .collect()
    }
}

impl IoBackend for ScheduledBackend {
    fn register_pd(&self, pd_id: PdId) {
        self.admission.register_pd(pd_id);
    }

    fn scheduler_snapshot(&self) -> Option<SchedulerSnapshot> {
        Some(self.snapshot())
    }

    fn submit_reads(&self, ops: &mut [StripRead<'_>]) -> ChunkletResult<()> {
        self.inner.submit_reads(ops)
    }

    fn submit_writes_detailed(&self, ops: &[StripWrite<'_>]) -> Vec<ChunkletResult<()>> {
        self.submit_scheduled(current_io_class(), ops)
    }

    fn submit_writes_detailed_with_class(
        &self,
        class: IoClass,
        ops: &[StripWrite<'_>],
    ) -> Vec<ChunkletResult<()>> {
        self.submit_scheduled(class, ops)
    }

    fn submit_flushes(&self, pds: &[Arc<PhysicalDisk>]) -> ChunkletResult<()> {
        let sync_pd_ids: Vec<_> = pds
            .iter()
            .filter(|pd| pd.sync_required())
            .map(|pd| pd.pd_id())
            .collect();
        if sync_pd_ids.is_empty() {
            return self.inner.submit_flushes(pds);
        }
        let _fence = self.admission.fence(sync_pd_ids);
        self.inner.submit_flushes(pds)
    }

    fn name(&self) -> &'static str {
        "scheduled"
    }
}

fn blocks_for_len(len: usize) -> Result<u64, String> {
    let len = u64::try_from(len).map_err(|_| "write length does not fit u64".to_string())?;
    Ok(len / BLOCK_SIZE + u64::from(len % BLOCK_SIZE != 0))
}

fn plan_ops(
    ops: &[StripWrite<'_>],
    wave_cap: u64,
    max_blocks: u64,
) -> Result<Vec<PlannedWave>, String> {
    let mut work = Vec::with_capacity(ops.len());
    for op in ops {
        work.push((op.pd.pd_id(), blocks_for_len(op.data.len())?));
    }
    plan_work(&work, wave_cap, max_blocks)
}

fn plan_work(
    work: &[(PdId, u64)],
    wave_cap: u64,
    max_blocks: u64,
) -> Result<Vec<PlannedWave>, String> {
    if wave_cap == 0 || max_blocks == 0 || wave_cap > max_blocks {
        return Err("scheduler wave cap must be within 1..=max blocks".into());
    }
    let mut waves = Vec::new();
    let mut indices = Vec::new();
    let mut by_pd = BTreeMap::<PdId, u64>::new();

    for (index, &(pd_id, blocks)) in work.iter().enumerate() {
        if blocks > wave_cap {
            seal_wave(&mut waves, &mut indices, &mut by_pd);
            waves.push(PlannedWave {
                indices: vec![index],
                demands: vec![Demand {
                    pd_id,
                    blocks: blocks.min(max_blocks),
                    requested_blocks: blocks,
                    exclusive: true,
                }],
            });
            continue;
        }

        let current = by_pd.get(&pd_id).copied().unwrap_or(0);
        if !indices.is_empty() && blocks > wave_cap.saturating_sub(current) {
            seal_wave(&mut waves, &mut indices, &mut by_pd);
        }
        indices.push(index);
        *by_pd.entry(pd_id).or_default() += blocks;
    }
    seal_wave(&mut waves, &mut indices, &mut by_pd);
    Ok(waves)
}

fn seal_wave(
    waves: &mut Vec<PlannedWave>,
    indices: &mut Vec<usize>,
    by_pd: &mut BTreeMap<PdId, u64>,
) {
    if indices.is_empty() {
        return;
    }
    waves.push(PlannedWave {
        indices: std::mem::take(indices),
        demands: std::mem::take(by_pd)
            .into_iter()
            .filter_map(|(pd_id, blocks)| {
                (blocks > 0).then_some(Demand {
                    pd_id,
                    blocks,
                    requested_blocks: blocks,
                    exclusive: false,
                })
            })
            .collect(),
    });
}

fn admission_errors(len: usize, message: String) -> Vec<ChunkletResult<()>> {
    (0..len)
        .map(|_| Err(ChunkletError::Config(message.clone())))
        .collect()
}

fn elapsed_ns(start: Instant) -> u64 {
    u64::try_from(start.elapsed().as_nanos()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests;
