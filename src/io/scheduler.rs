//! Per-PD write admission using the physical 4 KiB work emitted by LDs.
//!
//! Callers keep ownership of borrowed write buffers while waiting. The
//! scheduler queues only `(PdId, blocks)` metadata, admits each PD independently,
//! invokes the backend synchronously, and releases credits on completion or unwind.

use std::cell::Cell;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Arc;
use std::time::Instant;

use parking_lot::{Condvar, Mutex};

use crate::error::{ChunkletError, ChunkletResult};
use crate::io::backend::{IoBackend, StripRead, StripWrite};
use crate::pd::PhysicalDisk;
use crate::types::{PdId, BLOCK_SIZE};

mod completion;

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
struct GrantUnit {
    blocks: u64,
    requested_blocks: u64,
    exclusive: bool,
}

#[derive(Clone, Debug)]
struct Demand {
    pd_id: PdId,
    blocks: u64,
    requested_blocks: u64,
    grant_units: VecDeque<GrantUnit>,
}

#[derive(Debug)]
struct Waiter {
    ticket: u64,
    class: IoClass,
    demand: Demand,
    recorded_wait: bool,
    wait_started_at: Option<Instant>,
    accumulated_wait_ns: u64,
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
struct PlannedPdWave {
    pd_id: PdId,
    indices: Vec<usize>,
    demand: Demand,
}

struct PendingAdmission {
    controller: Arc<AdmissionController>,
    class: IoClass,
    tickets: BTreeMap<PdId, u64>,
    grant_cursor: Option<PdId>,
}

struct AdmittedSubset {
    prefixes: Vec<AdmittedPrefix>,
    permit: AdmissionPermit,
}

struct AdmittedPrefix {
    pd_id: PdId,
    units: usize,
    blocks: u64,
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

    fn queue(
        self: &Arc<Self>,
        class: IoClass,
        demands: Vec<Demand>,
    ) -> Result<PendingAdmission, String> {
        let mut pending = PendingAdmission {
            controller: self.clone(),
            class,
            tickets: BTreeMap::new(),
            grant_cursor: None,
        };
        pending.enqueue(demands)?;
        Ok(pending)
    }

    #[cfg(test)]
    fn admit(
        self: &Arc<Self>,
        class: IoClass,
        demands: Vec<Demand>,
    ) -> Result<AdmissionPermit, String> {
        if demands.len() > 1 {
            return Err("test admission helper accepts at most one PD demand".into());
        }
        let mut pending = self.queue(class, demands)?;
        if pending.is_empty() {
            return Ok(AdmissionPermit {
                controller: self.clone(),
                allocations: Mutex::new(Vec::new()),
            });
        }
        Ok(pending.admit_ready().permit)
    }

    fn enqueue_demands(
        &self,
        class: IoClass,
        demands: Vec<Demand>,
    ) -> Result<Vec<(PdId, u64)>, String> {
        let mut seen = BTreeSet::new();
        if demands.iter().any(|demand| !seen.insert(demand.pd_id)) {
            return Err("scheduler admission batch contains duplicate PD demand".into());
        }
        for demand in &demands {
            let charged = demand
                .grant_units
                .iter()
                .try_fold(0_u64, |total, unit| total.checked_add(unit.blocks));
            let requested = demand.grant_units.iter().try_fold(0_u64, |total, unit| {
                total.checked_add(unit.requested_blocks)
            });
            if demand.grant_units.is_empty()
                || charged != Some(demand.blocks)
                || requested != Some(demand.requested_blocks)
            {
                return Err("scheduler demand has invalid grant units".into());
            }
        }

        let mut state = self.state.lock();
        for demand in &demands {
            let queued = state
                .pds
                .get(&demand.pd_id)
                .map_or(0, |pd| pd.classes[class.index()].queued_blocks);
            queued
                .checked_add(demand.blocks)
                .ok_or_else(|| "scheduler queued block accounting overflow".to_string())?;
        }

        let mut tickets = Vec::with_capacity(demands.len());
        for demand in demands {
            let ticket = state.next_ticket;
            state.next_ticket = state.next_ticket.wrapping_add(1);
            let pd = state.pds.entry(demand.pd_id).or_default();
            {
                let class_state = &mut pd.classes[class.index()];
                class_state.queued_blocks += demand.blocks;
                class_state.queued_blocks_max =
                    class_state.queued_blocks_max.max(class_state.queued_blocks);
            }
            pd.total_queued_blocks_max = pd.total_queued_blocks_max.max(total_queued_blocks(pd));
            tickets.push((demand.pd_id, ticket));
            state.waiters.push_back(Waiter {
                ticket,
                class,
                demand,
                recorded_wait: false,
                wait_started_at: None,
                accumulated_wait_ns: 0,
            });
        }
        drop(state);
        self.changed.notify_all();
        Ok(tickets)
    }

    fn next_admissible_ticket_for_pd(&self, state: &State, pd_id: PdId) -> Option<u64> {
        let mut first = None;
        for waiter in state
            .waiters
            .iter()
            .filter(|waiter| waiter.demand.pd_id == pd_id)
        {
            let next_blocks = waiter
                .demand
                .grant_units
                .front()
                .expect("queued demand has no grant unit")
                .blocks;
            if self
                .admissible_budget(state, waiter)
                .is_none_or(|budget| next_blocks > budget)
            {
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
            && state.pds[&waiter.demand.pd_id].classes[waiter.class.index()].active_blocks
                < self.config.min_active_blocks(waiter.class)
    }

    fn admissible_budget(&self, state: &State, waiter: &Waiter) -> Option<u64> {
        if self.must_yield_to_older_same_class(state, waiter)
            || self.must_yield_to_background_deficit(state, waiter)
        {
            return None;
        }
        let demand = &waiter.demand;
        let next = demand
            .grant_units
            .front()
            .expect("queued demand has no grant unit");
        let pd = &state.pds[&demand.pd_id];
        if pd.flush_fenced
            || state
                .flush_waiters
                .iter()
                .any(|flush| flush.ticket < waiter.ticket && flush.pd_ids.contains(&demand.pd_id))
        {
            return None;
        }
        let active_total = pd.classes.iter().fold(0_u64, |total, class| {
            total.saturating_add(class.active_blocks)
        });
        if next.exclusive {
            return (active_total == 0).then_some(next.blocks);
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
        Some(
            self.config
                .max_active_blocks_per_pd
                .saturating_sub(active_total)
                .saturating_sub(protected_other),
        )
    }

    fn must_yield_to_older_same_class(&self, state: &State, waiter: &Waiter) -> bool {
        state.waiters.iter().any(|older| {
            older.ticket < waiter.ticket
                && older.class == waiter.class
                && older.demand.pd_id == waiter.demand.pd_id
        })
    }

    fn must_yield_to_background_deficit(&self, state: &State, waiter: &Waiter) -> bool {
        state.waiters.iter().any(|background| {
            background.ticket != waiter.ticket
                && self.has_background_deficit(state, background)
                && (waiter.class == IoClass::Foreground || background.ticket < waiter.ticket)
                && background.demand.pd_id == waiter.demand.pd_id
        })
    }

    fn activate_one(
        &self,
        state: &mut State,
        waiter: &mut Waiter,
        admission_event: bool,
    ) -> Allocation {
        close_wait_interval(waiter);
        let unit = waiter
            .demand
            .grant_units
            .pop_front()
            .expect("queued demand has no grant unit");
        let pd_id = waiter.demand.pd_id;
        let class = waiter.class;
        waiter.demand.blocks = waiter
            .demand
            .blocks
            .checked_sub(unit.blocks)
            .expect("grant exceeds queued demand blocks");
        waiter.demand.requested_blocks = waiter
            .demand
            .requested_blocks
            .checked_sub(unit.requested_blocks)
            .expect("grant exceeds queued requested blocks");
        let finished_ticket = waiter.demand.grant_units.is_empty();

        let pd = state.pds.get_mut(&pd_id).expect("queued PD missing");
        let active_total = pd.classes.iter().fold(0_u64, |total, class| {
            total.saturating_add(class.active_blocks)
        });
        let other_reservations =
            IoClass::ALL
                .iter()
                .filter(|&&other| other != class)
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
        let borrowed_blocks = unit.blocks.saturating_sub(non_borrowed_available);
        let wait_ns = if finished_ticket {
            std::mem::take(&mut waiter.accumulated_wait_ns)
        } else {
            0
        };

        let class_state = &mut pd.classes[class.index()];
        class_state.queued_blocks -= unit.blocks;
        class_state.active_blocks += unit.blocks;
        class_state.active_blocks_max =
            class_state.active_blocks_max.max(class_state.active_blocks);
        class_state.wait_ns = class_state.wait_ns.saturating_add(wait_ns);
        class_state.wait_max_ns = class_state.wait_max_ns.max(wait_ns);
        if admission_event {
            class_state.admission_events = class_state.admission_events.saturating_add(1);
        }
        class_state.admitted_blocks = class_state
            .admitted_blocks
            .saturating_add(unit.requested_blocks);
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
        pd.total_active_blocks_max = pd.total_active_blocks_max.max(total_active_blocks(pd));
        Allocation {
            pd_id,
            class,
            blocks: unit.blocks,
            borrowed_blocks,
        }
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
            waiter.ticket < flush.ticket && flush.pd_ids.contains(&waiter.demand.pd_id)
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

impl PendingAdmission {
    fn is_empty(&self) -> bool {
        self.tickets.is_empty()
    }

    fn enqueue(&mut self, demands: Vec<Demand>) -> Result<(), String> {
        if demands
            .iter()
            .any(|demand| self.tickets.contains_key(&demand.pd_id))
        {
            return Err("scheduler already has a pending demand for this PD".into());
        }
        let tickets = self.controller.enqueue_demands(self.class, demands)?;
        self.tickets.extend(tickets);
        Ok(())
    }

    fn take_ready(
        &mut self,
        controller: &Arc<AdmissionController>,
        state: &mut State,
        max_units: usize,
    ) -> Option<AdmittedSubset> {
        if max_units == 0 {
            return None;
        }
        let mut pd_order: Vec<_> = self.tickets.keys().copied().collect();
        if let Some(cursor) = self.grant_cursor {
            let start = pd_order
                .iter()
                .position(|pd_id| *pd_id > cursor)
                .unwrap_or(0);
            pd_order.rotate_left(start);
        }

        let mut allocations = BTreeMap::<PdId, Allocation>::new();
        let mut prefixes = BTreeMap::<PdId, AdmittedPrefix>::new();
        let mut units_left = max_units;
        while units_left > 0 {
            let mut progressed = false;
            for &pd_id in &pd_order {
                if units_left == 0 {
                    break;
                }
                let Some(&ticket) = self.tickets.get(&pd_id) else {
                    continue;
                };
                if controller.next_admissible_ticket_for_pd(state, pd_id) != Some(ticket) {
                    continue;
                }
                let position = state
                    .waiters
                    .iter()
                    .position(|waiter| waiter.ticket == ticket)
                    .expect("admission ticket disappeared");
                let mut waiter = state
                    .waiters
                    .remove(position)
                    .expect("admission ticket disappeared");
                debug_assert_eq!(waiter.demand.pd_id, pd_id);
                let first_for_pd = !prefixes.contains_key(&pd_id);
                let allocation = controller.activate_one(state, &mut waiter, first_for_pd);
                let allocation_entry = allocations.entry(pd_id).or_insert(Allocation {
                    pd_id,
                    class: allocation.class,
                    blocks: 0,
                    borrowed_blocks: 0,
                });
                allocation_entry.blocks = allocation_entry.blocks.saturating_add(allocation.blocks);
                allocation_entry.borrowed_blocks = allocation_entry
                    .borrowed_blocks
                    .saturating_add(allocation.borrowed_blocks);
                let prefix = prefixes.entry(pd_id).or_insert(AdmittedPrefix {
                    pd_id,
                    units: 0,
                    blocks: 0,
                });
                prefix.units += 1;
                prefix.blocks = prefix.blocks.saturating_add(allocation.blocks);
                units_left -= 1;
                self.grant_cursor = Some(pd_id);
                progressed = true;

                if waiter.demand.grant_units.is_empty() {
                    debug_assert_eq!(waiter.demand.blocks, 0);
                    debug_assert_eq!(waiter.demand.requested_blocks, 0);
                    self.tickets.remove(&pd_id);
                } else {
                    state.waiters.insert(position, waiter);
                }
            }
            if !progressed {
                break;
            }
        }

        for (pd_id, ticket) in self
            .tickets
            .iter()
            .map(|(&pd_id, &ticket)| (pd_id, ticket))
            .collect::<Vec<_>>()
        {
            let admissible = controller.next_admissible_ticket_for_pd(state, pd_id) == Some(ticket);
            let waiting_on = {
                let waiter = state
                    .waiters
                    .iter_mut()
                    .find(|waiter| waiter.ticket == ticket)
                    .expect("admission ticket disappeared");
                if admissible {
                    close_wait_interval(waiter);
                    None
                } else {
                    if waiter.wait_started_at.is_none() {
                        waiter.wait_started_at = Some(Instant::now());
                    }
                    if waiter.recorded_wait {
                        None
                    } else {
                        waiter.recorded_wait = true;
                        Some((waiter.demand.pd_id, waiter.class.index()))
                    }
                }
            };
            let Some((pd_id, class_index)) = waiting_on else {
                continue;
            };
            let class_state = &mut state
                .pds
                .get_mut(&pd_id)
                .expect("queued PD missing")
                .classes[class_index];
            class_state.wait_events = class_state.wait_events.saturating_add(1);
        }
        if prefixes.is_empty() {
            return None;
        }
        Some(AdmittedSubset {
            prefixes: prefixes.into_values().collect(),
            permit: AdmissionPermit {
                controller: controller.clone(),
                allocations: Mutex::new(allocations.into_values().collect()),
            },
        })
    }

    #[cfg(test)]
    fn try_admit_ready(&mut self) -> Option<AdmittedSubset> {
        self.try_admit_ready_limited(usize::MAX)
    }

    fn try_admit_ready_limited(&mut self, max_units: usize) -> Option<AdmittedSubset> {
        if self.tickets.is_empty() {
            return None;
        }
        let controller = self.controller.clone();
        let mut state = controller.state.lock();
        let admitted = self.take_ready(&controller, &mut state, max_units);
        drop(state);
        if admitted.is_some() {
            controller.changed.notify_all();
        }
        admitted
    }

    #[cfg(test)]
    fn admit_ready(&mut self) -> AdmittedSubset {
        self.admit_ready_limited(usize::MAX)
    }

    fn admit_ready_limited(&mut self, max_units: usize) -> AdmittedSubset {
        assert!(!self.tickets.is_empty(), "no pending admission demand");
        assert!(max_units > 0, "cannot admit zero units");
        let controller = self.controller.clone();
        let mut state = controller.state.lock();
        loop {
            if let Some(admitted) = self.take_ready(&controller, &mut state, max_units) {
                drop(state);
                controller.changed.notify_all();
                return admitted;
            }
            controller.changed.wait(&mut state);
        }
    }
}

impl Drop for PendingAdmission {
    fn drop(&mut self) {
        if self.tickets.is_empty() {
            return;
        }
        let mut state = self.controller.state.lock();
        for ticket in self.tickets.values() {
            let position = state
                .waiters
                .iter()
                .position(|waiter| waiter.ticket == *ticket)
                .expect("pending admission ticket disappeared");
            let waiter = state
                .waiters
                .remove(position)
                .expect("pending admission ticket disappeared");
            let mut waiter = waiter;
            close_wait_interval(&mut waiter);
            let class_state = &mut state
                .pds
                .get_mut(&waiter.demand.pd_id)
                .expect("queued PD missing")
                .classes[waiter.class.index()];
            debug_assert!(class_state.queued_blocks >= waiter.demand.blocks);
            class_state.queued_blocks -= waiter.demand.blocks;
            if waiter.recorded_wait {
                let wait_ns = waiter.accumulated_wait_ns;
                class_state.wait_ns = class_state.wait_ns.saturating_add(wait_ns);
                class_state.wait_max_ns = class_state.wait_max_ns.max(wait_ns);
            }
        }
        self.tickets.clear();
        drop(state);
        self.controller.changed.notify_all();
    }
}

struct AdmissionPermit {
    controller: Arc<AdmissionController>,
    allocations: Mutex<Vec<Allocation>>,
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
}

impl IoBackend for ScheduledBackend {
    fn register_pd(&self, pd_id: PdId) {
        self.admission.register_pd(pd_id);
        self.inner.register_pd(pd_id);
    }

    fn scheduler_snapshot(&self) -> Option<SchedulerSnapshot> {
        Some(self.snapshot())
    }

    fn execution_snapshot(&self) -> Option<crate::io::IoExecutionSnapshot> {
        self.inner.execution_snapshot()
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
) -> Result<BTreeMap<PdId, VecDeque<PlannedPdWave>>, String> {
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
) -> Result<BTreeMap<PdId, VecDeque<PlannedPdWave>>, String> {
    if wave_cap == 0 || max_blocks == 0 || wave_cap > max_blocks {
        return Err("scheduler wave cap must be within 1..=max blocks".into());
    }
    let mut work_by_pd = BTreeMap::<PdId, Vec<(usize, u64)>>::new();
    for (index, &(pd_id, blocks)) in work.iter().enumerate() {
        work_by_pd.entry(pd_id).or_default().push((index, blocks));
    }

    let mut lanes = BTreeMap::new();
    for (pd_id, work) in work_by_pd {
        let mut indices = Vec::with_capacity(work.len());
        let mut grant_units = VecDeque::with_capacity(work.len());
        let mut planned_blocks = 0_u64;
        let mut requested_blocks = 0_u64;
        for (index, blocks) in work {
            let exclusive = blocks > wave_cap;
            let charged_blocks = if exclusive {
                blocks.min(max_blocks)
            } else {
                blocks
            };
            indices.push(index);
            grant_units.push_back(GrantUnit {
                blocks: charged_blocks,
                requested_blocks: blocks,
                exclusive,
            });
            planned_blocks = planned_blocks
                .checked_add(charged_blocks)
                .ok_or_else(|| "scheduler charged block accounting overflow".to_string())?;
            requested_blocks = requested_blocks
                .checked_add(blocks)
                .ok_or_else(|| "scheduler requested block accounting overflow".to_string())?;
        }
        lanes.insert(
            pd_id,
            VecDeque::from([PlannedPdWave {
                pd_id,
                indices,
                demand: Demand {
                    pd_id,
                    blocks: planned_blocks,
                    requested_blocks,
                    grant_units,
                },
            }]),
        );
    }
    Ok(lanes)
}

fn admission_errors(len: usize, message: String) -> Vec<ChunkletResult<()>> {
    (0..len)
        .map(|_| Err(ChunkletError::Config(message.clone())))
        .collect()
}

fn elapsed_ns(start: Instant) -> u64 {
    u64::try_from(start.elapsed().as_nanos()).unwrap_or(u64::MAX)
}

fn close_wait_interval(waiter: &mut Waiter) {
    if let Some(started_at) = waiter.wait_started_at.take() {
        waiter.accumulated_wait_ns = waiter
            .accumulated_wait_ns
            .saturating_add(elapsed_ns(started_at));
    }
}

#[cfg(test)]
mod tests;
