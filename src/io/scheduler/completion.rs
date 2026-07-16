use super::*;
use crate::io::backend::{
    DispatchedCompletion, DispatchedWrite, WriteDispatch, WriteDispatchStatus,
};

struct ActiveWave {
    indices: Vec<usize>,
    next_to_dispatch: usize,
    remaining: usize,
}

struct SchedulerDispatch<'ops, 'data> {
    admission: Arc<AdmissionController>,
    class: IoClass,
    ops: &'ops [StripWrite<'data>],
    lanes: BTreeMap<PdId, VecDeque<PlannedPdWave>>,
    pending: PendingAdmission,
    active: BTreeMap<PdId, ActiveWave>,
    permits: Vec<AdmissionPermit>,
    op_state: Vec<u8>,
    dispatch_cursor: Option<PdId>,
    protocol_error: Option<String>,
}

impl AdmissionPermit {
    fn reclaim(&self, credits: &[(PdId, u64)]) -> u64 {
        let released = {
            let mut allocations = self.allocations.lock();
            let mut released = Vec::with_capacity(credits.len());
            for &(pd_id, blocks) in credits {
                if blocks == 0 {
                    continue;
                }
                let Some(position) = allocations
                    .iter()
                    .position(|allocation| allocation.pd_id == pd_id && allocation.blocks > 0)
                else {
                    continue;
                };
                let allocation = &mut allocations[position];
                let released_blocks = allocation.blocks.min(blocks);
                let released_borrowed = allocation.borrowed_blocks.min(released_blocks);
                allocation.blocks -= released_blocks;
                allocation.borrowed_blocks -= released_borrowed;
                released.push(Allocation {
                    pd_id,
                    class: allocation.class,
                    blocks: released_blocks,
                    borrowed_blocks: released_borrowed,
                });
                if allocation.blocks == 0 {
                    allocations.swap_remove(position);
                }
            }
            released
        };
        let released_blocks = released.iter().fold(0_u64, |total, allocation| {
            total.saturating_add(allocation.blocks)
        });
        self.controller.release(&released);
        released_blocks
    }
}

impl Drop for AdmissionPermit {
    fn drop(&mut self) {
        self.controller.release(self.allocations.get_mut());
    }
}

impl<'ops, 'data> SchedulerDispatch<'ops, 'data> {
    fn new(
        admission: Arc<AdmissionController>,
        class: IoClass,
        ops: &'ops [StripWrite<'data>],
        lanes: BTreeMap<PdId, VecDeque<PlannedPdWave>>,
    ) -> Result<Self, String> {
        let initial_demands = lanes
            .values()
            .filter_map(|lane| lane.front().map(|wave| wave.demand.clone()))
            .collect();
        let pending = admission.queue(class, initial_demands)?;
        Ok(Self {
            admission,
            class,
            ops,
            lanes,
            pending,
            active: BTreeMap::new(),
            permits: Vec::new(),
            op_state: vec![0; ops.len()],
            dispatch_cursor: None,
            protocol_error: None,
        })
    }

    fn install_admitted(&mut self, admitted: AdmittedSubset) {
        for pd_id in &admitted.pd_ids {
            let wave = self
                .lanes
                .get(pd_id)
                .and_then(|lane| lane.front())
                .expect("admitted PD lane disappeared");
            debug_assert_eq!(wave.pd_id, *pd_id);
            let previous = self.active.insert(
                *pd_id,
                ActiveWave {
                    indices: wave.indices.clone(),
                    next_to_dispatch: 0,
                    remaining: wave.indices.len(),
                },
            );
            debug_assert!(previous.is_none(), "PD already has an active wave");
        }
        self.permits.push(admitted.permit);
    }

    fn take_unsent(&mut self, max_ops: usize) -> Vec<DispatchedWrite<'data>> {
        if max_ops == 0 {
            return Vec::new();
        }
        let pd_ids: Vec<_> = self.active.keys().copied().collect();
        let start = self.dispatch_cursor.map_or(0, |cursor| {
            pd_ids.iter().position(|pd_id| *pd_id > cursor).unwrap_or(0)
        });
        let mut ready = Vec::new();
        while ready.len() < max_ops {
            let mut progressed = false;
            for offset in 0..pd_ids.len() {
                if ready.len() == max_ops {
                    break;
                }
                let pd_id = &pd_ids[(start + offset) % pd_ids.len()];
                let wave = self.active.get_mut(pd_id).expect("active PD disappeared");
                let Some(&index) = wave.indices.get(wave.next_to_dispatch) else {
                    continue;
                };
                debug_assert_eq!(self.op_state[index], 0);
                self.op_state[index] = 1;
                wave.next_to_dispatch += 1;
                ready.push(DispatchedWrite {
                    index,
                    write: self.ops[index].clone(),
                });
                self.dispatch_cursor = Some(*pd_id);
                progressed = true;
            }
            if !progressed {
                break;
            }
        }
        ready.sort_unstable_by_key(|admitted| admitted.index);
        ready
    }

    fn complete(&self) -> bool {
        self.pending.is_empty()
            && self.active.is_empty()
            && self.lanes.values().all(VecDeque::is_empty)
    }

    fn poll_inner(&mut self, max_ops: usize, may_wait: bool) -> WriteDispatchStatus<'data> {
        if self.protocol_error.is_some() {
            return WriteDispatchStatus::Complete;
        }
        let ready = self.take_unsent(max_ops);
        if !ready.is_empty() {
            return WriteDispatchStatus::Ready(ready);
        }
        if self.complete() {
            return WriteDispatchStatus::Complete;
        }

        let admitted = if may_wait && self.active.is_empty() && !self.pending.is_empty() {
            Some(self.pending.admit_ready())
        } else {
            self.pending.try_admit_ready()
        };
        if let Some(admitted) = admitted {
            self.install_admitted(admitted);
            let ready = self.take_unsent(max_ops);
            if !ready.is_empty() {
                return WriteDispatchStatus::Ready(ready);
            }
        }
        if self.complete() {
            WriteDispatchStatus::Complete
        } else {
            WriteDispatchStatus::Pending
        }
    }

    fn set_protocol_error(&mut self, message: impl Into<String>) {
        if self.protocol_error.is_none() {
            self.protocol_error = Some(message.into());
        }
    }

    fn finish_results(&self, mut results: Vec<ChunkletResult<()>>) -> Vec<ChunkletResult<()>> {
        while results.len() < self.ops.len() {
            results.push(Err(ChunkletError::Invariant(
                "driven IO backend returned too few write results".into(),
            )));
        }
        results.truncate(self.ops.len());
        if let Some(message) = &self.protocol_error {
            for result in &mut results {
                *result = Err(ChunkletError::Invariant(format!(
                    "write dispatch protocol failed: {message}"
                )));
            }
        }
        results
    }
}

impl<'data> WriteDispatch<'data> for SchedulerDispatch<'_, 'data> {
    fn poll_ready(&mut self, max_ops: usize) -> WriteDispatchStatus<'data> {
        self.poll_inner(max_ops, false)
    }

    fn wait_ready(&mut self, max_ops: usize) -> WriteDispatchStatus<'data> {
        self.poll_inner(max_ops, true)
    }

    fn writes_completed(&mut self, completions: &[DispatchedCompletion], service_ns: u64) {
        if self.protocol_error.is_some() {
            return;
        }
        for completion in completions {
            let Some(state) = self.op_state.get(completion.index) else {
                self.set_protocol_error(format!(
                    "write completion index {} is out of range",
                    completion.index
                ));
                return;
            };
            if *state != 1 {
                self.set_protocol_error(format!(
                    "write completion index {} is duplicate or was not dispatched",
                    completion.index
                ));
                return;
            }
        }

        let mut completed_by_pd = BTreeMap::<PdId, usize>::new();
        let mut metrics = Vec::with_capacity(completions.len());
        for completion in completions {
            let index = completion.index;
            self.op_state[index] = 2;
            let op = &self.ops[index];
            let pd_id = op.pd.pd_id();
            let blocks =
                blocks_for_len(op.data.len()).expect("planned write length became invalid");
            let admitted_blocks = blocks.min(self.admission.config.max_active_blocks_per_pd);
            let released = self
                .permits
                .iter()
                .map(|permit| permit.reclaim(&[(pd_id, admitted_blocks)]))
                .find(|&released| released > 0)
                .unwrap_or(0);
            if released != admitted_blocks {
                self.set_protocol_error(format!(
                    "write completion index {index} released {released}/{admitted_blocks} admitted blocks"
                ));
                return;
            }
            *completed_by_pd.entry(pd_id).or_default() += 1;
            metrics.push((pd_id, blocks, completion.failed));
        }
        self.permits
            .retain(|permit| !permit.allocations.lock().is_empty());
        self.admission
            .record_completion(self.class, &metrics, service_ns);

        let mut finished_pds = Vec::new();
        for (pd_id, completed) in completed_by_pd {
            let wave = self
                .active
                .get_mut(&pd_id)
                .expect("completion has no active PD wave");
            if completed > wave.remaining {
                let remaining = wave.remaining;
                self.set_protocol_error(format!(
                    "PD {pd_id} completed {completed} writes with only {remaining} remaining"
                ));
                return;
            }
            wave.remaining -= completed;
            if wave.remaining == 0 {
                finished_pds.push(pd_id);
            }
        }

        for pd_id in finished_pds {
            self.active.remove(&pd_id).expect("finished PD disappeared");
            let lane = self
                .lanes
                .get_mut(&pd_id)
                .expect("finished PD lane disappeared");
            let completed = lane.pop_front().expect("finished PD wave disappeared");
            debug_assert_eq!(completed.pd_id, pd_id);
            if let Some(next) = lane.front() {
                if let Err(message) = self.pending.enqueue(vec![next.demand.clone()]) {
                    self.set_protocol_error(message);
                    return;
                }
            }
        }
    }
}

impl ScheduledBackend {
    pub(super) fn submit_scheduled(
        &self,
        class: IoClass,
        ops: &[StripWrite<'_>],
    ) -> Vec<ChunkletResult<()>> {
        let max_blocks = self.admission.config.max_active_blocks_per_pd;
        let lanes = match plan_ops(ops, self.admission.config.wave_cap(class), max_blocks) {
            Ok(lanes) => lanes,
            Err(message) => return admission_errors(ops.len(), message),
        };
        let mut dispatch = match SchedulerDispatch::new(self.admission.clone(), class, ops, lanes) {
            Ok(dispatch) => dispatch,
            Err(message) => return admission_errors(ops.len(), message),
        };
        let results =
            self.inner
                .submit_writes_dispatched_with_class(class, ops.len(), &mut dispatch);
        dispatch.finish_results(results)
    }
}
