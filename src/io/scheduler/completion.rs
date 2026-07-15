use super::*;
use crate::io::backend::WriteCompletionObserver;

#[derive(Clone, Copy)]
struct OpCredit {
    pd_id: PdId,
    blocks: u64,
}

/// Maps backend completion indices back to the admission metadata retained by
/// the scheduler. No borrowed write buffer escapes the synchronous submit.
pub(super) struct CreditCompletionObserver<'a> {
    permit: &'a AdmissionPermit,
    credits: Vec<OpCredit>,
    state: Mutex<CompletionState>,
}

struct CompletionState {
    completed: Vec<bool>,
    service_ns_by_pd: BTreeMap<PdId, u64>,
}

impl<'a> CreditCompletionObserver<'a> {
    pub(super) fn new(permit: &'a AdmissionPermit, ops: &[StripWrite<'_>]) -> Self {
        Self {
            permit,
            credits: ops
                .iter()
                .map(|op| OpCredit {
                    pd_id: op.pd.pd_id(),
                    blocks: blocks_for_len(op.data.len())
                        .expect("planned write length became invalid"),
                })
                .collect(),
            state: Mutex::new(CompletionState {
                completed: vec![false; ops.len()],
                service_ns_by_pd: BTreeMap::new(),
            }),
        }
    }

    pub(super) fn service_ns_by_pd(&self) -> BTreeMap<PdId, u64> {
        self.state.lock().service_ns_by_pd.clone()
    }
}

impl WriteCompletionObserver for CreditCompletionObserver<'_> {
    fn writes_completed(&self, op_indices: &[usize], service_ns: u64) {
        let credits = {
            let mut state = self.state.lock();
            let mut credits = BTreeMap::<PdId, u64>::new();
            for &op_index in op_indices {
                let Some(done) = state.completed.get_mut(op_index) else {
                    continue;
                };
                if *done {
                    continue;
                }
                *done = true;
                let credit = self.credits[op_index];
                let blocks = credits.entry(credit.pd_id).or_default();
                *blocks = blocks.saturating_add(credit.blocks);
            }
            for &pd_id in credits.keys() {
                state
                    .service_ns_by_pd
                    .entry(pd_id)
                    .and_modify(|current| *current = (*current).max(service_ns))
                    .or_insert(service_ns);
            }
            credits.into_iter().collect::<Vec<_>>()
        };
        self.permit.reclaim(&credits);
    }
}

impl AdmissionPermit {
    fn reclaim(&self, credits: &[(PdId, u64)]) {
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
        self.controller.release(&released);
    }
}

impl Drop for AdmissionPermit {
    fn drop(&mut self) {
        self.controller.release(self.allocations.get_mut());
    }
}

impl ScheduledBackend {
    pub(super) fn submit_scheduled(
        &self,
        class: IoClass,
        ops: &[StripWrite<'_>],
    ) -> Vec<ChunkletResult<()>> {
        let max_blocks = self.admission.config.max_active_blocks_per_pd;
        let mut lanes = match plan_ops(ops, self.admission.config.wave_cap(class), max_blocks) {
            Ok(lanes) => lanes,
            Err(message) => return admission_errors(ops.len(), message),
        };
        let mut output: Vec<Option<ChunkletResult<()>>> =
            std::iter::repeat_with(|| None).take(ops.len()).collect();

        let initial_demands = lanes
            .values()
            .filter_map(|lane| lane.front().map(|wave| wave.demand.clone()))
            .collect();
        let mut pending = match self.admission.queue(class, initial_demands) {
            Ok(pending) => pending,
            Err(message) => return admission_errors(ops.len(), message),
        };
        while !pending.is_empty() {
            let AdmittedSubset { pd_ids, permit } = pending.admit_ready();
            let mut wave_indices = Vec::new();
            for pd_id in &pd_ids {
                let wave = lanes
                    .get(pd_id)
                    .and_then(|lane| lane.front())
                    .expect("admitted PD lane disappeared");
                debug_assert_eq!(wave.pd_id, *pd_id);
                wave_indices.extend_from_slice(&wave.indices);
            }
            wave_indices.sort_unstable();
            let wave_ops: Vec<_> = wave_indices
                .iter()
                .map(|&index| ops[index].clone())
                .collect();
            let completion = CreditCompletionObserver::new(&permit, &wave_ops);
            let service_started = Instant::now();
            let mut results = self
                .inner
                .submit_writes_detailed_observed_with_class(class, &wave_ops, &completion)
                .into_iter();
            let fallback_service_ns = elapsed_ns(service_started);
            let service_ns_by_pd = completion.service_ns_by_pd();
            let mut completions = Vec::with_capacity(wave_indices.len());
            for (&index, op) in wave_indices.iter().zip(&wave_ops) {
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
            let mut completions_by_pd = BTreeMap::<PdId, Vec<_>>::new();
            for completion in completions {
                completions_by_pd
                    .entry(completion.0)
                    .or_default()
                    .push(completion);
            }
            for (pd_id, completions) in completions_by_pd {
                self.admission.record_completion(
                    class,
                    &completions,
                    service_ns_by_pd
                        .get(&pd_id)
                        .copied()
                        .unwrap_or(fallback_service_ns),
                );
            }
            drop(permit);

            let mut next_demands = Vec::with_capacity(pd_ids.len());
            for pd_id in pd_ids {
                let lane = lanes.get_mut(&pd_id).expect("admitted PD lane disappeared");
                let completed = lane.pop_front().expect("admitted PD wave disappeared");
                debug_assert_eq!(completed.pd_id, pd_id);
                if let Some(next) = lane.front() {
                    next_demands.push(next.demand.clone());
                }
            }
            if let Err(message) = pending.enqueue(next_demands) {
                for result in output.iter_mut().filter(|result| result.is_none()) {
                    *result = Some(Err(ChunkletError::Config(message.clone())));
                }
                break;
            }
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
