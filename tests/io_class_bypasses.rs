//! Class propagation for write paths that historically bypassed `IoBackend`.

mod common;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use common::{make_pool, open_full, open_subset};
use onyx_chunklet::error::{ChunkletError, ChunkletResult};
use onyx_chunklet::io::sync_backend::SyncBackend;
use onyx_chunklet::io::{with_io_class, IoBackend, IoClass, StripRead, StripWrite};
use onyx_chunklet::pool::LdSpec;
use onyx_chunklet::types::{PdId, BLOCK_SIZE, CHUNKLET_HEADER_BYTES, CHUNKLET_SIZE};
use onyx_chunklet::Pool;

#[derive(Clone, Debug)]
struct CapturedCall {
    class: IoClass,
    op_count: usize,
    pds: Vec<PdId>,
}

#[derive(Default)]
struct GateState {
    second_maintenance_entered: bool,
    release_second_maintenance: bool,
}

struct ClassCaptureBackend {
    inner: Arc<dyn IoBackend>,
    calls: Mutex<Vec<CapturedCall>>,
    maintenance_calls: AtomicUsize,
    drain_meta_calls: AtomicUsize,
    gate: Mutex<GateState>,
    gate_cv: Condvar,
    gate_second_maintenance: bool,
    fail_second_drain_meta: bool,
}

impl ClassCaptureBackend {
    fn passive() -> Arc<Self> {
        Arc::new(Self::new(false, false))
    }

    fn gated_rebuild() -> Arc<Self> {
        Arc::new(Self::new(true, true))
    }

    fn new(gate_second_maintenance: bool, fail_second_drain_meta: bool) -> Self {
        Self {
            inner: Arc::new(SyncBackend),
            calls: Mutex::new(Vec::new()),
            maintenance_calls: AtomicUsize::new(0),
            drain_meta_calls: AtomicUsize::new(0),
            gate: Mutex::new(GateState::default()),
            gate_cv: Condvar::new(),
            gate_second_maintenance,
            fail_second_drain_meta,
        }
    }

    fn calls(&self) -> Vec<CapturedCall> {
        self.calls.lock().unwrap().clone()
    }

    fn wait_for_second_maintenance(&self) -> bool {
        let state = self.gate.lock().unwrap();
        let (state, timeout) = self
            .gate_cv
            .wait_timeout_while(state, Duration::from_secs(20), |state| {
                !state.second_maintenance_entered
            })
            .unwrap();
        state.second_maintenance_entered && !timeout.timed_out()
    }

    fn release_second_maintenance(&self) {
        let mut state = self.gate.lock().unwrap();
        state.release_second_maintenance = true;
        self.gate_cv.notify_all();
    }
}

impl IoBackend for ClassCaptureBackend {
    fn name(&self) -> &'static str {
        "class-capture"
    }

    fn submit_reads(&self, ops: &mut [StripRead<'_>]) -> ChunkletResult<()> {
        self.inner.submit_reads(ops)
    }

    fn submit_writes_detailed(&self, ops: &[StripWrite<'_>]) -> Vec<ChunkletResult<()>> {
        self.inner.submit_writes_detailed(ops)
    }

    fn submit_writes_detailed_with_class(
        &self,
        class: IoClass,
        ops: &[StripWrite<'_>],
    ) -> Vec<ChunkletResult<()>> {
        self.calls.lock().unwrap().push(CapturedCall {
            class,
            op_count: ops.len(),
            pds: ops.iter().map(|op| op.pd.pd_id()).collect(),
        });

        if class == IoClass::Maintenance {
            let call = self.maintenance_calls.fetch_add(1, Ordering::SeqCst) + 1;
            if self.gate_second_maintenance && call == 2 {
                let mut state = self.gate.lock().unwrap();
                state.second_maintenance_entered = true;
                self.gate_cv.notify_all();
                while !state.release_second_maintenance {
                    state = self.gate_cv.wait(state).unwrap();
                }
            }
        }

        if class == IoClass::DrainMeta {
            let call = self.drain_meta_calls.fetch_add(1, Ordering::SeqCst) + 1;
            if self.fail_second_drain_meta && call == 2 {
                return ops
                    .iter()
                    .map(|_| Err(ChunkletError::Invariant("injected shadow failure".into())))
                    .collect();
            }
        }

        self.inner.submit_writes_detailed_with_class(class, ops)
    }
}

fn install_backend(pool: &Pool, backend: Arc<ClassCaptureBackend>) {
    for info in pool.list_pds() {
        pool.pd(info.pd_id).unwrap().set_backend(backend.clone());
    }
}

#[test]
fn plain_and_raid0_inherit_the_callers_class() {
    let dir = tempfile::tempdir().unwrap();
    let (plain_pool, _) = make_pool(&dir, 2);
    let plain_id = plain_pool.create_ld(LdSpec::plain(2)).unwrap();
    let plain_backend = ClassCaptureBackend::passive();
    install_backend(&plain_pool, plain_backend.clone());
    let plain = plain_pool.open_ld(plain_id).unwrap();
    let chunklet_user = CHUNKLET_SIZE - CHUNKLET_HEADER_BYTES;
    let plain_data = vec![0x41; 2 * BLOCK_SIZE as usize];
    with_io_class(IoClass::DrainData, || {
        plain
            .write_at(chunklet_user - BLOCK_SIZE, &plain_data)
            .unwrap()
    });
    let plain_calls = plain_backend.calls();
    assert_eq!(plain_calls.len(), 1);
    assert_eq!(plain_calls[0].class, IoClass::DrainData);
    assert_eq!(plain_calls[0].op_count, 2);
    assert_ne!(plain_calls[0].pds[0], plain_calls[0].pds[1]);

    let raid0_dir = tempfile::tempdir().unwrap();
    let (raid0_pool, _) = make_pool(&raid0_dir, 2);
    let raid0_id = raid0_pool.create_ld(LdSpec::raid0(2, 1, 0)).unwrap();
    let raid0_backend = ClassCaptureBackend::passive();
    install_backend(&raid0_pool, raid0_backend.clone());
    let raid0 = raid0_pool.open_ld(raid0_id).unwrap();
    let raid0_data = vec![0x52; 2 * BLOCK_SIZE as usize];
    with_io_class(IoClass::DrainMeta, || {
        raid0.write_at(0, &raid0_data).unwrap()
    });
    let raid0_calls = raid0_backend.calls();
    assert_eq!(raid0_calls.len(), 1);
    assert_eq!(raid0_calls[0].class, IoClass::DrainMeta);
    assert_eq!(raid0_calls[0].op_count, 2);
    assert_ne!(raid0_calls[0].pds[0], raid0_calls[0].pds[1]);
}

#[test]
fn rebuild_marks_backfill_maintenance_and_write_forward_keeps_caller_class() {
    let dir = tempfile::tempdir().unwrap();
    let (pool, paths) = make_pool(&dir, 3);
    let ld_id = pool.create_ld(LdSpec::mirror(2, 1, 1, 0)).unwrap();
    let failed_pd = pool.find_ld(ld_id).unwrap().members[0].pd;
    drop(pool);

    let full = open_full(&paths);
    let failed_path = paths
        .iter()
        .position(|path| full.pd(failed_pd).unwrap().path() == path)
        .unwrap();
    drop(full);

    let degraded = open_subset(&paths, &[failed_path]);
    let backend = ClassCaptureBackend::gated_rebuild();
    install_backend(&degraded, backend.clone());
    let foreground = degraded.open_ld(ld_id).unwrap();
    let rebuild_pool = degraded.clone();
    let rebuild = std::thread::spawn(move || rebuild_pool.rebuild_ld(ld_id));

    let entered = backend.wait_for_second_maintenance();
    if !entered {
        backend.release_second_maintenance();
        let _ = rebuild.join();
        panic!("online rebuild did not reach its second Maintenance write");
    }

    let payload = vec![0x6d; BLOCK_SIZE as usize];
    let foreground_result = with_io_class(IoClass::DrainMeta, || foreground.write_at(0, &payload));
    backend.release_second_maintenance();
    let rebuild_result = rebuild.join().unwrap();

    assert!(
        foreground_result.is_ok(),
        "a shadow write failure must not fail the foreground write: {:?}",
        foreground_result
    );
    assert!(
        rebuild_result.is_err(),
        "the injected shadow failure must abort the rebuild"
    );

    let calls = backend.calls();
    assert!(
        calls
            .iter()
            .filter(|call| call.class == IoClass::Maintenance)
            .count()
            >= 2,
        "backfill writes were not submitted as Maintenance: {:?}",
        calls
    );
    let forwarded: Vec<_> = calls
        .iter()
        .filter(|call| call.class == IoClass::DrainMeta)
        .collect();
    assert_eq!(
        forwarded.len(),
        2,
        "expected one live-member write and one shadow write-forward: {:?}",
        calls
    );
    assert!(forwarded.iter().all(|call| call.op_count == 1));
    assert_ne!(forwarded[0].pds[0], forwarded[1].pds[0]);
}
