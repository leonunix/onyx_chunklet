//! `SyncBackend` — `std::thread::scope` fan-out, one thread per write.
//!
//! Always available, no kernel feature requirements. Used as the default
//! and as the fallback when `io_uring` init fails. Each PD's `pwrite`
//! runs on its own scoped thread so K+1 (R5) or K+2 (R6) member writes
//! land in parallel; for a single op we skip the spawn overhead and call
//! straight through.

use crate::error::{ChunkletError, ChunkletResult};
use crate::io::backend::{IoBackend, StripRead, StripWrite};

pub struct SyncBackend;

impl IoBackend for SyncBackend {
    fn name(&self) -> &'static str {
        "sync"
    }

    fn submit_reads(&self, ops: &mut [StripRead<'_>]) -> ChunkletResult<()> {
        if ops.is_empty() {
            return Ok(());
        }
        if ops.len() == 1 {
            let r = &mut ops[0];
            crate::numa::bind_current_to_node(r.pd.numa_node());
            return r
                .pd
                .read_chunklet_user(r.chunklet_index, r.in_chunklet_off, r.data);
        }
        std::thread::scope(|s| -> ChunkletResult<()> {
            let handles: Vec<_> = ops
                .iter_mut()
                .map(|r| {
                    s.spawn(move || {
                        crate::numa::bind_current_to_node(r.pd.numa_node());
                        r.pd.read_chunklet_user(r.chunklet_index, r.in_chunklet_off, r.data)
                    })
                })
                .collect();
            let mut first_err: Option<ChunkletError> = None;
            for h in handles {
                match h.join().expect("strip-read worker panicked") {
                    Ok(()) => {}
                    Err(e) => {
                        if first_err.is_none() {
                            first_err = Some(e);
                        }
                    }
                }
            }
            first_err.map_or(Ok(()), Err)
        })
    }

    fn submit_writes_detailed(&self, ops: &[StripWrite<'_>]) -> Vec<ChunkletResult<()>> {
        if ops.is_empty() {
            return Vec::new();
        }
        if ops.len() == 1 {
            let w = &ops[0];
            crate::numa::bind_current_to_node(w.pd.numa_node());
            return vec![w
                .pd
                .write_chunklet_user(w.chunklet_index, w.in_chunklet_off, w.data)];
        }
        // One scoped thread per op; collect each leg's result BY POSITION (no
        // first-error collapse) so the LD can absorb a within-budget subset of
        // failures. Every leg is joined, so all surviving legs are durable.
        std::thread::scope(|s| -> Vec<ChunkletResult<()>> {
            let handles: Vec<_> = ops
                .iter()
                .map(|w| {
                    s.spawn(move || {
                        crate::numa::bind_current_to_node(w.pd.numa_node());
                        w.pd.write_chunklet_user(w.chunklet_index, w.in_chunklet_off, w.data)
                    })
                })
                .collect();
            handles
                .into_iter()
                .map(|h| h.join().expect("strip-write worker panicked"))
                .collect()
        })
    }
}
