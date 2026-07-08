//! Online data-rebalance: converge per-PD used-skew by copying healthy members
//! onto an under-full PD, without data loss and without breaking set-PD
//! uniqueness. Skew is created by admitting a fresh (empty) PD after the LD is
//! laid out — exactly the returned-disk → rebalance scenario.

mod common;

use common::{make_pool, pattern};
use onyx_chunklet::io::{IoBackendKind, RawDevice};
use onyx_chunklet::pool::{LdSpec, RebalanceOptions};
use onyx_chunklet::{LdId, Pool, PoolConfig};

const PD_SIZE: u64 = 4 * 1024 * 1024 * 1024;

fn make_raid6(pool: &Pool, num_rows: u16) -> LdId {
    pool.create_ld(LdSpec::raid6(6, 1, num_rows, 12)).unwrap()
}

/// Admit a fresh blank PD → the existing PDs hold the LD, the new one is empty,
/// so per-PD used-skew jumps. Returns nothing; the pool now has one more PD.
fn admit_blank(pool: &Pool, dir: &tempfile::TempDir, name: &str) {
    let p = dir.path().join(name);
    let raw = RawDevice::open_or_create(&p, PD_SIZE).unwrap();
    pool.admit(
        raw,
        PoolConfig {
            spare_pct: 0,
            io_backend: IoBackendKind::Sync,
        },
    )
    .unwrap();
}

#[test]
fn rebalance_converges_and_preserves_data() {
    let dir = tempfile::tempdir().unwrap();
    let (pool, _paths) = make_pool(&dir, 10);
    let ld = make_raid6(&pool, 2);

    let data = pattern(0x55, 256 * 1024, 0);
    {
        let h = pool.open_ld(ld).unwrap();
        h.write_at(0, &data).unwrap();
        h.flush().unwrap();
    }

    admit_blank(&pool, &dir, "pd10");
    let skew_before = pool.metrics().unwrap().used_skew_chunklets;
    assert!(
        skew_before >= 2,
        "admitting an empty PD should create skew, got {skew_before}"
    );

    let report = pool
        .rebalance(RebalanceOptions {
            target_skew_pct: 15.0,
            max_moves: 100,
        })
        .unwrap();
    assert!(
        report.moves_committed >= 1,
        "rebalance should move at least one member onto the empty PD"
    );
    assert!(
        report.skew_after < skew_before,
        "skew must shrink: before={} after={}",
        skew_before,
        report.skew_after
    );

    // Data survives the move.
    let mut got = vec![0u8; data.len()];
    pool.open_ld(ld).unwrap().read_at(0, &mut got).unwrap();
    assert_eq!(got, data, "data must survive rebalance");
}

#[test]
fn rebalance_preserves_set_uniqueness() {
    let dir = tempfile::tempdir().unwrap();
    let (pool, _paths) = make_pool(&dir, 10);
    let ld = make_raid6(&pool, 2);
    admit_blank(&pool, &dir, "pd10");

    pool.rebalance(RebalanceOptions {
        target_skew_pct: 5.0,
        max_moves: 100,
    })
    .unwrap();

    let desc = pool.list_lds().into_iter().find(|d| d.id == ld).unwrap();
    let set_size = desc.set_size as usize;
    for (set_idx, set) in desc.members.chunks(set_size).enumerate() {
        let mut pds: Vec<_> = set.iter().map(|m| m.pd).collect();
        pds.sort();
        pds.dedup();
        assert_eq!(
            pds.len(),
            set_size,
            "set {set_idx} has duplicate PDs after rebalance: {:?}",
            set
        );
    }
}

#[test]
fn rebalance_respects_move_budget() {
    let dir = tempfile::tempdir().unwrap();
    let (pool, _paths) = make_pool(&dir, 10);
    let _ld = make_raid6(&pool, 2);
    admit_blank(&pool, &dir, "pd10");

    // Tight target so it wants to move; budget of 1 caps this invocation.
    let r1 = pool
        .rebalance(RebalanceOptions {
            target_skew_pct: 1.0,
            max_moves: 1,
        })
        .unwrap();
    assert!(r1.moves_committed <= 1, "budget must cap committed moves");

    // A follow-up invocation continues toward balance.
    let r2 = pool
        .rebalance(RebalanceOptions {
            target_skew_pct: 1.0,
            max_moves: 100,
        })
        .unwrap();
    assert!(
        r1.moves_committed + r2.moves_committed >= 1,
        "rebalance should make progress across invocations"
    );
}
