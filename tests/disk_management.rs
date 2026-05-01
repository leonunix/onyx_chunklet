mod common;

use onyx_chunklet::pool::LdSpec;
use onyx_chunklet::types::{ChunkletState, BLOCK_SIZE};
use onyx_chunklet::{Pool, PoolConfig};
use tempfile::TempDir;

use common::{make_pool, open_full};

#[test]
fn mark_pd_failed_persists_and_routes_reads_around_pd() {
    let dir = TempDir::new().unwrap();
    let (pool, paths) = make_pool(&dir, 3);
    let ld_id = pool.create_ld(LdSpec::mirror(2, 1, 1, 0)).unwrap();
    let ld = pool.open_ld(ld_id).unwrap();
    let payload = vec![0x5a; BLOCK_SIZE as usize];
    ld.write_at(0, &payload).unwrap();
    drop(ld);

    let failed_pd = pool.find_ld(ld_id).unwrap().members[0].pd;
    pool.mark_pd_failed(failed_pd).unwrap();
    assert_eq!(
        pool.metrics()
            .unwrap()
            .lds
            .iter()
            .find(|ld| ld.ld_id == ld_id)
            .unwrap()
            .failed_members,
        1
    );

    let degraded = pool.open_ld(ld_id).unwrap();
    let mut readback = vec![0; payload.len()];
    degraded.read_at(0, &mut readback).unwrap();
    assert_eq!(readback, payload);
    drop((degraded, pool));

    let reopened = open_full(&paths);
    assert_eq!(
        reopened
            .metrics()
            .unwrap()
            .pds
            .iter()
            .find(|pd| pd.pd_id == failed_pd)
            .unwrap()
            .state,
        onyx_chunklet::metrics::PdOperationalState::Failed
    );

    reopened.clear_pd_failed(failed_pd).unwrap();
    assert_eq!(
        reopened.pd_health(failed_pd),
        Some(onyx_chunklet::pool::PdHealth::Healthy)
    );
}

#[test]
fn rebalance_spares_promotes_and_releases_free_chunklets() {
    let dir = TempDir::new().unwrap();
    let mut raws = Vec::new();
    for i in 0..3 {
        let path = dir.path().join(format!("pd{}", i));
        raws.push(
            onyx_chunklet::io::RawDevice::open_or_create(&path, 4 * 1024 * 1024 * 1024).unwrap(),
        );
    }
    let pool = Pool::create(
        raws,
        PoolConfig {
            spare_pct: 0,
            ..Default::default()
        },
    )
    .unwrap();

    let report = pool.rebalance_spares(34).unwrap();
    assert_eq!(report.pds.len(), 3);
    for pd in &report.pds {
        assert_eq!(pd.spares_after, 2);
        assert_eq!(pd.free_after, 1);
    }
    for info in pool.list_pds() {
        let pd = pool.pd(info.pd_id).unwrap();
        let (_, bitmap, _) = pd.snapshot();
        assert_eq!(bitmap.count(ChunkletState::Spare), 2);
    }

    let report = pool.rebalance_spares(0).unwrap();
    for pd in &report.pds {
        assert_eq!(pd.spares_after, 0);
        assert_eq!(pd.free_after, 3);
    }
}

#[test]
fn auto_recover_rebuilds_failed_members() {
    let dir = TempDir::new().unwrap();
    let (pool, paths) = make_pool(&dir, 5);
    let ld_id = pool.create_ld(LdSpec::raid5(3, 1, 1, 0)).unwrap();
    let ld = pool.open_ld(ld_id).unwrap();
    let payload: Vec<u8> = (0..(3 * BLOCK_SIZE as usize))
        .map(|i| ((i * 11 + 7) % 251) as u8)
        .collect();
    ld.write_at(0, &payload).unwrap();
    drop(ld);

    let failed_pd = pool.find_ld(ld_id).unwrap().members[0].pd;
    let drop_idx = paths
        .iter()
        .position(|p| pool.pd(failed_pd).unwrap().path() == p)
        .unwrap();
    drop(pool);

    let degraded = common::open_subset(&paths, &[drop_idx]);
    let report = degraded.auto_recover(false);
    assert_eq!(report.failed, 0);
    assert_eq!(report.recovered, 1);
    assert_eq!(report.lds[0].rebuilt_members, 1);

    let ld = degraded.open_ld(ld_id).unwrap();
    let mut readback = vec![0; payload.len()];
    ld.read_at(0, &mut readback).unwrap();
    assert_eq!(readback, payload);
}
