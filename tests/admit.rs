//! Integration tests for `Pool::admit`: extending an existing pool with a
//! new blank PD and verifying that pre-existing LDs continue to serve IO
//! correctly afterward.

mod common;

use onyx_chunklet::io::RawDevice;
use onyx_chunklet::pool::LdSpec;
use onyx_chunklet::types::ChunkletState;
use onyx_chunklet::PoolConfig;
use tempfile::TempDir;

use common::{make_pool, open_full};

const PD_SIZE: u64 = 4 * 1024 * 1024 * 1024;

/// Regression: after admit-extending a pool, every existing LD's IO path
/// must still route through the original PDs (whose chunklets host the
/// data) and the new PD must end up with all-Free chunklets — admitted
/// PDs don't inherit any membership of pre-existing LDs.
#[test]
fn admit_then_existing_ld_io_round_trips() {
    let dir = TempDir::new().unwrap();
    let (pool, _paths) = make_pool(&dir, 4);
    let id = pool.create_ld(LdSpec::raid5(3, 1, 1, 0)).unwrap();
    let ld = pool.open_ld(id).unwrap();
    let payload: Vec<u8> = (0..(36 << 10))
        .map(|i| ((i * 31 + 5) % 251) as u8)
        .collect();
    ld.write_at(0, &payload).unwrap();

    // Admit a 5th PD into the running pool.
    let new_path = dir.path().join("pd_admit");
    let new_raw = RawDevice::open_or_create(&new_path, PD_SIZE).unwrap();
    let new_pd_id = pool
        .admit(
            new_raw,
            PoolConfig {
                spare_pct: 0,
                ..Default::default()
            },
        )
        .unwrap();
    assert_eq!(pool.pd_count(), 5);

    // The new PD has no chunklets allocated to the existing LD.
    let new_pd = pool.pd(new_pd_id).unwrap();
    let (_, bm, _) = new_pd.snapshot();
    assert_eq!(bm.count(ChunkletState::Used), 0);

    // The pre-existing LD reads back the original payload.
    let ld_post = pool.open_ld(id).unwrap();
    let mut readback = vec![0u8; payload.len()];
    ld_post.read_at(0, &mut readback).unwrap();
    assert_eq!(readback, payload, "IO after admit returned wrong data");

    // Writes still hit the same set of PDs (no descriptor migration).
    let new_payload = vec![0xc7u8; payload.len()];
    ld_post.write_at(0, &new_payload).unwrap();
    let mut readback2 = vec![0u8; payload.len()];
    ld_post.read_at(0, &mut readback2).unwrap();
    assert_eq!(readback2, new_payload);

    // Every member's PD is one of the original 4 (not the admitted 5th).
    let desc = pool.find_ld(id).unwrap();
    for m in &desc.members {
        assert_ne!(m.pd, new_pd_id, "post-admit member shouldn't be on new PD");
    }
}

/// After admit + reopen (full pool), the LD list is intact and IO still
/// works. Catches a regression where admit could leave a partially-updated
/// manifest such that reopen sees an inconsistent pool_pd_count.
#[test]
fn admit_persists_then_full_reopen_serves_io() {
    let dir = TempDir::new().unwrap();
    let (pool, paths) = make_pool(&dir, 3);
    let id = pool.create_ld(LdSpec::mirror(2, 1, 1, 0)).unwrap();
    let ld = pool.open_ld(id).unwrap();
    let payload: Vec<u8> = (0..(8 << 10)).map(|i| (i % 199) as u8).collect();
    ld.write_at(0, &payload).unwrap();
    drop(ld);

    let new_path = dir.path().join("pd_admit");
    let new_raw = RawDevice::open_or_create(&new_path, PD_SIZE).unwrap();
    pool.admit(
        new_raw,
        PoolConfig {
            spare_pct: 0,
            ..Default::default()
        },
    )
    .unwrap();
    drop(pool);

    let mut all_paths = paths;
    all_paths.push(new_path);
    let pool2 = open_full(&all_paths);
    assert_eq!(pool2.pd_count(), 4);
    let ld2 = pool2.open_ld(id).unwrap();
    let mut readback = vec![0u8; payload.len()];
    ld2.read_at(0, &mut readback).unwrap();
    assert_eq!(readback, payload);
}
