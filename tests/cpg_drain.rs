//! P7 tests: CPG creation + LD-in-CPG, plus PD drain.

use std::path::PathBuf;
use std::sync::Arc;

use onyx_chunklet::io::RawDevice;
use onyx_chunklet::pool::{CpgSpec, LdSpec};
use onyx_chunklet::types::{ChunkletState, HaDomain, RaidLevel};
use onyx_chunklet::{Pool, PoolConfig};
use tempfile::TempDir;

const PD_SIZE: u64 = 4 * 1024 * 1024 * 1024;

fn make_pool(dir: &TempDir, n: usize) -> (Arc<Pool>, Vec<PathBuf>) {
    let mut raws = Vec::new();
    let mut paths = Vec::new();
    for i in 0..n {
        let p = dir.path().join(format!("pd{}", i));
        raws.push(RawDevice::open_or_create(&p, PD_SIZE).unwrap());
        paths.push(p);
    }
    let pool = Pool::create(raws, PoolConfig { spare_pct: 0 }).unwrap();
    (pool, paths)
}

fn open_full(paths: &[PathBuf]) -> Arc<Pool> {
    let raws: Vec<_> = paths.iter().map(|p| RawDevice::open(p).unwrap()).collect();
    Pool::open(raws).unwrap()
}

// ---- CPG ------------------------------------------------------------------

#[test]
fn cpg_create_list_persist_drop() {
    let dir = TempDir::new().unwrap();
    let (pool, paths) = make_pool(&dir, 5);
    let cpg_id = pool
        .create_cpg(CpgSpec {
            name: "lv3-r6".into(),
            raid_level: RaidLevel::Raid6,
            set_size: 5,
            row_size: 1,
            strip_size_log2: 0,
            ha_domain: HaDomain::Pd,
        })
        .unwrap();
    assert_eq!(pool.list_cpgs().len(), 1);
    assert_eq!(pool.find_cpg(cpg_id).unwrap().name, "lv3-r6");
    assert_eq!(pool.find_cpg_by_name("lv3-r6").unwrap().id, cpg_id);
    drop(pool);

    let pool2 = open_full(&paths);
    let cpgs = pool2.list_cpgs();
    assert_eq!(cpgs.len(), 1);
    assert_eq!(cpgs[0].id, cpg_id);
    assert_eq!(cpgs[0].name, "lv3-r6");
    assert_eq!(cpgs[0].raid_level, RaidLevel::Raid6);

    pool2.drop_cpg(cpg_id).unwrap();
    assert!(pool2.list_cpgs().is_empty());
    drop(pool2);

    let pool3 = open_full(&paths);
    assert!(pool3.list_cpgs().is_empty());
}

#[test]
fn cpg_rejects_duplicate_names() {
    let dir = TempDir::new().unwrap();
    let (pool, _) = make_pool(&dir, 3);
    pool.create_cpg(CpgSpec {
        name: "dup".into(),
        raid_level: RaidLevel::Mirror,
        set_size: 2,
        row_size: 1,
        strip_size_log2: 0,
        ha_domain: HaDomain::Pd,
    })
    .unwrap();
    let err = pool
        .create_cpg(CpgSpec {
            name: "dup".into(),
            raid_level: RaidLevel::Raid5,
            set_size: 4,
            row_size: 1,
            strip_size_log2: 0,
            ha_domain: HaDomain::Pd,
        })
        .err()
        .unwrap();
    assert!(format!("{}", err).contains("already in use"));
}

#[test]
fn create_ld_in_cpg_uses_cpg_policy() {
    let dir = TempDir::new().unwrap();
    let (pool, _) = make_pool(&dir, 5);
    let cpg_id = pool
        .create_cpg(CpgSpec {
            name: "raid5-3p1".into(),
            raid_level: RaidLevel::Raid5,
            set_size: 4,
            row_size: 1,
            strip_size_log2: 0,
            ha_domain: HaDomain::Pd,
        })
        .unwrap();
    let ld_id = pool.create_ld_in_cpg(cpg_id, 1).unwrap();
    let desc = pool.find_ld(ld_id).unwrap();
    assert_eq!(desc.raid_level, RaidLevel::Raid5);
    assert_eq!(desc.set_size, 4);
    assert_eq!(desc.members.len(), 4);
}

// ---- Drain ----------------------------------------------------------------

#[test]
fn drain_pd_migrates_mirror_lds() {
    let dir = TempDir::new().unwrap();
    let (pool, _) = make_pool(&dir, 4); // 2-way mirror + 2 spare PDs
    let id = pool.create_ld(LdSpec::mirror(2, 1, 1, 0)).unwrap();
    let ld = pool.open_ld(id).unwrap();
    let payload: Vec<u8> = (0..(8 << 10)).map(|i| (i % 251) as u8).collect();
    ld.write_at(0, &payload).unwrap();
    drop(ld);

    let desc = pool.find_ld(id).unwrap();
    let drain_target = desc.members[0].pd;
    let report = pool.drain_pd(drain_target).unwrap();
    assert_eq!(report.lds_affected, vec![id]);
    assert_eq!(report.members_migrated, 1);

    let new_desc = pool.find_ld(id).unwrap();
    for m in &new_desc.members {
        assert_ne!(m.pd, drain_target, "member still on drained PD");
    }
    // Read back via remaining live copy.
    let ld_post = pool.open_ld(id).unwrap();
    let mut readback = vec![0u8; payload.len()];
    ld_post.read_at(0, &mut readback).unwrap();
    assert_eq!(readback, payload);
}

#[test]
fn drain_pd_refuses_when_plain_ld_uses_it() {
    let dir = TempDir::new().unwrap();
    let (pool, _) = make_pool(&dir, 3);
    let plain_id = pool.create_ld(LdSpec::plain(2)).unwrap();
    let plain_desc = pool.find_ld(plain_id).unwrap();
    let drain_target = plain_desc.members[0].pd;
    let err = pool.drain_pd(drain_target).err().unwrap();
    assert!(format!("{}", err).contains("no redundancy"));
    // After failure, drain flag is cleared.
    assert!(!pool.is_pd_draining(drain_target));
}

#[test]
fn drained_flag_persists_across_reopen() {
    let dir = TempDir::new().unwrap();
    let (pool, paths) = make_pool(&dir, 4);
    let id = pool.create_ld(LdSpec::mirror(2, 1, 1, 0)).unwrap();
    let drain_target = pool.find_ld(id).unwrap().members[0].pd;
    pool.drain_pd(drain_target).unwrap();
    drop(pool);

    let pool2 = open_full(&paths);
    // Drained PD should still appear in pd_list with DRAINED flag set.
    // (We don't have a public flag accessor yet; check via PD's body.)
    let pd = pool2.pd(drain_target).unwrap();
    let (body, _, _) = pd.snapshot();
    let entry = body.pd_list.iter().find(|e| e.pd_id == drain_target).unwrap();
    assert!(entry.flags & onyx_chunklet::superblock::pool_pd_flags::DRAINED != 0);
}

#[test]
fn drain_pd_rejects_failed_or_unknown_pd() {
    let dir = TempDir::new().unwrap();
    let (pool, _) = make_pool(&dir, 3);
    let bogus = onyx_chunklet::PdId::new_v4();
    assert!(pool.drain_pd(bogus).is_err());
}

#[test]
fn drain_marks_target_chunklets_used_on_replacement() {
    let dir = TempDir::new().unwrap();
    let (pool, _) = make_pool(&dir, 4);
    let id = pool.create_ld(LdSpec::mirror(2, 1, 1, 0)).unwrap();
    let drain_target = pool.find_ld(id).unwrap().members[0].pd;
    pool.drain_pd(drain_target).unwrap();
    // The replacement member's chunklet must be Used in the new PD's bitmap.
    let new_desc = pool.find_ld(id).unwrap();
    let new_member = new_desc.members[0];
    let pd = pool.pd(new_member.pd).unwrap();
    let (_, bitmap, _) = pd.snapshot();
    assert_eq!(
        bitmap.get(new_member.chunklet_index).unwrap(),
        ChunkletState::Used
    );
}
