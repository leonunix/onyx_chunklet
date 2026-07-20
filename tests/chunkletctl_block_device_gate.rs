use std::process::Command;

use onyx_chunklet::io::RawDevice;
use onyx_chunklet::types::{CHUNKLET_SIZE, PD_RESERVED_BYTES};
use onyx_chunklet::{Pool, PoolConfig};

const PD_SIZE: u64 = 2 * PD_RESERVED_BYTES + CHUNKLET_SIZE;

fn chunkletctl() -> Command {
    Command::new(env!("CARGO_BIN_EXE_chunkletctl"))
}

#[test]
fn pool_init_does_not_create_a_missing_device_path() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("missing-pd");

    let output = chunkletctl()
        .args(["pool", "init", "--spare-pct", "5"])
        .arg(&path)
        .output()
        .unwrap();

    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains(path.to_str().unwrap()));
    assert!(stderr.contains("open:"));
    assert!(!path.exists());
}

#[test]
fn pool_init_rejects_a_regular_file_without_modifying_it() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("regular-pd");
    let contents = b"pool init must not modify this file";
    std::fs::write(&path, contents).unwrap();

    let output = chunkletctl()
        .args(["pool", "init", "--spare-pct", "5"])
        .arg(&path)
        .output()
        .unwrap();

    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains(path.to_str().unwrap()));
    assert!(stderr.contains("expected an existing block device"));
    assert_eq!(std::fs::read(&path).unwrap(), contents);
}

#[test]
fn pool_admit_rejects_a_regular_file_without_modifying_it() {
    let dir = tempfile::tempdir().unwrap();
    let pool_path = dir.path().join("pool-pd");
    let raw = RawDevice::open_or_create(&pool_path, PD_SIZE).unwrap();
    let pool = Pool::create(
        vec![raw],
        PoolConfig {
            spare_pct: 0,
            ..Default::default()
        },
    )
    .unwrap();
    drop(pool);

    let candidate_path = dir.path().join("regular-candidate");
    let contents = b"pool admit must not modify this file";
    std::fs::write(&candidate_path, contents).unwrap();

    let output = chunkletctl()
        .args(["pool", "admit", "--pool"])
        .arg(&pool_path)
        .args(["--spare-pct", "5"])
        .arg(&candidate_path)
        .output()
        .unwrap();

    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains(candidate_path.to_str().unwrap()));
    assert!(stderr.contains("expected an existing block device"));
    assert_eq!(std::fs::read(&candidate_path).unwrap(), contents);
}
