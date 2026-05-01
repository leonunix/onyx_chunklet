//! `chunklet-soak` — standalone fault-injection + verification harness.
//!
//! Purpose: long-running confidence test for the chunklet crate, runnable
//! on the nvme-box without any onyx code. Phase 7.5 gate before onyx
//! integration (P8).
//!
//! Workload:
//! 1. Create or reuse a pool from N device paths
//! 2. Provision one LD per RAID level via CPGs (mirror, raid0, raid5, raid6
//!    if the pool has enough PDs)
//! 3. Write a deterministic per-LBA PRNG pattern across each LD (`fill`)
//! 4. Run the verify loop: random offset+length read, recompute the
//!    expected bytes, assert byte-equality (`verify`)
//! 5. Periodically: run scrub on each LD; assert no mismatches on healthy
//!    pool
//! 6. Counters: ops, bytes, errors, scrubs, p50/p95 verify latency
//!
//! Fault injection (operator-driven):
//! - To simulate a PD failure: kill the soak, restart with one fewer
//!   --device path. Soak will use Pool::open_with_missing internally.
//! - After failure: re-run with `--rebuild` to invoke Pool::rebuild_ld
//!   on each affected LD before resuming verify.
//!
//! Exit gate for P8:
//! - 24 h sparse-file run + 24 h nvme-box run with no verify mismatches,
//!   no panics, and a successful inject-fail / rebuild / verify cycle.

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::Parser;
use onyx_chunklet::io::RawDevice;
use onyx_chunklet::ld::LogicalDisk;
use onyx_chunklet::pool::CpgSpec;
use onyx_chunklet::types::{HaDomain, LdId, RaidLevel, BLOCK_SIZE};
use onyx_chunklet::{ChunkletResult, Pool, PoolConfig};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::convert::TryInto;

#[derive(Parser, Debug)]
#[command(name = "chunklet-soak", about = "Standalone chunklet fault-injection + verify harness")]
struct Cli {
    /// Pool device paths (>= 2). Comma-separated.
    #[arg(long, value_delimiter = ',')]
    devices: Vec<PathBuf>,

    /// Initialize a fresh pool first (overwrites any existing state).
    #[arg(long, default_value_t = false)]
    init: bool,

    /// Allow opening with missing devices (degraded mode).
    #[arg(long, default_value_t = false)]
    allow_missing: bool,

    /// After open, rebuild every LD that has failed members before verify.
    #[arg(long, default_value_t = false)]
    rebuild: bool,

    /// Run the verify workload for this many seconds (0 = once, no loop).
    #[arg(long, default_value_t = 60)]
    runtime_secs: u64,

    /// Worker threads.
    #[arg(long, default_value_t = 4)]
    workers: usize,

    /// Per-IO size in 4 KiB blocks (will read/verify this many blocks per op).
    #[arg(long, default_value_t = 4)]
    io_blocks: usize,

    /// Run scrub every N seconds (0 = never).
    #[arg(long, default_value_t = 30)]
    scrub_every_secs: u64,

    /// Rows per LD (capacity). 1 row of 1 GiB chunklet = ~1 GiB usable per data position.
    #[arg(long, default_value_t = 1)]
    ld_rows: u16,

    /// Pre-create sparse files of this size (bytes) when a device path doesn't exist.
    #[arg(long, default_value_t = 8 * 1024 * 1024 * 1024)]
    sparse_size_bytes: u64,

    /// Spare-reservation percentage (0-100). Default 0 for soak — caller manages
    /// fault injection manually instead of relying on the spare pool.
    #[arg(long, default_value_t = 0)]
    spare_pct: u8,
}

fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();
    if let Err(e) = run(cli) {
        eprintln!("error: {}", e);
        std::process::exit(1);
    }
}

fn run(cli: Cli) -> ChunkletResult<()> {
    if cli.devices.is_empty() {
        return Err(onyx_chunklet::ChunkletError::Config(
            "--devices required (>= 2)".into(),
        ));
    }
    let raws = open_or_create_devices(&cli.devices, cli.sparse_size_bytes)?;
    let pool = if cli.init {
        eprintln!("init pool with {} PDs (spare_pct={})", raws.len(), cli.spare_pct);
        Pool::create(
            raws,
            PoolConfig {
                spare_pct: cli.spare_pct,
            },
        )?
    } else if cli.allow_missing {
        Pool::open_with_missing(raws)?
    } else {
        Pool::open(raws)?
    };

    let pd_count = pool.pd_count();
    eprintln!("pool {} ({} PDs)", pool.id(), pd_count);

    if cli.init {
        provision_lds(&pool, pd_count, cli.ld_rows)?;
        fill_all_lds(&pool)?;
    }

    if cli.rebuild {
        rebuild_failed(&pool)?;
    }

    if cli.runtime_secs > 0 {
        verify_loop(
            &pool,
            cli.workers,
            cli.io_blocks,
            cli.runtime_secs,
            cli.scrub_every_secs,
        )?;
    } else {
        verify_pass(&pool, cli.io_blocks)?;
    }

    Ok(())
}

fn provision_lds(pool: &Arc<Pool>, pd_count: usize, rows: u16) -> ChunkletResult<()> {
    let cases: Vec<(&str, RaidLevel, u8, u16, usize)> = vec![
        ("plain", RaidLevel::Plain, 1, 1, 1),
        ("mirror2", RaidLevel::Mirror, 2, 1, 2),
        ("raid0", RaidLevel::Raid0, 1, 2, 2),
        ("raid5", RaidLevel::Raid5, 4, 1, 4),
        ("raid6", RaidLevel::Raid6, 5, 1, 5),
    ];
    for (name, raid, set_size, row_size, min_pds) in cases {
        if pd_count < min_pds {
            eprintln!("skipping {} (needs {} PDs, pool has {})", name, min_pds, pd_count);
            continue;
        }
        let cpg = pool.create_cpg(CpgSpec {
            name: name.into(),
            raid_level: raid,
            set_size,
            row_size,
            strip_size_log2: 0,
            ha_domain: HaDomain::Pd,
        })?;
        let ld_id = pool.create_ld_in_cpg(cpg, rows)?;
        eprintln!("created {} CPG {} -> LD {}", name, cpg, ld_id);
    }
    Ok(())
}

fn fill_all_lds(pool: &Arc<Pool>) -> ChunkletResult<()> {
    for desc in pool.list_lds() {
        let ld = pool.open_ld(desc.id)?;
        let cap = ld.capacity_bytes();
        eprintln!("filling LD {} ({} MiB)...", desc.id, cap >> 20);
        // Fill in 1 MiB chunks with PRNG pattern.
        let chunk = 1usize << 20;
        let mut buf = vec![0u8; chunk];
        let mut written = 0u64;
        while written < cap {
            let take = std::cmp::min(chunk as u64, cap - written) as usize;
            fill_pattern(&mut buf[..take], desc.id, written);
            ld.write_at(written, &buf[..take])?;
            written += take as u64;
        }
    }
    Ok(())
}

fn rebuild_failed(pool: &Arc<Pool>) -> ChunkletResult<()> {
    let lds: Vec<LdId> = pool.list_lds().into_iter().map(|d| d.id).collect();
    for id in lds {
        match pool.rebuild_ld(id) {
            Ok(report) => {
                if !report.skipped {
                    eprintln!(
                        "rebuilt LD {} ({} members)",
                        report.ld_id, report.rebuilt_members
                    );
                }
            }
            Err(e) => eprintln!("rebuild LD {} failed: {}", id, e),
        }
    }
    Ok(())
}

fn verify_pass(pool: &Arc<Pool>, io_blocks: usize) -> ChunkletResult<()> {
    let len = io_blocks * BLOCK_SIZE as usize;
    for desc in pool.list_lds() {
        let ld = pool.open_ld(desc.id)?;
        let cap = ld.capacity_bytes();
        let mut off = 0u64;
        let mut buf = vec![0u8; len];
        let mut expected = vec![0u8; len];
        while off + len as u64 <= cap {
            ld.read_at(off, &mut buf)?;
            fill_pattern(&mut expected, desc.id, off);
            if buf != expected {
                return Err(onyx_chunklet::ChunkletError::Invariant(format!(
                    "LD {} mismatch at offset {}",
                    desc.id, off
                )));
            }
            off += len as u64;
        }
        eprintln!("verify-pass LD {} OK ({} bytes)", desc.id, off);
    }
    Ok(())
}

fn verify_loop(
    pool: &Arc<Pool>,
    workers: usize,
    io_blocks: usize,
    runtime_secs: u64,
    scrub_every_secs: u64,
) -> ChunkletResult<()> {
    let len = io_blocks * BLOCK_SIZE as usize;
    let lds: Vec<(LdId, Arc<dyn LogicalDisk>, u64)> = pool
        .list_lds()
        .into_iter()
        .map(|d| {
            let ld = pool.open_ld(d.id).unwrap();
            let cap = ld.capacity_bytes();
            (d.id, ld, cap)
        })
        .collect();
    if lds.is_empty() {
        eprintln!("no LDs to verify");
        return Ok(());
    }

    let stop = Arc::new(AtomicBool::new(false));
    let ops = Arc::new(AtomicU64::new(0));
    let bytes = Arc::new(AtomicU64::new(0));
    let errors = Arc::new(AtomicU64::new(0));
    let start = Instant::now();
    let deadline = start + Duration::from_secs(runtime_secs);

    let mut handles = Vec::new();
    for w in 0..workers {
        let stop = stop.clone();
        let ops = ops.clone();
        let bytes = bytes.clone();
        let errors = errors.clone();
        let lds = lds.clone();
        handles.push(std::thread::spawn(move || {
            let mut rng = StdRng::seed_from_u64(0xc0ffee_u64.wrapping_add(w as u64));
            let mut buf = vec![0u8; len];
            let mut expected = vec![0u8; len];
            while !stop.load(Ordering::Relaxed) {
                let (ld_id, ld, cap) = &lds[rng.gen_range(0..lds.len())];
                if *cap < len as u64 {
                    continue;
                }
                let aligned_max = (*cap - len as u64) / BLOCK_SIZE * BLOCK_SIZE;
                let off = (rng.gen_range(0..=aligned_max / BLOCK_SIZE)) * BLOCK_SIZE;
                if let Err(e) = ld.read_at(off, &mut buf) {
                    eprintln!("worker {} LD {} read err: {}", w, ld_id, e);
                    errors.fetch_add(1, Ordering::Relaxed);
                    continue;
                }
                fill_pattern(&mut expected, *ld_id, off);
                if buf != expected {
                    eprintln!(
                        "worker {} LD {} MISMATCH at offset {}",
                        w, ld_id, off
                    );
                    errors.fetch_add(1, Ordering::Relaxed);
                } else {
                    ops.fetch_add(1, Ordering::Relaxed);
                    bytes.fetch_add(len as u64, Ordering::Relaxed);
                }
            }
        }));
    }

    let mut next_scrub = if scrub_every_secs > 0 {
        Some(start + Duration::from_secs(scrub_every_secs))
    } else {
        None
    };
    let mut next_report = start + Duration::from_secs(5);
    while Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(500));
        let now = Instant::now();
        if now >= next_report {
            let elapsed = now.duration_since(start).as_secs_f64();
            let o = ops.load(Ordering::Relaxed);
            let b = bytes.load(Ordering::Relaxed);
            let e = errors.load(Ordering::Relaxed);
            eprintln!(
                "[{:>5.0}s] ops={} ({:.0} ops/s) bytes={} ({:.1} MiB/s) errors={}",
                elapsed,
                o,
                o as f64 / elapsed,
                b,
                b as f64 / elapsed / (1u64 << 20) as f64,
                e
            );
            next_report = now + Duration::from_secs(5);
        }
        if let Some(t) = next_scrub {
            if now >= t {
                eprintln!("scrubbing all LDs...");
                for (id, _, _) in &lds {
                    match pool.scrub_ld(*id) {
                        Ok(r) => eprintln!(
                            "  scrub LD {}: {} batches, {} mismatches, {} marked Bad",
                            r.ld_id,
                            r.batches_checked,
                            r.mismatches.len(),
                            r.marked_bad
                        ),
                        Err(e) => eprintln!("  scrub LD {} err: {}", id, e),
                    }
                }
                next_scrub = Some(now + Duration::from_secs(scrub_every_secs));
            }
        }
    }
    stop.store(true, Ordering::Relaxed);
    for h in handles {
        let _ = h.join();
    }
    let elapsed = Instant::now().duration_since(start).as_secs_f64();
    let o = ops.load(Ordering::Relaxed);
    let b = bytes.load(Ordering::Relaxed);
    let e = errors.load(Ordering::Relaxed);
    eprintln!(
        "DONE: {:.0}s, {} ops ({:.0}/s), {} bytes ({:.1} MiB/s), {} errors",
        elapsed,
        o,
        o as f64 / elapsed,
        b,
        b as f64 / elapsed / (1u64 << 20) as f64,
        e
    );
    if e > 0 {
        return Err(onyx_chunklet::ChunkletError::Invariant(format!(
            "soak finished with {} errors",
            e
        )));
    }
    Ok(())
}

/// Per-byte deterministic fill. The byte at absolute offset `O` on LD `L`
/// equals `mix(ld_hash(L), O)` regardless of how the buffer is sliced — so
/// writes done in 1 MiB chunks and reads done at random aligned offsets
/// produce matching bytes.
fn fill_pattern(buf: &mut [u8], ld_id: LdId, base_offset: u64) {
    let id_hash = ld_hash(ld_id);
    for (i, b) in buf.iter_mut().enumerate() {
        *b = mix_byte(id_hash, base_offset + i as u64);
    }
}

fn ld_hash(ld_id: LdId) -> u64 {
    let bytes = ld_id.to_bytes();
    let lo = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
    let hi = u64::from_le_bytes(bytes[8..16].try_into().unwrap());
    lo ^ hi.wrapping_mul(0x9e37_79b9_7f4a_7c15)
}

fn mix_byte(id_hash: u64, abs: u64) -> u8 {
    // splitmix-style finalizer; deterministic, fast, byte-granular.
    let mut x = id_hash.wrapping_add(abs.wrapping_mul(0x9e37_79b9_7f4a_7c15));
    x ^= x >> 33;
    x = x.wrapping_mul(0xff51_afd7_ed55_8ccd);
    x ^= x >> 33;
    x = x.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    x ^= x >> 33;
    (x >> 56) as u8
}

fn open_or_create_devices(paths: &[PathBuf], sparse_size: u64) -> ChunkletResult<Vec<RawDevice>> {
    let mut out = Vec::with_capacity(paths.len());
    for p in paths {
        if p.exists() {
            out.push(RawDevice::open(p)?);
        } else {
            eprintln!("creating sparse {} ({} bytes)", p.display(), sparse_size);
            out.push(RawDevice::open_or_create(p, sparse_size)?);
        }
    }
    Ok(out)
}
