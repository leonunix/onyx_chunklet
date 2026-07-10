//! Standalone reopen-integrity harness for a raid6 LD.
//!
//! Reproduces onyx's exact LV3 geometry (raid6 6 data + 2 parity, num_rows=12,
//! 4 KiB strip -> ~72 GiB) on REAL PDs and hammers the chunklet-only failure
//! modes onyx exercises, then reopens the pool in a FRESH PROCESS (a true
//! restart) and verifies every block round-trips.
//!
//! Every block is tagged with (block_index, version) in its first 12 bytes, so
//! a post-reopen read that resolves to the wrong physical chunklet OR returns a
//! stale prior version is caught AND localized. Stale-version resurface after
//! reopen is exactly onyx's foreground-CRC signature.
//!
//! Phases (separate process invocations = genuine restarts):
//!   chunklet_reopen fill   --devices <9 PDs> [--data 6 --rows 12 ...]  # version 0 everywhere
//!   chunklet_reopen churn  --devices <9 PDs> --region-gib R --passes P --threads T
//!         # P passes of CONCURRENT single-block partial-RMW overwrites over the
//!         # first R GiB; round-robin block->thread so different threads RMW
//!         # different strips of the SAME stripe at once. Region ends at version P.
//!   chunklet_reopen verify --devices <9 PDs> [--region-gib R --version P]
//!         # blocks < region expect version P, the rest expect version 0.
//! Equal fill/verify crc + 0 mismatches = PASS.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use clap::{Parser, Subcommand};
use onyx_chunklet::io::RawDevice;
use onyx_chunklet::pool::LdSpec;
use onyx_chunklet::{ChunkletError, ChunkletResult, LogicalDisk, Pool, PoolConfig};

#[derive(Parser)]
#[command(
    name = "chunklet_reopen",
    about = "Fill/churn a raid6 LD, restart (reopen in a fresh process), verify every block round-trips."
)]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Create a FRESH pool, create a raid6 LD, fill every block at version 0.
    Fill {
        #[arg(long, num_args = 1.., required = true)]
        devices: Vec<PathBuf>,
        #[arg(long, default_value_t = 6)]
        data: u8,
        #[arg(long, default_value_t = 1)]
        row_size: u16,
        #[arg(long, default_value_t = 12)]
        rows: u16,
        #[arg(long, default_value_t = 12)]
        strip_log2: u8,
        #[arg(long, default_value_t = 0)]
        pd_size_gib: u64,
    },
    /// Open an existing pool and hammer the first `region-gib` GiB with `passes`
    /// rounds of concurrent single-block partial-RMW overwrites (version = pass).
    Churn {
        #[arg(long, num_args = 1.., required = true)]
        devices: Vec<PathBuf>,
        #[arg(long, default_value_t = 2)]
        region_gib: u64,
        #[arg(long, default_value_t = 10)]
        passes: u32,
        #[arg(long, default_value_t = 32)]
        threads: u32,
        #[arg(long, default_value_t = 0)]
        pd_size_gib: u64,
    },
    /// Reopen the pool (fresh process = restart) and verify every block.
    Verify {
        #[arg(long, num_args = 1.., required = true)]
        devices: Vec<PathBuf>,
        /// First this-many GiB were churned to `--version`; the rest stay at 0.
        #[arg(long, default_value_t = 0)]
        region_gib: u64,
        #[arg(long, default_value_t = 0)]
        version: u32,
        #[arg(long, default_value_t = 0)]
        pd_size_gib: u64,
    },
}

const CHUNK_BYTES: usize = 6 * 1024 * 1024;

fn fill_block(b: u64, v: u32, out: &mut [u8]) {
    out[0..8].copy_from_slice(&b.to_le_bytes());
    out[8..12].copy_from_slice(&v.to_le_bytes());
    let seed = b
        .wrapping_mul(0x9E37_79B9_7F4A_7C15)
        .wrapping_add(v as u64 * 0x1_0000_0001)
        .wrapping_add(0xABCD);
    for (i, x) in out.iter_mut().enumerate().skip(12) {
        *x = ((seed >> ((i % 8) * 8)) as u8) ^ (i as u8);
    }
}

fn open_raws(devices: &[PathBuf], pd_size_gib: u64) -> ChunkletResult<Vec<RawDevice>> {
    devices
        .iter()
        .map(|p| {
            if pd_size_gib > 0 {
                RawDevice::open_or_create(p, pd_size_gib * (1 << 30))
            } else {
                RawDevice::open(p)
            }
        })
        .collect()
}

fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .init();
    if let Err(e) = run() {
        eprintln!("error: {e}");
        std::process::exit(1);
    }
}

fn run() -> ChunkletResult<()> {
    match Cli::parse().cmd {
        Cmd::Fill {
            devices,
            data,
            row_size,
            rows,
            strip_log2,
            pd_size_gib,
        } => {
            let raws = open_raws(&devices, pd_size_gib)?;
            let pool = Pool::create(
                raws,
                PoolConfig {
                    spare_pct: 0,
                    ..Default::default()
                },
            )?;
            let id = pool.create_ld(LdSpec::raid6(data, row_size, rows, strip_log2))?;
            let ld = pool.open_ld(id)?;
            let bs = ld.block_size();
            let cap = ld.capacity_bytes();
            let nblocks = cap / bs as u64;
            println!(
                "FILL ld={id} raid6 data={data} rows={rows} cap={cap} bytes ({:.2} GiB) blocks={nblocks}",
                cap as f64 / (1u64 << 30) as f64
            );
            let chunk_blocks = (CHUNK_BYTES / bs) as u64;
            let mut buf = vec![0u8; CHUNK_BYTES];
            let mut crc: u32 = 0;
            let t = Instant::now();
            let mut b0 = 0u64;
            while b0 < nblocks {
                let n = chunk_blocks.min(nblocks - b0) as usize;
                for j in 0..n {
                    fill_block(b0 + j as u64, 0, &mut buf[j * bs..(j + 1) * bs]);
                }
                ld.write_at(b0 * bs as u64, &buf[..n * bs])?;
                crc = crc32c::crc32c_append(crc, &buf[..n * bs]);
                b0 += n as u64;
                if b0 % (chunk_blocks * 64) == 0 {
                    eprint!(
                        "\r  wrote {:.1}/{:.1} GiB",
                        (b0 * bs as u64) as f64 / 1e9,
                        cap as f64 / 1e9
                    );
                }
            }
            ld.flush()?;
            drop(pool);
            eprintln!();
            println!(
                "FILL DONE crc32c={crc:#010x} elapsed={:.1}s",
                t.elapsed().as_secs_f64()
            );
        }
        Cmd::Churn {
            devices,
            region_gib,
            passes,
            threads,
            pd_size_gib,
        } => {
            let raws = open_raws(&devices, pd_size_gib)?;
            let pool = Pool::open(raws)?;
            let desc = pool
                .list_lds()
                .into_iter()
                .next()
                .ok_or_else(|| ChunkletError::Config("no LD in pool".into()))?;
            let ld = pool.open_ld(desc.id)?;
            let bs = ld.block_size();
            let cap = ld.capacity_bytes();
            let nblocks = cap / bs as u64;
            let region_blocks = ((region_gib << 30) / bs as u64).min(nblocks);
            println!(
                "CHURN region={region_gib} GiB ({region_blocks} blocks) passes={passes} threads={threads} \
                 [concurrent single-block partial-RMW, round-robin block->thread]"
            );
            let t = Instant::now();
            for pass in 1..=passes {
                let done = Arc::new(AtomicU64::new(0));
                std::thread::scope(|s| {
                    for tid in 0..threads {
                        let ld = Arc::clone(&ld);
                        let done = Arc::clone(&done);
                        s.spawn(move || {
                            let mut blk = vec![0u8; bs];
                            let mut b = tid as u64;
                            while b < region_blocks {
                                fill_block(b, pass, &mut blk);
                                if let Err(e) = ld.write_at(b * bs as u64, &blk) {
                                    eprintln!("churn write err b={b}: {e}");
                                }
                                done.fetch_add(1, Ordering::Relaxed);
                                b += threads as u64;
                            }
                        });
                    }
                });
                eprintln!(
                    "  pass {pass}/{passes} done ({} block-writes, {:.0}s elapsed)",
                    done.load(Ordering::Relaxed),
                    t.elapsed().as_secs_f64()
                );
            }
            ld.flush()?;
            drop(pool);
            println!(
                "CHURN DONE region ends at version {passes}, elapsed={:.1}s",
                t.elapsed().as_secs_f64()
            );
        }
        Cmd::Verify {
            devices,
            region_gib,
            version,
            pd_size_gib,
        } => {
            let raws = open_raws(&devices, pd_size_gib)?;
            let pool = Pool::open(raws)?;
            let desc = pool
                .list_lds()
                .into_iter()
                .next()
                .ok_or_else(|| ChunkletError::Config("no LD in reopened pool".into()))?;
            let ld = pool.open_ld(desc.id)?;
            let bs = ld.block_size();
            let cap = ld.capacity_bytes();
            let nblocks = cap / bs as u64;
            let region_blocks = ((region_gib << 30) / bs as u64).min(nblocks);
            println!(
                "VERIFY ld={} cap ({:.2} GiB) blocks={nblocks} region={region_blocks} version_in_region={version}",
                desc.id,
                cap as f64 / (1u64 << 30) as f64
            );
            let chunk_blocks = (CHUNK_BYTES / bs) as u64;
            let mut buf = vec![0u8; CHUNK_BYTES];
            let mut expect = vec![0u8; bs];
            let mut crc: u32 = 0;
            let mut mism: u64 = 0;
            let mut first: Vec<String> = Vec::new();
            let t = Instant::now();
            let mut b0 = 0u64;
            while b0 < nblocks {
                let n = chunk_blocks.min(nblocks - b0) as usize;
                ld.read_at(b0 * bs as u64, &mut buf[..n * bs])?;
                crc = crc32c::crc32c_append(crc, &buf[..n * bs]);
                for j in 0..n {
                    let b = b0 + j as u64;
                    let want_v = if b < region_blocks { version } else { 0 };
                    let blk = &buf[j * bs..(j + 1) * bs];
                    fill_block(b, want_v, &mut expect);
                    if blk != expect.as_slice() {
                        mism += 1;
                        if first.len() < 30 {
                            let gb = u64::from_le_bytes(blk[0..8].try_into().unwrap());
                            let gv = u32::from_le_bytes(blk[8..12].try_into().unwrap());
                            first.push(format!("block {b} want(v{want_v}) got(block={gb},v{gv})"));
                        }
                    }
                }
                b0 += n as u64;
                if b0 % (chunk_blocks * 64) == 0 {
                    eprint!(
                        "\r  read {:.1}/{:.1} GiB, {mism} mism",
                        (b0 * bs as u64) as f64 / 1e9,
                        cap as f64 / 1e9
                    );
                }
            }
            eprintln!();
            println!(
                "VERIFY crc32c={crc:#010x} mismatches={mism}/{nblocks} elapsed={:.1}s",
                t.elapsed().as_secs_f64()
            );
            for s in &first {
                println!("  MISMATCH {s}");
            }
            if mism == 0 {
                println!("RESULT: PASS (all {nblocks} blocks round-tripped across reopen)");
            } else {
                println!("RESULT: FAIL ({mism} blocks corrupted after reopen)");
                std::process::exit(2);
            }
        }
    }
    Ok(())
}
