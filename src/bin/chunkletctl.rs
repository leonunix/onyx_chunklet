//! `chunkletctl` — operator CLI for inspecting and managing chunklet pools.
//!
//! P0 subcommands:
//!   - `pool init <dev...>`    — create a fresh pool from blank devices
//!   - `pool open <dev...>`    — open + validate an existing pool
//!   - `pool list <dev...>`    — list PDs in a pool (alias for open + print)
//!   - `pool admit --pool <existing-dev...> <new-dev>` — extend a pool
//!   - `pd scan <dev>`         — scan all 4 superblock slots, print decoded info

use std::path::PathBuf;

use clap::{Parser, Subcommand};
use onyx_chunklet::io::{AlignedBuf, RawDevice};
use onyx_chunklet::superblock::{SuperblockSlot, SLOT_BYTES};
use onyx_chunklet::types::{
    PD_RESERVED_BYTES, SUPERBLOCK_SLOT_A_OFFSET, SUPERBLOCK_SLOT_B_OFFSET,
};
use onyx_chunklet::{ChunkletResult, Pool, PoolConfig};

#[derive(Parser, Debug)]
#[command(name = "chunkletctl", version, about = "onyx-chunklet operator CLI")]
struct Cli {
    #[command(subcommand)]
    cmd: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Pool-level operations.
    Pool(PoolCmd),
    /// Per-PD low-level operations (scan slots, etc.).
    Pd(PdCmd),
}

#[derive(Parser, Debug)]
struct PoolCmd {
    #[command(subcommand)]
    op: PoolOp,
}

#[derive(Subcommand, Debug)]
enum PoolOp {
    /// Initialize a fresh pool from blank devices. **Wipes existing
    /// superblocks** on the listed devices.
    Init {
        /// Spare percentage (0-100). Default 5.
        #[arg(long, default_value_t = 5)]
        spare_pct: u8,
        /// Devices to admit into the new pool.
        devices: Vec<PathBuf>,
    },
    /// Open and validate an existing pool. Prints PD list on success.
    Open { devices: Vec<PathBuf> },
    /// Alias for `open`.
    List { devices: Vec<PathBuf> },
    /// Add a new blank device to an existing pool.
    Admit {
        /// Devices already in the pool (comma-separated).
        #[arg(long, required = true, value_delimiter = ',')]
        pool: Vec<PathBuf>,
        /// Spare percentage to use for the new PD. Default 5.
        #[arg(long, default_value_t = 5)]
        spare_pct: u8,
        /// New device to admit.
        device: PathBuf,
    },
}

#[derive(Parser, Debug)]
struct PdCmd {
    #[command(subcommand)]
    op: PdOp,
}

#[derive(Subcommand, Debug)]
enum PdOp {
    /// Decode all 4 superblock slots on a single PD and print their state.
    Scan { device: PathBuf },
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
    match cli.cmd {
        Command::Pool(p) => run_pool(p),
        Command::Pd(p) => run_pd(p),
    }
}

fn run_pool(cmd: PoolCmd) -> ChunkletResult<()> {
    match cmd.op {
        PoolOp::Init {
            spare_pct,
            devices,
        } => {
            if devices.is_empty() {
                return Err(onyx_chunklet::ChunkletError::Config(
                    "init requires at least one device".into(),
                ));
            }
            let raws = open_or_create_devices(&devices)?;
            let pool = Pool::create(raws, PoolConfig { spare_pct })?;
            println!("created pool: {}", pool.id());
            println!("PDs:");
            for info in pool.list_pds() {
                print_pd_line(&info);
            }
            Ok(())
        }
        PoolOp::Open { devices } | PoolOp::List { devices } => {
            let raws = open_devices(&devices)?;
            let pool = Pool::open(raws)?;
            println!("pool: {} ({} PDs)", pool.id(), pool.pd_count());
            for info in pool.list_pds() {
                print_pd_line(&info);
            }
            Ok(())
        }
        PoolOp::Admit {
            pool: pool_paths,
            spare_pct,
            device,
        } => {
            let raws = open_devices(&pool_paths)?;
            let pool = Pool::open(raws)?;
            let new_raw = open_or_create_one(&device)?;
            let new_id = pool.admit(new_raw, PoolConfig { spare_pct })?;
            println!("admitted {} into pool {}", new_id, pool.id());
            for info in pool.list_pds() {
                print_pd_line(&info);
            }
            Ok(())
        }
    }
}

fn run_pd(cmd: PdCmd) -> ChunkletResult<()> {
    match cmd.op {
        PdOp::Scan { device } => {
            let raw = RawDevice::open(&device)?;
            scan_pd_slots(&raw)
        }
    }
}

fn scan_pd_slots(raw: &RawDevice) -> ChunkletResult<()> {
    let pd_size = raw.size();
    println!(
        "device: {}\n  size: {} bytes ({:.2} GiB)",
        raw.path().display(),
        pd_size,
        pd_size as f64 / (1u64 << 30) as f64
    );
    let head_base: u64 = 0;
    let tail_base = pd_size - PD_RESERVED_BYTES;
    let slots = [
        ("head A", head_base + SUPERBLOCK_SLOT_A_OFFSET),
        ("head B", head_base + SUPERBLOCK_SLOT_B_OFFSET),
        ("tail A", tail_base + SUPERBLOCK_SLOT_A_OFFSET),
        ("tail B", tail_base + SUPERBLOCK_SLOT_B_OFFSET),
    ];
    for (label, offset) in slots {
        let mut buf = AlignedBuf::new(SLOT_BYTES)?;
        match raw.read_at(buf.as_mut_slice(), offset) {
            Err(e) => println!("  {} @ {}: read error: {}", label, offset, e),
            Ok(()) => match SuperblockSlot::decode(buf.as_slice()) {
                Ok(slot) => println!(
                    "  {} @ {}: pool={} pd={} gen={} chunklets={} pd_count={}",
                    label,
                    offset,
                    slot.pool_id,
                    slot.pd_id,
                    slot.manifest_gen,
                    slot.body.total_chunklets,
                    slot.body.pool_pd_count
                ),
                Err(e) => println!("  {} @ {}: decode error: {}", label, offset, e),
            },
        }
    }
    Ok(())
}

fn open_devices(paths: &[PathBuf]) -> ChunkletResult<Vec<RawDevice>> {
    let mut out = Vec::with_capacity(paths.len());
    for p in paths {
        out.push(RawDevice::open(p)?);
    }
    Ok(out)
}

fn open_or_create_devices(paths: &[PathBuf]) -> ChunkletResult<Vec<RawDevice>> {
    paths.iter().map(open_or_create_one).collect()
}

/// For ergonomic test / dev usage: if a path doesn't exist, create a sparse
/// file of `CHUNKLET_SOAK_PD_SIZE` (default 8 GiB) under that path.
fn open_or_create_one(path: &PathBuf) -> ChunkletResult<RawDevice> {
    if path.exists() {
        return RawDevice::open(path);
    }
    let size_bytes = std::env::var("CHUNKLET_SOAK_PD_SIZE")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(8 * 1024 * 1024 * 1024);
    eprintln!(
        "note: {} does not exist, creating sparse file of {} bytes",
        path.display(),
        size_bytes
    );
    RawDevice::open_or_create(path, size_bytes)
}

fn print_pd_line(info: &onyx_chunklet::pd::PdInfo) {
    println!(
        "  seq={:>3} pd={} gen={} chunklets={} size={} path={}",
        info.pd_seq_in_pool,
        info.pd_id,
        info.manifest_gen,
        info.total_chunklets,
        info.size_bytes,
        info.path.display()
    );
}
