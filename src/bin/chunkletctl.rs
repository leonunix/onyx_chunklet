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
use onyx_chunklet::pool::{CpgSpec, LdSpec};
use onyx_chunklet::superblock::{SuperblockSlot, SLOT_BYTES};
use onyx_chunklet::types::{
    HaDomain, LdId, PdId, RaidLevel, PD_RESERVED_BYTES, SUPERBLOCK_SLOT_A_OFFSET,
    SUPERBLOCK_SLOT_B_OFFSET,
};
use onyx_chunklet::{ChunkletResult, CpgId, Pool, PoolConfig};

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
    /// Logical disk operations (create / list / drop).
    Ld(LdCmd),
    /// Common Provisioning Group operations (declarative LD policy templates).
    Cpg(CpgCmd),
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
    /// Drain a PD (migrate all LD members onto other PDs, then mark DRAINED).
    Drain {
        #[arg(long, required = true, value_delimiter = ',')]
        pool: Vec<PathBuf>,
        /// PD uuid to drain.
        pd_id: String,
    },
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

#[derive(Parser, Debug)]
struct LdCmd {
    #[command(subcommand)]
    op: LdOp,
}

#[derive(Parser, Debug)]
struct CpgCmd {
    #[command(subcommand)]
    op: CpgOp,
}

#[derive(Subcommand, Debug)]
enum CpgOp {
    /// Create a new CPG.
    Create {
        #[arg(long, required = true, value_delimiter = ',')]
        pool: Vec<PathBuf>,
        #[arg(long)]
        name: String,
        /// raid level: plain | mirror | raid0 | raid5 | raid6
        #[arg(long)]
        raid: String,
        /// set_size (mirror copies, raid5 K+1, raid6 K+2, etc.)
        #[arg(long)]
        set_size: u8,
        #[arg(long, default_value_t = 1)]
        row_size: u16,
        #[arg(long, default_value_t = 0)]
        strip_log2: u8,
    },
    /// List all CPGs.
    List {
        #[arg(long, required = true, value_delimiter = ',')]
        pool: Vec<PathBuf>,
    },
    /// Drop a CPG by uuid.
    Drop {
        #[arg(long, required = true, value_delimiter = ',')]
        pool: Vec<PathBuf>,
        cpg_id: String,
    },
    /// Create an LD using a CPG's policy.
    CreateLd {
        #[arg(long, required = true, value_delimiter = ',')]
        pool: Vec<PathBuf>,
        cpg_id: String,
        #[arg(long, default_value_t = 1)]
        rows: u16,
    },
}

#[derive(Subcommand, Debug)]
enum LdOp {
    /// Create a new Plain LD on the pool.
    CreatePlain {
        /// Devices in the pool (comma-separated).
        #[arg(long, required = true, value_delimiter = ',')]
        pool: Vec<PathBuf>,
        /// Number of chunklets to claim.
        #[arg(long)]
        chunklets: u16,
    },
    /// Create a RAID-0 LD (striping, no redundancy).
    CreateRaid0 {
        #[arg(long, required = true, value_delimiter = ',')]
        pool: Vec<PathBuf>,
        /// Stripe width (chunklets per row).
        #[arg(long)]
        stripe_width: u16,
        #[arg(long, default_value_t = 1)]
        rows: u16,
        #[arg(long, default_value_t = 0)]
        strip_log2: u8,
    },
    /// Create a RAID-6 LD (data chunklets per set + P + Q).
    CreateRaid6 {
        #[arg(long, required = true, value_delimiter = ',')]
        pool: Vec<PathBuf>,
        /// Data chunklets per set (K). Set size will be K + 2.
        #[arg(long)]
        data_per_set: u8,
        #[arg(long, default_value_t = 1)]
        row_size: u16,
        #[arg(long, default_value_t = 1)]
        rows: u16,
        #[arg(long, default_value_t = 0)]
        strip_log2: u8,
    },
    /// Create a RAID-5 LD (data chunklets per set + 1 parity).
    CreateRaid5 {
        #[arg(long, required = true, value_delimiter = ',')]
        pool: Vec<PathBuf>,
        /// Data chunklets per set (K). Set size will be K + 1.
        #[arg(long)]
        data_per_set: u8,
        #[arg(long, default_value_t = 1)]
        row_size: u16,
        #[arg(long, default_value_t = 1)]
        rows: u16,
        #[arg(long, default_value_t = 0)]
        strip_log2: u8,
    },
    /// Create a Mirror LD (RAID-1 with copies=N, row=1; RAID-10 with row>1).
    CreateMirror {
        #[arg(long, required = true, value_delimiter = ',')]
        pool: Vec<PathBuf>,
        /// Mirror copies per set (>= 2).
        #[arg(long, default_value_t = 2)]
        copies: u8,
        /// Number of mirror sets striped within one row.
        #[arg(long, default_value_t = 1)]
        row_size: u16,
        /// Number of stripe rows (capacity multiplier).
        #[arg(long, default_value_t = 1)]
        rows: u16,
        /// Strip size as log2 bytes; 0 = 4 KiB block.
        #[arg(long, default_value_t = 0)]
        strip_log2: u8,
    },
    /// List all LDs on the pool.
    List {
        #[arg(long, required = true, value_delimiter = ',')]
        pool: Vec<PathBuf>,
    },
    /// Drop an LD from the pool by uuid.
    Drop {
        #[arg(long, required = true, value_delimiter = ',')]
        pool: Vec<PathBuf>,
        /// LD uuid.
        ld_id: String,
    },
    /// Run a scrub pass: verify parity / mirror copies, mark culprit
    /// chunklets Bad. Run `rebuild` afterwards to swap them onto fresh
    /// chunklets.
    Scrub {
        #[arg(long, required = true, value_delimiter = ',')]
        pool: Vec<PathBuf>,
        ld_id: String,
    },
    /// Rebuild an LD's failed members onto live PDs. Open the pool with
    /// `--allow-missing` to enter degraded mode first.
    Rebuild {
        #[arg(long, required = true, value_delimiter = ',')]
        pool: Vec<PathBuf>,
        /// LD uuid to rebuild.
        ld_id: String,
        /// Allow opening the pool with missing devices (= degraded).
        #[arg(long, default_value_t = false)]
        allow_missing: bool,
    },
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
        Command::Ld(p) => run_ld(p),
        Command::Cpg(p) => run_cpg(p),
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
        PoolOp::Drain { pool, pd_id } => {
            let parsed = uuid::Uuid::parse_str(&pd_id)
                .map_err(|e| onyx_chunklet::ChunkletError::Config(format!("bad uuid: {}", e)))?;
            let id = PdId::from_bytes(*parsed.as_bytes());
            let raws = open_devices(&pool)?;
            let pool = Pool::open(raws)?;
            let report = pool.drain_pd(id)?;
            println!(
                "drained PD {}: {} LDs affected, {} members migrated",
                report.pd_id,
                report.lds_affected.len(),
                report.members_migrated
            );
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

fn run_ld(cmd: LdCmd) -> ChunkletResult<()> {
    match cmd.op {
        LdOp::CreatePlain { pool, chunklets } => {
            let raws = open_devices(&pool)?;
            let pool = Pool::open(raws)?;
            let id = pool.create_ld(LdSpec::plain(chunklets))?;
            println!("created LD {} (Plain, {} chunklets)", id, chunklets);
            print_ld_table(&pool);
            Ok(())
        }
        LdOp::CreateRaid0 {
            pool,
            stripe_width,
            rows,
            strip_log2,
        } => {
            let raws = open_devices(&pool)?;
            let pool = Pool::open(raws)?;
            let id = pool.create_ld(LdSpec::raid0(stripe_width, rows, strip_log2))?;
            println!(
                "created LD {} (Raid0, stripe_width={} rows={} strip_log2={})",
                id, stripe_width, rows, strip_log2
            );
            print_ld_table(&pool);
            Ok(())
        }
        LdOp::CreateRaid6 {
            pool,
            data_per_set,
            row_size,
            rows,
            strip_log2,
        } => {
            let raws = open_devices(&pool)?;
            let pool = Pool::open(raws)?;
            let id = pool.create_ld(LdSpec::raid6(data_per_set, row_size, rows, strip_log2))?;
            println!(
                "created LD {} (Raid6, K={} +P+Q, row_size={} rows={} strip_log2={})",
                id, data_per_set, row_size, rows, strip_log2
            );
            print_ld_table(&pool);
            Ok(())
        }
        LdOp::CreateRaid5 {
            pool,
            data_per_set,
            row_size,
            rows,
            strip_log2,
        } => {
            let raws = open_devices(&pool)?;
            let pool = Pool::open(raws)?;
            let id = pool.create_ld(LdSpec::raid5(data_per_set, row_size, rows, strip_log2))?;
            println!(
                "created LD {} (Raid5, K={} +1 parity, row_size={} rows={} strip_log2={})",
                id, data_per_set, row_size, rows, strip_log2
            );
            print_ld_table(&pool);
            Ok(())
        }
        LdOp::CreateMirror {
            pool,
            copies,
            row_size,
            rows,
            strip_log2,
        } => {
            let raws = open_devices(&pool)?;
            let pool = Pool::open(raws)?;
            let id = pool.create_ld(LdSpec::mirror(copies, row_size, rows, strip_log2))?;
            println!(
                "created LD {} (Mirror, copies={} row_size={} rows={} strip_log2={})",
                id, copies, row_size, rows, strip_log2
            );
            print_ld_table(&pool);
            Ok(())
        }
        LdOp::List { pool } => {
            let raws = open_devices(&pool)?;
            let pool = Pool::open(raws)?;
            print_ld_table(&pool);
            Ok(())
        }
        LdOp::Drop { pool, ld_id } => {
            let parsed = uuid::Uuid::parse_str(&ld_id)
                .map_err(|e| onyx_chunklet::ChunkletError::Config(format!("bad uuid: {}", e)))?;
            let id = LdId::from_bytes(*parsed.as_bytes());
            let raws = open_devices(&pool)?;
            let pool = Pool::open(raws)?;
            pool.drop_ld(id)?;
            println!("dropped LD {}", id);
            print_ld_table(&pool);
            Ok(())
        }
        LdOp::Scrub { pool, ld_id } => {
            let parsed = uuid::Uuid::parse_str(&ld_id)
                .map_err(|e| onyx_chunklet::ChunkletError::Config(format!("bad uuid: {}", e)))?;
            let id = LdId::from_bytes(*parsed.as_bytes());
            let raws = open_devices(&pool)?;
            let pool = Pool::open(raws)?;
            let report = pool.scrub_ld(id)?;
            println!(
                "scrub of LD {}: {} batches checked, {} mismatches, {} chunklets marked Bad",
                id,
                report.batches_checked,
                report.mismatches.len(),
                report.marked_bad
            );
            for mm in &report.mismatches {
                println!(
                    "  set={} batch_off={} kind={:?}",
                    mm.set_idx, mm.batch_offset, mm.kind
                );
            }
            Ok(())
        }
        LdOp::Rebuild {
            pool,
            ld_id,
            allow_missing,
        } => {
            let parsed = uuid::Uuid::parse_str(&ld_id)
                .map_err(|e| onyx_chunklet::ChunkletError::Config(format!("bad uuid: {}", e)))?;
            let id = LdId::from_bytes(*parsed.as_bytes());
            let raws = open_devices(&pool)?;
            let pool = if allow_missing {
                Pool::open_with_missing(raws)?
            } else {
                Pool::open(raws)?
            };
            let report = pool.rebuild_ld(id)?;
            if report.skipped {
                println!("LD {} has no failed members; rebuild skipped", id);
            } else {
                println!(
                    "rebuilt {} failed members of LD {}",
                    report.rebuilt_members, id
                );
            }
            print_ld_table(&pool);
            Ok(())
        }
    }
}

fn run_cpg(cmd: CpgCmd) -> ChunkletResult<()> {
    match cmd.op {
        CpgOp::Create {
            pool,
            name,
            raid,
            set_size,
            row_size,
            strip_log2,
        } => {
            let raid_level = match raid.as_str() {
                "plain" => RaidLevel::Plain,
                "mirror" => RaidLevel::Mirror,
                "raid0" => RaidLevel::Raid0,
                "raid5" => RaidLevel::Raid5,
                "raid6" => RaidLevel::Raid6,
                other => {
                    return Err(onyx_chunklet::ChunkletError::Config(format!(
                        "unknown raid level '{}'; expected plain|mirror|raid0|raid5|raid6",
                        other
                    )))
                }
            };
            let raws = open_devices(&pool)?;
            let pool = Pool::open(raws)?;
            let id = pool.create_cpg(CpgSpec {
                name: name.clone(),
                raid_level,
                set_size,
                row_size,
                strip_size_log2: strip_log2,
                ha_domain: HaDomain::Pd,
            })?;
            println!("created CPG {} ({})", id, name);
            Ok(())
        }
        CpgOp::List { pool } => {
            let raws = open_devices(&pool)?;
            let pool = Pool::open(raws)?;
            let cpgs = pool.list_cpgs();
            if cpgs.is_empty() {
                println!("no CPGs");
                return Ok(());
            }
            println!("CPGs ({}):", cpgs.len());
            for c in cpgs {
                println!(
                    "  id={} name={} raid={:?} set={} row={} strip_log2={}",
                    c.id, c.name, c.raid_level, c.set_size, c.row_size, c.strip_size_log2
                );
            }
            Ok(())
        }
        CpgOp::Drop { pool, cpg_id } => {
            let parsed = uuid::Uuid::parse_str(&cpg_id)
                .map_err(|e| onyx_chunklet::ChunkletError::Config(format!("bad uuid: {}", e)))?;
            let id = CpgId::from_bytes(*parsed.as_bytes());
            let raws = open_devices(&pool)?;
            let pool = Pool::open(raws)?;
            pool.drop_cpg(id)?;
            println!("dropped CPG {}", id);
            Ok(())
        }
        CpgOp::CreateLd {
            pool,
            cpg_id,
            rows,
        } => {
            let parsed = uuid::Uuid::parse_str(&cpg_id)
                .map_err(|e| onyx_chunklet::ChunkletError::Config(format!("bad uuid: {}", e)))?;
            let id = CpgId::from_bytes(*parsed.as_bytes());
            let raws = open_devices(&pool)?;
            let pool = Pool::open(raws)?;
            let ld_id = pool.create_ld_in_cpg(id, rows)?;
            println!("created LD {} in CPG {} ({} rows)", ld_id, id, rows);
            print_ld_table(&pool);
            Ok(())
        }
    }
}

fn print_ld_table(pool: &Pool) {
    let lds = pool.list_lds();
    if lds.is_empty() {
        println!("no LDs");
        return;
    }
    println!("LDs ({}):", lds.len());
    for d in lds {
        println!(
            "  id={} raid={:?} set={} row={} rows={} members={}",
            d.id,
            d.raid_level,
            d.set_size,
            d.row_size,
            d.num_rows,
            d.members.len()
        );
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
