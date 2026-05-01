//! `chunklet-perf` - focused performance harness for one LD.

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::{Parser, ValueEnum};
use onyx_chunklet::io::{IoBackendKind, RawDevice};
use onyx_chunklet::ld::LogicalDisk;
use onyx_chunklet::pool::LdSpec;
use onyx_chunklet::types::{LdId, BLOCK_SIZE};
use onyx_chunklet::{ChunkletResult, Pool, PoolConfig};
use rand::rngs::StdRng;
use rand::{Rng, RngCore, SeedableRng};

#[derive(Parser, Debug)]
#[command(
    name = "chunklet-perf",
    about = "Run a focused chunklet LD performance workload"
)]
struct Cli {
    /// Pool device paths. Comma-separated.
    #[arg(long, value_delimiter = ',')]
    devices: Vec<PathBuf>,

    /// Initialize a fresh pool and create one LD before running.
    #[arg(long, default_value_t = false)]
    init: bool,

    /// Reuse an existing pool and LD. Defaults to the first LD when omitted.
    #[arg(long)]
    ld_id: Option<String>,

    /// RAID level to create when --init is used.
    #[arg(long, value_enum, default_value_t = PerfRaid::Raid5)]
    raid: PerfRaid,

    /// Number of data members for RAID5/RAID6, mirror copies for mirror, stripe width for raid0.
    #[arg(long, default_value_t = 3)]
    width: u16,

    /// LD rows to create when --init is used.
    #[arg(long, default_value_t = 1)]
    rows: u16,

    /// Strip size log2. 0 means 4 KiB.
    #[arg(long, default_value_t = 0)]
    strip_log2: u8,

    /// Sparse file size when a device path does not exist.
    #[arg(long, default_value_t = 8 * 1024 * 1024 * 1024)]
    sparse_size_bytes: u64,

    /// Pool spare percentage when --init is used.
    #[arg(long, default_value_t = 0)]
    spare_pct: u8,

    /// IO backend.
    #[arg(long, value_enum, default_value_t = PerfBackend::Sync)]
    backend: PerfBackend,

    /// Workload type.
    #[arg(long, value_enum, default_value_t = WorkloadKind::Randrw)]
    workload: WorkloadKind,

    /// Read percentage for randrw/seqrw.
    #[arg(long, default_value_t = 70)]
    read_pct: u8,

    /// Worker threads.
    #[arg(long, default_value_t = 4)]
    workers: usize,

    /// IO size in 4 KiB blocks.
    #[arg(long, default_value_t = 1)]
    io_blocks: usize,

    /// Run duration after warmup.
    #[arg(long, default_value_t = 30)]
    runtime_secs: u64,

    /// Warmup duration. Warmup IO is not counted.
    #[arg(long, default_value_t = 5)]
    warmup_secs: u64,

    /// Print interval.
    #[arg(long, default_value_t = 5)]
    report_secs: u64,

    /// Restrict workload to the first N bytes of the LD. 0 means full LD.
    #[arg(long, default_value_t = 0)]
    working_set_bytes: u64,

    /// Seed for deterministic random workloads.
    #[arg(long, default_value_t = 0xc0ffee)]
    seed: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum PerfRaid {
    Plain,
    Mirror,
    Raid0,
    Raid5,
    Raid6,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum PerfBackend {
    Sync,
    Uring,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum WorkloadKind {
    Read,
    Write,
    Randrw,
    Seqread,
    Seqwrite,
    Seqrw,
}

#[derive(Default)]
struct WorkerStats {
    ops: u64,
    read_ops: u64,
    write_ops: u64,
    bytes: u64,
    errors: u64,
    latency_us: Vec<u64>,
}

#[derive(Clone)]
struct SharedCounters {
    ops: Arc<AtomicU64>,
    read_ops: Arc<AtomicU64>,
    write_ops: Arc<AtomicU64>,
    bytes: Arc<AtomicU64>,
    errors: Arc<AtomicU64>,
}

impl SharedCounters {
    fn new() -> Self {
        Self {
            ops: Arc::new(AtomicU64::new(0)),
            read_ops: Arc::new(AtomicU64::new(0)),
            write_ops: Arc::new(AtomicU64::new(0)),
            bytes: Arc::new(AtomicU64::new(0)),
            errors: Arc::new(AtomicU64::new(0)),
        }
    }
}

fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .init();

    let cli = Cli::parse();
    if let Err(e) = run(cli) {
        eprintln!("error: {}", e);
        std::process::exit(1);
    }
}

fn run(cli: Cli) -> ChunkletResult<()> {
    validate_cli(&cli)?;
    let raws = open_or_create_devices(&cli.devices, cli.sparse_size_bytes)?;
    let backend = match cli.backend {
        PerfBackend::Sync => IoBackendKind::Sync,
        PerfBackend::Uring => IoBackendKind::Uring,
    };
    let pool = if cli.init {
        let pool = Pool::create(
            raws,
            PoolConfig {
                spare_pct: cli.spare_pct,
                io_backend: backend,
            },
        )?;
        let ld_id = pool.create_ld(ld_spec(cli.raid, cli.width, cli.rows, cli.strip_log2)?)?;
        eprintln!(
            "created pool {} and {:?} LD {} (rows={} width={})",
            pool.id(),
            cli.raid,
            ld_id,
            cli.rows,
            cli.width
        );
        pool
    } else {
        let pool = Pool::open(raws)?;
        pool.set_io_backend(backend);
        pool
    };

    let ld_id = match &cli.ld_id {
        Some(s) => parse_ld_id(s)?,
        None => pool
            .list_lds()
            .first()
            .map(|d| d.id)
            .ok_or_else(|| onyx_chunklet::ChunkletError::Config("pool has no LDs".into()))?,
    };
    let ld = pool.open_ld(ld_id)?;
    let io_len = cli.io_blocks * BLOCK_SIZE as usize;
    let work_bytes = effective_working_set(ld.capacity_bytes(), cli.working_set_bytes, io_len)?;

    eprintln!(
        "perf target: pool={} ld={} cap={} work={} io={} workers={} workload={:?} read_pct={} backend={:?}",
        pool.id(),
        ld_id,
        ld.capacity_bytes(),
        work_bytes,
        io_len,
        cli.workers,
        cli.workload,
        cli.read_pct,
        cli.backend
    );

    if cli.warmup_secs > 0 {
        eprintln!("warmup: {}s", cli.warmup_secs);
        run_phase(&cli, ld.clone(), work_bytes, io_len, cli.warmup_secs, false)?;
    }

    eprintln!("measure: {}s", cli.runtime_secs);
    let stats = run_phase(&cli, ld, work_bytes, io_len, cli.runtime_secs, true)?;
    print_summary(&stats, cli.runtime_secs, io_len);
    if stats.errors > 0 {
        return Err(onyx_chunklet::ChunkletError::Invariant(format!(
            "perf completed with {} IO errors",
            stats.errors
        )));
    }
    Ok(())
}

fn run_phase(
    cli: &Cli,
    ld: Arc<dyn LogicalDisk>,
    work_bytes: u64,
    io_len: usize,
    runtime_secs: u64,
    measured: bool,
) -> ChunkletResult<WorkerStats> {
    let stop = Arc::new(AtomicBool::new(false));
    let counters = SharedCounters::new();
    let start = Instant::now();
    let deadline = start + Duration::from_secs(runtime_secs);
    let mut handles = Vec::with_capacity(cli.workers);

    for worker in 0..cli.workers {
        let stop = stop.clone();
        let counters = counters.clone();
        let ld = ld.clone();
        let workload = cli.workload;
        let read_pct = cli.read_pct;
        let seed = cli.seed.wrapping_add(worker as u64);
        handles.push(std::thread::spawn(move || {
            worker_loop(
                worker, ld, stop, counters, workload, read_pct, seed, work_bytes, io_len,
            )
        }));
    }

    let mut next_report = start + Duration::from_secs(cli.report_secs.max(1));
    while Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(200));
        let now = Instant::now();
        if measured && now >= next_report {
            print_live(start.elapsed(), &counters);
            next_report = now + Duration::from_secs(cli.report_secs.max(1));
        }
    }
    stop.store(true, Ordering::Relaxed);

    let mut stats = WorkerStats::default();
    for handle in handles {
        let worker = handle
            .join()
            .map_err(|_| onyx_chunklet::ChunkletError::Invariant("perf worker panicked".into()))?;
        stats.ops += worker.ops;
        stats.read_ops += worker.read_ops;
        stats.write_ops += worker.write_ops;
        stats.bytes += worker.bytes;
        stats.errors += worker.errors;
        stats.latency_us.extend(worker.latency_us);
    }
    Ok(stats)
}

#[allow(clippy::too_many_arguments)]
fn worker_loop(
    worker: usize,
    ld: Arc<dyn LogicalDisk>,
    stop: Arc<AtomicBool>,
    counters: SharedCounters,
    workload: WorkloadKind,
    read_pct: u8,
    seed: u64,
    work_bytes: u64,
    io_len: usize,
) -> WorkerStats {
    let mut rng = StdRng::seed_from_u64(seed);
    let mut read_buf = vec![0u8; io_len];
    let mut write_buf = vec![0u8; io_len];
    rng.fill_bytes(&mut write_buf);
    let mut seq = (worker as u64 * io_len as u64) % work_bytes;
    let mut stats = WorkerStats::default();

    while !stop.load(Ordering::Relaxed) {
        let is_read = choose_read(workload, read_pct, &mut rng);
        let offset = choose_offset(workload, &mut rng, &mut seq, work_bytes, io_len as u64);
        if !is_read {
            stamp_buffer(&mut write_buf, stats.ops);
        }

        let t0 = Instant::now();
        let result = if is_read {
            ld.read_at(offset, &mut read_buf)
        } else {
            ld.write_at(offset, &write_buf)
        };
        let latency = t0.elapsed().as_micros() as u64;
        stats.latency_us.push(latency);

        match result {
            Ok(()) => {
                stats.ops += 1;
                stats.bytes += io_len as u64;
                if is_read {
                    stats.read_ops += 1;
                } else {
                    stats.write_ops += 1;
                }
                counters.ops.fetch_add(1, Ordering::Relaxed);
                counters.bytes.fetch_add(io_len as u64, Ordering::Relaxed);
                if is_read {
                    counters.read_ops.fetch_add(1, Ordering::Relaxed);
                } else {
                    counters.write_ops.fetch_add(1, Ordering::Relaxed);
                }
            }
            Err(e) => {
                stats.errors += 1;
                counters.errors.fetch_add(1, Ordering::Relaxed);
                eprintln!("worker {} IO error at offset {}: {}", worker, offset, e);
            }
        }
    }
    stats
}

fn choose_read(workload: WorkloadKind, read_pct: u8, rng: &mut StdRng) -> bool {
    match workload {
        WorkloadKind::Read | WorkloadKind::Seqread => true,
        WorkloadKind::Write | WorkloadKind::Seqwrite => false,
        WorkloadKind::Randrw | WorkloadKind::Seqrw => rng.gen_range(0..100) < read_pct,
    }
}

fn choose_offset(
    workload: WorkloadKind,
    rng: &mut StdRng,
    seq: &mut u64,
    work_bytes: u64,
    io_len: u64,
) -> u64 {
    match workload {
        WorkloadKind::Read | WorkloadKind::Write | WorkloadKind::Randrw => {
            let max_block = (work_bytes - io_len) / BLOCK_SIZE;
            rng.gen_range(0..=max_block) * BLOCK_SIZE
        }
        WorkloadKind::Seqread | WorkloadKind::Seqwrite | WorkloadKind::Seqrw => {
            let out = *seq;
            *seq += io_len;
            if *seq + io_len > work_bytes {
                *seq = 0;
            }
            out
        }
    }
}

fn print_live(elapsed: Duration, counters: &SharedCounters) {
    let secs = elapsed.as_secs_f64().max(0.001);
    let ops = counters.ops.load(Ordering::Relaxed);
    let bytes = counters.bytes.load(Ordering::Relaxed);
    let reads = counters.read_ops.load(Ordering::Relaxed);
    let writes = counters.write_ops.load(Ordering::Relaxed);
    let errors = counters.errors.load(Ordering::Relaxed);
    eprintln!(
        "[{:>5.1}s] ops={} read={} write={} iops={:.0} bw={:.1} MiB/s errors={}",
        secs,
        ops,
        reads,
        writes,
        ops as f64 / secs,
        bytes as f64 / secs / (1u64 << 20) as f64,
        errors
    );
}

fn print_summary(stats: &WorkerStats, runtime_secs: u64, io_len: usize) {
    let secs = runtime_secs.max(1) as f64;
    let mut lat = stats.latency_us.clone();
    lat.sort_unstable();
    let avg_us = if stats.ops == 0 {
        0.0
    } else {
        lat.iter().sum::<u64>() as f64 / lat.len().max(1) as f64
    };
    println!("ops={}", stats.ops);
    println!("read_ops={}", stats.read_ops);
    println!("write_ops={}", stats.write_ops);
    println!("bytes={}", stats.bytes);
    println!("io_bytes={}", io_len);
    println!("iops={:.2}", stats.ops as f64 / secs);
    println!(
        "throughput_mib_s={:.2}",
        stats.bytes as f64 / secs / (1u64 << 20) as f64
    );
    println!("avg_latency_us={:.2}", avg_us);
    println!("p50_latency_us={}", percentile(&lat, 50.0));
    println!("p95_latency_us={}", percentile(&lat, 95.0));
    println!("p99_latency_us={}", percentile(&lat, 99.0));
    println!("max_latency_us={}", lat.last().copied().unwrap_or(0));
    println!("errors={}", stats.errors);
}

fn percentile(sorted: &[u64], pct: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((pct / 100.0) * (sorted.len().saturating_sub(1) as f64)).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

fn validate_cli(cli: &Cli) -> ChunkletResult<()> {
    if cli.devices.is_empty() {
        return Err(onyx_chunklet::ChunkletError::Config(
            "--devices required".into(),
        ));
    }
    if cli.workers == 0 {
        return Err(onyx_chunklet::ChunkletError::Config(
            "--workers must be > 0".into(),
        ));
    }
    if cli.io_blocks == 0 {
        return Err(onyx_chunklet::ChunkletError::Config(
            "--io-blocks must be > 0".into(),
        ));
    }
    if cli.read_pct > 100 {
        return Err(onyx_chunklet::ChunkletError::Config(
            "--read-pct must be <= 100".into(),
        ));
    }
    if cli.runtime_secs == 0 {
        return Err(onyx_chunklet::ChunkletError::Config(
            "--runtime-secs must be > 0".into(),
        ));
    }
    Ok(())
}

fn ld_spec(raid: PerfRaid, width: u16, rows: u16, strip_log2: u8) -> ChunkletResult<LdSpec> {
    if width == 0 || rows == 0 {
        return Err(onyx_chunklet::ChunkletError::Config(
            "--width and --rows must be > 0".into(),
        ));
    }
    let spec = match raid {
        PerfRaid::Plain => LdSpec::plain(rows),
        PerfRaid::Mirror => LdSpec::mirror(width as u8, 1, rows, strip_log2),
        PerfRaid::Raid0 => LdSpec::raid0(width, rows, strip_log2),
        PerfRaid::Raid5 => LdSpec::raid5(width as u8, 1, rows, strip_log2),
        PerfRaid::Raid6 => LdSpec::raid6(width as u8, 1, rows, strip_log2),
    };
    Ok(spec)
}

fn parse_ld_id(s: &str) -> ChunkletResult<LdId> {
    let parsed = uuid::Uuid::parse_str(s)
        .map_err(|e| onyx_chunklet::ChunkletError::Config(format!("bad uuid: {}", e)))?;
    Ok(LdId::from_bytes(*parsed.as_bytes()))
}

fn effective_working_set(capacity: u64, requested: u64, io_len: usize) -> ChunkletResult<u64> {
    let work = if requested == 0 {
        capacity
    } else {
        requested.min(capacity)
    };
    if work < io_len as u64 {
        return Err(onyx_chunklet::ChunkletError::Config(format!(
            "working set {} is smaller than IO size {}",
            work, io_len
        )));
    }
    Ok((work / BLOCK_SIZE) * BLOCK_SIZE)
}

fn stamp_buffer(buf: &mut [u8], seq: u64) {
    let bytes = seq.to_le_bytes();
    let n = bytes.len().min(buf.len());
    buf[..n].copy_from_slice(&bytes[..n]);
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
