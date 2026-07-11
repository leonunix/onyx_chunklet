//! `chunklet-perf` - fio-ish performance harness for chunklet LDs.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::{Parser, ValueEnum};
use onyx_chunklet::io::{AlignedBuf, IoBackendKind, RawDevice};
use onyx_chunklet::ld::{gf256, LogicalDisk};
use onyx_chunklet::pool::LdSpec;
use onyx_chunklet::types::{LdId, BLOCK_SIZE};
use onyx_chunklet::{ChunkletResult, Pool, PoolConfig};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use serde::Deserialize;

#[derive(Parser, Debug)]
#[command(
    name = "chunklet-perf",
    about = "Run fio-ish chunklet LD performance workloads"
)]
struct Cli {
    /// TOML job file. CLI still supplies defaults for omitted fields.
    #[arg(long)]
    job_file: Option<PathBuf>,

    /// Pool device paths. Comma-separated.
    #[arg(long, value_delimiter = ',')]
    devices: Vec<PathBuf>,

    /// Initialize a fresh pool and create LDs for jobs without ld_id.
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

    /// Worker groups per job.
    #[arg(long, default_value_t = 4)]
    workers: usize,

    /// Outstanding sync lanes per worker. Total threads per job = workers * iodepth.
    #[arg(long, default_value_t = 1)]
    iodepth: usize,

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

    /// Start offset within the LD.
    #[arg(long, default_value_t = 0)]
    offset_bytes: u64,

    /// Random offset distribution.
    #[arg(long, value_enum, default_value_t = RandomDist::Uniform)]
    random_dist: RandomDist,

    /// Random offset alignment in 4 KiB blocks. 0 means use io_blocks.
    #[arg(long, default_value_t = 0)]
    random_align_blocks: usize,

    /// Percent of random IOs sent to the hot region when random-dist=hotspot.
    #[arg(long, default_value_t = 80)]
    hot_pct: u8,

    /// Percent of working set that is hot when random-dist=hotspot.
    #[arg(long, default_value_t = 20)]
    hotset_pct: u8,

    /// Verify reads and write-after-readback with deterministic offset pattern.
    #[arg(long, default_value_t = false)]
    verify: bool,

    /// Batch pure-write IOs through LogicalDisk::write_many_at.
    #[arg(long, default_value_t = false)]
    batch_writes: bool,

    /// Seed for deterministic random workloads.
    #[arg(long, default_value_t = 0xc0ffee)]
    seed: u64,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, ValueEnum)]
#[serde(rename_all = "kebab-case")]
enum PerfRaid {
    Plain,
    Mirror,
    Raid0,
    Raid5,
    Raid6,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, ValueEnum)]
#[serde(rename_all = "kebab-case")]
enum PerfBackend {
    Sync,
    Uring,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, ValueEnum)]
#[serde(rename_all = "kebab-case")]
enum WorkloadKind {
    Read,
    Write,
    Randrw,
    Seqread,
    Seqwrite,
    Seqrw,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, ValueEnum)]
#[serde(rename_all = "kebab-case")]
enum RandomDist {
    Uniform,
    Hotspot,
    Zipf,
}

#[derive(Debug, Deserialize)]
struct PerfFile {
    #[serde(default)]
    global: FileGlobal,
    #[serde(default, rename = "job")]
    jobs: Vec<FileJob>,
}

#[derive(Default, Debug, Deserialize)]
struct FileGlobal {
    devices: Option<Vec<PathBuf>>,
    init: Option<bool>,
    backend: Option<PerfBackend>,
    runtime_secs: Option<u64>,
    warmup_secs: Option<u64>,
    report_secs: Option<u64>,
    sparse_size_bytes: Option<u64>,
    spare_pct: Option<u8>,
    seed: Option<u64>,
}

#[derive(Default, Debug, Deserialize)]
struct FileJob {
    name: Option<String>,
    ld_id: Option<String>,
    raid: Option<PerfRaid>,
    width: Option<u16>,
    rows: Option<u16>,
    strip_log2: Option<u8>,
    workload: Option<WorkloadKind>,
    read_pct: Option<u8>,
    workers: Option<usize>,
    iodepth: Option<usize>,
    io_blocks: Option<usize>,
    working_set_bytes: Option<u64>,
    offset_bytes: Option<u64>,
    random_dist: Option<RandomDist>,
    random_align_blocks: Option<usize>,
    hot_pct: Option<u8>,
    hotset_pct: Option<u8>,
    verify: Option<bool>,
    batch_writes: Option<bool>,
    seed: Option<u64>,
}

#[derive(Clone)]
struct BenchJob {
    name: String,
    ld_id: LdId,
    ld: Arc<dyn LogicalDisk>,
    workload: WorkloadKind,
    read_pct: u8,
    workers: usize,
    iodepth: usize,
    io_len: usize,
    offset_bytes: u64,
    work_bytes: u64,
    random_dist: RandomDist,
    random_align: u64,
    hot_pct: u8,
    hotset_pct: u8,
    verify: bool,
    batch_writes: bool,
    seed: u64,
}

#[derive(Default)]
struct WorkerStats {
    job: String,
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
    let perf_file = load_job_file(&cli)?;
    validate_global(&cli, perf_file.as_ref())?;

    let global = perf_file.as_ref().map(|f| &f.global);
    let devices = merged_devices(&cli, global)?;
    let init = global.and_then(|g| g.init).unwrap_or(cli.init);
    let backend = global.and_then(|g| g.backend).unwrap_or(cli.backend);
    let sparse_size = global
        .and_then(|g| g.sparse_size_bytes)
        .unwrap_or(cli.sparse_size_bytes);
    let spare_pct = global.and_then(|g| g.spare_pct).unwrap_or(cli.spare_pct);
    let runtime_secs = global
        .and_then(|g| g.runtime_secs)
        .unwrap_or(cli.runtime_secs);
    let warmup_secs = global
        .and_then(|g| g.warmup_secs)
        .unwrap_or(cli.warmup_secs);
    let report_secs = global
        .and_then(|g| g.report_secs)
        .unwrap_or(cli.report_secs);
    let seed = global.and_then(|g| g.seed).unwrap_or(cli.seed);

    let raws = open_or_create_devices(&devices, sparse_size)?;
    let backend_kind = match backend {
        PerfBackend::Sync => IoBackendKind::Sync,
        PerfBackend::Uring => IoBackendKind::Uring,
    };
    let pool = if init {
        Pool::create(
            raws,
            PoolConfig {
                spare_pct,
                io_backend: backend_kind,
            },
        )?
    } else {
        let pool = Pool::open(raws)?;
        pool.set_io_backend(backend_kind);
        pool
    };

    let file_jobs = perf_file.as_ref().map(|f| f.jobs.as_slice()).unwrap_or(&[]);
    let jobs = build_jobs(
        &cli,
        file_jobs,
        init,
        seed,
        runtime_secs,
        warmup_secs,
        &pool,
    )?;

    eprintln!(
        "perf target: pool={} jobs={} backend={:?} runtime={}s warmup={}s",
        pool.id(),
        jobs.len(),
        backend,
        runtime_secs,
        warmup_secs
    );
    for job in &jobs {
        eprintln!(
            "  job={} ld={} work={} offset={} io={} random_align={} workers={} iodepth={} workload={:?} read_pct={} dist={:?} verify={} batch_writes={}",
            job.name,
            job.ld_id,
            job.work_bytes,
            job.offset_bytes,
            job.io_len,
            job.random_align,
            job.workers,
            job.iodepth,
            job.workload,
            job.read_pct,
            job.random_dist,
            job.verify,
            job.batch_writes
        );
    }

    if jobs.iter().any(|j| j.verify) {
        prefill_verify_jobs(&jobs)?;
    }
    if warmup_secs > 0 {
        eprintln!("warmup: {}s", warmup_secs);
        run_phase(&jobs, warmup_secs, report_secs, false)?;
    }

    eprintln!("measure: {}s", runtime_secs);
    let stats = run_phase(&jobs, runtime_secs, report_secs, true)?;
    print_summary(&stats, runtime_secs);
    print_gf256_summary();
    if stats.iter().any(|s| s.errors > 0) {
        let errors: u64 = stats.iter().map(|s| s.errors).sum();
        return Err(onyx_chunklet::ChunkletError::Invariant(format!(
            "perf completed with {} IO errors",
            errors
        )));
    }
    Ok(())
}

fn print_gf256_summary() {
    let s = gf256::stats_snapshot();
    println!("gf256.xor_avx512_calls={}", s.xor_avx512_calls);
    println!("gf256.xor_avx512_bytes={}", s.xor_avx512_bytes);
    println!("gf256.xor_avx2_calls={}", s.xor_avx2_calls);
    println!("gf256.xor_avx2_bytes={}", s.xor_avx2_bytes);
    println!("gf256.xor_scalar_calls={}", s.xor_scalar_calls);
    println!("gf256.xor_scalar_bytes={}", s.xor_scalar_bytes);
    println!("gf256.mul_avx512_calls={}", s.mul_avx512_calls);
    println!("gf256.mul_avx512_bytes={}", s.mul_avx512_bytes);
    println!("gf256.mul_avx2_calls={}", s.mul_avx2_calls);
    println!("gf256.mul_avx2_bytes={}", s.mul_avx2_bytes);
    println!("gf256.mul_scalar_calls={}", s.mul_scalar_calls);
    println!("gf256.mul_scalar_bytes={}", s.mul_scalar_bytes);
}

fn build_jobs(
    cli: &Cli,
    file_jobs: &[FileJob],
    init: bool,
    global_seed: u64,
    runtime_secs: u64,
    warmup_secs: u64,
    pool: &Arc<Pool>,
) -> ChunkletResult<Vec<BenchJob>> {
    if runtime_secs == 0 {
        return Err(onyx_chunklet::ChunkletError::Config(
            "runtime_secs must be > 0".into(),
        ));
    }
    let synthetic;
    let job_defs = if file_jobs.is_empty() {
        synthetic = vec![FileJob {
            name: Some("job0".into()),
            ld_id: cli.ld_id.clone(),
            raid: Some(cli.raid),
            width: Some(cli.width),
            rows: Some(cli.rows),
            strip_log2: Some(cli.strip_log2),
            workload: Some(cli.workload),
            read_pct: Some(cli.read_pct),
            workers: Some(cli.workers),
            iodepth: Some(cli.iodepth),
            io_blocks: Some(cli.io_blocks),
            working_set_bytes: Some(cli.working_set_bytes),
            offset_bytes: Some(cli.offset_bytes),
            random_dist: Some(cli.random_dist),
            random_align_blocks: Some(cli.random_align_blocks),
            hot_pct: Some(cli.hot_pct),
            hotset_pct: Some(cli.hotset_pct),
            verify: Some(cli.verify),
            batch_writes: Some(cli.batch_writes),
            seed: Some(cli.seed),
        }];
        synthetic.as_slice()
    } else {
        file_jobs
    };

    let mut out = Vec::with_capacity(job_defs.len());
    for (idx, job) in job_defs.iter().enumerate() {
        let name = job.name.clone().unwrap_or_else(|| format!("job{}", idx));
        let raid = job.raid.unwrap_or(cli.raid);
        let width = job.width.unwrap_or(cli.width);
        let rows = job.rows.unwrap_or(cli.rows);
        let strip_log2 = job.strip_log2.unwrap_or(cli.strip_log2);
        let ld_id =
            match &job.ld_id {
                Some(s) => parse_ld_id(s)?,
                None if init => pool.create_ld(ld_spec(raid, width, rows, strip_log2)?)?,
                None => pool.list_lds().first().map(|d| d.id).ok_or_else(|| {
                    onyx_chunklet::ChunkletError::Config("pool has no LDs".into())
                })?,
            };
        let ld = pool.open_ld(ld_id)?;
        let io_blocks = job.io_blocks.unwrap_or(cli.io_blocks);
        let io_len = io_blocks * BLOCK_SIZE as usize;
        let offset_bytes = job.offset_bytes.unwrap_or(cli.offset_bytes);
        let requested_work = job.working_set_bytes.unwrap_or(cli.working_set_bytes);
        let work_bytes =
            effective_working_set(ld.capacity_bytes(), offset_bytes, requested_work, io_len)?;
        let read_pct = job.read_pct.unwrap_or(cli.read_pct);
        let workers = job.workers.unwrap_or(cli.workers);
        let iodepth = job.iodepth.unwrap_or(cli.iodepth);
        let hot_pct = job.hot_pct.unwrap_or(cli.hot_pct);
        let hotset_pct = job.hotset_pct.unwrap_or(cli.hotset_pct);
        let random_align_blocks = job.random_align_blocks.unwrap_or(cli.random_align_blocks);
        let random_align = if random_align_blocks == 0 {
            io_len as u64
        } else {
            random_align_blocks as u64 * BLOCK_SIZE
        };
        let seed = job
            .seed
            .unwrap_or(global_seed.wrapping_add((idx as u64) << 32));
        let bench = BenchJob {
            name,
            ld_id,
            ld,
            workload: job.workload.unwrap_or(cli.workload),
            read_pct,
            workers,
            iodepth,
            io_len,
            offset_bytes,
            work_bytes,
            random_dist: job.random_dist.unwrap_or(cli.random_dist),
            random_align,
            hot_pct,
            hotset_pct,
            verify: job.verify.unwrap_or(cli.verify),
            batch_writes: job.batch_writes.unwrap_or(cli.batch_writes),
            seed,
        };
        validate_job(&bench, warmup_secs)?;
        out.push(bench);
    }
    Ok(out)
}

fn run_phase(
    jobs: &[BenchJob],
    runtime_secs: u64,
    report_secs: u64,
    measured: bool,
) -> ChunkletResult<Vec<WorkerStats>> {
    let stop = Arc::new(AtomicBool::new(false));
    let counters = SharedCounters::new();
    let start = Instant::now();
    let deadline = start + Duration::from_secs(runtime_secs);
    let mut handles = Vec::new();

    for job in jobs {
        let use_batch_writes = matches!(job.workload, WorkloadKind::Write | WorkloadKind::Seqwrite)
            && !job.verify
            && job.batch_writes;
        let lanes = if (job.workload == WorkloadKind::Read && !job.verify) || use_batch_writes {
            job.workers
        } else {
            job.workers * job.iodepth
        };
        for lane in 0..lanes {
            let stop = stop.clone();
            let counters = counters.clone();
            let job = job.clone();
            handles.push(std::thread::spawn(move || {
                if job.workload == WorkloadKind::Read && !job.verify {
                    read_batch_worker_loop(job, lane, stop, counters)
                } else if matches!(job.workload, WorkloadKind::Write | WorkloadKind::Seqwrite)
                    && !job.verify
                    && job.batch_writes
                {
                    write_batch_worker_loop(job, lane, stop, counters)
                } else {
                    worker_loop(job, lane, stop, counters)
                }
            }));
        }
    }

    let mut next_report = start + Duration::from_secs(report_secs.max(1));
    while Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(200));
        let now = Instant::now();
        if measured && now >= next_report {
            print_live(start.elapsed(), &counters);
            next_report = now + Duration::from_secs(report_secs.max(1));
        }
    }
    stop.store(true, Ordering::Relaxed);

    let mut stats = Vec::with_capacity(handles.len());
    for handle in handles {
        stats.push(
            handle.join().map_err(|_| {
                onyx_chunklet::ChunkletError::Invariant("perf worker panicked".into())
            })?,
        );
    }
    Ok(stats)
}

fn read_batch_worker_loop(
    job: BenchJob,
    lane: usize,
    stop: Arc<AtomicBool>,
    counters: SharedCounters,
) -> WorkerStats {
    let mut rng = StdRng::seed_from_u64(job.seed.wrapping_add(lane as u64));
    let mut bufs: Vec<AlignedBuf> = match alloc_aligned_batch(job.iodepth, job.io_len) {
        Ok(bufs) => bufs,
        Err(e) => return worker_init_error(job, lane, counters, "aligned read buffers", e),
    };
    let mut offsets = vec![0u64; job.iodepth];
    let mut seq = job.offset_bytes + ((lane as u64 * job.io_len as u64) % job.work_bytes);
    let mut stats = WorkerStats {
        job: job.name.clone(),
        ..Default::default()
    };

    while !stop.load(Ordering::Relaxed) {
        for offset in &mut offsets {
            *offset = choose_offset(&job, &mut rng, &mut seq);
        }
        let t0 = Instant::now();
        let mut ops: Vec<(u64, &mut [u8])> = offsets
            .iter()
            .copied()
            .zip(
                bufs.iter_mut()
                    .map(|buf| &mut buf.as_mut_slice()[..job.io_len]),
            )
            .collect();
        let result = job.ld.read_many_at(&mut ops);
        let elapsed = t0.elapsed().as_micros() as u64;
        let per_io_latency = (elapsed / job.iodepth.max(1) as u64).max(1);
        match result {
            Ok(()) => {
                for _ in 0..job.iodepth {
                    stats.ops += 1;
                    stats.read_ops += 1;
                    stats.bytes += job.io_len as u64;
                    stats.latency_us.push(per_io_latency);
                }
                counters
                    .ops
                    .fetch_add(job.iodepth as u64, Ordering::Relaxed);
                counters
                    .read_ops
                    .fetch_add(job.iodepth as u64, Ordering::Relaxed);
                counters
                    .bytes
                    .fetch_add(job.iodepth as u64 * job.io_len as u64, Ordering::Relaxed);
            }
            Err(e) => {
                stats.errors += job.iodepth as u64;
                counters
                    .errors
                    .fetch_add(job.iodepth as u64, Ordering::Relaxed);
                eprintln!("job={} lane={} batch read error: {}", job.name, lane, e);
            }
        }
    }
    stats
}

fn write_batch_worker_loop(
    job: BenchJob,
    lane: usize,
    stop: Arc<AtomicBool>,
    counters: SharedCounters,
) -> WorkerStats {
    let mut rng = StdRng::seed_from_u64(job.seed.wrapping_add(lane as u64));
    let mut bufs: Vec<AlignedBuf> = match alloc_aligned_batch(job.iodepth, job.io_len) {
        Ok(bufs) => bufs,
        Err(e) => return worker_init_error(job, lane, counters, "aligned write buffers", e),
    };
    let mut offsets = vec![0u64; job.iodepth];
    // Give every batched lane a disjoint sequential region. Offsetting lanes by
    // one IO makes adjacent iodepth-wide batches overlap almost completely,
    // turning a sequential throughput test into a stripe-lock collision test.
    let lane_region = (job.work_bytes / job.workers.max(1) as u64)
        / job.io_len as u64
        * job.io_len as u64;
    let mut seq = job.offset_bytes + lane as u64 * lane_region;
    let mut stats = WorkerStats {
        job: job.name.clone(),
        ..Default::default()
    };

    while !stop.load(Ordering::Relaxed) {
        for (i, offset) in offsets.iter_mut().enumerate() {
            *offset = choose_offset(&job, &mut rng, &mut seq);
            fill_verify_pattern(&mut bufs[i].as_mut_slice()[..job.io_len], *offset);
        }
        let t0 = Instant::now();
        let ops: Vec<(u64, &[u8])> = offsets
            .iter()
            .copied()
            .zip(bufs.iter().map(|buf| &buf.as_slice()[..job.io_len]))
            .collect();
        let result = job.ld.write_many_at(&ops);
        let elapsed = t0.elapsed().as_micros() as u64;
        let per_io_latency = (elapsed / job.iodepth.max(1) as u64).max(1);
        match result {
            Ok(()) => {
                for _ in 0..job.iodepth {
                    stats.ops += 1;
                    stats.write_ops += 1;
                    stats.bytes += job.io_len as u64;
                    stats.latency_us.push(per_io_latency);
                }
                counters
                    .ops
                    .fetch_add(job.iodepth as u64, Ordering::Relaxed);
                counters
                    .write_ops
                    .fetch_add(job.iodepth as u64, Ordering::Relaxed);
                counters
                    .bytes
                    .fetch_add(job.iodepth as u64 * job.io_len as u64, Ordering::Relaxed);
            }
            Err(e) => {
                stats.errors += job.iodepth as u64;
                counters
                    .errors
                    .fetch_add(job.iodepth as u64, Ordering::Relaxed);
                eprintln!("job={} lane={} batch write error: {}", job.name, lane, e);
            }
        }
    }
    stats
}

fn worker_loop(
    job: BenchJob,
    lane: usize,
    stop: Arc<AtomicBool>,
    counters: SharedCounters,
) -> WorkerStats {
    let mut rng = StdRng::seed_from_u64(job.seed.wrapping_add(lane as u64));
    let mut read_buf = match AlignedBuf::new(job.io_len) {
        Ok(buf) => buf,
        Err(e) => return worker_init_error(job, lane, counters, "aligned read buffer", e),
    };
    let mut write_buf = match AlignedBuf::new(job.io_len) {
        Ok(buf) => buf,
        Err(e) => return worker_init_error(job, lane, counters, "aligned write buffer", e),
    };
    let mut verify_buf = match AlignedBuf::new(job.io_len) {
        Ok(buf) => buf,
        Err(e) => return worker_init_error(job, lane, counters, "aligned verify buffer", e),
    };
    let mut seq = job.offset_bytes + ((lane as u64 * job.io_len as u64) % job.work_bytes);
    let mut stats = WorkerStats {
        job: job.name.clone(),
        ..Default::default()
    };

    while !stop.load(Ordering::Relaxed) {
        let is_read = choose_read(job.workload, job.read_pct, &mut rng);
        let offset = choose_offset(&job, &mut rng, &mut seq);
        if !is_read {
            fill_verify_pattern(&mut write_buf.as_mut_slice()[..job.io_len], offset);
        }

        let t0 = Instant::now();
        let result = if is_read {
            let result = job
                .ld
                .read_at(offset, &mut read_buf.as_mut_slice()[..job.io_len]);
            if result.is_ok() && job.verify {
                fill_verify_pattern(&mut verify_buf.as_mut_slice()[..job.io_len], offset);
                if read_buf.as_slice()[..job.io_len] != verify_buf.as_slice()[..job.io_len] {
                    Err(onyx_chunklet::ChunkletError::Invariant(format!(
                        "verify mismatch job={} offset={}",
                        job.name, offset
                    )))
                } else {
                    Ok(())
                }
            } else {
                result
            }
        } else {
            let result = job.ld.write_at(offset, &write_buf.as_slice()[..job.io_len]);
            if result.is_ok() && job.verify {
                match job
                    .ld
                    .read_at(offset, &mut verify_buf.as_mut_slice()[..job.io_len])
                {
                    Ok(())
                        if verify_buf.as_slice()[..job.io_len]
                            == write_buf.as_slice()[..job.io_len] =>
                    {
                        Ok(())
                    }
                    Ok(()) => Err(onyx_chunklet::ChunkletError::Invariant(format!(
                        "write verify mismatch job={} offset={}",
                        job.name, offset
                    ))),
                    Err(e) => Err(e),
                }
            } else {
                result
            }
        };
        let latency = t0.elapsed().as_micros() as u64;
        stats.latency_us.push(latency);

        match result {
            Ok(()) => {
                stats.ops += 1;
                stats.bytes += job.io_len as u64;
                if is_read {
                    stats.read_ops += 1;
                } else {
                    stats.write_ops += 1;
                }
                counters.ops.fetch_add(1, Ordering::Relaxed);
                counters
                    .bytes
                    .fetch_add(job.io_len as u64, Ordering::Relaxed);
                if is_read {
                    counters.read_ops.fetch_add(1, Ordering::Relaxed);
                } else {
                    counters.write_ops.fetch_add(1, Ordering::Relaxed);
                }
            }
            Err(e) => {
                stats.errors += 1;
                counters.errors.fetch_add(1, Ordering::Relaxed);
                eprintln!(
                    "job={} lane={} IO error offset={}: {}",
                    job.name, lane, offset, e
                );
            }
        }
    }
    stats
}

fn alloc_aligned_batch(count: usize, len: usize) -> ChunkletResult<Vec<AlignedBuf>> {
    (0..count).map(|_| AlignedBuf::new(len)).collect()
}

fn worker_init_error(
    job: BenchJob,
    lane: usize,
    counters: SharedCounters,
    what: &str,
    error: onyx_chunklet::ChunkletError,
) -> WorkerStats {
    counters.errors.fetch_add(1, Ordering::Relaxed);
    eprintln!(
        "job={} lane={} init error allocating {}: {}",
        job.name, lane, what, error
    );
    WorkerStats {
        job: job.name,
        errors: 1,
        ..Default::default()
    }
}

fn choose_read(workload: WorkloadKind, read_pct: u8, rng: &mut StdRng) -> bool {
    match workload {
        WorkloadKind::Read | WorkloadKind::Seqread => true,
        WorkloadKind::Write | WorkloadKind::Seqwrite => false,
        WorkloadKind::Randrw | WorkloadKind::Seqrw => rng.gen_range(0..100) < read_pct,
    }
}

fn choose_offset(job: &BenchJob, rng: &mut StdRng, seq: &mut u64) -> u64 {
    match job.workload {
        WorkloadKind::Read | WorkloadKind::Write | WorkloadKind::Randrw => {
            let rel = choose_random_rel(job, rng);
            job.offset_bytes + rel
        }
        WorkloadKind::Seqread | WorkloadKind::Seqwrite | WorkloadKind::Seqrw => {
            let out = *seq;
            *seq += job.io_len as u64;
            if *seq + job.io_len as u64 > job.offset_bytes + job.work_bytes {
                *seq = job.offset_bytes;
            }
            out
        }
    }
}

fn choose_random_rel(job: &BenchJob, rng: &mut StdRng) -> u64 {
    let align = job.random_align.max(BLOCK_SIZE);
    let slots = ((job.work_bytes - job.io_len as u64) / align) + 1;
    let slot = match job.random_dist {
        RandomDist::Uniform => rng.gen_range(0..slots),
        RandomDist::Hotspot => {
            let hot_slots = ((slots * job.hotset_pct.max(1) as u64) / 100).max(1);
            if rng.gen_range(0..100) < job.hot_pct {
                rng.gen_range(0..hot_slots)
            } else if hot_slots >= slots {
                rng.gen_range(0..slots)
            } else {
                rng.gen_range(hot_slots..slots)
            }
        }
        RandomDist::Zipf => {
            let u: f64 = rng.gen_range(0.0..1.0);
            ((u * u * slots as f64).floor() as u64).min(slots - 1)
        }
    };
    slot * align
}

fn prefill_verify_jobs(jobs: &[BenchJob]) -> ChunkletResult<()> {
    let mut ranges = Vec::new();
    for job in jobs.iter().filter(|j| j.verify) {
        ranges.push((job.ld_id, job.ld.clone(), job.offset_bytes, job.work_bytes));
    }
    ranges.sort_by_key(|(ld, _, start, len)| (*ld, *start, *len));
    ranges.dedup_by_key(|(ld, _, start, len)| (*ld, *start, *len));

    let chunk = 1usize << 20;
    let mut buf = vec![0u8; chunk];
    for (ld_id, ld, start, len) in ranges {
        eprintln!(
            "verify prefill: ld={} offset={} bytes={}",
            ld_id, start, len
        );
        let mut done = 0u64;
        while done < len {
            let take = std::cmp::min(chunk as u64, len - done) as usize;
            let offset = start + done;
            fill_verify_pattern(&mut buf[..take], offset);
            ld.write_at(offset, &buf[..take])?;
            done += take as u64;
        }
    }
    Ok(())
}

fn fill_verify_pattern(buf: &mut [u8], base_offset: u64) {
    for (i, b) in buf.iter_mut().enumerate() {
        *b = mix_byte(base_offset + i as u64);
    }
}

fn mix_byte(abs: u64) -> u8 {
    let mut x = abs.wrapping_mul(0x9e37_79b9_7f4a_7c15);
    x ^= x >> 33;
    x = x.wrapping_mul(0xff51_afd7_ed55_8ccd);
    x ^= x >> 33;
    (x >> 56) as u8
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

fn print_summary(stats: &[WorkerStats], runtime_secs: u64) {
    let secs = runtime_secs.max(1) as f64;
    let total = aggregate(stats);
    print_one_summary("total", &total, secs);

    let mut by_job: BTreeMap<String, WorkerStats> = BTreeMap::new();
    for stat in stats {
        merge_stats(by_job.entry(stat.job.clone()).or_default(), stat);
    }
    for (job, stat) in by_job {
        print_one_summary(&format!("job.{}", sanitize_key(&job)), &stat, secs);
    }
}

fn aggregate(stats: &[WorkerStats]) -> WorkerStats {
    let mut out = WorkerStats::default();
    for stat in stats {
        merge_stats(&mut out, stat);
    }
    out
}

fn merge_stats(dst: &mut WorkerStats, src: &WorkerStats) {
    dst.ops += src.ops;
    dst.read_ops += src.read_ops;
    dst.write_ops += src.write_ops;
    dst.bytes += src.bytes;
    dst.errors += src.errors;
    dst.latency_us.extend_from_slice(&src.latency_us);
}

fn print_one_summary(prefix: &str, stats: &WorkerStats, secs: f64) {
    let mut lat = stats.latency_us.clone();
    lat.sort_unstable();
    let avg_us = if lat.is_empty() {
        0.0
    } else {
        lat.iter().sum::<u64>() as f64 / lat.len() as f64
    };
    println!("{}.ops={}", prefix, stats.ops);
    println!("{}.read_ops={}", prefix, stats.read_ops);
    println!("{}.write_ops={}", prefix, stats.write_ops);
    println!("{}.bytes={}", prefix, stats.bytes);
    println!("{}.iops={:.2}", prefix, stats.ops as f64 / secs);
    println!(
        "{}.throughput_mib_s={:.2}",
        prefix,
        stats.bytes as f64 / secs / (1u64 << 20) as f64
    );
    println!("{}.avg_latency_us={:.2}", prefix, avg_us);
    println!("{}.p50_latency_us={}", prefix, percentile(&lat, 50.0));
    println!("{}.p95_latency_us={}", prefix, percentile(&lat, 95.0));
    println!("{}.p99_latency_us={}", prefix, percentile(&lat, 99.0));
    println!(
        "{}.max_latency_us={}",
        prefix,
        lat.last().copied().unwrap_or(0)
    );
    println!("{}.errors={}", prefix, stats.errors);
}

fn percentile(sorted: &[u64], pct: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((pct / 100.0) * (sorted.len().saturating_sub(1) as f64)).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

fn sanitize_key(s: &str) -> String {
    s.chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
        .collect()
}

fn load_job_file(cli: &Cli) -> ChunkletResult<Option<PerfFile>> {
    let Some(path) = &cli.job_file else {
        return Ok(None);
    };
    let s = std::fs::read_to_string(path).map_err(|e| {
        onyx_chunklet::ChunkletError::Config(format!("read {}: {}", path.display(), e))
    })?;
    let parsed = toml::from_str(&s).map_err(|e| {
        onyx_chunklet::ChunkletError::Config(format!("parse {}: {}", path.display(), e))
    })?;
    Ok(Some(parsed))
}

fn validate_global(cli: &Cli, file: Option<&PerfFile>) -> ChunkletResult<()> {
    if cli.devices.is_empty()
        && file
            .and_then(|f| f.global.devices.as_ref())
            .map(|d| d.is_empty())
            .unwrap_or(true)
    {
        return Err(onyx_chunklet::ChunkletError::Config(
            "--devices or [global].devices required".into(),
        ));
    }
    if let Some(file) = file {
        if file.jobs.is_empty() {
            return Err(onyx_chunklet::ChunkletError::Config(
                "job file must contain at least one [[job]]".into(),
            ));
        }
    }
    Ok(())
}

fn validate_job(job: &BenchJob, _warmup_secs: u64) -> ChunkletResult<()> {
    if job.workers == 0 || job.iodepth == 0 {
        return Err(onyx_chunklet::ChunkletError::Config(format!(
            "job {} workers and iodepth must be > 0",
            job.name
        )));
    }
    if job.io_len == 0 {
        return Err(onyx_chunklet::ChunkletError::Config(format!(
            "job {} io size must be > 0",
            job.name
        )));
    }
    if job.random_align == 0 || job.random_align % BLOCK_SIZE != 0 {
        return Err(onyx_chunklet::ChunkletError::Config(format!(
            "job {} random alignment must be a non-zero multiple of {}",
            job.name, BLOCK_SIZE
        )));
    }
    if job.read_pct > 100 || job.hot_pct > 100 || job.hotset_pct > 100 {
        return Err(onyx_chunklet::ChunkletError::Config(format!(
            "job {} percentages must be <= 100",
            job.name
        )));
    }
    Ok(())
}

fn merged_devices(cli: &Cli, global: Option<&FileGlobal>) -> ChunkletResult<Vec<PathBuf>> {
    if !cli.devices.is_empty() {
        return Ok(cli.devices.clone());
    }
    global
        .and_then(|g| g.devices.clone())
        .ok_or_else(|| onyx_chunklet::ChunkletError::Config("devices required".into()))
}

fn ld_spec(raid: PerfRaid, width: u16, rows: u16, strip_log2: u8) -> ChunkletResult<LdSpec> {
    if width == 0 || rows == 0 {
        return Err(onyx_chunklet::ChunkletError::Config(
            "width and rows must be > 0".into(),
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

fn effective_working_set(
    capacity: u64,
    offset: u64,
    requested: u64,
    io_len: usize,
) -> ChunkletResult<u64> {
    if offset >= capacity {
        return Err(onyx_chunklet::ChunkletError::Config(format!(
            "offset {} >= capacity {}",
            offset, capacity
        )));
    }
    let available = capacity - offset;
    let work = if requested == 0 {
        available
    } else {
        requested.min(available)
    };
    if work < io_len as u64 {
        return Err(onyx_chunklet::ChunkletError::Config(format!(
            "working set {} is smaller than IO size {}",
            work, io_len
        )));
    }
    Ok((work / BLOCK_SIZE) * BLOCK_SIZE)
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
