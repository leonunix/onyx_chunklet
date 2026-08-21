//! `chunklet-submit-probe` — isolate the *shape* of chunklet's write submission
//! from everything else it does.
//!
//! A RAID6 full-stripe write pushes one strip-sized SQE per member and then
//! waits for all of them. The wait is therefore the MAX of K+2 device latencies,
//! so every member that finishes early idles until the thread comes back with
//! its next stripe. On nvme-box that shows up as a per-drive queue depth of ~9
//! while 64-256 worker threads are running (see the write_path phase ledger:
//! 65% of a call is that wait, and the stripe lock is 0.08%).
//!
//! Widening the batch does not help — it makes the barrier wider, so the wait
//! becomes the max of *more* latencies. The only untested alternative is
//! decoupling submission from completion. Doing that inside `write_many_at`
//! means keeping the parity strips alive past the call and holding stripe locks
//! across completions, which is a real refactor with a real use-after-free risk,
//! so measure the payoff FIRST.
//!
//! This probe reproduces only the submission pattern: N threads, one ring each,
//! `width` strip-sized writes per stripe, at the thread's own disjoint offset
//! per device. No RAID, no parity, no locks, no allocator.
//!
//!   barrier  — push `width` SQEs, wait for all `width`, repeat. What chunklet does.
//!   window   — keep `window` stripes' SQEs outstanding, reaping only enough to
//!              stay under the cap. What a cross-call async submit would allow,
//!              at the price of deferred error reporting.
//!   batch    — per "call", stream `batch` stripes through a `window`-deep
//!              pipeline and drain ALL of them before the call returns. Keeps
//!              today's synchronous contract (no deferred errors, parity strips
//!              only need to outlive one call) and pays only a `window`-deep
//!              drain tail per call, which is negligible when batch >> window.
//!              If this matches `window`, the async refactor does not need to
//!              change the caller-visible contract at all.
//!
//! ⚠ Writes directly to the named block devices. Destructive by design.

use std::os::unix::io::AsRawFd;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::{Parser, ValueEnum};
use io_uring::{opcode, types, IoUring};
use onyx_chunklet::io::AlignedBuf;

#[derive(Parser, Debug)]
#[command(name = "chunklet-submit-probe", about = "Barrier vs sliding-window submission")]
struct Cli {
    /// Block devices, comma separated. One strip per device per stripe.
    #[arg(long, value_delimiter = ',')]
    devices: Vec<String>,

    /// Strip size in KiB — the per-device request size.
    #[arg(long, default_value_t = 128)]
    strip_kib: usize,

    /// Submitting threads.
    #[arg(long, default_value_t = 64)]
    threads: usize,

    /// Stripes in flight per thread. Only used by `window`.
    #[arg(long, default_value_t = 8)]
    window: usize,

    #[arg(long, value_enum, default_value_t = Mode::Barrier)]
    mode: Mode,

    /// Stripes per simulated `write_many_at` call. Only used by `batch`.
    #[arg(long, default_value_t = 64)]
    batch: usize,

    #[arg(long, default_value_t = 16)]
    runtime_secs: u64,

    /// Bytes per thread per device. Threads never overlap.
    #[arg(long, default_value_t = 8 * 1024 * 1024 * 1024)]
    region_bytes: u64,

    /// Skip the first this many bytes of every device.
    #[arg(long, default_value_t = 1024 * 1024 * 1024)]
    offset_bytes: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum Mode {
    Barrier,
    Window,
    Batch,
}

fn main() {
    let cli = Cli::parse();
    if cli.devices.is_empty() {
        eprintln!("--devices is required");
        std::process::exit(2);
    }
    let strip = cli.strip_kib * 1024;
    let width = cli.devices.len();

    let files: Vec<std::fs::File> = cli
        .devices
        .iter()
        .map(|path| {
            use std::os::unix::fs::OpenOptionsExt;
            std::fs::OpenOptions::new()
                .write(true)
                .custom_flags(libc::O_DIRECT)
                .open(path)
                .unwrap_or_else(|e| panic!("open {path}: {e}"))
        })
        .collect();
    let fds: Arc<Vec<i32>> = Arc::new(files.iter().map(|f| f.as_raw_fd()).collect());

    let stop = Arc::new(AtomicBool::new(false));
    let stripes = Arc::new(AtomicU64::new(0));
    // Ring depth has to hold a whole window plus slack, or `window` silently
    // degrades into `barrier` when the submission queue fills.
    let depth = ((cli.window.max(1) * width * 2).next_power_of_two() as u32).max(64);

    let mut handles = Vec::new();
    for t in 0..cli.threads {
        let fds = Arc::clone(&fds);
        let stop = Arc::clone(&stop);
        let stripes = Arc::clone(&stripes);
        let (mode, window, batch) = (cli.mode, cli.window.max(1), cli.batch);
        let base = cli.offset_bytes + t as u64 * cli.region_bytes;
        let region = cli.region_bytes;
        handles.push(std::thread::spawn(move || {
            let mut ring = IoUring::new(depth).expect("io_uring init");
            // `barrier` is the window-of-1 special case, so one loop covers all
            // three modes: how deep the pipeline runs, and how often the call
            // boundary forces it empty.
            let w = if mode == Mode::Barrier { 1 } else { window };
            let drain_every = if mode == Mode::Batch { batch.max(1) } else { usize::MAX };
            // One buffer per in-flight strip. Contents are irrelevant to the
            // submission shape, but must not be all-zero in case anything
            // downstream ever compresses; a single fill is enough.
            let slots = w * width;
            let mut bufs: Vec<AlignedBuf> = (0..slots)
                .map(|i| {
                    let mut b = AlignedBuf::new(strip).expect("aligned buffer");
                    b.as_mut_slice().fill((i as u8) | 1);
                    b
                })
                .collect();
            let mut off = 0u64;
            let mut inflight = 0usize;
            let mut local = 0u64;
            let mut slot = 0usize;
            let mut since_drain = 0usize;

            while !stop.load(Ordering::Relaxed) {
                // Push one stripe: one strip-sized write per device.
                for (d, &fd) in fds.iter().enumerate() {
                    let buf = &bufs[slot + d];
                    let e = opcode::Write::new(
                        types::Fd(fd),
                        buf.as_slice().as_ptr(),
                        strip as u32,
                    )
                    .offset(base + off)
                    .build();
                    // SAFETY: `bufs` outlives the ring, and the buffer at this
                    // slot is not reused until its completion has been reaped —
                    // `barrier` reaps every stripe, `window` keeps at most
                    // `window` stripes' slots live and cycles through them.
                    unsafe {
                        while ring.submission().push(&e).is_err() {
                            ring.submit().expect("submit to drain the SQ");
                        }
                    }
                }
                inflight += width;
                off = (off + strip as u64) % region;
                slot = (slot + width) % slots;
                since_drain += 1;

                // A call boundary forces the pipeline empty; otherwise keep at
                // most `w` stripes outstanding so the slot we reuse next has
                // certainly been reaped.
                let target = if since_drain >= drain_every {
                    since_drain = 0;
                    0
                } else {
                    (w - 1) * width
                };
                while inflight > target {
                    // `submit_and_wait(n)` returns once at least n CQEs are ready.
                    ring.submit_and_wait(inflight - target)
                        .expect("submit_and_wait");
                    let mut reaped = 0usize;
                    for cqe in ring.completion() {
                        if cqe.result() < 0 {
                            panic!(
                                "write failed: {}",
                                std::io::Error::from_raw_os_error(-cqe.result())
                            );
                        }
                        reaped += 1;
                    }
                    inflight -= reaped;
                    local += reaped as u64;
                }
            }
            // Drain so the buffers are not freed under in-flight DMA.
            while inflight > 0 {
                ring.submit_and_wait(1).expect("drain");
                let mut reaped = 0usize;
                for cqe in ring.completion() {
                    reaped += 1;
                    let _ = cqe;
                }
                inflight -= reaped;
                local += reaped as u64;
            }
            drop(bufs);
            stripes.fetch_add(local, Ordering::Relaxed);
        }));
    }

    let started = Instant::now();
    std::thread::sleep(Duration::from_secs(cli.runtime_secs));
    stop.store(true, Ordering::Relaxed);
    for h in handles {
        h.join().expect("worker thread");
    }
    let secs = started.elapsed().as_secs_f64();
    let strips = stripes.load(Ordering::Relaxed);
    let bytes = strips * strip as u64;
    println!(
        "mode={:?} threads={} width={} strip={}KiB window={} => {:.0} MB/s ({:.2} GB/s), {} strips in {:.1}s",
        cli.mode,
        cli.threads,
        width,
        cli.strip_kib,
        if cli.mode == Mode::Barrier { 1 } else { cli.window },
        bytes as f64 / secs / 1e6,
        bytes as f64 / secs / 1e9,
        strips,
        secs,
    );
}
