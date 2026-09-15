//! CPU-only microbenchmark for the RAID6 P/Q kernels.
//!
//! WHY THIS EXISTS. `chunklet_perf` needs devices and measures the whole write
//! path, so it cannot separate "the parity math is slow" from "the submit is
//! slow". The onyx box ledger (2026-09-14, `chunklet_r6_batch`) put `compute` at
//! **30.8% of a RAID6 batched write call** — 11.624 ms for 264 stripes, i.e.
//! 44 us to turn 6 x 4 KiB of data into P+Q, which is ~558 MB/s of input. A
//! 64-byte-at-a-time `vpshufb` kernel should be an order of magnitude past that,
//! so the question is where those 44 us go — and answering it needs a harness
//! with no disks, no io_uring and no metadb in the way.
//!
//! It takes NO devices and touches nothing, so it is safe to run next to a live
//! engine (and `perf` is safe on it, unlike the live-ublk rule).
//!
//!   chunklet-parity-bench [--strips 6] [--strip-bytes 4096] [--iters 20000]
//!                         [--warmup 2000] [--working-set-mb 0] [--shuffle]
//!                         [--threads 1]
//!
//! ⭐ `--threads` tests the other half of the production context: the engine has
//! ~44 cores busy, and a Xeon Gold 5318Y runs 512-bit instructions at a much
//! lower all-core licence frequency than at 1-thread turbo, on top of whatever
//! memory bandwidth the other threads are taking. Each thread gets its OWN
//! working set so they do not share cache lines.
//!
//! ⭐ `--working-set-mb` is the whole point once the hot number is known. With
//! `0` the same stripe is re-read every iteration, so it lives in L1 and the
//! result measures the KERNEL. Production never does that: onyx hands chunklet
//! 6.3 MB of freshly-compressed payload per batched call, written by compress
//! workers on other cores, so every parity read is a cold line. Set the working
//! set past L3 (36 MiB on nvme-box) to measure that instead, and add `--shuffle`
//! to break the linear order the hardware prefetcher would otherwise ride.
//!
//! Defaults are the shape onyx actually drives on nvme-box: RAID6 6+2 with a
//! 4 KiB strip, so one call is one 24 KiB full stripe.
//!
//! Reported per arm:
//!   ns/call      wall time of one `encode_pq` (one stripe)
//!   MB/s in      strips x strip_bytes per second — comparable to the box's 558
//!   cyc/64B      cycles per inner vector iteration, the number that says
//!                whether the kernel is doing ~10 instructions or ~240
//!
//! `--freq-ghz` only scales the cyc/64B column; it does not affect the timing.

use onyx_chunklet::io::AlignedBuf;
use onyx_chunklet::ld::{gf256, parity};
use std::hint::black_box;
use std::time::Instant;

fn arg_usize(args: &[String], name: &str, default: usize) -> usize {
    args.iter()
        .position(|a| a == name)
        .and_then(|i| args.get(i + 1))
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn arg_f64(args: &[String], name: &str, default: f64) -> f64 {
    args.iter()
        .position(|a| a == name)
        .and_then(|i| args.get(i + 1))
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// Deterministic non-trivial bytes. A constant fill would let the GF multiply
/// hit its `s == 0` scalar short-circuit and flatter the legacy arm.
fn stream(seed: u64, len: usize) -> Vec<u8> {
    let mut s = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    (0..len)
        .map(|_| {
            s ^= s << 13;
            s ^= s >> 7;
            s ^= s << 17;
            (s >> 24) as u8
        })
        .collect()
}

struct Arm {
    name: &'static str,
    ns_per_call: f64,
    mb_in: f64,
    cyc_per_64b: f64,
}

/// Read strips and P/Q scratch drawn from a working set big enough to miss
/// cache. `data` and `pq` are separate allocations so the six read slices and
/// the two written ones never alias.
struct WorkingSet {
    data: Vec<u8>,
    pq: Vec<u8>,
    /// Stripe order. Linear unless `--shuffle`.
    order: Vec<usize>,
    strips: usize,
    strip_bytes: usize,
}

impl WorkingSet {
    fn new(mb: usize, strips: usize, strip_bytes: usize, shuffle: bool) -> Self {
        let per_stripe = strips * strip_bytes;
        let n = ((mb * 1024 * 1024) / per_stripe).max(1);
        let mut data = vec![0u8; n * per_stripe];
        // Fill with the same non-trivial stream the hot arm uses, so the GF
        // multiply cannot hit its zero short-circuit.
        let mut s = 0x5EEDu64 | 1;
        for b in data.iter_mut() {
            s ^= s << 13;
            s ^= s >> 7;
            s ^= s << 17;
            *b = (s >> 24) as u8;
        }
        let mut order: Vec<usize> = (0..n).collect();
        if shuffle {
            // Deterministic Fisher-Yates; a fixed permutation keeps arms
            // comparable while defeating the streaming prefetcher.
            let mut r = 0x243F_6A88_85A3_08D3u64;
            for i in (1..n).rev() {
                r ^= r << 13;
                r ^= r >> 7;
                r ^= r << 17;
                order.swap(i, (r % (i as u64 + 1)) as usize);
            }
        }
        Self {
            data,
            pq: vec![0u8; n * 2 * strip_bytes],
            order,
            strips,
            strip_bytes,
        }
    }
}

fn run_arm_cold(
    name: &'static str,
    fused: bool,
    ws: &mut WorkingSet,
    iters: usize,
    warmup: usize,
    freq_ghz: f64,
) -> Arm {
    parity::set_fused(fused);
    let n = ws.order.len();
    let per_stripe = ws.strips * ws.strip_bytes;
    let sb = ws.strip_bytes;

    let mut go = |count: usize| {
        for it in 0..count {
            let st = ws.order[it % n];
            let base = st * per_stripe;
            // Split the two allocations so the borrow checker sees disjoint
            // read and write halves.
            let src = &ws.data[base..base + per_stripe];
            let data: Vec<(&[u8], u8)> = (0..ws.strips)
                .map(|k| (&src[k * sb..(k + 1) * sb], gf256::g_pow(k)))
                .collect();
            let pq_base = st * 2 * sb;
            let (p, q) = ws.pq[pq_base..pq_base + 2 * sb].split_at_mut(sb);
            parity::encode_pq(p, q, black_box(&data));
            black_box((&p, &q));
        }
    };

    go(warmup.min(n));
    let started = Instant::now();
    go(iters);
    let elapsed = started.elapsed().as_secs_f64();

    let bytes_in = per_stripe as f64 * iters as f64;
    let inner_iters = (sb / 64) as f64 * iters as f64;
    Arm {
        name,
        ns_per_call: elapsed * 1e9 / iters as f64,
        mb_in: bytes_in / elapsed / 1e6,
        cyc_per_64b: elapsed * freq_ghz * 1e9 / inner_iters,
    }
}

/// Production's exact allocation shape: `p`/`q` are a FRESH `AlignedBuf::uninit`
/// per stripe (`Seg6` allocates them in the plan phase and never touches them),
/// so `encode_pq`'s write is their first touch and any minor fault lands inside
/// the `compute` phase. The other arms reuse pre-faulted buffers, which is the
/// one structural difference left between this bench and `r6_compute`.
fn run_arm_fresh_pq(
    name: &'static str,
    ws: &mut WorkingSet,
    iters: usize,
    warmup: usize,
    freq_ghz: f64,
) -> Arm {
    parity::set_fused(true);
    let n = ws.order.len();
    let per_stripe = ws.strips * ws.strip_bytes;
    let sb = ws.strip_bytes;

    let mut go = |count: usize| {
        for it in 0..count {
            let base = ws.order[it % n] * per_stripe;
            let src = &ws.data[base..base + per_stripe];
            let data: Vec<(&[u8], u8)> = (0..ws.strips)
                .map(|k| (&src[k * sb..(k + 1) * sb], gf256::g_pow(k)))
                .collect();
            let mut p = AlignedBuf::uninit(sb).expect("p");
            let mut q = AlignedBuf::uninit(sb).expect("q");
            parity::encode_pq(p.as_mut_slice(), q.as_mut_slice(), black_box(&data));
            black_box((&p, &q));
        }
    };

    go(warmup.min(n));
    let started = Instant::now();
    go(iters);
    let elapsed = started.elapsed().as_secs_f64();
    Arm {
        name,
        ns_per_call: elapsed * 1e9 / iters as f64,
        mb_in: per_stripe as f64 * iters as f64 / elapsed / 1e6,
        cyc_per_64b: elapsed * freq_ghz * 1e9 / ((sb / 64) as f64 * iters as f64),
    }
}

fn run_arm(
    name: &'static str,
    fused: bool,
    strips: &[Vec<u8>],
    p: &mut [u8],
    q: &mut [u8],
    iters: usize,
    warmup: usize,
    freq_ghz: f64,
) -> Arm {
    let data: Vec<(&[u8], u8)> = strips
        .iter()
        .enumerate()
        .map(|(i, s)| (s.as_slice(), gf256::g_pow(i)))
        .collect();
    parity::set_fused(fused);

    for _ in 0..warmup {
        parity::encode_pq(p, q, black_box(&data));
        black_box((&p, &q));
    }
    let started = Instant::now();
    for _ in 0..iters {
        parity::encode_pq(p, q, black_box(&data));
        black_box((&p, &q));
    }
    let elapsed = started.elapsed().as_secs_f64();

    let strip_bytes = p.len();
    let bytes_in = (strips.len() * strip_bytes) as f64 * iters as f64;
    let ns_per_call = elapsed * 1e9 / iters as f64;
    // One inner vector iteration covers 64 bytes of every strip at once, so the
    // per-iteration cost is what the kernel's instruction count has to explain.
    let inner_iters = (strip_bytes / 64) as f64 * iters as f64;
    Arm {
        name,
        ns_per_call,
        mb_in: bytes_in / elapsed / 1e6,
        cyc_per_64b: elapsed * freq_ghz * 1e9 / inner_iters,
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let n_strips = arg_usize(&args, "--strips", 6);
    let strip_bytes = arg_usize(&args, "--strip-bytes", 4096);
    let iters = arg_usize(&args, "--iters", 20_000);
    let warmup = arg_usize(&args, "--warmup", 2_000);
    let freq_ghz = arg_f64(&args, "--freq-ghz", 2.1);
    let ws_mb = arg_usize(&args, "--working-set-mb", 0);
    let shuffle = args.iter().any(|a| a == "--shuffle");
    let threads = arg_usize(&args, "--threads", 1).max(1);

    if threads > 1 {
        // Per-thread working set, so the only thing shared is the machine: the
        // AVX-512 all-core licence frequency and the memory system.
        let per_thread_mb = if ws_mb > 0 { ws_mb } else { 0 };
        println!(
            "parity_bench: THREADS={threads} strips={n_strips} strip_bytes={strip_bytes} \
             iters={iters} working_set_mb={per_thread_mb}/thread shuffle={shuffle}"
        );
        parity::set_fused(true);
        let results: Vec<Arm> = std::thread::scope(|sc| {
            let handles: Vec<_> = (0..threads)
                .map(|_| {
                    sc.spawn(move || {
                        if per_thread_mb > 0 {
                            let mut w =
                                WorkingSet::new(per_thread_mb, n_strips, strip_bytes, shuffle);
                            run_arm_cold("cold", true, &mut w, iters, warmup, freq_ghz)
                        } else {
                            let strips: Vec<Vec<u8>> = (0..n_strips)
                                .map(|i| stream(0x5EED + i as u64, strip_bytes))
                                .collect();
                            let mut p = vec![0u8; strip_bytes];
                            let mut q = vec![0u8; strip_bytes];
                            run_arm(
                                "hot", true, &strips, &mut p, &mut q, iters, warmup, freq_ghz,
                            )
                        }
                    })
                })
                .collect();
            handles.into_iter().map(|h| h.join().unwrap()).collect()
        });
        let n = results.len() as f64;
        let mean_ns: f64 = results.iter().map(|a| a.ns_per_call).sum::<f64>() / n;
        let mean_mb: f64 = results.iter().map(|a| a.mb_in).sum::<f64>() / n;
        let min_mb = results.iter().map(|a| a.mb_in).fold(f64::MAX, f64::min);
        let max_mb = results.iter().map(|a| a.mb_in).fold(0.0, f64::max);
        println!();
        println!(
            "  per-thread mean {:.0} ns/call  {:.0} MB/s in  (spread {:.0}-{:.0})",
            mean_ns, mean_mb, min_mb, max_mb
        );
        println!("  AGGREGATE {:.0} MB/s in over {threads} threads", mean_mb * n);
        println!();
        println!("reference: production is 558 MB/s in AGGREGATE (0.87 cores of compute),");
        println!("           so compare the PER-THREAD column against 558, not the aggregate.");
        return;
    }

    if std::env::var_os("CHUNKLET_FUSED_PARITY").is_some() {
        eprintln!(
            "warning: CHUNKLET_FUSED_PARITY is set and overrides set_fused(), so the \
             fused/legacy arms below will both measure whichever it selects"
        );
    }

    println!(
        "parity_bench: strips={n_strips} strip_bytes={strip_bytes} iters={iters} \
         warmup={warmup} freq={freq_ghz} GHz"
    );
    println!("  (one call = one full stripe: {n_strips} x {strip_bytes} B in, 2 x {strip_bytes} B out)");

    let strips: Vec<Vec<u8>> = (0..n_strips)
        .map(|i| stream(0x5EED + i as u64, strip_bytes))
        .collect();
    let mut p = vec![0u8; strip_bytes];
    let mut q = vec![0u8; strip_bytes];
    let mut ws = (ws_mb > 0).then(|| {
        let w = WorkingSet::new(ws_mb, n_strips, strip_bytes, shuffle);
        println!(
            "  cold working set: {} stripes over {:.1} MiB data + {:.1} MiB pq, order={}",
            w.order.len(),
            w.data.len() as f64 / 1048576.0,
            w.pq.len() as f64 / 1048576.0,
            if shuffle { "shuffled" } else { "linear" }
        );
        w
    });

    let before = parity::stats_snapshot();
    // Interleave the arms rather than running each once: a cold first arm would
    // otherwise absorb page faults and frequency ramp for the whole run.
    let mut arms = Vec::new();
    for round in 0..2 {
        let f = run_arm(
            if round == 0 { "fused (auto)" } else { "fused (auto) #2" },
            true,
            &strips,
            &mut p,
            &mut q,
            iters,
            warmup,
            freq_ghz,
        );
        let l = run_arm(
            if round == 0 {
                "legacy two-operand"
            } else {
                "legacy two-operand #2"
            },
            false,
            &strips,
            &mut p,
            &mut q,
            iters,
            warmup,
            freq_ghz,
        );
        arms.push(f);
        arms.push(l);
        if let Some(w) = ws.as_mut() {
            arms.push(run_arm_fresh_pq(
                if round == 0 {
                    "COLD + fresh pq"
                } else {
                    "COLD + fresh pq #2"
                },
                w,
                iters,
                warmup,
                freq_ghz,
            ));
            arms.push(run_arm_cold(
                if round == 0 { "fused COLD" } else { "fused COLD #2" },
                true,
                w,
                iters,
                warmup,
                freq_ghz,
            ));
        }
    }
    parity::set_fused(true);
    let after = parity::stats_snapshot();

    println!();
    println!(
        "{:<24} {:>10} {:>10} {:>10}",
        "arm", "ns/call", "MB/s in", "cyc/64B"
    );
    for a in &arms {
        println!(
            "{:<24} {:>10.0} {:>10.0} {:>10.1}",
            a.name, a.ns_per_call, a.mb_in, a.cyc_per_64b
        );
    }

    println!();
    println!("dispatch (delta over the whole run) — proves WHICH kernel ran:");
    println!(
        "  encode avx512={} avx2={} scalar={} legacy={}  src_bytes={}",
        after.encode_calls_avx512 - before.encode_calls_avx512,
        after.encode_calls_avx2 - before.encode_calls_avx2,
        after.encode_calls_scalar - before.encode_calls_scalar,
        after.encode_calls_legacy - before.encode_calls_legacy,
        after.encode_src_bytes - before.encode_src_bytes,
    );
    let g = gf256::stats_snapshot();
    println!(
        "  gf256 xor avx512={} avx2={} scalar={} | mul avx512={} avx2={} scalar={}",
        g.xor_avx512_calls,
        g.xor_avx2_calls,
        g.xor_scalar_calls,
        g.mul_avx512_calls,
        g.mul_avx2_calls,
        g.mul_scalar_calls,
    );
    println!();
    println!("reference: the nvme-box onyx ledger measured this stage at 558 MB/s in");
    println!("           (compute 11.624 ms / 264 stripes = 44 us per 24 KiB stripe)");
}
