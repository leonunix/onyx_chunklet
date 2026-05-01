# chunklet-perf

`chunklet-perf` is a fio-ish LD performance harness. It supports single-job
CLI runs and multi-job TOML files with verify, iodepth, mixed workloads, and
non-uniform random distributions.

## Build

```bash
cargo build --release --bin chunklet-perf
```

## Quick sparse-file smoke

```bash
tmpdir=$(mktemp -d /tmp/chunklet-perf.XXXXXX)
cargo run --release --bin chunklet-perf -- \
  --init \
  --devices "$tmpdir/pd0,$tmpdir/pd1,$tmpdir/pd2,$tmpdir/pd3" \
  --raid raid5 \
  --width 3 \
  --rows 1 \
  --sparse-size-bytes $((4*1024*1024*1024)) \
  --workload randrw \
  --read-pct 50 \
  --workers 4 \
  --iodepth 4 \
  --io-blocks 1 \
  --runtime-secs 30 \
  --warmup-secs 5 \
  --working-set-bytes $((1024*1024*1024)) \
  --random-dist hotspot \
  --hot-pct 80 \
  --hotset-pct 20
```

Add `--verify` when you want deterministic pattern verification. Verify
prefills the job working set before measurement and verifies reads plus
write-after-readback. It is intentionally slower.

## Multi-job TOML

```toml
[global]
devices = ["/dev/nvme0n1", "/dev/nvme1n1", "/dev/nvme2n1", "/dev/nvme3n1", "/dev/nvme4n1"]
init = true
backend = "uring"
runtime_secs = 120
warmup_secs = 15
report_secs = 5
spare_pct = 0

[[job]]
name = "raid5_seqwrite"
raid = "raid5"
width = 3
rows = 8
workload = "seqwrite"
workers = 4
iodepth = 4
io_blocks = 16

[[job]]
name = "mirror_hot_verify"
raid = "mirror"
width = 2
rows = 4
workload = "randrw"
read_pct = 70
workers = 2
iodepth = 8
io_blocks = 1
working_set_bytes = 1073741824
random_dist = "hotspot"
hot_pct = 90
hotset_pct = 10
verify = true
```

Run it with:

```bash
target/release/chunklet-perf --job-file perf.toml
```

Supported random distributions:

- `uniform`: flat random over the working set.
- `hotspot`: `hot_pct` percent of IOs target the first `hotset_pct` percent.
- `zipf`: simple skew toward low offsets.

## NVMe example

```bash
target/release/chunklet-perf \
  --init \
  --devices /dev/nvme0n1,/dev/nvme1n1,/dev/nvme2n1,/dev/nvme3n1 \
  --raid raid5 \
  --width 3 \
  --rows 8 \
  --backend uring \
  --workload seqwrite \
  --workers 8 \
  --io-blocks 16 \
  --runtime-secs 120 \
  --warmup-secs 15
```

## Output

The final summary is `key=value`, suitable for scripts:

```text
total.ops=6339
total.read_ops=3171
total.write_ops=3168
total.bytes=25964544
total.iops=6339.00
total.throughput_mib_s=24.76
total.avg_latency_us=314.88
total.p50_latency_us=272
total.p95_latency_us=782
total.p99_latency_us=1029
total.max_latency_us=3997
total.errors=0
job.raid5_seqwrite.ops=1464
```
