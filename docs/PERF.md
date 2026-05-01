# chunklet-perf

`chunklet-perf` is a focused LD performance harness. It avoids verification
and scrub work so the measured path is mostly chunklet IO.

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
  --io-blocks 1 \
  --runtime-secs 30 \
  --warmup-secs 5 \
  --working-set-bytes $((1024*1024*1024))
```

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
ops=6339
read_ops=3171
write_ops=3168
bytes=25964544
io_bytes=4096
iops=6339.00
throughput_mib_s=24.76
avg_latency_us=314.88
p50_latency_us=272
p95_latency_us=782
p99_latency_us=1029
max_latency_us=3997
errors=0
```
