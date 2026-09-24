# Reliability changes: performance comparison

The focused fixes improve cached point reads, replay, and recovery while retaining
one data sync per steady-state commit group. Durable-write throughput is essentially
unchanged in this workload. The v3 format, record hashes, and sync-before-success
guarantee remain intact. Durable writes of larger records gain from zero-filled
allocation windows; see [Allocation windows](#allocation-windows).

## Measurements

Measured 2026-09-19 against main at `6bc7aed2d4870e124c0cb10553685145ca39d635`.
Five alternating baseline/current pairs used release builds and the **same updated
[benchmark harness](examples/bench.rs)** in both trees. Values are median records/s;
the range is the minimum–maximum across five runs. Higher is better.

| Scenario | Main median (range) | This change median (range) | Median delta |
| --- | ---: | ---: | ---: |
| Single-record durable writes | 461 (454–466) | 462 (456–464) | +0.2% |
| 1,000-record batch writes | 220,017 (196,056–233,460) | 217,604 (212,426–221,389) | −1.1% |
| Cached point reads | 727,866 (708,982–730,549) | 4,968,859 (4,384,111–5,034,767) | **6.83×** |
| Sequential replay | 44,636,174 (43,270,308–45,209,290) | 50,659,553 (49,879,434–52,035,358) | **+13.5%** |
| Active-file recovery/startup | 3,770,879 (3,569,907–4,127,041) | 5,281,263 (5,115,745–6,867,890) | **+40.1%** |
| Eight concurrent durable writers | 1,789 (1,336–1,828) | 1,809 (1,781–1,818) | +1.1% |

The write-rate differences are small relative to run variation. These measurements
do not establish a write-throughput improvement. Raw outputs and machine details
are retained in [docs/performance-results.json](docs/performance-results.json).

Environment: Intel Core Ultra 9 285, Linux x86-64, ext4 on `/dev/nvme0n1p2`,
Rust 1.97.0. WAL files were created on the repository's filesystem and removed
after each run. Builds and tests were not run concurrently with the comparison.

All payloads are 64 bytes. Each run performs 2,000 sequential writes, 100 batches
of 1,000 records, ten point-read passes over those 100,000 records, twenty replay
passes, one full restart of that active file, and eight threads each issuing 250
single-record writes. Recovery includes opening/recovering/restarting the manager,
including its new directory barriers; it excludes dropping the old manager.
Both builds used 64 MiB sparse preallocation, the default at the time.

Read and recovery data are warm in the page cache. Repeated read/replay passes
reduce the scheduling and CPU-frequency noise of the original few-millisecond
samples. This is a single-machine comparison, not a cold-storage or power-loss
experiment. Directory durability is additionally checked through ordering and
failure/retry tests.

## Deterministic costs

| Operation | Review baseline | This change | Regression evidence |
| --- | --- | --- | --- |
| Cached active-file point read, 64-byte payload | 4 reads + 1 stat | **1 read, 0 stats** | `snapshot_ends_at_creation_and_point_reads_skip_commit_metadata` in [tests/reliability.rs](tests/reliability.rs) |
| Recovery, 1,000 × 64-byte records | 1,004 reads | **5 reads** including metadata | [tests/recovery_cost.rs](tests/recovery_cost.rs); gate ≤16 |
| Recovery allocations | Owned payload allocated per record | **1 allocation** for both 1 and 1,000 records | Calling-thread allocator counter in [tests/recovery_cost.rs](tests/recovery_cost.rs) |
| Steady-state commit | 1 data sync | **1 data sync** | `steady_state_commit_uses_one_data_sync` in [tests/preallocation.rs](tests/preallocation.rs) |

The baseline operation counts were recorded during the review; the allocation
description is from the baseline decoder. Current counts were rerun after the
simplification pass. Recovery shares the stream buffer and verifies borrowed
payload slices; public reads/streams still return owned record data.

## Allocation windows

Measured 2026-09-24 on the same machine and filesystem (Linux 7.0, ext4 with
`data=ordered`). A script repeated the writer's commit: an 8 KiB record write at
the record end, a committed-end slot write, then `fdatasync`. Journal commits
come from `/proc/fs/jbd2/<device>/info`. 1,500 commits per row:

| Space past the record end | Commits/s | p50 | p99 | Journal commits per commit |
| --- | ---: | ---: | ---: | ---: |
| None (each write extends the file) | 234 | 4.19 ms | 6.06 ms | 1.00 |
| Sparse (`set_len`) | 234 | 4.17 ms | 5.92 ms | 1.00 |
| `fallocate` | 236 | 4.15 ms | 5.99 ms | 1.00 |
| Zero-filled in advance | 494 | 1.99 ms | 2.39 ms | 0.00 |

A write into a hole allocates blocks, and a write into a `fallocate`d range
converts unwritten extents; either way `fdatasync` must commit the journal to
make the change durable. Only blocks that already hold data avoid the commit.

The writer zero-fills inline: the commit that crosses into a new window writes
its zeros first, and that commit's `fdatasync` persists both. 3,000 commits per
row:

| Window | Commits/s | p99 | p99.9 | Max | Journal commits per commit |
| --- | ---: | ---: | ---: | ---: | ---: |
| 64 KiB | 421 | 4.97 ms | 6.63 ms | 16.27 ms | 0.126 |
| 256 KiB | 472 | 4.72 ms | 6.22 ms | 8.01 ms | 0.032 |
| 1 MiB | 484 | 3.93 ms | 6.05 ms | 9.41 ms | 0.009 |
| 4 MiB | 481 | 2.49 ms | 7.27 ms | 15.20 ms | 0.002 |

Through the crate with default options, 3,000 sequential 8 KiB writes ran at
441–446 writes/s (p50 2.14 ms, 0.03 journal commits per write), against 233
writes/s (p50 4.28 ms, 1.00) with 64 MiB sparse windows.

On an idle disk, 256 KiB and 1 MiB windows perform alike. A downstream consumer
running several writers on one filesystem saw 1 MiB windows raise maximum
latency to about 60 ms while 256 KiB windows held, which sets the default.

## Reproduce

Run `cargo run --release --example bench` from the repository on real storage.
For a comparison, use this change's harness in the baseline tree as well:

```bash
(
  set -eu
  baseline_dir=$(mktemp -d)
  trap 'rm -rf -- "$baseline_dir"' EXIT
  git archive 6bc7aed2d4870e124c0cb10553685145ca39d635 Cargo.toml Cargo.lock src examples |
    tar -x -C "$baseline_dir"
  cp examples/bench.rs "$baseline_dir/examples/bench.rs"
  cargo build --release --locked --example bench
  CARGO_TARGET_DIR="$baseline_dir/target" cargo build --release --locked \
    --manifest-path "$baseline_dir/Cargo.toml" --example bench
  for pair in 1 2 3 4 5; do
    if [ "$((pair % 2))" -eq 1 ]; then
      "$baseline_dir/target/release/examples/bench"
      ./target/release/examples/bench
    else
      ./target/release/examples/bench
      "$baseline_dir/target/release/examples/bench"
    fi
  done
)
```

Run the operation-count fixture with
`cargo test --test recovery_cost -- --nocapture`.
