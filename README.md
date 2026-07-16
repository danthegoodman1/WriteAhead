# WriteAhead

A partitioned WAL crate for building append-only, high-throughput durability systems.

A great component for a (distributed) data store. For distributed usage, consider FDB's model (you may want a custom in-memory index in front for custom ID→offset mappings).

## Guarantees

1. **Durability** — a write is acknowledged only after it is fdatasync'd to the active log file.
2. **Corruption detection** — every record carries a 128-bit murmur3 hash of its data; any bit-flip in a record (including its length header, caught via bounds check + hash mismatch) is detected on read.
3. **Crash recovery** — `start()` restores a consistent WAL after a crash at any point: every file is validated, torn tails are truncated, files that missed their seal mid-rotation are healed and sealed, and sealed files are never appended to. Every acknowledged record survives.
4. **Stable addressing** — `RecordID { file_id, file_offset }` stays valid for the life of the file (until retention deletes it).
5. **Sequential streaming** — a stream replays all records across files in write order.

## Usage

```rust
use writeahead::{WriteAhead, WriteAheadOptions, SimpleFile};

let mut wal = WriteAhead::<SimpleFile>::with_options(WriteAheadOptions {
    log_dir: "./write_ahead".into(),
    ..Default::default()
});
wal.start()?; // recovers existing state, must crash on error

// Durable batch write (one pwrite + one fdatasync per batch)
let ids = wal.write_batch(vec![b"hello".to_vec()]).await?;

// Point read by address
let record = wal.read(ids[0].file_id, ids[0].file_offset)?;

// Full replay
use futures::StreamExt;
let mut stream = wal.create_stream()?;
while let Some(record) = stream.next().await {
    let record = record?;
    // ...
}
```

Retention is opt-in via `RetentionOptions`: `max_total_size` deletes the oldest sealed files once total disk usage exceeds the bound, and `ttl` deletes sealed files whose seal timestamp is older than the window. The active file is never deleted; reads into deleted files return `LogfileNotFound`.

## Design

### Single writer, page-cache readers

One dedicated writer thread owns the active file; callers talk to it over a channel, and `write_batch` awaits the response without blocking the async executor. Writes queued while an fsync is in flight are **group-committed**: the actor drains them into a single pwrite + fdatasync and fans replies back out, so concurrent producers coalesce automatically (see `actor_concurrent_8w` in the benchmarks).

Readers use positional reads (`pread`) straight from the page cache — for WAL access patterns readers mostly want recently written data, which the page cache serves far better than direct-IO schemes like io_uring + O_DIRECT. Point reads speculatively fetch one small block so most records cost a single syscall; streams parse records out of 128 KiB readahead chunks.

### File format (version 2)

```text
| 8B magic | 1B version | records ... | 40B seal footer (sealed files only) |

record: | 16B murmur3_128(data) | 4B length (u32 LE) | data |
footer: | 8B magic | 8B seal timestamp (unix ms) | 8B records-end offset | 16B murmur3_128 of the previous 24B |
```

Data is stored raw — no escaping. A file is sealed iff its trailing 40 bytes parse as a footer whose own hash verifies and whose records-end field equals the footer's position, so record payloads (which may contain the magic bytes) cannot be mistaken for a seal.

The log directory is a sequence of `0000000000.log`, `0000000001.log`, … files. The highest id is the active file; rotation seals the current file (footer carries the seal timestamp used by ttl retention) and starts the next one. Directory entries are fsync'd on file creation.

### Recovery

On `start()`:

- Non-active files that aren't sealed (crash mid-rotation, torn footer) are healed: valid record prefix kept, garbage truncated, footer written.
- The active file gets a tail scan; a torn last record is truncated away. Since writes are only acknowledged after fsync, nothing acknowledged is ever lost.
- If the active file turns out to be sealed (crash between seal and next-file creation), a fresh file is started rather than appending to it.
- Non-log files in the directory are skipped.

## Benchmarks

`cargo run --release --example bench` (writes to the current directory — don't run it on tmpfs, where fsync is free and write numbers become fiction).

Snapshot from a desktop NVMe/ext4 box, 64-byte payloads:

| scenario | what it measures | rate |
| --- | --- | --- |
| `seq_write_1rec` | one record per `write_batch`, strict ack-then-next: the floor set by one fdatasync per write | ~230/s (~4.4ms/op) |
| `batch_write` | 1,000-record batches through `write_batch`: one pwrite + one fdatasync per batch | ~225k records/s |
| `read_by_id` | point reads of individual records by `RecordID`, served from the page cache | ~2.1M/s |
| `stream_replay` | full sequential replay of the log through `create_stream()` | ~34M/s (~2.1GB/s) |
| `actor_concurrent_8w` | 8 threads each submitting single-record writes to one writer actor and awaiting their acks; queued writes share fsyncs via group commit | ~950/s |

Durable-write rates are dominated by fdatasync latency, so they scale with batch/group size, not payload size — compare `seq_write_1rec` (1 record per sync) against `batch_write` (1,000 per sync) and `actor_concurrent_8w` (however many are queued per sync).

## Integrating with a thread-safe API framework (Axum, tonic, etc.)

`WriteAhead` has a single-owner API to keep the write path contention-free. Spawn a task/thread that owns it and communicate over a channel; you can micro-batch incoming writes (e.g. at most 500µs) into `write_batch` calls for extra throughput — and writes that arrive while an fsync is in flight already coalesce via group commit.

`WriteAheadStream` is `Send` and owns its file handles, so you can create a stream and hand it to a response body directly (see `test_write_ahead_stream_is_send`). Use a bounded channel if you bridge it manually so you don't bloat memory.
