# WriteAhead

[![crates.io](https://img.shields.io/crates/v/writeahead.svg)](https://crates.io/crates/writeahead)
[![docs.rs](https://docs.rs/writeahead/badge.svg)](https://docs.rs/writeahead)
[![CI](https://github.com/danthegoodman1/WriteAhead/actions/workflows/ci.yml/badge.svg)](https://github.com/danthegoodman1/WriteAhead/actions/workflows/ci.yml)

A partitioned WAL crate for building append-only, high-throughput durability systems.

A great component for a (distributed) data store. For distributed usage, consider FDB's model (you may want a custom in-memory index in front for custom ID→offset mappings).

## Guarantees

1. **Durability** — a write is acknowledged only after it is fdatasync'd to the active log file.
2. **Corruption detection** — every record carries a 128-bit murmur3 hash of its data; any bit-flip in a record (including its length header, caught via bounds check + hash mismatch) is detected on read.
3. **Crash recovery** — `start()` restores a consistent WAL after a crash at any point: every file is validated, torn tails are truncated, files that missed their seal mid-rotation are healed and sealed, and sealed files are never appended to. Every acknowledged record survives.
4. **Stable addressing** — `RecordID { file_id, file_offset }` stays valid for the life of the file (until retention deletes it).
5. **Sequential streaming** — a stream replays all records across files in write order, yielding each record with its `RecordID` so consumers can checkpoint their position.

## Quickstart

```toml
[dependencies]
writeahead = "0.2"
anyhow = "1"
futures = "0.3"
tokio = { version = "1", features = ["rt", "macros"] }
```

(Or track main directly: `writeahead = { git = "https://github.com/danthegoodman1/WriteAhead" }`.)

The full program below lives at [`examples/quickstart.rs`](examples/quickstart.rs) and runs with `cargo run --example quickstart`:

```rust
use futures::StreamExt;
use std::sync::Arc;
use writeahead::{SimpleFile, WriteAhead, WriteAheadOptions};

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let mut wal = WriteAhead::<SimpleFile>::with_options(WriteAheadOptions {
        log_dir: "./write_ahead".into(),
        ..Default::default()
    });
    wal.start()?; // recovers existing state; if this errors, crash
    let wal = Arc::new(wal); // WriteAhead is Sync: share it directly

    // Durable writes from any task or thread: clone a WriteHandle per
    // producer. Writes that arrive while an fsync is in flight coalesce
    // into one group commit automatically.
    let handle = wal.writer()?;
    let id = handle.write(b"hello".to_vec()).await?;
    let more = handle
        .write_batch(vec![b"a".to_vec(), b"b".to_vec()])
        .await?;
    println!("wrote {:?} and {:?}", id, more);

    // Point read by address
    let record = wal.read(id.file_id, id.file_offset)?;
    assert_eq!(record, b"hello");

    // Full replay, oldest first; each record comes with its address so
    // consumers can checkpoint how far they've gotten
    let mut stream = wal.create_stream()?;
    while let Some(entry) = stream.next().await {
        let (id, record) = entry?;
        println!("replayed {} bytes from {:?}", record.len(), id);
    }

    Ok(())
}
```

Retention is opt-in via `RetentionOptions`: `max_total_size` deletes the oldest sealed files once total disk usage exceeds the bound, and `ttl` deletes sealed files whose seal timestamp is older than the window. The active file is never deleted; reads into deleted files return `LogfileNotFound`. For consumer-driven cleanup, use `trim_before` instead — see the patterns below.

## Patterns

The WAL never decides on its own that a record is *done* — that knowledge lives with whatever consumes the log. `trim_before(file_id)` is the primitive that lets the consumer feed that knowledge back: it deletes every sealed file strictly below `file_id` (the active file is never touched) and returns how many files and bytes were reclaimed. Two common shapes, both runnable from `examples/`:

### Trim by consumer high-water mark

`cargo run --example trim_hwm` ([examples/trim_hwm.rs](examples/trim_hwm.rs))

A replaying consumer tracks the `RecordID` of the last record it has applied — its high-water mark. Records are yielded in order, so the moment the mark crosses into file `N`, every file below `N` is fully consumed and can be dropped:

```rust
let mut hwm: Option<RecordID> = None;
let mut stream = wal.create_stream()?;
while let Some(entry) = stream.next().await {
    let (id, record) = entry?;
    apply(&record); // your state machine / downstream sink

    if let Some(prev) = hwm {
        if id.file_id > prev.file_id {
            // Everything below the file we just entered is consumed
            handle.trim_before(id.file_id).await?;
        }
    }
    hwm = Some(id);
}
```

Checkpoint the mark durably (wherever your consumer state lives), and a restart resumes with `create_stream_from(hwm.file_id, hwm.file_offset)` instead of replaying from the beginning. Trim only *after* the checkpoint is durable — the trim is what makes the pre-mark log unrecoverable. Streams created before a trim are undisturbed (they hold their own open file handles); new reads into trimmed files return `LogfileNotFound`.

### Explicit compaction

`cargo run --example compaction` ([examples/compaction.rs](examples/compaction.rs))

The db-traditional shape, for when the log is the backing store of keyed state (an index maps each key to the `RecordID` of its latest value). Old files can't be trimmed by a high-water mark because a handful of cold, still-live records pin them while dead versions pile up around. Compaction rewrites the live records forward, then drops everything behind:

1. Pick a cutoff file id (e.g. the newest file the index points into).
2. Read every live record below the cutoff and `write_batch` the copies — they land in the active file, and the index is repointed at the new `RecordID`s.
3. `trim_before(cutoff)` — nothing below it is referenced anymore.

The rewrite goes through the normal durable write path, so a crash at any point is safe: before the trim, both copies exist and the index (rebuilt by replay or checkpointed) still resolves; the trim only happens after the new copies are acknowledged.

## Design

### Single writer, page-cache readers

One dedicated writer thread owns the active file and the directory lifecycle: writes, rotation, and retention all happen there, so nothing ever contends on the write path. Any number of tasks submit through cloned `WriteHandle`s and await their acks without blocking the async executor. Writes queued while an fsync is in flight are **group-committed**: the writer drains them into a single pwrite + fdatasync and fans replies back out, so concurrent producers coalesce automatically (see `handle_concurrent_8w` in the benchmarks). Each ack names the file the records landed in, so handles stay valid across rotations.

Readers use positional reads (`pread`) straight from the page cache — for WAL access patterns readers mostly want recently written data, which the page cache serves far better than direct-IO schemes like io_uring + O_DIRECT. Point reads speculatively fetch one small block so most records cost a single syscall; streams parse records out of 128 KiB readahead chunks.

### File format (version 2)

```text
| 8B magic | 1B version | records ... | 40B seal footer (sealed files only) |

record: | 16B murmur3_128(data) | 4B length (u32 LE) | data |
footer: | 8B magic | 8B seal timestamp (unix ms) | 8B records-end offset | 16B murmur3_128 of the previous 24B |
```

Data is stored raw — no escaping. A file is sealed iff its trailing 40 bytes parse as a footer whose own hash verifies and whose records-end field equals the footer's position, so record payloads (which may contain the magic bytes) cannot be mistaken for a seal.

The log directory is a sequence of `0000000000.log`, `0000000001.log`, … files. The highest id is the active file; rotation creates the next file first, then seals the previous one (the footer carries the seal timestamp used by ttl retention), so a crash mid-rotation always lands in a state recovery heals. Directory entries are fsync'd on file creation.

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
| `handle_concurrent_8w` | 8 threads each writing through cloned public `WriteHandle`s and awaiting their acks; queued writes share fsyncs via group commit | ~940/s |

Durable-write rates are dominated by fdatasync latency, so they scale with batch/group size, not payload size — compare `seq_write_1rec` (1 record per sync) against `batch_write` (1,000 per sync) and `handle_concurrent_8w` (however many are queued per sync).

### Throughput scales with in-flight writes

`handle_concurrent_8w` is not a ceiling — it's the throughput of exactly 8 in-flight writers. Group commit converts *concurrency* into batch size: every write queued while an fsync is in flight rides the next one, so single-record `handle.write()` calls (each individually fsync'd before its ack) scale with the number of concurrent writers. Measured on the same box:

| concurrent writers | durable writes/s |
| --- | --- |
| 8 | ~950 |
| 64 | ~7.2k |
| 512 | ~79k |
| 2,048 | ~290k |

Per-write latency stays at flush scale (a few ms) throughout — concurrency buys throughput, never latency. If your data already arrives in batches, `write_batch` is the same lever with bigger settings: ~760k records/s at 4,096-record batches and ~2M records/s at 16,384 on this disk, every record fsync'd before ack.

## Integrating with a thread-safe API framework (Axum, tonic, etc.)

Put the started `WriteAhead` in an `Arc` and share it — it's `Sync`. Clone a `WriteHandle` into each request handler (or once into your app state) and call `handle.write(...)` directly; there is no owner task to build and no upstream batching needed, because writes that arrive while a commit is in flight coalesce into the next group commit automatically.

`WriteAheadStream` is `Send` and owns its file handles, so you can create a stream and hand it to a response body directly (see `test_write_ahead_stream_is_send`). Use a bounded channel if you bridge it manually so you don't bloat memory.

Note: streams snapshot the set of log files at creation time and replay records acknowledged before that point; readers tailing the unsealed active file are best-effort and may surface transient errors if they race an in-flight write.
