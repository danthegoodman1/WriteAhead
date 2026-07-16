# Development Plan

## Overarching Goal

Make WriteAhead a correct, simple, fast partitioned WAL. The guarantees to preserve (and strengthen):

1. **Durability**: a write is acknowledged only after it is fsync'd to the active log file.
2. **Corruption detection**: any bit-flip in a record — including its length header — is detected on read.
3. **Crash recovery**: after a crash, `start()` restores the WAL to a consistent state: acknowledged records are readable, torn tails are truncated, and sealed files are never appended to.
4. **Stable addressing**: `RecordID { file_id, file_offset }` remains valid for the life of the file.
5. **Sequential streaming**: a stream replays all records across files in write order.

Non-goals: multi-writer support, io_uring reads (measured slower than page-cache reads per README; being removed), backward compatibility of the on-disk format (pre-1.0, no users to migrate).

Known confirmed defects driving this plan: (a) streams misread after any record containing `0xff` — repro'd with a 3-byte `0xff` record followed by a plain record; the second read fails with "failed to fill whole buffer" because `LogFileStream` advances by unescaped length (src/logfile.rs:415) while the on-disk record is longer; (b) restart onto a sealed last file appends records after the seal footer (src/write_ahead.rs:86-107 loads via `Logfile::new`, never checking the seal); (c) `Logfile::delete`/`Drop` never deletes anything (src/logfile.rs:210-229); (d) write path panics on IO errors and blocks the async executor on `rx.recv()` (src/write_ahead.rs:127-130).

## Implementation Principles

- The on-disk format is self-describing and verifiable: no byte escaping, no probabilistic detection where a checksum can make it deterministic.
- Errors from disk are returned, never panicked, on every public API path.
- One writer thread owns the active file; readers never block the writer.
- Sync file IO under the hood (pread/pwrite); async is a thin facade at the channel boundary only.
- Prefer deleting code to configuring it: features that aren't implemented (retention) either get implemented or removed from the API.

## Testing Strategy

- All tests use `tempfile` dirs — no shared `/tmp/NN.log` paths, no leftover `test_logs/`.
- Crash-recovery integration tests simulate kill points by manipulating files directly (truncate mid-record, seal-then-restart, empty active file).
- Property/roundtrip tests over arbitrary byte payloads (including `0xff` runs and empty records) for write→read and write→stream.
- `cargo test`, `cargo clippy -- -D warnings`, and `cargo fmt --check` pass at every phase gate; run in CI (GitHub Actions).
- Perf claims are backed by before/after numbers from the existing large-data batch/sequential tests (or a criterion bench) recorded in the ledger.

## Phase 1: On-disk format v2 — remove escaping, verifiable footer

Goal:
Replace the escape-based framing with a self-describing format that structurally fixes the stream-offset bug, protects the length header, and makes seal detection deterministic.

Scope:
- File header: 8-byte magic + 1-byte format version.
- Record: `hash(16B over len‖data) | len(u32) | raw data` — no `0xff` escaping anywhere (delete escape loop src/logfile.rs:336-344 and unescape loop src/logfile.rs:197-205).
- Seal footer: `magic(8B) | seal_timestamp(u64) | records_end(u64) | footer_hash(16B over prior fields)`. Sealed iff the trailing footer verifies AND `records_end == file_len - footer_len`. Replaces the last-9-bytes probe (src/logfile.rs:132-140).
- Reinstate the bounds check before reading record data: `offset + 20 + len <= records_end` (currently commented out, src/logfile.rs:183-186) so a corrupt length can't drive a huge allocation or bogus read.
- `LogFileStream` advances by `20 + len` where `len` is the on-disk length — identical to payload length now.

Out of scope:
- Recovery/startup behavior changes (Phase 2). Retention use of `seal_timestamp` (Phase 5).

Completion gate:
A record consisting of `[0xff; N]` roundtrips through write→read and write→stream (regression for the confirmed bug); a sealed file is detected sealed; a file whose payload ends in magic-like bytes is not misdetected; corrupt length header returns an error, not an allocation blowup.

Testing plan:
- Regression test: batch of `[0xff,0xff,0xff]` + plain record, streamed back fully.
- Unit tests: seal/unseal detection, footer hash mismatch → unsealed, truncated footer → unsealed, corrupt length → `PartialWrite`/`Corrupted` error.
- Roundtrip property test over arbitrary payloads (incl. empty record, payload containing the magic sequence).

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Incomplete | Work | 1A: Header + version byte; writer writes it only when file is empty | Missing: implementation in `LogFileWriter::new` (src/logfile.rs:252-268 currently rewrites header unconditionally). |
| Incomplete | Work | 1B: Record format without escaping; hash covers `len‖data` | Missing: new `write_records`/`read_record`; deletion of escape/unescape loops. |
| Incomplete | Work | 1C: Verifiable seal footer with timestamp + records_end + hash | Missing: new `seal()`/`from_file` footer logic. |
| Incomplete | Work | 1D: Bounds-check record reads against `records_end`/file length | Missing: reinstated check in `read_record`. |
| Incomplete | Decision | 1E: Keep murmur3 vs switch to xxh3/crc32c while format is open | Missing: pick one; murmur3 impl is unaudited AI-generated code (src/murmur3.rs:1-3). |
| Incomplete | Test | 1F: `0xff` stream regression test | Missing: test (bug confirmed by ad-hoc repro on 2026-07-16, removed after verification). |
| Incomplete | Test | 1G: Seal detection + corrupt-length unit tests | Missing: tests. |
| Incomplete | Gate | Format v2 roundtrip + seal detection all green | Missing: passing suite. |

## Phase 2: Crash-recovery correctness

Goal:
`start()` restores a consistent WAL after any crash point: validates every file, never appends to sealed files, truncates torn tails, and returns errors instead of panicking.

Scope:
- Load existing files via validated open (`from_file` semantics), not `Logfile::new` (src/write_ahead.rs:86). Fix the `Logfile::new` doc lie ("deleting any existing file" — it doesn't, src/logfile.rs:98).
- If the highest file is sealed on start, rotate to a new file instead of attaching a writer to it.
- Tail scan of the (unsealed) active file at startup: walk records verifying hashes; truncate at the first invalid/incomplete record; writer resumes from the truncated length. Handle the empty/header-only file case (crash between create and first write).
- Skip non-`.log` directory entries instead of failing `start()`; replace `parse::<u64>().unwrap()` (src/write_ahead.rs:88) and `launch(...).unwrap()` (src/write_ahead.rs:99,106) with propagated errors.
- Error propagation through the write path: `write_batch`/`rotate_log_file` return `Err` on writer failure instead of `unwrap()` panics (src/write_ahead.rs:127-130,148-155); writer actor doesn't `unwrap()` on send to a dropped caller (src/logfile.rs:293-306).
- fsync the log directory after file creation and rotation so new files survive crash.

Out of scope:
- Concurrent tailing of the active file by streams (documented as best-effort; readers of unsealed files may see torn tails until sealed).

Completion gate:
Kill-point matrix passes: crash after seal/before next-file-create, crash mid-record-write (torn tail), crash after create/before header — each followed by `start()` + full stream replay yields exactly the acknowledged records, and subsequent writes succeed.

Testing plan:
- Integration tests simulating each kill point by direct file manipulation (truncate mid-record, delete next file after seal, create empty file).
- Test: directory containing a stray `foo.txt` and `.DS_Store` — `start()` succeeds.
- Test: write error surfaces as `Err` from `write_batch` (e.g., seal the writer out-of-band, then write).

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Incomplete | Work | 2A: Validated file loading in `start()` | Missing: `from_file`-based load + magic/version check. |
| Incomplete | Work | 2B: Rotate instead of appending when last file is sealed | Missing: seal check + rotation on start (bug (b) in goal). |
| Incomplete | Work | 2C: Tail scan + truncate torn writes on active file | Missing: scan implementation + truncation. |
| Incomplete | Work | 2D: Tolerate stray files; no unwraps in `start()` | Missing: filter + error propagation. |
| Incomplete | Work | 2E: Error propagation in write/rotate/actor paths | Missing: `Result` plumbing replacing `unwrap()`s. |
| Incomplete | Work | 2F: Directory fsync on create/rotate | Missing: `File::open(dir).sync_all()` after creation. |
| Incomplete | Test | 2G: Kill-point crash matrix integration tests | Missing: tests. |
| Incomplete | Gate | All crash points recover to acknowledged-prefix state | Missing: passing matrix. |

## Phase 3: Architecture & API simplification

Goal:
One sync file-IO abstraction, one generic parameter, honest async boundaries, and no dead code — without changing the guarantees.

Scope:
- Delete `IOUringFile`, `GLOBAL_RING`, and the io_uring test suite (README already concludes page-cache reads win for WAL access patterns; src/fileio/io_uring.rs). Drop `io-uring`, `once_cell`, `libc`, and lib-target `tokio` from dependencies; move `tracing-subscriber` to dev-deps.
- Replace `FileWriter`/`FileReader` async traits with one sync `FileIo` trait: `open`, `read_at` (pread via `FileExt::read_exact_at`), `write_at`, `sync`, `len`. Separating `write_at` from `sync` enables Phase 4 group commit. Fixes the seek+read two-syscall pattern and the shared-cursor hazard in `SimpleFile` (src/fileio/simple_file.rs:31-38,57-64).
- Collapse `WriteAhead<WriteF, ReadF>` + `PhantomData` to `WriteAhead<F: FileIo = SimpleFile>`.
- Async facade: `write_batch` awaits the writer via `flume::recv_async` (no blocking `recv()` on the executor). Reads and streams become sync under the hood (they always completed synchronously anyway); this removes the poll-drops-future hazard in `LogFileStream::poll_next` (src/logfile.rs:432-439).
- Type/API cleanup: file ids are `u64` end-to-end (drop the `String` id on `Logfile`); `read_record(offset: u64)` by value; `WriteAhead::read(&self, ...)`; `create_stream`/`create_stream_from` return `Result` instead of `unwrap()`/`Option` panics (src/write_ahead.rs:175,196-207).
- Delete the broken `delete_on_drop` mechanism (src/logfile.rs:210-229); real deletion arrives in Phase 5. Remove dead imports (`mpsc`, `block_on`, `debug`) and commented-out code blocks.

Out of scope:
- Any on-disk format change (done in Phase 1). Performance work beyond what the trait shape enables (Phase 4).

Completion gate:
Library compiles with zero warnings under `cargo clippy -- -D warnings`; lib dependency tree is `anyhow, flume, futures, thiserror, tracing` only; all Phase 1–2 tests still pass; `WriteAheadStream` remains `Send` (existing `test_write_ahead_stream_is_send`).

Testing plan:
- Existing suite green after refactor (behavior-preserving).
- Concurrent-read smoke test: two threads reading the same `Logfile` via `read_at` (validates pread removes the shared-cursor hazard).
- `cargo tree` snapshot in ledger evidence for dependency trim.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Incomplete | Work | 3A: Remove io_uring module + deps | Missing: deletion + Cargo.toml trim. |
| Incomplete | Work | 3B: Sync `FileIo` trait with pread/pwrite + separate `sync` | Missing: trait + `SimpleFile` impl. |
| Incomplete | Work | 3C: Single-generic `WriteAhead<F = SimpleFile>` | Missing: refactor, `PhantomData` removal. |
| Incomplete | Work | 3D: `recv_async` in write path; sync reads/streams | Missing: implementation. |
| Incomplete | Work | 3E: u64 ids, by-value offsets, `&self` reads, `Result` stream constructors | Missing: API changes. |
| Incomplete | Work | 3F: Delete `delete_on_drop`, dead imports, commented code | Missing: cleanup (warnings currently emitted by `cargo check`). |
| Incomplete | Test | 3G: Concurrent pread smoke test | Missing: test. |
| Incomplete | Gate | Clippy-clean, trimmed deps, suite green, stream still `Send` | Missing: verification run. |

## Phase 4: Performance

Goal:
Higher throughput and lower per-record overhead without weakening durability (fsync-before-ack unchanged).

Scope:
- Group commit in the writer actor: after receiving one `Write`, drain all queued `Write` commands (`try_recv` loop, bounded by bytes), encode into one buffer, single `write_at` + single `sync`, then reply to every caller with its offsets. Today each command pays its own fsync (src/logfile.rs:278-310).
- Zero-copy encode: build records directly into the combined buffer with precomputed capacity `sum(20 + len)`; the per-record `record.to_vec()` copy (src/logfile.rs:337) disappears with escaping in Phase 1 — verify no full-payload copies remain.
- Stream readahead: `LogFileStream` reads fixed-size chunks (e.g. 64 KiB) and parses records from the buffer, replacing 2 preads + up to 2 `metadata()` syscalls per record (`file_length()` hits `fd.metadata()` every poll, src/fileio/simple_file.rs:40-42, src/logfile.rs:426-429). Cache file length in `Logfile` when sealed; live-query only for unsealed tailing.
- Single-pread `read_record` fast path: read `20 + expected_len` speculatively or keep two reads — decide by measurement.

Out of scope:
- io_uring, O_DIRECT, or any platform-specific IO strategy.

Completion gate:
Measured, recorded before/after numbers: batched-write throughput (existing `test_write_ahead_large_data_simple_batch` workload) improves under ≥4 concurrent producers via group commit; sequential stream read of ≥100k records shows syscall count reduction (strace or timing); no durability test regresses.

Testing plan:
- Multi-producer group-commit test: N tasks writing concurrently through a channel to one `WriteAhead`; verify every ack'd record reads back and file contents are contiguous/valid.
- Bench comparison recorded in ledger (criterion or timed tests, before/after).
- Re-run full crash matrix from Phase 2 (group commit must not ack before fsync).

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Incomplete | Work | 4A: Group commit (drain, single write+fsync, fan-out replies) | Missing: actor loop rework. |
| Incomplete | Work | 4B: Zero-copy encode with precomputed capacity | Missing: buffer sizing; removal of residual copies. |
| Incomplete | Work | 4C: Chunked readahead in `LogFileStream`; cached length for sealed files | Missing: implementation. |
| Incomplete | Decision | 4D: One-pread vs two-pread `read_record` | Missing: measurement + choice. |
| Incomplete | Test | 4E: Multi-producer correctness test | Missing: test. |
| Incomplete | Gate | Before/after numbers recorded; crash matrix still green | Missing: bench evidence. |

## Phase 5: Retention that actually works

Goal:
Implement `RetentionOptions` (`max_total_size`, `ttl`) end-to-end, or nothing pretends to exist.

Scope:
- Enforce retention on rotation and on `start()`: delete oldest sealed files while total size exceeds `max_total_size`; delete sealed files whose footer `seal_timestamp` (from Phase 1C) is older than `ttl`. Never delete the active file.
- Real deletion: remove from `log_files` map + `fs::remove_file` + directory fsync (replaces the no-op `delete_on_drop`, removed in 3F).
- Reads/streams over deleted files return `LogfileNotFound` — document this as the retention contract.

Out of scope:
- Archival/compaction hooks; snapshotting.

Completion gate:
With `max_total_size` set small, rotation deletes oldest sealed files and disk usage stays bounded; with `ttl` set, aged sealed files are removed on start; active file is never deleted; reads into deleted files error cleanly.

Testing plan:
- Integration tests for size-based and ttl-based deletion (ttl test uses injected/mocked timestamps in the footer, not sleeps).
- Test: retention disabled (`0` values) deletes nothing.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Incomplete | Work | 5A: Size-based retention on rotate/start | Missing: implementation. |
| Incomplete | Work | 5B: TTL retention using footer seal timestamp | Missing: implementation (depends on 1C). |
| Incomplete | Work | 5C: Real file deletion + dir fsync + map removal | Missing: implementation. |
| Incomplete | Test | 5D: Size/ttl/disabled retention tests | Missing: tests. |
| Incomplete | Gate | Disk usage bounded; active file safe; clean errors on deleted files | Missing: passing tests. |

## Phase 6: Test hygiene, CI, and docs

Goal:
A trustworthy, non-flaky suite and documentation that matches reality.

Scope:
- Migrate every test to `tempfile` dirs/paths. Today `simple_file` and `io_uring` test modules race on the same `/tmp/01.log`–`/tmp/10.log` paths within one parallel test binary (src/fileio/simple_file.rs:83-144 vs src/fileio/io_uring.rs:227-330); `test_write_ahead_stream` leaves `test_logs/` behind (src/write_ahead.rs:588-620 has no cleanup).
- Delete commented-out test corpses (`test_corrupted_file_header`, iterator tests in io_uring.rs — module goes away in 3A anyway).
- Property tests (proptest, dev-dep): arbitrary payload batches roundtrip through read-by-id and full stream replay, across rotations.
- GitHub Actions CI: `cargo fmt --check`, `cargo clippy -- -D warnings`, `cargo test` on Linux (+ macOS since io_uring is gone).
- README: remove io_uring section, document format v2, the guarantee list from this plan's goal, and the recovery contract.

Out of scope:
- Publishing to crates.io; MSRV policy.

Completion gate:
`cargo test` passes repeatedly (×20) with no inter-test interference and leaves the working tree clean; CI green on a PR; README describes the actual format and guarantees.

Testing plan:
- `for i in $(seq 20); do cargo test; done` with clean `git status` after.
- Proptest suite in `tests/`.
- CI run link as evidence.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Incomplete | Work | 6A: tempfile migration; self-cleaning tests | Missing: refactor of all test paths. |
| Incomplete | Work | 6B: Remove commented-out test corpses | Missing: deletion. |
| Incomplete | Test | 6C: Proptest roundtrip suite (read-by-id + stream, across rotation) | Missing: tests + dev-dep. |
| Incomplete | Work | 6D: GitHub Actions CI (fmt, clippy, test) | Missing: workflow file. |
| Incomplete | Doc | 6E: README rewrite (format v2, guarantees, recovery contract, drop io_uring notes) | Missing: doc update. |
| Incomplete | Gate | 20× clean test runs; CI green; docs match code | Missing: verification evidence. |
