# Development Plan

## Overarching Goal

Make WriteAhead a correct, simple, fast partitioned WAL. The guarantees to preserve (and strengthen):

1. **Durability**: a write is acknowledged only after it is fsync'd to the active log file.
2. **Corruption detection**: any bit-flip in a record — including its length header — is detected on read.
3. **Crash recovery**: after a crash, `start()` restores the WAL to a consistent state: acknowledged records are readable, torn tails are truncated, and sealed files are never appended to.
4. **Stable addressing**: `RecordID { file_id, file_offset }` remains valid for the life of the file.
5. **Sequential streaming**: a stream replays all records across files in write order.

Non-goals: multi-writer support, io_uring reads (measured slower than page-cache reads; removed), backward compatibility of the on-disk format (pre-1.0, no users to migrate).

Original defects driving this plan (all fixed on branch `wal-v2`): (a) streams misread after any record containing `0xff` — repro'd, fixed by format v2, regression test `test_stream_with_0xff_records`; (b) restart onto a sealed last file appended records after the seal footer — fixed, test `crash_after_seal_before_next_file_rotates`; (c) `Logfile::delete`/`Drop` never deleted anything — mechanism removed, real retention implemented; (d) write path panicked on IO errors and blocked the async executor on `rx.recv()` — errors now propagate, `recv_async` awaits.

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
- Perf claims are backed by before/after numbers from `examples/bench.rs` recorded in the ledger.

## Phase 1: On-disk format v2 — remove escaping, verifiable footer

Goal:
Replace the escape-based framing with a self-describing format that structurally fixes the stream-offset bug, protects the length header, and makes seal detection deterministic.

Scope:
- File header: 8-byte magic + 1-byte format version.
- Record: `hash(16B) | len(u32) | raw data` — no `0xff` escaping anywhere.
- Seal footer: `magic(8B) | seal_timestamp(u64) | records_end(u64) | footer_hash(16B)`; sealed iff the trailing footer verifies AND `records_end == file_len - footer_len`.
- Bounds-check record reads so a corrupt length can't drive a huge allocation or bogus read.
- Streams advance by on-disk length, identical to payload length now.

Completion gate:
`[0xff; N]` records roundtrip through write→read and write→stream; sealed detection is deterministic; corrupt length returns an error.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Work | 1A: Header + version byte; writer writes it only when file is empty | src/logfile.rs `write_header`/`validate_header`, `FORMAT_VERSION = 2`; `LogFileWriter::new` writes header only when `len == 0`. |
| Complete | Work | 1B: Record format without escaping | src/logfile.rs `write_group` encodes `hash|len|data` raw; escape/unescape loops deleted. Amended: hash covers data only — a corrupted length is still always caught (bounds check, else hash-over-wrong-extent mismatch), verified by `test_corrupted_length_is_bounded` + `test_corrupted_record`. |
| Complete | Work | 1C: Verifiable seal footer with timestamp + records_end + hash | src/logfile.rs `build_footer`/`parse_footer`/`seal`; tests `test_write_with_sealing`, `test_magic_number_payload_roundtrip`, `test_recover_and_seal_torn_footer`. |
| Complete | Work | 1D: Bounds-check record reads | src/logfile.rs `read_record_at` checked_add + records_end checks; test `test_corrupted_length_is_bounded`. |
| Complete | Decision | 1E: Keep murmur3 vs switch to xxh3/crc32c | Kept murmur3: zero new deps, known-value tests pass (src/murmur3.rs). xxh3 remains a future option; format version byte makes a later swap cheap. |
| Complete | Test | 1F: `0xff` stream regression test | `test_stream_with_0xff_records` (src/write_ahead.rs). |
| Complete | Test | 1G: Seal detection + corrupt-length unit tests | src/logfile.rs tests: `test_magic_number_payload_roundtrip`, `test_corrupted_length_is_bounded`, `test_recover_and_seal_torn_footer`. |
| Complete | Gate | Format v2 roundtrip + seal detection all green | `cargo test`: 33 tests pass (commit e1f3959 + follow-ups). |

## Phase 2: Crash-recovery correctness

Goal:
`start()` restores a consistent WAL after any crash point: validates every file, never appends to sealed files, truncates torn tails, and returns errors instead of panicking.

Completion gate:
Kill-point matrix passes: crash after seal/before next-file-create, crash mid-record-write, crash after create/before header — each followed by `start()` + full stream replay yields exactly the acknowledged records, and subsequent writes succeed.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Work | 2A: Validated file loading in `start()` | src/write_ahead.rs `start` uses `Logfile::open` (magic + version + footer validation) for every file. |
| Complete | Work | 2B: Rotate instead of appending when last file is sealed | src/write_ahead.rs `start` sealed branch; test `crash_after_seal_before_next_file_rotates` (tests/recovery.rs). |
| Complete | Work | 2C: Tail scan + truncate torn writes on active file | src/logfile.rs `recover_unsealed`/`scan_records_end`; tests `crash_mid_record_truncates_torn_tail`, `test_recover_truncates_torn_tail`, `test_recover_partial_header`. |
| Complete | Work | 2D: Tolerate stray files; no unwraps in `start()` | `file_id_from_path` returns `Option`, stray entries warned + skipped; test `stray_files_are_skipped`; duplicate ids error (`DuplicateLogfileId`). |
| Complete | Work | 2E: Error propagation in write/rotate/actor paths | `write_batch`/`rotate_log_file` return `Err` via `WriterClosed`/`NotStarted`; actor ignores dropped reply receivers; `test_write_with_sealing` asserts error-not-panic. |
| Complete | Work | 2F: Directory fsync on create/rotate | src/write_ahead.rs `sync_dir` called in `create_active_at` and after retention deletes. |
| Complete | Work | 2G: Heal unsealed non-active files (torn footer mid-rotation) | src/logfile.rs `recover_and_seal`; test `torn_footer_on_non_active_file_is_healed`. |
| Complete | Test | 2H: Kill-point crash matrix integration tests | tests/recovery.rs: 6 tests, all passing. |
| Complete | Gate | All crash points recover to acknowledged-prefix state | tests/recovery.rs suite green (commit e1f3959). |

## Phase 3: Architecture & API simplification

Goal:
One sync file-IO abstraction, one generic parameter, honest async boundaries, and no dead code — without changing the guarantees.

Completion gate:
Zero warnings under `cargo clippy -- -D warnings`; lib deps are `anyhow, flume, futures, thiserror, tracing` only; suite green; `WriteAheadStream` remains `Send`.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Work | 3A: Remove io_uring module + deps | src/fileio/io_uring.rs deleted; Cargo.toml lib deps = anyhow, flume, futures, thiserror, tracing; tokio/tracing-subscriber now dev-only. |
| Complete | Work | 3B: Sync `FileIo` trait with pread/pwrite + separate `sync` | src/fileio/mod.rs `FileIo` (`read_at`/`write_at`/`sync`/`len`/`set_len`); SimpleFile uses `FileExt::read_exact_at`/`write_all_at`. |
| Complete | Work | 3C: Single-generic `WriteAhead<F = SimpleFile>` | src/write_ahead.rs; `PhantomData` gone. |
| Complete | Work | 3D: `recv_async` in write path; sync reads/streams | `write_batch`/`rotate_log_file` await `recv_async`; `read`/`create_stream*`/`start` are sync; stream poll is synchronous (no future-drop hazard). |
| Complete | Work | 3E: u64 ids, by-value offsets, `&self` reads, `Result` stream constructors | `Logfile.id: u64`, `read(&self, u64, u64)`, `create_stream* -> Result`; `test_read_before_start_errors`. |
| Complete | Work | 3F: Delete `delete_on_drop`, dead imports, commented code | Mechanism removed (real deletion in Phase 5); `cargo clippy --all-targets -- -D warnings` clean. |
| Complete | Test | 3G: Concurrent pread smoke test | tests/concurrent_reads.rs `concurrent_readers_share_a_handle`. |
| Complete | Gate | Clippy-clean, trimmed deps, suite green, stream still `Send` | Clippy 0 errors; `test_write_ahead_stream_is_send` passes (commit e1f3959). |

## Phase 4: Performance

Goal:
Higher throughput and lower per-record overhead without weakening durability (fsync-before-ack unchanged).

Completion gate:
Measured before/after numbers from `examples/bench.rs`; crash matrix still green.

Measured results (ext4/NVMe, 64B payloads, medians across 2 baseline / 3 final runs):

| scenario | baseline (post-fixes) | after Phase 4 | delta |
| --- | --- | --- | --- |
| seq_write_1rec | ~4358 µs/op (230/s) | ~4247 µs/op (235/s) | ~unchanged (fsync-bound) |
| batch_write 1000/batch | ~211k rec/s | ~210k rec/s | unchanged (already 1 write+1 sync per batch) |
| read_by_id | ~0.53 µs/op (1.96M/s) | ~0.49 µs/op (2.05M/s) | ~8% faster |
| stream_replay | ~0.55 µs/op (1.81M/s) | ~0.028 µs/op (35M/s) | **~19x faster** |
| actor_concurrent_8w | ~4256 µs/op (235/s) | ~1087 µs/op (~940/s) | **~4x faster** |

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Work | 4A: Group commit (drain, single write+fsync, fan-out replies) | src/logfile.rs `actor_loop` + `write_group` (4MiB drain cap, Seal preserves order); bench actor_concurrent_8w 235/s → ~940/s. Note: the public `&mut write_batch` serializes callers, so coalescing benefits concurrent senders holding a cloned writer channel (bench + tests/group_commit.rs exercise this level). |
| Complete | Work | 4B: Zero-copy encode with precomputed capacity | `write_group` encodes straight into one `Vec::with_capacity(total)`; no per-record copies (landed with Phase 1's escaping removal, so it's inside the baseline). |
| Complete | Work | 4C: Chunked readahead in `LogFileStream`; bounded re-stat for unsealed files | 128KiB `STREAM_CHUNK` buffer; `known_end` re-stat only at buffer exhaustion; bench stream_replay 1.8M/s → ~35M/s. |
| Complete | Decision | 4D: One-pread vs two-pread `read_record` | Speculative 512B single pread. Measured: 4KiB window regressed (~0.64 µs/op — zero-init + memcpy dominated); 512B wins (~0.49 µs/op). |
| Complete | Test | 4E: Multi-producer correctness test | tests/group_commit.rs: 8 threads, unique offsets, all records intact. |
| Complete | Work | 4F: fdatasync instead of fsync | SimpleFile::sync uses `sync_data`. |
| Complete | Gate | Before/after numbers recorded; crash matrix still green | Table above; full suite (33 tests incl. recovery matrix) green after perf changes (commit 743d9e7). |

## Phase 5: Retention that actually works

Goal:
Implement `RetentionOptions` (`max_total_size`, `ttl`) end-to-end.

Completion gate:
Size retention bounds disk usage across rotations; ttl deletes aged sealed files on start; active file never deleted; reads into deleted files error cleanly.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Work | 5A: Size-based retention on rotate/start | src/write_ahead.rs `apply_retention`; test `size_retention_deletes_oldest_sealed_files`. |
| Complete | Work | 5B: TTL retention using footer seal timestamp | `apply_retention` ttl branch; test `ttl_retention_deletes_aged_files_on_start` (timestamps injected via `recover_and_seal`, no sleeps). |
| Complete | Work | 5C: Real file deletion + dir fsync + map removal | `apply_retention` removes map entry, `fs::remove_file`, `sync_dir`; reads after deletion return `LogfileNotFound`. |
| Complete | Test | 5D: Size/ttl/disabled retention tests | tests/retention.rs: 3 tests passing. |
| Complete | Gate | Disk usage bounded; active file safe; clean errors on deleted files | tests/retention.rs green (commit e1f3959). |

## Phase 6: Test hygiene, CI, and docs

Goal:
A trustworthy, non-flaky suite and documentation that matches reality.

Completion gate:
`cargo test` passes repeatedly with a clean tree; CI green on the PR; README describes the actual format and guarantees.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Work | 6A: tempfile migration; self-cleaning tests | Every test uses `tempfile::tempdir`; no shared `/tmp/NN.log`, no `test_logs/` output; bench uses `tempdir_in(".")` (real fs, auto-cleaned). |
| Complete | Work | 6B: Remove commented-out test corpses | Old generic test matrix + commented tests deleted with the io_uring module and logfile rewrite. |
| Complete | Test | 6C: Proptest roundtrip suite (read-by-id + stream, across rotation) | tests/roundtrip_prop.rs (32 cases, arbitrary payload batches, forced rotations). |
| Complete | Work | 6D: GitHub Actions CI (fmt, clippy, test) | .github/workflows/ci.yml (ubuntu + macos matrix). |
| Complete | Doc | 6E: README rewrite | README.md: guarantees, format v2 layout, recovery contract, bench table; io_uring section replaced with a one-line design note. |
| In Progress | Gate | 20× clean test runs; CI green; docs match code | 20 consecutive `cargo test` runs with zero failures + clean `git status` (2026-07-16, local). Remaining: first CI run goes green on the PR. |
