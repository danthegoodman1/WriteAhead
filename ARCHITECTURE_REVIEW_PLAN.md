# Focused Reliability and Performance Plan

Review baseline: 2026-09-05, `6bc7aed` (v0.3.0). Scope narrowed 2026-09-19 to the prioritized fixes. All retained phases are implemented and locally verified on 2026-09-19. P1 = correctness/durability; P2 = resource use/performance.

## Overarching Goal

Fix the demonstrated data-loss and visibility bugs, make directory durability and cache cleanup reliable, bound queued work, and reduce recovery I/O. Build on the existing dedicated writer, group commit, positional reads, and buffered streams.

Preserve successful-write durability, corruption detection, stable acknowledged RecordIDs until retention deletes their files, record order, and batch contiguity. Keep the existing construction and stream APIs and v3 format, adding only errors/options needed for these fixes. Streams will retain open file handles and capture fixed durable ends when created.

## Prioritized Findings

| ID / Priority | Finding | Evidence at the review baseline |
| --- | --- | --- |
| R1 / P1 | Recovery treats I/O errors as torn records and can truncate acknowledged data. | `src/logfile.rs:218`, `recover_unsealed` at line 415. After two acknowledged writes, one injected recovery EIO at the second record caused successful startup, loss of that record, and reuse of its address. |
| R2 / P1 | Repeated start or a second manager permits competing writers. | `src/write_ahead.rs:115`. Retaining an old WriteHandle across another `start()` produced two successful writes with the same RecordID; one overwrote the other. |
| R3 / P1 | Readers use commit slots visible before sync; streams include later appends. | `src/writer.rs:315`, `src/logfile.rs:621`, `src/logfile.rs:692`. A paused-sync probe exposed the pending record. A separate stream included a write submitted after stream creation. |
| R4 / P1 | Creation/retry can omit required directory durability barriers. | `src/write_ahead.rs:116` creates ancestors without syncing their parent entries. If the WAL-directory sync at `src/writer.rs:168` fails, retry can use the existing-file branch at line 174 without repeating it. Inspection finding; no power-loss reproduction. |
| R5 / P2 | Deleted files remain open in an idle reader cache; lifecycle events accumulate without reads. | `src/write_ahead.rs:246`, `src/writer.rs:161`. After warming a sealed-file cache entry and trimming it, `/proc/self/fd` retained its deleted descriptor until another read, even with no stream alive. |
| R6 / P2 | Queued work is unbounded, and zero-byte commands evade the drain byte limit. | `src/writer.rs:160` uses an unbounded channel; line 231 limits only bytes per round. Empty batches/trims can keep a round draining, and one batch can exceed the group byte limit. Inspection finding. |
| R7 / P2 | Active reads repeatedly read commit metadata; recovery reads and allocates per record. | `src/logfile.rs:327`, `src/logfile.rs:218`. Counters measured 4 reads + 1 stat for a 64-byte active point read and 1,004 reads to recover 1,000 × 64-byte records (84,000 encoded bytes). |

R4 applies the directory-entry durability requirement in [fsync(2)](https://man7.org/linux/man-pages/man2/fsync.2.html) to newly created directories' parents. R5's retained descriptors also retain storage, as described in [unlink(2)](https://man7.org/linux/man-pages/man2/unlink.2.html). Existing streams may legitimately hold deleted files open; idle cache entries should be released promptly.

## Implementation Principles

- Make the smallest change that closes each finding. Extend existing writer/reader synchronization; keep helpers local and tied to a concrete use.
- Acquire exclusive directory ownership before recovery and retain it until the writer exits, including when WriteHandles outlive the manager.
- Publish readable boundaries only after successful sync and before success replies. Failed/cancelled requests may still reach disk; an unsynced slot is not proof of durability.
- Preserve torn-tail repair and the older committed floor while propagating I/O failures without destructive repair. Distinguish acknowledged-data corruption from unacknowledged torn appends in tests.
- Preserve immediate unlink on retention, established streams' open handles, batch contiguity, and one sync per steady-state group.
- Use focused fault wrappers, barriers, and counters. Additional work must be necessary for a listed fix or supported by a measured bottleneck.
- Release temporary files, scratch caches, probe artifacts, processes, and handles as soon as they are no longer needed, including on failure. Retain necessary evidence in the repository's tests or plan before deleting temporary resources.

## Testing Strategy and Baseline

On 2026-09-05, `cargo test --all-targets --offline --locked` passed 54 tests; `cargo fmt --check` and `cargo clippy --all-targets --offline --locked -- -D warnings` passed with Rust/Cargo 1.97.0 on Linux. These are historical baseline results, not validation of remediation or a fresh run for this plan edit.

Seven review probes established the R1/R2/R3/R5 reproductions and R7 counts. The temporary harness, scratch Cargo cache, and probe build artifacts have been removed. The findings above retain the reproduction steps and measurements to convert into permanent regressions.

- Use FileIo wrappers for injected read/sync failures and counters; add a small directory-sync test hook where required for ordering/retry checks. Use barriers instead of sleeps for concurrency.
- Verify every acknowledged, untrimmed record keeps its data and RecordID through the tested recovery/restart sequences. Rerun existing torn-tail, preallocation, rotation, and trim coverage after relevant changes.
- Use separate-process ownership tests, paused-writer admission/publication tests, and handle counters to verify cache release without another read.
- Gate performance on read/allocation/sync counts. Compare the existing benchmark before/after hot-path changes on the same real storage setup and investigate repeatable regressions. The implementation comparison is recorded in [PERFORMANCE.md](PERFORMANCE.md); no power-loss experiment is claimed.

Fresh implementation validation on 2026-09-19: `cargo test --all-targets --offline --locked` passed **71 tests**; `cargo test --doc --offline --locked` passed (no doctests); `cargo fmt --check` and `cargo clippy --all-targets --offline --locked -- -D warnings` passed. Quickstart, trim-high-water-mark, and compaction examples executed successfully in automatically cleaned temporary directories.

The subsequent simplification pass removed the lifecycle event queue and redundant group staging, reused one buffered decoder, and replaced stream file lookup/removal with a current stream plus a remaining iterator. Exhausted stream handles close at EOF. The full verification above and the five-pair performance comparison were run after these changes; benchmark binaries, archived source/build trees, and example data were removed after use.

## Phase 1: Propagate Recovery I/O Errors Safely

Goal: close R1 without changing the recovery protocol.

Scope:

- **1A:** Make scans return errors separately from end offsets. Distinguish clean end, invalid/torn record bytes, and underlying I/O failure using existing error types where practical. Abort on I/O failure before slot repair or truncation; retain the older-slot floor and valid torn-tail repair.

Completion gate: an injected read failure leaves file bytes and length unchanged, startup reports the failure, and retry without the fault preserves all acknowledged records and addresses.

Testing plan: reproduce the two-commit EIO loss; fail reads at the first record and later in the latest batch; assert byte-for-byte non-mutation and successful retry; rerun torn-tail and older-floor corruption tests.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Scope | R1 safe recovery on I/O failure | `src/logfile.rs::scan_records_end` propagates read errors; `tests/reliability.rs::recovery_io_errors_never_modify_records_or_metadata` verifies unchanged bytes and IDs. |
| Complete | Work | 1A: Fallible scan and constrained repair | `scan_records_end` returns `Result<u64>`; `recover_unsealed` uses `?` before slot repair/truncation and retains the older-slot floor. |
| Complete | Test | Recovery fault/retry cases | `recovery_io_errors_never_modify_records_or_metadata` injects EIO at each of three records across two commits, checks non-mutation, and retries successfully. |
| Complete | Gate | Recovery errors preserve acknowledged data | Fault/retry regression and existing torn-tail, older-floor, and repair-floor tests in `src/logfile.rs` pass in the 71-test run. |

## Phase 2: Enforce Writer Ownership and Directory Durability

Goal: close R2/R4 within the current start/handle model.

Scope:

- **2A:** Reject repeated `start()` and acquire an OS-backed exclusive directory lock before recovery. Keep the lock with the writer for its entire lifetime so surviving WriteHandles still exclude another owner.
- **2B:** Ensure failed startup leaves no usable writer/channel or actor that continues mutating files. Keep cleanup/thread coordination internal; publish usable handles only after initialization succeeds.
- **2C:** Sync parent entries for newly created directory components and complete required WAL-directory barriers before accepting writes. Repeat barriers on startup/retry when existing files may come from interrupted initialization; propagate failures.

Completion gate: competing owners are rejected before mutation; failed startup releases ownership after cleanup; writes cannot precede required directory-sync calls, including on retry.

Testing plan: repeated start; two managers/processes, including path aliases; handle surviving manager drop; startup failure/retry; recorded sync ordering for nested-directory creation and injected directory-sync failure. Rerun rotation/restart tests.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Scope | R2/R4 ownership and directory barriers | `start` acquires a directory inode lock before recovery; `sync_parents` and `WalWriter::launch` complete startup barriers before publishing the writer. |
| Complete | Work | 2A: Start guard and writer-owned lock | `repeated_start_and_aliases_cannot_create_another_writer` plus `tests/ownership.rs` verify same-instance, alias, process, and surviving-handle exclusion/release. |
| Complete | Work | 2B: Failed-start cleanup | `failed_reader_initialization_leaves_no_actor_or_lock` verifies no writer or open test handles after failure and successful retry; spawning follows all fallible initialization. |
| Complete | Work | 2C: Parent sync and retry barriers | `src/fileio/mod.rs::startup_retries_parent_and_wal_directory_barriers` records ancestor/WAL ordering and retries injected failures before writes. |
| Complete | Test | Ownership/startup cases | Ownership, directory-hook, reader-initialization, and existing rotation/restart cases pass in `cargo test --all-targets --offline --locked`. |
| Complete | Gate | Exclusive ownership and barriers before writes | Separate-process exclusion/release, surviving-handle tests, and nested-directory sync failure/retry regression all pass. |

## Phase 3: Expose Only Synced Records to Readers

Goal: close R3 and remove R7's repeated metadata reads from normal active-file point access.

Scope:

- **3A:** Share durable record ends through a small synchronized addition to existing writer/reader state. Initialize from recovery; update only after successful sync and before replies. Cover rotation so membership and boundaries are captured consistently. Normal reads must not refresh their limit from tentative on-disk slots.
- **3B:** Keep the stream API and open file handles. Capture the file set and durable ends on creation and stop at those limits. Coordinate handle acquisition with deletion through the shared synchronization so completed streams remain usable after trim. Document fixed-boundary replay and existing inclusive resume semantics.

Completion gate: paused/failed sync never exposes the tentative suffix through manager reads/streams; later appends do not extend existing streams; pre-trim streams finish normally. A cached 64-byte active point read uses one data read and no commit-header reads/stat.

Testing plan: pause before underlying sync, return a sync error, and check visibility immediately after a successful reply; compare visibility to the last successful sync. Capture streams before append and during rotation/trim; verify IDs/order/end. Count point-read operations and preserve resume tests.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Scope | R3 durable visibility and R7 active-read overhead | `SharedReaders` carries durable ends; manager reads and snapshots use those ends without refreshing tentative on-disk slots. |
| Complete | Work | 3A: Publish ends after successful sync | `WalWriter::commit_group` publishes under the cache lock after sync and before replies; `readers_and_snapshots_never_expose_a_pending_or_failed_sync` passes. |
| Complete | Work | 3B: Capture stream boundaries with open handles | `WriteAhead::snapshot` captures handles and ends under one lock. Append/rotation/trim regressions pass; README documents finite replay and inclusive resume. |
| Complete | Test | Publication/capture races and read counts | Paused/failed-sync, fixed-EOF, rotation/trim, and point-read counter cases in `tests/reliability.rs` pass. |
| Complete | Gate | Durable bounded replay and one-read point access | Snapshot tests preserve IDs/order across append and trim; a 64-byte active read measures 1 read and 0 stats. |

## Phase 4: Bound Queued Work and Release Deleted Cache Entries

Goal: close R5/R6 with bounded admission/draining and prompt cache eviction.

Scope:

- **4A:** Add a bounded command queue and configurable maximum encoded batch size. Validate sizes with checked arithmetic before enqueueing; use asynchronous send so a full queue does not block the executor. Document queued-byte bounds and in-flight encoding overhead. Caller-owned requests waiting for admission remain outside that bound; it is not a hard process-memory cap.
- **4B:** Limit drain rounds by command count as well as bytes, including empty writes/trims. Preserve batch contiguity and existing rotation/grouping behavior. Account for one batch exceeding the group byte target, and reject batches above the admission limit.
- **4C:** Evict cache references as part of successful deletion rather than waiting for another read. Reuse Phase 3's shared state for lifecycle updates so write-only workloads do not accumulate undrained events. Preserve immediate unlink and streams' open handles. Clarify that trim byte counts do not imply freed storage while streams retain files.

Completion gate: queued commands/encoded bytes stay within configured bounds under stalled sync; zero-byte traffic cannot prevent a drain round from finishing; deleted files have no idle cache references after trim returns without another read. Existing streams still replay trimmed files.

Testing plan: stalled sync with submissions beyond capacity; cancellation while waiting; batch-limit boundaries/checked arithmetic; zero-byte traffic and per-round progress. Warm a cache entry, trim then remain idle, and count handle closure; repeat with a live stream, then finish/drop it. Exercise rotation without reads and count lifecycle work. Preserve the one-sync/group regression.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Scope | R5/R6 queue bounds and cache cleanup | `src/writer.rs` uses bounded async admission, byte/command drain limits, and direct cache eviction; lifecycle event queue removed. |
| Complete | Work | 4A: Bounded async queue and checked batch limit | `bounded_admission_is_async_and_cancellable_before_enqueue` exercises capacity 1, cancellation, and exact/over-limit sizes; invalid zero/overflow options fail before startup. README gives memory bounds. |
| Complete | Work | 4B: Command-count drain limit | Writer unit tests drain 192 zero-byte commands in three rounds without loss and preserve the next command at the byte limit; existing contiguity/one-sync tests pass. |
| Complete | Work | 4C: Cache eviction and lifecycle updates | Idle-trim and write-only-retention handle counters pass; pre-trim streams replay and release deleted descriptors at EOF even while the stream object remains alive. |
| Complete | Test | Admission, progress, and cache cases | Focused reliability/writer tests pass alongside existing group-commit, preallocation, trim, and retention suites. |
| Complete | Gate | Bounded queued work and prompt cache release | Stalled-sync admission/cancellation, zero-byte drain limits, idle cache eviction, and retained-stream lifetime assertions all pass. |

## Phase 5: Reuse Buffered Decoding for Recovery

Goal: close the remaining R7 recovery overhead while preserving v3 parsing and recovery behavior.

Scope:

- **5A:** Reuse buffered record decoding/checksum validation between streaming and recovery. Verify payload slices without allocating an owned recovery payload per record. Share parsing checks needed for Phase 1's error classification; keep the refactor local.
- **5D:** Add a recovery operation-count fixture and focused recovery case to the existing benchmark. Compare recovery reads/allocations and existing read/write benchmarks before/after changes; update affected README performance descriptions from evidence.

Completion gate: recovering 1,000 × 64-byte records uses at most 16 reads including metadata, with no per-record payload allocation. Existing decoding/recovery/rotation/preallocation tests pass; steady-state commits retain one sync/group.

Testing plan: fixtures for empty/binary payloads, buffer-spanning records, torn headers/payloads, corrupt lengths/hashes, and injected I/O failure. Measure review-fixture read/allocation counts and compare benchmarks on the same storage. Run formatting, Clippy, and repository tests.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Scope | R7 buffered recovery | `RecordBuffer` is shared by streaming/recovery; current recovery uses 5 reads and 1 allocation for 1,000 × 64-byte records. |
| Complete | Work | 5A: Reuse decoder without owned recovery payloads | `src/logfile.rs::RecordBuffer` borrows verified payloads and shares `RecordHeader` parsing/checksums with point reads; failure clears partially overwritten buffers. |
| Complete | Work | 5D: Focused measurement and documentation | [PERFORMANCE.md](PERFORMANCE.md), [raw runs](docs/performance-results.json), and the recovery case in `examples/bench.rs` record five alternating comparisons against `6bc7aed`. |
| Complete | Test | Buffer-boundary, corruption, and I/O fixtures | Large/empty/binary, buffer-spanning, unsynced torn-tail, corruption, and EIO cases pass; formatting, Clippy, all tests, and three runnable examples pass. |
| Complete | Gate | At most 16 recovery reads; no per-record payload allocation | `tests/recovery_cost.rs` measures 5 reads/1 allocation for both 1 and 1,000 records; `steady_state_commit_uses_one_data_sync` remains green. |

## Execution Order

Complete Phases 1–2 first, then Phase 3's durable boundaries, Phase 4's queue/cache fixes, and Phase 5's buffered recovery. Retained phase/item identifiers are unchanged; gaps reflect removed scope. Mark items complete only with implementation and validation evidence.
