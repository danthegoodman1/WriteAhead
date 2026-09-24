//! The write-side actor: owns the active log file and the directory
//! lifecycle. All writes funnel through one thread, which group-commits
//! queued batches (one pwrite + one fdatasync for everything pending),
//! rotates before a commit would exceed `max_file_size`, and applies retention.
//!
//! Rotation creates the next file *before* sealing the old one, so every
//! crash window lands in a state startup recovery already heals: an
//! unsealed non-active file is scanned and sealed, an empty highest file
//! becomes the active file.

use anyhow::{anyhow, Context, Result};
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::{Arc, RwLock, Weak};
use std::thread;
use tracing::{debug, error, trace, warn};

use crate::fileio::{sync_dir, FileIo};
use crate::logfile::{
    append_footer, encode_records, log_file_path, now_ms, read_commit_state, write_committed_end,
    write_header, CommitState, Logfile, LogfileError, FILE_HEADER_SIZE, FOOTER_SIZE,
    RECORD_HEADER_SIZE,
};
use crate::record::RecordID;
use crate::write_ahead::{ReaderMap, SharedReaders, WriteAheadError, WriteAheadOptions};

pub(crate) enum WriterCommand {
    Write(flume::Sender<Result<WriteAck>>, Vec<Vec<u8>>),
    Trim(flume::Sender<TrimStats>, u64),
}

/// What an explicit [WriteHandle::trim_before] call deleted.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TrimStats {
    pub files_deleted: u64,
    /// Lengths of unlinked files. Storage may remain pinned by open streams.
    pub bytes_reclaimed: u64,
}

#[derive(Debug)]
pub(crate) struct WriteAck {
    /// The file the batch landed in (rotation can happen between submission
    /// and commit, so this is part of the ack).
    pub file_id: u64,
    pub offsets: Vec<u64>,
}

#[derive(Debug, Clone)]
pub(crate) struct FileMeta {
    pub path: PathBuf,
    pub size: u64,
    pub seal_timestamp_ms: Option<u64>,
}

/// Cheaply cloneable handle for submitting durable writes from any task or
/// thread. A write resolves once its records are fsync'd; writes submitted
/// while a commit is in flight coalesce into the next group commit and
/// share its fsync.
///
/// Handles stay valid across log rotations (each ack names the file the
/// records landed in) and keep the writer alive even if the owning
/// [crate::WriteAhead] is dropped.
/// Dropping the last handle waits for accepted commands and thread exit.
#[derive(Debug, Clone)]
pub struct WriteHandle {
    runtime: Arc<WriterRuntime>,
}

#[derive(Debug)]
struct WriterRuntime {
    tx: Option<flume::Sender<WriterCommand>>,
    thread: Option<thread::JoinHandle<()>>,
    max_batch_bytes: usize,
}

impl Drop for WriterRuntime {
    fn drop(&mut self) {
        // Last handle: close admission, drain accepted commands, then release
        // the writer's directory lock. No actor retains this runtime Arc.
        drop(self.tx.take());
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

impl WriteHandle {
    /// Writes one record durably and returns its address.
    pub async fn write(&self, record: Vec<u8>) -> Result<RecordID> {
        let mut ids = self.write_batch(vec![record]).await?;
        Ok(ids.remove(0))
    }

    /// Writes a batch of records durably (contiguous, all in one file) and
    /// returns their addresses. Admission waits asynchronously when the queue
    /// is full. Cancellation after admission does not roll back the write.
    pub async fn write_batch(&self, records: Vec<Vec<u8>>) -> Result<Vec<RecordID>> {
        if batch_bytes(&records)? > self.runtime.max_batch_bytes {
            return Err(WriteAheadError::BatchTooLarge(self.runtime.max_batch_bytes).into());
        }
        let (tx, rx) = flume::bounded(1);
        self.runtime
            .tx
            .as_ref()
            .expect("live runtime")
            .send_async(WriterCommand::Write(tx, records))
            .await
            .map_err(|_| WriteAheadError::WriterClosed)?;
        let ack = rx
            .recv_async()
            .await
            .map_err(|_| WriteAheadError::WriterClosed)??;
        Ok(ack
            .offsets
            .iter()
            .map(|offset| RecordID::new(ack.file_id, *offset))
            .collect())
    }

    /// Deletes every sealed log file with id strictly below `file_id`,
    /// reclaiming disk space the consumer no longer needs — e.g. once a
    /// replay high-water mark has moved past a rotated file, or after an
    /// explicit compaction rewrote its live records. The active file and
    /// records at or above `file_id` are never touched.
    ///
    /// Resolves after the files are gone; failures to delete individual
    /// files are logged and skipped (retryable). Streams created before the
    /// trim keep their open file handles and finish undisturbed; new reads
    /// into trimmed files return `LogfileNotFound`.
    pub async fn trim_before(&self, file_id: u64) -> Result<TrimStats> {
        let (tx, rx) = flume::bounded(1);
        self.runtime
            .tx
            .as_ref()
            .expect("live runtime")
            .send_async(WriterCommand::Trim(tx, file_id))
            .await
            .map_err(|_| WriteAheadError::WriterClosed)?;
        Ok(rx
            .recv_async()
            .await
            .map_err(|_| WriteAheadError::WriterClosed)?)
    }
}

type ReplyTx = flume::Sender<Result<WriteAck>>;

/// Stop draining a group-commit round once this many payload bytes are
/// queued, so latency stays bounded under a firehose of writers.
const MAX_GROUP_COMMIT_BYTES: usize = 4 * 1024 * 1024;
const MAX_GROUP_COMMIT_COMMANDS: usize = 64;

/// Source buffer for zero-filling allocation windows.
static ZEROS: [u8; 64 * 1024] = [0; 64 * 1024];

pub(crate) fn validate_options(options: &WriteAheadOptions) -> Result<()> {
    if options.queue_capacity == 0
        || options.max_batch_bytes == 0
        || options
            .max_batch_bytes
            .checked_add(MAX_GROUP_COMMIT_BYTES)
            .is_none()
    {
        return Err(WriteAheadError::InvalidQueueOptions.into());
    }
    Ok(())
}

fn batch_bytes(records: &[Vec<u8>]) -> Result<usize> {
    records.iter().try_fold(0usize, |total, record| {
        if record.len() >= u32::MAX as usize {
            return Err(LogfileError::RecordTooLarge.into());
        }
        total
            .checked_add(RECORD_HEADER_SIZE as usize)
            .and_then(|size| size.checked_add(record.len()))
            .ok_or_else(|| WriteAheadError::BatchTooLarge(usize::MAX).into())
    })
}

/// Check both limits before receiving another command, so ending a round
/// never consumes a command that belongs to the next one.
fn command_group(
    first: WriterCommand,
    recv: &flume::Receiver<WriterCommand>,
) -> impl Iterator<Item = WriterCommand> + '_ {
    let mut first = Some(first);
    let mut bytes = 0usize;
    let mut commands = 0usize;
    std::iter::from_fn(move || {
        if commands == MAX_GROUP_COMMIT_COMMANDS || bytes >= MAX_GROUP_COMMIT_BYTES {
            return None;
        }
        let command = first.take().or_else(|| recv.try_recv().ok())?;
        commands += 1;
        if let WriterCommand::Write(_, data) = &command {
            bytes += batch_bytes(data).expect("validated batch");
        }
        Some(command)
    })
}

pub(crate) struct WalWriter<F: FileIo> {
    options: WriteAheadOptions,
    fio: F,
    file_id: u64,
    /// Logical end of committed records. Physical file length may be ahead
    /// because the active file is zero-filled in allocation windows.
    records_end: u64,
    /// End of the zero-filled allocation window.
    allocated_end: u64,
    commit_state: CommitState,
    /// Every live log file (including the active one), for retention.
    files: BTreeMap<u64, FileMeta>,
    recv: flume::Receiver<WriterCommand>,
    readers: Weak<RwLock<ReaderMap<F>>>,
    _directory_lock: std::fs::File,
}

impl<F: FileIo + 'static> WalWriter<F> {
    /// Opens the active file (writing its header if new) and applies startup
    /// retention synchronously, then spawns the actor thread. `files` is the
    /// recovered file registry, which may or may not include the active file.
    pub(crate) fn launch(
        options: WriteAheadOptions,
        active_id: u64,
        active_path: PathBuf,
        mut files: BTreeMap<u64, FileMeta>,
        directory_lock: std::fs::File,
    ) -> Result<(WriteHandle, SharedReaders<F>)> {
        let (tx, rx) = flume::bounded(options.queue_capacity);
        let max_batch_bytes = options.max_batch_bytes;

        let mut fio = F::open(&active_path)?;
        let len = fio.len()?;
        let (records_end, commit_state) = if len == 0 {
            write_header(&mut fio)?;
            fio.sync()?;
            (FILE_HEADER_SIZE, read_commit_state(&fio)?)
        } else if len < FILE_HEADER_SIZE {
            // The manager recovers files before launching the writer, so a
            // partial header here means that contract was violated.
            return Err(anyhow!(LogfileError::InvalidHeader));
        } else {
            let state = read_commit_state(&fio)?;
            (state.records_end, state)
        };
        // Also required on retry when a previous header sync succeeded but
        // its directory sync failed. Visibility alone is not durability.
        sync_dir(&options.log_dir)?;
        // Only bytes this writer filled count as allocated. Recovery has
        // already trimmed the active file to its committed end.
        let allocated_end = records_end;
        files.insert(
            active_id,
            FileMeta {
                path: active_path,
                size: records_end,
                seal_timestamp_ms: None,
            },
        );

        let mut writer = Self {
            options,
            fio,
            file_id: active_id,
            records_end,
            allocated_end,
            commit_state,
            files,
            recv: rx,
            readers: Weak::new(),
            _directory_lock: directory_lock,
        };
        // Fill the active file's first window after its header is durable.
        // The next record sync persists the fill, and its end is never used
        // as a logical record boundary.
        writer.ensure_preallocated(records_end);
        // Synchronous so `start()` returns with retention already applied
        writer.apply_retention();

        // Finish every fallible reader open before starting the actor. Failed
        // startup drops all handles and the lock without a hidden writer.
        let mut cache = BTreeMap::new();
        for (id, meta) in &writer.files {
            let logfile = Logfile::<F>::open(&meta.path)?;
            let end = logfile.records_end()?;
            cache.insert(*id, (Arc::new(logfile), end));
        }
        let readers = Arc::new(RwLock::new(cache));
        writer.readers = Arc::downgrade(&readers);
        let thread = thread::Builder::new()
            .name("wal-writer".into())
            .spawn(move || writer.actor_loop())
            .context("Failed to spawn writer thread")?;
        Ok((
            WriteHandle {
                runtime: Arc::new(WriterRuntime {
                    tx: Some(tx),
                    thread: Some(thread),
                    max_batch_bytes,
                }),
            },
            readers,
        ))
    }

    fn actor_loop(mut self) {
        while let Ok(first) = self.recv.recv() {
            let mut writes: Vec<(ReplyTx, Vec<Vec<u8>>)> = Vec::new();
            // Trims drained alongside writes run after the commit, so a trim
            // submitted after a write never races that write's file.
            let mut trims: Vec<(flume::Sender<TrimStats>, u64)> = Vec::new();
            for command in command_group(first, &self.recv) {
                match command {
                    WriterCommand::Write(reply, data) => writes.push((reply, data)),
                    WriterCommand::Trim(reply, upto) => trims.push((reply, upto)),
                }
            }

            if !writes.is_empty() {
                self.commit_group(writes);
            }
            for (reply, upto) in trims {
                let _ = reply.send(self.trim_before(upto));
            }

            // Rotation failure is not fatal: the current file keeps
            // accepting writes and rotation retries on the next commit.
            if let Err(e) = self.maybe_rotate() {
                error!("log rotation failed: {e:#}");
            }
        }
        trace!("wal writer actor shut down");
    }

    /// Encodes every queued batch into one buffer, does one write and one
    /// fsync, then answers each caller with its own offsets.
    fn commit_group(&mut self, group: Vec<(ReplyTx, Vec<Vec<u8>>)>) {
        // A dropped reply receiver is the caller's business, not our error:
        // send results are ignored throughout.
        // Admission already checked every record and batch. The drain limits
        // bound this sum, so no second validation/copy of the group is needed.
        let total: usize = group
            .iter()
            .map(|(_, records)| batch_bytes(records).expect("validated batch"))
            .sum();
        if total == 0 {
            // Nothing to persist (all batches empty): ack without an fsync
            for (reply, _) in group {
                let _ = reply.send(Ok(WriteAck {
                    file_id: self.file_id,
                    offsets: Vec::new(),
                }));
            }
            return;
        }

        // Keep the entire group in one file. If it does not fit in a
        // non-empty active file, rotate before assigning offsets. A group
        // larger than an empty file's capacity is accepted as a soft-cap
        // exception, then the oversized file is rotated after the commit.
        let total_u64 = total as u64;
        let proposed_end = self.records_end.checked_add(total_u64);
        let would_exceed = proposed_end
            .map(|end| end > self.options.max_file_size)
            .unwrap_or(true);
        if would_exceed && self.records_end > FILE_HEADER_SIZE {
            if let Err(e) = self.rotate() {
                let msg = format!("Failed to rotate before commit: {e:#}");
                for (reply, _) in group {
                    let _ = reply.send(Err(anyhow!("{msg}")));
                }
                return;
            }
        }

        let mut buf = Vec::with_capacity(total);
        let mut current_offset = self.records_end;
        let mut per_caller_offsets: Vec<Vec<u64>> = Vec::with_capacity(group.len());
        for (_, records) in &group {
            let offsets = encode_records(&mut buf, records, current_offset);
            current_offset += batch_bytes(records).expect("validated batch") as u64;
            per_caller_offsets.push(offsets);
        }

        let result = (|| -> Result<CommitState> {
            self.ensure_preallocated(current_offset);
            self.fio
                .write_at(self.records_end, &buf)
                .context("Failed to write records")?;
            let state = write_committed_end(&mut self.fio, self.commit_state, current_offset)?;
            // The record bytes, logical-end slot, and any allocation-window
            // extension are made durable by this single fdatasync.
            self.fio.sync().context("Failed to sync records")?;
            Ok(state)
        })();

        match result {
            Ok(commit_state) => {
                self.records_end = current_offset;
                self.commit_state = commit_state;
                if let Some(meta) = self.files.get_mut(&self.file_id) {
                    meta.size = current_offset;
                }
                if let Some(readers) = self.readers.upgrade() {
                    readers
                        .write()
                        .expect("reader cache poisoned")
                        .get_mut(&self.file_id)
                        .expect("active reader")
                        .1 = current_offset;
                }
                for ((reply, _), offsets) in group.into_iter().zip(per_caller_offsets) {
                    let _ = reply.send(Ok(WriteAck {
                        file_id: self.file_id,
                        offsets,
                    }));
                }
            }
            Err(e) => {
                // Nothing was acknowledged: records_end and commit_state stay
                // put. Failed writes stay invisible to live readers, but may
                // survive recovery if their bytes reached disk. anyhow::Error
                // isn't Clone, so each caller gets its own copy of the message.
                let msg = format!("{e:#}");
                for (reply, _) in group {
                    let _ = reply.send(Err(anyhow!("{msg}")));
                }
            }
        }
    }

    fn maybe_rotate(&mut self) -> Result<()> {
        if self.records_end <= self.options.max_file_size {
            return Ok(());
        }

        self.rotate()
    }

    fn rotate(&mut self) -> Result<()> {
        // Create the next file first: if we crash (or fail) between here and
        // the seal below, recovery sees an unsealed non-active file and
        // heals it, and an empty highest file becomes the active file.
        let next_id = self
            .file_id
            .checked_add(1)
            .ok_or(WriteAheadError::FileIdExhausted)?;
        let path = log_file_path(&self.options.log_dir, next_id);
        let mut fio = F::open(&path)?;
        write_header(&mut fio)?;
        fio.sync()?;
        sync_dir(&self.options.log_dir)?;
        let commit_state = read_commit_state(&fio)?;
        let readers = self.readers.upgrade();
        let reader = readers
            .as_ref()
            .map(|_| Logfile::<F>::open(&path))
            .transpose()?;
        self.files.insert(
            next_id,
            FileMeta {
                path: path.clone(),
                size: FILE_HEADER_SIZE,
                seal_timestamp_ms: None,
            },
        );
        if let (Some(readers), Some(reader)) = (readers, reader) {
            readers
                .write()
                .expect("reader cache poisoned")
                .insert(next_id, (Arc::new(reader), FILE_HEADER_SIZE));
        }

        // Seal the old file. On failure it stays unsealed and recovery
        // heals it at the next start; new writes still go to the new file.
        let ts = now_ms();
        match append_footer(&mut self.fio, self.records_end, ts) {
            Ok(()) => {
                if let Some(meta) = self.files.get_mut(&self.file_id) {
                    meta.size = self.records_end + FOOTER_SIZE;
                    meta.seal_timestamp_ms = Some(ts);
                }
                debug!("sealed log file {}", self.file_id);
            }
            Err(e) => warn!("failed to seal log file {}: {e:#}", self.file_id),
        }

        self.fio = fio;
        self.file_id = next_id;
        self.records_end = FILE_HEADER_SIZE;
        self.allocated_end = FILE_HEADER_SIZE;
        self.commit_state = commit_state;
        self.ensure_preallocated(FILE_HEADER_SIZE);

        self.apply_retention();
        Ok(())
    }

    /// Zero-fills the active file through the allocation window covering
    /// required_end, so commits overwrite blocks that are already allocated
    /// and written. The fill starts at required_end because the caller's
    /// records cover the space before it; the next commit's fdatasync
    /// persists both. The fill is best-effort: on failure (such as ENOSPC) it
    /// logs and leaves the rest of the window unfilled, so commits and
    /// retention proceed and the next window retries.
    /// None (and defensively Some(0)) leaves normal EOF-extending writes in
    /// place. Windows never extend beyond max_file_size unless a single
    /// oversized group is already past that soft cap.
    fn ensure_preallocated(&mut self, required_end: u64) {
        let Some(chunk) = self
            .options
            .preallocation_chunk_size
            .filter(|size| *size > 0)
        else {
            return;
        };
        let rounded = required_end
            .checked_add(chunk - 1)
            .map(|value| value / chunk * chunk)
            .unwrap_or(u64::MAX);
        let target = if required_end <= self.options.max_file_size {
            rounded.min(self.options.max_file_size).max(required_end)
        } else {
            // Oversized batches retain the existing soft-cap behavior but do
            // not allocate still farther past the cap.
            required_end
        };
        if target <= self.allocated_end {
            return;
        }
        let mut offset = self.allocated_end.max(required_end);
        while offset < target {
            let len = (target - offset).min(ZEROS.len() as u64);
            if let Err(e) = self.fio.write_at(offset, &ZEROS[..len as usize]) {
                warn!(
                    "failed to zero-fill log file {} to {target} bytes; leaving the window unfilled: {e:#}",
                    self.file_id
                );
                break;
            }
            offset += len;
        }
        self.allocated_end = target;
    }

    /// Deletes sealed files per `RetentionOptions`. The active file is never
    /// deleted. Failures are logged, not fatal — retention retries on the
    /// next rotation.
    fn apply_retention(&mut self) {
        if let Some(meta) = self.files.get_mut(&self.file_id) {
            meta.size = self.records_end;
        }

        let mut doomed: Vec<u64> = Vec::new();

        let ttl_ms = self.options.retention.ttl.as_millis() as u64;
        if ttl_ms > 0 {
            let cutoff = now_ms().saturating_sub(ttl_ms);
            doomed.extend(
                self.files
                    .iter()
                    .filter(|(id, meta)| {
                        **id != self.file_id && meta.seal_timestamp_ms.is_some_and(|ts| ts < cutoff)
                    })
                    .map(|(id, _)| *id),
            );
        }

        let max_total = self.options.retention.max_total_size;
        if max_total > 0 {
            let doomed_size: u64 = doomed
                .iter()
                .filter_map(|id| self.files.get(id))
                .map(|m| m.size)
                .sum();
            let mut total: u64 = self.files.values().map(|m| m.size).sum::<u64>() - doomed_size;
            for (id, meta) in &self.files {
                if total <= max_total {
                    break;
                }
                if *id == self.file_id || meta.seal_timestamp_ms.is_none() || doomed.contains(id) {
                    continue;
                }
                doomed.push(*id);
                total -= meta.size;
            }
        }

        self.delete_files(doomed, "retention");
    }

    /// Deletes every sealed file with id strictly below `upto`. The active
    /// file is never deleted; a non-active file that missed its seal (failed
    /// rotation) is skipped until recovery heals it.
    fn trim_before(&mut self, upto: u64) -> TrimStats {
        let doomed: Vec<u64> = self
            .files
            .iter()
            .filter(|(id, meta)| {
                **id < upto && **id != self.file_id && meta.seal_timestamp_ms.is_some()
            })
            .map(|(id, _)| *id)
            .collect();
        self.delete_files(doomed, "trim")
    }

    /// Removes files from disk and the registry, notifying the read side.
    /// Per-file failures are logged and the file is kept (retryable).
    fn delete_files(&mut self, doomed: Vec<u64>, why: &str) -> TrimStats {
        let mut stats = TrimStats::default();
        if doomed.is_empty() {
            return stats;
        }
        for id in doomed {
            if let Some(meta) = self.files.remove(&id) {
                debug!("{}: deleting logfile {}", why, id);
                if let Err(e) = std::fs::remove_file(&meta.path) {
                    warn!("{}: failed to delete {}: {e}", why, meta.path.display());
                    self.files.insert(id, meta);
                    continue;
                }
                stats.files_deleted += 1;
                stats.bytes_reclaimed += meta.size;
                if let Some(readers) = self.readers.upgrade() {
                    readers.write().expect("reader cache poisoned").remove(&id);
                }
            }
        }
        if let Err(e) = sync_dir(&self.options.log_dir) {
            warn!("{}: failed to sync log dir: {e:#}", why);
        }
        stats
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_byte_commands_have_bounded_rounds_without_dropping_work() {
        let (tx, rx) = flume::bounded(3 * MAX_GROUP_COMMIT_COMMANDS);
        for i in 0..3 * MAX_GROUP_COMMIT_COMMANDS {
            if i % 2 == 0 {
                tx.send(WriterCommand::Trim(flume::bounded(1).0, i as u64))
                    .unwrap();
            } else {
                tx.send(WriterCommand::Write(flume::bounded(1).0, Vec::new()))
                    .unwrap();
            }
        }
        for remaining in (0..3).rev() {
            let group: Vec<_> = command_group(rx.recv().unwrap(), &rx).collect();
            assert_eq!(group.len(), MAX_GROUP_COMMIT_COMMANDS);
            assert_eq!(rx.len(), remaining * MAX_GROUP_COMMIT_COMMANDS);
        }
    }

    #[test]
    fn byte_limit_does_not_consume_the_next_command() {
        let (tx, rx) = flume::bounded(2);
        tx.send(WriterCommand::Trim(flume::bounded(1).0, 7))
            .unwrap();
        let first = WriterCommand::Write(
            flume::bounded(1).0,
            vec![vec![
                0;
                MAX_GROUP_COMMIT_BYTES - RECORD_HEADER_SIZE as usize
            ]],
        );
        assert_eq!(command_group(first, &rx).count(), 1);
        assert!(matches!(rx.try_recv().unwrap(), WriterCommand::Trim(_, 7)));
    }
}
