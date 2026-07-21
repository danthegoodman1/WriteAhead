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
use std::thread;
use tracing::{debug, error, trace, warn};

use crate::fileio::{sync_dir, FileIo};
use crate::logfile::{
    append_footer, encode_records, log_file_path, now_ms, read_commit_state, write_committed_end,
    write_header, CommitState, LogfileError, FILE_HEADER_SIZE, FOOTER_SIZE, RECORD_HEADER_SIZE,
};
use crate::record::RecordID;
use crate::write_ahead::{WriteAheadError, WriteAheadOptions};

pub(crate) enum WriterCommand {
    Write(flume::Sender<Result<WriteAck>>, Vec<Vec<u8>>),
    Trim(flume::Sender<TrimStats>, u64),
}

/// What an explicit [WriteHandle::trim_before] call deleted.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TrimStats {
    pub files_deleted: u64,
    pub bytes_reclaimed: u64,
}

#[derive(Debug)]
pub(crate) struct WriteAck {
    /// The file the batch landed in (rotation can happen between submission
    /// and commit, so this is part of the ack).
    pub file_id: u64,
    pub offsets: Vec<u64>,
}

/// Lifecycle notifications for the read side's file cache.
#[derive(Debug, Clone)]
pub(crate) enum WriterEvent {
    Created(u64, PathBuf),
    Sealed(u64),
    Deleted(u64),
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
#[derive(Debug, Clone)]
pub struct WriteHandle {
    pub(crate) tx: flume::Sender<WriterCommand>,
}

impl WriteHandle {
    /// Writes one record durably and returns its address.
    pub async fn write(&self, record: Vec<u8>) -> Result<RecordID> {
        let mut ids = self.write_batch(vec![record]).await?;
        Ok(ids.remove(0))
    }

    /// Writes a batch of records durably (contiguous, all in one file) and
    /// returns their addresses.
    pub async fn write_batch(&self, records: Vec<Vec<u8>>) -> Result<Vec<RecordID>> {
        let (tx, rx) = flume::bounded(1);
        self.tx
            .send(WriterCommand::Write(tx, records))
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
        self.tx
            .send(WriterCommand::Trim(tx, file_id))
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

fn batch_bytes(records: &[Vec<u8>]) -> usize {
    records
        .iter()
        .map(|r| RECORD_HEADER_SIZE as usize + r.len())
        .sum()
}

pub(crate) struct WalWriter<F: FileIo> {
    options: WriteAheadOptions,
    fio: F,
    file_id: u64,
    /// Logical end of committed records. Physical file length may be ahead
    /// because the active file is sparsely grown in allocation windows.
    records_end: u64,
    allocated_end: u64,
    commit_state: CommitState,
    /// Every live log file (including the active one), for retention.
    files: BTreeMap<u64, FileMeta>,
    recv: flume::Receiver<WriterCommand>,
    /// Read side may be gone (manager dropped, handles alive) — send errors
    /// are ignored throughout.
    events: flume::Sender<WriterEvent>,
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
    ) -> Result<(flume::Sender<WriterCommand>, flume::Receiver<WriterEvent>)> {
        let (tx, rx) = flume::unbounded();
        let (event_tx, event_rx) = flume::unbounded();

        let mut fio = F::open(&active_path)?;
        let len = fio.len()?;
        let (records_end, commit_state) = if len == 0 {
            write_header(&mut fio)?;
            fio.sync()?;
            sync_dir(&options.log_dir)?;
            (FILE_HEADER_SIZE, read_commit_state(&fio)?)
        } else if len < FILE_HEADER_SIZE {
            // The manager recovers files before launching the writer, so a
            // partial header here means that contract was violated.
            return Err(anyhow!(LogfileError::InvalidHeader));
        } else {
            let state = read_commit_state(&fio)?;
            (state.records_end, state)
        };
        let allocated_end = len.max(records_end);
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
            events: event_tx,
        };
        // Grow the active file after its header is durable. The allocation
        // itself is covered by the next record sync and is never used as a
        // logical record boundary.
        writer.ensure_preallocated(records_end)?;
        // Synchronous so `start()` returns with retention already applied
        writer.apply_retention();

        thread::Builder::new()
            .name("wal-writer".into())
            .spawn(move || writer.actor_loop())
            .context("Failed to spawn writer thread")?;
        Ok((tx, event_rx))
    }

    fn actor_loop(mut self) {
        while let Ok(first) = self.recv.recv() {
            let mut writes: Vec<(ReplyTx, Vec<Vec<u8>>)> = Vec::new();
            // Trims drained alongside writes run after the commit, so a trim
            // submitted after a write never races that write's file.
            let mut trims: Vec<(flume::Sender<TrimStats>, u64)> = Vec::new();
            let mut queued_bytes = 0usize;

            match first {
                WriterCommand::Write(reply, data) => {
                    queued_bytes += batch_bytes(&data);
                    writes.push((reply, data));
                }
                WriterCommand::Trim(reply, upto) => trims.push((reply, upto)),
            }

            // Group commit: drain whatever else is already queued so all
            // pending writes share a single write+fsync.
            while queued_bytes < MAX_GROUP_COMMIT_BYTES {
                match self.recv.try_recv() {
                    Ok(WriterCommand::Write(reply, data)) => {
                        queued_bytes += batch_bytes(&data);
                        writes.push((reply, data));
                    }
                    Ok(WriterCommand::Trim(reply, upto)) => trims.push((reply, upto)),
                    Err(_) => break,
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
        let mut valid: Vec<(ReplyTx, Vec<Vec<u8>>)> = Vec::with_capacity(group.len());
        let mut total = 0usize;
        for (reply, records) in group {
            if records.iter().any(|r| r.len() >= u32::MAX as usize) {
                let _ = reply.send(Err(anyhow!(LogfileError::RecordTooLarge)));
                continue;
            }
            total += batch_bytes(&records);
            valid.push((reply, records));
        }
        if valid.is_empty() {
            return;
        }
        if total == 0 {
            // Nothing to persist (all batches empty): ack without an fsync
            for (reply, _) in valid {
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
                for (reply, _) in valid {
                    let _ = reply.send(Err(anyhow!("{}", msg.clone())));
                }
                return;
            }
        }

        let mut buf = Vec::with_capacity(total);
        let mut current_offset = self.records_end;
        let mut per_caller_offsets: Vec<Vec<u64>> = Vec::with_capacity(valid.len());
        for (_, records) in &valid {
            let offsets = encode_records(&mut buf, records, current_offset);
            current_offset += batch_bytes(records) as u64;
            per_caller_offsets.push(offsets);
        }

        let result = (|| -> Result<CommitState> {
            self.ensure_preallocated(current_offset)?;
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
                for ((reply, _), offsets) in valid.into_iter().zip(per_caller_offsets) {
                    let _ = reply.send(Ok(WriteAck {
                        file_id: self.file_id,
                        offsets,
                    }));
                }
            }
            Err(e) => {
                // Nothing was acknowledged: records_end and commit_state stay
                // put. Any partial bytes are beyond the committed boundary and
                // are discarded by recovery. anyhow::Error isn't Clone, so
                // each caller gets its own copy of the message.
                let msg = format!("{e:#}");
                for (reply, _) in valid {
                    let _ = reply.send(Err(anyhow!("{}", msg.clone())));
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
        self.files.insert(
            next_id,
            FileMeta {
                path: path.clone(),
                size: FILE_HEADER_SIZE,
                seal_timestamp_ms: None,
            },
        );
        let _ = self.events.send(WriterEvent::Created(next_id, path));

        // Seal the old file. On failure it stays unsealed and recovery
        // heals it at the next start; new writes still go to the new file.
        let ts = now_ms();
        match append_footer(&mut self.fio, self.records_end, ts) {
            Ok(()) => {
                if let Some(meta) = self.files.get_mut(&self.file_id) {
                    meta.size = self.records_end + FOOTER_SIZE;
                    meta.seal_timestamp_ms = Some(ts);
                }
                let _ = self.events.send(WriterEvent::Sealed(self.file_id));
                debug!("sealed log file {}", self.file_id);
            }
            Err(e) => warn!("failed to seal log file {}: {e:#}", self.file_id),
        }

        self.fio = fio;
        self.file_id = next_id;
        self.records_end = FILE_HEADER_SIZE;
        self.allocated_end = FILE_HEADER_SIZE;
        self.commit_state = commit_state;
        self.ensure_preallocated(FILE_HEADER_SIZE)?;

        self.apply_retention();
        Ok(())
    }

    /// Grows the active file to the allocation window covering required_end.
    /// None (and defensively Some(0)) leaves normal EOF-extending writes in
    /// place. Windows never extend beyond max_file_size unless a single
    /// oversized group is already past that soft cap.
    fn ensure_preallocated(&mut self, required_end: u64) -> Result<()> {
        let Some(chunk) = self
            .options
            .preallocation_chunk_size
            .filter(|size| *size > 0)
        else {
            return Ok(());
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
            return Ok(());
        }
        self.fio
            .set_len(target)
            .with_context(|| format!("Failed to preallocate active logfile to {target} bytes"))?;
        self.allocated_end = target;
        Ok(())
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
                let _ = self.events.send(WriterEvent::Deleted(id));
            }
        }
        if let Err(e) = sync_dir(&self.options.log_dir) {
            warn!("{}: failed to sync log dir: {e:#}", why);
        }
        stats
    }
}
