//! The write-side actor: owns the active log file and the directory
//! lifecycle. All writes funnel through one thread, which group-commits
//! queued batches (one pwrite + one fdatasync for everything pending),
//! rotates the file when it exceeds `max_file_size`, and applies retention.
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
    append_footer, encode_records, log_file_path, now_ms, write_header, LogfileError,
    FILE_HEADER_SIZE, FOOTER_SIZE, RECORD_HEADER_SIZE,
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
    file_length: u64,
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
        let file_length = if len == 0 {
            write_header(&mut fio)?;
            fio.sync()?;
            sync_dir(&options.log_dir)?;
            FILE_HEADER_SIZE
        } else if len < FILE_HEADER_SIZE {
            // The manager recovers files before launching the writer, so a
            // partial header here means that contract was violated.
            return Err(anyhow!(LogfileError::InvalidHeader));
        } else {
            len
        };
        files.insert(
            active_id,
            FileMeta {
                path: active_path,
                size: file_length,
                seal_timestamp_ms: None,
            },
        );

        let mut writer = Self {
            options,
            fio,
            file_id: active_id,
            file_length,
            files,
            recv: rx,
            events: event_tx,
        };
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
            if records.iter().any(|r| r.len() > u32::MAX as usize) {
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

        let mut buf = Vec::with_capacity(total);
        let mut current_offset = self.file_length;
        let mut per_caller_offsets: Vec<Vec<u64>> = Vec::with_capacity(valid.len());
        for (_, records) in &valid {
            let offsets = encode_records(&mut buf, records, current_offset);
            current_offset += batch_bytes(records) as u64;
            per_caller_offsets.push(offsets);
        }

        let result = self
            .fio
            .write_at(self.file_length, &buf)
            .context("Failed to write records")
            .and_then(|_| self.fio.sync().context("Failed to sync records"));

        match result {
            Ok(()) => {
                self.file_length = current_offset;
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
                // Nothing was acknowledged: file_length stays put, so any
                // partially written bytes are outside the record region and
                // get truncated by recovery. anyhow::Error isn't Clone, so
                // each caller gets its own copy of the message.
                let msg = format!("{e:#}");
                for (reply, _) in valid {
                    let _ = reply.send(Err(anyhow!("{}", msg.clone())));
                }
            }
        }
    }

    fn maybe_rotate(&mut self) -> Result<()> {
        if self.file_length <= self.options.max_file_size {
            return Ok(());
        }

        // Create the next file first: if we crash (or fail) between here and
        // the seal below, recovery sees an unsealed non-active file and
        // heals it, and an empty highest file becomes the active file.
        let next_id = self.file_id + 1;
        let path = log_file_path(&self.options.log_dir, next_id);
        let mut fio = F::open(&path)?;
        write_header(&mut fio)?;
        fio.sync()?;
        sync_dir(&self.options.log_dir)?;
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
        match append_footer(&mut self.fio, self.file_length, ts) {
            Ok(()) => {
                if let Some(meta) = self.files.get_mut(&self.file_id) {
                    meta.size = self.file_length + FOOTER_SIZE;
                    meta.seal_timestamp_ms = Some(ts);
                }
                let _ = self.events.send(WriterEvent::Sealed(self.file_id));
                debug!("sealed log file {}", self.file_id);
            }
            Err(e) => warn!("failed to seal log file {}: {e:#}", self.file_id),
        }

        self.fio = fio;
        self.file_id = next_id;
        self.file_length = FILE_HEADER_SIZE;

        self.apply_retention();
        Ok(())
    }

    /// Deletes sealed files per `RetentionOptions`. The active file is never
    /// deleted. Failures are logged, not fatal — retention retries on the
    /// next rotation.
    fn apply_retention(&mut self) {
        if let Some(meta) = self.files.get_mut(&self.file_id) {
            meta.size = self.file_length;
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
