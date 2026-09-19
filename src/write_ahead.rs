use anyhow::{anyhow, Context, Result};
use futures::Stream;
use std::{
    collections::BTreeMap,
    path::PathBuf,
    pin::Pin,
    sync::{Arc, RwLock},
    task::{Context as TaskContext, Poll},
    time::Duration,
};
use tracing::{debug, warn};

use crate::{
    fileio::{lock_dir, simple_file::SimpleFile, sync_parents, FileIo},
    logfile::{
        file_id_from_path, log_file_path, now_ms, recover_and_seal, recover_unsealed,
        LogFileStream, Logfile, FILE_HEADER_SIZE,
    },
    record::RecordID,
    writer::{WalWriter, WriteHandle},
};

/// The existing reader cache plus the last successfully synced end. All
/// publications and snapshot captures use the same short-lived lock.
pub(crate) type ReaderMap<F> = BTreeMap<u64, (Arc<Logfile<F>>, u64)>;
pub(crate) type SharedReaders<F> = Arc<RwLock<ReaderMap<F>>>;

/// The WAL manager: recovers on-disk state at startup, hands out
/// [WriteHandle]s for writing, and serves reads and streams.
///
/// `WriteAhead` is `Sync` — wrap it in an `Arc` and share it across tasks
/// and threads directly. Writes go through a dedicated writer thread that
/// owns the active file, group-committing everything queued behind one
/// fdatasync; rotation and retention happen on that thread too. Reads are
/// synchronous positional reads served by the page cache.
#[derive(Debug)]
pub struct WriteAhead<F: FileIo = SimpleFile> {
    options: WriteAheadOptions,
    readers: SharedReaders<F>,
    writer: Option<WriteHandle>,
}

#[derive(Debug, Clone)]
pub struct WriteAheadOptions {
    pub log_dir: PathBuf,
    /// Soft limit for the logical record region. The writer rotates before a
    /// commit group would cross it. A group larger than an empty file's
    /// capacity is kept intact in one oversized file, then rotated. The limit
    /// includes the format header; values below FILE_HEADER_SIZE therefore
    /// treat every non-empty group as oversized without truncating the header.
    pub max_file_size: u64,
    /// Sparse allocation-window size for active files. Ahead-of-time growth
    /// avoids persisting an EOF change on every fdatasync while committed-end
    /// metadata keeps the unused tail invisible to readers and recovery.
    /// None disables preallocation; Some(0) is also treated as disabled.
    pub preallocation_chunk_size: Option<u64>,
    /// Maximum queued commands, excluding the writer's current drain round.
    /// A full queue suspends async submission; must be greater than zero.
    pub queue_capacity: usize,
    /// Maximum encoded bytes per submitted batch, including record headers.
    /// Must be greater than zero. Defaults to 4 MiB.
    pub max_batch_bytes: usize,
    pub retention: RetentionOptions,
}

impl Default for WriteAheadOptions {
    fn default() -> Self {
        Self {
            log_dir: PathBuf::from("./write_ahead"),
            max_file_size: 1024 * 1024 * 1024,                // 1GB
            preallocation_chunk_size: Some(64 * 1024 * 1024), // 64MiB
            queue_capacity: 64,
            max_batch_bytes: 4 * 1024 * 1024,
            retention: RetentionOptions::default(),
        }
    }
}

#[derive(Default, Debug, Clone)]
pub struct RetentionOptions {
    /// The maximum total size of all log files. The oldest sealed files are
    /// deleted first; the active file is never deleted. Set to `0` to disable.
    pub max_total_size: u64,
    /// Delete sealed log files whose seal timestamp is older than this.
    /// Set to `Duration::ZERO` to disable.
    pub ttl: Duration,
}

#[derive(Debug, thiserror::Error)]
pub enum WriteAheadError {
    #[error("Logfile not found")]
    LogfileNotFound,

    #[error("WriteAhead has not been started")]
    NotStarted,

    #[error("WriteAhead has already been started")]
    AlreadyStarted,

    #[error("The log directory already has a writer")]
    DirectoryLocked,

    #[error("Invalid writer queue capacity or maximum batch size")]
    InvalidQueueOptions,

    #[error("Encoded batch exceeds the configured limit of {0} bytes")]
    BatchTooLarge(usize),

    #[error("The writer thread has shut down")]
    WriterClosed,

    #[error("Duplicate log file id {0} in log directory")]
    DuplicateLogfileId(u64),

    #[error("Log file id space exhausted")]
    FileIdExhausted,
}

impl<F: FileIo + 'static> WriteAhead<F> {
    pub fn with_options(options: WriteAheadOptions) -> Self {
        Self {
            options,
            readers: Arc::new(RwLock::new(BTreeMap::new())),
            writer: None,
        }
    }

    /// Starts the write ahead log manager, recovering existing state:
    ///
    /// - Every file is validated (magic number, format version).
    /// - Non-active files that missed their seal (crash mid-rotation) are
    ///   healed: torn tails truncated, then sealed.
    /// - The active (highest-id) file gets its torn tail truncated; if it
    ///   was already sealed, a new active file is created instead.
    /// - Files that don't look like log files (`<digits>.log`) are skipped.
    /// - Startup retention is applied before this returns.
    pub fn start(&mut self) -> Result<()> {
        if self.writer.is_some() {
            return Err(WriteAheadError::AlreadyStarted.into());
        }
        crate::writer::validate_options(&self.options)?;
        std::fs::create_dir_all(&self.options.log_dir).context("Failed to create log directory")?;
        let directory_lock = lock_dir(&self.options.log_dir)?;
        sync_parents(&self.options.log_dir)?;

        let mut found: Vec<(u64, PathBuf)> = Vec::new();
        let entries =
            std::fs::read_dir(&self.options.log_dir).context("Failed to read log directory")?;
        for entry in entries {
            let path = entry.context("Failed to read dir entry")?.path();
            match file_id_from_path(&path) {
                Some(id) => found.push((id, path)),
                None => {
                    warn!("skipping non-logfile in log dir: {}", path.display());
                }
            }
        }
        found.sort_by_key(|(id, _)| *id);
        for pair in found.windows(2) {
            if pair[0].0 == pair[1].0 {
                return Err(anyhow!(WriteAheadError::DuplicateLogfileId(pair[0].0)));
            }
        }

        // Recover every file and build the writer's file registry
        let mut registry: BTreeMap<u64, crate::writer::FileMeta> = BTreeMap::new();
        let (active_id, active_path) = match found.last().cloned() {
            None => {
                debug!("creating initial log file");
                (0, log_file_path(&self.options.log_dir, 0))
            }
            Some((last_id, last_path)) => {
                for (id, path) in found.iter().filter(|(id, _)| *id != last_id) {
                    debug!("loading existing logfile {}", id);
                    let logfile: Logfile<F> = Logfile::open(path)?;
                    let seal_ts = if logfile.sealed {
                        logfile.seal_timestamp_ms
                    } else {
                        // Crash between sealing and creating the next file,
                        // or the seal itself was torn: heal it now.
                        warn!("healing unsealed non-active logfile {}", id);
                        drop(logfile);
                        let ts = now_ms();
                        recover_and_seal::<F>(path, ts)?;
                        Some(ts)
                    };
                    registry.insert(
                        *id,
                        crate::writer::FileMeta {
                            path: path.clone(),
                            size: std::fs::metadata(path)?.len(),
                            seal_timestamp_ms: seal_ts,
                        },
                    );
                }

                let last_len = std::fs::metadata(&last_path)
                    .with_context(|| format!("Failed to stat {}", last_path.display()))?
                    .len();
                if last_len <= FILE_HEADER_SIZE {
                    // Crash between file creation and a durable header, or a
                    // valid empty active file.
                    debug!("recovering empty/header-only active logfile {}", last_id);
                    recover_unsealed::<F>(&last_path)?;
                    (last_id, last_path)
                } else {
                    let logfile: Logfile<F> = Logfile::open(&last_path)?;
                    if logfile.sealed {
                        // Crash after seal but before the next file existed
                        debug!("last logfile {} is sealed, rotating", last_id);
                        registry.insert(
                            last_id,
                            crate::writer::FileMeta {
                                path: last_path.clone(),
                                size: last_len,
                                seal_timestamp_ms: logfile.seal_timestamp_ms,
                            },
                        );
                        let next_id = last_id
                            .checked_add(1)
                            .ok_or(WriteAheadError::FileIdExhausted)?;
                        (next_id, log_file_path(&self.options.log_dir, next_id))
                    } else {
                        drop(logfile);
                        recover_unsealed::<F>(&last_path)?;
                        (last_id, last_path)
                    }
                }
            }
        };

        // Launch the writer: creates/initializes the active file and applies
        // startup retention synchronously before returning.
        let (writer, readers) = WalWriter::<F>::launch(
            self.options.clone(),
            active_id,
            active_path,
            registry,
            directory_lock,
        )?;
        self.readers = readers;
        self.writer = Some(writer);

        Ok(())
    }

    /// A cloneable handle for writing from any task or thread.
    pub fn writer(&self) -> Result<WriteHandle> {
        self.writer
            .clone()
            .ok_or_else(|| WriteAheadError::NotStarted.into())
    }

    /// Writes a batch of records durably (fsync'd before returning) and
    /// returns their addresses. Convenience for `self.writer()?.write_batch()`.
    pub async fn write_batch(&self, data: Vec<Vec<u8>>) -> Result<Vec<RecordID>> {
        self.writer()?.write_batch(data).await
    }

    /// Deletes every sealed log file with id strictly below `file_id` (see
    /// [WriteHandle::trim_before]). Convenience for
    /// `self.writer()?.trim_before(file_id)`.
    pub async fn trim_before(&self, file_id: u64) -> Result<crate::writer::TrimStats> {
        self.writer()?.trim_before(file_id).await
    }

    /// Reads a single record within the last successfully synced boundary.
    pub fn read(&self, logfile_id: u64, offset: u64) -> Result<Vec<u8>> {
        if self.writer.is_none() {
            return Err(WriteAheadError::NotStarted.into());
        }
        let (logfile, end) = self
            .readers
            .read()
            .expect("reader cache poisoned")
            .get(&logfile_id)
            .cloned()
            .ok_or(WriteAheadError::LogfileNotFound)?;
        logfile.read_record_until(offset, end)
    }

    /// Creates a finite snapshot of all currently durable records, oldest
    /// first. Later writes do not extend this stream.
    pub fn create_stream(&self) -> Result<WriteAheadStream<F>> {
        let cache = self.readers.read().expect("reader cache poisoned");
        let first = *cache.keys().next().ok_or(WriteAheadError::NotStarted)?;
        Self::snapshot(&cache, first, FILE_HEADER_SIZE)
    }

    /// Creates a durable snapshot starting inclusively at this record.
    /// Streams retain open handles and remain usable across rotation/trim.
    pub fn create_stream_from(&self, logfile_id: u64, offset: u64) -> Result<WriteAheadStream<F>> {
        let cache = self.readers.read().expect("reader cache poisoned");
        Self::snapshot(&cache, logfile_id, offset)
    }

    fn snapshot(cache: &ReaderMap<F>, logfile_id: u64, offset: u64) -> Result<WriteAheadStream<F>> {
        if !cache.contains_key(&logfile_id) {
            return Err(WriteAheadError::LogfileNotFound.into());
        }
        let streams: Vec<_> = cache
            .range(logfile_id..)
            .map(|(_, (logfile, end))| LogFileStream::new(logfile.snapshot(*end)))
            .collect();
        let mut remaining = streams.into_iter();
        let mut current = remaining.next().expect("checked above");
        current.set_stream_offset(offset);
        Ok(WriteAheadStream {
            current: Some(current),
            remaining,
        })
    }
}

/// Replays the durable records captured at creation, in order. Exhausted
/// file handles are released as the stream advances, including at final EOF.
pub struct WriteAheadStream<F: FileIo> {
    current: Option<LogFileStream<F>>,
    remaining: std::vec::IntoIter<LogFileStream<F>>,
}

impl<F: FileIo> Stream for WriteAheadStream<F> {
    type Item = Result<(RecordID, Vec<u8>)>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Option<Self::Item>> {
        while let Some(current) = self.current.as_mut() {
            match Pin::new(current).poll_next(cx) {
                Poll::Ready(None) => self.current = self.remaining.next(),
                other => return other,
            }
        }
        Poll::Ready(None)
    }
}

impl<F: FileIo> Unpin for WriteAheadStream<F> {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fileio::simple_file::SimpleFile;
    use futures::stream::StreamExt;
    use std::path::Path;

    fn test_wal(dir: &Path) -> WriteAhead<SimpleFile> {
        WriteAhead::with_options(WriteAheadOptions {
            log_dir: dir.to_path_buf(),
            ..Default::default()
        })
    }

    fn test_wal_rotating(dir: &Path, max_file_size: u64) -> WriteAhead<SimpleFile> {
        WriteAhead::with_options(WriteAheadOptions {
            log_dir: dir.to_path_buf(),
            max_file_size: FILE_HEADER_SIZE + max_file_size,
            ..Default::default()
        })
    }

    #[tokio::test]
    async fn test_write_read() {
        let dir = tempfile::tempdir().unwrap();
        let mut wal = test_wal(dir.path());
        wal.start().unwrap();

        let ids = wal
            .write_batch(vec![b"Hello, world!".to_vec()])
            .await
            .unwrap();
        let record = wal.read(ids[0].file_id, ids[0].file_offset).unwrap();
        assert_eq!(record, b"Hello, world!");
    }

    #[tokio::test]
    async fn test_rotation_and_read_back() {
        let dir = tempfile::tempdir().unwrap();
        let mut wal = test_wal_rotating(dir.path(), 128);
        wal.start().unwrap();

        let mut ids = Vec::new();
        for i in 0..10 {
            let batch = wal
                .write_batch(vec![format!("Hello, world! {}", i).into_bytes()])
                .await
                .unwrap();
            ids.extend(batch);
        }
        // Rotation must have happened
        assert!(ids.last().unwrap().file_id > 0);

        for (i, id) in ids.iter().enumerate() {
            let record = wal.read(id.file_id, id.file_offset).unwrap();
            assert_eq!(record, format!("Hello, world! {}", i).into_bytes());
        }
    }

    #[tokio::test]
    async fn test_stream_across_rotations() {
        let dir = tempfile::tempdir().unwrap();
        let mut wal = test_wal_rotating(dir.path(), 128);
        wal.start().unwrap();

        for i in 0..100 {
            wal.write_batch(vec![format!("Hello, world! {}", i).into_bytes()])
                .await
                .unwrap();
        }

        let mut stream = wal.create_stream().unwrap();
        for i in 0..100 {
            let (id, record) = stream.next().await.unwrap().unwrap();
            assert_eq!(record, format!("Hello, world! {}", i).into_bytes());
            assert_eq!(wal.read(id.file_id, id.file_offset).unwrap(), record);
        }
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn test_stream_with_0xff_records() {
        // Regression coverage: raw 0xff bytes must not affect stream offset
        // arithmetic.
        let dir = tempfile::tempdir().unwrap();
        let mut wal = test_wal(dir.path());
        wal.start().unwrap();

        let rec1 = vec![0xff, 0xff, 0xff];
        let rec2 = b"plain".to_vec();
        wal.write_batch(vec![rec1.clone(), rec2.clone()])
            .await
            .unwrap();

        let mut stream = wal.create_stream().unwrap();
        assert_eq!(stream.next().await.unwrap().unwrap().1, rec1);
        assert_eq!(stream.next().await.unwrap().unwrap().1, rec2);
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn test_create_stream_from_offset() {
        let dir = tempfile::tempdir().unwrap();
        let mut wal = test_wal(dir.path());
        wal.start().unwrap();

        let ids = wal
            .write_batch(vec![b"one".to_vec(), b"two".to_vec(), b"three".to_vec()])
            .await
            .unwrap();

        let mut stream = wal
            .create_stream_from(ids[1].file_id, ids[1].file_offset)
            .unwrap();
        let (id, record) = stream.next().await.unwrap().unwrap();
        assert_eq!((id, record.as_slice()), (ids[1], b"two".as_slice()));
        assert_eq!(stream.next().await.unwrap().unwrap().1, b"three");
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn test_write_handle_shared_across_tasks() {
        let dir = tempfile::tempdir().unwrap();
        let mut wal = test_wal(dir.path());
        wal.start().unwrap();
        let wal = std::sync::Arc::new(wal);

        let mut joins = Vec::new();
        for t in 0..4 {
            let handle = wal.writer().unwrap();
            joins.push(tokio::spawn(async move {
                let mut out = Vec::new();
                for i in 0..25 {
                    let payload = format!("task {t} record {i}").into_bytes();
                    let id = handle.write(payload.clone()).await.unwrap();
                    out.push((id, payload));
                }
                out
            }));
        }
        for join in joins {
            for (id, payload) in join.await.unwrap() {
                assert_eq!(wal.read(id.file_id, id.file_offset).unwrap(), payload);
            }
        }
    }

    #[test]
    fn test_write_ahead_stream_is_send() {
        fn assert_send<T: Send>() {}
        assert_send::<WriteAheadStream<SimpleFile>>();
    }

    #[test]
    fn test_write_ahead_is_sync_and_handle_clonable() {
        fn assert_send_sync<T: Send + Sync>() {}
        fn assert_clone<T: Clone>() {}
        assert_send_sync::<WriteAhead<SimpleFile>>();
        assert_send_sync::<WriteHandle>();
        assert_clone::<WriteHandle>();
    }

    #[test]
    fn test_read_before_start_errors() {
        let dir = tempfile::tempdir().unwrap();
        let wal = test_wal(dir.path());
        assert!(wal.read(0, FILE_HEADER_SIZE).is_err());
        assert!(wal.create_stream().is_err());
        assert!(wal.writer().is_err());
    }
}
