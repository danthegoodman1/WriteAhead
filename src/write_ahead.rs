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
    fileio::{simple_file::SimpleFile, FileIo},
    logfile::{
        file_id_from_path, log_file_path, now_ms, recover_and_seal, recover_unsealed,
        LogFileStream, Logfile, FILE_HEADER_SIZE,
    },
    record::RecordID,
    writer::{WalWriter, WriteHandle, WriterEvent},
};

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
    /// Reader cache, kept in sync with the writer via events and lazy opens.
    readers: RwLock<BTreeMap<u64, Arc<Logfile<F>>>>,
    writer_tx: Option<flume::Sender<crate::writer::WriterCommand>>,
    writer_events: Option<flume::Receiver<WriterEvent>>,
}

#[derive(Debug, Clone)]
pub struct WriteAheadOptions {
    pub log_dir: PathBuf,
    /// Rotate the active log file after its length exceeds this.
    pub max_file_size: u64,
    pub retention: RetentionOptions,
}

impl Default for WriteAheadOptions {
    fn default() -> Self {
        Self {
            log_dir: PathBuf::from("./write_ahead"),
            max_file_size: 1024 * 1024 * 1024, // 1GB
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

    #[error("The writer thread has shut down")]
    WriterClosed,

    #[error("Duplicate log file id {0} in log directory")]
    DuplicateLogfileId(u64),
}

impl<F: FileIo + 'static> WriteAhead<F> {
    pub fn with_options(options: WriteAheadOptions) -> Self {
        Self {
            options,
            readers: RwLock::new(BTreeMap::new()),
            writer_tx: None,
            writer_events: None,
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
        std::fs::create_dir_all(&self.options.log_dir).context("Failed to create log directory")?;

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
                if last_len < FILE_HEADER_SIZE {
                    // Crash between file creation and header write
                    debug!("recovering empty/partial active logfile {}", last_id);
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
                        (
                            last_id + 1,
                            log_file_path(&self.options.log_dir, last_id + 1),
                        )
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
        let (tx, events) =
            WalWriter::<F>::launch(self.options.clone(), active_id, active_path, registry)?;
        self.writer_tx = Some(tx);
        self.writer_events = Some(events);

        // Populate the reader cache from what survived recovery + retention
        let mut cache = BTreeMap::new();
        for entry in std::fs::read_dir(&self.options.log_dir)? {
            let path = entry?.path();
            if file_id_from_path(&path).is_some() {
                let logfile: Logfile<F> = Logfile::open(&path)?;
                cache.insert(logfile.id, Arc::new(logfile));
            }
        }
        *self.readers.write().expect("reader cache poisoned") = cache;

        Ok(())
    }

    /// A cloneable handle for writing from any task or thread.
    pub fn writer(&self) -> Result<WriteHandle> {
        Ok(WriteHandle {
            tx: self.writer_tx.clone().ok_or(WriteAheadError::NotStarted)?,
        })
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

    /// Applies pending writer lifecycle events to the reader cache.
    fn drain_events(&self) {
        let Some(events) = &self.writer_events else {
            return;
        };
        if events.is_empty() {
            return;
        }
        let mut cache = self.readers.write().expect("reader cache poisoned");
        while let Ok(event) = events.try_recv() {
            match event {
                WriterEvent::Created(id, path) => match Logfile::open(&path) {
                    Ok(logfile) => {
                        cache.insert(id, Arc::new(logfile));
                    }
                    Err(e) => warn!("failed to open new logfile {}: {e:#}", id),
                },
                WriterEvent::Sealed(id) => {
                    // Reopen so the reader caches the sealed record region
                    if let Some(existing) = cache.get(&id) {
                        match Logfile::open(existing.path()) {
                            Ok(logfile) => {
                                cache.insert(id, Arc::new(logfile));
                            }
                            Err(e) => warn!("failed to reopen sealed logfile {}: {e:#}", id),
                        }
                    }
                }
                WriterEvent::Deleted(id) => {
                    cache.remove(&id);
                }
            }
        }
    }

    fn reader(&self, logfile_id: u64) -> Result<Arc<Logfile<F>>> {
        self.drain_events();
        if let Some(logfile) = self
            .readers
            .read()
            .expect("reader cache poisoned")
            .get(&logfile_id)
        {
            return Ok(Arc::clone(logfile));
        }
        // Miss: the file may have been created since our last event drain
        let path = log_file_path(&self.options.log_dir, logfile_id);
        let logfile: Logfile<F> =
            Logfile::open(&path).map_err(|_| WriteAheadError::LogfileNotFound)?;
        let logfile = Arc::new(logfile);
        self.readers
            .write()
            .expect("reader cache poisoned")
            .insert(logfile_id, Arc::clone(&logfile));
        Ok(logfile)
    }

    /// Reads a single record by its location.
    pub fn read(&self, logfile_id: u64, offset: u64) -> Result<Vec<u8>> {
        self.reader(logfile_id)?.read_record(offset)
    }

    /// Creates a stream over all records in all log files, oldest first.
    /// Yields each record with its [RecordID], so consumers can checkpoint
    /// their position and resume via [Self::create_stream_from].
    pub fn create_stream(&self) -> Result<WriteAheadStream<F>> {
        self.drain_events();
        let first = *self
            .readers
            .read()
            .expect("reader cache poisoned")
            .keys()
            .next()
            .ok_or(WriteAheadError::NotStarted)?;
        self.create_stream_from(first, FILE_HEADER_SIZE)
    }

    /// Creates a stream starting at a record offset in a specific log file.
    /// The stream gets its own file handles, so it can be sent to another
    /// thread and consumed while writes continue.
    pub fn create_stream_from(&self, logfile_id: u64, offset: u64) -> Result<WriteAheadStream<F>> {
        self.drain_events();
        let paths: Vec<(u64, PathBuf)> = {
            let cache = self.readers.read().expect("reader cache poisoned");
            if !cache.contains_key(&logfile_id) {
                return Err(anyhow!(WriteAheadError::LogfileNotFound));
            }
            cache
                .range(logfile_id..)
                .map(|(id, logfile)| (*id, logfile.path().to_path_buf()))
                .collect()
        };

        let mut streams = BTreeMap::new();
        for (id, path) in paths {
            streams.insert(id, LogFileStream::new(Logfile::open(&path)?));
        }
        let mut current = streams.remove(&logfile_id).expect("checked above");
        current.set_stream_offset(offset);
        Ok(WriteAheadStream {
            logfiles: streams,
            active_log_id: logfile_id,
            current_stream: current,
        })
    }
}

/// Streams records across log files in order. Ends after the last record of
/// the last log file that existed when the stream was created.
pub struct WriteAheadStream<F: FileIo> {
    logfiles: BTreeMap<u64, LogFileStream<F>>,
    active_log_id: u64,
    current_stream: LogFileStream<F>,
}

impl<F: FileIo> Stream for WriteAheadStream<F> {
    type Item = Result<(RecordID, Vec<u8>)>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match Pin::new(&mut self.current_stream).poll_next(cx) {
                Poll::Ready(Some(result)) => return Poll::Ready(Some(result)),
                Poll::Ready(None) => {
                    // Current file exhausted, move to the next one
                    let next_id = self
                        .logfiles
                        .range((self.active_log_id + 1)..)
                        .next()
                        .map(|(k, _)| *k);
                    match next_id {
                        Some(id) => {
                            self.active_log_id = id;
                            self.current_stream =
                                self.logfiles.remove(&id).expect("key from range");
                        }
                        None => return Poll::Ready(None),
                    }
                }
                Poll::Pending => return Poll::Pending,
            }
        }
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
            max_file_size,
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
        // Regression: records full of 0xff bytes used to break stream offset
        // arithmetic under the v1 escaping scheme.
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
