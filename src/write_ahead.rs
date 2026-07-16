use anyhow::{anyhow, Context, Result};
use futures::Stream;
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    pin::Pin,
    task::{Context as TaskContext, Poll},
    time::Duration,
};
use tracing::{debug, warn};

use crate::{
    fileio::{simple_file::SimpleFile, FileIo},
    logfile::{
        file_id_from_path, now_ms, recover_and_seal, recover_unsealed, LogFileStream,
        LogFileWriter, Logfile, WriterCommand, FILE_HEADER_SIZE,
    },
    record::RecordID,
};

/// Manager that owns log file rotation, recovery, and retention.
///
/// A single owner drives all writes (one writer thread per active file, so
/// disk writes never contend); `write_batch` is async only so it can await
/// the writer's fsync without blocking the executor. Reads go straight to
/// the page cache via positional reads and are synchronous.
#[derive(Debug)]
pub struct WriteAhead<F: FileIo = SimpleFile> {
    options: WriteAheadOptions,
    log_files: BTreeMap<u64, Logfile<F>>,
    active_writer: Option<flume::Sender<WriterCommand>>,
    active_log_id: u64,
}

#[derive(Debug)]
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

#[derive(Default, Debug)]
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

fn padded_u64_string(id: u64) -> String {
    format!("{:010}", id)
}

fn sync_dir(dir: &Path) -> Result<()> {
    std::fs::File::open(dir)
        .and_then(|f| f.sync_all())
        .with_context(|| format!("Failed to fsync directory {}", dir.display()))?;
    Ok(())
}

impl<F: FileIo + 'static> WriteAhead<F> {
    pub fn with_options(options: WriteAheadOptions) -> Self {
        Self {
            options,
            log_files: BTreeMap::new(),
            active_writer: None,
            active_log_id: 0,
        }
    }

    fn path_for(&self, id: u64) -> PathBuf {
        self.options
            .log_dir
            .join(format!("{}.log", padded_u64_string(id)))
    }

    /// Starts the write ahead log manager, recovering existing state:
    ///
    /// - Every file is validated (magic number, format version).
    /// - Non-active files that missed their seal (crash mid-rotation) are
    ///   healed: torn tails truncated, then sealed.
    /// - The active (highest-id) file gets its torn tail truncated; if it
    ///   was already sealed, a new active file is created instead.
    /// - Files that don't look like log files (`<digits>.log`) are skipped.
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

        match found.last().cloned() {
            None => {
                debug!("creating initial log file");
                self.create_active(0)?;
            }
            Some((last_id, last_path)) => {
                for (id, path) in found.iter().filter(|(id, _)| *id != last_id) {
                    debug!("loading existing logfile {}", id);
                    let logfile = Logfile::open(path)?;
                    let logfile = if logfile.sealed {
                        logfile
                    } else {
                        // Crash happened between sealing and creating the next
                        // file, or the seal itself was torn: heal it now.
                        warn!("healing unsealed non-active logfile {}", id);
                        drop(logfile);
                        recover_and_seal::<F>(path, now_ms())?;
                        Logfile::open(path)?
                    };
                    self.log_files.insert(*id, logfile);
                }

                let last_len = std::fs::metadata(&last_path)
                    .with_context(|| format!("Failed to stat {}", last_path.display()))?
                    .len();
                if last_len < FILE_HEADER_SIZE {
                    // Crash between file creation and header write
                    debug!("recovering empty/partial active logfile {}", last_id);
                    recover_unsealed::<F>(&last_path)?;
                    self.create_active_at(last_id, &last_path)?;
                } else {
                    let logfile = Logfile::open(&last_path)?;
                    if logfile.sealed {
                        // Crash after seal but before the next file was created
                        debug!("last logfile {} is sealed, rotating", last_id);
                        self.log_files.insert(last_id, logfile);
                        self.create_active(last_id + 1)?;
                    } else {
                        drop(logfile);
                        recover_unsealed::<F>(&last_path)?;
                        self.create_active_at(last_id, &last_path)?;
                    }
                }
            }
        }

        self.apply_retention()?;
        Ok(())
    }

    fn create_active(&mut self, id: u64) -> Result<()> {
        let path = self.path_for(id);
        self.create_active_at(id, &path)
    }

    fn create_active_at(&mut self, id: u64, path: &Path) -> Result<()> {
        let writer = LogFileWriter::<F>::launch(path)?;
        // Make sure a newly created file's directory entry survives a crash
        sync_dir(&self.options.log_dir)?;
        let reader = Logfile::open(path)?;
        self.log_files.insert(id, reader);
        self.active_writer = Some(writer);
        self.active_log_id = id;
        Ok(())
    }

    /// Reads a single record by its location.
    pub fn read(&self, logfile_id: u64, offset: u64) -> Result<Vec<u8>> {
        let logfile = self
            .log_files
            .get(&logfile_id)
            .ok_or(WriteAheadError::LogfileNotFound)?;
        logfile.read_record(offset)
    }

    /// Writes a batch of records durably (fsync'd before returning) and
    /// returns their addresses. Rotates the log file afterwards if it
    /// exceeded `max_file_size`.
    pub async fn write_batch(&mut self, data: Vec<Vec<u8>>) -> Result<Vec<RecordID>> {
        let writer = self
            .active_writer
            .as_ref()
            .ok_or(WriteAheadError::NotStarted)?;
        let (tx, rx) = flume::bounded(1);
        writer
            .send(WriterCommand::Write(tx, data))
            .map_err(|_| WriteAheadError::WriterClosed)?;
        let response = rx
            .recv_async()
            .await
            .map_err(|_| WriteAheadError::WriterClosed)??;

        let ids = response
            .offsets
            .iter()
            .map(|offset| RecordID::new(self.active_log_id, *offset))
            .collect();

        if response.file_length > self.options.max_file_size {
            self.rotate_log_file().await?;
        }

        Ok(ids)
    }

    async fn rotate_log_file(&mut self) -> Result<()> {
        let writer = self
            .active_writer
            .as_ref()
            .ok_or(WriteAheadError::NotStarted)?;
        let old_id = self.active_log_id;
        debug!("rotating log file {}", old_id);

        let (tx, rx) = flume::bounded(1);
        writer
            .send(WriterCommand::Seal(tx))
            .map_err(|_| WriteAheadError::WriterClosed)?;
        rx.recv_async()
            .await
            .map_err(|_| WriteAheadError::WriterClosed)??;

        // Refresh the reader so it knows the file is sealed
        if let Some(old) = self.log_files.get(&old_id) {
            let path = old.path().to_path_buf();
            self.log_files.insert(old_id, Logfile::open(&path)?);
        }

        self.create_active(old_id + 1)?;
        self.apply_retention()?;
        Ok(())
    }

    /// Deletes sealed files per `RetentionOptions`. The active file is never
    /// deleted. Reads into deleted files return `LogfileNotFound`.
    fn apply_retention(&mut self) -> Result<()> {
        let mut to_delete: Vec<u64> = Vec::new();

        let ttl_ms = self.options.retention.ttl.as_millis() as u64;
        if ttl_ms > 0 {
            let cutoff = now_ms().saturating_sub(ttl_ms);
            for (id, logfile) in &self.log_files {
                if *id != self.active_log_id
                    && logfile.sealed
                    && logfile.seal_timestamp_ms.is_some_and(|ts| ts < cutoff)
                {
                    to_delete.push(*id);
                }
            }
        }

        let max_total = self.options.retention.max_total_size;
        if max_total > 0 {
            let mut sizes: BTreeMap<u64, u64> = BTreeMap::new();
            for (id, logfile) in &self.log_files {
                sizes.insert(*id, logfile.file_len()?);
            }
            let mut total: u64 = sizes.values().sum();
            for (id, size) in &sizes {
                if total <= max_total || *id == self.active_log_id || to_delete.contains(id) {
                    continue;
                }
                to_delete.push(*id);
                total -= size;
            }
        }

        if to_delete.is_empty() {
            return Ok(());
        }
        for id in &to_delete {
            if let Some(logfile) = self.log_files.remove(id) {
                debug!("retention: deleting logfile {}", id);
                std::fs::remove_file(logfile.path())
                    .with_context(|| format!("Failed to delete logfile {}", id))?;
            }
        }
        sync_dir(&self.options.log_dir)?;
        Ok(())
    }

    /// Creates a stream over all records in all log files, oldest first.
    pub fn create_stream(&self) -> Result<WriteAheadStream<F>> {
        let first = *self
            .log_files
            .keys()
            .next()
            .ok_or(WriteAheadError::NotStarted)?;
        self.create_stream_from(first, FILE_HEADER_SIZE)
    }

    /// Creates a stream starting at a record offset in a specific log file.
    /// The stream gets its own file handles, so it can be sent to another
    /// thread and consumed while writes continue.
    pub fn create_stream_from(&self, logfile_id: u64, offset: u64) -> Result<WriteAheadStream<F>> {
        if !self.log_files.contains_key(&logfile_id) {
            return Err(anyhow!(WriteAheadError::LogfileNotFound));
        }

        let mut streams = BTreeMap::new();
        for (id, logfile) in self.log_files.range(logfile_id..) {
            streams.insert(*id, LogFileStream::new(Logfile::open(logfile.path())?));
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
    type Item = Result<Vec<u8>>;

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
            let record = stream.next().await.unwrap().unwrap();
            assert_eq!(record, format!("Hello, world! {}", i).into_bytes());
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
        assert_eq!(stream.next().await.unwrap().unwrap(), rec1);
        assert_eq!(stream.next().await.unwrap().unwrap(), rec2);
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
        assert_eq!(stream.next().await.unwrap().unwrap(), b"two");
        assert_eq!(stream.next().await.unwrap().unwrap(), b"three");
        assert!(stream.next().await.is_none());
    }

    #[test]
    fn test_write_ahead_stream_is_send() {
        fn assert_send<T: Send>() {}
        assert_send::<WriteAheadStream<SimpleFile>>();
    }

    #[test]
    fn test_read_before_start_errors() {
        let dir = tempfile::tempdir().unwrap();
        let wal = test_wal(dir.path());
        assert!(wal.read(0, FILE_HEADER_SIZE).is_err());
        assert!(wal.create_stream().is_err());
    }
}
