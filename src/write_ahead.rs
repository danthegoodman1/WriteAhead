use anyhow::Result;
use std::{collections::BTreeMap, path::PathBuf, time::Duration};
use tracing::{debug, instrument, trace};

use anyhow::Context;

use crate::{
    fileio::FileIO,
    logfile::{file_id_from_path, Logfile},
    record::RecordID,
};

/// Manager controls the log file rotation and modification.
///
/// A manager is single threaded to ensure maximum throughput for disk operations.
/// Async is just a convenience for using with async server frameworks.
#[derive(Debug)]
pub struct WriteAhead<F: FileIO> {
    options: WriteAheadOptions,
    log_files: BTreeMap<u64, Logfile<F>>,
    active_log_file: Option<Logfile<F>>,
}

#[derive(Debug)]
pub struct WriteAheadOptions {
    pub log_dir: PathBuf,
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
    /// The maximum total size of all log files. Set to `0` to disable.
    pub max_total_size: usize,
    /// The maximum age of the log file, determined by the sealed timestamp. Set to `0` to disable.
    pub ttl: Duration,
}

#[derive(Debug, thiserror::Error)]
pub enum WriteAheadError {
    #[error("Logfile not found")]
    LogfileNotFound,

    #[error("Record not found")]
    RecordNotFound,
}

impl<F: FileIO> WriteAhead<F> {
    pub fn with_options(options: WriteAheadOptions) -> Self {
        Self {
            options,
            log_files: BTreeMap::new(),
            active_log_file: None,
        }
    }

    /// Start the write ahead log manager.
    /// If this errors, you must crash.
    #[instrument(skip(self), level = "trace")]
    pub async fn start(&mut self) -> Result<()> {
        std::fs::create_dir_all(&self.options.log_dir)?;
        let log_files =
            std::fs::read_dir(&self.options.log_dir).context("Failed to read log directory")?;

        for log_file in log_files {
            let log_file = log_file.context("Failed to get dir entry")?;
            let path = log_file.path();
            let file_id = file_id_from_path(&path)?;
            debug!("Loading existing logfile: {}", file_id);

            let logfile = Logfile::new(&path).await?;
            self.log_files
                .insert(file_id.parse::<u64>().unwrap(), logfile);
        }

        if self.log_files.is_empty() {
            debug!("Creating initial log file");
            let id_string = padded_u64_string(0);
            let logfile =
                Logfile::new(&self.options.log_dir.join(format!("{}.log", id_string))).await?;
            self.log_files.insert(0, logfile);
            // Take ownership of the last inserted logfile
            self.active_log_file = Some(self.log_files.remove(&0).unwrap());
        } else {
            // Take ownership of the last log file
            let last_key = *self.log_files.last_key_value().unwrap().0;
            let last_logfile = self.log_files.remove(&last_key).unwrap();
            debug!("Setting active log file to last log file: {}", last_key);
            self.active_log_file = Some(last_logfile);
        }

        Ok(())
    }

    #[instrument(skip(self), level = "trace")]
    pub async fn read(&mut self, logfile_id: u64, offset: u64) -> Result<Vec<u8>> {
        // Check active log file first
        if let Some(active_log) = &self.active_log_file {
            if active_log.id.parse::<u64>().unwrap() == logfile_id {
                return active_log.read_record(&offset).await;
            }
        }

        // Lookup log file in stored files
        let logfile = self
            .log_files
            .get(&logfile_id)
            .ok_or(WriteAheadError::LogfileNotFound)?;

        logfile.read_record(&offset).await
    }

    // #[instrument(skip(self, data), level = "trace")]
    // pub async fn write(&mut self, data: &[u8]) -> Result<RecordID> {
    //     // TODO: Maybe we make this a batch write internally with some flush interval, and we return a future that will resolve when the data is flushed to disk
    //     // Then we can queue them up in memory and flush them to disk in a background task

    //     // Write the record to the active log file
    //     let active_log = self.active_log_file.as_mut().unwrap();
    //     let offset = active_log.write_records(&[&data]).await?;
    //     let logfile_id = active_log.id.clone();

    //     // Check if we need to rotate the log file and create the new one
    //     if active_log.file_length() > self.options.max_file_size {
    //         self.rotate_log_file().await?;
    //     }

    //     Ok(RecordID::new(logfile_id.parse::<u64>().unwrap(), offset[0]))
    // }

    #[instrument(skip(self, data), level = "trace")]
    pub async fn write_batch(&mut self, data: &[&[u8]]) -> Result<Vec<RecordID>> {
        // Write the record to the active log file
        let active_log = self.active_log_file.as_mut().unwrap();
        let offset = active_log.write_records(data).await?;
        let logfile_id = active_log.id.clone();

        // Check if we need to rotate the log file and create the new one
        if active_log.file_length() > self.options.max_file_size {
            self.rotate_log_file().await?;
        }

        Ok(offset
            .iter()
            .map(|offset| RecordID::new(logfile_id.parse::<u64>().unwrap(), *offset))
            .collect())
    }

    #[instrument]
    async fn rotate_log_file(&mut self) -> Result<()> {
        let active_log = self.active_log_file.as_mut().unwrap();
        let logfile_id = active_log.id.clone();
        trace!("Rotating log file {}", logfile_id);

        active_log.seal().await?;
        let next_key = logfile_id.parse::<u64>().unwrap() + 1;
        let id_string = padded_u64_string(next_key);

        // Move current active log to the BTreeMap
        let old_log = self.active_log_file.take().unwrap();
        self.log_files
            .insert(old_log.id.parse::<u64>().unwrap(), old_log);

        // Create new active log
        let new_log =
            Logfile::new(&self.options.log_dir.join(format!("{}.log", id_string))).await?;

        self.active_log_file = Some(new_log);
        Ok(())
    }
}

fn padded_u64_string(id: u64) -> String {
    format!("{:010}", id)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    #[cfg(target_os = "linux")]
    use io_uring::IoUring;

    use tokio::sync::Mutex;
    use tracing::Level;
    use tracing_subscriber::{fmt::format::FmtSpan, layer::SubscriberExt, Layer};

    #[cfg(target_os = "linux")]
    use crate::fileio::io_uring::{IOUringFile, GLOBAL_RING};

    use crate::fileio::simple_file::SimpleFile;
    use std::sync::Once;

    use super::*;

    static LOGGER_ONCE: Once = Once::new();
    static URING_ONCE: Once = Once::new();

    const NUM_RECORDS: usize = 1000;
    const BATCH_SIZE: usize = 100;

    fn create_logger() {
        LOGGER_ONCE.call_once(|| {
            let subscriber = tracing_subscriber::registry().with(
                tracing_subscriber::fmt::layer()
                    .compact()
                    .with_file(true)
                    .with_line_number(true)
                    .with_span_events(FmtSpan::CLOSE)
                    .with_target(false)
                    .with_filter(
                        tracing_subscriber::filter::Targets::new().with_default(Level::DEBUG),
                    ),
            );

            tracing::subscriber::set_global_default(subscriber).unwrap();
        });

        #[cfg(target_os = "linux")]
        {
            URING_ONCE.call_once(|| {
                if GLOBAL_RING.get().is_none() {
                    let _ = GLOBAL_RING.set(Arc::new(Mutex::new(
                        IoUring::builder()
                            .setup_sqpoll(2) // 2000ms timeout
                            .build(100)
                            .unwrap(),
                    )));
                }
            });
        }
    }

    #[tokio::test]
    async fn test_write_ahead_create_delete() {
        let _ = std::fs::remove_dir_all("./test_logs/test_write_ahead_create_delete");
        create_logger();
        let mut opts = WriteAheadOptions::default();
        opts.log_dir = PathBuf::from("./test_logs/test_write_ahead_create_delete");

        let mut write_ahead = WriteAhead::<SimpleFile>::with_options(opts);
        write_ahead.start().await.unwrap();

        // Delete the test directory
        std::fs::remove_dir_all("./test_logs/test_write_ahead_create_delete").unwrap();
    }

    #[tokio::test]
    async fn test_write_ahead_write_read() {
        let _ = std::fs::remove_dir_all("./test_logs/test_write_ahead_write_read");
        create_logger();
        let mut opts = WriteAheadOptions::default();
        opts.log_dir = PathBuf::from("./test_logs/test_write_ahead_write_read");

        let mut write_ahead = WriteAhead::<SimpleFile>::with_options(opts);
        write_ahead.start().await.unwrap();

        // Write a record
        let record = write_ahead
            .write_batch(&["Hello, world!".as_bytes()])
            .await
            .unwrap();
        println!("record: {:?}", record);
        // Read the record
        let record = write_ahead
            .read(record[0].file_id, record[0].file_offset)
            .await
            .unwrap();
        assert_eq!(record, "Hello, world!".as_bytes());

        std::fs::remove_dir_all("./test_logs/test_write_ahead_write_read").unwrap();
    }

    #[tokio::test]
    async fn test_write_ahead_rotate_log_file() {
        let _ = std::fs::remove_dir_all("./test_logs/test_write_ahead_rotate_log_file");
        create_logger();
        let mut opts = WriteAheadOptions::default();
        opts.max_file_size = 1024; // 1KB
        opts.log_dir = PathBuf::from("./test_logs/test_write_ahead_rotate_log_file");

        let mut write_ahead = WriteAhead::<SimpleFile>::with_options(opts);
        write_ahead.start().await.unwrap();

        let mut records = Vec::new();

        // Write 1000 records
        for i in 0..200 {
            let record = write_ahead
                .write_batch(&[format!("Hello, world! {}", i).as_bytes()])
                .await
                .unwrap();
            records.push(record);
        }

        // Read back the records
        for i in 0..200 {
            let record = write_ahead
                .read(records[i][0].file_id, records[i][0].file_offset)
                .await
                .unwrap();
            assert_eq!(record, format!("Hello, world! {}", i).as_bytes());
        }

        std::fs::remove_dir_all("./test_logs/test_write_ahead_rotate_log_file").unwrap();
    }

    #[tokio::test]
    async fn test_write_ahead_large_data_simple_sequential() {
        let _ = std::fs::remove_dir_all("./test_logs/test_write_ahead_large_data_simple");
        create_logger();
        let mut opts = WriteAheadOptions::default();
        opts.log_dir = PathBuf::from("./test_logs/test_write_ahead_large_data_simple");

        let mut write_ahead = WriteAhead::<SimpleFile>::with_options(opts);
        write_ahead.start().await.unwrap();

        let mut records = Vec::new();

        let start = std::time::Instant::now();
        for i in 0..NUM_RECORDS {
            let record = write_ahead
                .write_batch(&[format!("Hello, world! {}", i).as_bytes()])
                .await
                .unwrap();
            records.push(record);
        }
        let end = std::time::Instant::now();
        debug!("Write time taken: {:?}", end.duration_since(start));

        // Read back the records
        let start = std::time::Instant::now();
        for i in 0..NUM_RECORDS {
            let record = write_ahead
                .read(records[i][0].file_id, records[i][0].file_offset)
                .await
                .unwrap();
            assert_eq!(record, format!("Hello, world! {}", i).as_bytes());
        }
        let end = std::time::Instant::now();
        debug!("Read time taken: {:?}", end.duration_since(start));

        std::fs::remove_dir_all("./test_logs/test_write_ahead_large_data_simple").unwrap();
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn test_write_ahead_large_data_uring_sequential() {
        let _ = std::fs::remove_dir_all("./test_logs/test_write_ahead_large_data_uring");
        create_logger();
        let mut opts = WriteAheadOptions::default();
        opts.log_dir = PathBuf::from("./test_logs/test_write_ahead_large_data_uring");

        let mut write_ahead = WriteAhead::<IOUringFile>::with_options(opts);
        write_ahead.start().await.unwrap();

        let mut records = Vec::new();

        let start = std::time::Instant::now();
        for i in 0..NUM_RECORDS {
            let record = write_ahead
                .write_batch(&[format!("Hello, world! {}", i).as_bytes()])
                .await
                .unwrap();
            records.push(record);
        }
        let end = std::time::Instant::now();
        debug!("Write time taken: {:?}", end.duration_since(start));

        // Read back the records
        let start = std::time::Instant::now();
        for i in 0..NUM_RECORDS {
            let record = write_ahead
                .read(records[i][0].file_id, records[i][0].file_offset)
                .await
                .unwrap();
            assert_eq!(record, format!("Hello, world! {}", i).as_bytes());
        }
        let end = std::time::Instant::now();
        debug!("Read time taken: {:?}", end.duration_since(start));

        std::fs::remove_dir_all("./test_logs/test_write_ahead_large_data_uring").unwrap();
    }

    #[tokio::test]
    async fn test_write_ahead_large_data_simple_batch() {
        let _ = std::fs::remove_dir_all("./test_logs/test_write_ahead_large_data_simple_batch");
        create_logger();
        let mut opts = WriteAheadOptions::default();
        opts.log_dir = PathBuf::from("./test_logs/test_write_ahead_large_data_simple_batch");

        let mut write_ahead = WriteAhead::<SimpleFile>::with_options(opts);
        write_ahead.start().await.unwrap();

        // Pre-build all the data
        let data: Vec<Vec<u8>> = (0..NUM_RECORDS)
            .map(|i| format!("Hello, world! {}", i).as_bytes().to_vec())
            .collect();

        let mut records = Vec::new();
        let start = std::time::Instant::now();

        // Process in chunks
        for chunk in data.chunks(BATCH_SIZE) {
            let batch_refs: Vec<&[u8]> = chunk.iter().map(|b| b.as_ref()).collect();
            let batch_records = write_ahead.write_batch(&batch_refs).await.unwrap();
            records.extend(batch_records);
        }

        let end = std::time::Instant::now();
        debug!("Batch write time taken: {:?}", end.duration_since(start));

        // Read back the records
        let start = std::time::Instant::now();
        for i in 0..NUM_RECORDS {
            let record = write_ahead
                .read(records[i].file_id, records[i].file_offset)
                .await
                .unwrap();
            assert_eq!(record, format!("Hello, world! {}", i).as_bytes());
        }
        let end = std::time::Instant::now();
        debug!("Read time taken: {:?}", end.duration_since(start));

        std::fs::remove_dir_all("./test_logs/test_write_ahead_large_data_simple_batch").unwrap();
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn test_write_ahead_large_data_uring_batch() {
        let _ = std::fs::remove_dir_all("./test_logs/test_write_ahead_large_data_uring_batch");
        create_logger();
        let mut opts = WriteAheadOptions::default();
        opts.log_dir = PathBuf::from("./test_logs/test_write_ahead_large_data_uring_batch");

        let mut write_ahead = WriteAhead::<IOUringFile>::with_options(opts);
        write_ahead.start().await.unwrap();

        // Pre-build all the data
        let data: Vec<Vec<u8>> = (0..NUM_RECORDS)
            .map(|i| format!("Hello, world! {}", i).as_bytes().to_vec())
            .collect();

        let mut records = Vec::new();
        let start = std::time::Instant::now();

        // Process in chunks
        for chunk in data.chunks(BATCH_SIZE) {
            let batch_refs: Vec<&[u8]> = chunk.iter().map(|b| b.as_ref()).collect();
            let batch_records = write_ahead.write_batch(&batch_refs).await.unwrap();
            records.extend(batch_records);
        }

        let end = std::time::Instant::now();
        debug!("Batch write time taken: {:?}", end.duration_since(start));

        // Read back the records
        let start = std::time::Instant::now();
        for i in 0..NUM_RECORDS {
            let record = write_ahead
                .read(records[i].file_id, records[i].file_offset)
                .await
                .unwrap();
            assert_eq!(record, format!("Hello, world! {}", i).as_bytes());
        }
        let end = std::time::Instant::now();
        debug!("Read time taken: {:?}", end.duration_since(start));

        std::fs::remove_dir_all("./test_logs/test_write_ahead_large_data_uring_batch").unwrap();
    }
}
