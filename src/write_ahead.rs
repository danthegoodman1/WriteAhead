use anyhow::Result;
use futures::Stream;
use std::{collections::BTreeMap, marker::PhantomData, path::PathBuf, pin::Pin, time::Duration};
use tracing::{debug, instrument, trace};

use anyhow::Context;

use crate::{
    fileio::{FileReader, FileWriter},
    logfile::{file_id_from_path, LogFileWriter, Logfile, WriterCommand},
    record::RecordID,
};

/// Manager controls the log file rotation and modification.
///
/// A manager is single threaded to ensure maximum throughput for disk operations.
/// Async is just a convenience for using with async server frameworks.
#[derive(Debug)]
pub struct WriteAhead<WriteF: FileWriter, ReadF: FileReader> {
    options: WriteAheadOptions,
    log_files: BTreeMap<u64, Logfile<ReadF>>, // TODO: make this a reader connection pool?
    active_log_file: Option<flume::Sender<WriterCommand>>,
    active_log_id: Option<u64>,
    _phantom: PhantomData<WriteF>,
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

impl<WriteF: FileWriter + 'static, ReadF: FileReader + 'static> WriteAhead<WriteF, ReadF> {
    pub fn with_options(options: WriteAheadOptions) -> Self {
        Self {
            options,
            log_files: BTreeMap::new(),
            active_log_file: None,
            active_log_id: None,
            _phantom: PhantomData,
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
            let path = self.options.log_dir.join(format!("{}.log", id_string));
            let logfile = Logfile::new(&path).await?;
            self.log_files.insert(0, logfile);

            // Create a writer for the new log file
            self.active_log_file = Some(LogFileWriter::<WriteF>::launch(&path).await.unwrap());
            self.active_log_id = Some(0);
        } else {
            // Create a writer for the last log file
            let last_key = *self.log_files.last_key_value().unwrap().0;
            let id_string = padded_u64_string(last_key);
            let path = self.options.log_dir.join(format!("{}.log", id_string));
            self.active_log_file = Some(LogFileWriter::<WriteF>::launch(&path).await.unwrap());
            self.active_log_id = Some(last_key);
        }

        Ok(())
    }

    #[instrument(skip(self), level = "trace")]
    pub async fn read(&mut self, logfile_id: u64, offset: u64) -> Result<Vec<u8>> {
        // Lookup log file in stored files
        let logfile = self
            .log_files
            .get(&logfile_id)
            .ok_or(WriteAheadError::LogfileNotFound)?;

        logfile.read_record(&offset).await
    }

    #[instrument(skip(self, data), level = "trace")]
    pub async fn write_batch(&mut self, data: Vec<Vec<u8>>) -> Result<Vec<RecordID>> {
        // Write the record to the active log file
        let active_log = self.active_log_file.as_mut().unwrap();
        let (tx, rx) = flume::unbounded();
        active_log.send(WriterCommand::Write(tx, data)).unwrap();
        let res = rx.recv().unwrap().unwrap();

        let result = res
            .offsets
            .iter()
            .map(|offset| RecordID::new(self.active_log_id.unwrap(), *offset))
            .collect();

        // Check if we need to rotate the log file and create the new one
        if res.file_length > self.options.max_file_size {
            self.rotate_log_file().await?;
        }

        Ok(result)
    }

    #[instrument(skip(self), level = "trace")]
    async fn rotate_log_file(&mut self) -> Result<()> {
        let active_log = self.active_log_file.as_mut().unwrap();
        let logfile_id = self.active_log_id.unwrap();
        trace!("Rotating log file {}", logfile_id);

        // Seal the log file
        let (tx, rx) = flume::unbounded();
        active_log.send(WriterCommand::Seal(tx)).unwrap();
        rx.recv().unwrap().unwrap();

        let next_key = logfile_id + 1;
        let id_string = padded_u64_string(next_key);

        let path = self.options.log_dir.join(format!("{}.log", id_string));

        // Create a new reader first for the new log file
        let logfile = Logfile::new(&path).await?;
        self.log_files.insert(next_key, logfile);

        // Create new active log
        self.active_log_file = Some(LogFileWriter::<WriteF>::launch(&path).await.unwrap());
        self.active_log_id = Some(next_key);

        Ok(())
    }

    // /// Creates a stream that will read all records from all log files sequentially
    // pub async fn create_stream(&self) -> WriteAheadStream<ReadF> {
    //     return self.create_stream_from(0, 8).await.unwrap();
    // }

    // /// Creates a stream starting from a specific position
    // pub async fn create_stream_from(
    //     &self,
    //     logfile_id: u64,
    //     offset: u64,
    // ) -> Option<WriteAheadStream<ReadF>> {
    //     // Create a clone of the log files, starting from logfile_id
    //     let keys: Vec<u64> = self
    //         .log_files
    //         .range(logfile_id..) // Get iterator starting from logfile_id
    //         .map(|(k, _)| *k)
    //         .collect();

    //     let mut tree_clone = BTreeMap::new();
    //     for key in keys {
    //         tree_clone.insert(
    //             key,
    //             LogFileStream::new(
    //                 Logfile::from_file(
    //                     &self
    //                         .options
    //                         .log_dir
    //                         .join(format!("{}.log", padded_u64_string(key))),
    //                 )
    //                 .await
    //                 .unwrap(),
    //             ),
    //         );
    //     }
    //     WriteAheadStream::from_position(tree_clone, logfile_id, offset)
    // }
}

fn padded_u64_string(id: u64) -> String {
    format!("{:010}", id)
}

// pub struct WriteAheadStream<F: FileReader> {
//     logfiles: BTreeMap<u64, LogFileStream<F>>,
//     active_log_id: u64,
//     current_stream: LogFileStream<F>,
// }

// impl<F: FileReader> WriteAheadStream<F> {
//     /// Creates a new WriteAheadStream starting from the beginning of all log files
//     pub fn new(mut logfiles: BTreeMap<u64, LogFileStream<F>>) -> Self {
//         let active_log_id = *logfiles.keys().next().unwrap_or(&0);
//         let current_stream = logfiles.remove(&active_log_id).unwrap();

//         Self {
//             logfiles,
//             active_log_id,
//             current_stream,
//         }
//     }

//     /// Creates a new WriteAheadStream starting from a specific log file and offset
//     pub fn from_position(
//         mut logfiles: BTreeMap<u64, LogFileStream<F>>,
//         logfile_id: u64,
//         offset: u64,
//     ) -> Option<Self> {
//         if !logfiles.contains_key(&logfile_id) {
//             return None;
//         }

//         let mut stream = logfiles.remove(&logfile_id).unwrap();
//         stream.set_stream_offset(offset.max(8)); // Ensure we start after magic number

//         Some(Self {
//             logfiles,
//             active_log_id: logfile_id,
//             current_stream: stream,
//         })
//     }
// }

// use std::task::{Context as TaskContext, Poll};

// impl<F: FileReader> Stream for WriteAheadStream<F> {
//     type Item = Result<Vec<u8>>;

//     fn poll_next(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Option<Self::Item>> {
//         loop {
//             // Try to read from current stream
//             match Pin::new(&mut self.current_stream).poll_next(cx) {
//                 Poll::Ready(Some(result)) => {
//                     return Poll::Ready(Some(result));
//                 }
//                 Poll::Ready(None) => {
//                     // Current stream is exhausted, try to move to next logfile
//                     let next_id = self
//                         .logfiles
//                         .range((self.active_log_id + 1)..)
//                         .next()
//                         .map(|(k, _)| *k);
//                     match next_id {
//                         Some(id) => {
//                             // Create new stream for next logfile
//                             self.active_log_id = id;
//                             self.current_stream = self.logfiles.remove(&id).unwrap();
//                             continue; // Try reading from new stream
//                         }
//                         None => return Poll::Ready(None), // No more logfiles
//                     }
//                 }
//                 Poll::Pending => return Poll::Pending,
//             }
//         }
//     }
// }

// impl<F: FileReader> Unpin for WriteAheadStream<F> {}

#[cfg(test)]
mod tests {

    #[cfg(target_os = "linux")]
    use tracing::Level;
    use tracing_subscriber::{fmt::format::FmtSpan, layer::SubscriberExt, Layer};

    #[cfg(target_os = "linux")]
    use crate::fileio::io_uring::IOUringFile;

    use crate::fileio::simple_file::SimpleFile;
    use std::sync::Once;

    use futures::stream::StreamExt;

    use super::*;

    static LOGGER_ONCE: Once = Once::new();

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
    }

    #[tokio::test]
    async fn test_write_ahead_create_delete() {
        let _ = std::fs::remove_dir_all("./test_logs/test_write_ahead_create_delete");
        create_logger();
        let mut opts = WriteAheadOptions::default();
        opts.log_dir = PathBuf::from("./test_logs/test_write_ahead_create_delete");

        let mut write_ahead = WriteAhead::<SimpleFile, SimpleFile>::with_options(opts);
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

        let mut write_ahead = WriteAhead::<SimpleFile, SimpleFile>::with_options(opts);
        write_ahead.start().await.unwrap();

        // Write a record
        let record = write_ahead
            .write_batch(vec!["Hello, world!".as_bytes().to_vec()])
            .await
            .unwrap();
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
        opts.max_file_size = 128; // 1KB
        opts.log_dir = PathBuf::from("./test_logs/test_write_ahead_rotate_log_file");

        let mut write_ahead = WriteAhead::<SimpleFile, SimpleFile>::with_options(opts);
        write_ahead.start().await.unwrap();

        let mut records = Vec::new();

        // Write 10 records
        for i in 0..10 {
            let record = write_ahead
                .write_batch(vec![format!("Hello, world! {}", i).as_bytes().to_vec()])
                .await
                .unwrap();
            records.push(record);
        }

        // Read back the records
        for i in 0..10 {
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

        let mut write_ahead = WriteAhead::<SimpleFile, SimpleFile>::with_options(opts);
        write_ahead.start().await.unwrap();

        let mut records = Vec::new();

        let start = std::time::Instant::now();
        for i in 0..NUM_RECORDS {
            let record = write_ahead
                .write_batch(vec![format!("Hello, world! {}", i).as_bytes().to_vec()])
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
    async fn test_write_ahead_large_data_mixed_sequential() {
        let _ = std::fs::remove_dir_all("./test_logs/test_write_ahead_large_data_mixed");
        create_logger();
        let mut opts = WriteAheadOptions::default();
        opts.log_dir = PathBuf::from("./test_logs/test_write_ahead_large_data_mixed");

        let mut write_ahead = WriteAhead::<SimpleFile, IOUringFile>::with_options(opts);
        write_ahead.start().await.unwrap();

        let mut records = Vec::new();

        let start = std::time::Instant::now();
        for i in 0..NUM_RECORDS {
            let record = write_ahead
                .write_batch(vec![format!("Hello, world! {}", i).as_bytes().to_vec()])
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

        std::fs::remove_dir_all("./test_logs/test_write_ahead_large_data_mixed").unwrap();
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn test_write_ahead_large_data_uring_sequential() {
        let _ = std::fs::remove_dir_all("./test_logs/test_write_ahead_large_data_uring");
        create_logger();
        let mut opts = WriteAheadOptions::default();
        opts.log_dir = PathBuf::from("./test_logs/test_write_ahead_large_data_uring");

        let mut write_ahead = WriteAhead::<IOUringFile, IOUringFile>::with_options(opts);
        write_ahead.start().await.unwrap();

        let mut records = Vec::new();

        let start = std::time::Instant::now();
        for i in 0..NUM_RECORDS {
            let record = write_ahead
                .write_batch(vec![format!("Hello, world! {}", i).as_bytes().to_vec()])
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

        let mut write_ahead = WriteAhead::<SimpleFile, SimpleFile>::with_options(opts);
        write_ahead.start().await.unwrap();

        // Pre-build all the data
        let data: Vec<Vec<u8>> = (0..NUM_RECORDS)
            .map(|i| format!("Hello, world! {}", i).as_bytes().to_vec())
            .collect();

        let mut records = Vec::new();
        let start = std::time::Instant::now();

        // Process in chunks
        for chunk in data.chunks(BATCH_SIZE) {
            let batch_refs: Vec<Vec<u8>> = chunk.iter().map(|b| b.to_vec()).collect();
            let batch_records = write_ahead.write_batch(batch_refs).await.unwrap();
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
    async fn test_write_ahead_large_data_mixed_batch() {
        let _ = std::fs::remove_dir_all("./test_logs/test_write_ahead_large_data_mixed_batch");
        create_logger();
        let mut opts = WriteAheadOptions::default();
        opts.log_dir = PathBuf::from("./test_logs/test_write_ahead_large_data_mixed_batch");

        let mut write_ahead = WriteAhead::<SimpleFile, IOUringFile>::with_options(opts);
        write_ahead.start().await.unwrap();

        // Pre-build all the data
        let data: Vec<Vec<u8>> = (0..NUM_RECORDS)
            .map(|i| format!("Hello, world! {}", i).as_bytes().to_vec())
            .collect();

        let mut records = Vec::new();
        let start = std::time::Instant::now();

        // Process in chunks
        for chunk in data.chunks(BATCH_SIZE) {
            let batch_refs: Vec<Vec<u8>> = chunk.iter().map(|b| b.to_vec()).collect();
            let batch_records = write_ahead.write_batch(batch_refs).await.unwrap();
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

        std::fs::remove_dir_all("./test_logs/test_write_ahead_large_data_mixed_batch").unwrap();
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn test_write_ahead_large_data_uring_batch() {
        let _ = std::fs::remove_dir_all("./test_logs/test_write_ahead_large_data_uring_batch");
        create_logger();
        let mut opts = WriteAheadOptions::default();
        opts.log_dir = PathBuf::from("./test_logs/test_write_ahead_large_data_uring_batch");

        let mut write_ahead = WriteAhead::<IOUringFile, IOUringFile>::with_options(opts);
        write_ahead.start().await.unwrap();

        // Pre-build all the data
        let data: Vec<Vec<u8>> = (0..NUM_RECORDS)
            .map(|i| format!("Hello, world! {}", i).as_bytes().to_vec())
            .collect();

        let mut records = Vec::new();
        let start = std::time::Instant::now();

        // Process in chunks
        for chunk in data.chunks(BATCH_SIZE) {
            let batch_refs: Vec<Vec<u8>> = chunk.iter().map(|b| b.to_vec()).collect();
            let batch_records = write_ahead.write_batch(batch_refs).await.unwrap();
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

    // #[tokio::test]
    // async fn test_write_ahead_stream() {
    //     let _ = std::fs::remove_dir_all("./test_logs/test_write_ahead_stream");
    //     create_logger();

    //     let mut opts = WriteAheadOptions::default();
    //     opts.max_file_size = 128; // 1KB
    //     opts.log_dir = PathBuf::from("./test_logs/test_write_ahead_stream");

    //     let mut write_ahead = WriteAhead::<SimpleFile, SimpleFile>::with_options(opts);
    //     write_ahead.start().await.unwrap();

    //     let mut records = Vec::new();

    //     // Write 100 records
    //     for i in 0..100 {
    //         let start = std::time::Instant::now();
    //         let record = write_ahead
    //             .write_batch(vec![format!("Hello, world! {}", i).as_bytes().to_vec()])
    //             .await
    //             .unwrap();
    //         let end = std::time::Instant::now();
    //         debug!("Write time taken: {:?}", end.duration_since(start));
    //         records.push(record);
    //     }

    //     // Read it back with the stream
    //     let mut stream = write_ahead.create_stream().await;
    //     for i in 0..100 {
    //         let record = stream.next().await.unwrap().unwrap();
    //         assert_eq!(record, format!("Hello, world! {}", i).as_bytes());
    //     }

    //     // Add cleanup
    //     std::fs::remove_dir_all("./test_logs/test_write_ahead_stream").unwrap();
    // }

    // #[test]
    // fn test_write_ahead_stream_is_send() {
    //     fn assert_send<T: Send>() {}
    //     assert_send::<WriteAheadStream<SimpleFile>>();
    // }

    // #[cfg(target_os = "linux")]
    // #[tokio::test]
    // async fn test_write_ahead_stream_uring() {
    //     debug!("Starting test_write_ahead_stream_uring");
    //     let _ = std::fs::remove_dir_all("./test_logs/test_write_ahead_stream_uring");
    //     create_logger();

    //     debug!("Creating WriteAhead instance");
    //     let mut opts = WriteAheadOptions::default();
    //     opts.max_file_size = 128; // 1KB
    //     opts.log_dir = PathBuf::from("./test_logs/test_write_ahead_stream_uring");

    //     let mut write_ahead = WriteAhead::<IOUringFile, IOUringFile>::with_options(opts);
    //     debug!("Starting WriteAhead");
    //     write_ahead.start().await.unwrap();
    //     debug!("WriteAhead started successfully");

    //     let mut records = Vec::new();

    //     debug!("Beginning to write 100 records");
    //     // Write 100 records
    //     for i in 0..100 {
    //         if i % 10 == 0 {
    //             debug!("Writing record batch {}/10", i / 10 + 1);
    //         }
    //         let start = std::time::Instant::now();
    //         let record = write_ahead
    //             .write_batch(vec![format!("Hello, world! {}", i).as_bytes().to_vec()])
    //             .await
    //             .unwrap();
    //         let end = std::time::Instant::now();
    //         debug!("Write time taken: {:?}", end.duration_since(start));
    //         records.push(record);
    //     }
    //     debug!("Finished writing all records");

    //     debug!("Creating stream for reading");
    //     let mut stream = write_ahead.create_stream().await;
    //     debug!("Beginning to read back records");

    //     for i in 0..100 {
    //         if i % 10 == 0 {
    //             debug!("Reading record batch {}/10", i / 10 + 1);
    //         }
    //         let record = stream.next().await.unwrap().unwrap();
    //         assert_eq!(record, format!("Hello, world! {}", i).as_bytes());
    //     }
    //     debug!("Finished reading all records");

    //     debug!("Cleaning up test directory");
    //     std::fs::remove_dir_all("./test_logs/test_write_ahead_stream_uring").unwrap();
    //     debug!("test_write_ahead_stream_uring completed successfully");
    // }
}
