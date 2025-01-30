use anyhow::{anyhow, Context, Result};
use futures::executor::block_on;
use futures::{pin_mut, Stream};
use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::sync::atomic::AtomicBool;
use std::sync::mpsc;
use std::task::{Context as TaskContext, Poll};
use std::thread;
use tracing::{debug, instrument, trace};

use crate::fileio::FileReader;
use crate::{fileio::FileWriter, murmur3::murmur3_128};

/// Logfile represents a log file on disk.
///
/// # Format
///
/// The log file has the following format
///
/// ## High level format
///
/// ```text
/// | 8 bytes (u64) - magic number | N bytes - records | 1 byte - sealed flag | 8 bytes (u64) - magic number |
/// ```
///
/// ## Record format
///
/// Each record is a 16 byte long array of bytes.
///
/// ```text
/// | 16 bytes (i64, i64) - murmur3 hash | 4 bytes (u32) - data length | N bytes - data |
///
/// # Corruption protection
///
/// A hash is created for each record, but not for the headers. That is because the headers
/// really just convenience info. So record data is protected by the hash, but the headers
/// are not.
///
/// # Escape 0xff
///
/// 0x00 is used as an escape character for 0xff. This is to prevent corruption of the file
/// when 0xff is written to the file.
#[derive(Debug)]
pub struct Logfile<F: FileReader> {
    pub sealed: bool,
    pub id: String,
    fio: F,
    delete_on_drop: AtomicBool,
}

const MAGIC_NUMBER: [u8; 8] = [0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff];

#[derive(Debug, thiserror::Error)]
pub enum LogfileError {
    #[error("Logfile is corrupted")]
    Corrupted,

    #[error("Partial write detected, you should trim this file, or recover from a replica")]
    PartialWrite,

    #[error("Cannot write to sealed logfile")]
    WriteToSealed,

    #[error("Invalid start magic number")]
    InvalidMagicNumber,

    #[error("Invalid offset {0}")]
    InvalidOffset(u64),

    #[error("Record is too large, must be less than {}", u32::MAX)]
    RecordTooLarge,
}

pub fn file_id_from_path(path: &Path) -> Result<String> {
    let filename = path
        .file_name()
        .context("Log file name is missing")?
        .to_str()
        .context("Log file name is not a valid UTF-8 string")?;

    let numeric_part = filename
        .trim_end_matches(".log")
        .chars()
        .skip_while(|c| !c.is_ascii_digit())
        .take_while(|c| c.is_ascii_digit())
        .collect::<String>();

    if numeric_part.is_empty() {
        return Err(anyhow!(LogfileError::InvalidMagicNumber));
    }

    Ok(numeric_part)
}

impl<F: FileReader> Logfile<F> {
    /// Creates a new logfile, deleting any existing file at the given path.
    ///
    /// The id MUST be unique per file!
    pub async fn new(path: &Path) -> Result<Self> {
        let fio = F::open(path).await?;

        let id = file_id_from_path(path)?;

        let logfile = Self {
            sealed: false,
            id: id.to_string(),
            fio,
            delete_on_drop: AtomicBool::new(false),
        };

        Ok(logfile)
    }

    pub async fn from_file(path: &Path) -> Result<Self> {
        let fd = F::open(path).await?;
        let file_length = fd.file_length()?;
        let id = file_id_from_path(path)?;

        // Read the first 8 bytes of the file
        let buffer = fd
            .read(0, 8)
            .await
            .context("Failed to read first 8 bytes of logfile")?;

        // Check if the first 8 bytes are the magic number
        if buffer[0..8] != MAGIC_NUMBER {
            return Err(anyhow!(LogfileError::InvalidMagicNumber));
        }

        // Try to read the last 9 bytes (1 for sealed flag + 8 for magic number)
        let mut sealed = false;
        if file_length >= 17 {
            // 8 header + at least 9 footer
            let buffer = fd.read(file_length - 9, 9).await?;
            if buffer[1..9] == MAGIC_NUMBER {
                sealed = buffer[0] == 1;
            }
        }

        Ok(Self {
            sealed,
            id,
            fio: fd,
            delete_on_drop: AtomicBool::new(false),
        })
    }

    pub async fn read_record(&self, offset: &u64) -> Result<Vec<u8>> {
        // Verify that the header is not trying to be read
        if *offset < 8 {
            return Err(anyhow!(LogfileError::InvalidOffset(*offset)));
        }
        trace!(
            "Reading record at offset {} sealed: {}",
            *offset,
            self.sealed
        );

        // Read the hash and length (20 bytes total)
        let header = self.fio.read(*offset, 20).await.context(
            "Failed to read record header, is the file corrupted, or a partial write occurred?",
        )?;

        // Parse the header
        let hash1 = i64::from_le_bytes(
            header[0..8]
                .try_into()
                .context("Failed to parse record hash1")?,
        );
        let hash2 = i64::from_le_bytes(
            header[8..16]
                .try_into()
                .context("Failed to parse record hash2")?,
        );
        let length = u32::from_le_bytes(
            header[16..20]
                .try_into()
                .context("Failed to parse record length")?,
        );

        // Verify that the file is long enough to contain the record
        // if *offset + 20 + length as u64 > self.file_length {
        //     return Err(anyhow!(LogfileError::PartialWrite));
        // }

        // Read the actual record data
        let mut record = self.fio.read(*offset + 20, length as u64).await?;

        // Verify the hash
        let (computed_hash1, computed_hash2) = murmur3_128(&record);
        if computed_hash1 != hash1 || computed_hash2 != hash2 {
            return Err(anyhow!(LogfileError::Corrupted));
        }

        // Replace 0x00 0xff with 0xff, scanning from end to start
        let mut i = record.len();
        while i > 0 {
            i -= 1;
            if i > 0 && record[i - 1] == 0x00 && record[i] == 0xff {
                record.remove(i - 1);
                i -= 1;
            }
        }

        Ok(record)
    }

    pub fn delete(&self) {
        self.delete_on_drop
            .store(true, std::sync::atomic::Ordering::Release);
    }

    fn file_length(&self) -> Result<u64, anyhow::Error> {
        self.fio.file_length()
    }
}

impl<F: FileReader> Drop for Logfile<F> {
    fn drop(&mut self) {
        if self
            .delete_on_drop
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            self.delete();
        }
    }
}

pub enum WriterCommand {
    Write(flume::Sender<Result<WriteResponse>>, Vec<Vec<u8>>),
    Seal(flume::Sender<Result<()>>),
}

#[derive(Debug)]
pub struct WriteResponse {
    pub offsets: Vec<u64>,
    pub file_length: u64,
}

#[derive(Debug)]
pub struct LogFileWriter<F: FileWriter> {
    id: String,
    fio: F,
    recv: flume::Receiver<WriterCommand>,
    sealed: bool,
    file_length: u64,
}

impl<F: FileWriter + 'static> LogFileWriter<F> {
    async fn new(path: &Path, recv: flume::Receiver<WriterCommand>) -> Result<Self> {
        let mut fio = F::open(path).await?;
        let mut header = [0u8; 8];
        header[0..8].copy_from_slice(&MAGIC_NUMBER);

        fio.write(0, &header).await?;

        let file_length = fio.file_length()?;
        let id = file_id_from_path(path)?;
        Ok(Self {
            id,
            fio,
            recv,
            sealed: false,
            file_length,
        })
    }

    #[instrument(level = "trace")]
    pub async fn launch(path: &Path) -> Result<flume::Sender<WriterCommand>> {
        let (tx, rx) = flume::unbounded();
        let logfile = Self::new(path, rx).await?;
        tokio::spawn(async move { logfile.actor_loop().await });
        Ok(tx)
    }

    async fn actor_loop(mut self) {
        loop {
            let msg = match self.recv.recv() {
                Ok(msg) => msg,
                Err(e) => {
                    trace!("Logfile {} writer actor disconnected", self.id);
                    return;
                }
            };
            match msg {
                WriterCommand::Write(return_chan, data) => {
                    trace!("Writing {} records to logfile {}", data.len(), self.id);
                    let offsets = match self.write_records(data).await {
                        Ok(offsets) => offsets,
                        Err(e) => {
                            return_chan.send(Err(e)).unwrap();
                            continue;
                        }
                    };
                    return_chan
                        .send(Ok(WriteResponse {
                            offsets,
                            file_length: self.file_length,
                        }))
                        .unwrap();
                }
                WriterCommand::Seal(return_chan) => {
                    trace!("Sealing logfile {}", self.id);
                    return_chan.send(self.seal().await).unwrap();
                }
            }
        }
    }

    async fn write_records(&mut self, records: Vec<Vec<u8>>) -> Result<Vec<u64>> {
        if self.sealed {
            return Err(anyhow!(LogfileError::WriteToSealed));
        }

        let mut offsets = Vec::with_capacity(records.len());
        // Calculate total buffer size needed:
        // For each record: 16 bytes (hash) + 4 bytes (length) + data length + potential escape bytes
        // let total_size = records
        //     .iter()
        //     .map(|r| {
        //         let escape_count = r.iter().filter(|&&b| b == 0xff).count();
        //         20 + r.len() + escape_count
        //     })
        //     .sum();
        // let mut combined_buffer = Vec::with_capacity(total_size);
        let mut combined_buffer = Vec::new();
        let mut current_offset = self.file_length;

        for record in records {
            if record.len() > u32::MAX as usize {
                return Err(anyhow!(LogfileError::RecordTooLarge));
            }

            // Replace 0xff with 0x00 0xff, but scan from end to start to handle insertions correctly
            let mut processed = record.to_vec();
            let mut i = processed.len();
            while i > 0 {
                i -= 1;
                if processed[i] == 0xff {
                    processed.insert(i, 0x00);
                }
            }

            // Verify the record is no longer than max u32 after escaping
            if processed.len() > u32::MAX as usize {
                return Err(anyhow!(LogfileError::RecordTooLarge));
            }

            // Generate a murmur3 hash of the record
            let (hash1, hash2) = murmur3_128(&processed);
            let length = processed.len() as u32;

            // Add record to the combined buffer
            offsets.push(current_offset);
            current_offset += 20 + processed.len() as u64;

            // Append header and data to combined buffer
            combined_buffer.extend_from_slice(&hash1.to_le_bytes());
            combined_buffer.extend_from_slice(&hash2.to_le_bytes());
            combined_buffer.extend_from_slice(&length.to_le_bytes());
            combined_buffer.extend_from_slice(&processed);
        }

        // Write all records in one operation
        self.fio
            .write(self.file_length, &combined_buffer)
            .await
            .context("Failed to write records")?;

        self.file_length = current_offset;
        Ok(offsets)
    }

    pub async fn seal(&mut self) -> Result<()> {
        if self.sealed {
            return Err(anyhow!(LogfileError::WriteToSealed));
        }

        self.sealed = true;

        let mut footer = [0u8; 9];
        footer[0] = 1; // sealed flag
        footer[1..9].copy_from_slice(&MAGIC_NUMBER);

        self.fio
            .write(self.file_length, &footer)
            .await
            .context("Failed to write footer")?;

        self.file_length += 9;
        Ok(())
    }
}

pub struct LogFileStream<F: FileReader> {
    logfile: Logfile<F>,
    offset: u64,
}

impl<F: FileReader> LogFileStream<F> {
    pub fn reset_stream(&mut self) {
        self.offset = 8;
    }

    pub fn stream_offset(&self) -> u64 {
        self.offset
    }

    pub fn set_stream_offset(&mut self, offset: u64) {
        self.offset = offset;
    }

    async fn read_and_move_offset(&mut self, offset: u64) -> Result<Vec<u8>> {
        let record = self.logfile.read_record(&offset).await?;
        self.offset += 20 + record.len() as u64;
        Ok(record)
    }
}

impl<F: FileReader> Stream for LogFileStream<F> {
    type Item = Result<Vec<u8>>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        if this.logfile.sealed && this.offset >= this.logfile.file_length()? - 9 {
            return Poll::Ready(None);
        } else if this.offset >= this.logfile.file_length()? {
            return Poll::Ready(None);
        }

        let future = this.read_and_move_offset(this.offset);
        pin_mut!(future);

        match future.poll(cx) {
            Poll::Ready(Ok(record)) => Poll::Ready(Some(Ok(record))),
            Poll::Ready(Err(e)) => Poll::Ready(Some(Err(e))),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<F: FileReader> LogFileStream<F> {
    // Add constructor
    pub fn new(logfile: Logfile<F>) -> Self {
        Self {
            logfile,
            offset: 8, // Start after magic number
        }
    }

    pub fn new_with_offset(logfile: Logfile<F>, offset: u64) -> Self {
        Self {
            logfile,
            offset: if offset < 8 { 8 } else { offset },
        }
    }
}

impl<F: FileReader> Unpin for LogFileStream<F> {}

#[cfg(test)]
pub mod tests {
    use super::*;
    use futures::stream::StreamExt;
    use std::{
        io::{Seek, Write},
        path::PathBuf,
    };

    pub async fn test_write_without_sealing<
        WriteF: FileWriter + 'static,
        ReadF: FileReader + 'static,
    >(
        path: PathBuf,
    ) {
        let writer = LogFileWriter::<WriteF>::launch(&path).await.unwrap();
        let (tx, rx) = flume::unbounded();
        writer
            .send(WriterCommand::Write(tx, vec![b"hello".to_vec()]))
            .unwrap();
        let response = rx.recv().unwrap().unwrap();
        let offsets = response.offsets;

        let logfile: Logfile<ReadF> = Logfile::from_file(&path).await.unwrap();
        assert_eq!(logfile.id, "01");
        assert!(!logfile.sealed);
        assert_eq!(logfile.read_record(&offsets[0]).await.unwrap(), b"hello");

        std::fs::remove_file(&path).unwrap();
    }

    pub async fn test_write_with_sealing<
        WriteF: FileWriter + 'static,
        ReadF: FileReader + 'static,
    >(
        path: PathBuf,
    ) {
        let writer = LogFileWriter::<WriteF>::launch(&path).await.unwrap();
        let (tx, rx) = flume::unbounded();
        writer
            .send(WriterCommand::Write(tx, vec![b"hello".to_vec()]))
            .unwrap();
        let response = rx.recv().unwrap().unwrap();
        let offsets = response.offsets;

        // Seal the file
        let (tx, rx) = flume::unbounded();
        writer.send(WriterCommand::Seal(tx)).unwrap();
        rx.recv().unwrap().unwrap();

        // Try to write after sealing - should fail
        let (tx, rx) = flume::unbounded();
        writer
            .send(WriterCommand::Write(tx, vec![b"world".to_vec()]))
            .unwrap();
        assert!(matches!(
            rx.recv()
                .unwrap()
                .unwrap_err()
                .downcast_ref::<LogfileError>(),
            Some(LogfileError::WriteToSealed)
        ));

        let logfile: Logfile<ReadF> = Logfile::from_file(&path).await.unwrap();
        assert_eq!(logfile.id, "02");
        assert!(logfile.sealed);
        assert_eq!(logfile.read_record(&offsets[0]).await.unwrap(), b"hello");

        std::fs::remove_file(&path).unwrap();
    }

    pub async fn test_corrupted_record<
        WriteF: FileWriter + 'static,
        ReadF: FileReader + 'static,
    >(
        path: PathBuf,
    ) {
        let writer = LogFileWriter::<WriteF>::launch(&path).await.unwrap();
        let (tx, rx) = flume::unbounded();
        writer
            .send(WriterCommand::Write(tx, vec![b"hello".to_vec()]))
            .unwrap();
        let response = rx.recv().unwrap().unwrap();
        let offsets = response.offsets;

        // Corrupt the record directly using a new file handle
        let logfile: Logfile<ReadF> = Logfile::from_file(&path).await.unwrap();
        let mut tmp = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        tmp.seek(std::io::SeekFrom::Start(offsets[0] + 22)).unwrap();
        tmp.write_all(b"x").unwrap();

        let result = logfile.read_record(&offsets[0]).await;
        assert!(matches!(
            result.unwrap_err().downcast_ref::<LogfileError>(),
            Some(LogfileError::Corrupted)
        ));

        std::fs::remove_file(&path).unwrap();
    }

    pub async fn test_corrupted_record_sealed<
        WriteF: FileWriter + 'static,
        ReadF: FileReader + 'static,
    >(
        path: PathBuf,
    ) {
        let writer = LogFileWriter::<WriteF>::launch(&path).await.unwrap();

        // Write and get offset
        let (tx, rx) = flume::unbounded();
        writer
            .send(WriterCommand::Write(tx, vec![b"hello".to_vec()]))
            .unwrap();
        let response = rx.recv().unwrap().unwrap();
        let offsets = response.offsets;

        // Seal the file
        let (tx, rx) = flume::unbounded();
        writer.send(WriterCommand::Seal(tx)).unwrap();
        rx.recv().unwrap().unwrap();

        // Corrupt the record
        let logfile: Logfile<ReadF> = Logfile::from_file(&path).await.unwrap();
        let mut tmp = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        tmp.seek(std::io::SeekFrom::Start(offsets[0] + 22)).unwrap();
        tmp.write_all(b"x").unwrap();
        assert!(logfile.read_record(&offsets[0]).await.is_err());

        std::fs::remove_file(&path).unwrap();
    }

    pub async fn test_100_records<WriteF: FileWriter + 'static, ReadF: FileReader + 'static>(
        path: PathBuf,
    ) {
        let writer = LogFileWriter::<WriteF>::launch(&path).await.unwrap();

        // Write 100 records
        let records: Vec<Vec<u8>> = (0..100)
            .map(|i| format!("record_{}", i).into_bytes())
            .collect();

        let (tx, rx) = flume::unbounded();
        writer.send(WriterCommand::Write(tx, records)).unwrap();
        let response = rx.recv().unwrap().unwrap();
        let offsets = response.offsets;

        // Seal the file
        let (tx, rx) = flume::unbounded();
        writer.send(WriterCommand::Seal(tx)).unwrap();
        rx.recv().unwrap().unwrap();

        // Verify all records
        let logfile: Logfile<ReadF> = Logfile::from_file(&path).await.unwrap();
        for (i, offset) in offsets.iter().enumerate() {
            let record = logfile.read_record(offset).await.unwrap();
            assert_eq!(String::from_utf8(record).unwrap(), format!("record_{}", i));
        }

        std::fs::remove_file(&path).unwrap();
    }

    pub async fn test_write_magic_number_without_sealing_escape<
        WriteF: FileWriter + 'static,
        ReadF: FileReader + 'static,
    >(
        path: PathBuf,
    ) {
        let writer = LogFileWriter::<WriteF>::launch(&path).await.unwrap();

        let (tx, rx) = flume::unbounded();
        writer
            .send(WriterCommand::Write(tx, vec![MAGIC_NUMBER.to_vec()]))
            .unwrap();
        let response = rx.recv().unwrap().unwrap();
        let offsets = response.offsets;

        let logfile: Logfile<ReadF> = Logfile::from_file(&path).await.unwrap();
        assert_eq!(logfile.id, "08");
        assert!(!logfile.sealed);
        assert_eq!(
            logfile.read_record(&offsets[0]).await.unwrap(),
            &MAGIC_NUMBER
        );

        std::fs::remove_file(&path).unwrap();
    }

    pub async fn test_write_magic_number_sealing_escape<
        WriteF: FileWriter + 'static,
        ReadF: FileReader + 'static,
    >(
        path: PathBuf,
    ) {
        let writer = LogFileWriter::<WriteF>::launch(&path).await.unwrap();

        // Write magic number
        let (tx, rx) = flume::unbounded();
        writer
            .send(WriterCommand::Write(tx, vec![MAGIC_NUMBER.to_vec()]))
            .unwrap();
        let response = rx.recv().unwrap().unwrap();
        let offsets = response.offsets;

        // Seal the file
        let (tx, rx) = flume::unbounded();
        writer.send(WriterCommand::Seal(tx)).unwrap();
        rx.recv().unwrap().unwrap();

        let logfile: Logfile<ReadF> = Logfile::from_file(&path).await.unwrap();
        assert_eq!(logfile.id, "09");
        assert!(logfile.sealed);
        assert_eq!(
            logfile.read_record(&offsets[0]).await.unwrap(),
            &MAGIC_NUMBER
        );

        std::fs::remove_file(&path).unwrap();
    }

    pub async fn test_bulk_writes<WriteF: FileWriter + 'static, ReadF: FileReader + 'static>(
        path: PathBuf,
    ) {
        let writer = LogFileWriter::<WriteF>::launch(&path).await.unwrap();

        let records = vec![b"first".to_vec(), b"second".to_vec(), b"third".to_vec()];

        let (tx, rx) = flume::unbounded();
        writer.send(WriterCommand::Write(tx, records)).unwrap();
        let response = rx.recv().unwrap().unwrap();
        let offsets = response.offsets;

        let logfile: Logfile<ReadF> = Logfile::from_file(&path).await.unwrap();
        assert_eq!(offsets.len(), 3);
        assert_eq!(logfile.read_record(&offsets[0]).await.unwrap(), b"first");
        assert_eq!(logfile.read_record(&offsets[1]).await.unwrap(), b"second");
        assert_eq!(logfile.read_record(&offsets[2]).await.unwrap(), b"third");

        std::fs::remove_file(&path).unwrap();
    }

    pub async fn test_stream<WriteF: FileWriter + 'static, ReadF: FileReader + 'static>(
        path: PathBuf,
    ) {
        let writer = LogFileWriter::<WriteF>::launch(&path).await.unwrap();

        // Write 10 records
        let records: Vec<Vec<u8>> = (0..10)
            .map(|i| format!("record_{}", i).into_bytes())
            .collect();

        let (tx, rx) = flume::unbounded();
        writer.send(WriterCommand::Write(tx, records)).unwrap();
        rx.recv().unwrap().unwrap();

        // Test streaming
        let logfile: Logfile<ReadF> = Logfile::from_file(&path).await.unwrap();
        let mut stream = LogFileStream::new(logfile);
        for i in 0..10 {
            let record = stream.next().await.unwrap().unwrap();
            assert_eq!(String::from_utf8(record).unwrap(), format!("record_{}", i));
        }
        let next_val = stream.next().await;
        assert!(next_val.is_none());

        std::fs::remove_file(&path).unwrap();
    }
}
