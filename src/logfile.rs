use anyhow::{anyhow, Context, Result};
use futures::{pin_mut, SinkExt, Stream, StreamExt};
use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Rc;
use std::task::{Context as TaskContext, Poll};
use tokio::sync::mpsc;
use tracing::trace;

use crate::{fileio::FileIO, murmur3::murmur3_128};

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
pub struct Logfile<F: FileIO> {
    pub sealed: bool,
    pub id: String,
    pub file_length: u64,
    fio: F,
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

impl<F: FileIO> Logfile<F> {
    /// Creates a new logfile, deleting any existing file at the given path.
    ///
    /// The id MUST be unique per file!
    pub async fn new(id: &str, fio: F) -> Result<Self> {
        let mut header = [0u8; 8];
        header[0..8].copy_from_slice(&MAGIC_NUMBER);

        let mut fio = fio;
        fio.write(0, &header)
            .await
            .context("Failed to write header")?;

        Ok(Self {
            sealed: false,
            id: id.to_string(),
            fio,
            file_length: 8,
        })
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

    pub async fn from_file(path: &Path) -> Result<Self> {
        let fio = F::open(path).await?;
        let file_length = fio.file_length().await;
        let id = Self::file_id_from_path(path)?;

        // Read the first 8 bytes of the file

        let buffer = fio
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
            let buffer = fio.read(file_length - 9, 9).await?;
            if buffer[1..9] == MAGIC_NUMBER {
                sealed = buffer[0] == 1;
            }
        }

        Ok(Self {
            sealed,
            id,
            fio,
            file_length,
        })
    }

    pub async fn write_records(&mut self, records: &[&[u8]]) -> Result<Vec<u64>> {
        if self.sealed {
            return Err(anyhow!(LogfileError::WriteToSealed));
        }

        let mut offsets = Vec::with_capacity(records.len());
        // TODO: optmization?
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

    pub async fn read_record(&self, offset: &u64) -> Result<Vec<u8>> {
        // Verify that the header is not trying to be read
        if *offset < 8 || (self.sealed && *offset >= self.file_length - 9) {
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
        if *offset + 20 + length as u64 > self.file_length {
            return Err(anyhow!(LogfileError::PartialWrite));
        }

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

    pub fn file_length(&self) -> u64 {
        self.file_length
    }
}

// pub struct ChannelStream {
//     chan: mpsc::Receiver<Result<Vec<u8>>>,
// }

// impl Stream for ChannelStream {
//     type Item = Result<Vec<u8>>;

//     fn poll_next(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Option<Self::Item>> {
//         // Try to receive from the channel
//         self.chan.poll_recv(cx)
//     }
// }

// impl ChannelStream {
//     pub fn new<F: FileIO + Send + 'static>(logfile: Rc<Logfile<F>>) -> ChannelStream {
//         let (tx, rx) = mpsc::channel(100);
//         tokio::spawn(async move {
//             let mut offset = 8;
//             loop {
//                 let record = logfile.read_record(&offset).await.expect("blah");
//                 let record_len = record.len() as u64;
//                 tx.send(Ok(record)).await.unwrap();
//                 offset += 20 + record_len;
//             }
//         });
//         ChannelStream { chan: rx }
//     }
// }

pub struct LogFileStream<F: FileIO> {
    logfile: Rc<Logfile<F>>,
    offset: u64,
}

impl<F: FileIO> LogFileStream<F> {
    pub fn new(logfile: Rc<Logfile<F>>) -> Self {
        Self { logfile, offset: 8 }
    }

    pub fn new_from_offset(logfile: Rc<Logfile<F>>, offset: u64) -> Self {
        Self { logfile, offset }
    }

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

impl<F: FileIO> Stream for LogFileStream<F> {
    type Item = Result<Vec<u8>>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Option<Self::Item>> {
        // If we've reached the end of the file (or the footer if sealed)
        let mut this = self;
        {
            let logfile = this.logfile.clone();

            let end_offset = if logfile.sealed {
                logfile.file_length - 9 // Account for the 9-byte footer
            } else {
                logfile.file_length
            };

            // Add this check to ensure we have enough bytes remaining for at least a record header
            if this.offset >= end_offset || end_offset - this.offset < 20 {
                return Poll::Ready(None);
            }
        }

        // Create a future to read the record
        let offset = this.offset;
        let future = this.read_and_move_offset(offset);
        pin_mut!(future); // Pin the future so we can poll it

        // Poll the future
        let result = match future.poll(cx) {
            Poll::Ready(result) => match result {
                Ok(record) => Poll::Ready(Some(Ok(record))),
                Err(e) => Poll::Ready(Some(Err(e))),
            },
            Poll::Pending => Poll::Pending,
        };

        result
    }
}

#[cfg(test)]
pub mod tests {
    use futures::stream::StreamExt;
    use std::{borrow::BorrowMut, path::PathBuf};

    use super::*;

    pub async fn test_write_without_sealing<F: FileIO>(f: F, path: PathBuf) {
        let mut logfile = Logfile::new(&Logfile::<F>::file_id_from_path(&path).unwrap(), f)
            .await
            .unwrap();
        let offsets = logfile.write_records(&[b"hello"]).await.unwrap();

        let logfile: Logfile<F> = Logfile::from_file(&path).await.unwrap();
        assert_eq!(logfile.id, "01");
        assert!(!logfile.sealed);
        assert_eq!(logfile.read_record(&offsets[0]).await.unwrap(), b"hello");

        // Clean up the test file
        std::fs::remove_file(&path).unwrap();
    }

    pub async fn test_write_with_sealing<F: FileIO>(f: F, path: PathBuf) {
        let mut logfile = Logfile::new(&Logfile::<F>::file_id_from_path(&path).unwrap(), f)
            .await
            .unwrap();
        let offsets = logfile.write_records(&[b"hello"]).await.unwrap();
        logfile.seal().await.unwrap();

        // Try to write after sealing
        let result = logfile.write_records(&[b"world"]).await;
        assert!(matches!(
            result.unwrap_err().downcast_ref::<LogfileError>(),
            Some(LogfileError::WriteToSealed)
        ));

        // Try to seal again
        let result = logfile.seal().await;
        assert!(matches!(
            result.unwrap_err().downcast_ref::<LogfileError>(),
            Some(LogfileError::WriteToSealed)
        ));

        let logfile: Logfile<F> = Logfile::from_file(&path).await.unwrap();
        assert_eq!(logfile.id, "02");
        assert!(logfile.sealed);
        assert_eq!(logfile.read_record(&offsets[0]).await.unwrap(), b"hello");

        // Clean up the test file
        std::fs::remove_file(&path).unwrap();
    }

    pub async fn test_corrupted_record<F: FileIO>(f: F, path: PathBuf) {
        let mut logfile = Logfile::new(&Logfile::<F>::file_id_from_path(&path).unwrap(), f)
            .await
            .unwrap();
        let offsets = logfile.write_records(&[b"hello"]).await.unwrap();

        // Corrupt the record
        logfile.fio.write(offsets[0] + 22, &[b'x']).await.unwrap();

        let result = logfile.read_record(&offsets[0]).await;
        assert!(matches!(
            result.unwrap_err().downcast_ref::<LogfileError>(),
            Some(LogfileError::Corrupted)
        ));

        // Clean up the test file
        std::fs::remove_file(&path).unwrap();
    }

    pub async fn test_corrupted_record_sealed<F: FileIO>(f: F, path: PathBuf) {
        let mut logfile = Logfile::new(&Logfile::<F>::file_id_from_path(&path).unwrap(), f)
            .await
            .unwrap();
        let offsets = logfile.write_records(&[b"hello"]).await.unwrap();
        logfile.seal().await.unwrap();

        // Corrupt the record by modifying a single byte in the middle
        logfile.fio.write(offsets[0] + 22, &[b'x']).await.unwrap();

        // Attempting to read the corrupted record should fail due to hash mismatch
        assert!(logfile.read_record(&offsets[0]).await.is_err());

        // Clean up the test file
        std::fs::remove_file(&path).unwrap();
    }

    pub async fn test_corrupted_file_header<F: FileIO>(f: F, path: PathBuf) {
        let invalid_header = [0x00; 16];
        let mut f = f;
        f.write(0, &invalid_header).await.unwrap();

        let result = Logfile::<F>::from_file(&path).await;
        assert!(result.is_err());

        if let Err(e) = result {
            assert!(e.downcast_ref::<LogfileError>().is_some());
        } else {
            panic!("Expected an error");
        }

        // Clean up the test file
        std::fs::remove_file(&path).unwrap();
    }

    pub async fn test_100_records<F: FileIO>(f: F, path: PathBuf) {
        // Create a new logfile and write 100 records
        let mut logfile = Logfile::new(&Logfile::<F>::file_id_from_path(&path).unwrap(), f)
            .await
            .unwrap();

        // Prepare records
        let records: Vec<Vec<u8>> = (0..100)
            .map(|i| format!("record_{}", i).into_bytes())
            .collect();
        let record_refs: Vec<&[u8]> = records.iter().map(|r| r.as_slice()).collect();

        // Write all records at once
        let offsets = logfile.write_records(&record_refs).await.unwrap();

        // Seal the file
        logfile.seal().await.unwrap();

        // Reopen the file and verify all records
        let logfile: Logfile<F> = Logfile::from_file(&path).await.unwrap();
        for (i, offset) in offsets.iter().enumerate() {
            let record = logfile.read_record(offset).await.unwrap();
            assert_eq!(
                String::from_utf8(record).unwrap(),
                format!("record_{}", i),
                "Record mismatch at offset {}",
                offset
            );
        }

        // Clean up the test file
        std::fs::remove_file(&path).unwrap();
    }

    pub async fn test_stream<F: FileIO>(f: F, path: PathBuf) {
        let mut logfile = Logfile::new(&Logfile::<F>::file_id_from_path(&path).unwrap(), f)
            .await
            .unwrap();

        // Add some records
        for i in 0..100 {
            logfile
                .write_records(&[format!("record_{}", i).as_bytes()])
                .await
                .unwrap();
        }

        // First iteration
        let logfile = Rc::new(logfile);
        {
            let mut iter = LogFileStream::new(logfile.clone());
            for i in 0..100 {
                let record = iter.next().await.unwrap().unwrap();
                assert_eq!(String::from_utf8(record).unwrap(), format!("record_{}", i));
            }
            assert!(iter.next().await.is_none());
        }
        // Second iteration - verify we can create a new iterator and read again
        let mut iter = LogFileStream::new_from_offset(logfile, 8);
        for i in 0..100 {
            let record = iter.next().await.unwrap().unwrap();
            assert_eq!(String::from_utf8(record).unwrap(), format!("record_{}", i));
        }
        assert!(iter.next().await.is_none());
    }

    pub async fn test_write_magic_number_without_sealing_escape<F: FileIO>(f: F, path: PathBuf) {
        let mut logfile = Logfile::new(&Logfile::<F>::file_id_from_path(&path).unwrap(), f)
            .await
            .unwrap();
        let offsets = logfile.write_records(&[&MAGIC_NUMBER]).await.unwrap();

        let logfile: Logfile<F> = Logfile::from_file(&path).await.unwrap();
        assert_eq!(logfile.id, "08");
        assert!(!logfile.sealed);
        assert_eq!(
            logfile.read_record(&offsets[0]).await.unwrap(),
            &MAGIC_NUMBER
        );

        // Clean up the test file
        std::fs::remove_file(&path).unwrap();
    }

    pub async fn test_write_magic_number_sealing_escape<F: FileIO>(f: F, path: PathBuf) {
        let mut logfile = Logfile::new(&Logfile::<F>::file_id_from_path(&path).unwrap(), f)
            .await
            .unwrap();
        let offsets = logfile.write_records(&[&MAGIC_NUMBER]).await.unwrap();
        logfile.seal().await.unwrap();

        let logfile: Logfile<F> = Logfile::from_file(&path).await.unwrap();
        assert_eq!(logfile.id, "09");
        assert!(logfile.sealed);
        assert_eq!(
            logfile.read_record(&offsets[0]).await.unwrap(),
            &MAGIC_NUMBER
        );

        // Clean up the test file
        std::fs::remove_file(&path).unwrap();
    }

    pub async fn test_write_magic_number_without_sealing_escape_iterator<F: FileIO>(
        f: F,
        path: PathBuf,
    ) {
        let mut logfile = Logfile::new(&Logfile::<F>::file_id_from_path(&path).unwrap(), f)
            .await
            .unwrap();
        logfile.write_records(&[&MAGIC_NUMBER]).await.unwrap();

        let file_length = logfile.file_length.clone();
        let logfile = Rc::new(logfile);
        let mut iter = LogFileStream::new(logfile.clone());
        let record = iter.next().await.unwrap().unwrap();
        assert_eq!(record, MAGIC_NUMBER);
        println!("offset: {} file length: {}", iter.offset, file_length);
        let res = iter.next().await;
        println!("res: {:?}", res);
        assert!(res.is_none());

        let logfile: Logfile<F> = Logfile::from_file(&path).await.unwrap();
        assert_eq!(logfile.id, "10");
        assert!(!logfile.sealed);

        let logfile = Rc::new(logfile);

        let mut iter = LogFileStream::new(logfile.clone());
        let record = iter.next().await.unwrap().unwrap();
        assert_eq!(record, MAGIC_NUMBER);
        assert!(iter.next().await.is_none());

        // Clean up the test file
        std::fs::remove_file(&path).unwrap();
    }

    pub async fn test_write_magic_number_with_sealing_escape_iterator<F: FileIO>(
        f: F,
        path: PathBuf,
    ) {
        let mut logfile = Logfile::new(&Logfile::<F>::file_id_from_path(&path).unwrap(), f)
            .await
            .unwrap();
        logfile.write_records(&[&MAGIC_NUMBER]).await.unwrap();

        // First iteration
        let mut log_rc = Rc::new(logfile);
        {
            let mut iter = LogFileStream::new(log_rc.clone());
            let record = iter.next().await.unwrap().unwrap();
            assert_eq!(record, MAGIC_NUMBER);
            assert!(iter.next().await.is_none());
        }

        // Seal the file
        {
            let mut logfile = log_rc.borrow_mut();
            logfile.seal().await.unwrap();
        }

        // Second iteration after sealing
        let mut iter = LogFileStream::new(log_rc.clone());
        let record = iter.next().await.unwrap().unwrap();
        assert_eq!(record, MAGIC_NUMBER);
        assert!(iter.next().await.is_none());

        // Verify with a fresh file handle
        let logfile: Logfile<F> = Logfile::from_file(&path).await.unwrap();
        assert_eq!(logfile.id, "11");
        assert!(logfile.sealed);

        let mut iter = LogFileStream::new(log_rc.clone());
        let record = iter.next().await.unwrap().unwrap();
        assert_eq!(record, MAGIC_NUMBER);
        assert!(iter.next().await.is_none());

        // Clean up the test file
        std::fs::remove_file(&path).unwrap();
    }

    pub async fn test_partial_write<F: FileIO>(f: F, path: PathBuf) {
        let mut logfile = Logfile::new(&Logfile::<F>::file_id_from_path(&path).unwrap(), f)
            .await
            .unwrap();
        let data: Vec<u8> = b"hello".to_vec();
        let offset = logfile.write_records(&[&data]).await.unwrap();

        // Simulate a partial write by truncating the file
        logfile.file_length -= 1;

        let result = logfile.read_record(&offset[0]).await;
        assert!(matches!(
            result.unwrap_err().downcast_ref::<LogfileError>(),
            Some(LogfileError::PartialWrite)
        ));

        // ... cleanup ...
        std::fs::remove_file(&path).unwrap();
    }

    pub async fn test_write_too_large_record<F: FileIO>(f: F, path: PathBuf) {
        let mut logfile = Logfile::new(&Logfile::<F>::file_id_from_path(&path).unwrap(), f)
            .await
            .unwrap();
        let data: Vec<u8> = vec![0; (u32::MAX as usize) + 1];
        let result = logfile.write_records(&[&data]).await;
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err().downcast_ref::<LogfileError>(),
            Some(LogfileError::RecordTooLarge)
        ));

        // Clean up the test file
        std::fs::remove_file(&path).unwrap();
    }

    // Add a new test for bulk writes
    pub async fn test_bulk_writes<F: FileIO>(f: F, path: PathBuf) {
        let mut logfile = Logfile::new(&Logfile::<F>::file_id_from_path(&path).unwrap(), f)
            .await
            .unwrap();

        // Prepare multiple records
        let records: Vec<&[u8]> = vec![b"first", b"second", b"third"];
        let offsets = logfile.write_records(&records).await.unwrap();

        // Verify each record
        assert_eq!(offsets.len(), 3);
        assert_eq!(logfile.read_record(&offsets[0]).await.unwrap(), b"first");
        assert_eq!(logfile.read_record(&offsets[1]).await.unwrap(), b"second");
        assert_eq!(logfile.read_record(&offsets[2]).await.unwrap(), b"third");

        // Clean up the test file
        std::fs::remove_file(&path).unwrap();
    }
}
