use anyhow::{Context, Result};
use std::{
    fs::{File, OpenOptions},
    io::{BufReader, Read, Seek, Write},
    path::{Path, PathBuf},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use crate::murmur3::murmur3_128;

/// Logfile represents a log file on disk.
///
/// # Format
///
/// The log file has the following format
///
/// ## High level format
///
/// ```text
/// | 8 bytes (u64) - magic number | 8 bytes (u64) - created_at nanoseconds | N bytes - records | 8 bytes (u64) - sealed_at nanoseconds | 8 bytes (u64) - magic number |
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
#[derive(Debug)]
pub struct Logfile {
    created_at: SystemTime,
    sealed_at: Option<SystemTime>,
    id: u64,
    fd: File,
}

const MAGIC_NUMBER: [u8; 8] = [0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42];

impl Logfile {
    /// Creates a new logfile, deleting any existing file at the given path.
    pub fn new(id: u64, fd: File) -> Result<Self> {
        let created_at = SystemTime::now();

        let mut header = [0u8; 16];
        header[0..8].copy_from_slice(&MAGIC_NUMBER);
        header[8..16].copy_from_slice(
            &created_at
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
                .to_le_bytes()[0..8],
        );

        let mut fd = fd;
        fd.write_all(&header).context("Failed to write header")?;
        fd.sync_all().context("Failed to sync header")?;

        Ok(Self {
            created_at,
            sealed_at: None,
            id,
            fd,
        })
    }

    pub fn file_id_from_path(path: &Path) -> Result<u64> {
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

        numeric_part
            .parse::<u64>()
            .context("Log file name does not contain a valid u64")
    }

    pub fn from_file(path: &Path) -> Result<Self> {
        let mut fd = File::open(path)?;
        let id = Self::file_id_from_path(path)?;

        // Read the first 16 bytes of the file
        let mut buffer: [u8; 16] = [0; 16];
        fd.read_exact(&mut buffer)
            .context("Failed to read first 16 bytes of logfile")?;

        // Check if the first 8 bytes are the magic number
        if buffer[0..8] != MAGIC_NUMBER {
            return Err(anyhow::anyhow!("Invalid start magic number"));
        }

        // Read the next 8 bytes as the created_at timestamp
        let created_at = UNIX_EPOCH
            + Duration::from_nanos(u64::from_le_bytes(
                buffer[8..16]
                    .try_into()
                    .context("Failed to read created_at timestamp")?,
            ));

        // Try to read the last 16 bytes, but don't fail if they don't exist
        let mut sealed_at = None;
        let mut buffer: [u8; 16] = [0; 16];
        if let Ok(metadata) = fd.metadata() {
            if metadata.len() >= 32 {
                // Only try to read footer if file is long enough
                fd.seek(std::io::SeekFrom::End(-16))?;
                if fd.read_exact(&mut buffer).is_ok() && buffer[0..8] != MAGIC_NUMBER {
                    return Err(anyhow::anyhow!("Invalid end magic number"));
                }
                sealed_at = Some(
                    UNIX_EPOCH
                        + Duration::from_nanos(u64::from_le_bytes(
                            buffer[8..16]
                                .try_into()
                                .context("Failed to read sealed_at timestamp")?,
                        )),
                );
            }
        }

        Ok(Self {
            created_at,
            sealed_at,
            id,
            fd,
        })
    }

    pub fn write_record(&mut self, record: &[u8]) -> Result<u64> {
        // Get the current offset BEFORE writing - this is where the record will start
        let offset = self.fd.seek(std::io::SeekFrom::Current(0))?;

        // Verify the record is no longer than max u32
        if record.len() > u32::MAX as usize {
            return Err(anyhow::anyhow!(
                "Record is too large, must be less than {}",
                u32::MAX
            ));
        }

        // Generate a murmur3 hash of the record
        let (hash1, hash2) = murmur3_128(record);
        let length = record.len() as u32;

        // Pre-allocate buffer with exact size (16 bytes for hash + 8 bytes for length + record length)
        let mut buf = Vec::with_capacity(24 + record.len());
        buf.extend_from_slice(&hash1.to_le_bytes());
        buf.extend_from_slice(&hash2.to_le_bytes());
        buf.extend_from_slice(&length.to_le_bytes());
        buf.extend_from_slice(record);

        self.fd.write_all(&buf).context("Failed to write record")?;
        self.fd
            .sync_all()
            .context("Failed to sync header and record")?;

        // Return the starting offset of the record
        Ok(offset)
    }

    pub fn read_record(&mut self, offset: u64) -> Result<Vec<u8>> {
        // Seek to the specified offset
        self.fd
            .seek(std::io::SeekFrom::Start(offset))
            .context("Failed to seek to record")?;

        // Read the hash and length (20 bytes total)
        let mut header = [0u8; 20];
        self.fd
            .read_exact(&mut header)
            .context("Failed to read record header")?;

        // Parse the header
        let hash1 = i64::from_le_bytes(header[0..8].try_into().unwrap());
        let hash2 = i64::from_le_bytes(header[8..16].try_into().unwrap());
        let length = u32::from_le_bytes(header[16..20].try_into().unwrap());

        // Read the actual record data
        let mut record = vec![0u8; length as usize];
        self.fd
            .read_exact(&mut record)
            .context("Failed to read record data")?;

        // Verify the hash
        let (computed_hash1, computed_hash2) = murmur3_128(&record);
        if computed_hash1 != hash1 || computed_hash2 != hash2 {
            return Err(anyhow::anyhow!(
                "Invalid hash for record, this file is corrupted"
            ));
        }

        Ok(record)
    }

    pub fn seal(&mut self) -> Result<()> {
        if let Some(_) = self.sealed_at {
            return Err(anyhow::anyhow!("Logfile already sealed"));
        }

        self.sealed_at = Some(SystemTime::now());

        let mut footer = [0u8; 16];
        footer[0..8].copy_from_slice(&MAGIC_NUMBER);
        footer[8..16].copy_from_slice(
            &self
                .sealed_at
                .unwrap()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
                .to_le_bytes()[0..8],
        );

        self.fd
            .write_all(&footer)
            .context("Failed to write footer")?;
        self.fd.sync_all().context("Failed to sync footer")?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_write_without_sealing() {
        let path = PathBuf::from("/tmp/1.log");
        let fd = File::create(&path).unwrap();

        let mut logfile = Logfile::new(1, fd).unwrap();
        let offset = logfile.write_record(b"hello").unwrap();
        let created_at = logfile.created_at;

        let mut logfile = Logfile::from_file(&path).unwrap();
        assert_eq!(logfile.id, 1);
        assert_eq!(logfile.created_at, created_at);
        assert_eq!(logfile.sealed_at, None);
        assert_eq!(logfile.read_record(offset).unwrap(), b"hello");

        // Clean up the test file
        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn test_write_with_sealing() {
        let path = PathBuf::from("/tmp/2.log");
        let fd = File::create(&path).unwrap();

        let mut logfile = Logfile::new(2, fd).unwrap();
        let offset = logfile.write_record(b"hello").unwrap();
        logfile.seal().unwrap();
        let created_at = logfile.created_at;
        let sealed_at = logfile.sealed_at.unwrap();

        let mut logfile = Logfile::from_file(&path).unwrap();
        assert_eq!(logfile.id, 2);
        assert_eq!(logfile.created_at, created_at);
        assert_eq!(logfile.sealed_at, Some(sealed_at));
        assert_eq!(logfile.read_record(offset).unwrap(), b"hello");

        // Clean up the test file
        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn test_corrupted_sealed_file() {
        let path = PathBuf::from("/tmp/3.log");
        let fd = File::create(&path).unwrap();

        // Create and seal a valid logfile
        let mut logfile = Logfile::new(3, fd).unwrap();
        logfile.write_record(b"hello").unwrap();
        logfile.seal().unwrap();

        // Corrupt the file by appending invalid data after the footer
        let mut fd = OpenOptions::new().append(true).open(&path).unwrap();
        fd.write_all(b"corrupted").unwrap();
        fd.sync_all().unwrap();

        // Attempting to open the corrupted file should fail
        let res = Logfile::from_file(&path);
        assert!(res.is_err());

        // Clean up the test file
        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn test_corrupted_record() {
        let path = PathBuf::from("/tmp/4.log");
        let fd = File::create(&path).unwrap();

        let mut logfile = Logfile::new(4, fd).unwrap();
        let offset = logfile.write_record(b"hello").unwrap();

        // Corrupt the record by modifying a single byte in the middle
        logfile
            .fd
            .seek(std::io::SeekFrom::Start(offset + 22)) // Skip past hash, length, and position to middle of "hello"
            .unwrap();
        logfile.fd.write_all(&[b'x']).unwrap(); // Replace one byte with 'x'
        logfile.fd.sync_all().unwrap();

        // Attempting to read the corrupted record should fail due to hash mismatch
        assert!(logfile.read_record(offset).is_err());

        // Clean up the test file
        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn test_corrupted_file_header() {
        let path = PathBuf::from("/tmp/5.log");
        let mut fd = File::create(&path).unwrap();

        // Write an invalid magic number
        let invalid_header = [0xFF; 16];
        fd.write_all(&invalid_header).unwrap();
        fd.sync_all().unwrap();

        // Attempting to open the file with invalid header should fail
        let result = Logfile::from_file(&path);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Invalid start magic number"));

        // Clean up the test file
        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn test_100_records() {
        let path = PathBuf::from("/tmp/100.log");
        let fd = File::create(&path).unwrap();

        // Create a new logfile and write 100 records
        let mut logfile = Logfile::new(100, fd).unwrap();
        let mut offsets = Vec::with_capacity(100);

        for i in 0..100 {
            let record = format!("record_{}", i);
            let offset = logfile.write_record(record.as_bytes()).unwrap();
            offsets.push((offset, record));
        }

        // Seal the file
        logfile.seal().unwrap();

        // Reopen the file and verify all records
        let mut logfile = Logfile::from_file(&path).unwrap();
        for (offset, expected_record) in offsets {
            let record = logfile.read_record(offset).unwrap();
            assert_eq!(
                String::from_utf8(record).unwrap(),
                expected_record,
                "Record mismatch at offset {}",
                offset
            );
        }

        // Clean up the test file
        std::fs::remove_file(&path).unwrap();
    }
}
