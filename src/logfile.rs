use anyhow::{Context, Result};
use std::{
    fs::File,
    io::{Read, Seek, Write},
    path::Path,
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
pub struct Logfile {
    pub sealed: bool,
    pub id: String,
    pub file_length: u64,
    fd: File,
    iter_offset: u64,
}

const MAGIC_NUMBER: [u8; 8] = [0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff];

impl Logfile {
    /// Creates a new logfile, deleting any existing file at the given path.
    ///
    /// The id MUST be unique per file!
    pub fn new(id: &str, fd: File) -> Result<Self> {
        let mut header = [0u8; 8];
        header[0..8].copy_from_slice(&MAGIC_NUMBER);

        let mut fd = fd;
        fd.write_all(&header).context("Failed to write header")?;
        fd.sync_all().context("Failed to sync header")?;

        Ok(Self {
            sealed: false,
            id: id.to_string(),
            fd,
            iter_offset: 8,
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
            return Err(anyhow::anyhow!("Log file name does not contain a valid ID"));
        }

        Ok(numeric_part)
    }

    pub fn from_file(path: &Path) -> Result<Self> {
        let mut fd = File::open(path)?;
        let file_length = fd.metadata()?.len();
        let id = Self::file_id_from_path(path)?;

        // Read the first 8 bytes of the file
        let mut buffer = [0u8; 8];
        fd.read_exact(&mut buffer)
            .context("Failed to read first 8 bytes of logfile")?;

        // Check if the first 8 bytes are the magic number
        if buffer[0..8] != MAGIC_NUMBER {
            return Err(anyhow::anyhow!("Invalid start magic number"));
        }

        // Try to read the last 9 bytes (1 for sealed flag + 8 for magic number)
        let mut sealed = false;
        let mut buffer = [0u8; 9];
        if let Ok(metadata) = fd.metadata() {
            if metadata.len() >= 17 {
                // 8 header + at least 9 footer
                fd.seek(std::io::SeekFrom::End(-9))?;
                if fd.read_exact(&mut buffer).is_ok() && buffer[1..9] == MAGIC_NUMBER {
                    sealed = buffer[0] == 1;
                }
            }
        }

        Ok(Self {
            sealed,
            id,
            fd,
            iter_offset: 8,
            file_length,
        })
    }

    pub fn write_record(&mut self, record: &[u8]) -> Result<u64> {
        let offset = self.fd.seek(std::io::SeekFrom::Current(0))?;

        // Replace 0xff with 0x00 0xff, but scan from end to start to handle insertions correctly
        let mut record = record.to_vec();
        let mut i = record.len();
        while i > 0 {
            i -= 1;
            if record[i] == 0xff {
                record.insert(i, 0x00);
            }
        }

        // Verify the record is no longer than max u32
        if record.len() > u32::MAX as usize {
            return Err(anyhow::anyhow!(
                "Record is too large, must be less than {}",
                u32::MAX
            ));
        }

        // Generate a murmur3 hash of the record
        let (hash1, hash2) = murmur3_128(&record);
        let length = record.len() as u32;

        // Pre-allocate buffer with exact size (16 bytes for hash + 8 bytes for length + record length)
        let mut buf = Vec::with_capacity(24 + record.len());
        buf.extend_from_slice(&hash1.to_le_bytes());
        buf.extend_from_slice(&hash2.to_le_bytes());
        buf.extend_from_slice(&length.to_le_bytes());
        buf.extend_from_slice(&record);

        self.fd.write_all(&buf).context("Failed to write record")?;
        self.fd
            .sync_all()
            .context("Failed to sync header and record")?;

        self.file_length += buf.len() as u64;
        Ok(offset)
    }

    pub fn read_record(&mut self, offset: &u64) -> Result<Vec<u8>> {
        // Seek to the specified offset
        self.fd
            .seek(std::io::SeekFrom::Start(*offset))
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

    pub fn seal(&mut self) -> Result<()> {
        if self.sealed {
            return Err(anyhow::anyhow!("Logfile already sealed"));
        }

        self.sealed = true;

        let mut footer = [0u8; 9];
        footer[0] = 1; // sealed flag
        footer[1..9].copy_from_slice(&MAGIC_NUMBER);

        self.fd
            .write_all(&footer)
            .context("Failed to write footer")?;
        self.fd.sync_all().context("Failed to sync footer")?;

        self.file_length += 9;
        Ok(())
    }

    pub fn reset_iter(&mut self) {
        self.iter_offset = 8;
    }

    pub fn iter_offset(&self) -> u64 {
        self.iter_offset
    }

    pub fn set_iter_offset(&mut self, offset: u64) {
        self.iter_offset = offset;
    }

    pub fn file_length(&self) -> u64 {
        self.file_length
    }
}

/// Iterator for the log file records
///
/// If an error occurs, the iterator will panic
impl Iterator for Logfile {
    type Item = Result<Vec<u8>>;

    fn next(&mut self) -> Option<Self::Item> {
        let offset = self.iter_offset;

        // If file is sealed, check if we've reached the footer
        if self.sealed {
            // Use tracked file length instead of querying filesystem
            if offset >= self.file_length - 9 {
                return None;
            }
        }

        // Try to read record at current offset
        match self.read_record(&offset) {
            Ok(record) => {
                // Update offset to point to next record
                // 20 = header size (16 bytes hash + 4 bytes length)
                self.iter_offset += 20 + record.len() as u64;
                Some(Ok(record))
            }
            Err(e) => {
                // If we hit EOF, return None to end iteration
                if let Some(err) = e.downcast_ref::<std::io::Error>() {
                    if err.kind() == std::io::ErrorKind::UnexpectedEof {
                        return None;
                    }
                }
                // For other errors, return the error
                Some(Err(e))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{fs::OpenOptions, path::PathBuf};

    use super::*;

    #[test]
    fn test_write_without_sealing() {
        let path = PathBuf::from("/tmp/01.log");
        let fd = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .unwrap();

        let mut logfile = Logfile::new("01", fd).unwrap();
        let offset = logfile.write_record(b"hello").unwrap();

        let mut logfile = Logfile::from_file(&path).unwrap();
        assert_eq!(logfile.id, "01");
        assert!(!logfile.sealed);
        assert_eq!(logfile.read_record(&offset).unwrap(), b"hello");

        // Clean up the test file
        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn test_write_with_sealing() {
        let path = PathBuf::from("/tmp/02.log");
        let fd = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .unwrap();

        let mut logfile = Logfile::new("02", fd).unwrap();
        let offset = logfile.write_record(b"hello").unwrap();
        logfile.seal().unwrap();

        let mut logfile = Logfile::from_file(&path).unwrap();
        assert_eq!(logfile.id, "02");
        assert!(logfile.sealed);
        assert_eq!(logfile.read_record(&offset).unwrap(), b"hello");

        // Clean up the test file
        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn test_corrupted_record() {
        let path = PathBuf::from("/tmp/03.log");
        let fd = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .unwrap();

        let mut logfile = Logfile::new("03", fd).unwrap();
        let offset = logfile.write_record(b"hello").unwrap();

        // Corrupt the record by modifying a single byte in the middle
        logfile
            .fd
            .seek(std::io::SeekFrom::Start(offset + 22)) // Skip past hash, length, and position to middle of "hello"
            .unwrap();
        logfile.fd.write_all(&[b'x']).unwrap(); // Replace one byte with 'x'
        logfile.fd.sync_all().unwrap();

        // Attempting to read the corrupted record should fail due to hash mismatch
        assert!(logfile.read_record(&offset).is_err());

        // Clean up the test file
        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn test_corrupted_record_sealed() {
        let path = PathBuf::from("/tmp/04.log");
        let fd = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .unwrap();

        let mut logfile = Logfile::new("04", fd).unwrap();
        let offset = logfile.write_record(b"hello").unwrap();
        logfile.seal().unwrap();

        // Corrupt the record by modifying a single byte in the middle
        logfile
            .fd
            .seek(std::io::SeekFrom::Start(offset + 22))
            .unwrap();
        logfile.fd.write_all(&[b'x']).unwrap();
        logfile.fd.sync_all().unwrap();

        // Attempting to read the corrupted record should fail due to hash mismatch
        assert!(logfile.read_record(&offset).is_err());

        // Clean up the test file
        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn test_corrupted_file_header() {
        let path = PathBuf::from("/tmp/05.log");
        let mut fd = File::create(&path).unwrap();

        // Write an invalid magic number
        let invalid_header = [0x00; 16];
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
        let path = PathBuf::from("/tmp/06.log");
        let fd = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .unwrap();

        // Create a new logfile and write 100 records
        let mut logfile = Logfile::new("06", fd).unwrap();
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
            let record = logfile.read_record(&offset).unwrap();
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

    #[test]
    fn test_iterator() {
        let path = PathBuf::from("/tmp/07.log");
        let fd = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .unwrap();

        let mut logfile = Logfile::new("07", fd).unwrap();

        // Add some records
        for i in 0..100 {
            logfile
                .write_record(format!("record_{}", i).as_bytes())
                .unwrap();
        }

        for i in 0..100 {
            let record = logfile.next().unwrap().unwrap();
            assert_eq!(String::from_utf8(record).unwrap(), format!("record_{}", i));
        }

        // Reset the iterator and verify that we can read the same records again
        logfile.reset_iter();
        for i in 0..100 {
            let record = logfile.next().unwrap().unwrap();
            assert_eq!(String::from_utf8(record).unwrap(), format!("record_{}", i));
        }

        // Verify that we've reached the end of the file
        assert!(logfile.next().is_none());

        // Clean up the test file
        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn test_write_magic_number_without_sealing_escape() {
        let path = PathBuf::from("/tmp/08.log");
        let fd = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .unwrap();

        let mut logfile = Logfile::new("08", fd).unwrap();
        let offset = logfile.write_record(&MAGIC_NUMBER).unwrap();

        let mut logfile = Logfile::from_file(&path).unwrap();
        assert_eq!(logfile.id, "08");
        assert!(!logfile.sealed);
        assert_eq!(logfile.read_record(&offset).unwrap(), &MAGIC_NUMBER);

        // Clean up the test file
        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn test_write_magic_number_sealing_escape() {
        let path = PathBuf::from("/tmp/09.log");
        let fd = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .unwrap();

        let mut logfile = Logfile::new("0   9", fd).unwrap();
        let offset = logfile.write_record(&MAGIC_NUMBER).unwrap();
        logfile.seal().unwrap();

        let mut logfile = Logfile::from_file(&path).unwrap();
        assert_eq!(logfile.id, "09");
        assert!(logfile.sealed);
        assert_eq!(logfile.read_record(&offset).unwrap(), &MAGIC_NUMBER);

        // Clean up the test file
        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn test_write_magic_number_without_sealing_escape_iterator() {
        let path = PathBuf::from("/tmp/10.log");
        let fd = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .unwrap();

        let mut logfile = Logfile::new("10", fd).unwrap();
        logfile.write_record(&MAGIC_NUMBER).unwrap();

        let record = logfile.next().unwrap().unwrap();
        assert_eq!(record, MAGIC_NUMBER);

        let mut logfile = Logfile::from_file(&path).unwrap();
        assert_eq!(logfile.id, "10");
        assert!(!logfile.sealed);

        let record = logfile.next().unwrap().unwrap();
        assert_eq!(record, MAGIC_NUMBER);

        // Clean up the test file
        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn test_write_magic_number_with_sealing_escape_iterator() {
        let path = PathBuf::from("/tmp/11.log");
        let fd = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .unwrap();

        let mut logfile = Logfile::new("11", fd).unwrap();
        let offset = logfile.write_record(&MAGIC_NUMBER).unwrap();

        let record = logfile.next().unwrap().unwrap();
        assert_eq!(record, MAGIC_NUMBER);

        logfile.seal().unwrap();
        logfile.set_iter_offset(offset);

        let record = logfile.next().unwrap().unwrap();
        assert_eq!(record, MAGIC_NUMBER);

        let mut logfile = Logfile::from_file(&path).unwrap();
        assert_eq!(logfile.id, "11");
        assert!(logfile.sealed);

        let record = logfile.next().unwrap().unwrap();
        assert_eq!(record, MAGIC_NUMBER);

        // Clean up the test file
        std::fs::remove_file(&path).unwrap();
    }
}
