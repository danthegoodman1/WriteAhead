use anyhow::{Context, Result};
use std::path::Path;

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
    fd: F,
}

const MAGIC_NUMBER: [u8; 8] = [0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff];

impl<F: FileIO> Logfile<F> {
    /// Creates a new logfile, deleting any existing file at the given path.
    ///
    /// The id MUST be unique per file!
    pub fn new(id: &str, fd: F) -> Result<Self> {
        let mut header = [0u8; 8];
        header[0..8].copy_from_slice(&MAGIC_NUMBER);

        fd.write(0, &header).context("Failed to write header")?;

        Ok(Self {
            sealed: false,
            id: id.to_string(),
            fd,
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
        let fd = F::open(path)?;
        let file_length = fd.file_length();
        let id = Self::file_id_from_path(path)?;

        // Read the first 8 bytes of the file

        let buffer = fd
            .read(0, 8)
            .context("Failed to read first 8 bytes of logfile")?;

        // Check if the first 8 bytes are the magic number
        if buffer[0..8] != MAGIC_NUMBER {
            return Err(anyhow::anyhow!("Invalid start magic number"));
        }

        // Try to read the last 9 bytes (1 for sealed flag + 8 for magic number)
        let mut sealed = false;
        if file_length >= 17 {
            // 8 header + at least 9 footer
            let buffer = fd.read(file_length - 9, 9)?;
            if buffer[1..9] == MAGIC_NUMBER {
                sealed = buffer[0] == 1;
            }
        }

        Ok(Self {
            sealed,
            id,
            fd,
            file_length,
        })
    }

    pub fn write_record(&mut self, record: &[u8]) -> Result<u64> {
        if self.sealed {
            return Err(anyhow::anyhow!("Cannot write to sealed logfile"));
        }

        let offset = self.file_length;

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

        self.fd
            .write(offset, &buf)
            .context("Failed to write record")?;

        self.file_length += buf.len() as u64;
        Ok(offset)
    }

    pub fn read_record(&mut self, offset: &u64) -> Result<Vec<u8>> {
        // Read the hash and length (20 bytes total)
        let header = self.fd.read(*offset, 20)?;

        // Parse the header
        let hash1 = i64::from_le_bytes(header[0..8].try_into().unwrap());
        let hash2 = i64::from_le_bytes(header[8..16].try_into().unwrap());
        let length = u32::from_le_bytes(header[16..20].try_into().unwrap());

        // Read the actual record data
        let mut record = self.fd.read(*offset + 20, length as u64)?;

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
            .write(self.file_length, &footer)
            .context("Failed to write footer")?;

        self.file_length += 9;
        Ok(())
    }

    pub fn iter(&mut self) -> LogFileIterator<F> {
        LogFileIterator {
            logfile: self,
            offset: 8,
        }
    }

    pub fn file_length(&self) -> u64 {
        self.file_length
    }
}

pub struct LogFileIterator<'a, F: FileIO> {
    logfile: &'a mut Logfile<F>,
    offset: u64,
}

impl<'a, F: FileIO> LogFileIterator<'a, F> {
    pub fn reset_iter(&mut self) {
        self.offset = 8;
    }

    pub fn iter_offset(&self) -> u64 {
        self.offset
    }

    pub fn set_iter_offset(&mut self, offset: u64) {
        self.offset = offset;
    }
}

/// Iterator for the log file records
///
/// If an error occurs, the iterator will panic
impl<'a, F: FileIO> Iterator for LogFileIterator<'a, F> {
    type Item = Result<Vec<u8>>;

    fn next(&mut self) -> Option<Self::Item> {
        let offset = self.offset;

        // If file is sealed, check if we've reached the footer
        if self.logfile.sealed {
            if offset >= self.logfile.file_length - 9 {
                return None;
            }
        }

        // Try to read record at current offset
        match self.logfile.read_record(&offset) {
            Ok(record) => {
                self.offset += 20 + record.len() as u64;
                Some(Ok(record))
            }
            Err(e) => {
                if let Some(err) = e.downcast_ref::<std::io::Error>() {
                    if err.kind() == std::io::ErrorKind::UnexpectedEof {
                        return None;
                    }
                }
                Some(Err(e))
            }
        }
    }
}

#[cfg(test)]
pub mod tests {
    use std::{fs::OpenOptions, path::PathBuf};

    use super::*;

    pub fn test_write_without_sealing<F: FileIO>(f: F, path: PathBuf) {
        let mut logfile =
            Logfile::new(&Logfile::<F>::file_id_from_path(&path).unwrap(), f).unwrap();
        let offset = logfile.write_record(b"hello").unwrap();

        let mut logfile: Logfile<F> = Logfile::from_file(&path).unwrap();
        assert_eq!(logfile.id, "01");
        assert!(!logfile.sealed);
        assert_eq!(logfile.read_record(&offset).unwrap(), b"hello");

        // Clean up the test file
        std::fs::remove_file(&path).unwrap();
    }

    pub fn test_write_with_sealing<F: FileIO>(f: F, path: PathBuf) {
        let mut logfile =
            Logfile::new(&Logfile::<F>::file_id_from_path(&path).unwrap(), f).unwrap();
        let offset = logfile.write_record(b"hello").unwrap();
        logfile.seal().unwrap();

        let mut logfile: Logfile<F> = Logfile::from_file(&path).unwrap();
        assert_eq!(logfile.id, "02");
        assert!(logfile.sealed);
        assert_eq!(logfile.read_record(&offset).unwrap(), b"hello");

        // Clean up the test file
        std::fs::remove_file(&path).unwrap();
    }

    pub fn test_corrupted_record<F: FileIO>(f: F, path: PathBuf) {
        let mut logfile =
            Logfile::new(&Logfile::<F>::file_id_from_path(&path).unwrap(), f).unwrap();
        let offset = logfile.write_record(b"hello").unwrap();

        // Corrupt the record by modifying a single byte in the middle
        logfile.fd.write(offset + 22, &[b'x']).unwrap(); // Replace one byte with 'x'

        // Attempting to read the corrupted record should fail due to hash mismatch
        assert!(logfile.read_record(&offset).is_err());

        // Clean up the test file
        std::fs::remove_file(&path).unwrap();
    }

    pub fn test_corrupted_record_sealed<F: FileIO>(f: F, path: PathBuf) {
        let mut logfile =
            Logfile::new(&Logfile::<F>::file_id_from_path(&path).unwrap(), f).unwrap();
        let offset = logfile.write_record(b"hello").unwrap();
        logfile.seal().unwrap();

        // Corrupt the record by modifying a single byte in the middle
        logfile.fd.write(offset + 22, &[b'x']).unwrap();

        // Attempting to read the corrupted record should fail due to hash mismatch
        assert!(logfile.read_record(&offset).is_err());

        // Clean up the test file
        std::fs::remove_file(&path).unwrap();
    }

    pub fn test_corrupted_file_header<F: FileIO>(f: F, path: PathBuf) {
        // Write an invalid magic number
        let invalid_header = [0x00; 16];
        f.write(0, &invalid_header).unwrap();

        // Attempting to open the file with invalid header should fail
        let result: Result<Logfile<F>> = Logfile::from_file(&path);
        assert!(result.is_err());
        assert!(result
            .err()
            .unwrap()
            .to_string()
            .contains("Invalid start magic number"));

        // Clean up the test file
        std::fs::remove_file(&path).unwrap();
    }

    pub fn test_100_records<F: FileIO>(f: F, path: PathBuf) {
        // Create a new logfile and write 100 records
        let mut logfile =
            Logfile::new(&Logfile::<F>::file_id_from_path(&path).unwrap(), f).unwrap();
        let mut offsets = Vec::with_capacity(100);

        for i in 0..100 {
            let record = format!("record_{}", i);
            let offset = logfile.write_record(record.as_bytes()).unwrap();
            offsets.push((offset, record));
        }

        // Seal the file
        logfile.seal().unwrap();

        // Reopen the file and verify all records
        let mut logfile: Logfile<F> = Logfile::from_file(&path).unwrap();
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

    pub fn test_iterator<F: FileIO>(f: F, path: PathBuf) {
        let mut logfile =
            Logfile::new(&Logfile::<F>::file_id_from_path(&path).unwrap(), f).unwrap();

        // Add some records
        for i in 0..100 {
            logfile
                .write_record(format!("record_{}", i).as_bytes())
                .unwrap();
        }

        // First iteration
        {
            let mut iter = logfile.iter();
            for i in 0..100 {
                let record = iter.next().unwrap().unwrap();
                assert_eq!(String::from_utf8(record).unwrap(), format!("record_{}", i));
            }
            assert!(iter.next().is_none());
        }

        // Second iteration - verify we can create a new iterator and read again
        {
            let mut iter = logfile.iter();
            for i in 0..100 {
                let record = iter.next().unwrap().unwrap();
                assert_eq!(String::from_utf8(record).unwrap(), format!("record_{}", i));
            }
            assert!(iter.next().is_none());
        }
    }

    pub fn test_write_magic_number_without_sealing_escape<F: FileIO>(f: F, path: PathBuf) {
        let mut logfile =
            Logfile::new(&Logfile::<F>::file_id_from_path(&path).unwrap(), f).unwrap();
        let offset = logfile.write_record(&MAGIC_NUMBER).unwrap();

        let mut logfile: Logfile<F> = Logfile::from_file(&path).unwrap();
        assert_eq!(logfile.id, "08");
        assert!(!logfile.sealed);
        assert_eq!(logfile.read_record(&offset).unwrap(), &MAGIC_NUMBER);

        // Clean up the test file
        std::fs::remove_file(&path).unwrap();
    }

    pub fn test_write_magic_number_sealing_escape<F: FileIO>(f: F, path: PathBuf) {
        let mut logfile =
            Logfile::new(&Logfile::<F>::file_id_from_path(&path).unwrap(), f).unwrap();
        let offset = logfile.write_record(&MAGIC_NUMBER).unwrap();
        logfile.seal().unwrap();

        let mut logfile: Logfile<F> = Logfile::from_file(&path).unwrap();
        assert_eq!(logfile.id, "09");
        assert!(logfile.sealed);
        assert_eq!(logfile.read_record(&offset).unwrap(), &MAGIC_NUMBER);

        // Clean up the test file
        std::fs::remove_file(&path).unwrap();
    }

    pub fn test_write_magic_number_without_sealing_escape_iterator<F: FileIO>(f: F, path: PathBuf) {
        let mut logfile =
            Logfile::new(&Logfile::<F>::file_id_from_path(&path).unwrap(), f).unwrap();
        logfile.write_record(&MAGIC_NUMBER).unwrap();

        let mut iter = logfile.iter();
        let record = iter.next().unwrap().unwrap();
        assert_eq!(record, MAGIC_NUMBER);
        assert!(iter.next().is_none());

        let mut logfile: Logfile<F> = Logfile::from_file(&path).unwrap();
        assert_eq!(logfile.id, "10");
        assert!(!logfile.sealed);

        let mut iter = logfile.iter();
        let record = iter.next().unwrap().unwrap();
        assert_eq!(record, MAGIC_NUMBER);
        assert!(iter.next().is_none());

        // Clean up the test file
        std::fs::remove_file(&path).unwrap();
    }

    pub fn test_write_magic_number_with_sealing_escape_iterator<F: FileIO>(f: F, path: PathBuf) {
        let mut logfile =
            Logfile::new(&Logfile::<F>::file_id_from_path(&path).unwrap(), f).unwrap();
        logfile.write_record(&MAGIC_NUMBER).unwrap();

        // First iteration
        {
            let mut iter = logfile.iter();
            let record = iter.next().unwrap().unwrap();
            assert_eq!(record, MAGIC_NUMBER);
            assert!(iter.next().is_none());
        }

        // Seal the file
        logfile.seal().unwrap();

        // Second iteration after sealing
        {
            let mut iter = logfile.iter();
            let record = iter.next().unwrap().unwrap();
            assert_eq!(record, MAGIC_NUMBER);
            assert!(iter.next().is_none());
        }

        // Verify with a fresh file handle
        let mut logfile: Logfile<F> = Logfile::from_file(&path).unwrap();
        assert_eq!(logfile.id, "11");
        assert!(logfile.sealed);

        let mut iter = logfile.iter();
        let record = iter.next().unwrap().unwrap();
        assert_eq!(record, MAGIC_NUMBER);
        assert!(iter.next().is_none());

        // Clean up the test file
        std::fs::remove_file(&path).unwrap();
    }
}
