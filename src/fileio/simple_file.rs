use std::fs::{File, OpenOptions};
use std::os::unix::fs::FileExt;
use std::path::Path;

use anyhow::{Context, Result};

use super::FileIo;

/// Buffered-by-the-page-cache file IO using positional reads and writes.
///
/// For WAL access patterns (readers mostly reading recently written data)
/// the page cache consistently beats direct-IO approaches like io_uring
/// with O_DIRECT, so this is the default and recommended implementation.
#[derive(Debug)]
pub struct SimpleFile {
    fd: File,
}

impl FileIo for SimpleFile {
    fn open(path: &Path) -> Result<Self> {
        let fd = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)
            .with_context(|| format!("Failed to open {}", path.display()))?;
        Ok(Self { fd })
    }

    fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<()> {
        self.fd
            .read_exact_at(buf, offset)
            .with_context(|| format!("Failed to read {} bytes at offset {}", buf.len(), offset))?;
        Ok(())
    }

    fn write_at(&mut self, offset: u64, data: &[u8]) -> Result<()> {
        self.fd.write_all_at(data, offset).with_context(|| {
            format!("Failed to write {} bytes at offset {}", data.len(), offset)
        })?;
        Ok(())
    }

    fn sync(&mut self) -> Result<()> {
        // fdatasync: flushes data and the metadata needed to read it back
        // (including file size), skipping timestamp-only metadata updates.
        self.fd.sync_data().context("Failed to fdatasync")?;
        Ok(())
    }

    fn len(&self) -> Result<u64> {
        Ok(self.fd.metadata().context("Failed to stat file")?.len())
    }

    fn set_len(&mut self, len: u64) -> Result<()> {
        self.fd
            .set_len(len)
            .with_context(|| format!("Failed to truncate file to {}", len))?;
        Ok(())
    }
}
