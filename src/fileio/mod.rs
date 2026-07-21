use std::path::Path;

use anyhow::Result;

/// Positional, synchronous file IO.
///
/// # Why a trait
///
/// 1. Lets tests substitute fault-injecting or in-memory implementations.
/// 2. Lets integrators add use-case specific behavior (write-through caching,
///    custom placement, etc.) without forking the crate.
///
/// All reads and writes are positional (pread/pwrite style) so a single
/// handle can serve concurrent readers without a shared cursor. `write_at`
/// does not flush: callers that need durability must call `sync`, which is
/// what lets the log writer batch many writes under one fsync.
#[allow(clippy::len_without_is_empty)]
pub trait FileIo
where
    Self: Sized + Send + Sync + std::fmt::Debug,
{
    /// Opens a file for writing, creating it if missing.
    fn open(path: &Path) -> Result<Self>;
    /// Opens an existing file; errors if it does not exist. Read and
    /// recovery paths use this so they can never create stray files.
    fn open_existing(path: &Path) -> Result<Self>;
    /// Read exactly `buf.len()` bytes at `offset`. Errors if the file is too short.
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<()>;
    /// Write all of `data` at `offset`. Durability requires a subsequent `sync`.
    fn write_at(&mut self, offset: u64, data: &[u8]) -> Result<()>;
    /// Flush written data (and the metadata needed to read it back) to disk.
    fn sync(&mut self) -> Result<()>;
    fn len(&self) -> Result<u64>;
    /// Changes the physical file length. The writer uses growth as sparse
    /// preallocation and shrinkage to remove allocation tails during recovery
    /// and sealing.
    fn set_len(&mut self, len: u64) -> Result<()>;
}

pub mod simple_file;

/// fsyncs a directory so newly created/removed entries survive a crash.
pub(crate) fn sync_dir(dir: &Path) -> Result<()> {
    use anyhow::Context;
    std::fs::File::open(dir)
        .and_then(|f| f.sync_all())
        .with_context(|| format!("Failed to fsync directory {}", dir.display()))?;
    Ok(())
}
