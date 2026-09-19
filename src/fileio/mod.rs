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

/// Locks the directory inode, so aliases of a path share the same lock and
/// no lockfile needs to be created, removed, or recovered. The writer owns
/// the returned handle until it has stopped mutating the directory.
pub(crate) fn lock_dir(dir: &Path) -> Result<std::fs::File> {
    use crate::write_ahead::WriteAheadError;
    let lock = std::fs::File::open(dir)?;
    match lock.try_lock() {
        Ok(()) => Ok(lock),
        Err(std::fs::TryLockError::WouldBlock) => Err(WriteAheadError::DirectoryLocked.into()),
        Err(std::fs::TryLockError::Error(e)) => Err(e.into()),
    }
}

/// Persist the entries that make the log directory reachable. Sync existing
/// ancestors too: a retry after an interrupted creation cannot tell which
/// entries have already reached disk. This runs only at startup.
pub(crate) fn sync_parents(dir: &Path) -> Result<()> {
    let absolute = dir.canonicalize()?;
    for parent in absolute.ancestors().skip(1) {
        sync_dir(parent)?;
    }
    Ok(())
}

/// fsyncs a directory so newly created/removed entries survive a crash.
pub(crate) fn sync_dir(dir: &Path) -> Result<()> {
    use anyhow::Context;
    #[cfg(test)]
    tests::before_sync(dir)?;
    std::fs::File::open(dir)
        .and_then(|f| f.sync_all())
        .with_context(|| format!("Failed to fsync directory {}", dir.display()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{SimpleFile, WriteAhead, WriteAheadOptions};
    use std::{cell::RefCell, path::PathBuf};

    #[derive(Default)]
    struct SyncProbe {
        calls: Vec<PathBuf>,
        fail: Option<PathBuf>,
    }

    thread_local! {
        static PROBE: RefCell<Option<SyncProbe>> = const { RefCell::new(None) };
    }

    pub(super) fn before_sync(dir: &Path) -> Result<()> {
        PROBE.with(|probe| {
            if let Some(probe) = probe.borrow_mut().as_mut() {
                probe.calls.push(dir.to_path_buf());
                if probe.fail.as_deref() == Some(dir) {
                    probe.fail = None;
                    return Err(std::io::Error::from_raw_os_error(5).into());
                }
            }
            Ok(())
        })
    }

    #[test]
    fn startup_retries_parent_and_wal_directory_barriers() {
        for fail_on_parent in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().canonicalize().unwrap().join("new-parent/wal");
            let fail = if fail_on_parent {
                path.parent().unwrap().to_path_buf()
            } else {
                path.clone()
            };
            PROBE.with(|p| {
                *p.borrow_mut() = Some(SyncProbe {
                    fail: Some(fail.clone()),
                    ..Default::default()
                })
            });
            let mut wal = WriteAhead::<SimpleFile>::with_options(WriteAheadOptions {
                log_dir: path.clone(),
                preallocation_chunk_size: None,
                ..Default::default()
            });
            assert!(wal.start().is_err());
            assert!(wal.writer().is_err());
            let file = path.join("0000000000.log");
            if fail_on_parent {
                assert!(!file.exists());
            } else {
                assert_eq!(
                    std::fs::metadata(&file).unwrap().len(),
                    crate::logfile::FILE_HEADER_SIZE
                );
            }
            wal.start().unwrap();
            let calls = PROBE.with(|p| p.borrow_mut().take().unwrap().calls);
            let parents: Vec<_> = path
                .canonicalize()
                .unwrap()
                .ancestors()
                .skip(1)
                .map(Path::to_path_buf)
                .collect();
            let retry_start = calls.iter().position(|p| p == &fail).unwrap() + 1;
            assert_eq!(&calls[retry_start..calls.len() - 1], parents);
            assert_eq!(calls.last().unwrap(), &path);
            let id =
                futures::executor::block_on(wal.write_batch(vec![b"durable".to_vec()])).unwrap()[0];
            drop(wal);
            let mut reopened = WriteAhead::<SimpleFile>::with_options(WriteAheadOptions {
                log_dir: path,
                ..Default::default()
            });
            reopened.start().unwrap();
            assert_eq!(
                reopened.read(id.file_id, id.file_offset).unwrap(),
                b"durable"
            );
        }
    }
}
