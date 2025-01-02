use std::path::Path;

use anyhow::Result;

/// A trait for file operations.
///
/// # Why
///
/// Abstracting this to a trait has a few benefits:
///
/// 1. Can easily integrate into any storage system
/// 2. Allows developers to create custom implementations with various use-case specific optimizations such as pre-fetching for iterators and batching
/// 3. Allows for platform-specific storage systems, like io_uring for linux
pub trait FileIO
where
    Self: Sized,
{
    fn new(fd: std::fs::File) -> Result<Self>;
    fn open(path: &Path) -> Result<Self>;
    fn read(&self, offset: u64, size: u64) -> Result<Vec<u8>>;
    fn write(&self, offset: u64, data: &[u8]) -> Result<()>;
    fn file_length(&self) -> u64;
}

pub mod io_uring;
pub mod simple_file;
