use std::path::Path;

use anyhow::Result;
use std::future::Future;

/// A trait for file operations.
///
/// # Why
///
/// Abstracting this to a trait has a few benefits:
///
/// 1. Can easily integrate into any storage system, like io_uring for linux
/// 2. Allows developers to create custom implementations with various use-case specific optimizations such as pre-fetching for iterators and batching
/// 3. Allows for customization like write-through caching, or prefetching for iterators
pub trait FileWriter
where
    Self: Sized + Send + Sync + std::fmt::Debug,
{
    fn open(path: &Path) -> impl Future<Output = Result<Self>> + Send;
    fn write(&mut self, offset: u64, data: &[u8]) -> Result<()>;
    fn file_length(&self) -> u64;
}

pub trait FileReader
where
    Self: Sized + Send + Sync + std::fmt::Debug,
{
    fn open(path: &Path) -> impl Future<Output = Result<Self>> + Send;
    fn read(&self, offset: u64, size: u64) -> impl Future<Output = Result<Vec<u8>>> + Send;
    fn file_length(&self) -> u64;
}

pub mod io_uring;
pub mod simple_file;
