use std::path::Path;

use anyhow::Result;

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
