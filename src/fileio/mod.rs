use std::path::Path;

use anyhow::Result;

pub trait FileIO {
    fn new(fd: std::fs::File) -> Result<Box<Self>>;
    fn open(path: &Path) -> Result<Box<Self>>;
    fn read(&self, offset: u64, size: u64) -> Result<Vec<u8>>;
    fn write(&self, offset: u64, data: &[u8]) -> Result<()>;
}

pub mod io_uring;
pub mod simple_file;
