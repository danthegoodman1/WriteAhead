use std::fs::{File, OpenOptions};

use tracing::instrument;

use super::{FileReader, FileWriter};

#[derive(Debug)]
pub struct SimpleFile {
    pub fd: File,
}

impl SimpleFile {
    pub fn new(fd: std::fs::File) -> anyhow::Result<Self> {
        Ok(SimpleFile { fd })
    }
}

impl FileWriter for SimpleFile {
    async fn open(path: &std::path::Path) -> anyhow::Result<Self> {
        let fd = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)
            .unwrap();
        Self::new(fd)
    }

    #[instrument(skip(self), level = "trace")]
    async fn read(&self, offset: u64, size: u64) -> anyhow::Result<Vec<u8>> {
        use std::io::{Read, Seek};

        let mut fd = &self.fd;
        fd.seek(std::io::SeekFrom::Start(offset))?;

        let mut buffer = vec![0u8; size as usize];
        fd.read_exact(&mut buffer)?;

        Ok(buffer)
    }

    #[instrument(skip(self, data), level = "trace")]
    fn write(&mut self, offset: u64, data: &[u8]) -> anyhow::Result<()> {
        use std::io::{Seek, Write};

        let mut fd = &self.fd;
        fd.seek(std::io::SeekFrom::Start(offset))?;
        fd.write_all(data)?;
        fd.sync_all()?;

        Ok(())
    }

    async fn file_length(&self) -> u64 {
        self.fd.metadata().unwrap().len()
    }
}

impl FileReader for SimpleFile {
    async fn open(path: &std::path::Path) -> anyhow::Result<Self> {
        let fd = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)
            .unwrap();
        Self::new(fd)
    }

    #[instrument(skip(self), level = "trace")]
    async fn read(&self, offset: u64, size: u64) -> anyhow::Result<Vec<u8>> {
        use std::io::{Read, Seek};

        let mut fd = &self.fd;
        fd.seek(std::io::SeekFrom::Start(offset))?;

        let mut buffer = vec![0u8; size as usize];
        fd.read_exact(&mut buffer)?;

        Ok(buffer)
    }

    async fn file_length(&self) -> u64 {
        self.fd.metadata().unwrap().len()
    }
}

#[cfg(test)]
mod tests {

    use crate::logfile;

    use super::*;

    use std::{fs::OpenOptions, path::PathBuf};

    #[tokio::test]
    async fn test_write_without_sealing() {
        let path = PathBuf::from("/tmp/01.log");

        logfile::tests::test_write_without_sealing::<SimpleFile, SimpleFile>(path).await;
    }

    #[tokio::test]
    async fn test_write_with_sealing() {
        let path = PathBuf::from("/tmp/02.log");
        logfile::tests::test_write_with_sealing::<SimpleFile, SimpleFile>(path).await;
    }

    #[tokio::test]
    async fn test_corrupted_record() {
        let path = PathBuf::from("/tmp/03.log");

        logfile::tests::test_corrupted_record::<SimpleFile, SimpleFile>(path).await;
    }

    #[tokio::test]
    async fn test_corrupted_record_sealed() {
        let path = PathBuf::from("/tmp/04.log");

        logfile::tests::test_corrupted_record_sealed::<SimpleFile, SimpleFile>(path).await;
    }

    // #[tokio::test]
    // async fn test_corrupted_file_header() {
    //     let path = PathBuf::from("/tmp/05.log");

    //     logfile::tests::test_corrupted_file_header::<SimpleFile>(path).await;
    // }

    #[tokio::test]
    async fn test_100_records() {
        let path = PathBuf::from("/tmp/06.log");

        logfile::tests::test_100_records::<SimpleFile, SimpleFile>(path).await;
    }

    #[tokio::test]
    async fn test_write_magic_number_without_sealing_escape() {
        let path = PathBuf::from("/tmp/08.log");

        logfile::tests::test_write_magic_number_without_sealing_escape::<SimpleFile, SimpleFile>(
            path,
        )
        .await;
    }

    #[tokio::test]
    async fn test_write_magic_number_sealing_escape() {
        let path = PathBuf::from("/tmp/09.log");

        logfile::tests::test_write_magic_number_sealing_escape::<SimpleFile, SimpleFile>(path)
            .await;
    }

    // #[tokio::test]
    // async fn test_stream() {
    //     let path = PathBuf::from("/tmp/07.log");

    //     logfile::tests::test_stream::<SimpleFile>(path).await;
    // }
}
