use std::fs::File;

use super::FileIO;

pub struct SimpleFile {
    pub fd: File,
}

impl SimpleFile {
    pub fn new(fd: std::fs::File) -> anyhow::Result<Self> {
        Ok(SimpleFile { fd })
    }
}

impl FileIO for SimpleFile {
    fn open(path: &std::path::Path) -> anyhow::Result<Self> {
        let fd = File::open(path)?;
        Self::new(fd)
    }

    fn read(&self, offset: u64, size: u64) -> anyhow::Result<Vec<u8>> {
        use std::io::{Read, Seek};

        let mut fd = &self.fd;
        fd.seek(std::io::SeekFrom::Start(offset))?;

        let mut buffer = vec![0u8; size as usize];
        fd.read_exact(&mut buffer)?;

        Ok(buffer)
    }

    fn write(&self, offset: u64, data: &[u8]) -> anyhow::Result<()> {
        use std::io::{Seek, Write};

        let mut fd = &self.fd;
        fd.seek(std::io::SeekFrom::Start(offset))?;
        fd.write_all(data)?;
        fd.sync_all()?;

        Ok(())
    }

    fn file_length(&self) -> u64 {
        self.fd.metadata().unwrap().len()
    }
}

#[cfg(test)]
mod tests {
    use crate::logfile::{self, Logfile};

    use super::*;

    use std::{fs::OpenOptions, path::PathBuf};

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
        let f = SimpleFile::new(fd).unwrap();

        logfile::tests::test_write_without_sealing(f, path);
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
        let f = SimpleFile::new(fd).unwrap();

        logfile::tests::test_write_with_sealing(f, path);
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
        let f = SimpleFile::new(fd).unwrap();

        logfile::tests::test_corrupted_record(f, path);
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
        let f = SimpleFile::new(fd).unwrap();

        logfile::tests::test_corrupted_record_sealed(f, path);
    }

    #[test]
    fn test_corrupted_file_header() {
        let path = PathBuf::from("/tmp/05.log");
        let fd = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .unwrap();
        let f = SimpleFile::new(fd).unwrap();

        logfile::tests::test_corrupted_file_header(f, path);
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
        let f = SimpleFile::new(fd).unwrap();

        logfile::tests::test_100_records(f, path);
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
        let f = SimpleFile::new(fd).unwrap();

        logfile::tests::test_iterator(f, path);
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
        let f = SimpleFile::new(fd).unwrap();

        logfile::tests::test_write_magic_number_without_sealing_escape(f, path);
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
        let f = SimpleFile::new(fd).unwrap();

        logfile::tests::test_write_magic_number_sealing_escape(f, path);
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
        let f = SimpleFile::new(fd).unwrap();

        logfile::tests::test_write_magic_number_without_sealing_escape_iterator(f, path);
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
        let f = SimpleFile::new(fd).unwrap();

        logfile::tests::test_write_magic_number_with_sealing_escape_iterator(f, path);
    }
}
