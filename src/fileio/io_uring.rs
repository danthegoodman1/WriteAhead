#[cfg(target_os = "linux")]
mod linux_impl {
    use crate::fileio::FileIO;

    use io_uring::{opcode, IoUring};
    use once_cell::sync::OnceCell;
    use std::os::unix::io::AsRawFd;
    use std::path::Path;
    use std::sync::Arc;
    use tokio::sync::Mutex;
    use tracing::instrument;

    /// Gloal ring for use with [crate::fileio::FileIO::open]
    pub static GLOBAL_RING: OnceCell<Arc<Mutex<IoUring>>> = OnceCell::new();

    pub struct IOUringFile {
        fd: Option<std::fs::File>,
        ring: Arc<Mutex<IoUring>>,
    }

    impl std::fmt::Debug for IOUringFile {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "IOUringFile {{ fd: {:?} }}", self.fd)
        }
    }

    impl IOUringFile {
        pub fn new(
            device_path: &Path,
            ring: Arc<tokio::sync::Mutex<IoUring>>,
        ) -> std::io::Result<Self> {
            let fd = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(device_path)?;

            Ok(Self { fd: Some(fd), ring })
        }

        /// Reads a block from the device into the given buffer.
        #[instrument(skip(self, buffer), level = "trace")]
        pub async fn read_block(&self, offset: u64, buffer: &mut [u8]) -> std::io::Result<()> {
            let fd = io_uring::types::Fd(self.fd.as_ref().unwrap().as_raw_fd());

            let read_e = opcode::Read::new(fd, buffer.as_mut_ptr(), buffer.len() as _)
                .offset(offset)
                .build()
                .user_data(0x42);

            // Lock the ring for this operation
            let mut ring = self.ring.lock().await;

            unsafe {
                ring.submission()
                    .push(&read_e)
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
            }

            ring.submit_and_wait(1)?;

            // Process completion
            while let Some(cqe) = ring.completion().next() {
                if cqe.result() < 0 {
                    return Err(std::io::Error::from_raw_os_error(-cqe.result()));
                }
            }

            Ok(())
        }

        /// Writes multiple blocks to the device in a single submission
        #[instrument(skip(self, data), level = "trace")]
        pub async fn write_data(&self, offset: u64, data: &[u8]) -> std::io::Result<()> {
            let fd = io_uring::types::Fd(self.fd.as_ref().unwrap().as_raw_fd());

            let write_e = opcode::Write::new(fd, data.as_ptr(), data.len() as _)
                .offset(offset)
                .build()
                .user_data(0x43);

            // Lock the ring for this operation
            let mut ring = self.ring.lock().await;

            unsafe {
                ring.submission()
                    .push(&write_e)
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
            }

            ring.submit_and_wait(1)?;

            // Process completion
            while let Some(cqe) = ring.completion().next() {
                if cqe.result() < 0 {
                    return Err(std::io::Error::from_raw_os_error(-cqe.result()));
                }
            }

            Ok(())
        }
    }

    impl Drop for IOUringFile {
        fn drop(&mut self) {
            if let Some(fd) = self.fd.take() {
                drop(fd);
            }
        }
    }

    use anyhow::Result;

    impl FileIO for IOUringFile {
        async fn open(path: &Path) -> Result<Self> {
            // Check if the once cell is already initialized
            match GLOBAL_RING.get() {
                Some(ring) => Ok(Self::new(path, ring.clone())?),
                None => Err(anyhow::anyhow!("Global ring not initialized, initialize")),
            }
        }

        async fn read(&self, offset: u64, size: u64) -> anyhow::Result<Vec<u8>> {
            let mut buffer = vec![0u8; size as usize];
            self.read_block(offset, &mut buffer).await?;
            Ok(buffer)
        }

        #[instrument(skip(self, data), level = "trace")]
        async fn write(&mut self, offset: u64, data: &[u8]) -> anyhow::Result<()> {
            self.write_data(offset, data).await?;
            Ok(())
        }

        async fn file_length(&self) -> u64 {
            self.fd
                .as_ref()
                .and_then(|f| f.metadata().ok())
                .map(|m| m.len())
                .unwrap_or(0)
        }
    }
}

#[cfg(target_os = "linux")]
pub use linux_impl::*;

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use io_uring::IoUring;
    use linux_impl::{IOUringFile, GLOBAL_RING};
    use std::sync::Mutex as StdMutex;
    use tokio::sync::Mutex;

    static TEST_MUTEX: StdMutex<()> = StdMutex::new(());

    use crate::logfile;

    use super::*;
    use std::{
        path::{Path, PathBuf},
        sync::Arc,
    };

    const BLOCK_SIZE: usize = 4096;

    #[tokio::test]
    async fn test_io_uring_read_write() -> Result<(), Box<dyn std::error::Error>> {
        // Create a shared io_uring instance
        // Create a shared io_uring instance
        let ring = Arc::new(Mutex::new(IoUring::new(128)?));

        // Create a temporary file path
        let temp_file = tempfile::NamedTempFile::new()?;
        let temp_path = temp_file.path().to_str().unwrap();

        // Create a new device instance
        let device = IOUringFile::new(&Path::new(temp_path), ring)?;

        // Test data
        let mut write_data = [0u8; BLOCK_SIZE];
        let hello = b"Hello, world!\n";
        write_data[..hello.len()].copy_from_slice(hello);

        // Write test
        device.write_data(0, &write_data).await?;

        // Read test
        let mut read_buffer = [0u8; BLOCK_SIZE];
        device.read_block(0, &mut read_buffer).await?;

        // Verify the contents
        assert_eq!(&read_buffer[..hello.len()], hello);
        println!("Read data: {:?}", &read_buffer[..hello.len()]);
        // As a string
        println!(
            "Read data (string): {}",
            String::from_utf8_lossy(&read_buffer[..hello.len()])
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_io_uring_sqpoll() -> Result<(), Box<dyn std::error::Error>> {
        // Create a shared io_uring instance with SQPOLL enabled
        let ring = Arc::new(Mutex::new(
            IoUring::builder()
                .setup_sqpoll(2000) // 2000ms timeout
                .build(128)?,
        ));

        // Create a temporary file path
        let temp_file = tempfile::NamedTempFile::new()?;
        let temp_path = temp_file.path().to_str().unwrap();

        // Create a new device instance
        let device = IOUringFile::new(&Path::new(temp_path), ring)?;

        // Test data
        let mut write_data = [0u8; BLOCK_SIZE];
        let test_data = b"Testing SQPOLL mode!\n";
        write_data[..test_data.len()].copy_from_slice(test_data);

        // Write test
        device.write_data(0, &write_data).await?;

        // Read test
        let mut read_buffer = [0u8; BLOCK_SIZE];
        device.read_block(0, &mut read_buffer).await?;

        // Verify the contents
        assert_eq!(&read_buffer[..test_data.len()], test_data);

        Ok(())
    }

    #[tokio::test]
    async fn test_write_without_sealing() {
        let _guard = TEST_MUTEX.lock().unwrap();
        if GLOBAL_RING.get().is_none() {
            let _ = GLOBAL_RING.set(Arc::new(Mutex::new(IoUring::new(128).unwrap())));
        }

        let path = PathBuf::from("/tmp/01.log");

        logfile::tests::test_write_without_sealing::<IOUringFile>(path).await;
    }

    #[tokio::test]
    async fn test_write_with_sealing() {
        let _guard = TEST_MUTEX.lock().unwrap();
        if GLOBAL_RING.get().is_none() {
            let _ = GLOBAL_RING.set(Arc::new(Mutex::new(IoUring::new(128).unwrap())));
        }

        let path = PathBuf::from("/tmp/02.log");

        logfile::tests::test_write_with_sealing::<IOUringFile>(path).await;
    }

    #[tokio::test]
    async fn test_corrupted_record() {
        let _guard = TEST_MUTEX.lock().unwrap();
        if GLOBAL_RING.get().is_none() {
            let _ = GLOBAL_RING.set(Arc::new(Mutex::new(IoUring::new(128).unwrap())));
        }

        let path = PathBuf::from("/tmp/03.log");

        logfile::tests::test_corrupted_record::<IOUringFile>(path).await;
    }

    #[tokio::test]
    async fn test_corrupted_record_sealed() {
        let _guard = TEST_MUTEX.lock().unwrap();
        if GLOBAL_RING.get().is_none() {
            let _ = GLOBAL_RING.set(Arc::new(Mutex::new(IoUring::new(128).unwrap())));
        }

        let path = PathBuf::from("/tmp/04.log");

        logfile::tests::test_corrupted_record_sealed::<IOUringFile>(path).await;
    }

    #[tokio::test]
    async fn test_corrupted_file_header() {
        let _guard = TEST_MUTEX.lock().unwrap();
        if GLOBAL_RING.get().is_none() {
            let _ = GLOBAL_RING.set(Arc::new(Mutex::new(IoUring::new(128).unwrap())));
        }

        let path = PathBuf::from("/tmp/05.log");

        logfile::tests::test_corrupted_file_header::<IOUringFile>(path).await;
    }

    #[tokio::test]
    async fn test_100_records() {
        let _guard = TEST_MUTEX.lock().unwrap();
        if GLOBAL_RING.get().is_none() {
            let _ = GLOBAL_RING.set(Arc::new(Mutex::new(IoUring::new(128).unwrap())));
        }

        let path = PathBuf::from("/tmp/06.log");

        logfile::tests::test_100_records::<IOUringFile>(path).await;
    }

    // FIXME
    #[tokio::test]
    async fn test_stream() {
        let _guard = TEST_MUTEX.lock().unwrap();
        if GLOBAL_RING.get().is_none() {
            let _ = GLOBAL_RING.set(Arc::new(Mutex::new(IoUring::new(128).unwrap())));
        }

        let path = PathBuf::from("/tmp/07.log");

        logfile::tests::test_stream::<IOUringFile>(path).await;
    }

    #[tokio::test]
    async fn test_write_magic_number_without_sealing_escape() {
        let _guard = TEST_MUTEX.lock().unwrap();
        if GLOBAL_RING.get().is_none() {
            let _ = GLOBAL_RING.set(Arc::new(Mutex::new(IoUring::new(128).unwrap())));
        }

        let path = PathBuf::from("/tmp/08.log");

        logfile::tests::test_write_magic_number_without_sealing_escape::<IOUringFile>(path).await;
    }

    #[tokio::test]
    async fn test_write_magic_number_sealing_escape() {
        let _guard = TEST_MUTEX.lock().unwrap();
        if GLOBAL_RING.get().is_none() {
            let _ = GLOBAL_RING.set(Arc::new(Mutex::new(IoUring::new(128).unwrap())));
        }

        let path = PathBuf::from("/tmp/09.log");

        logfile::tests::test_write_magic_number_sealing_escape::<IOUringFile>(path).await;
    }

    // FIXME
    #[tokio::test]
    async fn test_write_magic_number_without_sealing_escape_iterator() {
        let _guard = TEST_MUTEX.lock().unwrap();
        if GLOBAL_RING.get().is_none() {
            let _ = GLOBAL_RING.set(Arc::new(Mutex::new(IoUring::new(128).unwrap())));
        }

        let path = PathBuf::from("/tmp/10.log");

        logfile::tests::test_write_magic_number_without_sealing_escape_iterator::<IOUringFile>(
            path,
        )
        .await;
    }

    #[tokio::test]
    async fn test_write_magic_number_with_sealing_escape_iterator() {
        let _guard = TEST_MUTEX.lock().unwrap();
        if GLOBAL_RING.get().is_none() {
            let _ = GLOBAL_RING.set(Arc::new(Mutex::new(IoUring::new(128).unwrap())));
        }

        let path = PathBuf::from("/tmp/11.log");

        logfile::tests::test_write_magic_number_with_sealing_escape_iterator::<IOUringFile>(path)
            .await;
    }

    #[tokio::test]
    async fn test_write_too_large_record() {
        let _guard = TEST_MUTEX.lock().unwrap();
        if GLOBAL_RING.get().is_none() {
            let _ = GLOBAL_RING.set(Arc::new(Mutex::new(IoUring::new(128).unwrap())));
        }

        let path = PathBuf::from("/tmp/12.log");

        logfile::tests::test_write_too_large_record::<IOUringFile>(path).await;
    }
}
