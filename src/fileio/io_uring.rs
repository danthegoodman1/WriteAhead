#[cfg(target_os = "linux")]
mod linux_impl {
    use crate::fileio::{FileReader, FileWriter};

    use io_uring::IoUring;
    use io_uring_actor::io_uring::IOUringAPI;
    use std::os::unix::fs::OpenOptionsExt;
    use std::path::Path;
    use tracing::instrument;

    pub struct IOUringFile<const BLOCK_SIZE: usize> {
        api: IOUringAPI<BLOCK_SIZE>,
    }

    impl<const BLOCK_SIZE: usize> std::fmt::Debug for IOUringFile<BLOCK_SIZE> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "IOUringFile")
        }
    }

    impl<const BLOCK_SIZE: usize> IOUringFile<BLOCK_SIZE> {
        pub async fn new(device_path: &Path) -> std::io::Result<Self> {
            let fd = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .custom_flags(libc::O_DSYNC)
                .open(device_path)?;

            let ring = IoUring::new(128)?;

            let api = IOUringAPI::new(fd, ring, 128).await?;

            Ok(Self { api })
        }
    }

    use anyhow::Result;

    impl<const BLOCK_SIZE: usize> FileReader for IOUringFile<BLOCK_SIZE> {
        async fn open(path: &Path) -> Result<Self> {
            // Already opened, just return
            let file = Self::new(path).await?;
            Ok(file)
        }

        async fn read(&self, offset: u64, size: u64) -> anyhow::Result<Vec<u8>> {
            let buffer = self.api.read(offset, size as usize).await?;
            Ok(buffer)
        }

        fn file_length(&self) -> Result<u64, anyhow::Error> {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;

            // Call the asynchronous connect method using the runtime.
            let metadata = rt.block_on(self.api.get_metadata())?;
            Ok(metadata.stx_size)
        }
    }

    impl<const BLOCK_SIZE: usize> FileWriter for IOUringFile<BLOCK_SIZE> {
        async fn open(path: &Path) -> Result<Self> {
            // Already opened, just return
            let file = Self::new(path).await?;
            Ok(file)
        }

        async fn write(&mut self, offset: u64, data: &[u8]) -> Result<(), anyhow::Error> {
            self.api.write(offset, data.to_vec()).await?;
            Ok(())
        }

        fn file_length(&self) -> Result<u64, anyhow::Error> {
            println!("file_length");
            // FIXME: blocking here
            let rt = tokio::runtime::Handle::current();
            println!("got rt");

            // Call the asynchronous connect method using the runtime.
            let metadata = rt.block_on(self.api.get_metadata())?;
            println!("got file length");
            Ok(metadata.stx_size)
        }
    }
}

#[cfg(target_os = "linux")]
pub use linux_impl::*;

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use io_uring::IoUring;
    use linux_impl::IOUringFile;
    use std::sync::Mutex as StdMutex;
    use tokio::sync::Mutex;

    static TEST_MUTEX: StdMutex<()> = StdMutex::new(());

    use crate::{fileio::simple_file::SimpleFile, logfile};

    use super::*;
    use std::path::PathBuf;

    const BLOCK_SIZE: usize = 4096;

    #[tokio::test]
    async fn test_write_without_sealing() {
        let _guard = TEST_MUTEX.lock().unwrap();

        let path = PathBuf::from("/tmp/01.log");

        logfile::tests::test_write_without_sealing::<
            IOUringFile<BLOCK_SIZE>,
            IOUringFile<BLOCK_SIZE>,
        >(path)
        .await;
    }

    #[tokio::test]
    async fn test_write_with_sealing() {
        let _guard = TEST_MUTEX.lock().unwrap();

        let path = PathBuf::from("/tmp/02.log");

        logfile::tests::test_write_with_sealing::<SimpleFile, IOUringFile<BLOCK_SIZE>>(path).await;
    }

    #[tokio::test]
    async fn test_corrupted_record() {
        let _guard = TEST_MUTEX.lock().unwrap();

        let path = PathBuf::from("/tmp/03.log");

        logfile::tests::test_corrupted_record::<SimpleFile, IOUringFile<BLOCK_SIZE>>(path).await;
    }

    #[tokio::test]
    async fn test_corrupted_record_sealed() {
        let _guard = TEST_MUTEX.lock().unwrap();

        let path = PathBuf::from("/tmp/04.log");

        logfile::tests::test_corrupted_record_sealed::<SimpleFile, IOUringFile<BLOCK_SIZE>>(path)
            .await;
    }

    // #[tokio::test]
    // async fn test_corrupted_file_header() {
    //     let _guard = TEST_MUTEX.lock().unwrap();
    //     if GLOBAL_RING.get().is_none() {
    //         let _ = GLOBAL_RING.set(Arc::new(Mutex::new(IoUring::new(128).unwrap())));
    //     }

    //     let path = PathBuf::from("/tmp/05.log");

    //     logfile::tests::test_corrupted_file_header::<IOUringFile>(path).await;
    // }

    #[tokio::test]
    async fn test_100_records() {
        let _guard = TEST_MUTEX.lock().unwrap();

        let path = PathBuf::from("/tmp/06.log");

        logfile::tests::test_100_records::<SimpleFile, IOUringFile<BLOCK_SIZE>>(path).await;
    }

    // FIXME
    #[tokio::test]
    async fn test_stream() {
        let _guard = TEST_MUTEX.lock().unwrap();

        let path = PathBuf::from("/tmp/10.log");

        logfile::tests::test_stream::<SimpleFile, IOUringFile<BLOCK_SIZE>>(path).await;
    }

    #[tokio::test]
    async fn test_write_magic_number_without_sealing_escape() {
        let _guard = TEST_MUTEX.lock().unwrap();

        let path = PathBuf::from("/tmp/08.log");

        logfile::tests::test_write_magic_number_without_sealing_escape::<
            SimpleFile,
            IOUringFile<BLOCK_SIZE>,
        >(path)
        .await;
    }

    #[tokio::test]
    async fn test_write_magic_number_sealing_escape() {
        let _guard = TEST_MUTEX.lock().unwrap();

        let path = PathBuf::from("/tmp/09.log");

        logfile::tests::test_write_magic_number_sealing_escape::<SimpleFile, IOUringFile<BLOCK_SIZE>>(
            path,
        )
        .await;
    }

    // FIXME
    // #[tokio::test]
    // async fn test_write_magic_number_without_sealing_escape_iterator() {
    //     let _guard = TEST_MUTEX.lock().unwrap();
    //     if GLOBAL_RING.get().is_none() {
    //         let _ = GLOBAL_RING.set(Arc::new(Mutex::new(IoUring::new(128).unwrap())));
    //     }

    //     let path = PathBuf::from("/tmp/10.log");

    //     logfile::tests::test_write_magic_number_without_sealing_escape_iterator::<IOUringFile>(
    //         path,
    //     )
    //     .await;
    // }

    // #[tokio::test]
    // async fn test_write_magic_number_with_sealing_escape_iterator() {
    //     let _guard = TEST_MUTEX.lock().unwrap();
    //     if GLOBAL_RING.get().is_none() {
    //         let _ = GLOBAL_RING.set(Arc::new(Mutex::new(IoUring::new(128).unwrap())));
    //     }

    //     let path = PathBuf::from("/tmp/11.log");

    //     logfile::tests::test_write_magic_number_with_sealing_escape_iterator::<IOUringFile>(path)
    //         .await;
    // }

    // #[tokio::test]
    // async fn test_write_too_large_record() {
    //     let _guard = TEST_MUTEX.lock().unwrap();
    //     if GLOBAL_RING.get().is_none() {
    //         let _ = GLOBAL_RING.set(Arc::new(Mutex::new(IoUring::new(128).unwrap())));
    //     }

    //     let path = PathBuf::from("/tmp/12.log");

    //     logfile::tests::test_write_too_large_record::<IOUringFile>(path).await;
    // }
}
