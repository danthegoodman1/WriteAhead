#[cfg(target_os = "linux")]
mod linux_impl {
    use crate::fileio::{FileReader, FileWriter};

    use std::env;
    use std::path::{Path, PathBuf};
    use tracing::instrument;

    #[derive(Debug)]
    pub enum IOUringCommand {
        Read {
            offset: u64,
            size: u64,
            response: flume::Sender<std::io::Result<Vec<u8>>>,
        },
        Write {
            offset: u64,
            data: Vec<u8>,
            response: flume::Sender<std::io::Result<()>>,
        },
        FileLength {
            response: flume::Sender<std::io::Result<u64>>,
        },
    }

    pub struct IOUringFile {
        command_sender: flume::Sender<IOUringCommand>,
    }

    struct IOUringActor {
        command_receiver: flume::Receiver<IOUringCommand>,
    }

    const DEFAULT_SQPOLL_TIMEOUT: u32 = 20;
    const DEFAULT_URING_SIZE: u32 = 32;
    const DEFAULT_USE_SQPOLL: bool = false;

    fn get_sqpoll_timeout() -> u32 {
        env::var("SQPOLL_TIMEOUT_MS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_SQPOLL_TIMEOUT)
    }

    fn get_uring_size() -> u32 {
        env::var("URING_SIZE")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_URING_SIZE)
    }

    fn get_use_sqpoll() -> bool {
        env::var("USE_SQPOLL")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_USE_SQPOLL)
    }

    lazy_static::lazy_static! {
        static ref SQPOLL_TIMEOUT: u32 = get_sqpoll_timeout();
        static ref URING_SIZE: u32 = get_uring_size();
        static ref USE_SQPOLL: bool = get_use_sqpoll();
    }

    impl std::fmt::Debug for IOUringFile {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(
                f,
                "IOUringFile {{ command_sender: {:?} }}",
                self.command_sender
            )
        }
    }

    impl IOUringFile {
        pub async fn new(device_path: &Path) -> std::io::Result<Self> {
            let (tx, rx) = flume::unbounded();
            println!("IOUringFile::new - Created command channels");

            let actor = IOUringActor {
                command_receiver: rx,
            };

            println!("IOUringFile::new - Spawning actor task");
            // FIXME: Can't use nested runtimes, don't want to replace the parent runtime with tokio_uring's since it's so much older
            tokio_uring::builder()
                .entries(get_uring_size())
                .uring_builder(tokio_uring::uring_builder().setup_cqsize(1024))
                .start(actor.run(device_path.to_path_buf()));

            Ok(Self { command_sender: tx })
        }
    }

    impl IOUringActor {
        async fn run(self, device_path: PathBuf) {
            // Build ring runtime
            let fd = tokio_uring::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(device_path)
                .await
                .unwrap();

            println!("IOUringActor::run - Starting actor loop");
            loop {
                println!("IOUringActor::run - Waiting for command");
                let cmd = match self.command_receiver.recv_async().await {
                    Ok(cmd) => cmd,
                    Err(e) => {
                        println!("IOUringActor::run - Error receiving command: {}", e);
                        return;
                    }
                };
                println!("IOUringActor::run - Received command: {:?}", cmd);

                match cmd {
                    IOUringCommand::Read {
                        offset,
                        size,
                        response,
                    } => {
                        println!(
                            "IOUringActor::run - Handling read command: offset={}, size={}",
                            offset, size
                        );

                        let buf = vec![0u8; size as usize];
                        let (result, buf) = fd.read_at(buf, offset).await;
                        match result {
                            Ok(n) => {
                                println!("IOUringActor::run - Read {} bytes", n);
                                let _ = response.send_async(Ok(buf)).await;
                                println!("IOUringActor::run - Sent read response");
                            }
                            Err(e) => {
                                println!("IOUringActor::run - Read error: {}", e);
                                let _ = response.send_async(Err(e)).await;
                                println!("IOUringActor::run - Sent read error response");
                            }
                        }
                    }

                    IOUringCommand::Write {
                        offset,
                        data,
                        response,
                    } => {
                        println!(
                            "IOUringActor::run - Handling write command: offset={}, size={}",
                            offset,
                            data.len()
                        );
                        let (result, _) = fd.write_at(data, offset).submit().await;
                        match result {
                            Ok(n) => {
                                println!("IOUringActor::run - Wrote {} bytes", n);
                                let _ = response.send_async(Ok(())).await;
                                println!("IOUringActor::run - Sent write response");
                            }
                            Err(e) => {
                                println!("IOUringActor::run - Write error: {}", e);
                                let _ = response.send_async(Err(e)).await;
                                println!("IOUringActor::run - Sent write error response");
                            }
                        }
                    }

                    IOUringCommand::FileLength { response } => {
                        println!("IOUringActor::run - Handling file length command");
                        let result = fd.statx().await.unwrap();
                        println!("IOUringActor::run - File length: {}", result.stx_size);
                        let _ = response.send_async(Ok(result.stx_size)).await;
                        println!("IOUringActor::run - Sent file length response");
                    }
                }
            }
            // println!("IOUringActor::run - Actor loop terminated");
        }
    }

    impl FileReader for IOUringFile {
        async fn open(path: &Path) -> anyhow::Result<Self> {
            println!("FileReader::open - Opening file at {:?}", path);
            let result = Self::new(path).await?;
            println!("FileReader::open - File opened successfully");
            Ok(result)
        }

        async fn read(&self, offset: u64, size: u64) -> anyhow::Result<Vec<u8>> {
            println!(
                "FileReader::read - Reading {} bytes at offset {}",
                size, offset
            );
            let (tx, rx) = flume::unbounded();
            println!("FileReader::read - Sending read command");
            self.command_sender
                .send_async(IOUringCommand::Read {
                    offset,
                    size,
                    response: tx,
                })
                .await?;
            println!("FileReader::read - Waiting for response");
            let result = rx.recv_async().await.unwrap().unwrap();
            println!("FileReader::read - Received {} bytes", result.len());
            Ok(result)
        }

        async fn file_length(&self) -> anyhow::Result<u64> {
            println!("FileReader::file_length - Getting file length");
            let (tx, rx) = flume::unbounded();
            self.command_sender
                .send_async(IOUringCommand::FileLength { response: tx })
                .await?;
            println!("FileReader::file_length - Waiting for response");
            let length = rx.recv_async().await.unwrap().unwrap();
            println!("FileReader::file_length - File length is {}", length);
            Ok(length)
        }
    }

    impl FileWriter for IOUringFile {
        async fn open(path: &Path) -> anyhow::Result<Self> {
            println!("FileWriter::open - Opening file at {:?}", path);
            let result = Self::new(path).await?;
            println!("FileWriter::open - File opened successfully");
            Ok(result)
        }

        async fn write(&mut self, offset: u64, data: &[u8]) -> anyhow::Result<()> {
            println!(
                "FileWriter::write - Writing {} bytes at offset {}",
                data.len(),
                offset
            );
            let (tx, rx) = flume::unbounded();
            println!("FileWriter::write - Sending write command");
            self.command_sender
                .send_async(IOUringCommand::Write {
                    offset,
                    data: data.to_vec(),
                    response: tx,
                })
                .await?;
            println!("FileWriter::write - Waiting for response");
            let result = rx.recv_async().await.unwrap().unwrap();
            println!("FileWriter::write - Write completed successfully");
            Ok(result)
        }

        async fn file_length(&self) -> anyhow::Result<u64> {
            println!("FileWriter::file_length - Getting file length");
            let (tx, rx) = flume::unbounded();
            self.command_sender
                .send_async(IOUringCommand::FileLength { response: tx })
                .await?;
            println!("FileWriter::file_length - Waiting for response");
            let length = rx.recv_async().await.unwrap().unwrap();
            println!("FileWriter::file_length - File length is {}", length);
            Ok(length)
        }
    }
}

#[cfg(target_os = "linux")]
pub use linux_impl::*;

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use crate::logfile;
    use linux_impl::IOUringFile;

    use super::*;
    use std::path::PathBuf;

    #[tokio::test]
    async fn test_write_without_sealing() {
        // cargo test fileio::io_uring::tests::test_write_without_sealing -- --nocapture
        eprintln!("Starting test_write_without_sealing");
        let path = PathBuf::from("/tmp/01.log");

        match tokio::time::timeout(
            std::time::Duration::from_secs(5),
            logfile::tests::test_write_without_sealing::<IOUringFile, IOUringFile>(path),
        )
        .await
        {
            Ok(_) => (),
            Err(elapsed) => panic!("Test timed out after 5 seconds: {}", elapsed),
        }
    }

    #[tokio::test]
    async fn test_write_with_sealing() {
        let path = PathBuf::from("/tmp/02.log");

        logfile::tests::test_write_with_sealing::<IOUringFile, IOUringFile>(path).await;
    }

    #[tokio::test]
    async fn test_corrupted_record() {
        let path = PathBuf::from("/tmp/03.log");

        logfile::tests::test_corrupted_record::<IOUringFile, IOUringFile>(path).await;
    }

    #[tokio::test]
    async fn test_corrupted_record_sealed() {
        let path = PathBuf::from("/tmp/04.log");

        logfile::tests::test_corrupted_record_sealed::<IOUringFile, IOUringFile>(path).await;
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
        let path = PathBuf::from("/tmp/06.log");

        logfile::tests::test_100_records::<IOUringFile, IOUringFile>(path).await;
    }

    // #[tokio::test]
    // async fn test_stream() {
    //     let path = PathBuf::from("/tmp/07.log");

    //     logfile::tests::test_stream::<IOUringFile, IOUringFile>(path).await;
    // }

    #[tokio::test]
    async fn test_write_magic_number_without_sealing_escape() {
        let path = PathBuf::from("/tmp/08.log");

        logfile::tests::test_write_magic_number_without_sealing_escape::<IOUringFile, IOUringFile>(
            path,
        )
        .await;
    }

    #[tokio::test]
    async fn test_write_magic_number_sealing_escape() {
        let path = PathBuf::from("/tmp/09.log");

        logfile::tests::test_write_magic_number_sealing_escape::<IOUringFile, IOUringFile>(path)
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
