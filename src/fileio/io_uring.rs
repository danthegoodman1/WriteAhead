#[cfg(target_os = "linux")]
mod linux_impl {
    use crate::fileio::{FileReader, FileWriter};

    use io_uring::{opcode, IoUring};
    use std::env;
    use std::os::unix::io::AsRawFd;
    use std::path::Path;
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
    }

    pub struct IOUringFile {
        fd: std::fs::File,
        command_sender: flume::Sender<IOUringCommand>,
    }

    struct IOUringActor {
        fd: std::fs::File,
        ring: IoUring,
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
            write!(f, "IOUringFile {{ fd: {:?} }}", self.fd)
        }
    }

    impl IOUringFile {
        pub fn new(device_path: &Path) -> std::io::Result<Self> {
            println!("IOUringFile::new - Creating new file at {:?}", device_path);
            let fd = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(device_path)?;
            println!("IOUringFile::new - File opened successfully");

            let ring = if *USE_SQPOLL {
                println!("IOUringFile::new - Creating ring with SQPOLL");
                IoUring::builder()
                    .setup_sqpoll(*SQPOLL_TIMEOUT)
                    .build(*URING_SIZE)?
            } else {
                println!("IOUringFile::new - Creating standard ring");
                IoUring::new(*URING_SIZE)?
            };
            println!("IOUringFile::new - Ring created successfully");

            let (tx, rx) = flume::unbounded();
            println!("IOUringFile::new - Created command channels");

            let actor = IOUringActor {
                fd: fd.try_clone()?,
                ring,
                command_receiver: rx,
            };

            println!("IOUringFile::new - Spawning actor task");
            tokio::spawn(actor.run());

            Ok(Self {
                fd,
                command_sender: tx,
            })
        }

        // pub async fn read_block(&self, offset: u64, buffer: &mut [u8]) -> std::io::Result<()> {
        //     println!(
        //         "read_block - Starting read at offset {} with size {}",
        //         offset,
        //         buffer.len()
        //     );
        //     let (tx, rx) = flume::unbounded();
        //     println!("read_block - Sending read command");
        //     self.command_sender
        //         .send(IOUringCommand::Read {
        //             offset,
        //             size: buffer.len() as u64,
        //             response: tx,
        //         })
        //         .unwrap();

        //     println!("read_block - Waiting for response");
        //     let data = rx.recv_async().await.unwrap()?;
        //     println!("read_block - Received {} bytes", data.len());
        //     buffer.copy_from_slice(&data);
        //     println!("read_block - Completed successfully");
        //     Ok(())
        // }

        // pub async fn write_data(&self, offset: u64, data: &[u8]) -> std::io::Result<()> {
        //     println!(
        //         "write_data - Starting write at offset {} with size {}",
        //         offset,
        //         data.len()
        //     );
        //     let (tx, rx) = flume::unbounded();
        //     println!("write_data - Sending write command");
        //     self.command_sender
        //         .send(IOUringCommand::Write {
        //             offset,
        //             data: data.to_vec(),
        //             response: tx,
        //         })
        //         .unwrap();
        //     println!("write_data - Waiting for response");
        //     let _ = rx.recv_async().await.unwrap();
        //     println!("write_data - Completed successfully");
        //     Ok(())
        // }
    }

    impl IOUringActor {
        async fn run(mut self) {
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
                        let result = self.handle_read(offset, size).await;
                        println!("IOUringActor::run - Read result: {:?}", result.is_ok());
                        let _ = response.send_async(result).await;
                        println!("IOUringActor::run - Sent read response");
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
                        let result = self.handle_write(offset, &data).await;
                        println!("IOUringActor::run - Write result: {:?}", result.is_ok());
                        let _ = response.send_async(result).await;
                        println!("IOUringActor::run - Sent write response");
                    }
                }
            }
            // println!("IOUringActor::run - Actor loop terminated");
        }

        async fn handle_read(&mut self, offset: u64, size: u64) -> std::io::Result<Vec<u8>> {
            println!("handle_read - Creating buffer of size {}", size);
            let mut buffer = vec![0u8; size as usize];
            let fd = io_uring::types::Fd(self.fd.as_raw_fd());

            println!("handle_read - Preparing read operation");
            let read_e = opcode::Read::new(fd, buffer.as_mut_ptr(), buffer.len() as _)
                .offset(offset)
                .build()
                .user_data(0x42);

            println!("handle_read - Submitting read operation");
            unsafe {
                self.ring
                    .submission()
                    .push(&read_e)
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
            }

            println!("handle_read - Waiting for completion");
            self.ring.submit_and_wait(1)?;

            while let Some(cqe) = self.ring.completion().next() {
                println!("handle_read - Completion result: {}", cqe.result());
                if cqe.result() < 0 {
                    println!("handle_read - Error: {}", -cqe.result());
                    return Err(std::io::Error::from_raw_os_error(-cqe.result()));
                }
            }

            println!("handle_read - Completed successfully");
            Ok(buffer)
        }

        async fn handle_write(&mut self, offset: u64, data: &[u8]) -> std::io::Result<()> {
            println!(
                "handle_write - Starting write operation of {} bytes at offset {}",
                data.len(),
                offset
            );
            let fd = io_uring::types::Fd(self.fd.as_raw_fd());

            println!("handle_write - Preparing write operation");
            let write_e = opcode::Write::new(fd, data.as_ptr(), data.len() as _)
                .offset(offset)
                .build()
                .user_data(0x43);

            println!("handle_write - Submitting write operation");
            unsafe {
                self.ring
                    .submission()
                    .push(&write_e)
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
            }

            println!("handle_write - Waiting for completion");
            self.ring.submit_and_wait(1)?;

            while let Some(cqe) = self.ring.completion().next() {
                println!("handle_write - Completion result: {}", cqe.result());
                if cqe.result() < 0 {
                    println!("handle_write - Error: {}", -cqe.result());
                    return Err(std::io::Error::from_raw_os_error(-cqe.result()));
                }
                let bytes_written = cqe.result();
                if bytes_written as usize != data.len() {
                    println!(
                        "handle_write - Incomplete write: {} of {} bytes",
                        bytes_written,
                        data.len()
                    );
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!(
                            "Incomplete write: {} of {} bytes",
                            bytes_written,
                            data.len()
                        ),
                    ));
                }
                return Ok(());
            }

            println!("handle_write - Error: No completion received");
            Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "No completion received",
            ))
        }
    }

    impl FileReader for IOUringFile {
        async fn open(path: &Path) -> anyhow::Result<Self> {
            println!("FileReader::open - Opening file at {:?}", path);
            let result = Self::new(path)?;
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

        fn file_length(&self) -> u64 {
            println!("FileReader::file_length - Getting file length");
            let length = self.fd.metadata().unwrap().len();
            println!("FileReader::file_length - File length is {}", length);
            length
        }
    }

    impl FileWriter for IOUringFile {
        async fn open(path: &Path) -> anyhow::Result<Self> {
            println!("FileWriter::open - Opening file at {:?}", path);
            let result = Self::new(path)?;
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

        fn file_length(&self) -> u64 {
            println!("FileWriter::file_length - Getting file length");
            let length = self.fd.metadata().unwrap().len();
            println!("FileWriter::file_length - File length is {}", length);
            length
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

    // FIXME
    #[tokio::test]
    async fn test_stream() {
        let path = PathBuf::from("/tmp/07.log");

        logfile::tests::test_stream::<IOUringFile, IOUringFile>(path).await;
    }

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
