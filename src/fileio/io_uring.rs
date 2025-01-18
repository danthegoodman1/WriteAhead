#[cfg(target_os = "linux")]
mod linux_impl {
    use crate::fileio::{FileReader, FileWriter};

    use io_uring::{opcode, IoUring};
    use std::env;
    use std::os::unix::io::AsRawFd;
    use std::path::Path;
    use tracing::instrument;

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
            let fd = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(device_path)?;

            let ring = if *USE_SQPOLL {
                IoUring::builder()
                    .setup_sqpoll(*SQPOLL_TIMEOUT)
                    .build(*URING_SIZE)?
            } else {
                IoUring::new(*URING_SIZE)?
            };

            let (tx, rx) = flume::unbounded();

            let actor = IOUringActor {
                fd: fd.try_clone()?,
                ring,
                command_receiver: rx,
            };

            tokio::spawn(actor.run());

            Ok(Self {
                fd,
                command_sender: tx,
            })
        }

        pub async fn read_block(&self, offset: u64, buffer: &mut [u8]) -> std::io::Result<()> {
            let (tx, rx) = flume::unbounded();
            self.command_sender
                .send(IOUringCommand::Read {
                    offset,
                    size: buffer.len() as u64,
                    response: tx,
                })
                .unwrap();

            let data = rx.recv_async().await.unwrap()?;
            buffer.copy_from_slice(&data);
            Ok(())
        }

        pub async fn write_data(&self, offset: u64, data: &[u8]) -> std::io::Result<()> {
            let (tx, rx) = flume::unbounded();
            self.command_sender
                .send(IOUringCommand::Write {
                    offset,
                    data: data.to_vec(),
                    response: tx,
                })
                .unwrap();
            let _ = rx.recv_async().await.unwrap();
            Ok(())
        }
    }

    impl IOUringActor {
        async fn run(mut self) {
            while let Ok(cmd) = self.command_receiver.recv_async().await {
                match cmd {
                    IOUringCommand::Read {
                        offset,
                        size,
                        response,
                    } => {
                        let result = self.handle_read(offset, size).await;
                        let _ = response.send(result);
                    }
                    IOUringCommand::Write {
                        offset,
                        data,
                        response,
                    } => {
                        let result = self.handle_write(offset, &data).await;
                        let _ = response.send(result);
                    }
                }
            }
        }

        async fn handle_read(&mut self, offset: u64, size: u64) -> std::io::Result<Vec<u8>> {
            let mut buffer = vec![0u8; size as usize];
            let fd = io_uring::types::Fd(self.fd.as_raw_fd());

            let read_e = opcode::Read::new(fd, buffer.as_mut_ptr(), buffer.len() as _)
                .offset(offset)
                .build()
                .user_data(0x42);

            unsafe {
                self.ring
                    .submission()
                    .push(&read_e)
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
            }

            self.ring.submit_and_wait(1)?;

            while let Some(cqe) = self.ring.completion().next() {
                if cqe.result() < 0 {
                    return Err(std::io::Error::from_raw_os_error(-cqe.result()));
                }
            }

            Ok(buffer)
        }

        async fn handle_write(&mut self, offset: u64, data: &[u8]) -> std::io::Result<()> {
            let fd = io_uring::types::Fd(self.fd.as_raw_fd());

            let write_e = opcode::Write::new(fd, data.as_ptr(), data.len() as _)
                .offset(offset)
                .build()
                .user_data(0x43);

            unsafe {
                self.ring
                    .submission()
                    .push(&write_e)
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
            }

            self.ring.submit_and_wait(1)?;

            while let Some(cqe) = self.ring.completion().next() {
                if cqe.result() < 0 {
                    return Err(std::io::Error::from_raw_os_error(-cqe.result()));
                }
            }

            Ok(())
        }
    }

    impl FileReader for IOUringFile {
        async fn open(path: &Path) -> anyhow::Result<Self> {
            Ok(Self::new(path)?)
        }

        async fn read(&self, offset: u64, size: u64) -> anyhow::Result<Vec<u8>> {
            let (tx, rx) = flume::unbounded();
            self.command_sender.send(IOUringCommand::Read {
                offset,
                size,
                response: tx,
            })?;
            Ok(rx.recv_async().await??)
        }

        fn file_length(&self) -> u64 {
            self.fd.metadata().unwrap().len()
        }
    }

    impl FileWriter for IOUringFile {
        async fn open(path: &Path) -> anyhow::Result<Self> {
            Ok(Self::new(path)?)
        }

        async fn write(&mut self, offset: u64, data: &[u8]) -> anyhow::Result<()> {
            let (tx, rx) = flume::unbounded();
            self.command_sender.send(IOUringCommand::Write {
                offset,
                data: data.to_vec(),
                response: tx,
            })?;
            Ok(rx.recv_async().await??)
        }

        fn file_length(&self) -> u64 {
            self.fd.metadata().unwrap().len()
        }
    }
}

#[cfg(target_os = "linux")]
pub use linux_impl::*;

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use io_uring::IoUring;
    use linux_impl::IOUringFile;
    use tokio::sync::Mutex;

    use crate::{fileio::simple_file::SimpleFile, logfile};

    use super::*;
    use std::{
        path::{Path, PathBuf},
        sync::Arc,
    };

    const BLOCK_SIZE: usize = 4096;

    #[tokio::test]
    async fn test_write_without_sealing() {
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
        let path = PathBuf::from("/tmp/10.log");

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
