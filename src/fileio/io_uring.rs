// Trait for aligned pages to implement
#[cfg(target_os = "linux")]
pub trait AlignedBuffer {
    fn as_ptr(&self) -> *const u8;
    fn as_mut_ptr(&mut self) -> *mut u8;
    fn len(&self) -> usize;
}

/**
 * Macro to create an aligned page type for a given size
 *
 * This macro creates a new struct with the given name and alignment,
 * and implements the AlignedBuffer trait for it.
 *
 * You might need to import the AlignedBuffer trait to use this macro.
 */
#[macro_export]
#[cfg(target_os = "linux")]
macro_rules! create_aligned_page {
    ($name:ident, $alignment:expr) => {
        #[repr(align($alignment))]
        pub struct $name<const N: usize>(pub [u8; N]);

        impl<const N: usize> AlignedBuffer for $name<N> {
            fn as_ptr(&self) -> *const u8 {
                self.0.as_ptr()
            }
            fn as_mut_ptr(&mut self) -> *mut u8 {
                self.0.as_mut_ptr()
            }
            fn len(&self) -> usize {
                self.0.len()
            }
        }
    };
}

#[cfg(target_os = "linux")]
mod linux_impl {
    use super::AlignedBuffer;
    use io_uring::{opcode, IoUring};
    use std::os::unix::fs::OpenOptionsExt;
    use std::os::unix::io::AsRawFd;
    use std::sync::Arc;
    use tokio::sync::Mutex;

    // Convenience 4k (4096)
    create_aligned_page!(Page4K, 4096);

    pub struct IOUringDevice<const BLOCK_SIZE: usize> {
        fd: Option<std::fs::File>,
        ring: Arc<Mutex<IoUring>>,
    }

    impl<const BLOCK_SIZE: usize> IOUringDevice<BLOCK_SIZE> {
        pub fn new(device_path: &str, ring: Arc<Mutex<IoUring>>) -> std::io::Result<Self> {
            let fd = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .custom_flags(libc::O_DIRECT)
                .open(device_path)?;

            Ok(Self { fd: Some(fd), ring })
        }

        /// Reads a block from the device into the given buffer.
        pub async fn read_block<T: AlignedBuffer>(
            &mut self,
            offset: u64,
            buffer: &mut T,
        ) -> std::io::Result<()> {
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

        /// Writes a block to the device from the given buffer.
        pub async fn write_block<T: AlignedBuffer>(
            &mut self,
            offset: u64,
            buffer: &T,
        ) -> std::io::Result<()> {
            let fd = io_uring::types::Fd(self.fd.as_ref().unwrap().as_raw_fd());

            let write_e = opcode::Write::new(fd, buffer.as_ptr(), buffer.len() as _)
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

        /// Deallocates the block at the given offset using `FALLOC_FL_PUNCH_HOLE`, which creates a hole in the file
        /// and releases the associated storage space. On SSDs this triggers the TRIM command for better performance
        /// and wear leveling.
        pub async fn trim_block(&mut self, offset: u64) -> std::io::Result<()> {
            let fd = io_uring::types::Fd(self.fd.as_ref().unwrap().as_raw_fd());

            // FALLOC_FL_PUNCH_HOLE (0x02) | FALLOC_FL_KEEP_SIZE (0x01)
            const PUNCH_HOLE: i32 = 0x02 | 0x01;

            let trim_e = opcode::Fallocate::new(fd, BLOCK_SIZE as u64)
                .offset(offset)
                .mode(PUNCH_HOLE)
                .build()
                .user_data(0x44);

            // Lock the ring for this operation
            let mut ring = self.ring.lock().await;

            unsafe {
                ring.submission()
                    .push(&trim_e)
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

    impl<const BLOCK_SIZE: usize> Drop for IOUringDevice<BLOCK_SIZE> {
        fn drop(&mut self) {
            if let Some(fd) = self.fd.take() {
                drop(fd);
            }
        }
    }
}

#[cfg(target_os = "linux")]
pub use linux_impl::*;

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use io_uring::IoUring;
    use linux_impl::{IOUringDevice, Page4K};
    use tokio::sync::Mutex;

    use super::*;
    use std::sync::Arc;

    const BLOCK_SIZE: usize = 4096;

    #[tokio::test]
    async fn test_io_uring_read_write() -> Result<(), Box<dyn std::error::Error>> {
        // Create a shared io_uring instance
        let ring = Arc::new(Mutex::new(IoUring::new(128)?));

        // Create a temporary file path
        let temp_file = tempfile::NamedTempFile::new()?;
        let temp_path = temp_file.path().to_str().unwrap();

        // Create a new device instance
        let mut device = IOUringDevice::<BLOCK_SIZE>::new(temp_path, ring)?;

        // Test data
        let mut write_data = [0u8; BLOCK_SIZE];
        let hello = b"Hello, world!\n";
        write_data[..hello.len()].copy_from_slice(hello);
        let write_page = Page4K(write_data);

        // Write test
        device.write_block(0, &write_page).await?;

        // Read test
        let mut read_buffer = Page4K([0u8; BLOCK_SIZE]);
        device.read_block(0, &mut read_buffer).await?;

        // Verify the contents
        assert_eq!(&read_buffer.0[..hello.len()], hello);
        println!("Read data: {:?}", &read_buffer.0[..hello.len()]);
        // As a string
        println!(
            "Read data (string): {}",
            String::from_utf8_lossy(&read_buffer.0[..hello.len()])
        );

        // Try trimming the block
        device.trim_block(0).await?;

        // Read the block again
        let mut read_buffer = Page4K([0u8; BLOCK_SIZE]);
        device.read_block(0, &mut read_buffer).await?;

        // Verify the contents are zeroed
        assert_eq!(&read_buffer.0, &[0u8; BLOCK_SIZE]);

        Ok(())
    }

    #[tokio::test]
    async fn test_io_uring_sqpoll() -> Result<(), Box<dyn std::error::Error>> {
        create_aligned_page!(Page4K, 4096); // test creating an aliged page with a macro

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
        let mut device = IOUringDevice::<BLOCK_SIZE>::new(temp_path, ring)?;

        // Test data
        let mut write_data = [0u8; BLOCK_SIZE];
        let test_data = b"Testing SQPOLL mode!\n";
        write_data[..test_data.len()].copy_from_slice(test_data);
        let write_page = Page4K(write_data);

        // Write test
        device.write_block(0, &write_page).await?;

        // Read test
        let mut read_buffer = Page4K([0u8; BLOCK_SIZE]);
        device.read_block(0, &mut read_buffer).await?;

        // Verify the contents
        assert_eq!(&read_buffer.0[..test_data.len()], test_data);

        Ok(())
    }
}
