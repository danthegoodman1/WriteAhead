use std::time::Instant;

/// Logfile represents a log file on disk.
///
/// # Format
///
/// A log file always starts and ends with the magic bytes (TODO)
///
/// The name of the file must be the file ID, a monotonically increasing integer
///
/// The first bytes after the magic bytes represent the creation timestamp of the log file.
///
/// The last bytes after the magic bytes represent the sealed time of the log file if it exists.
/// This means that the log file is immutable and cannot be modified.
pub struct Logfile {
    created_at: Instant,
    sealed_at: Option<Instant>,
    id: u64,
}

impl Logfile {
    pub fn new(id: u64) -> Self {
        Self {
            created_at: Instant::now(),
            sealed_at: None,
            id,
        }
    }
}
