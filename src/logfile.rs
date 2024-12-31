use std::time::Instant;

pub struct Logfile {
    created_at: Instant,
    sealed_at: Option<Instant>,
    path: String,
}

/// Logfile represents a log file on disk.
///
/// # Format
///
/// A log file always starts and ends with the magic bytes ---
///
/// The first bytes after the magic bytes represent the creation timestamp of the log file.
///
/// The last bytes after the magic bytes represent the sealed time of the log file if it exists.
/// This means that the log file is immutable and cannot be modified.
impl Logfile {
    pub fn new(path: String) -> Self {
        Self {
            created_at: Instant::now(),
            sealed_at: None,
            path,
        }
    }
}
