use std::time::Instant;

/// Logfile represents a log file on disk.
///
/// # Format
///
/// A log file always starts and ends with the magic bytes (TODO)
///
/// The first bytes after the magic bytes represent the creation timestamp of the log file,
/// and the file ID (a monotonically increasing integer).
///
/// The last bytes after the magic bytes represent the sealed time of the log file if it exists.
/// This means that the log file is immutable and cannot be modified.
pub struct Logfile {
    created_at: Instant,
    sealed_at: Option<Instant>,
    path: String,
}

impl Logfile {
    pub fn new(path: String) -> Self {
        Self {
            created_at: Instant::now(),
            sealed_at: None,
            path,
        }
    }
}

/// RecordID represents a record's location in a log file.
#[derive(Debug, PartialEq, Eq)]
pub struct RecordID {
    file_id: u64,
    file_offset: u64,
}

impl RecordID {
    pub fn new(file_id: u64, file_offset: u64) -> Self {
        Self {
            file_id,
            file_offset,
        }
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&self.file_id.to_le_bytes());
        buf.extend_from_slice(&self.file_offset.to_le_bytes());
        buf
    }

    pub fn deserialize(buf: &[u8]) -> Self {
        Self {
            file_id: u64::from_le_bytes(buf[0..8].try_into().unwrap()),
            file_offset: u64::from_le_bytes(buf[8..16].try_into().unwrap()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_id_serde() {
        let id = RecordID::new(1, 100);
        let serialized = id.serialize();
        let deserialized = RecordID::deserialize(&serialized);
        assert_eq!(id, deserialized);
    }
}
