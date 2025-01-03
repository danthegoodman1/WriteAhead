/// RecordID represents a record's location in a log file.
#[derive(Debug, PartialEq, Eq)]
pub struct RecordID {
    pub file_id: u64,
    pub file_offset: u64,
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
