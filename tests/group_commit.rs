//! Group commit through the public API: concurrent WriteHandle writers all
//! get unique addresses and durable, intact records — including across
//! rotation boundaries.

use std::collections::HashSet;
use std::sync::Arc;
use writeahead::{RecordID, SimpleFile, WriteAhead, WriteAheadOptions};

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_handles_get_unique_intact_records() {
    let dir = tempfile::tempdir().unwrap();
    let mut wal = WriteAhead::<SimpleFile>::with_options(WriteAheadOptions {
        log_dir: dir.path().to_path_buf(),
        // Small enough that concurrent writes cross many rotations
        max_file_size: 4096,
        ..Default::default()
    });
    wal.start().unwrap();
    let wal = Arc::new(wal);

    let mut joins = Vec::new();
    for t in 0..8 {
        let handle = wal.writer().unwrap();
        joins.push(tokio::spawn(async move {
            let mut acked: Vec<(RecordID, Vec<u8>)> = Vec::new();
            for i in 0..200 {
                let payload = format!("thread {t} record {i}").into_bytes();
                let id = handle.write(payload.clone()).await.unwrap();
                acked.push((id, payload));
            }
            acked
        }));
    }

    let mut all = Vec::new();
    for join in joins {
        all.extend(join.await.unwrap());
    }

    // Coalesced commits must still hand out disjoint addresses
    let unique: HashSet<RecordID> = all.iter().map(|(id, _)| *id).collect();
    assert_eq!(unique.len(), all.len(), "duplicate record ids");

    // Rotation must have happened under load
    let max_file = all.iter().map(|(id, _)| id.file_id).max().unwrap();
    assert!(max_file > 0, "expected writes to cross rotations");

    // Every acknowledged record reads back intact
    for (id, payload) in &all {
        assert_eq!(&wal.read(id.file_id, id.file_offset).unwrap(), payload);
    }
}
