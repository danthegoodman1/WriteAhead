//! Retention enforcement: size- and ttl-based deletion of sealed files.
//! Retention runs on the writer thread — at startup (synchronously, before
//! `start()` returns) and after each rotation.

use std::time::Duration;
use writeahead::logfile::{now_ms, recover_and_seal, FILE_HEADER_SIZE, FOOTER_SIZE};
use writeahead::{RetentionOptions, SimpleFile, WriteAhead, WriteAheadOptions};

#[tokio::test]
async fn size_retention_deletes_oldest_sealed_files() {
    let dir = tempfile::tempdir().unwrap();
    let max_file_size = FILE_HEADER_SIZE + 128;
    let max_total_size = 3 * (max_file_size + FOOTER_SIZE);
    let mut wal = WriteAhead::<SimpleFile>::with_options(WriteAheadOptions {
        log_dir: dir.path().to_path_buf(),
        max_file_size,
        preallocation_chunk_size: Some(4_096),
        retention: RetentionOptions {
            max_total_size,
            ..Default::default()
        },
    });
    wal.start().unwrap();

    let mut ids = Vec::new();
    for i in 0..40 {
        ids.extend(
            wal.write_batch(vec![format!("record number {}", i).into_bytes()])
                .await
                .unwrap(),
        );
    }
    // Rotation/retention for the last write may still be in flight on the
    // writer thread; one more ack guarantees everything before it settled.
    wal.write_batch(vec![b"barrier".to_vec()]).await.unwrap();

    // Old sealed files must have been deleted to hold the size bound
    // (slack: the bound is enforced at rotation time, before the current
    // file grows past max_file_size again)
    let total: u64 = std::fs::read_dir(dir.path())
        .unwrap()
        .map(|e| e.unwrap().metadata().unwrap().len())
        .sum();
    assert!(
        total <= max_total_size + max_file_size + FOOTER_SIZE,
        "disk usage {} not bounded by retention",
        total
    );

    // Earliest records are gone, latest still readable
    let first = &ids[0];
    let last = ids.last().unwrap();
    assert!(wal.read(first.file_id, first.file_offset).is_err());
    assert!(wal.read(last.file_id, last.file_offset).is_ok());
}

#[tokio::test]
async fn ttl_retention_deletes_aged_files_on_start() {
    let dir = tempfile::tempdir().unwrap();

    // Craft an old sealed file 0 and a fresh sealed file 1
    {
        let mut wal = WriteAhead::<SimpleFile>::with_options(WriteAheadOptions {
            log_dir: dir.path().to_path_buf(),
            ..Default::default()
        });
        wal.start().unwrap();
        wal.write_batch(vec![b"ancient".to_vec()]).await.unwrap();
        drop(wal);
    }
    let hour_ms = 3_600_000;
    recover_and_seal::<SimpleFile>(
        &dir.path().join("0000000000.log"),
        now_ms().saturating_sub(3 * hour_ms),
    )
    .unwrap();
    std::fs::write(dir.path().join("0000000001.log"), b"").unwrap();
    recover_and_seal::<SimpleFile>(&dir.path().join("0000000001.log"), now_ms()).unwrap();

    let mut wal = WriteAhead::<SimpleFile>::with_options(WriteAheadOptions {
        log_dir: dir.path().to_path_buf(),
        retention: RetentionOptions {
            ttl: Duration::from_secs(2 * 60 * 60),
            ..Default::default()
        },
        ..Default::default()
    });
    wal.start().unwrap();

    assert!(
        !dir.path().join("0000000000.log").exists(),
        "aged file should be deleted"
    );
    assert!(
        dir.path().join("0000000001.log").exists(),
        "fresh file should be retained"
    );
    // Reads into the deleted file error cleanly
    assert!(wal.read(0, 9).is_err());
}

#[tokio::test]
async fn retention_disabled_deletes_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let mut wal = WriteAhead::<SimpleFile>::with_options(WriteAheadOptions {
        log_dir: dir.path().to_path_buf(),
        max_file_size: FILE_HEADER_SIZE + 64,
        ..Default::default()
    });
    wal.start().unwrap();

    let mut ids = Vec::new();
    for i in 0..20 {
        ids.extend(
            wal.write_batch(vec![format!("record {}", i).into_bytes()])
                .await
                .unwrap(),
        );
    }
    for (i, id) in ids.iter().enumerate() {
        assert_eq!(
            wal.read(id.file_id, id.file_offset).unwrap(),
            format!("record {}", i).into_bytes()
        );
    }
}
