//! Explicit trimming: `trim_before(file_id)` deletes sealed files below the
//! consumer's high-water mark, the active file is untouchable, and the
//! result survives restart.

use futures::StreamExt;
use writeahead::logfile::FILE_HEADER_SIZE;
use writeahead::{SimpleFile, WriteAhead, WriteAheadOptions};

fn rotating_wal(dir: &std::path::Path, max_file_size: u64) -> WriteAhead<SimpleFile> {
    WriteAhead::with_options(WriteAheadOptions {
        log_dir: dir.to_path_buf(),
        max_file_size: FILE_HEADER_SIZE + max_file_size,
        ..Default::default()
    })
}

fn log_file_ids(dir: &std::path::Path) -> Vec<u64> {
    let mut ids: Vec<u64> = std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| writeahead::logfile::file_id_from_path(&e.unwrap().path()))
        .collect();
    ids.sort();
    ids
}

/// Writes records until the WAL has rotated through several files, returning
/// each record's (id, payload).
async fn fill_files(wal: &WriteAhead<SimpleFile>) -> Vec<(writeahead::RecordID, Vec<u8>)> {
    let handle = wal.writer().unwrap();
    let mut out = Vec::new();
    for i in 0..60 {
        let payload = format!("record number {i:04}").into_bytes();
        let id = handle.write(payload.clone()).await.unwrap();
        out.push((id, payload));
    }
    assert!(out.last().unwrap().0.file_id >= 3, "expected rotations");
    out
}

#[tokio::test]
async fn trim_deletes_sealed_files_below_watermark() {
    let dir = tempfile::tempdir().unwrap();
    let mut wal = rotating_wal(dir.path(), 256);
    wal.start().unwrap();
    let records = fill_files(&wal).await;
    let hwm_file = records.last().unwrap().0.file_id;

    let stats = wal.trim_before(hwm_file).await.unwrap();
    assert!(stats.files_deleted >= 3);
    assert!(stats.bytes_reclaimed > 0);
    assert!(log_file_ids(dir.path()).iter().all(|id| *id >= hwm_file));

    for (id, payload) in &records {
        let read = wal.read(id.file_id, id.file_offset);
        if id.file_id < hwm_file {
            assert!(read.is_err(), "trimmed record must be gone: {id:?}");
        } else {
            assert_eq!(&read.unwrap(), payload);
        }
    }

    // Streams start from the first surviving file
    let mut stream = wal.create_stream().unwrap();
    let survivors: Vec<_> = records
        .iter()
        .filter(|(id, _)| id.file_id >= hwm_file)
        .collect();
    for (id, payload) in survivors {
        let (got_id, got) = stream.next().await.unwrap().unwrap();
        assert_eq!((got_id, &got), (*id, payload));
    }
    assert!(stream.next().await.is_none());
}

#[tokio::test]
async fn trim_never_deletes_the_active_file() {
    let dir = tempfile::tempdir().unwrap();
    let mut wal = rotating_wal(dir.path(), 256);
    wal.start().unwrap();
    fill_files(&wal).await;

    let stats = wal.trim_before(u64::MAX).await.unwrap();
    assert!(stats.files_deleted > 0);
    let remaining = log_file_ids(dir.path());
    assert_eq!(remaining.len(), 1, "only the active file survives");

    // Still writable and readable after trimming everything else
    let id = wal
        .write_batch(vec![b"still alive".to_vec()])
        .await
        .unwrap()[0];
    assert!(
        id.file_id >= remaining[0],
        "a full active file may rotate before this write"
    );
    assert_eq!(
        wal.read(id.file_id, id.file_offset).unwrap(),
        b"still alive"
    );
}

#[tokio::test]
async fn trim_is_a_noop_below_the_oldest_file() {
    let dir = tempfile::tempdir().unwrap();
    let mut wal = rotating_wal(dir.path(), 256);
    wal.start().unwrap();
    fill_files(&wal).await;

    let before = log_file_ids(dir.path());
    let stats = wal.trim_before(0).await.unwrap();
    assert_eq!(stats.files_deleted, 0);
    assert_eq!(stats.bytes_reclaimed, 0);
    assert_eq!(log_file_ids(dir.path()), before);
}

#[tokio::test]
async fn trim_survives_restart() {
    let dir = tempfile::tempdir().unwrap();
    let mut wal = rotating_wal(dir.path(), 256);
    wal.start().unwrap();
    let records = fill_files(&wal).await;
    let hwm_file = records.last().unwrap().0.file_id;
    wal.trim_before(hwm_file).await.unwrap();
    drop(wal);

    let mut wal = rotating_wal(dir.path(), 256);
    wal.start().unwrap();
    let survivors: Vec<_> = records
        .iter()
        .filter(|(id, _)| id.file_id >= hwm_file)
        .collect();
    let mut stream = wal.create_stream().unwrap();
    for (id, payload) in survivors {
        let (got_id, got) = stream.next().await.unwrap().unwrap();
        assert_eq!((got_id, &got), (*id, payload));
    }
    assert!(stream.next().await.is_none());
}

#[tokio::test]
async fn trim_through_cloned_handle_evicts_readers() {
    let dir = tempfile::tempdir().unwrap();
    let mut wal = rotating_wal(dir.path(), 256);
    wal.start().unwrap();
    let records = fill_files(&wal).await;
    let first = records.first().unwrap();

    // Warm the reader cache on the file we're about to trim
    assert_eq!(
        wal.read(first.0.file_id, first.0.file_offset).unwrap(),
        first.1
    );

    let handle = wal.writer().unwrap();
    let hwm_file = records.last().unwrap().0.file_id;
    handle.trim_before(hwm_file).await.unwrap();
    assert!(
        wal.read(first.0.file_id, first.0.file_offset).is_err(),
        "cached reader must be evicted after trim"
    );
}
