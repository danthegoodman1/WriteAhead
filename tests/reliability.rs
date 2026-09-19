mod support;

use futures::{StreamExt, TryStreamExt};
use std::sync::atomic::Ordering;
use support::{Control, FaultFile};
use writeahead::logfile::{FILE_HEADER_SIZE, RECORD_HEADER_SIZE};
use writeahead::write_ahead::WriteAheadError;
use writeahead::{SimpleFile, WriteAhead, WriteAheadOptions};

fn options(dir: &std::path::Path) -> WriteAheadOptions {
    WriteAheadOptions {
        log_dir: dir.into(),
        preallocation_chunk_size: None,
        ..Default::default()
    }
}

#[test]
fn repeated_start_and_aliases_cannot_create_another_writer() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("wal");
    let alias = dir.path().join("alias");
    let mut wal = WriteAhead::<SimpleFile>::with_options(options(&path));
    wal.start().unwrap();
    let handle = wal.writer().unwrap();
    let id = futures::executor::block_on(handle.write(b"keep".to_vec())).unwrap();
    assert!(matches!(
        wal.start().unwrap_err().downcast_ref::<WriteAheadError>(),
        Some(WriteAheadError::AlreadyStarted)
    ));
    std::os::unix::fs::symlink(&path, &alias).unwrap();
    let mut other = WriteAhead::<SimpleFile>::with_options(options(&alias));
    assert!(matches!(
        other.start().unwrap_err().downcast_ref::<WriteAheadError>(),
        Some(WriteAheadError::DirectoryLocked)
    ));
    drop(wal);
    assert!(
        other.start().is_err(),
        "surviving handle must keep ownership"
    );
    futures::executor::block_on(handle.write(b"also keep".to_vec())).unwrap();
    drop(handle);
    other.start().unwrap();
    assert_eq!(other.read(id.file_id, id.file_offset).unwrap(), b"keep");
}

#[tokio::test]
async fn failed_reader_initialization_leaves_no_actor_or_lock() {
    let dir = tempfile::tempdir().unwrap();
    let control = Control::new(dir.path());
    control.fail_open.store(true, Ordering::SeqCst);
    let mut wal = WriteAhead::<FaultFile>::with_options(options(dir.path()));
    assert!(wal.start().is_err());
    assert!(wal.writer().is_err());
    assert_eq!(control.handles(&dir.path().join("0000000000.log")), 0);
    wal.start().unwrap();
    wal.write_batch(vec![b"retry".to_vec()]).await.unwrap();
}

#[tokio::test]
async fn readers_and_snapshots_never_expose_a_pending_or_failed_sync() {
    let dir = tempfile::tempdir().unwrap();
    let control = Control::new(dir.path());
    let mut wal = WriteAhead::<FaultFile>::with_options(options(dir.path()));
    wal.start().unwrap();
    let first = wal.write_batch(vec![b"committed".to_vec()]).await.unwrap()[0];
    let end = first.file_offset + RECORD_HEADER_SIZE + 9;
    for fail in [true, false] {
        let gate = control.pause_sync();
        control.fail_sync.store(fail, Ordering::SeqCst);
        let handle = wal.writer().unwrap();
        let thread = std::thread::spawn(move || {
            futures::executor::block_on(handle.write(b"pending".to_vec()))
        });
        gate.wait();
        assert!(wal.read(first.file_id, end).is_err());
        let snapshot = wal.create_stream().unwrap();
        let existing = wal
            .create_stream()
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(existing, vec![(first, b"committed".to_vec())]);
        drop(gate);
        let result = thread.join().unwrap();
        assert_eq!(result.is_err(), fail);
        assert_eq!(snapshot.try_collect::<Vec<_>>().await.unwrap(), existing);
        if fail {
            assert!(wal.read(first.file_id, end).is_err());
        } else {
            assert_eq!(wal.read(first.file_id, end).unwrap(), b"pending");
        }
    }
}

#[tokio::test]
async fn snapshot_ends_at_creation_and_point_reads_skip_commit_metadata() {
    let dir = tempfile::tempdir().unwrap();
    let control = Control::new(dir.path());
    let mut wal = WriteAhead::<FaultFile>::with_options(options(dir.path()));
    wal.start().unwrap();
    let id = wal.write_batch(vec![vec![7; 64]]).await.unwrap()[0];
    let mut snapshot = wal.create_stream().unwrap();
    wal.write_batch(vec![b"later".to_vec()]).await.unwrap();
    control.reads.store(0, Ordering::SeqCst);
    control.stats.store(0, Ordering::SeqCst);
    assert_eq!(wal.read(id.file_id, id.file_offset).unwrap(), vec![7; 64]);
    assert_eq!(control.reads.load(Ordering::SeqCst), 1);
    assert_eq!(control.stats.load(Ordering::SeqCst), 0);
    assert_eq!(snapshot.next().await.unwrap().unwrap().0, id);
    assert!(snapshot.next().await.is_none());
    wal.write_batch(vec![b"even later".to_vec()]).await.unwrap();
    assert!(snapshot.next().await.is_none());
}

#[tokio::test]
async fn trim_releases_idle_cache_handles_and_preserves_existing_streams() {
    for keep_stream in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let control = Control::new(dir.path());
        let mut opts = options(dir.path());
        opts.max_file_size = FILE_HEADER_SIZE + 32;
        let mut wal = WriteAhead::<FaultFile>::with_options(opts);
        wal.start().unwrap();
        let first = wal.write_batch(vec![vec![1; 10]]).await.unwrap()[0];
        let second = wal.write_batch(vec![vec![2; 10]]).await.unwrap()[0];
        let path = dir.path().join("0000000000.log");
        wal.read(first.file_id, first.file_offset).unwrap();
        let stream = keep_stream.then(|| wal.create_stream().unwrap());
        wal.trim_before(second.file_id).await.unwrap();
        assert!(!path.exists());
        assert_eq!(control.handles(&path), usize::from(keep_stream));
        if let Some(mut stream) = stream {
            let mut got = Vec::new();
            while let Some(record) = stream.next().await {
                got.push(record.unwrap());
            }
            assert_eq!(got, vec![(first, vec![1; 10]), (second, vec![2; 10])]);
            assert_eq!(
                control.handles(&path),
                0,
                "EOF releases handles even if the stream stays alive"
            );
            assert!(stream.next().await.is_none());
        }
        assert_eq!(control.handles(&path), 0);
        assert!(wal.read(first.file_id, first.file_offset).is_err());
    }
}

#[tokio::test]
async fn bounded_admission_is_async_and_cancellable_before_enqueue() {
    let dir = tempfile::tempdir().unwrap();
    let control = Control::new(dir.path());
    let mut opts = options(dir.path());
    opts.queue_capacity = 1;
    opts.max_batch_bytes = 24;
    let mut wal = WriteAhead::<FaultFile>::with_options(opts);
    wal.start().unwrap();
    let gate = control.pause_sync();
    let handle = wal.writer().unwrap();
    let first =
        std::thread::spawn(move || futures::executor::block_on(handle.write(b"1234".to_vec())));
    gate.wait();
    let handle = wal.writer().unwrap();
    let mut queued = Box::pin(handle.write(b"next".to_vec()));
    assert!(futures::poll!(&mut queued).is_pending());
    let mut waiting = Box::pin(handle.write(b"drop".to_vec()));
    let start = std::time::Instant::now();
    assert!(futures::poll!(&mut waiting).is_pending());
    assert!(
        start.elapsed() < std::time::Duration::from_secs(1),
        "admission blocked the executor"
    );
    drop(waiting);
    drop(gate);
    first.join().unwrap().unwrap();
    queued.await.unwrap();
    let error = handle.write(b"12345".to_vec()).await.unwrap_err();
    assert!(matches!(
        error.downcast_ref::<WriteAheadError>(),
        Some(WriteAheadError::BatchTooLarge(24))
    ));
    let records = wal
        .create_stream()
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(
        records
            .iter()
            .map(|(_, r)| r.as_slice())
            .collect::<Vec<_>>(),
        [b"1234", b"next"]
    );
}

#[test]
fn invalid_queue_options_fail_before_startup() {
    for (capacity, batch) in [(0, 32), (1, 0), (1, usize::MAX)] {
        let dir = tempfile::tempdir().unwrap();
        let mut opts = options(dir.path());
        opts.queue_capacity = capacity;
        opts.max_batch_bytes = batch;
        let mut wal = WriteAhead::<SimpleFile>::with_options(opts);
        assert!(matches!(
            wal.start().unwrap_err().downcast_ref::<WriteAheadError>(),
            Some(WriteAheadError::InvalidQueueOptions)
        ));
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
    }
}

#[tokio::test]
async fn buffered_recovery_preserves_large_and_empty_records_and_repairs_unsynced_tail() {
    let dir = tempfile::tempdir().unwrap();
    let control = Control::new(dir.path());
    let mut wal = WriteAhead::<FaultFile>::with_options(options(dir.path()));
    wal.start().unwrap();
    let records: Vec<Vec<u8>> = [0, 131_052, 1, 131_073, 300_000, 0]
        .into_iter()
        .enumerate()
        .map(|(i, length)| vec![i as u8; length])
        .collect();
    let ids = wal.write_batch(records.clone()).await.unwrap();
    control.fail_sync.store(true, Ordering::SeqCst);
    assert!(wal.write_batch(vec![vec![9; 200_000]]).await.is_err());
    drop(wal);
    let path = dir.path().join("0000000000.log");
    let len = std::fs::metadata(&path).unwrap().len();
    std::fs::OpenOptions::new()
        .write(true)
        .open(&path)
        .unwrap()
        .set_len(len - 17)
        .unwrap();
    let mut wal = WriteAhead::<FaultFile>::with_options(options(dir.path()));
    wal.start().unwrap();
    let actual = wal
        .create_stream()
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(actual, ids.into_iter().zip(records).collect::<Vec<_>>());
}

#[tokio::test]
async fn snapshot_during_rotation_survives_subsequent_trim() {
    let dir = tempfile::tempdir().unwrap();
    let control = Control::new(dir.path());
    let mut opts = options(dir.path());
    opts.max_file_size = FILE_HEADER_SIZE + 32;
    let mut wal = WriteAhead::<FaultFile>::with_options(opts);
    wal.start().unwrap();
    let first = wal.write_batch(vec![vec![1; 10]]).await.unwrap()[0];
    let gate = control.pause_sync(); // pause the next file's header sync
    let handle = wal.writer().unwrap();
    let writer = std::thread::spawn(move || futures::executor::block_on(handle.write(vec![2; 10])));
    gate.wait();
    let snapshot = wal.create_stream().unwrap();
    drop(gate);
    let second = writer.join().unwrap().unwrap();
    wal.trim_before(second.file_id).await.unwrap();
    assert_eq!(
        snapshot.try_collect::<Vec<_>>().await.unwrap(),
        vec![(first, vec![1; 10])]
    );
    assert_eq!(
        wal.create_stream()
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap(),
        vec![(second, vec![2; 10])]
    );
}

#[tokio::test]
async fn write_only_retention_does_not_keep_deleted_readers() {
    let dir = tempfile::tempdir().unwrap();
    let control = Control::new(dir.path());
    let mut opts = options(dir.path());
    opts.max_file_size = FILE_HEADER_SIZE + 32;
    opts.retention.max_total_size = 2 * (opts.max_file_size + writeahead::logfile::FOOTER_SIZE);
    let mut wal = WriteAhead::<FaultFile>::with_options(opts);
    wal.start().unwrap();
    for _ in 0..30 {
        wal.write_batch(vec![vec![1; 10]]).await.unwrap();
    }
    // No read or stream call has run to service lifecycle events.
    for id in 0..28 {
        let path = dir.path().join(format!("{id:010}.log"));
        assert!(!path.exists());
        assert_eq!(control.handles(&path), 0);
    }
}

#[tokio::test]
async fn recovery_io_errors_never_modify_records_or_metadata() {
    let dir = tempfile::tempdir().unwrap();
    let control = Control::new(dir.path());
    let mut wal = WriteAhead::<FaultFile>::with_options(options(dir.path()));
    wal.start().unwrap();
    let mut ids = wal.write_batch(vec![vec![1; 1024]]).await.unwrap();
    ids.extend(
        wal.write_batch(vec![vec![2; 1024], vec![3; 1024]])
            .await
            .unwrap(),
    );
    drop(wal);
    let path = dir.path().join("0000000000.log");
    let before = std::fs::read(&path).unwrap();
    for id in &ids {
        control.fail_read.store(id.file_offset, Ordering::SeqCst);
        let mut retry = WriteAhead::<FaultFile>::with_options(options(dir.path()));
        let error = retry.start().unwrap_err();
        assert!(
            error.downcast_ref::<std::io::Error>().is_some(),
            "{error:#}"
        );
        assert_eq!(std::fs::read(&path).unwrap(), before);
        assert!(retry.writer().is_err());
        control.fail_read.store(u64::MAX, Ordering::SeqCst);
        retry.start().unwrap();
        for (i, id) in ids.iter().enumerate() {
            assert_eq!(
                retry.read(id.file_id, id.file_offset).unwrap(),
                vec![i as u8 + 1; 1024]
            );
        }
    }
}
