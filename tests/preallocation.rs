//! Allocation-window, early-rotation, and active-file tail behavior.

use futures::StreamExt;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use writeahead::logfile::{Logfile, FILE_HEADER_SIZE, FOOTER_SIZE, RECORD_HEADER_SIZE};
use writeahead::{FileIo, SimpleFile, WriteAhead, WriteAheadOptions};

/// Per-test sync counters, indexed by `CountingFile`'s ID so tests running in
/// parallel never see each other's syncs.
static DATA_SYNCS: [AtomicUsize; 3] = [const { AtomicUsize::new(0) }; 3];

/// Counts data syncs and rejects growth through `set_len`: allocation windows
/// must be written so commits overwrite allocated blocks. Writes that would
/// end past `DISK` bytes fail as a full disk.
#[derive(Debug)]
struct CountingFile<const ID: usize, const DISK: u64 = { u64::MAX }>(SimpleFile);

impl<const ID: usize, const DISK: u64> FileIo for CountingFile<ID, DISK> {
    fn open(path: &Path) -> anyhow::Result<Self> {
        Ok(Self(<SimpleFile as FileIo>::open(path)?))
    }

    fn open_existing(path: &Path) -> anyhow::Result<Self> {
        Ok(Self(<SimpleFile as FileIo>::open_existing(path)?))
    }

    fn read_at(&self, offset: u64, buf: &mut [u8]) -> anyhow::Result<()> {
        self.0.read_at(offset, buf)
    }

    fn write_at(&mut self, offset: u64, data: &[u8]) -> anyhow::Result<()> {
        if offset + data.len() as u64 > DISK {
            return Err(std::io::Error::from(std::io::ErrorKind::StorageFull).into());
        }
        self.0.write_at(offset, data)
    }

    fn sync(&mut self) -> anyhow::Result<()> {
        DATA_SYNCS[ID].fetch_add(1, Ordering::SeqCst);
        self.0.sync()
    }

    fn len(&self) -> anyhow::Result<u64> {
        self.0.len()
    }

    fn set_len(&mut self, len: u64) -> anyhow::Result<()> {
        anyhow::ensure!(len <= self.0.len()?, "sparse growth to {len} bytes");
        self.0.set_len(len)
    }
}

fn options(
    dir: &std::path::Path,
    max_file_size: u64,
    preallocation_chunk_size: Option<u64>,
) -> WriteAheadOptions {
    WriteAheadOptions {
        log_dir: dir.to_path_buf(),
        max_file_size,
        preallocation_chunk_size,
        ..Default::default()
    }
}

#[tokio::test]
async fn default_configured_and_disabled_allocation_windows() {
    let default_dir = tempfile::tempdir().unwrap();
    let mut default_wal = WriteAhead::<SimpleFile>::with_options(WriteAheadOptions {
        log_dir: default_dir.path().to_path_buf(),
        ..Default::default()
    });
    default_wal.start().unwrap();
    assert_eq!(
        std::fs::metadata(default_dir.path().join("0000000000.log"))
            .unwrap()
            .len(),
        256 * 1024
    );

    let configured_dir = tempfile::tempdir().unwrap();
    let mut configured_wal = WriteAhead::<SimpleFile>::with_options(options(
        configured_dir.path(),
        100_000,
        Some(16_384),
    ));
    configured_wal.start().unwrap();
    assert_eq!(
        std::fs::metadata(configured_dir.path().join("0000000000.log"))
            .unwrap()
            .len(),
        16_384
    );
    configured_wal
        .write_batch(vec![vec![1; 5_000]])
        .await
        .unwrap();
    assert_eq!(
        std::fs::metadata(configured_dir.path().join("0000000000.log"))
            .unwrap()
            .len(),
        32_768
    );

    let disabled_dir = tempfile::tempdir().unwrap();
    let mut disabled_wal =
        WriteAhead::<SimpleFile>::with_options(options(disabled_dir.path(), 100_000, None));
    disabled_wal.start().unwrap();
    assert_eq!(
        std::fs::metadata(disabled_dir.path().join("0000000000.log"))
            .unwrap()
            .len(),
        FILE_HEADER_SIZE
    );
    disabled_wal.write_batch(vec![vec![1; 20]]).await.unwrap();
    assert_eq!(
        std::fs::metadata(disabled_dir.path().join("0000000000.log"))
            .unwrap()
            .len(),
        FILE_HEADER_SIZE + RECORD_HEADER_SIZE + 20
    );

    let zero_dir = tempfile::tempdir().unwrap();
    let mut zero_wal =
        WriteAhead::<SimpleFile>::with_options(options(zero_dir.path(), 100_000, Some(0)));
    zero_wal.start().unwrap();
    assert_eq!(
        std::fs::metadata(zero_dir.path().join("0000000000.log"))
            .unwrap()
            .len(),
        FILE_HEADER_SIZE
    );
    zero_wal.write_batch(vec![vec![1; 20]]).await.unwrap();
    assert_eq!(
        std::fs::metadata(zero_dir.path().join("0000000000.log"))
            .unwrap()
            .len(),
        FILE_HEADER_SIZE + RECORD_HEADER_SIZE + 20
    );
}

#[tokio::test]
async fn steady_state_commit_uses_one_data_sync() {
    let dir = tempfile::tempdir().unwrap();
    let mut wal =
        WriteAhead::<CountingFile<0>>::with_options(options(dir.path(), 65_536, Some(16_384)));
    wal.start().unwrap();
    let before = DATA_SYNCS[0].load(Ordering::SeqCst);

    wal.write_batch(vec![b"one sync".to_vec()]).await.unwrap();

    assert_eq!(DATA_SYNCS[0].load(Ordering::SeqCst) - before, 1);
}

#[tokio::test]
async fn windows_are_zero_filled_across_growth_and_rotation() {
    let dir = tempfile::tempdir().unwrap();
    let max_file_size = 40_000;
    let mut wal = WriteAhead::<CountingFile<1>>::with_options(options(
        dir.path(),
        max_file_size,
        Some(16_384),
    ));
    wal.start().unwrap();
    let active_len = |id: u64| {
        std::fs::metadata(dir.path().join(format!("{id:010}.log")))
            .unwrap()
            .len()
    };
    assert_eq!(active_len(0), 16_384);

    // Cross into the second window, then into the clamped third.
    let mut ids = Vec::new();
    for fill in 1..=3u8 {
        ids.extend(wal.write_batch(vec![vec![fill; 9_000]]).await.unwrap());
    }
    assert!(ids.iter().all(|id| id.file_id == 0));
    assert_eq!(active_len(0), max_file_size);

    // The rotated file gets its own windows: this record ends in its second.
    let rotated = wal.write_batch(vec![vec![4; 9_000]]).await.unwrap()[0];
    assert_eq!(rotated.file_id, 1);
    assert_eq!(active_len(1), 32_768);

    for (fill, id) in (1..=4u8).zip(ids.iter().chain([&rotated])) {
        assert_eq!(
            wal.read(id.file_id, id.file_offset).unwrap(),
            vec![fill; 9_000]
        );
    }
}

#[tokio::test]
async fn full_disk_leaves_window_unfilled_without_failing_writes() {
    // Room for the header and one small record, but not the window.
    type NearlyFull = CountingFile<2, { FILE_HEADER_SIZE + 1_000 }>;
    let dir = tempfile::tempdir().unwrap();
    let opts = options(dir.path(), 65_536, Some(16_384));
    let id = {
        let mut wal = WriteAhead::<NearlyFull>::with_options(opts.clone());
        wal.start().unwrap();
        wal.write_batch(vec![b"fits".to_vec()]).await.unwrap()[0]
    };

    let mut wal = WriteAhead::<NearlyFull>::with_options(opts);
    wal.start().unwrap();
    assert_eq!(wal.read(id.file_id, id.file_offset).unwrap(), b"fits");
}

#[tokio::test]
async fn rotates_before_overflow_and_truncates_sealed_allocation_tail() {
    let dir = tempfile::tempdir().unwrap();
    let max_file_size = FILE_HEADER_SIZE + 150;
    let mut wal =
        WriteAhead::<SimpleFile>::with_options(options(dir.path(), max_file_size, Some(4_096)));
    wal.start().unwrap();

    let first = wal.write_batch(vec![vec![1; 80]]).await.unwrap()[0];
    let second = wal.write_batch(vec![vec![2; 80]]).await.unwrap()[0];
    assert_eq!(first.file_id, 0);
    assert_eq!(second.file_id, 1, "second commit must rotate first");

    let first_records_end = FILE_HEADER_SIZE + RECORD_HEADER_SIZE + 80;
    let first_path = dir.path().join("0000000000.log");
    assert_eq!(
        std::fs::metadata(&first_path).unwrap().len(),
        first_records_end + FOOTER_SIZE,
        "sealed file must not retain its allocation window"
    );
    let first_log: Logfile<SimpleFile> = Logfile::open(&first_path).unwrap();
    assert!(first_log.sealed);
    assert_eq!(first_log.records_end().unwrap(), first_records_end);
}

#[tokio::test]
async fn restart_preserves_empty_records_and_ignores_unused_tail() {
    let dir = tempfile::tempdir().unwrap();
    let opts = options(dir.path(), 65_536, Some(16_384));
    let ids = {
        let mut wal = WriteAhead::<SimpleFile>::with_options(opts.clone());
        wal.start().unwrap();
        let ids = wal
            .write_batch(vec![Vec::new(), b"x".to_vec()])
            .await
            .unwrap();
        assert_eq!(
            std::fs::metadata(dir.path().join("0000000000.log"))
                .unwrap()
                .len(),
            16_384
        );
        ids
    };

    let mut wal = WriteAhead::<SimpleFile>::with_options(opts);
    wal.start().unwrap();
    assert_eq!(wal.read(ids[0].file_id, ids[0].file_offset).unwrap(), b"");
    assert_eq!(wal.read(ids[1].file_id, ids[1].file_offset).unwrap(), b"x");

    let next = wal.write_batch(vec![b"y".to_vec()]).await.unwrap()[0];
    assert_eq!(
        next.file_offset,
        ids[1].file_offset + RECORD_HEADER_SIZE + 1
    );

    let mut stream = wal.create_stream().unwrap();
    let mut records = Vec::new();
    while let Some(record) = stream.next().await {
        records.push(record.unwrap().1);
    }
    assert_eq!(records, vec![Vec::new(), b"x".to_vec(), b"y".to_vec()]);
}

#[tokio::test]
async fn oversized_batch_stays_contiguous_then_rotates() {
    let dir = tempfile::tempdir().unwrap();
    let tiny_max = FILE_HEADER_SIZE - 1;
    let mut wal =
        WriteAhead::<SimpleFile>::with_options(options(dir.path(), tiny_max, Some(4_096)));
    wal.start().unwrap();
    assert_eq!(
        std::fs::metadata(dir.path().join("0000000000.log"))
            .unwrap()
            .len(),
        FILE_HEADER_SIZE,
        "a max below the header must never shrink the header"
    );

    let oversized = wal
        .write_batch(vec![vec![1; 40], vec![2; 40]])
        .await
        .unwrap();
    assert!(
        oversized.iter().all(|id| id.file_id == 0),
        "an oversized batch must not be split"
    );
    assert_eq!(
        oversized[1].file_offset,
        oversized[0].file_offset + RECORD_HEADER_SIZE + 40
    );

    // This submission cannot run until the writer's post-commit rotation has
    // completed, making the oversized-file behavior deterministic.
    let next = wal.write_batch(vec![b"next".to_vec()]).await.unwrap()[0];
    assert_eq!(next.file_id, 1);
    assert_eq!(
        std::fs::metadata(dir.path().join("0000000000.log"))
            .unwrap()
            .len(),
        FILE_HEADER_SIZE + 2 * (RECORD_HEADER_SIZE + 40) + FOOTER_SIZE
    );
}
