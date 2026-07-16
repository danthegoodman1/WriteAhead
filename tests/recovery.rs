//! Crash-recovery kill-point matrix: each test crafts the on-disk state a
//! crash would leave behind, then verifies `start()` restores a consistent
//! WAL where every acknowledged record is readable and new writes work.

use futures::StreamExt;
use writeahead::logfile::{recover_and_seal, FILE_HEADER_SIZE, FOOTER_SIZE};
use writeahead::{SimpleFile, WriteAhead, WriteAheadOptions};

fn opts(dir: &std::path::Path) -> WriteAheadOptions {
    WriteAheadOptions {
        log_dir: dir.to_path_buf(),
        ..Default::default()
    }
}

async fn write_str(wal: &mut WriteAhead<SimpleFile>, s: &str) -> writeahead::RecordID {
    wal.write_batch(vec![s.as_bytes().to_vec()])
        .await
        .unwrap()
        .remove(0)
}

async fn collect_stream(wal: &WriteAhead<SimpleFile>) -> Vec<Vec<u8>> {
    let mut out = Vec::new();
    let mut stream = wal.create_stream().unwrap();
    while let Some(r) = stream.next().await {
        out.push(r.unwrap());
    }
    out
}

#[tokio::test]
async fn restart_resumes_appending() {
    let dir = tempfile::tempdir().unwrap();
    let mut wal = WriteAhead::<SimpleFile>::with_options(opts(dir.path()));
    wal.start().unwrap();
    let id1 = write_str(&mut wal, "before restart").await;
    drop(wal);

    let mut wal = WriteAhead::<SimpleFile>::with_options(opts(dir.path()));
    wal.start().unwrap();
    let id2 = write_str(&mut wal, "after restart").await;

    assert_eq!(
        wal.read(id1.file_id, id1.file_offset).unwrap(),
        b"before restart"
    );
    assert_eq!(
        wal.read(id2.file_id, id2.file_offset).unwrap(),
        b"after restart"
    );
    assert_eq!(collect_stream(&wal).await.len(), 2);
}

#[tokio::test]
async fn crash_after_seal_before_next_file_rotates() {
    // Simulate: last file is sealed but the next file was never created.
    let dir = tempfile::tempdir().unwrap();
    let mut wal = WriteAhead::<SimpleFile>::with_options(opts(dir.path()));
    wal.start().unwrap();
    let id1 = write_str(&mut wal, "sealed away").await;
    drop(wal);

    // Seal file 0 out-of-band (as rotation would, crashing right after)
    let path = dir.path().join("0000000000.log");
    recover_and_seal::<SimpleFile>(&path, 1).unwrap();

    let mut wal = WriteAhead::<SimpleFile>::with_options(opts(dir.path()));
    wal.start().unwrap();

    // Must NOT have appended to the sealed file: new writes go to file 1
    let id2 = write_str(&mut wal, "fresh file").await;
    assert_eq!(id2.file_id, 1, "writer must rotate off a sealed file");
    assert_eq!(
        wal.read(id1.file_id, id1.file_offset).unwrap(),
        b"sealed away"
    );
    let all = collect_stream(&wal).await;
    assert_eq!(all, vec![b"sealed away".to_vec(), b"fresh file".to_vec()]);
}

#[tokio::test]
async fn crash_mid_record_truncates_torn_tail() {
    let dir = tempfile::tempdir().unwrap();
    let mut wal = WriteAhead::<SimpleFile>::with_options(opts(dir.path()));
    wal.start().unwrap();
    let id1 = write_str(&mut wal, "acknowledged").await;
    write_str(&mut wal, "will be torn").await;
    drop(wal);

    // Tear the last record mid-write
    let path = dir.path().join("0000000000.log");
    let len = std::fs::metadata(&path).unwrap().len();
    let f = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
    f.set_len(len - 5).unwrap();

    let mut wal = WriteAhead::<SimpleFile>::with_options(opts(dir.path()));
    wal.start().unwrap();

    // The torn record is gone, the acknowledged prefix survives, and the
    // file accepts new appends.
    assert_eq!(
        wal.read(id1.file_id, id1.file_offset).unwrap(),
        b"acknowledged"
    );
    let id3 = write_str(&mut wal, "appended after recovery").await;
    assert_eq!(id3.file_id, 0);
    let all = collect_stream(&wal).await;
    assert_eq!(
        all,
        vec![
            b"acknowledged".to_vec(),
            b"appended after recovery".to_vec()
        ]
    );
}

#[tokio::test]
async fn crash_before_header_heals_empty_active_file() {
    // File 0 is sealed and complete; file 1 was created but never got its
    // header (0 bytes). Recovery should adopt file 1 as the active file.
    let dir = tempfile::tempdir().unwrap();
    let mut wal = WriteAhead::<SimpleFile>::with_options(opts(dir.path()));
    wal.start().unwrap();
    let id1 = write_str(&mut wal, "in file zero").await;
    drop(wal);

    recover_and_seal::<SimpleFile>(&dir.path().join("0000000000.log"), 1).unwrap();
    std::fs::write(dir.path().join("0000000001.log"), b"").unwrap();

    let mut wal = WriteAhead::<SimpleFile>::with_options(opts(dir.path()));
    wal.start().unwrap();

    let id2 = write_str(&mut wal, "in file one").await;
    assert_eq!(id2.file_id, 1);
    assert_eq!(id2.file_offset, FILE_HEADER_SIZE);
    assert_eq!(
        wal.read(id1.file_id, id1.file_offset).unwrap(),
        b"in file zero"
    );
    assert_eq!(collect_stream(&wal).await.len(), 2);
}

#[tokio::test]
async fn torn_footer_on_non_active_file_is_healed() {
    // File 0's seal footer was torn mid-write during rotation, but file 1
    // already exists. Recovery should re-seal file 0.
    let dir = tempfile::tempdir().unwrap();
    let mut wal = WriteAhead::<SimpleFile>::with_options(opts(dir.path()));
    wal.start().unwrap();
    let id1 = write_str(&mut wal, "old file record").await;
    drop(wal);

    let path0 = dir.path().join("0000000000.log");
    recover_and_seal::<SimpleFile>(&path0, 1).unwrap();
    // Tear half the footer off
    let len = std::fs::metadata(&path0).unwrap().len();
    let f = std::fs::OpenOptions::new()
        .write(true)
        .open(&path0)
        .unwrap();
    f.set_len(len - FOOTER_SIZE / 2).unwrap();
    // File 1 exists with records
    std::fs::write(dir.path().join("0000000001.log"), b"").unwrap();

    let mut wal = WriteAhead::<SimpleFile>::with_options(opts(dir.path()));
    wal.start().unwrap();

    let id2 = write_str(&mut wal, "active file record").await;
    assert_eq!(id2.file_id, 1);
    assert_eq!(
        wal.read(id1.file_id, id1.file_offset).unwrap(),
        b"old file record"
    );
    let all = collect_stream(&wal).await;
    assert_eq!(
        all,
        vec![b"old file record".to_vec(), b"active file record".to_vec()]
    );
}

#[tokio::test]
async fn stray_files_are_skipped() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("notes.txt"), b"hi").unwrap();
    std::fs::write(dir.path().join(".hidden"), b"hi").unwrap();
    std::fs::write(dir.path().join("abc.log"), b"not a wal file").unwrap();

    let mut wal = WriteAhead::<SimpleFile>::with_options(opts(dir.path()));
    wal.start().unwrap();
    let id = write_str(&mut wal, "works").await;
    assert_eq!(wal.read(id.file_id, id.file_offset).unwrap(), b"works");
}
