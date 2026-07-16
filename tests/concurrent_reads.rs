//! Positional reads share one file handle safely across threads (no seek cursor).

use std::sync::Arc;
use writeahead::logfile::Logfile;
use writeahead::{SimpleFile, WriteAhead, WriteAheadOptions};

#[tokio::test]
async fn concurrent_readers_share_a_handle() {
    let dir = tempfile::tempdir().unwrap();
    let mut wal = WriteAhead::<SimpleFile>::with_options(WriteAheadOptions {
        log_dir: dir.path().to_path_buf(),
        ..Default::default()
    });
    wal.start().unwrap();

    let records: Vec<Vec<u8>> = (0..500)
        .map(|i| format!("record_{i}").into_bytes())
        .collect();
    let ids = wal.write_batch(records.clone()).await.unwrap();
    drop(wal);

    let logfile: Arc<Logfile<SimpleFile>> =
        Arc::new(Logfile::open(&dir.path().join("0000000000.log")).unwrap());

    let mut handles = Vec::new();
    for t in 0..4 {
        let lf = Arc::clone(&logfile);
        let ids: Vec<u64> = ids.iter().map(|id| id.file_offset).collect();
        let expected = records.clone();
        handles.push(std::thread::spawn(move || {
            // Interleave differently per thread to force cursor contention
            // (which positional reads must not have)
            for (i, offset) in ids.iter().enumerate().skip(t % 4) {
                assert_eq!(lf.read_record(*offset).unwrap(), expected[i]);
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
}
