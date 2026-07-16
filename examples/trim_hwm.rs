//! The consumer high-water-mark pattern: a consumer replays the WAL and
//! tracks the address of the last record it has applied (its high-water
//! mark). Whenever the mark crosses into a new file, every file below it is
//! fully consumed and can be trimmed — the log only ever holds what still
//! needs replaying.
//!
//! Run with: cargo run --example trim_hwm

use futures::StreamExt;
use std::sync::Arc;
use writeahead::{RecordID, SimpleFile, WriteAhead, WriteAheadOptions};

fn dir_kib(dir: &std::path::Path) -> u64 {
    std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().metadata().unwrap().len())
        .sum::<u64>()
        / 1024
}

/// Stand-in for the consumer's real work (apply to a state machine, index
/// into a store, forward downstream, ...).
fn apply(_record: &[u8]) {}

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let mut wal = WriteAhead::<SimpleFile>::with_options(WriteAheadOptions {
        log_dir: dir.path().to_path_buf(),
        max_file_size: 16 * 1024, // small files so the demo rotates often
        ..Default::default()
    });
    wal.start()?;
    let wal = Arc::new(wal);

    // A producer fills the log across several rotations
    let handle = wal.writer()?;
    for chunk in 0..50u32 {
        let batch: Vec<Vec<u8>> = (0..100)
            .map(|i| format!("event {:05}", chunk * 100 + i).into_bytes())
            .collect();
        handle.write_batch(batch).await?;
    }
    println!("log size before consuming: {} KiB", dir_kib(dir.path()));

    // The consumer replays and remembers its position. In a real system,
    // trim only after the high-water mark itself is durably checkpointed —
    // trimming is what makes a crash-restart replay start at the mark
    // instead of at the beginning.
    let mut hwm: Option<RecordID> = None;
    let mut consumed = 0usize;
    let mut stream = wal.create_stream()?;
    while let Some(entry) = stream.next().await {
        let (id, record) = entry?;
        apply(&record);
        consumed += 1;

        // Crossing into a new file means everything below it is consumed.
        // The stream itself is undisturbed: it holds its own open file
        // handles, so trimming under it is safe.
        if let Some(prev) = hwm {
            if id.file_id > prev.file_id {
                let stats = handle.trim_before(id.file_id).await?;
                if stats.files_deleted > 0 {
                    println!(
                        "hwm entered file {}: trimmed {} file(s), reclaimed {} KiB",
                        id.file_id,
                        stats.files_deleted,
                        stats.bytes_reclaimed / 1024,
                    );
                }
            }
        }
        hwm = Some(id);
    }
    println!(
        "consumed {consumed} records; log size after: {} KiB",
        dir_kib(dir.path())
    );

    // More records arrive while the consumer is away...
    let late: Vec<Vec<u8>> = (0..5)
        .map(|i| format!("late event {i}").into_bytes())
        .collect();
    handle.write_batch(late).await?;

    // ...so after a restart, resume from the checkpointed mark instead of
    // the start (the record at the mark is already applied, so skip it).
    let hwm = hwm.expect("consumed at least one record");
    let mut resume = wal.create_stream_from(hwm.file_id, hwm.file_offset)?;
    let mut pending = 0usize;
    while let Some(entry) = resume.next().await {
        let (id, _record) = entry?;
        if id == hwm {
            continue;
        }
        pending += 1;
    }
    println!("resuming from {hwm:?}: {pending} records left to replay");
    Ok(())
}
