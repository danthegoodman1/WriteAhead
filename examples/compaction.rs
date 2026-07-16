//! DB-traditional explicit compaction: the WAL holds `key=value` puts and an
//! in-memory index maps each key to the address of its latest value.
//!
//! Cold keys written once early get stranded in the oldest files, pinning
//! them while dead versions of hot keys pile up around them. Compaction
//! rewrites the live records to the head of the log, repoints the index at
//! the new copies, then trims everything below the cutoff.
//!
//! Run with: cargo run --example compaction

use std::collections::HashMap;
use std::sync::Arc;
use writeahead::{RecordID, SimpleFile, WriteAhead, WriteAheadOptions};

fn dir_stats(dir: &std::path::Path) -> (u64, usize) {
    let entries: Vec<_> = std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap())
        .collect();
    let bytes = entries.iter().map(|e| e.metadata().unwrap().len()).sum();
    (bytes, entries.len())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let mut wal = WriteAhead::<SimpleFile>::with_options(WriteAheadOptions {
        log_dir: dir.path().to_path_buf(),
        max_file_size: 8 * 1024, // small files so the demo rotates often
        ..Default::default()
    });
    wal.start()?;
    let wal = Arc::new(wal);
    let handle = wal.writer()?;

    let mut index: HashMap<String, RecordID> = HashMap::new();
    let mut expected: HashMap<String, String> = HashMap::new();
    let put_batch = |index: &mut HashMap<String, RecordID>, keys: &[String], ids: Vec<RecordID>| {
        for (key, id) in keys.iter().cloned().zip(ids) {
            index.insert(key, id);
        }
    };

    // Initial load: 100 keys, written once
    let all_keys: Vec<String> = (0..100).map(|k| format!("key-{k:03}")).collect();
    let batch: Vec<Vec<u8>> = all_keys
        .iter()
        .map(|k| {
            expected.insert(k.clone(), format!("{k}=v0"));
            format!("{k}=v0").into_bytes()
        })
        .collect();
    let ids = handle.write_batch(batch).await?;
    put_batch(&mut index, &all_keys, ids);

    // Then a hot subset gets overwritten constantly: 49 of every hot key's
    // 50 versions are dead weight, and the cold keys' single live versions
    // pin the oldest file
    let hot_keys: Vec<String> = (0..10).map(|k| format!("key-{k:03}")).collect();
    for round in 1..=50 {
        let batch: Vec<Vec<u8>> = hot_keys
            .iter()
            .map(|k| {
                expected.insert(k.clone(), format!("{k}=v{round}"));
                format!("{k}=v{round}").into_bytes()
            })
            .collect();
        let ids = handle.write_batch(batch).await?;
        put_batch(&mut index, &hot_keys, ids);
    }
    let (bytes, files) = dir_stats(dir.path());
    println!(
        "before compaction: {} KiB across {} files ({} live keys)",
        bytes / 1024,
        files,
        index.len(),
    );

    // --- Explicit compaction ---
    // 1. Cutoff: the newest file the index points into. Live records below
    //    it will be rewritten; everything else below it is dead.
    let cutoff = index.values().map(|id| id.file_id).max().unwrap();

    // 2. Rewrite the live records sitting below the cutoff to the head of
    //    the log (they land in the active file, at or above the cutoff) and
    //    repoint the index. RecordIDs change — that's the point: the index
    //    is the source of truth for where a key lives now.
    let stale_keys: Vec<String> = index
        .iter()
        .filter(|(_, id)| id.file_id < cutoff)
        .map(|(key, _)| key.clone())
        .collect();
    let live_values: Vec<Vec<u8>> = stale_keys
        .iter()
        .map(|key| {
            let id = index[key];
            wal.read(id.file_id, id.file_offset)
        })
        .collect::<anyhow::Result<_>>()?;
    let ids = handle.write_batch(live_values).await?;
    put_batch(&mut index, &stale_keys, ids);

    // 3. Nothing below the cutoff is referenced anymore: drop it.
    let stats = handle.trim_before(cutoff).await?;
    println!(
        "compacted: rewrote {} live records, trimmed {} files, reclaimed {} KiB",
        stale_keys.len(),
        stats.files_deleted,
        stats.bytes_reclaimed / 1024,
    );

    // Every key still reads its latest value through the index
    for (key, id) in &index {
        let value = wal.read(id.file_id, id.file_offset)?;
        assert_eq!(value, expected[key].as_bytes());
    }
    let (bytes, files) = dir_stats(dir.path());
    println!(
        "after compaction: {} KiB across {} files; all {} keys intact",
        bytes / 1024,
        files,
        index.len(),
    );
    Ok(())
}
