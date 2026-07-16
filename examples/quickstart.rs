//! The README quickstart, verbatim and runnable: `cargo run --example quickstart`

use futures::StreamExt;
use std::sync::Arc;
use writeahead::{SimpleFile, WriteAhead, WriteAheadOptions};

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let mut wal = WriteAhead::<SimpleFile>::with_options(WriteAheadOptions {
        log_dir: "./write_ahead".into(),
        ..Default::default()
    });
    wal.start()?; // recovers existing state; if this errors, crash
    let wal = Arc::new(wal); // WriteAhead is Sync: share it directly

    // Durable writes from any task or thread: clone a WriteHandle per
    // producer. Writes that arrive while an fsync is in flight coalesce
    // into one group commit automatically.
    let handle = wal.writer()?;
    let id = handle.write(b"hello".to_vec()).await?;
    let more = handle
        .write_batch(vec![b"a".to_vec(), b"b".to_vec()])
        .await?;
    println!("wrote {:?} and {:?}", id, more);

    // Point read by address
    let record = wal.read(id.file_id, id.file_offset)?;
    assert_eq!(record, b"hello");

    // Full replay, oldest first; each record comes with its address so
    // consumers can checkpoint how far they've gotten
    let mut stream = wal.create_stream()?;
    while let Some(entry) = stream.next().await {
        let (id, record) = entry?;
        println!("replayed {} bytes from {:?}", record.len(), id);
    }

    Ok(())
}
