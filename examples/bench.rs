//! Benchmark harness: run with `cargo run --release --example bench`.
//!
//! Prints one line per scenario: name, total time, and derived rate, so
//! before/after comparisons are a diff of two runs.

use std::time::Instant;
use writeahead::logfile::{LogFileWriter, WriterCommand};
use writeahead::{SimpleFile, WriteAhead, WriteAheadOptions};

const SEQ_WRITES: usize = 2_000;
const BATCHES: usize = 100;
const BATCH_SIZE: usize = 1_000;
const CONCURRENT_THREADS: usize = 8;
const CONCURRENT_WRITES_PER_THREAD: usize = 250;
const PAYLOAD_LEN: usize = 64;

fn payload(i: usize) -> Vec<u8> {
    let mut p = format!("record payload number {i:012} ").into_bytes();
    p.resize(PAYLOAD_LEN, b'x');
    p
}

fn report(name: &str, total: std::time::Duration, ops: usize, bytes: usize) {
    let per_op_us = total.as_secs_f64() * 1e6 / ops as f64;
    let ops_per_s = ops as f64 / total.as_secs_f64();
    let mb_per_s = bytes as f64 / (1024.0 * 1024.0) / total.as_secs_f64();
    println!(
        "{name:<24} total={total:>12.3?} ops={ops:>7} per_op={per_op_us:>9.2}us rate={ops_per_s:>12.0}/s throughput={mb_per_s:>8.2}MB/s"
    );
}

fn wal_in(dir: &std::path::Path) -> WriteAhead<SimpleFile> {
    WriteAhead::with_options(WriteAheadOptions {
        log_dir: dir.to_path_buf(),
        ..Default::default()
    })
}

/// One record per write_batch call: latency of the full ack path (fsync-bound).
async fn bench_seq_write(dir: &std::path::Path) {
    let mut wal = wal_in(dir);
    wal.start().unwrap();
    let start = Instant::now();
    for i in 0..SEQ_WRITES {
        wal.write_batch(vec![payload(i)]).await.unwrap();
    }
    report(
        "seq_write_1rec",
        start.elapsed(),
        SEQ_WRITES,
        SEQ_WRITES * PAYLOAD_LEN,
    );
}

/// Large batches: encode + single write + single fsync per batch.
async fn bench_batch_write(
    dir: &std::path::Path,
) -> (WriteAhead<SimpleFile>, Vec<writeahead::RecordID>) {
    let mut wal = wal_in(dir);
    wal.start().unwrap();
    let data: Vec<Vec<Vec<u8>>> = (0..BATCHES)
        .map(|b| {
            (0..BATCH_SIZE)
                .map(|i| payload(b * BATCH_SIZE + i))
                .collect()
        })
        .collect();
    let mut ids = Vec::with_capacity(BATCHES * BATCH_SIZE);
    let start = Instant::now();
    for batch in data {
        ids.extend(wal.write_batch(batch).await.unwrap());
    }
    report(
        "batch_write",
        start.elapsed(),
        BATCHES * BATCH_SIZE,
        BATCHES * BATCH_SIZE * PAYLOAD_LEN,
    );
    (wal, ids)
}

/// Point reads of every record written by bench_batch_write.
fn bench_read_by_id(wal: &WriteAhead<SimpleFile>, ids: &[writeahead::RecordID]) {
    let start = Instant::now();
    for id in ids {
        let rec = wal.read(id.file_id, id.file_offset).unwrap();
        assert_eq!(rec.len(), PAYLOAD_LEN);
    }
    report(
        "read_by_id",
        start.elapsed(),
        ids.len(),
        ids.len() * PAYLOAD_LEN,
    );
}

/// Full sequential replay through the stream API.
async fn bench_stream_replay(wal: &WriteAhead<SimpleFile>, expected: usize) {
    use futures::StreamExt;
    let start = Instant::now();
    let mut stream = wal.create_stream().unwrap();
    let mut n = 0usize;
    while let Some(r) = stream.next().await {
        n += r.unwrap().len();
    }
    assert_eq!(n, expected * PAYLOAD_LEN);
    report(
        "stream_replay",
        start.elapsed(),
        expected,
        expected * PAYLOAD_LEN,
    );
}

/// N threads submitting single-record writes concurrently to one writer
/// actor: measures fsync coalescing (group commit).
fn bench_actor_concurrent(dir: &std::path::Path) {
    std::fs::create_dir_all(dir).unwrap();
    let path = dir.join("0000000000.log");
    let writer = LogFileWriter::<SimpleFile>::launch(&path).unwrap();
    let start = Instant::now();
    let mut handles = Vec::new();
    for t in 0..CONCURRENT_THREADS {
        let writer = writer.clone();
        handles.push(std::thread::spawn(move || {
            for i in 0..CONCURRENT_WRITES_PER_THREAD {
                let (tx, rx) = flume::bounded(1);
                writer
                    .send(WriterCommand::Write(
                        tx,
                        vec![payload(t * CONCURRENT_WRITES_PER_THREAD + i)],
                    ))
                    .unwrap();
                rx.recv().unwrap().unwrap();
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    let ops = CONCURRENT_THREADS * CONCURRENT_WRITES_PER_THREAD;
    report(
        "actor_concurrent_8w",
        start.elapsed(),
        ops,
        ops * PAYLOAD_LEN,
    );
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    println!(
        "writeahead bench: {} seq writes, {}x{} batch, {} threads x {} concurrent, {}B payloads",
        SEQ_WRITES, BATCHES, BATCH_SIZE, CONCURRENT_THREADS, CONCURRENT_WRITES_PER_THREAD, PAYLOAD_LEN
    );

    // Run on the real filesystem, not /tmp: tmpfs makes fsync free and
    // would wildly overstate write performance.
    let dir = tempfile::tempdir_in(".").unwrap();
    bench_seq_write(&dir.path().join("seq")).await;

    let batch_dir = dir.path().join("batch");
    let (wal, ids) = bench_batch_write(&batch_dir).await;
    bench_read_by_id(&wal, &ids);
    bench_stream_replay(&wal, BATCHES * BATCH_SIZE).await;

    bench_actor_concurrent(&dir.path().join("actor"));
}
