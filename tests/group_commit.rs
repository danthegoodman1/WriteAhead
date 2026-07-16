//! Group commit correctness: concurrent writers submitting to one writer
//! actor all get unique offsets and durable, intact records.

use writeahead::logfile::{LogFileWriter, Logfile, WriterCommand};
use writeahead::SimpleFile;

#[test]
fn concurrent_writers_get_unique_intact_records() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("0000000000.log");
    let writer = LogFileWriter::<SimpleFile>::launch(&path).unwrap();

    let mut handles = Vec::new();
    for t in 0..8 {
        let writer = writer.clone();
        handles.push(std::thread::spawn(move || {
            let mut acked = Vec::new();
            for i in 0..200 {
                let payload = format!("thread {t} record {i}").into_bytes();
                let (tx, rx) = flume::bounded(1);
                writer
                    .send(WriterCommand::Write(tx, vec![payload.clone()]))
                    .unwrap();
                let response = rx.recv().unwrap().unwrap();
                acked.push((response.offsets[0], payload));
            }
            acked
        }));
    }
    let mut all = Vec::new();
    for h in handles {
        all.extend(h.join().unwrap());
    }

    // Coalesced writes must still hand out disjoint offsets
    let mut offsets: Vec<u64> = all.iter().map(|(o, _)| *o).collect();
    offsets.sort_unstable();
    offsets.dedup();
    assert_eq!(offsets.len(), all.len(), "duplicate record offsets");

    // And every acknowledged record reads back intact
    let logfile: Logfile<SimpleFile> = Logfile::open(&path).unwrap();
    for (offset, payload) in &all {
        assert_eq!(&logfile.read_record(*offset).unwrap(), payload);
    }
}
