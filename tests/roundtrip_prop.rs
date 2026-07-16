//! Property test: arbitrary payload batches roundtrip through read-by-id and
//! full stream replay, across file rotations.

use futures::StreamExt;
use proptest::prelude::*;
use writeahead::{SimpleFile, WriteAhead, WriteAheadOptions};

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]

    #[test]
    fn roundtrip_arbitrary_batches(
        batches in prop::collection::vec(
            prop::collection::vec(prop::collection::vec(any::<u8>(), 0..300), 1..8),
            1..6,
        )
    ) {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let dir = tempfile::tempdir().unwrap();
            let mut wal = WriteAhead::<SimpleFile>::with_options(WriteAheadOptions {
                log_dir: dir.path().to_path_buf(),
                max_file_size: 512, // force rotations
                ..Default::default()
            });
            wal.start().unwrap();

            let mut expected: Vec<Vec<u8>> = Vec::new();
            let mut ids = Vec::new();
            for batch in &batches {
                ids.extend(wal.write_batch(batch.clone()).await.unwrap());
                expected.extend(batch.iter().cloned());
            }

            // Read back by id
            for (id, want) in ids.iter().zip(&expected) {
                let got = wal.read(id.file_id, id.file_offset).unwrap();
                prop_assert_eq!(&got, want);
            }

            // Full stream replay
            let mut stream = wal.create_stream().unwrap();
            let mut got = Vec::new();
            while let Some(r) = stream.next().await {
                got.push(r.unwrap());
            }
            prop_assert_eq!(got, expected);
            Ok(())
        })?;
    }
}
