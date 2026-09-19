//! Count allocations on the calling thread only; background writes and the
//! test harness are outside the measured recovery region.
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::path::Path;
use writeahead::{FileIo, SimpleFile, WriteAhead, WriteAheadOptions};

thread_local! {
    static ALLOCATIONS: Cell<Option<usize>> = const { Cell::new(None) };
    static READS: Cell<usize> = const { Cell::new(0) };
}

struct AllocationCounter;
fn count_allocation() {
    let _ = ALLOCATIONS.try_with(|n| {
        if let Some(value) = n.get() {
            n.set(Some(value + 1));
        }
    });
}

unsafe impl GlobalAlloc for AllocationCounter {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        count_allocation();
        unsafe { System.alloc(layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        count_allocation();
        unsafe { System.alloc_zeroed(layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        count_allocation();
        unsafe { System.realloc(ptr, layout, size) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static ALLOCATOR: AllocationCounter = AllocationCounter;

#[derive(Debug)]
struct CountedFile(SimpleFile);
impl FileIo for CountedFile {
    fn open(path: &Path) -> anyhow::Result<Self> {
        Ok(Self(SimpleFile::open(path)?))
    }
    fn open_existing(path: &Path) -> anyhow::Result<Self> {
        Ok(Self(SimpleFile::open_existing(path)?))
    }
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> anyhow::Result<()> {
        READS.with(|n| n.set(n.get() + 1));
        self.0.read_at(offset, buf)
    }
    fn write_at(&mut self, offset: u64, data: &[u8]) -> anyhow::Result<()> {
        self.0.write_at(offset, data)
    }
    fn sync(&mut self) -> anyhow::Result<()> {
        self.0.sync()
    }
    fn len(&self) -> anyhow::Result<u64> {
        self.0.len()
    }
    fn set_len(&mut self, len: u64) -> anyhow::Result<()> {
        self.0.set_len(len)
    }
}

fn recovery_cost(records: usize) -> (usize, usize) {
    let dir = tempfile::tempdir().unwrap();
    let mut wal = WriteAhead::<SimpleFile>::with_options(WriteAheadOptions {
        log_dir: dir.path().into(),
        preallocation_chunk_size: None,
        ..Default::default()
    });
    wal.start().unwrap();
    futures::executor::block_on(wal.write_batch(vec![vec![1; 64]; records])).unwrap();
    drop(wal);
    let path = dir.path().join("0000000000.log");
    READS.with(|n| n.set(0));
    ALLOCATIONS.with(|n| n.set(Some(0)));
    let result = writeahead::logfile::recover_unsealed::<CountedFile>(&path);
    let allocations = ALLOCATIONS.with(|n| n.replace(None).unwrap());
    let reads = READS.with(Cell::get);
    assert_eq!(
        result.unwrap(),
        writeahead::logfile::FILE_HEADER_SIZE + records as u64 * 84
    );
    (reads, allocations)
}

#[test]
fn recovery_reads_chunks_and_allocations_do_not_scale_with_record_count() {
    let small = recovery_cost(1);
    let large = recovery_cost(1000);
    println!(
        "recovery 1 record: {} reads, {} allocations; 1000 records: {} reads, {} allocations",
        small.0, small.1, large.0, large.1
    );
    assert!(large.0 <= 16, "record-at-a-time recovery: {large:?}");
    assert!(
        large.1 <= small.1 + 1,
        "per-record recovery allocations: {small:?} -> {large:?}"
    );
}
