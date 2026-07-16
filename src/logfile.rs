use anyhow::{anyhow, Context, Result};
use futures::Stream;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::task::{Context as TaskContext, Poll};
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{trace, warn};

use crate::fileio::FileIo;
use crate::murmur3::murmur3_128;

/// Logfile represents a log file on disk.
///
/// # Format (version 2)
///
/// ## File layout
///
/// ```text
/// | 8 bytes - magic number | 1 byte - format version | N bytes - records | 40 bytes - seal footer (sealed files only) |
/// ```
///
/// ## Record layout
///
/// ```text
/// | 16 bytes (i64, i64 LE) - murmur3_128 of data | 4 bytes (u32 LE) - data length | N bytes - data (raw) |
/// ```
///
/// ## Seal footer layout
///
/// ```text
/// | 8 bytes - magic number | 8 bytes (u64 LE) - seal timestamp (unix ms) | 8 bytes (u64 LE) - records end offset | 16 bytes - murmur3_128 of the previous 24 bytes |
/// ```
///
/// # Corruption protection
///
/// Record data is stored raw (no byte escaping). Every record carries a
/// 128-bit hash of its data; a corrupted length field is caught either by
/// the bounds check against the record region or by the hash mismatch that
/// follows from reading the wrong extent, so any single corruption in a
/// record is detected (false pass probability ~2^-128).
///
/// A file is sealed iff its trailing 40 bytes parse as a footer whose own
/// hash verifies AND whose stored records-end offset equals
/// `file_len - FOOTER_SIZE`. Record payloads can contain arbitrary bytes
/// (including the magic number) without being mistaken for a footer.
#[derive(Debug)]
pub struct Logfile<F: FileIo> {
    pub id: u64,
    pub sealed: bool,
    /// Unix ms timestamp from the seal footer, when sealed.
    pub seal_timestamp_ms: Option<u64>,
    /// End of the record region. `Some` iff sealed; unsealed files grow.
    records_end: Option<u64>,
    path: PathBuf,
    fio: F,
}

pub const MAGIC_NUMBER: [u8; 8] = [0xff; 8];
pub const FORMAT_VERSION: u8 = 2;
/// Magic number + version byte.
pub const FILE_HEADER_SIZE: u64 = 9;
/// Record hash + length prefix.
pub const RECORD_HEADER_SIZE: u64 = 20;
/// Magic + timestamp + records-end + footer hash.
pub const FOOTER_SIZE: u64 = 40;

#[derive(Debug, thiserror::Error)]
pub enum LogfileError {
    #[error("Logfile is corrupted (record hash mismatch)")]
    Corrupted,

    #[error("Record extends past the end of the record region: partial write or invalid offset")]
    PartialWrite,

    #[error("Cannot write to sealed logfile")]
    WriteToSealed,

    #[error("Invalid magic number")]
    InvalidMagicNumber,

    #[error("Invalid or truncated file header")]
    InvalidHeader,

    #[error("Unsupported format version {0}")]
    UnsupportedVersion(u8),

    #[error("Invalid offset {0}")]
    InvalidOffset(u64),

    #[error("File name is not a numeric log file name: {0}")]
    InvalidFileName(String),

    #[error("Record is too large, must be less than {} bytes", u32::MAX)]
    RecordTooLarge,
}

/// Parses a log file id from a `<digits>.log` file name.
/// Returns `None` for anything else so callers can skip stray files.
pub fn file_id_from_path(path: &Path) -> Option<u64> {
    let name = path.file_name()?.to_str()?;
    let stem = name.strip_suffix(".log")?;
    if stem.is_empty() || !stem.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    stem.parse::<u64>().ok()
}

/// Current unix time in milliseconds — the timestamp format used in seal footers.
pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before unix epoch")
        .as_millis() as u64
}

/// Reads and verifies the record at `offset`, returning the data and the
/// offset of the next record.
fn read_record_at<F: FileIo>(fio: &F, offset: u64, records_end: u64) -> Result<(Vec<u8>, u64)> {
    if offset < FILE_HEADER_SIZE {
        return Err(anyhow!(LogfileError::InvalidOffset(offset)));
    }
    let data_start = offset
        .checked_add(RECORD_HEADER_SIZE)
        .ok_or(LogfileError::InvalidOffset(offset))?;
    if data_start > records_end {
        return Err(anyhow!(LogfileError::PartialWrite));
    }

    let mut header = [0u8; RECORD_HEADER_SIZE as usize];
    fio.read_at(offset, &mut header)
        .context("Failed to read record header")?;
    let hash1 = i64::from_le_bytes(header[0..8].try_into().unwrap());
    let hash2 = i64::from_le_bytes(header[8..16].try_into().unwrap());
    let length = u32::from_le_bytes(header[16..20].try_into().unwrap()) as u64;

    // Bounds check before allocating: a corrupted length must not drive a
    // giant allocation or a read past the record region.
    let data_end = data_start
        .checked_add(length)
        .ok_or(LogfileError::PartialWrite)?;
    if data_end > records_end {
        return Err(anyhow!(LogfileError::PartialWrite));
    }

    let mut data = vec![0u8; length as usize];
    fio.read_at(data_start, &mut data)
        .context("Failed to read record data")?;

    let (computed1, computed2) = murmur3_128(&data);
    if computed1 != hash1 || computed2 != hash2 {
        return Err(anyhow!(LogfileError::Corrupted));
    }

    Ok((data, data_end))
}

/// Walks records from the start of the file and returns the end offset of
/// the longest valid prefix. Only meaningful for unsealed files.
fn scan_records_end<F: FileIo>(fio: &F, file_len: u64) -> u64 {
    let mut offset = FILE_HEADER_SIZE;
    loop {
        match read_record_at(fio, offset, file_len) {
            Ok((_, next)) => offset = next,
            Err(_) => return offset,
        }
    }
}

fn build_footer(records_end: u64, ts_ms: u64) -> [u8; FOOTER_SIZE as usize] {
    let mut f = [0u8; FOOTER_SIZE as usize];
    f[0..8].copy_from_slice(&MAGIC_NUMBER);
    f[8..16].copy_from_slice(&ts_ms.to_le_bytes());
    f[16..24].copy_from_slice(&records_end.to_le_bytes());
    let (h1, h2) = murmur3_128(&f[0..24]);
    f[24..32].copy_from_slice(&h1.to_le_bytes());
    f[32..40].copy_from_slice(&h2.to_le_bytes());
    f
}

/// Returns the seal timestamp if `buf` is a valid footer for a file of `file_len` bytes.
fn parse_footer(buf: &[u8; FOOTER_SIZE as usize], file_len: u64) -> Option<u64> {
    if buf[0..8] != MAGIC_NUMBER {
        return None;
    }
    let (h1, h2) = murmur3_128(&buf[0..24]);
    if buf[24..32] != h1.to_le_bytes() || buf[32..40] != h2.to_le_bytes() {
        return None;
    }
    let records_end = u64::from_le_bytes(buf[16..24].try_into().unwrap());
    if records_end != file_len - FOOTER_SIZE {
        return None;
    }
    Some(u64::from_le_bytes(buf[8..16].try_into().unwrap()))
}

fn validate_header<F: FileIo>(fio: &F) -> Result<()> {
    let mut header = [0u8; FILE_HEADER_SIZE as usize];
    fio.read_at(0, &mut header)
        .context("Failed to read file header")?;
    if header[0..8] != MAGIC_NUMBER {
        return Err(anyhow!(LogfileError::InvalidMagicNumber));
    }
    if header[8] != FORMAT_VERSION {
        return Err(anyhow!(LogfileError::UnsupportedVersion(header[8])));
    }
    Ok(())
}

fn write_header<F: FileIo>(fio: &mut F) -> Result<()> {
    let mut header = [0u8; FILE_HEADER_SIZE as usize];
    header[0..8].copy_from_slice(&MAGIC_NUMBER);
    header[8] = FORMAT_VERSION;
    fio.write_at(0, &header).context("Failed to write header")?;
    Ok(())
}

/// Recovers an unsealed file in place: validates the header, scans for the
/// longest valid record prefix, and truncates anything after it (torn
/// writes). Returns the record-region end offset, or 0 for a file that never
/// got a complete header (truncated to empty; the writer re-initializes it).
///
/// Must NOT be called on a sealed file — the scan would treat the footer as
/// a torn record and truncate it off.
pub fn recover_unsealed<F: FileIo>(path: &Path) -> Result<u64> {
    let mut fio = F::open(path)?;
    let len = fio.len()?;
    if len < FILE_HEADER_SIZE {
        if len > 0 {
            warn!(
                "log file {} has a partial header ({} bytes), truncating to empty",
                path.display(),
                len
            );
            fio.set_len(0)?;
            fio.sync()?;
        }
        return Ok(0);
    }
    validate_header(&fio)?;
    let end = scan_records_end(&fio, len);
    if end < len {
        warn!(
            "log file {} has a torn tail: truncating {} -> {}",
            path.display(),
            len,
            end
        );
        fio.set_len(end)?;
        fio.sync()?;
    }
    Ok(end)
}

/// Recovers an unsealed file (see [recover_unsealed]) and then seals it with
/// the given timestamp. Used at startup to heal files that should have been
/// sealed but crashed mid-rotation, and by tests to craft aged files.
pub fn recover_and_seal<F: FileIo>(path: &Path, ts_ms: u64) -> Result<u64> {
    let mut records_end = recover_unsealed::<F>(path)?;
    let mut fio = F::open(path)?;
    if records_end == 0 {
        write_header(&mut fio)?;
        records_end = FILE_HEADER_SIZE;
    }
    fio.write_at(records_end, &build_footer(records_end, ts_ms))?;
    fio.sync()?;
    Ok(records_end)
}

impl<F: FileIo> Logfile<F> {
    /// Opens an existing log file, validating the header and detecting the
    /// seal footer.
    pub fn open(path: &Path) -> Result<Self> {
        let id = file_id_from_path(path)
            .ok_or_else(|| LogfileError::InvalidFileName(path.display().to_string()))?;
        let fio = F::open(path)?;
        let len = fio.len()?;
        if len < FILE_HEADER_SIZE {
            return Err(anyhow!(LogfileError::InvalidHeader));
        }
        validate_header(&fio)?;

        let mut sealed = false;
        let mut seal_timestamp_ms = None;
        let mut records_end = None;
        if len >= FILE_HEADER_SIZE + FOOTER_SIZE {
            let mut fbuf = [0u8; FOOTER_SIZE as usize];
            fio.read_at(len - FOOTER_SIZE, &mut fbuf)?;
            if let Some(ts) = parse_footer(&fbuf, len) {
                sealed = true;
                seal_timestamp_ms = Some(ts);
                records_end = Some(len - FOOTER_SIZE);
            }
        }

        Ok(Self {
            id,
            sealed,
            seal_timestamp_ms,
            records_end,
            path: path.to_path_buf(),
            fio,
        })
    }

    /// Reads and verifies the record at `offset`.
    pub fn read_record(&self, offset: u64) -> Result<Vec<u8>> {
        self.read_record_and_next(offset).map(|(data, _)| data)
    }

    /// Reads the record at `offset` and returns it with the next record's offset.
    pub fn read_record_and_next(&self, offset: u64) -> Result<(Vec<u8>, u64)> {
        trace!(
            "reading record at offset {} sealed: {}",
            offset,
            self.sealed
        );
        read_record_at(&self.fio, offset, self.records_end()?)
    }

    /// End of the record region: fixed for sealed files, the live file
    /// length for unsealed (actively written) files.
    pub fn records_end(&self) -> Result<u64> {
        match self.records_end {
            Some(end) => Ok(end),
            None => self.fio.len(),
        }
    }

    pub fn file_len(&self) -> Result<u64> {
        self.fio.len()
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

pub enum WriterCommand {
    Write(flume::Sender<Result<WriteResponse>>, Vec<Vec<u8>>),
    Seal(flume::Sender<Result<()>>),
}

#[derive(Debug)]
pub struct WriteResponse {
    pub offsets: Vec<u64>,
    pub file_length: u64,
}

/// Single-threaded writer actor that owns the write handle of the active
/// log file. Communicate with it via the `flume::Sender` from [Self::launch];
/// dropping all senders shuts the actor down.
#[derive(Debug)]
pub struct LogFileWriter<F: FileIo> {
    id: u64,
    fio: F,
    recv: flume::Receiver<WriterCommand>,
    sealed: bool,
    file_length: u64,
}

impl<F: FileIo + 'static> LogFileWriter<F> {
    fn new(path: &Path, recv: flume::Receiver<WriterCommand>) -> Result<Self> {
        let id = file_id_from_path(path)
            .ok_or_else(|| LogfileError::InvalidFileName(path.display().to_string()))?;
        let mut fio = F::open(path)?;
        let len = fio.len()?;
        let file_length = if len == 0 {
            write_header(&mut fio)?;
            fio.sync()?;
            FILE_HEADER_SIZE
        } else if len < FILE_HEADER_SIZE {
            // The manager recovers files before attaching a writer, so a
            // partial header here means that contract was violated.
            return Err(anyhow!(LogfileError::InvalidHeader));
        } else {
            len
        };
        Ok(Self {
            id,
            fio,
            recv,
            sealed: false,
            file_length,
        })
    }

    /// Creates the file (writing the header if it's empty) and spawns the
    /// writer thread. The file must be recovered (no torn tail, not sealed)
    /// before calling this.
    pub fn launch(path: &Path) -> Result<flume::Sender<WriterCommand>> {
        let (tx, rx) = flume::unbounded();
        let writer = Self::new(path, rx)?;
        thread::Builder::new()
            .name(format!("wal-writer-{}", writer.id))
            .spawn(move || writer.actor_loop())
            .context("Failed to spawn writer thread")?;
        Ok(tx)
    }

    fn actor_loop(mut self) {
        while let Ok(msg) = self.recv.recv() {
            match msg {
                WriterCommand::Write(reply, data) => {
                    // The caller may have given up; a dropped receiver is not our error.
                    let _ = reply.send(self.write_records(data));
                }
                WriterCommand::Seal(reply) => {
                    let _ = reply.send(self.seal());
                }
            }
        }
        trace!("logfile {} writer actor disconnected", self.id);
    }

    fn write_records(&mut self, records: Vec<Vec<u8>>) -> Result<WriteResponse> {
        if self.sealed {
            return Err(anyhow!(LogfileError::WriteToSealed));
        }

        let mut total = 0usize;
        for record in &records {
            if record.len() > u32::MAX as usize {
                return Err(anyhow!(LogfileError::RecordTooLarge));
            }
            total += RECORD_HEADER_SIZE as usize + record.len();
        }

        let mut buf = Vec::with_capacity(total);
        let mut offsets = Vec::with_capacity(records.len());
        let mut current_offset = self.file_length;

        for record in &records {
            let (hash1, hash2) = murmur3_128(record);
            offsets.push(current_offset);
            current_offset += RECORD_HEADER_SIZE + record.len() as u64;

            buf.extend_from_slice(&hash1.to_le_bytes());
            buf.extend_from_slice(&hash2.to_le_bytes());
            buf.extend_from_slice(&(record.len() as u32).to_le_bytes());
            buf.extend_from_slice(record);
        }

        self.fio
            .write_at(self.file_length, &buf)
            .context("Failed to write records")?;
        self.fio.sync().context("Failed to sync records")?;

        self.file_length = current_offset;
        Ok(WriteResponse {
            offsets,
            file_length: current_offset,
        })
    }

    fn seal(&mut self) -> Result<()> {
        if self.sealed {
            return Err(anyhow!(LogfileError::WriteToSealed));
        }
        self.fio
            .write_at(self.file_length, &build_footer(self.file_length, now_ms()))
            .context("Failed to write footer")?;
        self.fio.sync().context("Failed to sync footer")?;
        self.sealed = true;
        self.file_length += FOOTER_SIZE;
        Ok(())
    }
}

/// Streams records from a single log file in order.
///
/// For unsealed files the stream ends at the current end of file; records
/// written after that are picked up on subsequent polls until the poll that
/// observes no new data, which ends the stream.
pub struct LogFileStream<F: FileIo> {
    logfile: Logfile<F>,
    offset: u64,
}

impl<F: FileIo> LogFileStream<F> {
    pub fn new(logfile: Logfile<F>) -> Self {
        Self {
            logfile,
            offset: FILE_HEADER_SIZE,
        }
    }

    pub fn new_with_offset(logfile: Logfile<F>, offset: u64) -> Self {
        Self {
            logfile,
            offset: offset.max(FILE_HEADER_SIZE),
        }
    }

    pub fn reset_stream(&mut self) {
        self.offset = FILE_HEADER_SIZE;
    }

    pub fn stream_offset(&self) -> u64 {
        self.offset
    }

    pub fn set_stream_offset(&mut self, offset: u64) {
        self.offset = offset.max(FILE_HEADER_SIZE);
    }
}

impl<F: FileIo> Stream for LogFileStream<F> {
    type Item = Result<Vec<u8>>;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut TaskContext<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        let records_end = match this.logfile.records_end() {
            Ok(end) => end,
            Err(e) => return Poll::Ready(Some(Err(e))),
        };
        if this.offset >= records_end {
            return Poll::Ready(None);
        }

        match this.logfile.read_record_and_next(this.offset) {
            Ok((record, next)) => {
                this.offset = next;
                Poll::Ready(Some(Ok(record)))
            }
            Err(e) => Poll::Ready(Some(Err(e))),
        }
    }
}

impl<F: FileIo> Unpin for LogFileStream<F> {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fileio::simple_file::SimpleFile;
    use futures::stream::StreamExt;
    use std::path::PathBuf;

    fn temp_log(name: &str) -> (tempfile::TempDir, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(name);
        (dir, path)
    }

    fn write(
        writer: &flume::Sender<WriterCommand>,
        records: Vec<Vec<u8>>,
    ) -> Result<WriteResponse> {
        let (tx, rx) = flume::bounded(1);
        writer.send(WriterCommand::Write(tx, records)).unwrap();
        rx.recv().unwrap()
    }

    fn seal(writer: &flume::Sender<WriterCommand>) -> Result<()> {
        let (tx, rx) = flume::bounded(1);
        writer.send(WriterCommand::Seal(tx)).unwrap();
        rx.recv().unwrap()
    }

    #[test]
    fn test_write_without_sealing() {
        let (_dir, path) = temp_log("0000000001.log");
        let writer = LogFileWriter::<SimpleFile>::launch(&path).unwrap();
        let offsets = write(&writer, vec![b"hello".to_vec()]).unwrap().offsets;

        let logfile: Logfile<SimpleFile> = Logfile::open(&path).unwrap();
        assert_eq!(logfile.id, 1);
        assert!(!logfile.sealed);
        assert_eq!(logfile.read_record(offsets[0]).unwrap(), b"hello");
    }

    #[test]
    fn test_write_with_sealing() {
        let (_dir, path) = temp_log("0000000002.log");
        let writer = LogFileWriter::<SimpleFile>::launch(&path).unwrap();
        let offsets = write(&writer, vec![b"hello".to_vec()]).unwrap().offsets;
        seal(&writer).unwrap();

        // Writing after sealing fails with an error, not a panic
        let err = write(&writer, vec![b"world".to_vec()]).unwrap_err();
        assert!(matches!(
            err.downcast_ref::<LogfileError>(),
            Some(LogfileError::WriteToSealed)
        ));

        let logfile: Logfile<SimpleFile> = Logfile::open(&path).unwrap();
        assert_eq!(logfile.id, 2);
        assert!(logfile.sealed);
        assert!(logfile.seal_timestamp_ms.unwrap() > 0);
        assert_eq!(logfile.read_record(offsets[0]).unwrap(), b"hello");
    }

    #[test]
    fn test_corrupted_record() {
        use std::io::{Seek, Write};
        let (_dir, path) = temp_log("0000000003.log");
        let writer = LogFileWriter::<SimpleFile>::launch(&path).unwrap();
        let offsets = write(&writer, vec![b"hello".to_vec()]).unwrap().offsets;

        let mut tmp = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        tmp.seek(std::io::SeekFrom::Start(
            offsets[0] + RECORD_HEADER_SIZE + 2,
        ))
        .unwrap();
        tmp.write_all(b"x").unwrap();

        let logfile: Logfile<SimpleFile> = Logfile::open(&path).unwrap();
        let err = logfile.read_record(offsets[0]).unwrap_err();
        assert!(matches!(
            err.downcast_ref::<LogfileError>(),
            Some(LogfileError::Corrupted)
        ));
    }

    #[test]
    fn test_corrupted_length_is_bounded() {
        use std::io::{Seek, Write};
        let (_dir, path) = temp_log("0000000004.log");
        let writer = LogFileWriter::<SimpleFile>::launch(&path).unwrap();
        let offsets = write(&writer, vec![b"hello".to_vec()]).unwrap().offsets;
        seal(&writer).unwrap();

        // Blow up the length field to u32::MAX
        let mut tmp = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        tmp.seek(std::io::SeekFrom::Start(offsets[0] + 16)).unwrap();
        tmp.write_all(&u32::MAX.to_le_bytes()).unwrap();

        let logfile: Logfile<SimpleFile> = Logfile::open(&path).unwrap();
        let err = logfile.read_record(offsets[0]).unwrap_err();
        assert!(matches!(
            err.downcast_ref::<LogfileError>(),
            Some(LogfileError::PartialWrite)
        ));
    }

    #[test]
    fn test_100_records() {
        let (_dir, path) = temp_log("0000000006.log");
        let writer = LogFileWriter::<SimpleFile>::launch(&path).unwrap();
        let records: Vec<Vec<u8>> = (0..100)
            .map(|i| format!("record_{}", i).into_bytes())
            .collect();
        let offsets = write(&writer, records).unwrap().offsets;
        seal(&writer).unwrap();

        let logfile: Logfile<SimpleFile> = Logfile::open(&path).unwrap();
        for (i, offset) in offsets.iter().enumerate() {
            let record = logfile.read_record(*offset).unwrap();
            assert_eq!(String::from_utf8(record).unwrap(), format!("record_{}", i));
        }
    }

    #[test]
    fn test_magic_number_payload_roundtrip() {
        // Payloads full of magic bytes must not confuse framing or seal detection
        let (_dir, path) = temp_log("0000000008.log");
        let writer = LogFileWriter::<SimpleFile>::launch(&path).unwrap();
        let payload = MAGIC_NUMBER.repeat(4);
        let offsets = write(&writer, vec![payload.clone()]).unwrap().offsets;

        let logfile: Logfile<SimpleFile> = Logfile::open(&path).unwrap();
        assert!(!logfile.sealed, "magic-byte payload misdetected as seal");
        assert_eq!(logfile.read_record(offsets[0]).unwrap(), payload);

        seal(&writer).unwrap();
        let logfile: Logfile<SimpleFile> = Logfile::open(&path).unwrap();
        assert!(logfile.sealed);
        assert_eq!(logfile.read_record(offsets[0]).unwrap(), payload);
    }

    #[test]
    fn test_bulk_writes() {
        let (_dir, path) = temp_log("0000000011.log");
        let writer = LogFileWriter::<SimpleFile>::launch(&path).unwrap();
        let records = vec![b"first".to_vec(), b"second".to_vec(), b"third".to_vec()];
        let offsets = write(&writer, records).unwrap().offsets;

        let logfile: Logfile<SimpleFile> = Logfile::open(&path).unwrap();
        assert_eq!(offsets.len(), 3);
        assert_eq!(logfile.read_record(offsets[0]).unwrap(), b"first");
        assert_eq!(logfile.read_record(offsets[1]).unwrap(), b"second");
        assert_eq!(logfile.read_record(offsets[2]).unwrap(), b"third");
    }

    #[tokio::test]
    async fn test_stream() {
        let (_dir, path) = temp_log("0000000010.log");
        let writer = LogFileWriter::<SimpleFile>::launch(&path).unwrap();
        let records: Vec<Vec<u8>> = (0..10)
            .map(|i| format!("record_{}", i).into_bytes())
            .collect();
        write(&writer, records).unwrap();

        let logfile: Logfile<SimpleFile> = Logfile::open(&path).unwrap();
        let mut stream = LogFileStream::new(logfile);
        for i in 0..10 {
            let record = stream.next().await.unwrap().unwrap();
            assert_eq!(String::from_utf8(record).unwrap(), format!("record_{}", i));
        }
        assert!(stream.next().await.is_none());
    }

    #[test]
    fn test_recover_truncates_torn_tail() {
        let (_dir, path) = temp_log("0000000012.log");
        let writer = LogFileWriter::<SimpleFile>::launch(&path).unwrap();
        let offsets = write(&writer, vec![b"keep-me".to_vec(), b"torn".to_vec()])
            .unwrap()
            .offsets;
        drop(writer);

        // Tear the second record: cut the file 3 bytes short
        let full_len = std::fs::metadata(&path).unwrap().len();
        let f = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        f.set_len(full_len - 3).unwrap();

        let end = recover_unsealed::<SimpleFile>(&path).unwrap();
        assert_eq!(end, offsets[1], "should truncate at the torn record");
        assert_eq!(std::fs::metadata(&path).unwrap().len(), offsets[1]);

        let logfile: Logfile<SimpleFile> = Logfile::open(&path).unwrap();
        assert_eq!(logfile.read_record(offsets[0]).unwrap(), b"keep-me");
    }

    #[test]
    fn test_recover_and_seal_torn_footer() {
        let (_dir, path) = temp_log("0000000013.log");
        let writer = LogFileWriter::<SimpleFile>::launch(&path).unwrap();
        let response = write(&writer, vec![b"hello".to_vec()]).unwrap();
        let records_end = response.file_length;
        seal(&writer).unwrap();

        // Tear the footer: cut it in half
        let f = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        f.set_len(records_end + FOOTER_SIZE / 2).unwrap();

        let logfile: Logfile<SimpleFile> = Logfile::open(&path).unwrap();
        assert!(!logfile.sealed, "torn footer must not read as sealed");
        drop(logfile);

        let healed_end = recover_and_seal::<SimpleFile>(&path, 12345).unwrap();
        assert_eq!(healed_end, records_end);
        let logfile: Logfile<SimpleFile> = Logfile::open(&path).unwrap();
        assert!(logfile.sealed);
        assert_eq!(logfile.seal_timestamp_ms, Some(12345));
        assert_eq!(logfile.read_record(response.offsets[0]).unwrap(), b"hello");
    }

    #[test]
    fn test_recover_partial_header() {
        let (_dir, path) = temp_log("0000000014.log");
        std::fs::write(&path, [0xff, 0xff, 0xff]).unwrap();
        let end = recover_unsealed::<SimpleFile>(&path).unwrap();
        assert_eq!(end, 0);
        assert_eq!(std::fs::metadata(&path).unwrap().len(), 0);

        // Writer can take over the healed file
        let writer = LogFileWriter::<SimpleFile>::launch(&path).unwrap();
        let offsets = write(&writer, vec![b"fresh".to_vec()]).unwrap().offsets;
        let logfile: Logfile<SimpleFile> = Logfile::open(&path).unwrap();
        assert_eq!(logfile.read_record(offsets[0]).unwrap(), b"fresh");
    }

    #[test]
    fn test_file_id_from_path() {
        assert_eq!(file_id_from_path(Path::new("/a/0000000042.log")), Some(42));
        assert_eq!(file_id_from_path(Path::new("/a/7.log")), Some(7));
        assert_eq!(file_id_from_path(Path::new("/a/foo.log")), None);
        assert_eq!(file_id_from_path(Path::new("/a/12abc.log")), None);
        assert_eq!(file_id_from_path(Path::new("/a/42.txt")), None);
        assert_eq!(file_id_from_path(Path::new("/a/.log")), None);
    }
}
