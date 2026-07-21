use anyhow::{anyhow, Context, Result};
use futures::Stream;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::task::{Context as TaskContext, Poll};
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{trace, warn};

use crate::fileio::FileIo;
use crate::murmur3::murmur3_128;
use crate::record::RecordID;

/// Logfile represents a log file on disk.
///
/// # Format (version 3)
///
/// ## File layout
///
/// ```text
/// | 4 KiB format block | 4 KiB slot 0 block | 4 KiB slot 1 block | N bytes - records | 40 bytes - seal footer (sealed files only) |
/// ```
///
/// ## Record layout
///
/// ```text
/// | 16 bytes (i64, i64 LE) - murmur3_128 of data | 4 bytes (u32 LE) - bitwise-inverted data length | N bytes - data (raw) |
/// ```
///
/// ## Seal footer layout
///
/// ```text
/// | 8 bytes - magic number | 8 bytes (u64 LE) - seal timestamp (unix ms) | 8 bytes (u64 LE) - records end offset | 16 bytes - murmur3_128 of the previous 24 bytes |
/// ```
///
/// ## Committed-end slot layout
///
/// ```text
/// | 8 bytes - generation | 8 bytes - committed records-end offset | 16 bytes - murmur3_128 of the previous 16 bytes |
/// ```
///
/// The two slots live in distinct aligned filesystem blocks and alternate on
/// commits. Under the WAL's torn-write model, damage to the block targeted by
/// the newest slot write cannot damage the older slot's block. Record bytes
/// and the new slot are covered by the same fdatasync, allowing the active
/// file's physical length to be grown ahead of the logical record end without
/// exposing the unused sparse tail to readers or recovery.
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
pub const FORMAT_VERSION: u8 = 3;
/// Magic number + version byte, before the committed-end slots.
const FILE_HEADER_PREFIX_SIZE: u64 = 9;
const COMMIT_SLOT_SIZE: u64 = 32;
const COMMIT_SLOT_COUNT: u64 = 2;
const HEADER_BLOCK_SIZE: u64 = 4 * 1024;
/// Current (v3) header: one format block plus one block per commit slot.
pub const FILE_HEADER_SIZE: u64 = (1 + COMMIT_SLOT_COUNT) * HEADER_BLOCK_SIZE;
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

    #[error("No valid committed-end metadata in v3 logfile header")]
    InvalidCommittedEnd,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CommitState {
    pub generation: u64,
    pub records_end: u64,
    slot: usize,
}

#[derive(Debug, Clone, Copy)]
struct FileHeader {
    commit: CommitState,
    commit_slots: [Option<CommitState>; COMMIT_SLOT_COUNT as usize],
}

/// The canonical path of a log file id inside a log directory.
pub(crate) fn log_file_path(dir: &Path, id: u64) -> PathBuf {
    dir.join(format!("{:010}.log", id))
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

/// Point reads speculatively fetch this much in one pread: records smaller
/// than this cost one syscall instead of two (header, then data). Kept small
/// because the buffer is zero-initialized per read — a large window costs
/// more in memory traffic than the saved syscall.
const SPECULATIVE_READ: u64 = 512;

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

    // One speculative pread covers the header plus (usually) the whole record
    let first_len = (records_end - offset).min(SPECULATIVE_READ.max(RECORD_HEADER_SIZE));
    let mut first = vec![0u8; first_len as usize];
    fio.read_at(offset, &mut first)
        .context("Failed to read record header")?;
    let hash1 = i64::from_le_bytes(first[0..8].try_into().unwrap());
    let hash2 = i64::from_le_bytes(first[8..16].try_into().unwrap());
    let encoded_length = u32::from_le_bytes(first[16..20].try_into().unwrap());
    // Lengths are stored inverted so an unwritten sparse region (all zeroes)
    // cannot masquerade as a run of valid empty records. u32::MAX payloads
    // are rejected on write, making encoded zero permanently invalid.
    if encoded_length == 0 {
        return Err(anyhow!(LogfileError::PartialWrite));
    }
    let length = (!encoded_length) as u64;

    // Bounds check before allocating: a corrupted length must not drive a
    // giant allocation or a read past the record region.
    let data_end = data_start
        .checked_add(length)
        .ok_or(LogfileError::PartialWrite)?;
    if data_end > records_end {
        return Err(anyhow!(LogfileError::PartialWrite));
    }

    let data = if RECORD_HEADER_SIZE + length <= first_len {
        first[RECORD_HEADER_SIZE as usize..(RECORD_HEADER_SIZE + length) as usize].to_vec()
    } else {
        let mut data = vec![0u8; length as usize];
        let have = (first_len - RECORD_HEADER_SIZE) as usize;
        data[..have].copy_from_slice(&first[RECORD_HEADER_SIZE as usize..]);
        fio.read_at(offset + first_len, &mut data[have..])
            .context("Failed to read record data")?;
        data
    };

    let (computed1, computed2) = murmur3_128(&data);
    if computed1 != hash1 || computed2 != hash2 {
        return Err(anyhow!(LogfileError::Corrupted));
    }

    Ok((data, data_end))
}

/// Walks records from the start of the file and returns the end offset of
/// the longest valid prefix. Only meaningful for unsealed files.
fn scan_records_end<F: FileIo>(fio: &F, scan_limit: u64) -> u64 {
    let mut offset = FILE_HEADER_SIZE;
    loop {
        match read_record_at(fio, offset, scan_limit) {
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

fn commit_slot_offset(slot: usize) -> u64 {
    (slot as u64 + 1) * HEADER_BLOCK_SIZE
}

fn build_commit_slot(generation: u64, records_end: u64) -> [u8; COMMIT_SLOT_SIZE as usize] {
    let mut slot = [0u8; COMMIT_SLOT_SIZE as usize];
    slot[0..8].copy_from_slice(&generation.to_le_bytes());
    slot[8..16].copy_from_slice(&records_end.to_le_bytes());
    let (h1, h2) = murmur3_128(&slot[0..16]);
    slot[16..24].copy_from_slice(&h1.to_le_bytes());
    slot[24..32].copy_from_slice(&h2.to_le_bytes());
    slot
}

fn parse_commit_slot(buf: &[u8], slot: usize) -> Option<CommitState> {
    let (h1, h2) = murmur3_128(&buf[0..16]);
    if buf[16..24] != h1.to_le_bytes() || buf[24..32] != h2.to_le_bytes() {
        return None;
    }
    Some(CommitState {
        generation: u64::from_le_bytes(buf[0..8].try_into().unwrap()),
        records_end: u64::from_le_bytes(buf[8..16].try_into().unwrap()),
        slot,
    })
}

fn read_header<F: FileIo>(fio: &F, file_len: u64) -> Result<FileHeader> {
    if file_len < FILE_HEADER_SIZE {
        return Err(anyhow!(LogfileError::InvalidHeader));
    }
    let mut base = [0u8; FILE_HEADER_PREFIX_SIZE as usize];
    fio.read_at(0, &mut base)
        .context("Failed to read file header")?;
    if base[0..8] != MAGIC_NUMBER {
        return Err(anyhow!(LogfileError::InvalidMagicNumber));
    }
    if base[8] != FORMAT_VERSION {
        return Err(anyhow!(LogfileError::UnsupportedVersion(base[8])));
    }
    let mut commit_slots = [None; COMMIT_SLOT_COUNT as usize];
    for (slot, state) in commit_slots.iter_mut().enumerate() {
        let mut buf = [0u8; COMMIT_SLOT_SIZE as usize];
        fio.read_at(commit_slot_offset(slot), &mut buf)
            .with_context(|| format!("Failed to read committed-end slot {slot}"))?;
        *state =
            parse_commit_slot(&buf, slot).filter(|state| state.records_end >= FILE_HEADER_SIZE);
    }
    let commit = commit_slots
        .iter()
        .flatten()
        .copied()
        .max_by_key(|state| state.generation)
        .ok_or(LogfileError::InvalidCommittedEnd)?;
    Ok(FileHeader {
        commit,
        commit_slots,
    })
}

pub(crate) fn write_header<F: FileIo>(fio: &mut F) -> Result<()> {
    let mut header = [0u8; FILE_HEADER_SIZE as usize];
    header[0..8].copy_from_slice(&MAGIC_NUMBER);
    header[8] = FORMAT_VERSION;
    let slot_offset = commit_slot_offset(0) as usize;
    header[slot_offset..slot_offset + COMMIT_SLOT_SIZE as usize]
        .copy_from_slice(&build_commit_slot(0, FILE_HEADER_SIZE));
    fio.write_at(0, &header).context("Failed to write header")?;
    Ok(())
}

/// Returns the newest valid v3 committed-end slot. The caller must have
/// recovered the file first if record bytes may be torn.
pub(crate) fn read_commit_state<F: FileIo>(fio: &F) -> Result<CommitState> {
    let len = fio.len()?;
    let header = read_header(fio, len)?;
    Ok(header.commit)
}

/// Writes the alternate v3 committed-end slot without syncing. The caller
/// writes record bytes first and covers both writes with one fdatasync.
pub(crate) fn write_committed_end<F: FileIo>(
    fio: &mut F,
    previous: CommitState,
    records_end: u64,
) -> Result<CommitState> {
    let generation = previous
        .generation
        .checked_add(1)
        .ok_or_else(|| anyhow!("committed-end generation exhausted"))?;
    let slot = 1 - previous.slot;
    fio.write_at(
        commit_slot_offset(slot),
        &build_commit_slot(generation, records_end),
    )
    .context("Failed to write committed-end metadata")?;
    Ok(CommitState {
        generation,
        records_end,
        slot,
    })
}

/// Rewrites a torn newest commit's slot in place during recovery. Keeping the
/// alternate slot untouched preserves the last fully committed end if recovery
/// itself crashes before this repair is synced.
fn repair_committed_end<F: FileIo>(
    fio: &mut F,
    previous: CommitState,
    records_end: u64,
) -> Result<()> {
    fio.write_at(
        commit_slot_offset(previous.slot),
        &build_commit_slot(previous.generation, records_end),
    )
    .context("Failed to repair committed-end metadata")
}

/// Appends a seal footer at `records_end` and syncs. The caller must ensure
/// the file's record region actually ends there.
pub(crate) fn append_footer<F: FileIo>(fio: &mut F, records_end: u64, ts_ms: u64) -> Result<()> {
    // Active v3 files may have a sparse allocation window beyond records_end.
    // Remove it before placing the footer so sealed file length stays exact.
    fio.set_len(records_end)
        .context("Failed to truncate allocation tail before sealing")?;
    fio.write_at(records_end, &build_footer(records_end, ts_ms))
        .context("Failed to write footer")?;
    fio.sync().context("Failed to sync footer")?;
    Ok(())
}

/// Encodes `records` into `buf` as they would sit on disk starting at
/// `start_offset`, returning each record's file offset. Records must already
/// be validated (< u32::MAX bytes each).
pub(crate) fn encode_records(
    buf: &mut Vec<u8>,
    records: &[Vec<u8>],
    start_offset: u64,
) -> Vec<u64> {
    let mut offsets = Vec::with_capacity(records.len());
    let mut current = start_offset;
    for record in records {
        let (hash1, hash2) = murmur3_128(record);
        offsets.push(current);
        current += RECORD_HEADER_SIZE + record.len() as u64;

        buf.extend_from_slice(&hash1.to_le_bytes());
        buf.extend_from_slice(&hash2.to_le_bytes());
        buf.extend_from_slice(&(!(record.len() as u32)).to_le_bytes());
        buf.extend_from_slice(record);
    }
    offsets
}

/// Recovers an unsealed file in place: validates the header, scans for the
/// longest valid record prefix, and truncates anything after it (torn
/// writes). Returns the record-region end offset, or 0 for a file that never
/// got a complete header (truncated to empty; the writer re-initializes it).
///
/// Must NOT be called on a sealed file — the scan would treat the footer as
/// a torn record and truncate it off.
pub fn recover_unsealed<F: FileIo>(path: &Path) -> Result<u64> {
    let mut fio = F::open_existing(path)?;
    let len = fio.len()?;
    if len < FILE_HEADER_PREFIX_SIZE {
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

    // A v3 header can tear while a newly-created rotation target is being
    // initialized. It cannot contain acknowledged records until its complete
    // header has been synced, so the active-file recovery path may safely
    // reset this partial header and let the writer initialize it again.
    let mut base = [0u8; FILE_HEADER_PREFIX_SIZE as usize];
    fio.read_at(0, &mut base)?;
    if base[0..8] != MAGIC_NUMBER {
        return Err(anyhow!(LogfileError::InvalidMagicNumber));
    }
    if base[8] != FORMAT_VERSION {
        return Err(anyhow!(LogfileError::UnsupportedVersion(base[8])));
    }
    if len < FILE_HEADER_SIZE {
        warn!(
            "log file {} has a partial v3 header ({} bytes), truncating to empty",
            path.display(),
            len
        );
        fio.set_len(0)?;
        fio.sync()?;
        return Ok(0);
    }

    let header = match read_header(&fio, len) {
        Ok(header) => header,
        Err(e)
            if len == FILE_HEADER_SIZE
                && matches!(
                    e.downcast_ref::<LogfileError>(),
                    Some(LogfileError::InvalidCommittedEnd)
                ) =>
        {
            // Preallocation only begins after a complete header is synced.
            // A header-sized active file with no valid slot therefore cannot
            // contain acknowledged records and is safe to reinitialize.
            warn!(
                "log file {} has a full-length torn header with no valid slot; truncating to empty",
                path.display()
            );
            fio.set_len(0)?;
            fio.sync()?;
            return Ok(0);
        }
        Err(e) => return Err(e),
    };
    let commit = header.commit;
    // The older slot is an already-committed floor. A torn newest commit may
    // be shortened to a valid record prefix, but recovery must never discard
    // bytes covered by the previous successful fdatasync.
    let committed_floor = header
        .commit_slots
        .iter()
        .flatten()
        .copied()
        .filter(|state| state.generation < commit.generation)
        .max_by_key(|state| state.generation)
        .map(|state| state.records_end)
        .unwrap_or(FILE_HEADER_SIZE);
    if committed_floor > len {
        return Err(anyhow!(LogfileError::Corrupted));
    }

    // Never scan the sparse allocation tail: the checksummed newest slot is
    // the upper bound. If a crash persisted the slot but only a prefix of its
    // record bytes, retain that longest valid prefix.
    let scan_limit = commit.records_end.min(len);
    let end = scan_records_end(&fio, scan_limit);
    if end < committed_floor {
        return Err(anyhow!(LogfileError::Corrupted));
    }
    let repair_from = (end != commit.records_end).then_some(commit);

    let mut changed = false;
    if let Some(previous) = repair_from {
        repair_committed_end(&mut fio, previous, end)?;
        changed = true;
    }
    if end != len {
        warn!(
            "log file {} has a torn tail: truncating {} -> {}",
            path.display(),
            len,
            end
        );
        fio.set_len(end)?;
        changed = true;
    }
    if changed {
        fio.sync()?;
    }
    Ok(end)
}

/// Recovers an unsealed file (see [recover_unsealed]) and then seals it with
/// the given timestamp. Used at startup to heal files that should have been
/// sealed but crashed mid-rotation, and by tests to craft aged files.
pub fn recover_and_seal<F: FileIo>(path: &Path, ts_ms: u64) -> Result<u64> {
    let mut records_end = recover_unsealed::<F>(path)?;
    let mut fio = F::open_existing(path)?;
    if records_end == 0 {
        write_header(&mut fio)?;
        records_end = FILE_HEADER_SIZE;
    }
    append_footer(&mut fio, records_end, ts_ms)?;
    Ok(records_end)
}

impl<F: FileIo> Logfile<F> {
    /// Opens an existing log file, validating the header and detecting the
    /// seal footer.
    pub fn open(path: &Path) -> Result<Self> {
        let id = file_id_from_path(path)
            .ok_or_else(|| LogfileError::InvalidFileName(path.display().to_string()))?;
        let fio = F::open_existing(path)?;
        let len = fio.len()?;
        if len < FILE_HEADER_SIZE {
            return Err(anyhow!(LogfileError::InvalidHeader));
        }
        read_header(&fio, len)?;

        let mut logfile = Self {
            id,
            sealed: false,
            seal_timestamp_ms: None,
            records_end: None,
            path: path.to_path_buf(),
            fio,
        };
        logfile.refresh_seal()?;
        Ok(logfile)
    }

    /// Re-checks for a seal footer on an unsealed file and returns the
    /// record-region end from that same single length observation. Rotation
    /// seals the previously active file asynchronously, so a long-lived
    /// reader or stream can watch a file transition to sealed; the appended
    /// footer bytes must not be parsed as a record. Callers must use the
    /// returned end (not a fresh `len()`) — a second length observation
    /// could already include a footer this probe never saw.
    pub(crate) fn refresh_seal(&mut self) -> Result<u64> {
        if self.sealed {
            return self.records_end();
        }
        let committed_end = read_commit_state(&self.fio)?.records_end;
        let footer_end = committed_end
            .checked_add(FOOTER_SIZE)
            .ok_or(LogfileError::PartialWrite)?;
        // Rotation first shrinks a sparse active file to committed_end, then
        // writes the footer. Retry once if that shrink lands between our
        // length observation and positional read; healthy rotation must not
        // leak the transient EOF to readers.
        for attempt in 0..2 {
            let len = self.fio.len()?;
            if len != footer_end {
                return Ok(committed_end);
            }
            let mut fbuf = [0u8; FOOTER_SIZE as usize];
            match self.fio.read_at(committed_end, &mut fbuf) {
                Ok(()) => {
                    if let Some(ts) = parse_footer(&fbuf, len) {
                        self.sealed = true;
                        self.seal_timestamp_ms = Some(ts);
                        self.records_end = Some(committed_end);
                    }
                    return Ok(committed_end);
                }
                Err(_) if attempt == 0 => continue,
                Err(e) => return Err(e),
            }
        }
        Ok(committed_end)
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

    /// End of the record region: fixed for sealed files and read from the
    /// committed-end metadata for active files.
    pub fn records_end(&self) -> Result<u64> {
        match self.records_end {
            Some(end) => Ok(end),
            None => Ok(read_commit_state(&self.fio)?.records_end),
        }
    }

    pub fn file_len(&self) -> Result<u64> {
        self.fio.len()
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Raw positional read within the file, for buffered streaming.
    pub(crate) fn read_into(&self, offset: u64, buf: &mut [u8]) -> Result<()> {
        self.fio.read_at(offset, buf)
    }
}

/// Streams records from a single log file in order.
///
/// For unsealed files the stream ends at the current committed record end;
/// records written after that are picked up on subsequent polls until the
/// poll that observes no new data, which ends the stream.
pub struct LogFileStream<F: FileIo> {
    logfile: Logfile<F>,
    offset: u64,
    buf: Vec<u8>,
    /// Absolute file offset of `buf[0]`.
    buf_start: u64,
    /// Last observed record-region end for unsealed files (see records_end).
    known_end: u64,
}

/// Readahead chunk for sequential streaming: records are parsed out of this
/// buffer instead of paying two preads per record.
const STREAM_CHUNK: u64 = 128 * 1024;

impl<F: FileIo> LogFileStream<F> {
    pub fn new(logfile: Logfile<F>) -> Self {
        Self::new_with_offset(logfile, FILE_HEADER_SIZE)
    }

    pub fn new_with_offset(logfile: Logfile<F>, offset: u64) -> Self {
        Self {
            logfile,
            offset: offset.max(FILE_HEADER_SIZE),
            buf: Vec::new(),
            buf_start: 0,
            known_end: 0,
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

    /// Record-region end. For unsealed files, only re-stats the file once
    /// the previously observed end has been consumed — and re-probes the
    /// seal footer first, since rotation may have sealed the file while we
    /// were streaming it (the footer must not be parsed as a record).
    fn records_end(&mut self) -> Result<u64> {
        if self.logfile.sealed {
            return self.logfile.records_end();
        }
        if self.offset < self.known_end {
            return Ok(self.known_end);
        }
        self.known_end = self.logfile.refresh_seal()?;
        Ok(self.known_end)
    }

    /// Makes `buf` cover `[start, start + need)`. The caller must have
    /// checked that the range lies within the record region.
    fn ensure_buffered(&mut self, start: u64, need: u64, records_end: u64) -> Result<()> {
        let have_end = self.buf_start + self.buf.len() as u64;
        if start >= self.buf_start && start + need <= have_end {
            return Ok(());
        }
        let read_len = need.max((records_end - start).min(STREAM_CHUNK));
        self.buf.resize(read_len as usize, 0);
        self.logfile.read_into(start, &mut self.buf)?;
        self.buf_start = start;
        Ok(())
    }

    fn next_record(&mut self) -> Result<Option<(u64, Vec<u8>)>> {
        let records_end = self.records_end()?;
        if self.offset >= records_end {
            return Ok(None);
        }
        if self.offset + RECORD_HEADER_SIZE > records_end {
            return Err(anyhow!(LogfileError::PartialWrite));
        }

        self.ensure_buffered(self.offset, RECORD_HEADER_SIZE, records_end)?;
        let base = (self.offset - self.buf_start) as usize;
        let hash1 = i64::from_le_bytes(self.buf[base..base + 8].try_into().unwrap());
        let hash2 = i64::from_le_bytes(self.buf[base + 8..base + 16].try_into().unwrap());
        let encoded_length = u32::from_le_bytes(self.buf[base + 16..base + 20].try_into().unwrap());
        if encoded_length == 0 {
            return Err(anyhow!(LogfileError::PartialWrite));
        }
        let length = (!encoded_length) as u64;

        let data_end = self
            .offset
            .checked_add(RECORD_HEADER_SIZE + length)
            .ok_or(LogfileError::PartialWrite)?;
        if data_end > records_end {
            return Err(anyhow!(LogfileError::PartialWrite));
        }

        // May refill (and move) the buffer, so recompute the base offset
        self.ensure_buffered(self.offset, RECORD_HEADER_SIZE + length, records_end)?;
        let base = (self.offset - self.buf_start) as usize;
        let data_base = base + RECORD_HEADER_SIZE as usize;
        let data = self.buf[data_base..data_base + length as usize].to_vec();

        let (computed1, computed2) = murmur3_128(&data);
        if computed1 != hash1 || computed2 != hash2 {
            return Err(anyhow!(LogfileError::Corrupted));
        }

        let record_offset = self.offset;
        self.offset = data_end;
        Ok(Some((record_offset, data)))
    }
}

impl<F: FileIo> Stream for LogFileStream<F> {
    /// Each record with its stable address, so consumers can checkpoint
    /// their replay position (see the high-water-mark pattern in the README).
    type Item = Result<(RecordID, Vec<u8>)>;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut TaskContext<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        match this.next_record() {
            Ok(Some((offset, record))) => {
                Poll::Ready(Some(Ok((RecordID::new(this.logfile.id, offset), record))))
            }
            Ok(None) => Poll::Ready(None),
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
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

    static SHRINK_ON_FOOTER_READ: AtomicBool = AtomicBool::new(false);
    static SHRINK_OFFSET: AtomicU64 = AtomicU64::new(u64::MAX);

    #[derive(Debug)]
    struct ShrinkRaceFile {
        fd: std::fs::File,
    }

    impl FileIo for ShrinkRaceFile {
        fn open(path: &Path) -> Result<Self> {
            let fd = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(path)?;
            Ok(Self { fd })
        }

        fn open_existing(path: &Path) -> Result<Self> {
            let fd = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(path)?;
            Ok(Self { fd })
        }

        fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<()> {
            if offset == SHRINK_OFFSET.load(Ordering::SeqCst)
                && SHRINK_ON_FOOTER_READ.swap(false, Ordering::SeqCst)
            {
                self.fd.set_len(offset)?;
            }
            std::os::unix::fs::FileExt::read_exact_at(&self.fd, buf, offset)?;
            Ok(())
        }

        fn write_at(&mut self, offset: u64, data: &[u8]) -> Result<()> {
            std::os::unix::fs::FileExt::write_all_at(&self.fd, data, offset)?;
            Ok(())
        }

        fn sync(&mut self) -> Result<()> {
            self.fd.sync_data()?;
            Ok(())
        }

        fn len(&self) -> Result<u64> {
            Ok(self.fd.metadata()?.len())
        }

        fn set_len(&mut self, len: u64) -> Result<()> {
            self.fd.set_len(len)?;
            Ok(())
        }
    }

    fn temp_log(name: &str) -> (tempfile::TempDir, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(name);
        (dir, path)
    }

    /// Appends records to a (possibly new) log file the same way the writer
    /// actor does: header if empty, encoded batch, one write + sync.
    fn write_raw(path: &Path, records: &[Vec<u8>]) -> Vec<u64> {
        let mut fio = SimpleFile::open(path).unwrap();
        if fio.len().unwrap() == 0 {
            write_header(&mut fio).unwrap();
        }
        let state = read_commit_state(&fio).unwrap();
        let end = state.records_end;
        let mut buf = Vec::new();
        let offsets = encode_records(&mut buf, records, end);
        fio.write_at(end, &buf).unwrap();
        write_committed_end(&mut fio, state, end + buf.len() as u64).unwrap();
        fio.sync().unwrap();
        offsets
    }

    fn seal_raw(path: &Path) {
        let fio = SimpleFile::open(path).unwrap();
        let end = read_commit_state(&fio).unwrap().records_end;
        drop(fio);
        let mut fio = SimpleFile::open(path).unwrap();
        append_footer(&mut fio, end, now_ms()).unwrap();
    }

    #[test]
    fn test_write_without_sealing() {
        let (_dir, path) = temp_log("0000000001.log");
        let offsets = write_raw(&path, &[b"hello".to_vec()]);

        let logfile: Logfile<SimpleFile> = Logfile::open(&path).unwrap();
        assert_eq!(logfile.id, 1);
        assert!(!logfile.sealed);
        assert_eq!(logfile.read_record(offsets[0]).unwrap(), b"hello");
    }

    #[test]
    fn test_refresh_seal_retries_sparse_shrink_race() {
        let (_dir, path) = temp_log("0000000019.log");
        let mut fio = SimpleFile::open(&path).unwrap();
        write_header(&mut fio).unwrap();
        // Make the active sparse length look exactly like a footer-bearing
        // file, then truncate at the instant the footer probe starts.
        fio.set_len(FILE_HEADER_SIZE + FOOTER_SIZE).unwrap();
        fio.sync().unwrap();
        drop(fio);

        SHRINK_OFFSET.store(FILE_HEADER_SIZE, Ordering::SeqCst);
        SHRINK_ON_FOOTER_READ.store(true, Ordering::SeqCst);
        let logfile: Logfile<ShrinkRaceFile> = Logfile::open(&path).unwrap();
        assert!(!logfile.sealed);
        assert_eq!(logfile.records_end().unwrap(), FILE_HEADER_SIZE);
    }

    #[test]
    fn test_write_with_sealing() {
        let (_dir, path) = temp_log("0000000002.log");
        let offsets = write_raw(&path, &[b"hello".to_vec()]);
        seal_raw(&path);

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
        let offsets = write_raw(&path, &[b"hello".to_vec()]);

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
        let offsets = write_raw(&path, &[b"hello".to_vec()]);
        seal_raw(&path);

        // Encoded 1 decodes to u32::MAX - 1.
        let mut tmp = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        tmp.seek(std::io::SeekFrom::Start(offsets[0] + 16)).unwrap();
        tmp.write_all(&1u32.to_le_bytes()).unwrap();

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
        let records: Vec<Vec<u8>> = (0..100)
            .map(|i| format!("record_{}", i).into_bytes())
            .collect();
        let offsets = write_raw(&path, &records);
        seal_raw(&path);

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
        let payload = MAGIC_NUMBER.repeat(4);
        let offsets = write_raw(&path, std::slice::from_ref(&payload));

        let logfile: Logfile<SimpleFile> = Logfile::open(&path).unwrap();
        assert!(!logfile.sealed, "magic-byte payload misdetected as seal");
        assert_eq!(logfile.read_record(offsets[0]).unwrap(), payload);

        seal_raw(&path);
        let logfile: Logfile<SimpleFile> = Logfile::open(&path).unwrap();
        assert!(logfile.sealed);
        assert_eq!(logfile.read_record(offsets[0]).unwrap(), payload);
    }

    #[test]
    fn test_bulk_writes() {
        let (_dir, path) = temp_log("0000000011.log");
        let records = vec![b"first".to_vec(), b"second".to_vec(), b"third".to_vec()];
        let offsets = write_raw(&path, &records);

        let logfile: Logfile<SimpleFile> = Logfile::open(&path).unwrap();
        assert_eq!(offsets.len(), 3);
        assert_eq!(logfile.read_record(offsets[0]).unwrap(), b"first");
        assert_eq!(logfile.read_record(offsets[1]).unwrap(), b"second");
        assert_eq!(logfile.read_record(offsets[2]).unwrap(), b"third");
    }

    #[tokio::test]
    async fn test_stream() {
        let (_dir, path) = temp_log("0000000010.log");
        let records: Vec<Vec<u8>> = (0..10)
            .map(|i| format!("record_{}", i).into_bytes())
            .collect();
        let offsets = write_raw(&path, &records);

        let logfile: Logfile<SimpleFile> = Logfile::open(&path).unwrap();
        let mut stream = LogFileStream::new(logfile);
        for (i, offset) in offsets.iter().enumerate() {
            let (id, record) = stream.next().await.unwrap().unwrap();
            assert_eq!(id, RecordID::new(10, *offset));
            assert_eq!(String::from_utf8(record).unwrap(), format!("record_{}", i));
        }
        assert!(stream.next().await.is_none());
    }

    #[test]
    fn test_recover_truncates_torn_tail() {
        let (_dir, path) = temp_log("0000000012.log");
        let offsets = write_raw(&path, &[b"keep-me".to_vec(), b"torn".to_vec()]);

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
        let offsets = write_raw(&path, &[b"hello".to_vec()]);
        let records_end = std::fs::metadata(&path).unwrap().len();
        seal_raw(&path);

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
        assert_eq!(logfile.read_record(offsets[0]).unwrap(), b"hello");
    }

    #[test]
    fn test_recover_partial_header() {
        let (_dir, path) = temp_log("0000000014.log");
        std::fs::write(&path, [0xff, 0xff, 0xff]).unwrap();
        let end = recover_unsealed::<SimpleFile>(&path).unwrap();
        assert_eq!(end, 0);
        assert_eq!(std::fs::metadata(&path).unwrap().len(), 0);

        // A fresh write can take over the healed (empty) file
        let offsets = write_raw(&path, &[b"fresh".to_vec()]);
        let logfile: Logfile<SimpleFile> = Logfile::open(&path).unwrap();
        assert_eq!(logfile.read_record(offsets[0]).unwrap(), b"fresh");
    }

    #[test]
    fn test_recover_discards_tail_when_newest_end_slot_is_torn() {
        use std::os::unix::fs::FileExt;

        let (_dir, path) = temp_log("0000000015.log");
        let first = write_raw(&path, &[b"committed".to_vec()])[0];
        let first_end = first + RECORD_HEADER_SIZE + b"committed".len() as u64;
        let second = write_raw(&path, &[b"slot-will-tear".to_vec()])[0];

        // Commit 2 alternates back to slot 0. Corrupt its checksum so
        // recovery must use slot 1 (commit 1) and discard commit 2's bytes.
        let f = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        f.write_all_at(&[0x7f], commit_slot_offset(0) + 16).unwrap();

        assert_eq!(recover_unsealed::<SimpleFile>(&path).unwrap(), first_end);
        assert_eq!(std::fs::metadata(&path).unwrap().len(), first_end);
        let logfile: Logfile<SimpleFile> = Logfile::open(&path).unwrap();
        assert_eq!(logfile.read_record(first).unwrap(), b"committed");
        assert!(logfile.read_record(second).is_err());
    }

    #[test]
    fn test_commit_slots_are_in_distinct_aligned_blocks() {
        let first = commit_slot_offset(0);
        let second = commit_slot_offset(1);
        assert_eq!(first % HEADER_BLOCK_SIZE, 0);
        assert_eq!(second % HEADER_BLOCK_SIZE, 0);
        assert_ne!(first / HEADER_BLOCK_SIZE, second / HEADER_BLOCK_SIZE);
        assert!(second + COMMIT_SLOT_SIZE <= FILE_HEADER_SIZE);
    }

    #[test]
    fn test_recovery_never_truncates_below_older_committed_slot() {
        use std::os::unix::fs::FileExt;

        let (_dir, path) = temp_log("0000000016.log");
        let first = write_raw(&path, &[b"must-survive".to_vec()])[0];
        write_raw(&path, &[b"newest".to_vec()]);
        let original_len = std::fs::metadata(&path).unwrap().len();

        // Damage data covered by the older valid slot. This is not a torn
        // newest commit, so recovery must report corruption rather than
        // silently roll back an already-committed record.
        let f = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        f.write_all_at(&[0x7f], first + RECORD_HEADER_SIZE).unwrap();

        let err = recover_unsealed::<SimpleFile>(&path).unwrap_err();
        assert!(matches!(
            err.downcast_ref::<LogfileError>(),
            Some(LogfileError::Corrupted)
        ));
        assert_eq!(std::fs::metadata(&path).unwrap().len(), original_len);
    }

    #[test]
    fn test_recovery_does_not_parse_sparse_zeroes_as_empty_records() {
        let (_dir, path) = temp_log("0000000017.log");
        let mut fio = SimpleFile::open(&path).unwrap();
        write_header(&mut fio).unwrap();
        let state = read_commit_state(&fio).unwrap();
        let sparse_end = FILE_HEADER_SIZE + 2 * RECORD_HEADER_SIZE;
        fio.set_len(sparse_end).unwrap();
        // Simulate a committed-end slot reaching disk while the corresponding
        // record pages remain unwritten sparse zeroes.
        write_committed_end(&mut fio, state, sparse_end).unwrap();
        fio.sync().unwrap();
        drop(fio);

        assert_eq!(
            recover_unsealed::<SimpleFile>(&path).unwrap(),
            FILE_HEADER_SIZE
        );
        assert_eq!(std::fs::metadata(&path).unwrap().len(), FILE_HEADER_SIZE);
    }

    #[test]
    fn test_recovery_repair_keeps_previous_commit_as_floor() {
        use std::os::unix::fs::FileExt;

        let (_dir, path) = temp_log("0000000018.log");
        let first = write_raw(&path, &[b"older".to_vec()])[0];
        let first_end = first + RECORD_HEADER_SIZE + b"older".len() as u64;
        let second = write_raw(&path, &[b"torn-newest".to_vec()])[0];
        let f = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        f.set_len(second + RECORD_HEADER_SIZE + 2).unwrap();

        assert_eq!(recover_unsealed::<SimpleFile>(&path).unwrap(), first_end);

        // If recovery had overwritten the alternate (older) slot, this
        // corruption could be mistaken for another recoverable newest tail.
        let f = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        f.write_all_at(&[0x7f], first + RECORD_HEADER_SIZE).unwrap();
        let err = recover_unsealed::<SimpleFile>(&path).unwrap_err();
        assert!(matches!(
            err.downcast_ref::<LogfileError>(),
            Some(LogfileError::Corrupted)
        ));
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
