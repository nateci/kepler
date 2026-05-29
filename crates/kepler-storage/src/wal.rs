//! Write-ahead log.
//!
//! On-disk format. Each record:
//!
//! ```text
//!   ┌──────────────┬──────────────┬─────────────┬───────┬──────────────┐
//!   │ body_len u32 │   crc32 u32  │  seq u64    │ kind  │   payload    │
//!   │   (LE)       │   (LE) of    │   (LE)      │  u8   │              │
//!   │              │   the body   │             │       │              │
//!   └──────────────┴──────────────┴─────────────┴───────┴──────────────┘
//!     4 bytes        4 bytes        8 bytes      1 byte   payload bytes
//!
//!   ◀──── header ───────────────▶◀──────────── body (body_len bytes) ──▶
//! ```
//!
//! Segments are files named `wal-<start_seq, 20-digit>.log` so a sorted
//! directory listing is in sequence order. The active segment is the
//! highest-numbered one; appends go there. Rotation cuts a new segment
//! once `max_segment_bytes` is exceeded.
//!
//! Recovery on open: scan each segment, stop at first CRC mismatch / short
//! read, truncate the file at the last good byte. This collapses torn writes
//! from a crash mid-append into a clean tail.

use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Seek, Write};
use std::path::{Path, PathBuf};

use bytes::Bytes;
use parking_lot::Mutex;

use kepler_types::{Error, Result};

// ---- public types --------------------------------------------------------

pub type WalOffset = u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalEntryKind {
    Put,
    Delete,
    Commit,
}

impl WalEntryKind {
    fn encode(self) -> u8 {
        match self {
            WalEntryKind::Put => 0,
            WalEntryKind::Delete => 1,
            WalEntryKind::Commit => 2,
        }
    }

    fn decode(byte: u8) -> Option<Self> {
        match byte {
            0 => Some(WalEntryKind::Put),
            1 => Some(WalEntryKind::Delete),
            2 => Some(WalEntryKind::Commit),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct WalEntry {
    pub seq: u64,
    pub kind: WalEntryKind,
    pub payload: Bytes,
}

pub trait Wal: Send + Sync {
    /// Append; durability depends on `sync_on_append` config / explicit
    /// [`Wal::sync`].
    fn append(&self, entry: WalEntry) -> Result<WalOffset>;

    /// Force durability for everything appended so far.
    fn sync(&self) -> Result<()>;

    /// Replay all entries with `seq >= offset`.
    fn read_from(&self, offset: WalOffset) -> Result<Vec<WalEntry>>;

    /// Discard segments whose entries are all strictly before `offset`. The
    /// active segment is never deleted, so some entries `< offset` may remain
    /// until it rotates.
    fn truncate(&self, before: WalOffset) -> Result<()>;

    /// Highest `seq` known to the WAL, or `None` if empty.
    fn last_seq(&self) -> Result<Option<u64>>;
}

// ---- in-memory impl (tests) ---------------------------------------------

#[derive(Default)]
pub struct MemWal {
    entries: Mutex<Vec<WalEntry>>,
}

impl MemWal {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Wal for MemWal {
    fn append(&self, entry: WalEntry) -> Result<WalOffset> {
        let seq = entry.seq;
        self.entries.lock().push(entry);
        Ok(seq)
    }

    fn sync(&self) -> Result<()> {
        Ok(())
    }

    fn read_from(&self, offset: WalOffset) -> Result<Vec<WalEntry>> {
        Ok(self
            .entries
            .lock()
            .iter()
            .filter(|e| e.seq >= offset)
            .cloned()
            .collect())
    }

    fn truncate(&self, before: WalOffset) -> Result<()> {
        self.entries.lock().retain(|e| e.seq >= before);
        Ok(())
    }

    fn last_seq(&self) -> Result<Option<u64>> {
        Ok(self.entries.lock().last().map(|e| e.seq))
    }
}

// ---- on-disk impl --------------------------------------------------------

const RECORD_HEADER: usize = 8; // u32 body_len + u32 crc
const BODY_HEADER: usize = 9;   // u64 seq + u8 kind
const MAX_RECORD_BODY: u32 = 64 * 1024 * 1024; // 64 MiB sanity cap

#[derive(Debug, Clone)]
pub struct WalConfig {
    pub dir: PathBuf,
    pub max_segment_bytes: u64,
    /// If true, fsync after every append. Slow but correct. Group commit /
    /// periodic sync are future work.
    pub sync_on_append: bool,
}

impl WalConfig {
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self {
            dir: dir.into(),
            max_segment_bytes: 64 * 1024 * 1024, // 64 MiB
            sync_on_append: true,
        }
    }
}

pub struct DiskWal {
    config: WalConfig,
    inner: Mutex<Inner>,
}

struct Inner {
    /// Sorted by `start_seq` ascending. Last element is the active segment.
    segments: Vec<Segment>,
    /// Active segment writer (always points at `segments.last()`).
    active: BufWriter<File>,
}

#[derive(Debug)]
struct Segment {
    path: PathBuf,
    start_seq: u64,
    /// Highest seq written to this segment, or `None` if empty.
    last_seq: Option<u64>,
    /// Current file size in bytes.
    size: u64,
}

impl DiskWal {
    pub fn open(config: WalConfig) -> Result<Self> {
        fs::create_dir_all(&config.dir)?;

        let mut found: Vec<(u64, PathBuf)> = fs::read_dir(&config.dir)?
            .filter_map(|e| e.ok())
            .filter_map(|e| {
                let name = e.file_name().to_string_lossy().into_owned();
                parse_segment_start(&name).map(|s| (s, e.path()))
            })
            .collect();
        found.sort_by_key(|(s, _)| *s);

        let mut segments = Vec::new();
        for (start_seq, path) in found {
            segments.push(recover_segment(&path, start_seq)?);
        }

        if segments.is_empty() {
            let path = config.dir.join(segment_filename(0));
            File::create(&path)?;
            segments.push(Segment { path, start_seq: 0, last_seq: None, size: 0 });
        }

        let active_path = segments.last().expect("non-empty by construction").path.clone();
        let active = OpenOptions::new().append(true).open(&active_path)?;
        let active = BufWriter::new(active);

        Ok(Self { config, inner: Mutex::new(Inner { segments, active }) })
    }

    /// Total number of segment files currently on disk.
    pub fn segment_count(&self) -> usize {
        self.inner.lock().segments.len()
    }

    /// Rotate the active segment. Caller holds `inner`.
    fn rotate(&self, inner: &mut Inner) -> Result<()> {
        inner.active.flush()?;
        inner.active.get_ref().sync_data()?;

        let last = inner.segments.last().expect("at least one segment");
        let next_start = last.last_seq.map(|s| s + 1).unwrap_or(last.start_seq);
        let new_path = self.config.dir.join(segment_filename(next_start));
        File::create(&new_path)?;
        let new_file = OpenOptions::new().append(true).open(&new_path)?;

        inner.segments.push(Segment {
            path: new_path,
            start_seq: next_start,
            last_seq: None,
            size: 0,
        });
        inner.active = BufWriter::new(new_file);
        Ok(())
    }
}

impl Wal for DiskWal {
    fn append(&self, entry: WalEntry) -> Result<WalOffset> {
        let seq = entry.seq;
        let mut buf = Vec::with_capacity(RECORD_HEADER + BODY_HEADER + entry.payload.len());
        encode_record(&entry, &mut buf);
        let written = buf.len() as u64;

        let mut inner = self.inner.lock();
        inner.active.write_all(&buf)?;

        if self.config.sync_on_append {
            inner.active.flush()?;
            inner.active.get_ref().sync_data()?;
        }

        let seg = inner.segments.last_mut().expect("at least one segment");
        seg.size += written;
        seg.last_seq = Some(seq);

        let needs_rotation = seg.size >= self.config.max_segment_bytes;
        if needs_rotation {
            self.rotate(&mut inner)?;
        }

        Ok(seq)
    }

    fn sync(&self) -> Result<()> {
        let mut inner = self.inner.lock();
        inner.active.flush()?;
        inner.active.get_ref().sync_data()?;
        Ok(())
    }

    fn read_from(&self, offset: WalOffset) -> Result<Vec<WalEntry>> {
        let mut inner = self.inner.lock();
        inner.active.flush()?; // make recent appends visible to readers

        let mut out = Vec::new();
        for seg in inner.segments.iter() {
            let interesting = match seg.last_seq {
                Some(last) => last >= offset,
                None => false,
            };
            if !interesting {
                continue;
            }
            let file = OpenOptions::new().read(true).open(&seg.path)?;
            let mut reader = BufReader::new(file);
            loop {
                match read_one_record(&mut reader)? {
                    Some(entry) => {
                        if entry.seq >= offset {
                            out.push(entry);
                        }
                    }
                    None => break,
                }
            }
        }
        Ok(out)
    }

    fn truncate(&self, before: WalOffset) -> Result<()> {
        let mut inner = self.inner.lock();
        let last_idx = inner.segments.len() - 1;

        let mut to_delete = Vec::new();
        let mut keep = Vec::with_capacity(inner.segments.len());
        for (i, seg) in inner.segments.drain(..).enumerate() {
            let removable = i != last_idx
                && match seg.last_seq {
                    Some(last) => last < before,
                    None => true,
                };
            if removable {
                to_delete.push(seg.path);
            } else {
                keep.push(seg);
            }
        }
        inner.segments = keep;

        for path in to_delete {
            fs::remove_file(&path)?;
        }
        Ok(())
    }

    fn last_seq(&self) -> Result<Option<u64>> {
        let inner = self.inner.lock();
        Ok(inner.segments.iter().rev().find_map(|s| s.last_seq))
    }
}

// ---- helpers ------------------------------------------------------------

fn segment_filename(start_seq: u64) -> String {
    format!("wal-{:020}.log", start_seq)
}

fn parse_segment_start(name: &str) -> Option<u64> {
    let rest = name.strip_prefix("wal-")?.strip_suffix(".log")?;
    rest.parse::<u64>().ok()
}

fn encode_record(entry: &WalEntry, buf: &mut Vec<u8>) {
    let body_len = (BODY_HEADER + entry.payload.len()) as u32;
    let mut body = Vec::with_capacity(body_len as usize);
    body.extend_from_slice(&entry.seq.to_le_bytes());
    body.push(entry.kind.encode());
    body.extend_from_slice(&entry.payload);

    let crc = crc32fast::hash(&body);

    buf.extend_from_slice(&body_len.to_le_bytes());
    buf.extend_from_slice(&crc.to_le_bytes());
    buf.extend_from_slice(&body);
}

/// Read one record. Returns `Ok(None)` for clean EOF, torn write, CRC
/// mismatch, or implausible length — the caller treats all these as "stop
/// reading this segment."
fn read_one_record<R: Read>(reader: &mut R) -> io::Result<Option<WalEntry>> {
    let mut header = [0u8; RECORD_HEADER];
    if !read_exact_or_eof(reader, &mut header)? {
        return Ok(None);
    }
    let body_len = u32::from_le_bytes(header[..4].try_into().unwrap());
    let stored_crc = u32::from_le_bytes(header[4..8].try_into().unwrap());

    if body_len < BODY_HEADER as u32 || body_len > MAX_RECORD_BODY {
        return Ok(None);
    }

    let mut body = vec![0u8; body_len as usize];
    if !read_exact_or_eof(reader, &mut body)? {
        return Ok(None);
    }

    if crc32fast::hash(&body) != stored_crc {
        return Ok(None);
    }

    let seq = u64::from_le_bytes(body[..8].try_into().unwrap());
    let kind = match WalEntryKind::decode(body[8]) {
        Some(k) => k,
        None => return Ok(None),
    };
    let payload = Bytes::copy_from_slice(&body[9..]);
    Ok(Some(WalEntry { seq, kind, payload }))
}

/// Read exact bytes, but treat any EOF (including a short read mid-record)
/// as a clean stop rather than an error.
fn read_exact_or_eof<R: Read>(reader: &mut R, buf: &mut [u8]) -> io::Result<bool> {
    let mut read_so_far = 0;
    while read_so_far < buf.len() {
        match reader.read(&mut buf[read_so_far..]) {
            Ok(0) => return Ok(false),
            Ok(n) => read_so_far += n,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        }
    }
    Ok(true)
}

fn recover_segment(path: &Path, start_seq: u64) -> Result<Segment> {
    let mut file = OpenOptions::new().read(true).write(true).open(path)?;
    let mut reader = BufReader::new(&mut file);

    let mut last_seq: Option<u64> = None;
    let mut good_bytes: u64 = 0;

    loop {
        let pos = reader.stream_position()?;
        match read_one_record(&mut reader)? {
            Some(entry) => {
                last_seq = Some(entry.seq);
                good_bytes = reader.stream_position()?;
                let _ = pos;
            }
            None => {
                // Truncate any partial / corrupted tail.
                drop(reader);
                file.set_len(good_bytes)?;
                file.sync_data()?;
                break;
            }
        }
    }

    Ok(Segment { path: path.to_path_buf(), start_seq, last_seq, size: good_bytes })
}

// `?` converts `std::io::Error` into `kepler_types::Error` via the `#[from]`
// on `Error::Io` — no helper needed.
fn _assert_from() {
    fn _check<E: From<io::Error>>() {}
    _check::<Error>();
}

// ---- tests --------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::SeekFrom;
    use tempfile::TempDir;

    fn entry(seq: u64, kind: WalEntryKind, payload: &[u8]) -> WalEntry {
        WalEntry { seq, kind, payload: Bytes::copy_from_slice(payload) }
    }

    fn small_config(dir: &Path) -> WalConfig {
        WalConfig {
            dir: dir.to_path_buf(),
            max_segment_bytes: 512, // small to force rotation in tests
            sync_on_append: true,
        }
    }

    #[test]
    fn append_then_read_roundtrip() {
        let tmp = TempDir::new().unwrap();
        let wal = DiskWal::open(small_config(tmp.path())).unwrap();
        wal.append(entry(1, WalEntryKind::Put, b"alpha")).unwrap();
        wal.append(entry(2, WalEntryKind::Delete, b"")).unwrap();
        wal.append(entry(3, WalEntryKind::Commit, b"end")).unwrap();

        let entries = wal.read_from(0).unwrap();
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].seq, 1);
        assert_eq!(entries[0].payload.as_ref(), b"alpha");
        assert_eq!(entries[1].kind, WalEntryKind::Delete);
        assert_eq!(entries[2].seq, 3);
    }

    #[test]
    fn read_from_offset_filters() {
        let tmp = TempDir::new().unwrap();
        let wal = DiskWal::open(small_config(tmp.path())).unwrap();
        for i in 1..=5 {
            wal.append(entry(i, WalEntryKind::Put, &i.to_le_bytes())).unwrap();
        }
        let entries = wal.read_from(3).unwrap();
        assert_eq!(entries.iter().map(|e| e.seq).collect::<Vec<_>>(), vec![3, 4, 5]);
    }

    #[test]
    fn persists_across_reopen() {
        let tmp = TempDir::new().unwrap();
        {
            let wal = DiskWal::open(small_config(tmp.path())).unwrap();
            wal.append(entry(1, WalEntryKind::Put, b"one")).unwrap();
            wal.append(entry(2, WalEntryKind::Put, b"two")).unwrap();
        }
        let wal = DiskWal::open(small_config(tmp.path())).unwrap();
        let entries = wal.read_from(0).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[1].payload.as_ref(), b"two");
        assert_eq!(wal.last_seq().unwrap(), Some(2));
    }

    #[test]
    fn rotates_at_max_segment_size() {
        let tmp = TempDir::new().unwrap();
        let wal = DiskWal::open(small_config(tmp.path())).unwrap();
        let blob = vec![0xAB; 200];
        for i in 1..=10 {
            wal.append(entry(i, WalEntryKind::Put, &blob)).unwrap();
        }
        // 10 records × (8 header + 9 body header + 200 payload) ≈ 2170 bytes
        // with max_segment_bytes=512, expect >= 4 segments.
        assert!(wal.segment_count() >= 4, "got {} segments", wal.segment_count());

        let entries = wal.read_from(0).unwrap();
        assert_eq!(entries.len(), 10);
    }

    #[test]
    fn truncate_removes_old_segments() {
        let tmp = TempDir::new().unwrap();
        let wal = DiskWal::open(small_config(tmp.path())).unwrap();
        let blob = vec![0u8; 200];
        for i in 1..=10 {
            wal.append(entry(i, WalEntryKind::Put, &blob)).unwrap();
        }
        let before = wal.segment_count();
        wal.truncate(9).unwrap();
        let after = wal.segment_count();
        assert!(after < before, "expected segments to be deleted: {before} -> {after}");

        // Active segment still holds something; entries with seq >= 9 still readable.
        let entries = wal.read_from(9).unwrap();
        assert!(entries.iter().all(|e| e.seq >= 9));
        assert!(entries.iter().any(|e| e.seq == 10));
    }

    #[test]
    fn recovers_from_torn_tail() {
        let tmp = TempDir::new().unwrap();
        {
            let wal = DiskWal::open(small_config(tmp.path())).unwrap();
            wal.append(entry(1, WalEntryKind::Put, b"good")).unwrap();
            wal.append(entry(2, WalEntryKind::Put, b"good")).unwrap();
        }

        // Find the active segment file and append garbage that looks like a
        // half-written record.
        let seg_path = std::fs::read_dir(tmp.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .find(|p| p.file_name().and_then(|n| n.to_str()).map(|n| n.starts_with("wal-")).unwrap_or(false))
            .unwrap();
        {
            let mut f = OpenOptions::new().append(true).open(&seg_path).unwrap();
            // Plausible header claiming a 200-byte body, then only 5 bytes — torn.
            f.write_all(&200u32.to_le_bytes()).unwrap();
            f.write_all(&0xDEAD_BEEFu32.to_le_bytes()).unwrap();
            f.write_all(b"abcde").unwrap();
            f.sync_data().unwrap();
        }

        // Reopen — recovery should truncate the torn tail.
        let wal = DiskWal::open(small_config(tmp.path())).unwrap();
        let entries = wal.read_from(0).unwrap();
        assert_eq!(entries.len(), 2, "torn tail should not appear as a record");
        assert_eq!(wal.last_seq().unwrap(), Some(2));

        // Further appends should land cleanly after the truncated tail.
        wal.append(entry(3, WalEntryKind::Put, b"after")).unwrap();
        let entries = wal.read_from(0).unwrap();
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[2].payload.as_ref(), b"after");
    }

    #[test]
    fn rejects_corrupted_crc() {
        let tmp = TempDir::new().unwrap();
        {
            let wal = DiskWal::open(small_config(tmp.path())).unwrap();
            wal.append(entry(1, WalEntryKind::Put, b"first")).unwrap();
            wal.append(entry(2, WalEntryKind::Put, b"second")).unwrap();
        }

        // Flip a byte well inside the second record to break its CRC.
        let seg_path = std::fs::read_dir(tmp.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .find(|p| p.file_name().and_then(|n| n.to_str()).map(|n| n.starts_with("wal-")).unwrap_or(false))
            .unwrap();
        let len = fs::metadata(&seg_path).unwrap().len();
        let mut f = OpenOptions::new().read(true).write(true).open(&seg_path).unwrap();
        f.seek(SeekFrom::Start(len - 3)).unwrap();
        f.write_all(&[0xFF]).unwrap();
        f.sync_data().unwrap();
        drop(f);

        // Reopen: the second record should be dropped during recovery.
        let wal = DiskWal::open(small_config(tmp.path())).unwrap();
        let entries = wal.read_from(0).unwrap();
        assert_eq!(entries.len(), 1, "corrupted record should not appear");
        assert_eq!(entries[0].seq, 1);
    }

    #[test]
    fn empty_dir_opens_cleanly() {
        let tmp = TempDir::new().unwrap();
        let wal = DiskWal::open(small_config(tmp.path())).unwrap();
        assert_eq!(wal.last_seq().unwrap(), None);
        assert_eq!(wal.read_from(0).unwrap().len(), 0);
    }
}
