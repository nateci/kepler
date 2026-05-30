//! SSTable v0 — immutable, sorted, on-disk run.
//!
//! File layout:
//!
//! ```text
//!   ┌──────────────────────────────────┐
//!   │   Data section                   │  sorted records, see encode_record
//!   │   (variable size)                │
//!   ├──────────────────────────────────┤
//!   │   Index section                  │  every record's (keylen u32, key,
//!   │                                  │  offset u64), sorted by key
//!   ├──────────────────────────────────┤
//!   │   Footer (40 bytes fixed):       │
//!   │     index_offset  u64            │
//!   │     index_len     u64            │
//!   │     num_entries   u64            │
//!   │     max_seq       u64            │
//!   │     magic         u64            │
//!   └──────────────────────────────────┘
//! ```
//!
//! v0 design notes / known limitations:
//! - **Full in-memory index.** Every key's `(key, offset)` is loaded on open.
//!   Small files only; production wants a sparse index + block cache.
//! - **No bloom filter.** Every `get` that misses still pays a binary search +
//!   one open() per miss. Add a bloom in a follow-up.
//! - **One `File::open` per `get`.** Cheap on Linux/macOS, more expensive on
//!   Windows. Pool of file handles or `Mmap` is a later optimization.
//! - **No compression.** Records are stored raw.

use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use bytes::Bytes;

use kepler_types::{Error, Key, Result, Value};

const RECORD_PUT: u8 = 0;
const RECORD_TOMBSTONE: u8 = 1;
const FOOTER_SIZE: u64 = 40;
const SST_MAGIC: u64 = 0xCAFE_F00D_DEAD_BEEF;

pub struct SsTable {
    path: PathBuf,
    generation: u64,
    /// `(key, file_offset)` pairs, sorted by key.
    index: Vec<(Bytes, u64)>,
    max_seq: u64,
}

impl SsTable {
    pub fn open(path: &Path, generation: u64) -> Result<Self> {
        let mut file = File::open(path)?;
        let file_len = file.metadata()?.len();
        if file_len < FOOTER_SIZE {
            return Err(Error::Storage(format!(
                "sstable {:?}: file too small ({} bytes)",
                path, file_len
            )));
        }

        file.seek(SeekFrom::Start(file_len - FOOTER_SIZE))?;
        let mut footer = [0u8; FOOTER_SIZE as usize];
        file.read_exact(&mut footer)?;

        let index_offset = u64::from_le_bytes(footer[0..8].try_into().unwrap());
        let index_len = u64::from_le_bytes(footer[8..16].try_into().unwrap());
        let _num_entries = u64::from_le_bytes(footer[16..24].try_into().unwrap());
        let max_seq = u64::from_le_bytes(footer[24..32].try_into().unwrap());
        let magic = u64::from_le_bytes(footer[32..40].try_into().unwrap());

        if magic != SST_MAGIC {
            return Err(Error::Storage(format!(
                "sstable {:?}: bad magic 0x{:x}",
                path, magic
            )));
        }
        if index_offset + index_len + FOOTER_SIZE != file_len {
            return Err(Error::Storage(format!(
                "sstable {:?}: footer offsets don't add up",
                path
            )));
        }

        file.seek(SeekFrom::Start(index_offset))?;
        let mut index_bytes = vec![0u8; index_len as usize];
        file.read_exact(&mut index_bytes)?;

        let index = parse_index(&index_bytes)
            .ok_or_else(|| Error::Storage(format!("sstable {:?}: corrupt index", path)))?;

        Ok(Self { path: path.to_path_buf(), generation, index, max_seq })
    }

    pub fn max_seq(&self) -> u64 {
        self.max_seq
    }

    pub fn num_entries(&self) -> usize {
        self.index.len()
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn generation(&self) -> u64 {
        self.generation
    }

    /// Read every record in key order. Used by the compactor.
    pub fn iter_all(&self) -> Result<Vec<(Key, Option<Value>)>> {
        if self.index.is_empty() {
            return Ok(Vec::new());
        }
        let file = File::open(&self.path)?;
        let mut reader = BufReader::new(file);
        let mut out = Vec::with_capacity(self.index.len());
        for _ in 0..self.index.len() {
            out.push(read_record(&mut reader)?);
        }
        Ok(out)
    }

    /// Look up `key`.
    /// - `Ok(None)`: not in this SST at all
    /// - `Ok(Some(Some(v)))`: key has value `v`
    /// - `Ok(Some(None))`: key has a tombstone in this SST
    pub fn get(&self, key: &[u8]) -> Result<Option<Option<Value>>> {
        let pos = match self.index.binary_search_by(|(k, _)| k.as_ref().cmp(key)) {
            Ok(i) => i,
            Err(_) => return Ok(None),
        };
        let (_, offset) = &self.index[pos];

        let mut file = File::open(&self.path)?;
        file.seek(SeekFrom::Start(*offset))?;
        let (_, value) = read_record(&mut file)?;
        Ok(Some(value))
    }

    /// Collect entries with `start <= key < end`. Includes tombstones so the
    /// caller can decide how to merge with newer layers.
    pub fn scan(&self, start: &[u8], end: &[u8]) -> Result<Vec<(Key, Option<Value>)>> {
        let lo = self.index.partition_point(|(k, _)| k.as_ref() < start);
        let hi = self.index.partition_point(|(k, _)| k.as_ref() < end);
        if lo >= hi {
            return Ok(Vec::new());
        }

        let (_, start_offset) = &self.index[lo];
        let file = File::open(&self.path)?;
        let mut reader = BufReader::new(file);
        reader.seek(SeekFrom::Start(*start_offset))?;

        let mut out = Vec::with_capacity(hi - lo);
        for _ in lo..hi {
            out.push(read_record(&mut reader)?);
        }
        Ok(out)
    }
}

pub struct SsTableWriter;

impl SsTableWriter {
    /// Write `entries` to `path` via a `.tmp` file + rename, so a partially
    /// written SST is never visible to readers.
    ///
    /// Entries MUST already be sorted by key. The BTreeMap-backed `MemTable`
    /// guarantees this for the flush path.
    pub fn write<'a, I>(path: &Path, entries: I, max_seq: u64) -> Result<()>
    where
        I: IntoIterator<Item = (&'a Key, &'a Option<Value>)>,
    {
        let tmp_path = path.with_extension("sst.tmp");
        {
            let file = File::create(&tmp_path)?;
            let mut writer = BufWriter::new(file);

            let mut index: Vec<(Bytes, u64)> = Vec::new();
            let mut offset: u64 = 0;
            let mut num_entries: u64 = 0;

            for (key, value) in entries {
                index.push((key.clone(), offset));
                let bytes = encode_record(key, value);
                writer.write_all(&bytes)?;
                offset += bytes.len() as u64;
                num_entries += 1;
            }

            // Index section
            let index_offset = offset;
            let mut index_buf = Vec::new();
            for (key, off) in &index {
                index_buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
                index_buf.extend_from_slice(key);
                index_buf.extend_from_slice(&off.to_le_bytes());
            }
            writer.write_all(&index_buf)?;
            let index_len = index_buf.len() as u64;

            // Footer
            let mut footer = Vec::with_capacity(FOOTER_SIZE as usize);
            footer.extend_from_slice(&index_offset.to_le_bytes());
            footer.extend_from_slice(&index_len.to_le_bytes());
            footer.extend_from_slice(&num_entries.to_le_bytes());
            footer.extend_from_slice(&max_seq.to_le_bytes());
            footer.extend_from_slice(&SST_MAGIC.to_le_bytes());
            writer.write_all(&footer)?;

            writer.flush()?;
            writer.get_ref().sync_data()?;
        }

        std::fs::rename(&tmp_path, path)?;
        Ok(())
    }
}

fn encode_record(key: &[u8], value: &Option<Value>) -> Vec<u8> {
    let val_bytes: &[u8] = value.as_ref().map(|v| v.as_ref()).unwrap_or(&[]);
    let cap = 1 + 4 + key.len() + if value.is_some() { 4 + val_bytes.len() } else { 0 };
    let mut buf = Vec::with_capacity(cap);
    buf.push(if value.is_some() { RECORD_PUT } else { RECORD_TOMBSTONE });
    buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
    buf.extend_from_slice(key);
    if value.is_some() {
        buf.extend_from_slice(&(val_bytes.len() as u32).to_le_bytes());
        buf.extend_from_slice(val_bytes);
    }
    buf
}

fn read_record<R: Read>(reader: &mut R) -> Result<(Key, Option<Value>)> {
    let mut type_buf = [0u8; 1];
    reader.read_exact(&mut type_buf)?;

    let mut keylen_buf = [0u8; 4];
    reader.read_exact(&mut keylen_buf)?;
    let keylen = u32::from_le_bytes(keylen_buf) as usize;

    let mut key = vec![0u8; keylen];
    reader.read_exact(&mut key)?;
    let key = Bytes::from(key);

    let value = match type_buf[0] {
        RECORD_PUT => {
            let mut vallen_buf = [0u8; 4];
            reader.read_exact(&mut vallen_buf)?;
            let vallen = u32::from_le_bytes(vallen_buf) as usize;
            let mut val = vec![0u8; vallen];
            reader.read_exact(&mut val)?;
            Some(Bytes::from(val))
        }
        RECORD_TOMBSTONE => None,
        other => return Err(Error::Storage(format!("sstable: bad record type {}", other))),
    };
    Ok((key, value))
}

fn parse_index(bytes: &[u8]) -> Option<Vec<(Bytes, u64)>> {
    let mut out = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        if i + 4 > bytes.len() {
            return None;
        }
        let keylen = u32::from_le_bytes(bytes[i..i + 4].try_into().ok()?) as usize;
        i += 4;
        if i + keylen + 8 > bytes.len() {
            return None;
        }
        let key = Bytes::copy_from_slice(&bytes[i..i + keylen]);
        i += keylen;
        let offset = u64::from_le_bytes(bytes[i..i + 8].try_into().ok()?);
        i += 8;
        out.push((key, offset));
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn b(s: &str) -> Bytes {
        Bytes::copy_from_slice(s.as_bytes())
    }

    fn write_simple_sst(dir: &Path, name: &str, kvs: &[(&str, Option<&str>)]) -> PathBuf {
        let path = dir.join(name);
        let entries: Vec<(Key, Option<Value>)> = kvs
            .iter()
            .map(|(k, v)| (b(k), v.map(b)))
            .collect();
        let iter = entries.iter().map(|(k, v)| (k, v));
        SsTableWriter::write(&path, iter, 42).unwrap();
        path
    }

    #[test]
    fn roundtrip_writes_and_reads() {
        let tmp = TempDir::new().unwrap();
        let path = write_simple_sst(
            tmp.path(),
            "test.sst",
            &[("a", Some("1")), ("b", Some("2")), ("c", Some("3"))],
        );
        let sst = SsTable::open(&path, 1).unwrap();
        assert_eq!(sst.num_entries(), 3);
        assert_eq!(sst.max_seq(), 42);
        assert_eq!(sst.get(b"a").unwrap(), Some(Some(b("1"))));
        assert_eq!(sst.get(b"b").unwrap(), Some(Some(b("2"))));
        assert_eq!(sst.get(b"c").unwrap(), Some(Some(b("3"))));
        assert_eq!(sst.get(b"missing").unwrap(), None);
    }

    #[test]
    fn tombstone_roundtrip() {
        let tmp = TempDir::new().unwrap();
        let path =
            write_simple_sst(tmp.path(), "t.sst", &[("a", Some("1")), ("b", None), ("c", Some("3"))]);
        let sst = SsTable::open(&path, 1).unwrap();
        assert_eq!(sst.get(b"a").unwrap(), Some(Some(b("1"))));
        assert_eq!(sst.get(b"b").unwrap(), Some(None));
        assert_eq!(sst.get(b"c").unwrap(), Some(Some(b("3"))));
    }

    #[test]
    fn scan_range() {
        let tmp = TempDir::new().unwrap();
        let path = write_simple_sst(
            tmp.path(),
            "s.sst",
            &[
                ("a", Some("1")),
                ("b", Some("2")),
                ("c", Some("3")),
                ("d", Some("4")),
            ],
        );
        let sst = SsTable::open(&path, 1).unwrap();
        let got = sst.scan(b"b", b"d").unwrap();
        let keys: Vec<_> = got.into_iter().map(|(k, _)| k).collect();
        assert_eq!(keys, vec![b("b"), b("c")]);
    }

    #[test]
    fn rejects_bad_magic() {
        use std::fs::OpenOptions;
        use std::io::Write as _;
        let tmp = TempDir::new().unwrap();
        let path =
            write_simple_sst(tmp.path(), "bad.sst", &[("a", Some("1"))]);
        // Overwrite the magic in the footer with garbage.
        let mut f = OpenOptions::new().write(true).read(true).open(&path).unwrap();
        let len = f.metadata().unwrap().len();
        f.seek(SeekFrom::Start(len - 8)).unwrap();
        f.write_all(&[0xFFu8; 8]).unwrap();
        f.sync_data().unwrap();
        assert!(SsTable::open(&path, 1).is_err());
    }

    #[test]
    fn empty_value_roundtrip() {
        let tmp = TempDir::new().unwrap();
        let path = write_simple_sst(tmp.path(), "e.sst", &[("k", Some(""))]);
        let sst = SsTable::open(&path, 1).unwrap();
        assert_eq!(sst.get(b"k").unwrap(), Some(Some(b(""))));
    }
}
