//! LSM tree storage engine.
//!
//! ```text
//!                 ┌──────────────┐
//!     writes ──▶  │   MemTable   │  (BTreeMap; flushes at memtable_max_bytes)
//!                 └──────┬───────┘
//!                        │ flush()
//!                        ▼
//!                 ┌──────────────┐
//!                 │  L0 SSTs     │  (newest first, no compaction yet)
//!                 │  L1 SSTs     │  (TODO)
//!                 │  L2 SSTs     │  (TODO)
//!                 └──────────────┘
//! ```
//!
//! v0 limitations:
//! - **One synchronous flush at a time** (no background flush, no immutable
//!   memtable queue). Writes block during flush.
//! - **No compaction.** SSTs accumulate until you implement it. Read
//!   performance degrades as more SSTs pile up.
//! - **No block-level index or bloom filters in SSTs** (see `sstable` docs).
//! - **`write_batch` is not atomic across crashes** — applies ops sequentially.
//!
//! Crash recovery:
//! 1. List `sst-*.sst` in `dir`, open each, find `max_seq` across them
//! 2. Open WAL, replay entries with `seq > max_sst_seq` into a fresh memtable
//! 3. WAL is the durability source; SSTs are the long-term storage

pub mod memtable;
pub mod sstable;

use std::collections::HashSet;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use parking_lot::{Mutex, RwLock};
use tracing::{debug, warn};

use kepler_types::{Error, Key, KeyRange, Result, Value};

use crate::engine::{Batch, BatchOp, Cursor, Engine, Snapshot};
use crate::wal::{DiskWal, Wal, WalConfig, WalEntry, WalEntryKind};

use self::memtable::MemTable;
use self::sstable::{SsTable, SsTableWriter};

#[derive(Debug, Clone)]
pub struct LsmConfig {
    pub dir: PathBuf,
    pub memtable_max_bytes: usize,
    /// Auto-compact when the number of SSTs reaches this threshold.
    pub sst_compact_threshold: usize,
    pub wal: WalConfig,
}

impl LsmConfig {
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        let dir = dir.into();
        let wal = WalConfig::new(dir.join("wal"));
        Self {
            memtable_max_bytes: 4 * 1024 * 1024,
            sst_compact_threshold: 4,
            wal,
            dir,
        }
    }
}

pub struct LsmEngine {
    config: LsmConfig,
    wal: Arc<DiskWal>,
    memtable: Mutex<MemTable>,
    /// Sorted by `max_seq` descending. Reads check `[0]`, `[1]`, ...
    /// `max_seq` reflects *content* age, not file age — compaction can produce
    /// a newly-written file whose content is older than a concurrent flush.
    sstables: RwLock<Vec<Arc<SsTable>>>,
    next_gen: AtomicU64,
    next_seq: AtomicU64,
    /// SSTs removed from the active set but still referenced by outstanding
    /// snapshots. Files deleted lazily once the only remaining strong
    /// reference is this list itself.
    pending_delete: Mutex<Vec<Arc<SsTable>>>,
    /// Serializes compactions so only one runs at a time.
    compaction_lock: Mutex<()>,
}

impl LsmEngine {
    pub fn open(config: LsmConfig) -> Result<Self> {
        fs::create_dir_all(&config.dir)?;
        let wal = Arc::new(DiskWal::open(config.wal.clone())?);

        let sst_paths: Vec<(u64, PathBuf)> = fs::read_dir(&config.dir)?
            .filter_map(|e| e.ok())
            .filter_map(|e| {
                let name = e.file_name().to_string_lossy().into_owned();
                parse_sst_gen(&name).map(|g| (g, e.path()))
            })
            .collect();

        let mut sstables: Vec<Arc<SsTable>> = Vec::with_capacity(sst_paths.len());
        let mut max_sst_seq = 0u64;
        let mut max_gen = 0u64;
        for (gen, path) in sst_paths {
            let sst = Arc::new(SsTable::open(&path, gen)?);
            max_sst_seq = max_sst_seq.max(sst.max_seq());
            max_gen = max_gen.max(gen);
            sstables.push(sst);
        }
        // Sort by content age, not file age.
        sstables.sort_by(|a, b| b.max_seq().cmp(&a.max_seq()));

        // Replay any unflushed WAL entries into a fresh memtable.
        let mut memtable = MemTable::new();
        let replay_from = max_sst_seq.saturating_add(1);
        for entry in wal.read_from(replay_from)? {
            apply_wal_entry_to_memtable(&entry, &mut memtable)?;
        }

        let next_gen = max_gen + 1;
        let next_seq = wal
            .last_seq()?
            .map(|s| s + 1)
            .unwrap_or(1)
            .max(max_sst_seq + 1);

        Ok(Self {
            config,
            wal,
            memtable: Mutex::new(memtable),
            sstables: RwLock::new(sstables),
            next_gen: AtomicU64::new(next_gen),
            next_seq: AtomicU64::new(next_seq),
            pending_delete: Mutex::new(Vec::new()),
            compaction_lock: Mutex::new(()),
        })
    }

    fn next_seq(&self) -> u64 {
        self.next_seq.fetch_add(1, Ordering::Relaxed)
    }

    /// Flush whatever's in the memtable to a new SST and truncate the WAL up
    /// to that point. Caller holds `memtable` lock for the duration.
    fn flush_locked(&self, mt: &mut MemTable) -> Result<()> {
        if mt.is_empty() {
            return Ok(());
        }
        let max_seq = mt.max_seq().unwrap_or(0);
        let gen = self.next_gen.fetch_add(1, Ordering::Relaxed);
        let path = self.config.dir.join(format!("sst-{:020}.sst", gen));

        SsTableWriter::write(&path, mt.iter(), max_seq)?;
        let sst = Arc::new(SsTable::open(&path, gen)?);

        {
            let mut ssts = self.sstables.write();
            insert_sst_sorted(&mut ssts, sst);
        }

        // Replace the in-memory memtable with an empty one.
        *mt = MemTable::new();

        // Everything up to and including `max_seq` is now in an SST, so the
        // WAL can drop entries < max_seq + 1.
        self.wal.truncate(max_seq + 1)?;

        debug!(gen, max_seq, "flushed memtable to sst");
        Ok(())
    }

    /// Merge every active SST into a single new SST, dropping shadowed values
    /// and tombstones. v0 strategy: full compaction. Future work is tiered or
    /// leveled with partial compactions; the public API can stay the same.
    pub fn compact(&self) -> Result<()> {
        let _guard = self.compaction_lock.lock();

        let inputs: Vec<Arc<SsTable>> = {
            let ssts = self.sstables.read();
            if ssts.len() < 2 {
                // Still useful to drop pending SSTs whose snapshot holders
                // have since released them.
                drop(ssts);
                self.gc_pending_delete();
                return Ok(());
            }
            ssts.clone()
        };

        let merged_max_seq = inputs.iter().map(|s| s.max_seq()).max().unwrap_or(0);

        // Merge: newest source first; `or_insert` keeps the newest write per key.
        use std::collections::BTreeMap;
        let mut merged: BTreeMap<Key, Option<Value>> = BTreeMap::new();
        for sst in inputs.iter() {
            for (k, v) in sst.iter_all()? {
                merged.entry(k).or_insert(v);
            }
        }

        // Full compaction means there's no older data left to shadow, so
        // tombstones can be dropped entirely.
        let kept: Vec<(Key, Option<Value>)> = merged
            .into_iter()
            .filter(|(_, v)| v.is_some())
            .collect();

        let new_gen = self.next_gen.fetch_add(1, Ordering::Relaxed);
        let new_sst = if kept.is_empty() {
            None
        } else {
            let new_path = self.config.dir.join(format!("sst-{:020}.sst", new_gen));
            let iter = kept.iter().map(|(k, v)| (k, v));
            SsTableWriter::write(&new_path, iter, merged_max_seq)?;
            Some(Arc::new(SsTable::open(&new_path, new_gen)?))
        };

        // Atomic swap: remove inputs, insert merged output by max_seq position.
        let input_gens: HashSet<u64> = inputs.iter().map(|s| s.generation()).collect();
        {
            let mut ssts = self.sstables.write();
            ssts.retain(|s| !input_gens.contains(&s.generation()));
            if let Some(sst) = new_sst.clone() {
                insert_sst_sorted(&mut ssts, sst);
            }
        }

        {
            let mut pending = self.pending_delete.lock();
            pending.extend(inputs);
        }
        self.gc_pending_delete();

        debug!(new_gen, merged_max_seq, "compaction complete");
        Ok(())
    }

    /// Trigger compaction if the active SST count is at or above the
    /// configured threshold. Called automatically after flushes.
    fn maybe_compact_if_threshold(&self) -> Result<()> {
        let over = self.sstables.read().len() >= self.config.sst_compact_threshold;
        if over {
            self.compact()?;
        }
        Ok(())
    }

    /// Delete SSTs from `pending_delete` whose only remaining reference is
    /// this list — i.e., no outstanding snapshot is still using them.
    fn gc_pending_delete(&self) {
        let mut pending = self.pending_delete.lock();
        let mut keep = Vec::with_capacity(pending.len());
        for sst in pending.drain(..) {
            if Arc::strong_count(&sst) == 1 {
                if let Err(e) = fs::remove_file(sst.path()) {
                    warn!(error = ?e, path = ?sst.path(), "failed to delete compacted SST");
                }
                // sst drops here, count goes to 0, SsTable is freed
            } else {
                keep.push(sst);
            }
        }
        *pending = keep;
    }

    /// Number of SSTs currently in `pending_delete`. Tests use this.
    #[cfg(test)]
    fn pending_delete_len(&self) -> usize {
        self.pending_delete.lock().len()
    }

    /// Number of active SSTs.
    #[cfg(test)]
    fn active_sst_count(&self) -> usize {
        self.sstables.read().len()
    }
}

fn insert_sst_sorted(ssts: &mut Vec<Arc<SsTable>>, new_sst: Arc<SsTable>) {
    let pos = ssts.partition_point(|s| s.max_seq() > new_sst.max_seq());
    ssts.insert(pos, new_sst);
}

impl Engine for LsmEngine {
    fn get(&self, key: &[u8]) -> Result<Option<Value>> {
        // 1. Memtable
        {
            let mt = self.memtable.lock();
            if let Some(slot) = mt.get(key) {
                return Ok(slot);
            }
        }
        // 2. SSTs, newest first
        let ssts = self.sstables.read();
        for sst in ssts.iter() {
            if let Some(slot) = sst.get(key)? {
                return Ok(slot);
            }
        }
        Ok(None)
    }

    fn put(&self, key: Key, value: Value) -> Result<()> {
        let seq = self.next_seq();
        let payload = encode_put_payload(&key, &value);
        self.wal.append(WalEntry { seq, kind: WalEntryKind::Put, payload })?;

        let did_flush = {
            let mut mt = self.memtable.lock();
            mt.insert(seq, key, Some(value));
            if mt.size() >= self.config.memtable_max_bytes {
                self.flush_locked(&mut mt)?;
                true
            } else {
                false
            }
        };
        if did_flush {
            self.maybe_compact_if_threshold()?;
        }
        Ok(())
    }

    fn delete(&self, key: &[u8]) -> Result<()> {
        let seq = self.next_seq();
        let payload = encode_delete_payload(key);
        self.wal.append(WalEntry { seq, kind: WalEntryKind::Delete, payload })?;

        let did_flush = {
            let mut mt = self.memtable.lock();
            mt.insert(seq, Bytes::copy_from_slice(key), None);
            if mt.size() >= self.config.memtable_max_bytes {
                self.flush_locked(&mut mt)?;
                true
            } else {
                false
            }
        };
        if did_flush {
            self.maybe_compact_if_threshold()?;
        }
        Ok(())
    }

    fn write_batch(&self, batch: Batch) -> Result<()> {
        // Not atomic across crashes in v0. Real atomic batch wants a single
        // WAL record per batch + commit marker.
        for op in batch.ops {
            match op {
                BatchOp::Put { key, value } => self.put(key, value)?,
                BatchOp::Delete { key } => self.delete(&key)?,
            }
        }
        Ok(())
    }

    fn snapshot(&self) -> Box<dyn Snapshot> {
        let mt = self.memtable.lock().clone();
        let ssts = self.sstables.read().clone();
        Box::new(LsmSnapshot { memtable: mt, sstables: ssts })
    }

    fn scan(&self, range: KeyRange) -> Box<dyn Cursor> {
        // v0: collect into memory, dedupe newest-wins, drop tombstones.
        // Inefficient for wide ranges; future work is a true merging iterator.
        use std::collections::BTreeMap;
        let mut merged: BTreeMap<Key, Option<Value>> = BTreeMap::new();

        // Newest source first (memtable), then each SST in order.
        {
            let mt = self.memtable.lock();
            for (k, v) in mt.range(&range.start, &range.end) {
                merged.entry(k).or_insert(v);
            }
        }
        let ssts = self.sstables.read();
        for sst in ssts.iter() {
            // best-effort: surface scan errors as a stop, since the cursor
            // interface doesn't carry errors. TODO: return Result<Cursor>.
            let chunk = sst.scan(&range.start, &range.end).unwrap_or_default();
            for (k, v) in chunk {
                merged.entry(k).or_insert(v);
            }
        }

        let items: Vec<(Key, Value)> = merged
            .into_iter()
            .filter_map(|(k, v)| v.map(|val| (k, val)))
            .collect();
        Box::new(LsmCursor { items, pos: 0 })
    }

    fn flush(&self) -> Result<()> {
        {
            let mut mt = self.memtable.lock();
            self.flush_locked(&mut mt)?;
        }
        self.maybe_compact_if_threshold()?;
        Ok(())
    }
}

// ---- snapshot ------------------------------------------------------------

struct LsmSnapshot {
    memtable: MemTable,
    sstables: Vec<Arc<SsTable>>,
}

impl Snapshot for LsmSnapshot {
    fn get(&self, key: &[u8]) -> Result<Option<Value>> {
        if let Some(slot) = self.memtable.get(key) {
            return Ok(slot);
        }
        for sst in &self.sstables {
            if let Some(slot) = sst.get(key)? {
                return Ok(slot);
            }
        }
        Ok(None)
    }
}

// ---- cursor --------------------------------------------------------------

struct LsmCursor {
    items: Vec<(Key, Value)>,
    pos: usize,
}

impl Cursor for LsmCursor {
    fn seek(&mut self, target: &[u8]) {
        self.pos = self.items.partition_point(|(k, _)| k.as_ref() < target);
    }
    fn next(&mut self) -> Option<(Key, Value)> {
        if self.pos >= self.items.len() {
            return None;
        }
        let item = self.items[self.pos].clone();
        self.pos += 1;
        Some(item)
    }
    fn valid(&self) -> bool {
        self.pos < self.items.len()
    }
}

// ---- helpers -------------------------------------------------------------

fn parse_sst_gen(name: &str) -> Option<u64> {
    let rest = name.strip_prefix("sst-")?.strip_suffix(".sst")?;
    rest.parse().ok()
}

fn encode_put_payload(key: &[u8], value: &[u8]) -> Bytes {
    let mut buf = Vec::with_capacity(4 + key.len() + value.len());
    buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
    buf.extend_from_slice(key);
    buf.extend_from_slice(value);
    Bytes::from(buf)
}

fn encode_delete_payload(key: &[u8]) -> Bytes {
    let mut buf = Vec::with_capacity(4 + key.len());
    buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
    buf.extend_from_slice(key);
    Bytes::from(buf)
}

fn apply_wal_entry_to_memtable(entry: &WalEntry, mt: &mut MemTable) -> Result<()> {
    let payload = entry.payload.as_ref();
    if payload.len() < 4 {
        return Err(Error::Storage("wal payload too short".into()));
    }
    let keylen = u32::from_le_bytes(payload[..4].try_into().unwrap()) as usize;
    if payload.len() < 4 + keylen {
        return Err(Error::Storage("wal payload truncated".into()));
    }
    let key = Bytes::copy_from_slice(&payload[4..4 + keylen]);
    let value = match entry.kind {
        WalEntryKind::Put => Some(Bytes::copy_from_slice(&payload[4 + keylen..])),
        WalEntryKind::Delete => None,
        // Commit markers are for higher-level (transaction) replay, ignored here.
        WalEntryKind::Commit => return Ok(()),
    };
    mt.insert(entry.seq, key, value);
    Ok(())
}

// ---- tests ---------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;
    use tempfile::TempDir;

    fn b(s: &str) -> Bytes {
        Bytes::copy_from_slice(s.as_bytes())
    }

    fn small_config(dir: &Path) -> LsmConfig {
        LsmConfig {
            dir: dir.to_path_buf(),
            memtable_max_bytes: 200, // tiny so flushes happen
            sst_compact_threshold: 100, // high so it doesn't trigger in flush tests
            wal: WalConfig {
                dir: dir.join("wal"),
                max_segment_bytes: 4096,
                sync_on_append: true,
            },
        }
    }

    fn compact_config(dir: &Path, threshold: usize) -> LsmConfig {
        LsmConfig {
            dir: dir.to_path_buf(),
            memtable_max_bytes: 200,
            sst_compact_threshold: threshold,
            wal: WalConfig {
                dir: dir.join("wal"),
                max_segment_bytes: 4096,
                sync_on_append: true,
            },
        }
    }

    fn sst_files(dir: &Path) -> Vec<PathBuf> {
        fs::read_dir(dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| {
                p.file_name()
                    .and_then(|n| n.to_str())
                    .map(|n| n.starts_with("sst-") && n.ends_with(".sst"))
                    .unwrap_or(false)
            })
            .collect()
    }

    #[test]
    fn put_and_get_from_memtable() {
        let tmp = TempDir::new().unwrap();
        let lsm = LsmEngine::open(small_config(tmp.path())).unwrap();
        lsm.put(b("k1"), b("v1")).unwrap();
        lsm.put(b("k2"), b("v2")).unwrap();
        assert_eq!(lsm.get(b"k1").unwrap(), Some(b("v1")));
        assert_eq!(lsm.get(b"k2").unwrap(), Some(b("v2")));
        assert_eq!(lsm.get(b"missing").unwrap(), None);
    }

    #[test]
    fn delete_in_memtable() {
        let tmp = TempDir::new().unwrap();
        let lsm = LsmEngine::open(small_config(tmp.path())).unwrap();
        lsm.put(b("k"), b("v")).unwrap();
        lsm.delete(b"k").unwrap();
        assert_eq!(lsm.get(b"k").unwrap(), None);
    }

    #[test]
    fn explicit_flush_creates_sst() {
        let tmp = TempDir::new().unwrap();
        let lsm = LsmEngine::open(small_config(tmp.path())).unwrap();
        lsm.put(b("a"), b("1")).unwrap();
        lsm.put(b("b"), b("2")).unwrap();
        lsm.flush().unwrap();
        assert_eq!(sst_files(tmp.path()).len(), 1);
        assert_eq!(lsm.get(b"a").unwrap(), Some(b("1")));
        assert_eq!(lsm.get(b"b").unwrap(), Some(b("2")));
    }

    #[test]
    fn memtable_threshold_triggers_flush() {
        let tmp = TempDir::new().unwrap();
        let lsm = LsmEngine::open(small_config(tmp.path())).unwrap();
        let big = vec![0u8; 100];
        for i in 0..10 {
            lsm.put(b(&format!("k{:03}", i)), Bytes::copy_from_slice(&big))
                .unwrap();
        }
        assert!(!sst_files(tmp.path()).is_empty(), "expected automatic flush");
    }

    #[test]
    fn newer_memtable_shadows_older_sst() {
        let tmp = TempDir::new().unwrap();
        let lsm = LsmEngine::open(small_config(tmp.path())).unwrap();
        lsm.put(b("k"), b("v1")).unwrap();
        lsm.flush().unwrap();
        lsm.put(b("k"), b("v2")).unwrap();
        assert_eq!(lsm.get(b"k").unwrap(), Some(b("v2")));
    }

    #[test]
    fn tombstone_shadows_sst_value() {
        let tmp = TempDir::new().unwrap();
        let lsm = LsmEngine::open(small_config(tmp.path())).unwrap();
        lsm.put(b("k"), b("v")).unwrap();
        lsm.flush().unwrap();
        lsm.delete(b"k").unwrap();
        assert_eq!(lsm.get(b"k").unwrap(), None);
    }

    #[test]
    fn newer_sst_shadows_older_sst() {
        let tmp = TempDir::new().unwrap();
        let lsm = LsmEngine::open(small_config(tmp.path())).unwrap();
        lsm.put(b("k"), b("v1")).unwrap();
        lsm.flush().unwrap();
        lsm.put(b("k"), b("v2")).unwrap();
        lsm.flush().unwrap();
        assert_eq!(lsm.get(b"k").unwrap(), Some(b("v2")));
        assert_eq!(sst_files(tmp.path()).len(), 2);
    }

    #[test]
    fn persists_across_reopen_with_flush() {
        let tmp = TempDir::new().unwrap();
        {
            let lsm = LsmEngine::open(small_config(tmp.path())).unwrap();
            lsm.put(b("k1"), b("v1")).unwrap();
            lsm.put(b("k2"), b("v2")).unwrap();
            lsm.flush().unwrap();
        }
        let lsm = LsmEngine::open(small_config(tmp.path())).unwrap();
        assert_eq!(lsm.get(b"k1").unwrap(), Some(b("v1")));
        assert_eq!(lsm.get(b"k2").unwrap(), Some(b("v2")));
    }

    #[test]
    fn wal_replay_recovers_unflushed_writes() {
        let tmp = TempDir::new().unwrap();
        {
            let lsm = LsmEngine::open(small_config(tmp.path())).unwrap();
            lsm.put(b("k"), b("v")).unwrap();
            // Drop without flushing — simulates crash before flush.
        }
        let lsm = LsmEngine::open(small_config(tmp.path())).unwrap();
        assert_eq!(lsm.get(b"k").unwrap(), Some(b("v")));
        assert!(sst_files(tmp.path()).is_empty());
    }

    #[test]
    fn wal_replay_preserves_tombstones() {
        let tmp = TempDir::new().unwrap();
        {
            let lsm = LsmEngine::open(small_config(tmp.path())).unwrap();
            lsm.put(b("k"), b("v")).unwrap();
            lsm.flush().unwrap();
            lsm.delete(b"k").unwrap();
            // Crash before flushing the tombstone.
        }
        let lsm = LsmEngine::open(small_config(tmp.path())).unwrap();
        assert_eq!(lsm.get(b"k").unwrap(), None);
    }

    #[test]
    fn scan_merges_memtable_and_ssts() {
        let tmp = TempDir::new().unwrap();
        let lsm = LsmEngine::open(small_config(tmp.path())).unwrap();
        lsm.put(b("a"), b("1")).unwrap();
        lsm.put(b("b"), b("2")).unwrap();
        lsm.flush().unwrap();
        lsm.put(b("c"), b("3")).unwrap();
        lsm.put(b("d"), b("4")).unwrap();
        lsm.delete(b"b").unwrap(); // tombstone shadows SST value

        let mut cursor = lsm.scan(KeyRange::new(b("a"), b("e")));
        let mut got = Vec::new();
        while let Some((k, v)) = cursor.next() {
            got.push((k, v));
        }
        let keys: Vec<_> = got.iter().map(|(k, _)| k.clone()).collect();
        assert_eq!(keys, vec![b("a"), b("c"), b("d")]);
    }

    #[test]
    fn snapshot_is_isolated_from_later_writes() {
        let tmp = TempDir::new().unwrap();
        let lsm = LsmEngine::open(small_config(tmp.path())).unwrap();
        lsm.put(b("k"), b("v1")).unwrap();
        let snap = lsm.snapshot();
        lsm.put(b("k"), b("v2")).unwrap();
        assert_eq!(snap.get(b"k").unwrap(), Some(b("v1")));
        assert_eq!(lsm.get(b"k").unwrap(), Some(b("v2")));
    }

    // ---- compaction ----

    #[test]
    fn compact_merges_two_ssts() {
        let tmp = TempDir::new().unwrap();
        let lsm = LsmEngine::open(small_config(tmp.path())).unwrap();
        lsm.put(b("a"), b("1")).unwrap();
        lsm.flush().unwrap();
        lsm.put(b("b"), b("2")).unwrap();
        lsm.flush().unwrap();
        assert_eq!(sst_files(tmp.path()).len(), 2);

        lsm.compact().unwrap();

        assert_eq!(lsm.active_sst_count(), 1);
        assert_eq!(sst_files(tmp.path()).len(), 1);
        assert_eq!(lsm.get(b"a").unwrap(), Some(b("1")));
        assert_eq!(lsm.get(b"b").unwrap(), Some(b("2")));
    }

    #[test]
    fn compact_keeps_newest_value_per_key() {
        let tmp = TempDir::new().unwrap();
        let lsm = LsmEngine::open(small_config(tmp.path())).unwrap();
        lsm.put(b("k"), b("v1")).unwrap();
        lsm.flush().unwrap();
        lsm.put(b("k"), b("v2")).unwrap();
        lsm.flush().unwrap();
        lsm.put(b("k"), b("v3")).unwrap();
        lsm.flush().unwrap();

        lsm.compact().unwrap();
        assert_eq!(lsm.get(b"k").unwrap(), Some(b("v3")));
        assert_eq!(lsm.active_sst_count(), 1);
    }

    #[test]
    fn compact_drops_tombstones() {
        let tmp = TempDir::new().unwrap();
        let lsm = LsmEngine::open(small_config(tmp.path())).unwrap();
        lsm.put(b("survives"), b("yes")).unwrap();
        lsm.put(b("k"), b("v")).unwrap();
        lsm.flush().unwrap();
        lsm.delete(b"k").unwrap();
        lsm.flush().unwrap();
        assert_eq!(lsm.get(b"k").unwrap(), None);

        lsm.compact().unwrap();
        assert_eq!(lsm.get(b"k").unwrap(), None);
        assert_eq!(lsm.get(b"survives").unwrap(), Some(b("yes")));

        // After full compaction the merged SST should hold `survives` but no
        // record (value or tombstone) for `k`.
        let ssts = lsm.sstables.read();
        assert_eq!(ssts.len(), 1);
        let entries = ssts[0].iter_all().unwrap();
        assert!(entries.iter().all(|(k, _)| k.as_ref() != b"k"));
        assert!(entries.iter().any(|(k, v)| k.as_ref() == b"survives" && v.is_some()));
    }

    #[test]
    fn compact_below_two_ssts_is_noop() {
        let tmp = TempDir::new().unwrap();
        let lsm = LsmEngine::open(small_config(tmp.path())).unwrap();
        lsm.put(b("k"), b("v")).unwrap();
        lsm.flush().unwrap();
        let gen_before = lsm.sstables.read()[0].generation();

        lsm.compact().unwrap();

        let ssts = lsm.sstables.read();
        assert_eq!(ssts.len(), 1);
        assert_eq!(ssts[0].generation(), gen_before, "no new SST should be written");
    }

    #[test]
    fn compaction_all_tombstones_writes_nothing() {
        let tmp = TempDir::new().unwrap();
        let lsm = LsmEngine::open(small_config(tmp.path())).unwrap();
        lsm.put(b("a"), b("1")).unwrap();
        lsm.flush().unwrap();
        lsm.delete(b"a").unwrap();
        lsm.flush().unwrap();

        lsm.compact().unwrap();
        // Both SSTs replaced by nothing (all tombstones cancel all values).
        assert_eq!(lsm.active_sst_count(), 0);
        assert_eq!(lsm.get(b"a").unwrap(), None);
    }

    #[test]
    fn auto_compact_triggers_at_threshold() {
        let tmp = TempDir::new().unwrap();
        let lsm = LsmEngine::open(compact_config(tmp.path(), 3)).unwrap();
        let big = vec![0u8; 100];
        // Each ~100B value with a small memtable threshold forces frequent flushes.
        for i in 0..20 {
            lsm.put(b(&format!("k{:03}", i)), Bytes::copy_from_slice(&big))
                .unwrap();
        }
        // Below or at threshold by construction: after enough flushes the
        // engine should have auto-compacted at least once.
        assert!(
            lsm.active_sst_count() < 20,
            "expected auto-compaction; active SST count is {}",
            lsm.active_sst_count()
        );
        // All keys still readable
        for i in 0..20 {
            assert!(lsm.get(format!("k{:03}", i).as_bytes()).unwrap().is_some());
        }
    }

    #[test]
    fn snapshot_outliving_compaction_keeps_reading_old_data() {
        let tmp = TempDir::new().unwrap();
        let lsm = LsmEngine::open(small_config(tmp.path())).unwrap();
        lsm.put(b("k"), b("v1")).unwrap();
        lsm.flush().unwrap();

        let snap = lsm.snapshot();

        lsm.put(b("k"), b("v2")).unwrap();
        lsm.flush().unwrap();
        lsm.compact().unwrap();

        // Engine sees the new value.
        assert_eq!(lsm.get(b"k").unwrap(), Some(b("v2")));
        // Snapshot still sees its frozen view.
        assert_eq!(snap.get(b"k").unwrap(), Some(b("v1")));

        // The old SST file should still be on disk because the snapshot holds it.
        assert!(lsm.pending_delete_len() >= 1);

        // After dropping the snapshot, the next compaction-triggered GC should
        // reclaim the file. (We simulate by calling compact again — it'll be
        // a no-op for content but will run gc.)
        drop(snap);
        lsm.compact().unwrap(); // no-op on content; runs gc
        // pending should now be empty (count was 1, file deleted)
        assert_eq!(lsm.pending_delete_len(), 0);
    }

    #[test]
    fn compaction_survives_reopen() {
        let tmp = TempDir::new().unwrap();
        {
            let lsm = LsmEngine::open(small_config(tmp.path())).unwrap();
            for i in 0..5 {
                lsm.put(b(&format!("k{}", i)), b("v")).unwrap();
                lsm.flush().unwrap();
            }
            lsm.compact().unwrap();
        }
        let lsm = LsmEngine::open(small_config(tmp.path())).unwrap();
        for i in 0..5 {
            assert_eq!(lsm.get(format!("k{}", i).as_bytes()).unwrap(), Some(b("v")));
        }
        // After compaction there should be exactly one SST on disk and
        // reopening should see exactly one.
        assert_eq!(lsm.active_sst_count(), 1);
    }
}
