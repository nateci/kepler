//! In-memory sorted KV buffer that absorbs writes before they hit the LSM.
//!
//! Each entry tracks the `seq` at which it was written so that out-of-order
//! WAL replay (two concurrent writers that hit the WAL in opposite order)
//! still resolves to the newest write per key. `None` value = tombstone.

use std::collections::BTreeMap;
use std::collections::btree_map::Entry as BTreeEntry;

use bytes::Bytes;
use kepler_types::{Key, Value};

#[derive(Default, Clone)]
pub struct MemTable {
    data: BTreeMap<Key, (u64, Option<Value>)>,
    /// Cheap approximate accounting for "is this memtable big enough to flush".
    /// Per-entry overhead of 16 bytes is a hand-wave for BTreeMap node cost.
    size: usize,
    max_seq: Option<u64>,
}

const ENTRY_OVERHEAD: usize = 16;

impl MemTable {
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert `(key, value)` written at `seq`. If an entry with `seq' >= seq`
    /// already exists for this key, the call is a no-op — concurrent writers
    /// that hit the WAL out of seq order resolve correctly on replay.
    pub fn insert(&mut self, seq: u64, key: Key, value: Option<Value>) {
        let key_size = key.len();
        let new_val_size = value.as_ref().map(|v| v.len()).unwrap_or(0);

        match self.data.entry(key) {
            BTreeEntry::Occupied(mut e) => {
                if e.get().0 >= seq {
                    return; // older write, ignore
                }
                let old_val_size = e.get().1.as_ref().map(|v| v.len()).unwrap_or(0);
                self.size = self
                    .size
                    .saturating_sub(key_size + old_val_size + ENTRY_OVERHEAD);
                e.insert((seq, value));
                self.size += key_size + new_val_size + ENTRY_OVERHEAD;
            }
            BTreeEntry::Vacant(e) => {
                e.insert((seq, value));
                self.size += key_size + new_val_size + ENTRY_OVERHEAD;
            }
        }

        self.max_seq = Some(self.max_seq.map_or(seq, |s| s.max(seq)));
    }

    /// Look up `key`. Returns:
    /// - `None`: key not present in this memtable
    /// - `Some(Some(v))`: key has value `v`
    /// - `Some(None)`: key has a tombstone here
    pub fn get(&self, key: &[u8]) -> Option<Option<Value>> {
        self.data.get(key).map(|(_seq, v)| v.clone())
    }

    /// Iterate entries in key order. Used by the flush path.
    pub fn iter(&self) -> impl Iterator<Item = (&Key, &Option<Value>)> {
        self.data.iter().map(|(k, (_s, v))| (k, v))
    }

    /// Collect entries with `start <= key < end`.
    pub fn range(&self, start: &[u8], end: &[u8]) -> Vec<(Key, Option<Value>)> {
        let s = Bytes::copy_from_slice(start);
        let e = Bytes::copy_from_slice(end);
        self.data
            .range(s..e)
            .map(|(k, (_seq, v))| (k.clone(), v.clone()))
            .collect()
    }

    pub fn size(&self) -> usize {
        self.size
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    pub fn max_seq(&self) -> Option<u64> {
        self.max_seq
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn b(s: &str) -> Bytes {
        Bytes::copy_from_slice(s.as_bytes())
    }

    #[test]
    fn insert_and_get() {
        let mut mt = MemTable::new();
        mt.insert(1, b("k"), Some(b("v")));
        assert_eq!(mt.get(b"k"), Some(Some(b("v"))));
        assert_eq!(mt.get(b"missing"), None);
    }

    #[test]
    fn newer_seq_overwrites_older() {
        let mut mt = MemTable::new();
        mt.insert(1, b("k"), Some(b("v1")));
        mt.insert(5, b("k"), Some(b("v2")));
        assert_eq!(mt.get(b"k"), Some(Some(b("v2"))));
        assert_eq!(mt.max_seq(), Some(5));
    }

    #[test]
    fn older_seq_is_ignored() {
        let mut mt = MemTable::new();
        mt.insert(5, b("k"), Some(b("v2")));
        mt.insert(1, b("k"), Some(b("v1")));
        assert_eq!(mt.get(b"k"), Some(Some(b("v2"))));
    }

    #[test]
    fn tombstone_overrides_value() {
        let mut mt = MemTable::new();
        mt.insert(1, b("k"), Some(b("v")));
        mt.insert(2, b("k"), None);
        assert_eq!(mt.get(b"k"), Some(None));
    }

    #[test]
    fn iter_is_sorted() {
        let mut mt = MemTable::new();
        for (i, k) in ["c", "a", "b"].iter().enumerate() {
            mt.insert(i as u64 + 1, b(k), Some(b("v")));
        }
        let keys: Vec<_> = mt.iter().map(|(k, _)| k.clone()).collect();
        assert_eq!(keys, vec![b("a"), b("b"), b("c")]);
    }

    #[test]
    fn range_returns_inclusive_start_exclusive_end() {
        let mut mt = MemTable::new();
        for (i, k) in ["a", "b", "c", "d"].iter().enumerate() {
            mt.insert(i as u64 + 1, b(k), Some(b("x")));
        }
        let r = mt.range(b"b", b"d");
        let keys: Vec<_> = r.into_iter().map(|(k, _)| k).collect();
        assert_eq!(keys, vec![b("b"), b("c")]);
    }
}
