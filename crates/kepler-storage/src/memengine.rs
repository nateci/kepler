//! `BTreeMap`-backed in-memory `Engine`. Not durable, not optimized — useful
//! for testing higher layers (Raft, MVCC, server) before the LSM lands.

use std::collections::BTreeMap;
use std::sync::Arc;

use parking_lot::RwLock;

use kepler_types::{Key, KeyRange, Result, Value};

use crate::engine::{Batch, BatchOp, Cursor, Engine, Snapshot};

#[derive(Default, Clone)]
pub struct MemEngine {
    inner: Arc<RwLock<BTreeMap<Key, Value>>>,
}

impl MemEngine {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Engine for MemEngine {
    fn get(&self, key: &[u8]) -> Result<Option<Value>> {
        Ok(self.inner.read().get(key).cloned())
    }

    fn put(&self, key: Key, value: Value) -> Result<()> {
        self.inner.write().insert(key, value);
        Ok(())
    }

    fn delete(&self, key: &[u8]) -> Result<()> {
        self.inner.write().remove(key);
        Ok(())
    }

    fn write_batch(&self, batch: Batch) -> Result<()> {
        let mut guard = self.inner.write();
        for op in batch.ops {
            match op {
                BatchOp::Put { key, value } => {
                    guard.insert(key, value);
                }
                BatchOp::Delete { key } => {
                    guard.remove(&key);
                }
            }
        }
        Ok(())
    }

    fn snapshot(&self) -> Box<dyn Snapshot> {
        Box::new(MemSnapshot { data: self.inner.read().clone() })
    }

    fn scan(&self, range: KeyRange) -> Box<dyn Cursor> {
        let items: Vec<(Key, Value)> = self
            .inner
            .read()
            .range(range.start.clone()..range.end.clone())
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        Box::new(MemCursor { items, pos: 0 })
    }

    fn flush(&self) -> Result<()> {
        Ok(())
    }
}

struct MemSnapshot {
    data: BTreeMap<Key, Value>,
}

impl Snapshot for MemSnapshot {
    fn get(&self, key: &[u8]) -> Result<Option<Value>> {
        Ok(self.data.get(key).cloned())
    }
}

struct MemCursor {
    items: Vec<(Key, Value)>,
    pos: usize,
}

impl Cursor for MemCursor {
    fn seek(&mut self, target: &[u8]) {
        self.pos = self
            .items
            .partition_point(|(k, _)| k.as_ref() < target);
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

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn b(s: &str) -> Bytes {
        Bytes::copy_from_slice(s.as_bytes())
    }

    #[test]
    fn put_get_roundtrip() {
        let e = MemEngine::new();
        e.put(b("k"), b("v")).unwrap();
        assert_eq!(e.get(b"k").unwrap(), Some(b("v")));
    }

    #[test]
    fn delete_removes_key() {
        let e = MemEngine::new();
        e.put(b("k"), b("v")).unwrap();
        e.delete(b"k").unwrap();
        assert_eq!(e.get(b"k").unwrap(), None);
    }

    #[test]
    fn batch_writes_atomically_in_order() {
        let e = MemEngine::new();
        let mut batch = Batch::new();
        batch.put(b("a"), b("1")).put(b("b"), b("2")).delete(b("a"));
        e.write_batch(batch).unwrap();
        assert_eq!(e.get(b"a").unwrap(), None);
        assert_eq!(e.get(b"b").unwrap(), Some(b("2")));
    }

    #[test]
    fn snapshot_is_isolated_from_later_writes() {
        let e = MemEngine::new();
        e.put(b("k"), b("v1")).unwrap();
        let snap = e.snapshot();
        e.put(b("k"), b("v2")).unwrap();
        assert_eq!(snap.get(b"k").unwrap(), Some(b("v1")));
        assert_eq!(e.get(b"k").unwrap(), Some(b("v2")));
    }

    #[test]
    fn scan_returns_keys_in_range() {
        let e = MemEngine::new();
        for k in ["a", "b", "c", "d"] {
            e.put(b(k), b("x")).unwrap();
        }
        let mut cur = e.scan(KeyRange::new(b("b"), b("d")));
        let mut keys = Vec::new();
        while let Some((k, _)) = cur.next() {
            keys.push(k);
        }
        assert_eq!(keys, vec![b("b"), b("c")]);
    }
}
