//! `KvStateMachine` — bridges Raft commits to a `kepler_storage::Engine`.
//!
//! Every committed entry carries an opaque blob (`Entry::data`) which we
//! interpret as a KV command. The encoding is intentionally simple for v0:
//!
//! ```text
//!   ┌─────────┬───────────┬─────────┬──────────────┐
//!   │ op u8   │ keylen u32│  key    │  value       │
//!   │         │   (LE)    │  bytes  │  bytes (only │
//!   │         │           │         │   if op=Put) │
//!   └─────────┴───────────┴─────────┴──────────────┘
//! ```
//!
//! `op = 0` → Put (value follows the key, length = rest-of-payload).
//! `op = 1` → Delete (no value).
//!
//! Snapshots aren't implemented in v0 (would need a way to snapshot the
//! underlying `Engine`, which `LsmEngine` does via SST file lists).

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;

use kepler_raft::{Entry, EntryKind, StateMachine};
use kepler_storage::Engine;
use kepler_types::{Error, LogIndex, Result};

/// Opaque KV command encoded into a Raft entry's payload.
#[derive(Debug, Clone)]
pub enum Command {
    Put { key: Bytes, value: Bytes },
    Delete { key: Bytes },
}

impl Command {
    pub fn encode(&self) -> Bytes {
        match self {
            Command::Put { key, value } => encode_put(key, value),
            Command::Delete { key } => encode_delete(key),
        }
    }

    pub fn decode(data: &[u8]) -> Result<Self> {
        if data.is_empty() {
            return Err(Error::InvalidArgument("empty kv command".into()));
        }
        let op = data[0];
        let rest = &data[1..];
        if rest.len() < 4 {
            return Err(Error::InvalidArgument("kv command: truncated keylen".into()));
        }
        let keylen = u32::from_le_bytes(rest[..4].try_into().unwrap()) as usize;
        if rest.len() < 4 + keylen {
            return Err(Error::InvalidArgument("kv command: truncated key".into()));
        }
        let key = Bytes::copy_from_slice(&rest[4..4 + keylen]);
        match op {
            0 => {
                let value = Bytes::copy_from_slice(&rest[4 + keylen..]);
                Ok(Command::Put { key, value })
            }
            1 => Ok(Command::Delete { key }),
            other => Err(Error::InvalidArgument(format!("unknown kv op {}", other))),
        }
    }
}

pub fn encode_put(key: &[u8], value: &[u8]) -> Bytes {
    let mut buf = Vec::with_capacity(1 + 4 + key.len() + value.len());
    buf.push(0u8);
    buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
    buf.extend_from_slice(key);
    buf.extend_from_slice(value);
    Bytes::from(buf)
}

pub fn encode_delete(key: &[u8]) -> Bytes {
    let mut buf = Vec::with_capacity(1 + 4 + key.len());
    buf.push(1u8);
    buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
    buf.extend_from_slice(key);
    Bytes::from(buf)
}

pub struct KvStateMachine<E: Engine> {
    engine: Arc<E>,
    applied: AtomicU64,
}

impl<E: Engine> KvStateMachine<E> {
    pub fn new(engine: Arc<E>) -> Self {
        Self { engine, applied: AtomicU64::new(0) }
    }

    pub fn engine(&self) -> &Arc<E> {
        &self.engine
    }
}

impl<E: Engine> StateMachine for KvStateMachine<E> {
    fn apply(&self, entry: &Entry) -> Result<Bytes> {
        match entry.kind {
            EntryKind::Normal => {
                if !entry.data.is_empty() {
                    match Command::decode(&entry.data)? {
                        Command::Put { key, value } => self.engine.put(key, value)?,
                        Command::Delete { key } => self.engine.delete(&key)?,
                    }
                }
                // Empty data is allowed — leader no-op entries land here.
            }
            EntryKind::ConfChange => {
                // Membership changes: not implemented in v0.
            }
        }
        // applied is monotonic; never go backwards on stale re-apply.
        let mut cur = self.applied.load(Ordering::Acquire);
        while entry.index > cur {
            match self.applied.compare_exchange_weak(
                cur,
                entry.index,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(actual) => cur = actual,
            }
        }
        Ok(Bytes::new())
    }

    fn snapshot(&self) -> Result<Bytes> {
        Err(Error::Storage("KvStateMachine snapshot not implemented".into()))
    }

    fn restore(&self, _snapshot: Bytes) -> Result<()> {
        Err(Error::Storage("KvStateMachine restore not implemented".into()))
    }

    fn applied_index(&self) -> LogIndex {
        self.applied.load(Ordering::Acquire)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kepler_storage::MemEngine;

    fn make_sm() -> (KvStateMachine<MemEngine>, Arc<MemEngine>) {
        let engine = Arc::new(MemEngine::new());
        let sm = KvStateMachine::new(engine.clone());
        (sm, engine)
    }

    fn entry(index: u64, data: Bytes) -> Entry {
        Entry { index, term: 1, kind: EntryKind::Normal, data }
    }

    #[test]
    fn command_put_roundtrip() {
        let encoded = encode_put(b"k", b"v");
        match Command::decode(&encoded).unwrap() {
            Command::Put { key, value } => {
                assert_eq!(key.as_ref(), b"k");
                assert_eq!(value.as_ref(), b"v");
            }
            other => panic!("expected Put, got {:?}", other),
        }
    }

    #[test]
    fn command_delete_roundtrip() {
        let encoded = encode_delete(b"k");
        match Command::decode(&encoded).unwrap() {
            Command::Delete { key } => assert_eq!(key.as_ref(), b"k"),
            other => panic!("expected Delete, got {:?}", other),
        }
    }

    #[test]
    fn apply_put_writes_to_engine() {
        let (sm, engine) = make_sm();
        sm.apply(&entry(1, encode_put(b"k", b"v"))).unwrap();
        assert_eq!(engine.get(b"k").unwrap().as_deref(), Some(&b"v"[..]));
        assert_eq!(sm.applied_index(), 1);
    }

    #[test]
    fn apply_delete_removes_from_engine() {
        let (sm, engine) = make_sm();
        sm.apply(&entry(1, encode_put(b"k", b"v"))).unwrap();
        sm.apply(&entry(2, encode_delete(b"k"))).unwrap();
        assert!(engine.get(b"k").unwrap().is_none());
        assert_eq!(sm.applied_index(), 2);
    }

    #[test]
    fn empty_data_is_a_noop_but_advances_applied() {
        let (sm, _engine) = make_sm();
        sm.apply(&entry(7, Bytes::new())).unwrap();
        assert_eq!(sm.applied_index(), 7);
    }

    #[test]
    fn applied_index_is_monotonic() {
        let (sm, _engine) = make_sm();
        sm.apply(&entry(5, encode_put(b"a", b"x"))).unwrap();
        sm.apply(&entry(3, encode_put(b"b", b"y"))).unwrap(); // out-of-order
        assert_eq!(sm.applied_index(), 5);
    }

    #[test]
    fn rejects_malformed_command() {
        let (sm, _engine) = make_sm();
        let bad = Bytes::from_static(&[42u8]); // unknown op + truncated
        assert!(sm.apply(&entry(1, bad)).is_err());
    }
}
