//! In-memory `RaftStorage` implementation.
//!
//! Useful for tests and bootstrapping. Cloning a `MemRaftStorage` produces a
//! shared handle (Arc-backed), so two `Node`s can be constructed against the
//! same underlying state to test restart / persistence semantics.
//!
//! v0 does not implement snapshots; `snapshot()` and `apply_snapshot()` return
//! an error.

use std::sync::Arc;

use parking_lot::Mutex;

use kepler_types::{Error, LogIndex, Result, Term};

use crate::storage::RaftStorage;
use crate::types::{ConfState, Entry, HardState, Snapshot};

#[derive(Default, Clone)]
pub struct MemRaftStorage {
    inner: Arc<Mutex<Inner>>,
}

#[derive(Default)]
struct Inner {
    /// Log entries. `entries[i]` has index `i + 1` (Raft logs are 1-indexed).
    entries: Vec<Entry>,
    hard_state: HardState,
    conf_state: ConfState,
}

impl MemRaftStorage {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_conf(conf: ConfState) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Inner {
                entries: Vec::new(),
                hard_state: HardState::default(),
                conf_state: conf,
            })),
        }
    }
}

impl RaftStorage for MemRaftStorage {
    fn initial_state(&self) -> Result<(HardState, ConfState)> {
        let inner = self.inner.lock();
        Ok((inner.hard_state.clone(), inner.conf_state.clone()))
    }

    fn entries(&self, low: LogIndex, high: LogIndex) -> Result<Vec<Entry>> {
        if low == 0 {
            return Err(Error::Raft("entries: low index 0 is invalid".into()));
        }
        let inner = self.inner.lock();
        let lo = ((low - 1) as usize).min(inner.entries.len());
        let hi = ((high.saturating_sub(1)) as usize).min(inner.entries.len());
        if lo >= hi {
            return Ok(Vec::new());
        }
        Ok(inner.entries[lo..hi].to_vec())
    }

    fn term(&self, idx: LogIndex) -> Result<Term> {
        if idx == 0 {
            return Ok(0);
        }
        let inner = self.inner.lock();
        let offset = (idx - 1) as usize;
        if offset >= inner.entries.len() {
            return Err(Error::Raft(format!("term: index {} out of range", idx)));
        }
        Ok(inner.entries[offset].term)
    }

    fn first_index(&self) -> Result<LogIndex> {
        Ok(1)
    }

    fn last_index(&self) -> Result<LogIndex> {
        Ok(self.inner.lock().entries.len() as LogIndex)
    }

    fn snapshot(&self) -> Result<Snapshot> {
        Err(Error::Raft("snapshots not implemented in MemRaftStorage".into()))
    }

    fn append(&self, entries: &[Entry]) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        let mut inner = self.inner.lock();
        let first_idx = entries[0].index;
        if first_idx == 0 {
            return Err(Error::Raft("append: index 0 is invalid".into()));
        }
        let offset = (first_idx - 1) as usize;
        if offset > inner.entries.len() {
            return Err(Error::Raft(format!(
                "append: gap (current last {}, appending first {})",
                inner.entries.len(),
                first_idx
            )));
        }
        // Truncate any conflicting entries (`offset == len` means a clean append).
        inner.entries.truncate(offset);
        inner.entries.extend_from_slice(entries);
        Ok(())
    }

    fn save_hard_state(&self, hs: &HardState) -> Result<()> {
        self.inner.lock().hard_state = hs.clone();
        Ok(())
    }

    fn apply_snapshot(&self, _snap: Snapshot) -> Result<()> {
        Err(Error::Raft("snapshots not implemented in MemRaftStorage".into()))
    }

    fn compact(&self, _idx: LogIndex) -> Result<()> {
        // No-op for v0 (no snapshotting yet).
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::EntryKind;
    use bytes::Bytes;

    fn entry(index: LogIndex, term: Term, data: &[u8]) -> Entry {
        Entry {
            index,
            term,
            kind: EntryKind::Normal,
            data: Bytes::copy_from_slice(data),
        }
    }

    #[test]
    fn append_and_read_back() {
        let s = MemRaftStorage::new();
        s.append(&[entry(1, 1, b"a"), entry(2, 1, b"b")]).unwrap();
        let got = s.entries(1, 3).unwrap();
        assert_eq!(got.len(), 2);
        assert_eq!(got[0].data.as_ref(), b"a");
        assert_eq!(s.last_index().unwrap(), 2);
        assert_eq!(s.term(2).unwrap(), 1);
    }

    #[test]
    fn append_truncates_conflicting_tail() {
        let s = MemRaftStorage::new();
        s.append(&[entry(1, 1, b"a"), entry(2, 1, b"b"), entry(3, 1, b"c")])
            .unwrap();
        // Overwrite from index 2 with a different entry at term 2.
        s.append(&[entry(2, 2, b"B")]).unwrap();
        assert_eq!(s.last_index().unwrap(), 2);
        assert_eq!(s.term(2).unwrap(), 2);
        assert_eq!(s.entries(1, 3).unwrap()[1].data.as_ref(), b"B");
    }

    #[test]
    fn append_rejects_gaps() {
        let s = MemRaftStorage::new();
        s.append(&[entry(1, 1, b"a")]).unwrap();
        let err = s.append(&[entry(3, 1, b"c")]);
        assert!(err.is_err());
    }

    #[test]
    fn hard_state_roundtrip() {
        let s = MemRaftStorage::new();
        let hs = HardState { term: 5, vote: Some(2), commit: 3 };
        s.save_hard_state(&hs).unwrap();
        let (restored, _) = s.initial_state().unwrap();
        assert_eq!(restored.term, 5);
        assert_eq!(restored.vote, Some(2));
        assert_eq!(restored.commit, 3);
    }

    #[test]
    fn clone_shares_state() {
        let a = MemRaftStorage::new();
        let b = a.clone();
        a.append(&[entry(1, 1, b"x")]).unwrap();
        assert_eq!(b.last_index().unwrap(), 1);
    }
}
