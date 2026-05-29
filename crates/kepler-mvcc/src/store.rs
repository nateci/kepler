use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use kepler_types::{Error, Key, Result, Timestamp, Value};
use kepler_storage::Engine;

use crate::clock::HybridLogicalClock;
use crate::txn::Txn;

pub struct MvccStore<E: Engine> {
    engine: Arc<E>,
    clock: HybridLogicalClock,
    next_txn_id: AtomicU64,
}

impl<E: Engine> MvccStore<E> {
    pub fn new(engine: Arc<E>) -> Self {
        Self {
            engine,
            clock: HybridLogicalClock::new(),
            next_txn_id: AtomicU64::new(1),
        }
    }

    pub fn engine(&self) -> &Arc<E> {
        &self.engine
    }

    /// Read the latest version of `key` visible at `read_ts`.
    pub fn read_at(&self, key: &[u8], _read_ts: Timestamp) -> Result<Option<Value>> {
        // TODO: decode versioned key prefix, scan for first version <= read_ts.
        // For now, ignore MVCC encoding and read directly.
        self.engine.get(key)
    }

    /// Write `value` for `key` at `commit_ts`. Used by the commit pipeline.
    pub fn write(&self, key: Key, value: Value, _commit_ts: Timestamp) -> Result<()> {
        // TODO: encode `enc(key, commit_ts) -> value`.
        self.engine.put(key, value)
    }

    pub fn begin_txn(&self) -> Txn {
        let id = self.next_txn_id.fetch_add(1, Ordering::Relaxed);
        Txn::new(id, self.clock.now())
    }

    /// Commit a transaction.
    /// Returns the assigned commit timestamp on success, or
    /// [`Error::TxnConflict`] if SSI conflict detection rejects it.
    pub fn commit(&self, txn: Txn) -> Result<Timestamp> {
        // TODO: real SSI:
        //   1) acquire commit_ts from clock
        //   2) check each txn.reads for writes in (read_ts, commit_ts]
        //   3) if clean, atomically apply writes + status record
        let commit_ts = self.clock.now();
        for (key, value) in txn.writes {
            match value {
                Some(v) => self.engine.put(key, v)?,
                None => self.engine.delete(&key)?,
            }
        }
        Ok(commit_ts)
    }

    pub fn abort(&self, _txn: Txn) {
        // TODO: drop intents, no-op for now since intents aren't materialized
    }

    /// Return [`Error::TxnConflict`] — for use by the transaction pipeline.
    pub fn conflict() -> Error {
        Error::TxnConflict
    }
}
