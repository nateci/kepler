use std::collections::HashSet;

use kepler_types::{Key, Timestamp, Value};

pub type TxnId = u64;

#[derive(Debug)]
pub struct Txn {
    pub id: TxnId,
    /// Timestamp at which this transaction reads.
    pub read_ts: Timestamp,
    /// Pending writes. `None` value = delete.
    pub writes: Vec<(Key, Option<Value>)>,
    /// Keys read — used for SSI conflict detection.
    pub reads: HashSet<Key>,
}

impl Txn {
    pub fn new(id: TxnId, read_ts: Timestamp) -> Self {
        Self { id, read_ts, writes: Vec::new(), reads: HashSet::new() }
    }
}
