use kepler_types::{Key, KeyRange, Result, Value};

/// A pluggable KV storage engine. Implementations include [`super::MemEngine`]
/// for tests and the LSM tree in [`super::lsm`] for production.
///
/// All methods are synchronous — the engine performs its own background work
/// (compaction, flushing) and any internal IO is hidden behind these calls.
pub trait Engine: Send + Sync + 'static {
    fn get(&self, key: &[u8]) -> Result<Option<Value>>;

    fn put(&self, key: Key, value: Value) -> Result<()>;

    fn delete(&self, key: &[u8]) -> Result<()>;

    /// Apply a batch of writes atomically.
    fn write_batch(&self, batch: Batch) -> Result<()>;

    /// Take a point-in-time read view of the engine. Reads through the snapshot
    /// see a consistent state even as the engine is mutated.
    fn snapshot(&self) -> Box<dyn Snapshot>;

    /// Iterate keys in `[range.start, range.end)`.
    fn scan(&self, range: KeyRange) -> Box<dyn Cursor>;

    /// Force any in-memory state to durable storage (e.g. flush memtable).
    fn flush(&self) -> Result<()>;
}

/// Point-in-time read view.
pub trait Snapshot: Send + Sync {
    fn get(&self, key: &[u8]) -> Result<Option<Value>>;
}

/// Forward-only iterator over engine keys.
pub trait Cursor: Send {
    /// Position the cursor at the first key `>= target`.
    fn seek(&mut self, target: &[u8]);

    /// Advance to the next key-value pair, returning `None` at end-of-range.
    fn next(&mut self) -> Option<(Key, Value)>;

    fn valid(&self) -> bool;
}

/// Atomic batch of writes. Pass to [`Engine::write_batch`].
#[derive(Default, Debug)]
pub struct Batch {
    pub(crate) ops: Vec<BatchOp>,
}

#[derive(Debug)]
pub(crate) enum BatchOp {
    Put { key: Key, value: Value },
    Delete { key: Key },
}

impl Batch {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn put(&mut self, key: Key, value: Value) -> &mut Self {
        self.ops.push(BatchOp::Put { key, value });
        self
    }

    pub fn delete(&mut self, key: Key) -> &mut Self {
        self.ops.push(BatchOp::Delete { key });
        self
    }

    pub fn len(&self) -> usize {
        self.ops.len()
    }

    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }
}
