use bytes::Bytes;

use kepler_types::{LogIndex, Result};

use crate::types::Entry;

/// The application state machine that Raft replicates against.
///
/// Implementors decode `entry.data` into application-level commands
/// (e.g. KV `Put` / `Delete` / `TxnCommit`) and apply them. The returned
/// bytes are routed back to the client that proposed the command.
pub trait StateMachine: Send + Sync {
    fn apply(&self, entry: &Entry) -> Result<Bytes>;

    /// Snapshot the state machine for log compaction.
    fn snapshot(&self) -> Result<Bytes>;

    /// Restore from a snapshot installed by Raft.
    fn restore(&self, snapshot: Bytes) -> Result<()>;

    /// Last entry index this state machine has applied. Used by Raft to know
    /// when it's safe to compact the log.
    fn applied_index(&self) -> LogIndex;
}
