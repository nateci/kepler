use kepler_types::{LogIndex, Result, Term};

use crate::types::{ConfState, Entry, HardState, Snapshot};

/// Durable storage *for the Raft log itself*. Separate from the application
/// state machine's storage (the KV engine). Implementations persist log
/// entries and `HardState` (term + vote + commit).
pub trait RaftStorage: Send + Sync {
    fn initial_state(&self) -> Result<(HardState, ConfState)>;

    fn entries(&self, low: LogIndex, high: LogIndex) -> Result<Vec<Entry>>;

    fn term(&self, idx: LogIndex) -> Result<Term>;

    fn first_index(&self) -> Result<LogIndex>;

    fn last_index(&self) -> Result<LogIndex>;

    fn snapshot(&self) -> Result<Snapshot>;

    fn append(&self, entries: &[Entry]) -> Result<()>;

    fn save_hard_state(&self, hs: &HardState) -> Result<()>;

    fn apply_snapshot(&self, snap: Snapshot) -> Result<()>;

    /// Discard log entries strictly before `idx` (post-snapshot compaction).
    fn compact(&self, idx: LogIndex) -> Result<()>;
}
