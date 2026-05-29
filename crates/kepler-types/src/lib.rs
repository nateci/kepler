//! Shared types and the top-level `Error` enum used across the workspace.
//!
//! Per-crate errors (e.g. inside `kepler-storage`) collapse into
//! [`Error::Storage`] / [`Error::Raft`] / etc via string conversion at crate
//! boundaries to avoid circular crate dependencies.

use bytes::Bytes;

pub type NodeId = u64;
pub type Term = u64;
pub type LogIndex = u64;

/// Hybrid Logical Clock timestamp: high 48 bits physical (ms), low 16 bits logical.
pub type Timestamp = u64;

pub type Key = Bytes;
pub type Value = Bytes;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("not leader (hint: {0:?})")]
    NotLeader(Option<NodeId>),

    #[error("key not found")]
    NotFound,

    #[error("transaction conflict")]
    TxnConflict,

    #[error("io: {0}")]
    Io(#[from] std::io::Error),

    #[error("storage: {0}")]
    Storage(String),

    #[error("raft: {0}")]
    Raft(String),

    #[error("network: {0}")]
    Network(String),

    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    #[error("internal: {0}")]
    Internal(String),
}

pub type Result<T> = std::result::Result<T, Error>;

/// Half-open key range `[start, end)`.
#[derive(Clone, Debug)]
pub struct KeyRange {
    pub start: Key,
    pub end: Key,
}

impl KeyRange {
    pub fn new(start: impl Into<Key>, end: impl Into<Key>) -> Self {
        Self { start: start.into(), end: end.into() }
    }

    pub fn contains(&self, key: &[u8]) -> bool {
        key >= self.start.as_ref() && key < self.end.as_ref()
    }
}
