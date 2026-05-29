use bytes::Bytes;

use kepler_types::{LogIndex, NodeId, Term};

#[derive(Debug, Clone)]
pub struct Entry {
    pub index: LogIndex,
    pub term: Term,
    pub kind: EntryKind,
    pub data: Bytes,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntryKind {
    /// Application command — `data` is opaque to Raft, decoded by the state
    /// machine.
    Normal,
    /// Configuration change — `data` decodes to a `ConfChange`.
    ConfChange,
}

/// Persistent state that survives crashes. Saved before responding to RPCs.
#[derive(Debug, Clone, Default)]
pub struct HardState {
    pub term: Term,
    pub vote: Option<NodeId>,
    pub commit: LogIndex,
}

/// Cluster membership.
#[derive(Debug, Clone, Default)]
pub struct ConfState {
    pub voters: Vec<NodeId>,
    pub learners: Vec<NodeId>,
}

#[derive(Debug, Clone)]
pub struct Snapshot {
    pub index: LogIndex,
    pub term: Term,
    pub conf_state: ConfState,
    pub data: Bytes,
}

#[derive(Debug, Clone)]
pub struct Message {
    pub from: NodeId,
    pub to: NodeId,
    pub term: Term,
    pub body: MessageBody,
}

#[derive(Debug, Clone)]
pub enum MessageBody {
    AppendEntries {
        prev_log_index: LogIndex,
        prev_log_term: Term,
        entries: Vec<Entry>,
        leader_commit: LogIndex,
    },
    AppendResponse {
        success: bool,
        match_index: LogIndex,
    },
    RequestVote {
        last_log_index: LogIndex,
        last_log_term: Term,
    },
    VoteResponse {
        granted: bool,
    },
    InstallSnapshot {
        snapshot: Snapshot,
    },
    InstallSnapshotResponse,
}
