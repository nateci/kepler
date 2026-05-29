//! Wire types for the Kv and Raft RPC surfaces.
//!
//! These are hand-written stubs so the rest of the workspace can compile without
//! pulling in `tonic` / `prost` yet. Replace with `tonic-build`-generated code
//! when the gRPC layer comes online; the struct shapes match the planned
//! protobuf definitions in `proto/kepler.proto` (TODO: add).

use bytes::Bytes;
use kepler_types::{LogIndex, NodeId, Term};

// -- KV service ------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct GetRequest {
    pub key: Bytes,
}

#[derive(Debug, Clone)]
pub struct GetResponse {
    pub value: Option<Bytes>,
    pub not_leader: Option<NotLeaderHint>,
}

#[derive(Debug, Clone)]
pub struct PutRequest {
    pub key: Bytes,
    pub value: Bytes,
}

#[derive(Debug, Clone, Default)]
pub struct PutResponse {
    pub not_leader: Option<NotLeaderHint>,
}

#[derive(Debug, Clone)]
pub struct DeleteRequest {
    pub key: Bytes,
}

#[derive(Debug, Clone, Default)]
pub struct DeleteResponse {
    pub not_leader: Option<NotLeaderHint>,
}

#[derive(Debug, Clone)]
pub struct ScanRequest {
    pub start: Bytes,
    pub end: Bytes,
    pub limit: u32,
}

#[derive(Debug, Clone)]
pub struct ScanItem {
    pub key: Bytes,
    pub value: Bytes,
}

#[derive(Debug, Clone, Default)]
pub struct NotLeaderHint {
    pub leader_id: NodeId,
}

// -- Raft service ----------------------------------------------------------

#[derive(Debug, Clone)]
pub struct AppendEntriesRequest {
    pub term: Term,
    pub leader_id: NodeId,
    pub prev_log_index: LogIndex,
    pub prev_log_term: Term,
    pub entries: Vec<Entry>,
    pub leader_commit: LogIndex,
}

#[derive(Debug, Clone)]
pub struct AppendEntriesResponse {
    pub term: Term,
    pub success: bool,
    pub match_index: LogIndex,
}

#[derive(Debug, Clone)]
pub struct RequestVoteRequest {
    pub term: Term,
    pub candidate_id: NodeId,
    pub last_log_index: LogIndex,
    pub last_log_term: Term,
}

#[derive(Debug, Clone)]
pub struct RequestVoteResponse {
    pub term: Term,
    pub vote_granted: bool,
}

#[derive(Debug, Clone)]
pub struct InstallSnapshotChunk {
    pub term: Term,
    pub leader_id: NodeId,
    pub last_included_index: LogIndex,
    pub last_included_term: Term,
    pub offset: u64,
    pub data: Bytes,
    pub done: bool,
}

#[derive(Debug, Clone)]
pub struct InstallSnapshotResponse {
    pub term: Term,
}

#[derive(Debug, Clone)]
pub struct Entry {
    pub index: LogIndex,
    pub term: Term,
    pub data: Bytes,
}
