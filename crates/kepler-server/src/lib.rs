//! Server-side machinery:
//!   - `state_machine` — `KvStateMachine` bridges Raft commits → KV engine writes
//!   - `cluster` — deterministic multi-node test harness
//!   - `transport` — Raft transport trait + `SimTransport` for async/gRPC future

pub mod cluster;
pub mod state_machine;
pub mod transport;

pub use cluster::Cluster;
pub use state_machine::{encode_delete, encode_put, Command, KvStateMachine};
pub use transport::{SimTransport, Transport};
