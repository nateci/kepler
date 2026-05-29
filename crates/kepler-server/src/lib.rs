//! Server-side machinery: the `Transport` trait that abstracts inter-node
//! Raft RPC, plus a `SimTransport` for tests and a `GrpcTransport` stub for
//! production.
//!
//! The state machine that bridges Raft commits → KV engine writes also lives
//! here (TODO).

pub mod transport;

pub use transport::{SimTransport, Transport};
