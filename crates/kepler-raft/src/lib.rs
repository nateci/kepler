//! Raft consensus, pure-logic style (no IO).
//!
//! Follows the etcd-style `Node + Ready` pattern: this module decides *what*
//! to do (persist these entries, send these messages, apply these committed
//! entries). The driver in `kepler-server` does the IO and feeds messages
//! back in via [`Node::step`].
//!
//! Read sections 5 and 6 of the Raft paper (Ongaro & Ousterhout, 2014) before
//! implementing. Section 5.4 (safety) deserves three reads.

pub mod node;
pub mod storage;
pub mod state_machine;
pub mod types;

pub use node::{Config, Node, Ready};
pub use state_machine::StateMachine;
pub use storage::RaftStorage;
pub use types::{ConfState, Entry, HardState, Message, MessageBody, Snapshot};
