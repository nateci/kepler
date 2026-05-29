//! Storage layer: pluggable `Engine` trait, `Wal` trait, and concrete
//! implementations.
//!
//! The `Engine` trait is the abstraction that the Raft state machine writes
//! against. `MemEngine` is a `BTreeMap`-backed implementation useful for
//! testing higher layers without the real LSM. The real LSM tree lives in
//! [`lsm`] (TODO).

pub mod engine;
pub mod memengine;
pub mod wal;
pub mod lsm;

pub use engine::{Batch, Cursor, Engine, Snapshot};
pub use lsm::{LsmConfig, LsmEngine};
pub use memengine::MemEngine;
pub use wal::{DiskWal, MemWal, Wal, WalConfig, WalEntry, WalEntryKind, WalOffset};
