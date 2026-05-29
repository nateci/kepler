//! MVCC layer on top of [`kepler_storage::Engine`].
//!
//! Logical model:
//!   - Every write to user key `K` at timestamp `T` is stored under encoded
//!     key `enc(K, T)`, where `T` sorts newest-first within `K`.
//!   - Reads at timestamp `T_read` scan forward from `enc(K, T_read)` and
//!     return the first version `<= T_read`.
//!   - Transactions accumulate reads + writes in memory; commit assigns a
//!     commit timestamp and atomically writes the batch + a transaction
//!     status record (Committed / Aborted).
//!
//! Skeleton only — the encoding, conflict detection, and write-pipeline are TODO.

pub mod clock;
pub mod store;
pub mod txn;

pub use clock::HybridLogicalClock;
pub use store::MvccStore;
pub use txn::{Txn, TxnId};
