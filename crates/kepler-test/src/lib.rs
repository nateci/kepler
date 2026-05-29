//! Integration test + fault-injection harness.
//!
//! This crate will host:
//!   - `sim::Cluster`: spawn N nodes wired through `SimTransport`, with knobs
//!     for partitioning, message delay, message drop, and clock skew
//!   - `linearizability`: a checker (or wrapper around `porcupine`) that
//!     verifies recorded client histories satisfy linearizability
//!   - Workload generators: YCSB-style A/B/C/D/F mixes
//!
//! Empty until Phase 3 (multi-node Raft) is ready to test.
