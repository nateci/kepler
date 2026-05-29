# kepler

A distributed, linearizable key-value store in Rust. From-scratch LSM-tree storage engine, from-scratch Raft consensus, MVCC transactions.

## Status

Scaffold. Trait boundaries defined, `MemEngine` and `SimTransport` working for higher-layer testing. Real LSM, Raft, MVCC, and gRPC server are stubs.

## Layout

| Crate | Purpose |
|---|---|
| `kepler-types` | Shared types: `NodeId`, `Term`, `LogIndex`, `Timestamp`, `Error` |
| `kepler-proto` | Wire types (eventually tonic-generated; stub structs for now) |
| `kepler-storage` | `Engine` trait, `Wal` trait, `MemEngine`, LSM implementation (TODO) |
| `kepler-raft` | `Node`, `RaftStorage`, `StateMachine` traits + Raft skeleton (TODO) |
| `kepler-mvcc` | `MvccStore`, `Txn`, `HybridLogicalClock` |
| `kepler-server` | `Transport` trait, `SimTransport`, gRPC server binary (TODO) |
| `kepler-client` | Client library (TODO) |
| `kepler-test` | Fault injection + linearizability harness (TODO) |

## Build

```sh
cargo check --workspace
cargo test --workspace
```

## Roadmap

- **Phase 0** — Scaffold + traits + `MemEngine`
- **Phase 1** — Real LSM storage engine (WAL, MemTable, SSTable, compaction)
- **Phase 2** — Single-node Raft on top of storage
- **Phase 3** — Multi-node Raft over gRPC, fault tolerance
- **Phase 4** — MVCC + transactions
- **Phase 5** — Benchmarks + Jepsen-style chaos tests + writeup
- **Phase 5.5** — Sharding (stretch)
