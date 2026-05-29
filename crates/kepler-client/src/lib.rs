//! Kepler client library. Discovers the current Raft leader, caches it, and
//! transparently retries on `NotLeader` redirects.
//!
//! Skeleton — to be fleshed out once the gRPC server is real.

use std::sync::atomic::{AtomicU64, Ordering};

use kepler_types::{Key, NodeId, Result, Value};

pub struct Client {
    endpoints: Vec<String>,
    leader_hint: AtomicU64,
}

impl Client {
    /// Build a client. Does no network IO; first request resolves the leader.
    pub fn new(endpoints: Vec<String>) -> Self {
        Self { endpoints, leader_hint: AtomicU64::new(0) }
    }

    pub fn endpoints(&self) -> &[String] {
        &self.endpoints
    }

    pub fn cached_leader(&self) -> Option<NodeId> {
        match self.leader_hint.load(Ordering::Relaxed) {
            0 => None,
            id => Some(id),
        }
    }

    pub async fn get(&self, _key: &[u8]) -> Result<Option<Value>> {
        todo!("dial leader, send GetRequest, follow NotLeader hints")
    }

    pub async fn put(&self, _key: Key, _value: Value) -> Result<()> {
        todo!()
    }

    pub async fn delete(&self, _key: &[u8]) -> Result<()> {
        todo!()
    }
}
