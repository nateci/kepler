//! Inter-node transport. Trait-shaped so we can plug in a real gRPC transport
//! in production and an in-memory simulator in tests (the simulator is where
//! fault injection — partitions, delays, drops — happens).

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use parking_lot::Mutex;
use tokio::sync::mpsc;

use kepler_raft::Message;
use kepler_types::{Error, NodeId, Result};

#[async_trait]
pub trait Transport: Send + Sync {
    async fn send(&self, to: NodeId, msg: Message) -> Result<()>;
    fn local_id(&self) -> NodeId;
}

/// In-memory transport. Every node in the simulated cluster shares an
/// `Arc<Mutex<HashMap<NodeId, Sender>>>` so they can deliver to one another.
/// Failure injection methods (`partition`, `heal`, `drop_rate`) live here.
pub struct SimTransport {
    local: NodeId,
    inboxes: Arc<Mutex<HashMap<NodeId, mpsc::UnboundedSender<Message>>>>,
    partitions: Arc<Mutex<Vec<(NodeId, NodeId)>>>,
}

impl SimTransport {
    pub fn new(
        local: NodeId,
        inboxes: Arc<Mutex<HashMap<NodeId, mpsc::UnboundedSender<Message>>>>,
        partitions: Arc<Mutex<Vec<(NodeId, NodeId)>>>,
    ) -> Self {
        Self { local, inboxes, partitions }
    }

    /// Helper to build a fully-connected cluster. Returns one transport per
    /// node and one receiver per node (drain these in the node's driver
    /// loop).
    pub fn new_cluster(
        nodes: &[NodeId],
    ) -> (
        Vec<SimTransport>,
        HashMap<NodeId, mpsc::UnboundedReceiver<Message>>,
    ) {
        let partitions = Arc::new(Mutex::new(Vec::new()));
        let mut senders = HashMap::new();
        let mut receivers = HashMap::new();
        for &n in nodes {
            let (tx, rx) = mpsc::unbounded_channel();
            senders.insert(n, tx);
            receivers.insert(n, rx);
        }
        let shared = Arc::new(Mutex::new(senders));
        let transports = nodes
            .iter()
            .map(|&n| SimTransport::new(n, shared.clone(), partitions.clone()))
            .collect();
        (transports, receivers)
    }

    pub fn partition(&self, a: NodeId, b: NodeId) {
        self.partitions.lock().push((a, b));
    }

    pub fn heal(&self) {
        self.partitions.lock().clear();
    }

    fn is_partitioned(&self, a: NodeId, b: NodeId) -> bool {
        self.partitions
            .lock()
            .iter()
            .any(|&(x, y)| (x == a && y == b) || (x == b && y == a))
    }
}

#[async_trait]
impl Transport for SimTransport {
    async fn send(&self, to: NodeId, msg: Message) -> Result<()> {
        if self.is_partitioned(self.local, to) {
            // Silently drop — Raft is supposed to tolerate this.
            return Ok(());
        }
        let sender = {
            let guard = self.inboxes.lock();
            guard.get(&to).cloned()
        };
        match sender {
            Some(tx) => tx
                .send(msg)
                .map_err(|e| Error::Network(format!("send to {to}: {e}"))),
            None => Err(Error::Network(format!("unknown peer {to}"))),
        }
    }

    fn local_id(&self) -> NodeId {
        self.local
    }
}
