//! Deterministic multi-node Raft test harness.
//!
//! `Cluster` spins up N `Node`s sharing in-memory storage and a directly
//! routed message bus (no async, no real network). Tests drive simulated
//! time via [`Cluster::tick_all`] / [`Cluster::step`] / [`Cluster::run_for`]
//! and can introduce faults via [`Cluster::partition`], [`Cluster::isolate`],
//! [`Cluster::kill`].
//!
//! Election timeouts are staggered (instead of randomized) so single-leader
//! election is deterministic in tests — the lowest-id node always wins from
//! a cold start.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;

use kepler_raft::{Config, MemRaftStorage, Message, Node, Role, StateMachine};
use kepler_storage::{Engine, MemEngine};
use kepler_types::{Error, LogIndex, NodeId, Result, Term};

use crate::state_machine::{encode_delete, encode_put, KvStateMachine};

pub struct Cluster {
    nodes: Vec<ClusterNode>,
    partitions: HashSet<(NodeId, NodeId)>,
}

struct ClusterNode {
    id: NodeId,
    node: Node,
    #[allow(dead_code)]
    storage: MemRaftStorage,
    engine: Arc<MemEngine>,
    sm: Arc<KvStateMachine<MemEngine>>,
    inbox: Vec<Message>,
    alive: bool,
}

impl Cluster {
    pub fn new(ids: Vec<NodeId>) -> Self {
        let nodes = ids
            .iter()
            .enumerate()
            .map(|(idx, &id)| {
                let storage = MemRaftStorage::new();
                let engine = Arc::new(MemEngine::new());
                let sm = Arc::new(KvStateMachine::new(engine.clone()));
                let peers: Vec<NodeId> = ids.iter().filter(|&&i| i != id).copied().collect();
                // Staggered election timeouts replace real Raft's randomization
                // for test determinism. Lowest-id node always wins from a cold
                // start.
                let election_timeout = 10 + (idx as u32) * 5;
                let config = Config {
                    id,
                    peers,
                    heartbeat_interval: 1,
                    election_timeout,
                    max_entries_per_msg: 64,
                    leader_lease: Duration::from_secs(1),
                };
                let node = Node::new(config, Box::new(storage.clone())).unwrap();
                ClusterNode {
                    id,
                    node,
                    storage,
                    engine,
                    sm,
                    inbox: Vec::new(),
                    alive: true,
                }
            })
            .collect();
        Self { nodes, partitions: HashSet::new() }
    }

    /// Tick every alive node once.
    pub fn tick_all(&mut self) {
        for cn in &mut self.nodes {
            if cn.alive {
                cn.node.tick();
            }
        }
    }

    /// Run delivery rounds until quiescence or `max_rounds` is hit.
    pub fn step(&mut self, max_rounds: usize) {
        for _ in 0..max_rounds {
            if self.deliver_round() == 0 {
                break;
            }
        }
    }

    /// Tick + settle, repeated. The typical test driver.
    pub fn run_for(&mut self, ticks: u32, settle_per_tick: usize) {
        for _ in 0..ticks {
            self.tick_all();
            self.step(settle_per_tick);
        }
    }

    fn deliver_round(&mut self) -> usize {
        let mut activity = 0;

        // 1. Each alive node processes its inbox.
        for cn in &mut self.nodes {
            if !cn.alive {
                continue;
            }
            let inbox = std::mem::take(&mut cn.inbox);
            activity += inbox.len();
            for msg in inbox {
                let _ = cn.node.step(msg);
            }
        }

        // 2. Each alive node drains ready; outbound messages collected.
        let mut outbox: Vec<Message> = Vec::new();
        for cn in &mut self.nodes {
            if !cn.alive {
                continue;
            }
            let ready = cn.node.ready();
            activity += ready.committed.len() + ready.messages.len();
            for entry in &ready.committed {
                let _ = cn.sm.apply(entry);
            }
            outbox.extend(ready.messages.iter().cloned());
            cn.node.advance(ready);
        }

        // 3. Route, dropping messages for partitions or dead nodes.
        for msg in outbox {
            if self.is_partitioned(msg.from, msg.to) {
                continue;
            }
            if let Some(target) = self.nodes.iter_mut().find(|n| n.id == msg.to) {
                if target.alive {
                    target.inbox.push(msg);
                }
            }
        }

        activity
    }

    // ---- client-facing helpers ------------------------------------------

    /// Find the leader with the highest term. After a partition, the old
    /// leader may still believe it's the leader at its old term while a new
    /// leader has been elected at a higher term on the majority side. The
    /// higher-term one is the real leader.
    pub fn find_leader(&self) -> Option<NodeId> {
        self.nodes
            .iter()
            .filter(|n| n.alive && n.node.is_leader())
            .max_by_key(|n| n.node.term())
            .map(|n| n.id)
    }

    pub fn propose_to(&mut self, id: NodeId, cmd: Bytes) -> Result<()> {
        let cn = self
            .nodes
            .iter_mut()
            .find(|n| n.id == id)
            .ok_or_else(|| Error::InvalidArgument(format!("no such node {}", id)))?;
        if !cn.alive {
            return Err(Error::Internal(format!("node {} is dead", id)));
        }
        cn.node.propose(cmd)
    }

    pub fn put_at_leader(&mut self, key: &[u8], value: &[u8]) -> Result<NodeId> {
        let leader = self
            .find_leader()
            .ok_or_else(|| Error::Internal("no leader".into()))?;
        self.propose_to(leader, encode_put(key, value))?;
        Ok(leader)
    }

    pub fn delete_at_leader(&mut self, key: &[u8]) -> Result<NodeId> {
        let leader = self
            .find_leader()
            .ok_or_else(|| Error::Internal("no leader".into()))?;
        self.propose_to(leader, encode_delete(key))?;
        Ok(leader)
    }

    // ---- fault injection -----------------------------------------------

    pub fn partition(&mut self, a: NodeId, b: NodeId) {
        self.partitions.insert((a, b));
        self.partitions.insert((b, a));
    }

    /// Cut `id` off from every other node in the cluster.
    pub fn isolate(&mut self, id: NodeId) {
        let others: Vec<NodeId> = self
            .nodes
            .iter()
            .filter(|n| n.id != id)
            .map(|n| n.id)
            .collect();
        for other in others {
            self.partition(id, other);
        }
    }

    pub fn heal(&mut self) {
        self.partitions.clear();
    }

    pub fn is_partitioned(&self, a: NodeId, b: NodeId) -> bool {
        self.partitions.contains(&(a, b))
    }

    pub fn kill(&mut self, id: NodeId) {
        if let Some(cn) = self.nodes.iter_mut().find(|n| n.id == id) {
            cn.alive = false;
            cn.inbox.clear();
        }
    }

    pub fn revive(&mut self, id: NodeId) {
        if let Some(cn) = self.nodes.iter_mut().find(|n| n.id == id) {
            cn.alive = true;
        }
    }

    // ---- inspection ----------------------------------------------------

    pub fn engine_get(&self, id: NodeId, key: &[u8]) -> Option<Bytes> {
        self.nodes
            .iter()
            .find(|n| n.id == id)
            .and_then(|n| n.engine.get(key).ok().flatten())
    }

    pub fn applied_index(&self, id: NodeId) -> Option<LogIndex> {
        self.nodes
            .iter()
            .find(|n| n.id == id)
            .map(|n| n.sm.applied_index())
    }

    pub fn role(&self, id: NodeId) -> Option<Role> {
        self.nodes.iter().find(|n| n.id == id).map(|n| n.node.role())
    }

    pub fn term(&self, id: NodeId) -> Option<Term> {
        self.nodes.iter().find(|n| n.id == id).map(|n| n.node.term())
    }

    pub fn ids(&self) -> Vec<NodeId> {
        self.nodes.iter().map(|n| n.id).collect()
    }

    pub fn alive_ids(&self) -> Vec<NodeId> {
        self.nodes.iter().filter(|n| n.alive).map(|n| n.id).collect()
    }
}

// ---- tests --------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// Run ticks until any node is leader, or fail.
    fn run_until_leader(cluster: &mut Cluster, max_ticks: u32) -> NodeId {
        for _ in 0..max_ticks {
            cluster.tick_all();
            cluster.step(200);
            if let Some(leader) = cluster.find_leader() {
                return leader;
            }
        }
        panic!(
            "no leader elected within {} ticks; roles = {:?}",
            max_ticks,
            cluster
                .ids()
                .into_iter()
                .map(|id| (id, cluster.role(id), cluster.term(id)))
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn three_node_cluster_elects_a_leader() {
        let mut cluster = Cluster::new(vec![1, 2, 3]);
        let leader = run_until_leader(&mut cluster, 30);
        // With staggered timeouts, lowest-id node wins from cold start.
        assert_eq!(leader, 1);
        // Term should be 1 — no failed elections.
        assert_eq!(cluster.term(1), Some(1));
    }

    #[test]
    fn replicated_writes_propagate_to_all_nodes() {
        let mut cluster = Cluster::new(vec![1, 2, 3]);
        run_until_leader(&mut cluster, 30);

        cluster.put_at_leader(b"k", b"v").unwrap();
        cluster.run_for(5, 200);

        for id in cluster.ids() {
            let v = cluster.engine_get(id, b"k");
            assert_eq!(
                v.as_deref(),
                Some(&b"v"[..]),
                "node {} missing replicated data",
                id
            );
        }
    }

    #[test]
    fn delete_propagates_to_followers() {
        let mut cluster = Cluster::new(vec![1, 2, 3]);
        run_until_leader(&mut cluster, 30);

        cluster.put_at_leader(b"k", b"v").unwrap();
        cluster.run_for(5, 200);
        cluster.delete_at_leader(b"k").unwrap();
        cluster.run_for(5, 200);

        for id in cluster.ids() {
            assert!(
                cluster.engine_get(id, b"k").is_none(),
                "node {} still has deleted key",
                id
            );
        }
    }

    #[test]
    fn cluster_survives_follower_death() {
        let mut cluster = Cluster::new(vec![1, 2, 3]);
        run_until_leader(&mut cluster, 30);
        cluster.kill(3);

        cluster.put_at_leader(b"k", b"v").unwrap();
        cluster.run_for(10, 200);

        assert_eq!(cluster.engine_get(1, b"k").as_deref(), Some(&b"v"[..]));
        assert_eq!(cluster.engine_get(2, b"k").as_deref(), Some(&b"v"[..]));
        // Dead node never got it.
        assert!(cluster.engine_get(3, b"k").is_none());
    }

    #[test]
    fn leader_death_triggers_failover() {
        let mut cluster = Cluster::new(vec![1, 2, 3]);
        let leader = run_until_leader(&mut cluster, 30);

        cluster.put_at_leader(b"early", b"v1").unwrap();
        cluster.run_for(5, 200);

        cluster.kill(leader);

        // New leader must be one of the survivors.
        let new_leader = run_until_leader(&mut cluster, 50);
        assert_ne!(new_leader, leader);

        cluster.put_at_leader(b"late", b"v2").unwrap();
        cluster.run_for(10, 200);

        for id in cluster.alive_ids() {
            assert_eq!(
                cluster.engine_get(id, b"early").as_deref(),
                Some(&b"v1"[..]),
                "live node {} missing pre-failover write",
                id
            );
            assert_eq!(
                cluster.engine_get(id, b"late").as_deref(),
                Some(&b"v2"[..]),
                "live node {} missing post-failover write",
                id
            );
        }
    }

    #[test]
    fn minority_partition_cannot_commit() {
        let mut cluster = Cluster::new(vec![1, 2, 3]);
        let leader = run_until_leader(&mut cluster, 30);

        cluster.put_at_leader(b"k0", b"v0").unwrap();
        cluster.run_for(5, 200);

        // Isolate the leader (now in minority {leader} vs majority {others}).
        cluster.isolate(leader);

        // Let the majority side time out and elect. Old leader will keep
        // believing it's leader at the old term until it hears a higher term;
        // `find_leader` prefers the highest-term leader.
        cluster.run_for(80, 200);
        let new_leader = cluster.find_leader().expect("majority elects new leader");
        assert_ne!(new_leader, leader);

        // Write goes through on the new leader.
        cluster
            .propose_to(new_leader, encode_put(b"k_new", b"v_new"))
            .unwrap();
        cluster.run_for(10, 200);

        // Majority sees the new value.
        for id in cluster.ids().into_iter().filter(|&id| id != leader) {
            assert_eq!(
                cluster.engine_get(id, b"k_new").as_deref(),
                Some(&b"v_new"[..]),
                "majority node {} missing new write",
                id
            );
        }
        // Isolated old leader does not.
        assert!(cluster.engine_get(leader, b"k_new").is_none());
    }

    #[test]
    fn partition_heal_converges_state() {
        let mut cluster = Cluster::new(vec![1, 2, 3]);
        let old_leader = run_until_leader(&mut cluster, 30);

        cluster.put_at_leader(b"k0", b"v0").unwrap();
        cluster.run_for(5, 200);

        // Isolate the old leader; majority elects a new one.
        cluster.isolate(old_leader);
        cluster.run_for(80, 200);
        let new_leader = cluster.find_leader().expect("majority elects new leader");
        assert_ne!(new_leader, old_leader);

        cluster
            .propose_to(new_leader, encode_put(b"k_new", b"v_new"))
            .unwrap();
        cluster.run_for(10, 200);

        // Heal: now the isolated node should catch up.
        cluster.heal();
        cluster.run_for(50, 200);

        for id in cluster.ids() {
            assert_eq!(
                cluster.engine_get(id, b"k0").as_deref(),
                Some(&b"v0"[..]),
                "node {} missing pre-partition write after heal",
                id
            );
            assert_eq!(
                cluster.engine_get(id, b"k_new").as_deref(),
                Some(&b"v_new"[..]),
                "node {} missing post-partition write after heal",
                id
            );
        }
    }

    #[test]
    fn many_proposals_all_apply_in_order_on_followers() {
        let mut cluster = Cluster::new(vec![1, 2, 3]);
        run_until_leader(&mut cluster, 30);

        for i in 0..50u32 {
            let key = format!("k{:03}", i);
            let value = format!("v{:03}", i);
            cluster.put_at_leader(key.as_bytes(), value.as_bytes()).unwrap();
        }
        cluster.run_for(20, 200);

        for id in cluster.ids() {
            assert_eq!(
                cluster.applied_index(id),
                Some(50),
                "node {} applied_index wrong",
                id
            );
            for i in 0..50u32 {
                let key = format!("k{:03}", i);
                let value = format!("v{:03}", i);
                assert_eq!(
                    cluster.engine_get(id, key.as_bytes()).as_deref(),
                    Some(value.as_bytes()),
                    "node {} missing key {}",
                    id,
                    key
                );
            }
        }
    }
}
