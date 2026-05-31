//! Raft `Node` — single-node working, multi-node-ready FSM.
//!
//! Driver pattern (etcd-raft style):
//!
//! ```ignore
//! loop {
//!     tokio::select! {
//!         _ = tick_interval.tick()   => node.tick(),
//!         msg = inbound.recv()       => node.step(msg)?,
//!         data = proposal.recv()     => node.propose(data)?,
//!     }
//!     let ready = node.ready();
//!     for msg in &ready.messages { transport.send(msg).await? }
//!     node.apply_to(&state_machine, &ready.committed)?;
//!     node.advance(ready);
//! }
//! ```
//!
//! v0 simplifications:
//! - Node writes log entries to `RaftStorage` directly inside `propose` /
//!   `step` rather than deferring to the driver via `Ready::entries`. The
//!   `entries` field stays in the public API for forward compatibility but is
//!   always empty.
//! - `election_timeout` is used directly without randomization — fine for
//!   single-node tests. Multi-node liveness will require randomization in
//!   `[election_timeout, 2 * election_timeout)`.
//! - No snapshots, no learners, no joint-consensus membership changes.

use std::collections::HashMap;
use std::time::Duration;

use bytes::Bytes;
use tracing::debug;

use kepler_types::{Error, LogIndex, NodeId, Result, Term};

use crate::state_machine::StateMachine;
use crate::storage::RaftStorage;
use crate::types::{Entry, EntryKind, HardState, Message, MessageBody, Snapshot};

#[derive(Debug, Clone)]
pub struct Config {
    pub id: NodeId,
    /// Peer IDs, NOT including self. Empty = single-node cluster.
    pub peers: Vec<NodeId>,
    /// Send a heartbeat every `heartbeat_interval` ticks.
    pub heartbeat_interval: u32,
    /// Become a candidate after this many ticks with no heartbeat.
    pub election_timeout: u32,
    /// Cap on entries per `AppendEntries` RPC.
    pub max_entries_per_msg: usize,
    /// How long the leader's lease holds before it must re-establish.
    pub leader_lease: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            id: 1,
            peers: Vec::new(),
            heartbeat_interval: 1,
            election_timeout: 10,
            max_entries_per_msg: 64,
            leader_lease: Duration::from_secs(1),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Follower,
    Candidate,
    Leader,
}

pub struct Node {
    id: NodeId,
    role: Role,

    // Persistent (mirrored into RaftStorage::save_hard_state).
    current_term: Term,
    voted_for: Option<NodeId>,
    commit_index: LogIndex,

    // Volatile.
    last_applied: LogIndex,
    leader_id: Option<NodeId>,

    // Leader-only state. Per-peer indices.
    next_index: HashMap<NodeId, LogIndex>,
    match_index: HashMap<NodeId, LogIndex>,

    // Candidate-only state. Tracks granted/refused votes received this term.
    votes_received: HashMap<NodeId, bool>,

    // Tick counter; meaning depends on role.
    elapsed_ticks: u32,

    storage: Box<dyn RaftStorage>,

    // Outgoing work surfaced via Ready.
    pending_messages: Vec<Message>,
    pending_hard_state: Option<HardState>,
    pending_snapshot: Option<Snapshot>,

    config: Config,
}

/// Outgoing work for the driver. Each non-empty / `Some` field is the
/// driver's responsibility to handle before calling [`Node::advance`].
#[derive(Default, Debug)]
pub struct Ready {
    pub messages: Vec<Message>,
    /// Always empty in v0 — entries are persisted by the Node directly.
    /// Reserved for the etcd-raft-style "unstable log" model in a future
    /// refactor.
    pub entries: Vec<Entry>,
    pub hard_state: Option<HardState>,
    pub committed: Vec<Entry>,
    pub snapshot: Option<Snapshot>,
}

impl Node {
    pub fn new(config: Config, storage: Box<dyn RaftStorage>) -> Result<Self> {
        let (hs, _cs) = storage.initial_state()?;
        Ok(Self {
            id: config.id,
            role: Role::Follower,
            current_term: hs.term,
            voted_for: hs.vote,
            commit_index: hs.commit,
            last_applied: hs.commit,
            leader_id: None,
            next_index: HashMap::new(),
            match_index: HashMap::new(),
            votes_received: HashMap::new(),
            elapsed_ticks: 0,
            storage,
            pending_messages: Vec::new(),
            pending_hard_state: None,
            pending_snapshot: None,
            config,
        })
    }

    // ---- public driver entry points -------------------------------------

    /// Drive timers forward. Call every ~heartbeat_interval real time.
    pub fn tick(&mut self) {
        self.elapsed_ticks += 1;
        match self.role {
            Role::Follower | Role::Candidate => {
                if self.elapsed_ticks >= self.config.election_timeout {
                    self.start_election();
                }
            }
            Role::Leader => {
                if self.elapsed_ticks >= self.config.heartbeat_interval {
                    self.broadcast_heartbeat();
                    self.elapsed_ticks = 0;
                }
            }
        }
    }

    /// Propose a client command. Only the leader can propose.
    pub fn propose(&mut self, data: Bytes) -> Result<()> {
        if self.role != Role::Leader {
            return Err(Error::NotLeader(self.leader_id));
        }
        let last_idx = self.storage.last_index()?;
        let new_idx = last_idx + 1;
        let entry = Entry {
            index: new_idx,
            term: self.current_term,
            kind: EntryKind::Normal,
            data,
        };
        self.storage.append(&[entry])?;
        self.match_index.insert(self.id, new_idx);

        // Eagerly replicate to followers (minimizes proposal latency vs.
        // waiting for the next heartbeat).
        let peers: Vec<NodeId> = self.config.peers.clone();
        for peer in peers {
            self.send_append_entries(peer);
        }

        self.maybe_advance_commit()?;
        Ok(())
    }

    /// Process an incoming message from a peer.
    pub fn step(&mut self, msg: Message) -> Result<()> {
        // Term handling rule: any higher term causes a step-down.
        if msg.term > self.current_term {
            let leader = if matches!(msg.body, MessageBody::AppendEntries { .. }) {
                Some(msg.from)
            } else {
                None
            };
            self.become_follower(msg.term, leader);
        } else if msg.term < self.current_term {
            // Stale; drop silently. (A real impl would send a response so the
            // peer learns of the higher term.)
            return Ok(());
        }

        let from = msg.from;
        match msg.body {
            MessageBody::AppendEntries {
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
            } => self.handle_append_entries(from, prev_log_index, prev_log_term, entries, leader_commit)?,
            MessageBody::AppendResponse { success, match_index } => {
                if self.role == Role::Leader {
                    self.handle_append_response(from, success, match_index)?;
                }
            }
            MessageBody::RequestVote { last_log_index, last_log_term } => {
                self.handle_request_vote(from, last_log_index, last_log_term)?;
            }
            MessageBody::VoteResponse { granted } => {
                if self.role == Role::Candidate {
                    self.handle_vote_response(from, granted);
                }
            }
            MessageBody::InstallSnapshot { .. } | MessageBody::InstallSnapshotResponse => {
                // TODO: snapshots.
            }
        }
        Ok(())
    }

    /// Drain pending side effects. The driver must process all non-empty
    /// fields and then call `advance(ready)`.
    pub fn ready(&mut self) -> Ready {
        let committed = if self.commit_index > self.last_applied {
            self.storage
                .entries(self.last_applied + 1, self.commit_index + 1)
                .unwrap_or_default()
        } else {
            Vec::new()
        };
        Ready {
            messages: std::mem::take(&mut self.pending_messages),
            entries: Vec::new(),
            hard_state: self.pending_hard_state.take(),
            committed,
            snapshot: self.pending_snapshot.take(),
        }
    }

    /// Acknowledge that the work surfaced in the matching `Ready` is done.
    pub fn advance(&mut self, ready: Ready) {
        if let Some(last) = ready.committed.last() {
            self.last_applied = self.last_applied.max(last.index);
        }
    }

    /// Apply committed entries to the given state machine. Convenience
    /// wrapper around the typical `ready` → driver loop.
    pub fn apply_to(&mut self, sm: &dyn StateMachine, entries: &[Entry]) -> Result<()> {
        for entry in entries {
            sm.apply(entry)?;
            self.last_applied = self.last_applied.max(entry.index);
        }
        Ok(())
    }

    // ---- accessors ------------------------------------------------------

    pub fn id(&self) -> NodeId { self.id }
    pub fn term(&self) -> Term { self.current_term }
    pub fn role(&self) -> Role { self.role }
    pub fn is_leader(&self) -> bool { self.role == Role::Leader }
    pub fn leader_id(&self) -> Option<NodeId> { self.leader_id }
    pub fn commit_index(&self) -> LogIndex { self.commit_index }
    pub fn last_applied(&self) -> LogIndex { self.last_applied }

    // ---- private helpers ------------------------------------------------

    fn start_election(&mut self) {
        self.role = Role::Candidate;
        self.current_term += 1;
        self.voted_for = Some(self.id);
        self.votes_received.clear();
        self.votes_received.insert(self.id, true);
        self.elapsed_ticks = 0;
        self.leader_id = None;

        debug!(id = self.id, term = self.current_term, "starting election");
        self.persist_hard_state();

        let last_log_index = self.storage.last_index().unwrap_or(0);
        let last_log_term = if last_log_index == 0 {
            0
        } else {
            self.storage.term(last_log_index).unwrap_or(0)
        };

        for &peer in &self.config.peers {
            self.pending_messages.push(Message {
                from: self.id,
                to: peer,
                term: self.current_term,
                body: MessageBody::RequestVote { last_log_index, last_log_term },
            });
        }

        // Single-node cluster: we already hold a majority (just self).
        self.maybe_become_leader();
    }

    fn maybe_become_leader(&mut self) {
        if self.role != Role::Candidate {
            return;
        }
        let granted = self.votes_received.values().filter(|v| **v).count();
        let cluster_size = self.config.peers.len() + 1;
        let majority = cluster_size / 2 + 1;
        if granted >= majority {
            self.become_leader();
        }
    }

    fn become_leader(&mut self) {
        self.role = Role::Leader;
        self.leader_id = Some(self.id);
        self.elapsed_ticks = 0;

        let last_idx = self.storage.last_index().unwrap_or(0);
        self.next_index.clear();
        self.match_index.clear();
        for &peer in &self.config.peers {
            self.next_index.insert(peer, last_idx + 1);
            self.match_index.insert(peer, 0);
        }

        debug!(id = self.id, term = self.current_term, "became leader");
        // Establish authority with an immediate heartbeat (carries current
        // log state so followers can catch up).
        self.broadcast_heartbeat();
        // For single-node, attempt to commit any pre-existing entries from
        // the new term right away. (For pre-existing entries from earlier
        // terms, the paper requires a current-term entry first — we skip
        // the explicit no-op for v0.)
        let _ = self.maybe_advance_commit();
    }

    fn become_follower(&mut self, term: Term, leader: Option<NodeId>) {
        let term_changed = term > self.current_term;
        self.role = Role::Follower;
        if term_changed {
            self.current_term = term;
            self.voted_for = None;
        }
        self.leader_id = leader;
        self.elapsed_ticks = 0;
        self.persist_hard_state();
    }

    fn maybe_advance_commit(&mut self) -> Result<()> {
        if self.role != Role::Leader {
            return Ok(());
        }
        let old_commit = self.commit_index;
        let last_idx = self.storage.last_index()?;
        let cluster_size = self.config.peers.len() + 1;
        let majority = cluster_size / 2 + 1;

        // Raft safety: a leader may only commit entries from its current term
        // by counting replicas. Earlier-term entries get implicitly committed
        // by Log Matching when the index advances past them.
        for n in (self.commit_index + 1)..=last_idx {
            let term_at_n = match self.storage.term(n) {
                Ok(t) => t,
                Err(_) => continue,
            };
            if term_at_n != self.current_term {
                continue;
            }
            let count = 1 + self.match_index.values().filter(|&&mi| mi >= n).count();
            if count >= majority {
                self.commit_index = n;
            }
        }

        if self.commit_index > old_commit {
            self.persist_hard_state();
        }
        Ok(())
    }

    fn broadcast_heartbeat(&mut self) {
        let peers: Vec<NodeId> = self.config.peers.clone();
        for peer in peers {
            self.send_append_entries(peer);
        }
    }

    /// Build and queue an `AppendEntries` to `peer` based on the leader's
    /// per-peer `next_index`. If the peer is fully caught up, this is an
    /// empty heartbeat.
    fn send_append_entries(&mut self, peer: NodeId) {
        let next = *self.next_index.get(&peer).unwrap_or(&1);
        let prev_log_index = next.saturating_sub(1);
        let prev_log_term = if prev_log_index == 0 {
            0
        } else {
            self.storage.term(prev_log_index).unwrap_or(0)
        };
        let last_idx = self.storage.last_index().unwrap_or(0);
        let entries = if next > last_idx {
            Vec::new()
        } else {
            let high_excl =
                (next + self.config.max_entries_per_msg as u64).min(last_idx + 1);
            self.storage.entries(next, high_excl).unwrap_or_default()
        };
        self.pending_messages.push(Message {
            from: self.id,
            to: peer,
            term: self.current_term,
            body: MessageBody::AppendEntries {
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit: self.commit_index,
            },
        });
    }

    fn handle_append_entries(
        &mut self,
        leader: NodeId,
        prev_log_index: LogIndex,
        prev_log_term: Term,
        entries: Vec<Entry>,
        leader_commit: LogIndex,
    ) -> Result<()> {
        // Hearing from a current-term leader resets the election timer.
        self.elapsed_ticks = 0;
        self.leader_id = Some(leader);
        if self.role == Role::Candidate {
            // A current-term leader exists; abandon the candidacy.
            self.become_follower(self.current_term, Some(leader));
        }

        // Log consistency check: prev_log_index must match in term.
        let last_idx = self.storage.last_index()?;
        let consistent = if prev_log_index == 0 {
            true
        } else if prev_log_index > last_idx {
            false
        } else {
            self.storage.term(prev_log_index)? == prev_log_term
        };

        if !consistent {
            self.pending_messages.push(Message {
                from: self.id,
                to: leader,
                term: self.current_term,
                body: MessageBody::AppendResponse { success: false, match_index: 0 },
            });
            return Ok(());
        }

        if !entries.is_empty() {
            // RaftStorage::append already truncates conflicts on its own.
            self.storage.append(&entries)?;
        }

        let new_last_idx = self.storage.last_index()?;

        if leader_commit > self.commit_index {
            let new_commit = leader_commit.min(new_last_idx);
            if new_commit > self.commit_index {
                self.commit_index = new_commit;
                self.persist_hard_state();
            }
        }

        self.pending_messages.push(Message {
            from: self.id,
            to: leader,
            term: self.current_term,
            body: MessageBody::AppendResponse { success: true, match_index: new_last_idx },
        });
        Ok(())
    }

    fn handle_append_response(
        &mut self,
        peer: NodeId,
        success: bool,
        match_index: LogIndex,
    ) -> Result<()> {
        if success {
            self.match_index.insert(peer, match_index);
            self.next_index.insert(peer, match_index + 1);
            self.maybe_advance_commit()?;
        } else {
            // Step back and retry immediately (eager retry; real impls do
            // bin-search back-off via "conflict info" returned by the follower).
            if let Some(ni) = self.next_index.get_mut(&peer) {
                *ni = (*ni).saturating_sub(1).max(1);
            }
            self.send_append_entries(peer);
        }
        Ok(())
    }

    fn handle_request_vote(
        &mut self,
        candidate: NodeId,
        last_log_index: LogIndex,
        last_log_term: Term,
    ) -> Result<()> {
        let our_last_idx = self.storage.last_index()?;
        let our_last_term = if our_last_idx == 0 {
            0
        } else {
            self.storage.term(our_last_idx)?
        };

        // Up-to-date rule (§5.4.1): candidate's log must be at least as
        // up-to-date as ours.
        let log_ok = last_log_term > our_last_term
            || (last_log_term == our_last_term && last_log_index >= our_last_idx);
        let can_vote = self.voted_for.is_none() || self.voted_for == Some(candidate);
        let granted = log_ok && can_vote;

        if granted {
            self.voted_for = Some(candidate);
            self.elapsed_ticks = 0;
            self.persist_hard_state();
        }

        self.pending_messages.push(Message {
            from: self.id,
            to: candidate,
            term: self.current_term,
            body: MessageBody::VoteResponse { granted },
        });
        Ok(())
    }

    fn handle_vote_response(&mut self, peer: NodeId, granted: bool) {
        self.votes_received.insert(peer, granted);
        self.maybe_become_leader();
    }

    fn persist_hard_state(&mut self) {
        let hs = HardState {
            term: self.current_term,
            vote: self.voted_for,
            commit: self.commit_index,
        };
        // If the underlying storage write fails, the driver will see no
        // hard_state in Ready and surface the error elsewhere. v0 swallows
        // the error since MemRaftStorage never fails.
        let _ = self.storage.save_hard_state(&hs);
        self.pending_hard_state = Some(hs);
    }
}

// ---- tests --------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mem_storage::MemRaftStorage;
    use parking_lot::Mutex;

    fn single_node_config(id: NodeId) -> Config {
        Config {
            id,
            peers: Vec::new(),
            heartbeat_interval: 1,
            election_timeout: 5,
            max_entries_per_msg: 64,
            leader_lease: Duration::from_secs(1),
        }
    }

    fn make_node(id: NodeId) -> Node {
        Node::new(single_node_config(id), Box::new(MemRaftStorage::new())).unwrap()
    }

    fn tick_n(node: &mut Node, n: u32) {
        for _ in 0..n {
            node.tick();
        }
    }

    /// State machine that records every applied entry.
    #[derive(Default)]
    struct MockSm {
        applied: Mutex<Vec<Entry>>,
    }

    impl StateMachine for MockSm {
        fn apply(&self, entry: &Entry) -> Result<Bytes> {
            self.applied.lock().push(entry.clone());
            Ok(Bytes::new())
        }
        fn snapshot(&self) -> Result<Bytes> {
            Ok(Bytes::new())
        }
        fn restore(&self, _snapshot: Bytes) -> Result<()> {
            Ok(())
        }
        fn applied_index(&self) -> LogIndex {
            self.applied.lock().last().map(|e| e.index).unwrap_or(0)
        }
    }

    #[test]
    fn fresh_node_starts_as_follower_at_term_0() {
        let node = make_node(1);
        assert_eq!(node.role(), Role::Follower);
        assert_eq!(node.term(), 0);
        assert_eq!(node.commit_index(), 0);
        assert!(node.leader_id().is_none());
    }

    #[test]
    fn election_timeout_promotes_single_node_to_leader() {
        let mut node = make_node(1);
        tick_n(&mut node, 5);
        assert_eq!(node.role(), Role::Leader);
        assert_eq!(node.term(), 1);
        assert_eq!(node.leader_id(), Some(1));
    }

    #[test]
    fn follower_propose_returns_not_leader() {
        let mut node = make_node(1);
        let err = node.propose(Bytes::from_static(b"x"));
        match err {
            Err(Error::NotLeader(_)) => {}
            other => panic!("expected NotLeader, got {:?}", other),
        }
    }

    #[test]
    fn leader_propose_commits_immediately_on_single_node() {
        let mut node = make_node(1);
        tick_n(&mut node, 5);
        node.propose(Bytes::from_static(b"hello")).unwrap();
        assert_eq!(node.commit_index(), 1);
    }

    #[test]
    fn ready_returns_committed_entries() {
        let mut node = make_node(1);
        tick_n(&mut node, 5);
        node.propose(Bytes::from_static(b"a")).unwrap();
        node.propose(Bytes::from_static(b"b")).unwrap();

        let ready = node.ready();
        assert_eq!(ready.committed.len(), 2);
        assert_eq!(ready.committed[0].data.as_ref(), b"a");
        assert_eq!(ready.committed[1].data.as_ref(), b"b");
        // Each proposal advances commit + persists hard_state.
        assert!(ready.hard_state.is_some());
    }

    #[test]
    fn advance_marks_committed_entries_as_applied() {
        let mut node = make_node(1);
        tick_n(&mut node, 5);
        node.propose(Bytes::from_static(b"a")).unwrap();

        assert_eq!(node.last_applied(), 0);
        let ready = node.ready();
        node.advance(ready);
        assert_eq!(node.last_applied(), 1);

        let ready2 = node.ready();
        assert!(ready2.committed.is_empty());
    }

    #[test]
    fn apply_to_drives_state_machine() {
        let mut node = make_node(1);
        tick_n(&mut node, 5);
        node.propose(Bytes::from_static(b"x")).unwrap();
        node.propose(Bytes::from_static(b"y")).unwrap();

        let sm = MockSm::default();
        let ready = node.ready();
        node.apply_to(&sm, &ready.committed).unwrap();

        let applied = sm.applied.lock();
        assert_eq!(applied.len(), 2);
        assert_eq!(applied[0].data.as_ref(), b"x");
        assert_eq!(applied[0].index, 1);
        assert_eq!(applied[1].data.as_ref(), b"y");
        assert_eq!(applied[1].index, 2);
        assert_eq!(node.last_applied(), 2);
    }

    #[test]
    fn term_and_commit_persist_across_recreate() {
        let storage = MemRaftStorage::new();
        {
            let mut node = Node::new(
                single_node_config(1),
                Box::new(storage.clone()),
            )
            .unwrap();
            tick_n(&mut node, 5);
            node.propose(Bytes::from_static(b"data")).unwrap();
            assert!(node.is_leader());
            assert_eq!(node.term(), 1);
            assert_eq!(node.commit_index(), 1);
        }

        let node = Node::new(
            single_node_config(1),
            Box::new(storage.clone()),
        )
        .unwrap();
        assert_eq!(node.term(), 1);
        assert_eq!(node.commit_index(), 1);
        // Roles always reset on restart — leadership is term-scoped state.
        assert_eq!(node.role(), Role::Follower);
        // The proposed entry is still in the log.
        let entries = storage.entries(1, 2).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].data.as_ref(), b"data");
    }

    #[test]
    fn re_elects_after_restart_and_keeps_existing_log() {
        let storage = MemRaftStorage::new();
        {
            let mut node = Node::new(single_node_config(1), Box::new(storage.clone())).unwrap();
            tick_n(&mut node, 5);
            node.propose(Bytes::from_static(b"old")).unwrap();
        }

        let mut node = Node::new(single_node_config(1), Box::new(storage.clone())).unwrap();
        // Stays at restored term until next timeout.
        assert_eq!(node.term(), 1);
        tick_n(&mut node, 5);
        // New election runs at term 2.
        assert!(node.is_leader());
        assert_eq!(node.term(), 2);
        // Old entry is still present.
        assert_eq!(storage.entries(1, 2).unwrap()[0].data.as_ref(), b"old");

        // And we can propose more on top.
        node.propose(Bytes::from_static(b"new")).unwrap();
        assert_eq!(storage.last_index().unwrap(), 2);
        assert_eq!(node.commit_index(), 2);
    }

    #[test]
    fn higher_term_message_forces_step_down() {
        let mut node = make_node(1);
        tick_n(&mut node, 5);
        assert!(node.is_leader());
        assert_eq!(node.term(), 1);

        // Inject an AppendEntries from a fictional peer at term 99.
        node.step(Message {
            from: 2,
            to: 1,
            term: 99,
            body: MessageBody::AppendEntries {
                prev_log_index: 0,
                prev_log_term: 0,
                entries: Vec::new(),
                leader_commit: 0,
            },
        })
        .unwrap();

        assert_eq!(node.role(), Role::Follower);
        assert_eq!(node.term(), 99);
        assert_eq!(node.leader_id(), Some(2));
    }
}
