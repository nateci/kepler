//! `Node` — the Raft state machine, pure logic.
//!
//! The driver loop:
//!
//! ```ignore
//! loop {
//!     tokio::select! {
//!         _ = tick_interval.tick()   => node.tick(),
//!         msg = inbound.recv()       => node.step(msg)?,
//!         data = proposal.recv()     => node.propose(data)?,
//!     }
//!     let ready = node.ready();
//!     // 1. persist ready.entries + ready.hard_state
//!     // 2. send ready.messages
//!     // 3. apply ready.committed to state machine
//!     // 4. install ready.snapshot if Some
//!     node.advance(ready);
//! }
//! ```
//!
//! Anything that touches IO lives in the driver, not here.

use std::time::Duration;

use bytes::Bytes;

use kepler_types::{LogIndex, NodeId, Result, Term};

use crate::state_machine::StateMachine;
use crate::storage::RaftStorage;
use crate::types::{Entry, HardState, Message, Snapshot};

#[derive(Debug, Clone)]
pub struct Config {
    pub id: NodeId,
    pub peers: Vec<NodeId>,
    /// Send a heartbeat every `heartbeat_interval` ticks.
    pub heartbeat_interval: u32,
    /// Election timeout (in ticks). Randomized in [election_timeout, 2x).
    pub election_timeout: u32,
    /// Max log entries per AppendEntries.
    pub max_entries_per_msg: usize,
    /// How long the leader's lease holds before it must re-establish.
    pub leader_lease: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            id: 0,
            peers: Vec::new(),
            heartbeat_interval: 1,
            election_timeout: 10,
            max_entries_per_msg: 64,
            leader_lease: Duration::from_secs(1),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)] // variants used once Raft logic lands
enum Role {
    Follower,
    Candidate,
    Leader,
}

pub struct Node {
    id: NodeId,
    role: Role,
    current_term: Term,
    voted_for: Option<NodeId>,
    commit_index: LogIndex,
    last_applied: LogIndex,

    #[allow(dead_code)]
    storage: Box<dyn RaftStorage>,
    // pending state to surface in the next `ready()`
    pending_messages: Vec<Message>,
    pending_entries: Vec<Entry>,
    pending_hard_state: Option<HardState>,
    pending_committed: Vec<Entry>,
    pending_snapshot: Option<Snapshot>,

    #[allow(dead_code)]
    config: Config,
}

/// Outgoing work for the driver. Each field, if non-empty / `Some`, is the
/// driver's responsibility to handle before calling [`Node::advance`].
#[derive(Default)]
pub struct Ready {
    pub messages: Vec<Message>,
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
            storage,
            pending_messages: Vec::new(),
            pending_entries: Vec::new(),
            pending_hard_state: None,
            pending_committed: Vec::new(),
            pending_snapshot: None,
            config,
        })
    }

    /// Drive elections / heartbeats forward. Call every ~100ms.
    pub fn tick(&mut self) {
        // TODO: handle election timeout when follower/candidate; emit heartbeats
        //       when leader.
        let _ = self.role;
    }

    /// Propose a new entry on the leader. Returns error if not leader.
    pub fn propose(&mut self, _data: Bytes) -> Result<()> {
        // TODO: append entry locally, queue AppendEntries to followers
        Ok(())
    }

    /// Process an incoming message from a peer.
    pub fn step(&mut self, _msg: Message) -> Result<()> {
        // TODO: dispatch by message body; handle RequestVote, AppendEntries,
        //       and responses; advance commit index when majority replicate.
        Ok(())
    }

    /// Drain pending side effects for the driver to perform.
    pub fn ready(&mut self) -> Ready {
        Ready {
            messages: std::mem::take(&mut self.pending_messages),
            entries: std::mem::take(&mut self.pending_entries),
            hard_state: self.pending_hard_state.take(),
            committed: std::mem::take(&mut self.pending_committed),
            snapshot: self.pending_snapshot.take(),
        }
    }

    /// Acknowledge that the work surfaced in the matching `Ready` is done.
    pub fn advance(&mut self, _ready: Ready) {
        // TODO: update applied_index, possibly trigger log compaction.
    }

    /// Apply committed entries to the given state machine. The driver calls
    /// this after persisting `Ready::entries` and processing `Ready::committed`.
    pub fn apply_to(&mut self, sm: &dyn StateMachine, entries: &[Entry]) -> Result<()> {
        for entry in entries {
            sm.apply(entry)?;
            self.last_applied = entry.index;
        }
        Ok(())
    }

    pub fn id(&self) -> NodeId { self.id }
    pub fn term(&self) -> Term { self.current_term }
    pub fn is_leader(&self) -> bool { self.role == Role::Leader }
    pub fn commit_index(&self) -> LogIndex { self.commit_index }

    #[allow(dead_code)]
    fn voted_for(&self) -> Option<NodeId> { self.voted_for }
}
