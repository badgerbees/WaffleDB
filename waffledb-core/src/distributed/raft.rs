/// RAFT Consensus Implementation for WaffleDB
/// Handles leader election, log replication, and state machine application

use std::sync::Arc;
use tokio::sync::RwLock;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{info, warn, debug};
use std::collections::HashMap;

/// RAFT states
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum RaftState {
    Follower,
    Candidate,
    Leader,
}

/// Log entry in RAFT log
#[derive(Debug, Clone)]
pub struct LogEntry {
    pub term: u64,
    pub index: u64,
    pub data: Vec<u8>, // Serialized operation
}

/// Volatile state on all servers
#[derive(Debug, Clone)]
pub struct RaftVolatileState {
    pub current_term: u64,
    pub voted_for: Option<String>, // Candidate ID we voted for in current term
    pub log: Vec<LogEntry>,
}

/// Persistent state on all servers
#[derive(Debug, Clone)]
pub struct RaftPersistentState {
    pub commit_index: u64,
    pub last_applied: u64,
}

/// Volatile state on leaders
#[derive(Debug, Clone)]
pub struct RaftLeaderState {
    pub next_index: HashMap<String, u64>,    // Next log index to send to each follower
    pub match_index: HashMap<String, u64>,   // Highest log index known to be replicated
}

/// Main RAFT Coordinator
pub struct RaftCoordinator {
    pub node_id: String,
    pub peers: Vec<String>,
    
    // Persistent state (survives crashes)
    pub volatile: Arc<RwLock<RaftVolatileState>>,
    pub persistent: Arc<RwLock<RaftPersistentState>>,
    
    // Volatile state
    pub state: Arc<RwLock<RaftState>>,
    pub leader_state: Arc<RwLock<Option<RaftLeaderState>>>,
    
    // Current leader (None if unknown)
    pub current_leader: Arc<RwLock<Option<String>>>,
    
    // Timing
    pub election_timeout_ms: u64,
    pub heartbeat_interval_ms: u64,
}

impl RaftCoordinator {
    pub fn new(
        node_id: String,
        peers: Vec<String>,
        election_timeout_ms: u64,
        heartbeat_interval_ms: u64,
    ) -> Self {
        let peers_for_state: HashMap<String, u64> = peers
            .iter()
            .map(|p| (p.clone(), 1))
            .collect();

        let leader_state = RaftLeaderState {
            next_index: peers_for_state.clone(),
            match_index: peers.iter().map(|p| (p.clone(), 0)).collect(),
        };

        info!(
            "RAFT Node initialized: node_id={}, peers={:?}, election_timeout={}ms, heartbeat_interval={}ms",
            node_id, peers, election_timeout_ms, heartbeat_interval_ms
        );

        RaftCoordinator {
            node_id,
            peers,
            volatile: Arc::new(RwLock::new(RaftVolatileState {
                current_term: 0,
                voted_for: None,
                log: vec![],
            })),
            persistent: Arc::new(RwLock::new(RaftPersistentState {
                commit_index: 0,
                last_applied: 0,
            })),
            state: Arc::new(RwLock::new(RaftState::Follower)),
            leader_state: Arc::new(RwLock::new(Some(leader_state))),
            current_leader: Arc::new(RwLock::new(None)),
            election_timeout_ms,
            heartbeat_interval_ms,
        }
    }

    /// Get current state
    pub async fn get_state(&self) -> RaftState {
        *self.state.read().await
    }

    /// Get current term
    pub async fn get_current_term(&self) -> u64 {
        self.volatile.read().await.current_term
    }

    /// Get current leader
    pub async fn get_leader(&self) -> Option<String> {
        self.current_leader.read().await.clone()
    }

    /// Append entry to log (leader only)
    pub async fn append_entry(&self, data: Vec<u8>) -> Result<u64, String> {
        let state = self.state.read().await;
        if *state != RaftState::Leader {
            let leader = self.current_leader.read().await.clone();
            return Err(format!("Not leader. Current leader: {:?}", leader));
        }
        drop(state);

        let mut volatile = self.volatile.write().await;
        let term = volatile.current_term;
        let index = volatile.log.len() as u64 + 1;

        volatile.log.push(LogEntry {
            term,
            index,
            data,
        });

        info!(
            "RAFT: Leader {} appended entry at index {} (term {})",
            self.node_id, index, term
        );

        Ok(index)
    }

    /// Handle AppendEntries RPC from leader
    pub async fn handle_append_entries(
        &self,
        leader_id: String,
        term: u64,
        prev_log_index: u64,
        prev_log_term: u64,
        entries: Vec<LogEntry>,
        leader_commit: u64,
    ) -> (u64, bool) {
        let mut volatile = self.volatile.write().await;

        // If term is newer, update
        if term > volatile.current_term {
            volatile.current_term = term;
            volatile.voted_for = None;
            *self.state.write().await = RaftState::Follower;
            *self.current_leader.write().await = Some(leader_id.clone());
            info!(
                "RAFT: Follower {} updated term to {} from leader {}",
                self.node_id, term, leader_id
            );
        }

        // If term is behind, reject
        if term < volatile.current_term {
            return (volatile.current_term, false);
        }

        // Check log continuity
        if prev_log_index > 0 {
            if (prev_log_index as usize) > volatile.log.len() {
                warn!(
                    "RAFT: Follower {} log too short. Need index {}, have {}",
                    self.node_id,
                    prev_log_index,
                    volatile.log.len()
                );
                return (volatile.current_term, false);
            }

            if volatile.log[(prev_log_index - 1) as usize].term != prev_log_term {
                warn!(
                    "RAFT: Follower {} term mismatch at index {}",
                    self.node_id, prev_log_index
                );
                return (volatile.current_term, false);
            }
        }

        // Append new entries
        for entry in entries {
            volatile.log.push(entry);
        }

        // Update commit index
        let mut persistent = self.persistent.write().await;
        if leader_commit > persistent.commit_index {
            persistent.commit_index = leader_commit.min(volatile.log.len() as u64);
            debug!(
                "RAFT: Follower {} updated commit_index to {}",
                self.node_id, persistent.commit_index
            );
        }

        info!(
            "RAFT: Follower {} accepted entries from leader {}. Log size: {}",
            self.node_id,
            leader_id,
            volatile.log.len()
        );

        (volatile.current_term, true)
    }

    /// Handle RequestVote RPC from candidate
    pub async fn handle_request_vote(
        &self,
        candidate_id: String,
        term: u64,
        last_log_index: u64,
        last_log_term: u64,
    ) -> (u64, bool) {
        let mut volatile = self.volatile.write().await;

        // If term is newer, update and reset vote
        if term > volatile.current_term {
            volatile.current_term = term;
            volatile.voted_for = None;
            *self.state.write().await = RaftState::Follower;
            info!(
                "RAFT: Follower {} updated term to {} for election from {}",
                self.node_id, term, candidate_id
            );
        }

        // If term is behind, reject
        if term < volatile.current_term {
            return (volatile.current_term, false);
        }

        // If already voted in this term, reject
        if let Some(voted_for) = &volatile.voted_for {
            if voted_for != &candidate_id {
                debug!(
                    "RAFT: Follower {} already voted for {} in term {}",
                    self.node_id, voted_for, term
                );
                return (volatile.current_term, false);
            }
        }

        // Check if candidate's log is up-to-date
        let last_log_idx = if volatile.log.is_empty() {
            0
        } else {
            volatile.log[volatile.log.len() - 1].index
        };
        let last_log_t = if volatile.log.is_empty() {
            0
        } else {
            volatile.log[volatile.log.len() - 1].term
        };

        if last_log_term < last_log_t || (last_log_term == last_log_t && last_log_index < last_log_idx)
        {
            debug!(
                "RAFT: Follower {} candidate {} log not up-to-date",
                self.node_id, candidate_id
            );
            return (volatile.current_term, false);
        }

        // Grant vote
        volatile.voted_for = Some(candidate_id.clone());
        info!(
            "RAFT: Follower {} voted for candidate {} in term {}",
            self.node_id, candidate_id, term
        );

        (volatile.current_term, true)
    }

    /// Become leader (called after winning election)
    pub async fn become_leader(&self) {
        let mut state = self.state.write().await;
        *state = RaftState::Leader;

        let volatile = self.volatile.read().await;
        let next_index = volatile.log.len() as u64 + 1;

        let mut leader_state = self.leader_state.write().await;
        let mut new_state = RaftLeaderState {
            next_index: self.peers.iter().map(|p| (p.clone(), next_index)).collect(),
            match_index: self.peers.iter().map(|p| (p.clone(), 0)).collect(),
        };
        *leader_state = Some(new_state);

        *self.current_leader.write().await = Some(self.node_id.clone());

        info!(
            "RAFT: Node {} became LEADER in term {}",
            self.node_id, volatile.current_term
        );
    }

    /// Get the last log index and term
    pub async fn get_last_log_index_and_term(&self) -> (u64, u64) {
        let volatile = self.volatile.read().await;
        if volatile.log.is_empty() {
            (0, 0)
        } else {
            let last = &volatile.log[volatile.log.len() - 1];
            (last.index, last.term)
        }
    }

    /// Check if committed entries should be applied to state machine
    pub async fn get_entries_to_apply(&self) -> Vec<LogEntry> {
        let volatile = self.volatile.read().await;
        let mut persistent = self.persistent.write().await;

        if persistent.commit_index > persistent.last_applied {
            let start = persistent.last_applied as usize;
            let end = persistent.commit_index as usize;
            let entries: Vec<LogEntry> = volatile.log[start..end]
                .iter()
                .cloned()
                .collect();
            persistent.last_applied = persistent.commit_index;
            entries
        } else {
            vec![]
        }
    }

    /// Update match_index for a follower (leader only)
    pub async fn update_match_index(&self, follower: String, index: u64) {
        if let Some(ref mut leader_state) = *self.leader_state.write().await {
            leader_state.match_index.insert(follower, index);
            
            // Update commit_index based on replicated entries
            let indices: Vec<u64> = leader_state
                .match_index
                .values()
                .copied()
                .collect();
            let mut sorted = indices;
            sorted.sort();
            
            if sorted.len() >= (self.peers.len() + 1) / 2 {
                let median = sorted[sorted.len() / 2];
                let volatile = self.volatile.read().await;
                if median > 0 && volatile.log[(median - 1) as usize].term == volatile.current_term {
                    let mut persistent = self.persistent.write().await;
                    if median > persistent.commit_index {
                        persistent.commit_index = median;
                        debug!(
                            "RAFT: Leader {} updated commit_index to {} (replicated to majority)",
                            self.node_id, median
                        );
                    }
                }
            }
        }
    }
}
