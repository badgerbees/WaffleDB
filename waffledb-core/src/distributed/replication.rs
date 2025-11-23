/// Lightweight RAFT-based replication
/// 
/// Replicates WAL entries and metadata across nodes.
/// Does NOT replicate HNSW graph (stays local to each shard).

use std::collections::VecDeque;
use serde::{Serialize, Deserialize};
use crate::core::errors::{Result, WaffleError, ErrorCode};

/// RAFT log entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    pub term: u64,
    pub index: u64,
    pub operation: Operation,
}

/// Operations replicated via RAFT
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Operation {
    /// Insert document
    Insert {
        doc_id: String,
        vector: Vec<f32>,
        metadata: String, // JSON serialized
    },
    /// Delete document
    Delete {
        doc_id: String,
    },
    /// Update metadata
    UpdateMetadata {
        doc_id: String,
        metadata: String,
    },
    /// Snapshot marker
    Snapshot {
        snapshot_id: String,
        term: u64,
        index: u64,
    },
}

/// Replication state for a node
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicationState {
    /// Node is a follower (replicates from leader)
    Follower,
    /// Node is a candidate (election in progress)
    Candidate,
    /// Node is the leader (accepts writes)
    Leader,
}

/// Replication configuration
#[derive(Debug, Clone)]
pub struct ReplicationConfig {
    /// Node ID in the RAFT cluster
    pub node_id: String,
    /// List of peer node IDs
    pub peers: Vec<String>,
    /// Election timeout in milliseconds
    pub election_timeout_ms: u64,
    /// Heartbeat interval in milliseconds
    pub heartbeat_interval_ms: u64,
}

impl Default for ReplicationConfig {
    fn default() -> Self {
        Self {
            node_id: "node0".to_string(),
            peers: vec![],
            election_timeout_ms: 1500,
            heartbeat_interval_ms: 500,
        }
    }
}

/// RAFT replication manager
#[derive(Debug)]
pub struct ReplicationManager {
    config: ReplicationConfig,
    state: ReplicationState,
    current_term: u64,
    voted_for: Option<String>,
    log: VecDeque<LogEntry>,
    commit_index: u64,
    last_applied: u64,
}

impl ReplicationManager {
    /// Create new replication manager
    pub fn new(config: ReplicationConfig) -> Result<Self> {
        if config.node_id.is_empty() {
            return Err(WaffleError::WithCode {
                code: ErrorCode::ValidationFailed,
                message: "node_id cannot be empty".to_string(),
            });
        }

        Ok(Self {
            config,
            state: ReplicationState::Follower,
            current_term: 0,
            voted_for: None,
            log: VecDeque::new(),
            commit_index: 0,
            last_applied: 0,
        })
    }

    /// Get current replication state
    pub fn state(&self) -> ReplicationState {
        self.state
    }

    /// Get current term
    pub fn current_term(&self) -> u64 {
        self.current_term
    }

    /// Append entry to local log
    pub fn append_entry(&mut self, operation: Operation) -> Result<LogEntry> {
        let term = self.current_term;
        let index = (self.log.len() as u64) + 1;

        let entry = LogEntry {
            term,
            index,
            operation,
        };

        self.log.push_back(entry.clone());

        Ok(entry)
    }

    /// Transition to leader state
    pub fn become_leader(&mut self) -> Result<()> {
        self.state = ReplicationState::Leader;
        self.current_term += 1;
        self.voted_for = Some(self.config.node_id.clone());
        Ok(())
    }

    /// Transition to follower state
    pub fn become_follower(&mut self, term: u64) -> Result<()> {
        if term > self.current_term {
            self.current_term = term;
            self.voted_for = None;
        }
        self.state = ReplicationState::Follower;
        Ok(())
    }

    /// Get log entries from index onwards
    pub fn get_log_entries(&self, from_index: u64) -> Vec<LogEntry> {
        self.log
            .iter()
            .skip(from_index as usize)
            .cloned()
            .collect()
    }

    /// Apply committed entries
    pub fn apply_committed_entries(&mut self, new_commit_index: u64) -> Vec<LogEntry> {
        let mut applied = Vec::new();

        while self.last_applied < new_commit_index && self.last_applied < self.log.len() as u64 {
            if let Some(entry) = self.log.get(self.last_applied as usize) {
                applied.push(entry.clone());
                self.last_applied += 1;
            }
        }

        self.commit_index = new_commit_index;
        applied
    }

    /// Get log length
    pub fn log_length(&self) -> u64 {
        self.log.len() as u64
    }

    /// Clear log (after snapshot)
    pub fn clear_log(&mut self) {
        self.log.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_replication_manager_creation() {
        let config = ReplicationConfig::default();
        let manager = ReplicationManager::new(config).unwrap();
        assert_eq!(manager.state(), ReplicationState::Follower);
        assert_eq!(manager.current_term(), 0);
    }

    #[test]
    fn test_append_entry() {
        let config = ReplicationConfig::default();
        let mut manager = ReplicationManager::new(config).unwrap();

        let op = Operation::Insert {
            doc_id: "doc1".to_string(),
            vector: vec![1.0, 2.0],
            metadata: "{}".to_string(),
        };

        let entry = manager.append_entry(op).unwrap();
        assert_eq!(entry.index, 1);
        assert_eq!(manager.log_length(), 1);
    }

    #[test]
    fn test_become_leader() {
        let config = ReplicationConfig::default();
        let mut manager = ReplicationManager::new(config).unwrap();

        manager.become_leader().unwrap();
        assert_eq!(manager.state(), ReplicationState::Leader);
    }

    #[test]
    fn test_become_follower() {
        let config = ReplicationConfig::default();
        let mut manager = ReplicationManager::new(config).unwrap();

        manager.become_leader().unwrap();
        manager.become_follower(1).unwrap();
        assert_eq!(manager.state(), ReplicationState::Follower);
    }

    #[test]
    fn test_apply_committed() {
        let config = ReplicationConfig::default();
        let mut manager = ReplicationManager::new(config).unwrap();

        for i in 0..5 {
            let op = Operation::Insert {
                doc_id: format!("doc{}", i),
                vector: vec![1.0],
                metadata: "{}".to_string(),
            };
            manager.append_entry(op).ok();
        }

        let applied = manager.apply_committed_entries(3);
        assert_eq!(applied.len(), 3);
    }
}
