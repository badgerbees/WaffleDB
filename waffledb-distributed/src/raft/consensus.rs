/// RAFT consensus implementation for WaffleDB distributed mode
/// 
/// This module implements the core RAFT consensus algorithm to ensure:
/// - Strong consistency across replicas
/// - Safe leader election with term tracking
/// - Deterministic log replication with term/index ordering
/// - Automatic failover and recovery

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use crate::raft::node::{RaftNode, RaftRole};

/// Log entry that must be replicated across cluster
#[derive(Debug, Clone)]
pub struct LogEntry {
    pub index: usize,
    pub term: u64,
    pub command: Vec<u8>, // Serialized command: Insert, Delete, etc.
}

/// Append entries RPC - used for heartbeat and replication
#[derive(Debug, Clone)]
pub struct AppendEntriesRequest {
    pub term: u64,
    pub leader_id: String,
    pub prev_log_index: usize,
    pub prev_log_term: u64,
    pub entries: Vec<LogEntry>,
    pub leader_commit: usize,
}

/// Response to append entries RPC
#[derive(Debug, Clone)]
pub struct AppendEntriesResponse {
    pub term: u64,
    pub success: bool,
    pub match_index: usize, // Highest replicated index on this node
}

/// Request vote RPC - used for leader election
#[derive(Debug, Clone)]
pub struct RequestVoteRequest {
    pub term: u64,
    pub candidate_id: String,
    pub last_log_index: usize,
    pub last_log_term: u64,
}

/// Response to request vote RPC
#[derive(Debug, Clone)]
pub struct RequestVoteResponse {
    pub term: u64,
    pub vote_granted: bool,
}

/// Consensus state machine that implements RAFT
pub struct RaftConsensus {
    node: Arc<Mutex<RaftNode>>,
    log: Arc<Mutex<Vec<LogEntry>>>,
    state_machine: Arc<Mutex<HashMap<String, Vec<u8>>>>, // State on this node
    #[allow(dead_code)]
    next_index: Arc<Mutex<HashMap<String, usize>>>, // For each peer: next log index to send
    #[allow(dead_code)]
    match_index: Arc<Mutex<HashMap<String, usize>>>, // For each peer: highest replicated index
}

impl RaftConsensus {
    /// Create new RAFT consensus with initial state
    pub fn new(id: String, peers: Vec<String>) -> Self {
        let node = RaftNode::new(id.clone(), peers.clone());
        
        let mut next_idx = HashMap::new();
        let mut match_idx = HashMap::new();
        for peer in &peers {
            next_idx.insert(peer.clone(), 0);
            match_idx.insert(peer.clone(), 0);
        }
        
        RaftConsensus {
            node: Arc::new(Mutex::new(node)),
            log: Arc::new(Mutex::new(vec![])),
            state_machine: Arc::new(Mutex::new(HashMap::new())),
            next_index: Arc::new(Mutex::new(next_idx)),
            match_index: Arc::new(Mutex::new(match_idx)),
        }
    }
    
    /// Append entry (command) to log - called by leader
    /// Returns (index, term) where command is stored
    pub fn append_command(&self, command: Vec<u8>) -> Result<(usize, u64), String> {
        let node = self.node.lock().map_err(|e| format!("Lock error: {}", e))?;
        
        if node.role != RaftRole::Leader {
            return Err("Not leader".to_string());
        }
        
        let mut log = self.log.lock().map_err(|e| format!("Lock error: {}", e))?;
        let index = log.len();
        let term = node.current_term;
        
        log.push(LogEntry {
            index,
            term,
            command,
        });
        
        Ok((index, term))
    }
    
    /// Apply committed entries to state machine
    /// Called when commit_index advances (entries are replicated)
    pub fn apply_committed_entries(&self) -> Result<usize, String> {
        let mut node = self.node.lock().map_err(|e| format!("Lock error: {}", e))?;
        let log = self.log.lock().map_err(|e| format!("Lock error: {}", e))?;
        let mut state = self.state_machine.lock().map_err(|e| format!("Lock error: {}", e))?;
        
        let mut applied = 0;
        while node.last_applied < node.commit_index && node.last_applied < log.len() {
            if let Some(entry) = log.get(node.last_applied) {
                // Apply command to state machine
                // For simplicity, store key=node_id, value=command
                state.insert(
                    format!("entry_{}", node.last_applied),
                    entry.command.clone(),
                );
                node.last_applied += 1;
                applied += 1;
            } else {
                break;
            }
        }
        
        Ok(applied)
    }
    
    /// Handle append entries RPC from leader
    pub fn handle_append_entries(&self, req: AppendEntriesRequest) -> Result<AppendEntriesResponse, String> {
        let mut node = self.node.lock().map_err(|e| format!("Lock error: {}", e))?;
        let mut log = self.log.lock().map_err(|e| format!("Lock error: {}", e))?;
        
        // If term in request is higher, update and become follower
        if req.term > node.current_term {
            node.current_term = req.term;
            node.become_follower(req.term);
        }
        
        // Reject if our term is higher
        if req.term < node.current_term {
            return Ok(AppendEntriesResponse {
                term: node.current_term,
                success: false,
                match_index: log.len().saturating_sub(1),
            });
        }
        
        // Check log consistency: if we don't have prev_log_index, fail
        if req.prev_log_index > 0 && req.prev_log_index > log.len() {
            return Ok(AppendEntriesResponse {
                term: node.current_term,
                success: false,
                match_index: log.len().saturating_sub(1),
            });
        }
        
        // Check term matches at prev_log_index
        if req.prev_log_index > 0 {
            if let Some(entry) = log.get(req.prev_log_index - 1) {
                if entry.term != req.prev_log_term {
                    // Conflicting entries: truncate
                    log.truncate(req.prev_log_index - 1);
                    return Ok(AppendEntriesResponse {
                        term: node.current_term,
                        success: false,
                        match_index: log.len().saturating_sub(1),
                    });
                }
            }
        }
        
        // Append new entries
        for entry in req.entries {
            if log.len() <= entry.index {
                log.push(entry);
            }
        }
        
        // Update commit index
        let match_index = log.len().saturating_sub(1);
        node.commit_index = std::cmp::min(req.leader_commit, log.len().saturating_sub(1));
        
        Ok(AppendEntriesResponse {
            term: node.current_term,
            success: true,
            match_index,
        })
    }
    
    /// Handle request vote RPC for election
    pub fn handle_request_vote(&self, req: RequestVoteRequest) -> Result<RequestVoteResponse, String> {
        let mut node = self.node.lock().map_err(|e| format!("Lock error: {}", e))?;
        let log = self.log.lock().map_err(|e| format!("Lock error: {}", e))?;
        
        // Update term if needed
        if req.term > node.current_term {
            node.current_term = req.term;
            node.voted_for = None;
            node.become_follower(req.term);
        }
        
        // Can't vote for older term
        if req.term < node.current_term {
            return Ok(RequestVoteResponse {
                term: node.current_term,
                vote_granted: false,
            });
        }
        
        // Check if already voted in this term
        if let Some(voted) = &node.voted_for {
            if voted != &req.candidate_id {
                return Ok(RequestVoteResponse {
                    term: node.current_term,
                    vote_granted: false,
                });
            }
        }
        
        // Check candidate's log is up-to-date
        let last_log_index = log.len().saturating_sub(1);
        let last_log_term = log.last()
            .map(|e| e.term)
            .unwrap_or(0);
        
        if req.last_log_term < last_log_term || 
           (req.last_log_term == last_log_term && req.last_log_index < last_log_index) {
            return Ok(RequestVoteResponse {
                term: node.current_term,
                vote_granted: false,
            });
        }
        
        // Grant vote
        node.voted_for = Some(req.candidate_id);
        Ok(RequestVoteResponse {
            term: node.current_term,
            vote_granted: true,
        })
    }
    
    /// Get current node state
    pub fn get_state(&self) -> Result<(u64, bool), String> {
        let node = self.node.lock().map_err(|e| format!("Lock error: {}", e))?;
        Ok((node.current_term, node.is_leader()))
    }
    
    /// Get current log length
    pub fn get_log_len(&self) -> Result<usize, String> {
        let log = self.log.lock().map_err(|e| format!("Lock error: {}", e))?;
        Ok(log.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_raft_initialization() {
        let consensus = RaftConsensus::new(
            "node1".to_string(),
            vec!["node2".to_string(), "node3".to_string()],
        );
        
        let (term, is_leader) = consensus.get_state().unwrap();
        assert_eq!(term, 0);
        assert!(!is_leader);
    }
    
    #[test]
    fn test_append_command_requires_leader() {
        let consensus = RaftConsensus::new("node1".to_string(), vec![]);
        
        // Follower cannot append
        let result = consensus.append_command(vec![1, 2, 3]);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Not leader"));
    }
    
    #[test]
    fn test_request_vote_election() {
        let consensus = RaftConsensus::new("node1".to_string(), vec![]);
        
        let request = RequestVoteRequest {
            term: 1,
            candidate_id: "node2".to_string(),
            last_log_index: 0,
            last_log_term: 0,
        };
        
        let response = consensus.handle_request_vote(request).unwrap();
        assert!(response.vote_granted);
        assert_eq!(response.term, 1);
    }
    
    #[test]
    fn test_append_entries_replication() {
        let consensus = RaftConsensus::new("node1".to_string(), vec![]);
        
        // Simulate leader sending entries
        let request = AppendEntriesRequest {
            term: 1,
            leader_id: "leader".to_string(),
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![LogEntry {
                index: 0,
                term: 1,
                command: vec![1, 2, 3],
            }],
            leader_commit: 0,
        };
        
        let response = consensus.handle_append_entries(request).unwrap();
        assert!(response.success);
        
        let log_len = consensus.get_log_len().unwrap();
        assert_eq!(log_len, 1);
    }
}
