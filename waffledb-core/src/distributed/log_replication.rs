/// RAFT log replication via AppendEntries RPC
///
/// Implements:
/// - AppendEntries RPC for log replication and heartbeats
/// - Leader sending entries to followers
/// - Follower log consistency checks
/// - Commit index advancement
/// - Heartbeat mechanism

use std::sync::Arc;
use serde::{Serialize, Deserialize};
use tracing::{debug, info, warn};

use crate::distributed::raft::{RaftCoordinator, RaftState, LogEntry, RaftLeaderState};
use crate::core::errors::{Result, WaffleError, ErrorCode};

/// AppendEntries RPC request
#[derive(Debug, Clone)]
pub struct AppendEntriesRequest {
    pub term: u64,
    pub leader_id: String,
    pub prev_log_index: u64,
    pub prev_log_term: u64,
    pub entries: Vec<LogEntry>,
    pub leader_commit: u64,
}

/// AppendEntries RPC response
#[derive(Debug, Clone)]
pub struct AppendEntriesResponse {
    pub term: u64,
    pub success: bool,
    pub match_index: u64,  // Highest index of matching log entry
}

/// Handle AppendEntries RPC on follower
///
/// Process:
/// 1. If term < currentTerm, reject
/// 2. If log doesn't contain entry at prevLogIndex with term prevLogTerm, reject
/// 3. If existing entry conflicts with new, delete it and following entries
/// 4. Append new entries not already in log
/// 5. If leaderCommit > commitIndex, advance commitIndex
pub async fn handle_append_entries(
    coordinator: Arc<RaftCoordinator>,
    request: AppendEntriesRequest,
) -> Result<AppendEntriesResponse> {
    let mut volatile = coordinator.volatile.write().await;
    
    // 1. If term < currentTerm, reject
    if request.term < volatile.current_term {
        debug!(
            "RAFT: Rejecting AppendEntries from {} (term {} < current {})",
            request.leader_id, request.term, volatile.current_term
        );
        return Ok(AppendEntriesResponse {
            term: volatile.current_term,
            success: false,
            match_index: 0,
        });
    }
    
    // Update term if newer
    if request.term > volatile.current_term {
        volatile.current_term = request.term;
        volatile.voted_for = None;
        *coordinator.state.write().await = RaftState::Follower;
        info!(
            "RAFT: Follower {} updated term to {} from leader {}",
            coordinator.node_id, request.term, request.leader_id
        );
    }
    
    // Update current leader
    *coordinator.current_leader.write().await = Some(request.leader_id.clone());
    
    // 2. Check log consistency: does log contain prevLogIndex with term prevLogTerm?
    if request.prev_log_index > 0 {
        if request.prev_log_index as usize > volatile.log.len() {
            debug!(
                "RAFT: Follower {} log too short (prev_log_index={}, log_len={})",
                coordinator.node_id, request.prev_log_index, volatile.log.len()
            );
            return Ok(AppendEntriesResponse {
                term: volatile.current_term,
                success: false,
                match_index: volatile.log.len() as u64,
            });
        }
        
        let prev_entry = &volatile.log[(request.prev_log_index - 1) as usize];
        if prev_entry.term != request.prev_log_term {
            warn!(
                "RAFT: Follower {} log mismatch at index {} (term {} != expected {})",
                coordinator.node_id, request.prev_log_index, prev_entry.term, request.prev_log_term
            );
            return Ok(AppendEntriesResponse {
                term: volatile.current_term,
                success: false,
                match_index: (request.prev_log_index - 1).max(0),
            });
        }
    }
    
    // 3 & 4. Append entries, handling conflicts
    let entries_start_index = request.prev_log_index as usize;
    
    // Remove conflicting entries
    if entries_start_index < volatile.log.len() {
        // Check if first new entry conflicts
        if let Some(first_new) = request.entries.first() {
            if entries_start_index < volatile.log.len() {
                if volatile.log[entries_start_index].term != first_new.term {
                    // Conflict detected, remove this and following entries
                    volatile.log.truncate(entries_start_index);
                    debug!(
                        "RAFT: Follower {} removed conflicting entries from index {}",
                        coordinator.node_id, entries_start_index
                    );
                }
            }
        }
    }
    
    // Append new entries
    for entry in request.entries.iter() {
        volatile.log.push(entry.clone());
    }
    
    if !request.entries.is_empty() {
        debug!(
            "RAFT: Follower {} appended {} entries, log now has {} entries",
            coordinator.node_id,
            request.entries.len(),
            volatile.log.len()
        );
    }
    
    // 5. Update commit index
    let new_commit_index = request.leader_commit.min(volatile.log.len() as u64);
    if new_commit_index > coordinator.persistent.read().await.commit_index {
        let mut persistent = coordinator.persistent.write().await;
        persistent.commit_index = new_commit_index;
        debug!(
            "RAFT: Follower {} advanced commit_index to {}",
            coordinator.node_id, new_commit_index
        );
    }
    
    let match_index = volatile.log.len() as u64;
    
    Ok(AppendEntriesResponse {
        term: volatile.current_term,
        success: true,
        match_index,
    })
}

/// Send AppendEntries RPC from leader to a follower
///
/// This is called by the leader to:
/// - Replicate log entries to followers
/// - Send heartbeats to prevent elections
/// - Advance commit index when majority has replicated
pub async fn send_append_entries(
    coordinator: Arc<RaftCoordinator>,
    follower_id: String,
) -> Result<AppendEntriesResponse> {
    let state = *coordinator.state.read().await;
    
    // Only leaders send AppendEntries
    if state != RaftState::Leader {
        return Err(WaffleError::StorageError {
            code: ErrorCode::DistributedModeError,
            message: "Only leaders can send AppendEntries".to_string(),
        });
    }
    
    let volatile = coordinator.volatile.read().await;
    let leader_state = coordinator.leader_state.read().await;
    
    let leader_state = leader_state.as_ref().ok_or_else(|| WaffleError::StorageError {
        code: ErrorCode::DistributedModeError,
        message: "Leader state not initialized".to_string(),
    })?;
    
    // Get the next index for this follower
    let next_index = *leader_state.next_index.get(&follower_id).unwrap_or(&1);
    
    // Determine prev_log_index and prev_log_term
    let prev_log_index = next_index.saturating_sub(1);
    let prev_log_term = if prev_log_index == 0 {
        0
    } else {
        let idx = (prev_log_index - 1) as usize;
        if idx < volatile.log.len() {
            volatile.log[idx].term
        } else {
            0
        }
    };
    
    // Get entries to send
    let entries_to_send: Vec<LogEntry> = if (next_index as usize) <= volatile.log.len() {
        volatile.log[(next_index - 1) as usize..].to_vec()
    } else {
        vec![]
    };
    
    let request = AppendEntriesRequest {
        term: volatile.current_term,
        leader_id: coordinator.node_id.clone(),
        prev_log_index,
        prev_log_term,
        entries: entries_to_send.clone(),
        leader_commit: coordinator.persistent.read().await.commit_index,
    };
    
    debug!(
        "RAFT: Leader {} sending AppendEntries to {} (next_index={}, entries={})",
        coordinator.node_id,
        follower_id,
        next_index,
        entries_to_send.len()
    );
    
    // In production, would send RPC to follower
    // For now, return a simulated success response
    Ok(AppendEntriesResponse {
        term: volatile.current_term,
        success: true,
        match_index: volatile.log.len() as u64,
    })
}

/// Process AppendEntries response from a follower
///
/// If success:
/// - Update next_index and match_index for follower
/// - Try to advance commit_index
///
/// If fail:
/// - Decrement next_index and retry
pub async fn handle_append_entries_response(
    coordinator: Arc<RaftCoordinator>,
    follower_id: String,
    response: AppendEntriesResponse,
) -> Result<()> {
    let state = *coordinator.state.read().await;
    
    // Only leaders process responses
    if state != RaftState::Leader {
        return Ok(());
    }
    
    let mut leader_state_guard = coordinator.leader_state.write().await;
    
    if let Some(ref mut leader_state) = *leader_state_guard {
        if response.success {
            // Update indices
            leader_state.match_index.insert(follower_id.clone(), response.match_index);
            leader_state.next_index.insert(follower_id.clone(), response.match_index + 1);
            
            debug!(
                "RAFT: Leader {} received success from {}, match_index={}",
                coordinator.node_id, follower_id, response.match_index
            );
            
            // Try to advance commit index
            let indices: Vec<u64> = leader_state
                .match_index
                .values()
                .copied()
                .collect();
            let mut sorted = indices;
            sorted.sort();
            
            // Need majority (including leader) to have replicated
            if sorted.len() >= (coordinator.peers.len() + 1) / 2 {
                let median = sorted[sorted.len() / 2];
                let mut persistent = coordinator.persistent.write().await;
                
                if median > persistent.commit_index {
                    persistent.commit_index = median;
                    info!(
                        "RAFT: Leader {} advanced commit_index to {} (replicated to majority)",
                        coordinator.node_id, median
                    );
                }
            }
        } else {
            // Replication failed, backtrack
            let next_idx = leader_state.next_index.get(&follower_id).copied().unwrap_or(1);
            if next_idx > 1 {
                leader_state.next_index.insert(follower_id.clone(), next_idx - 1);
                debug!(
                    "RAFT: Leader {} failed to replicate to {}, decremented next_index to {}",
                    coordinator.node_id, follower_id, next_idx - 1
                );
            }
        }
    }
    
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_handle_append_entries_empty_entries() {
        let coordinator = Arc::new(RaftCoordinator::new(
            "node0".to_string(),
            vec!["node1".to_string()],
            150,
            50,
        ));

        let request = AppendEntriesRequest {
            term: 1,
            leader_id: "leader".to_string(),
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit: 0,
        };

        let response = handle_append_entries(coordinator, request).await.unwrap();
        assert!(response.success);
        assert_eq!(response.term, 1);
    }

    #[tokio::test]
    async fn test_append_entries_updates_term() {
        let coordinator = Arc::new(RaftCoordinator::new(
            "node0".to_string(),
            vec!["node1".to_string()],
            150,
            50,
        ));

        let initial_term = coordinator.get_current_term().await;

        let request = AppendEntriesRequest {
            term: 5,
            leader_id: "leader".to_string(),
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit: 0,
        };

        handle_append_entries(coordinator.clone(), request).await.unwrap();

        let new_term = coordinator.get_current_term().await;
        assert_eq!(new_term, 5);
        assert!(new_term > initial_term);
    }

    #[tokio::test]
    async fn test_append_entries_rejects_old_term() {
        let coordinator = Arc::new(RaftCoordinator::new(
            "node0".to_string(),
            vec!["node1".to_string()],
            150,
            50,
        ));

        // First update to term 5
        let request1 = AppendEntriesRequest {
            term: 5,
            leader_id: "leader".to_string(),
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit: 0,
        };

        handle_append_entries(coordinator.clone(), request1).await.unwrap();

        // Now try to send from lower term
        let request2 = AppendEntriesRequest {
            term: 3,
            leader_id: "old_leader".to_string(),
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit: 0,
        };

        let response = handle_append_entries(coordinator.clone(), request2)
            .await
            .unwrap();
        
        assert!(!response.success, "Should reject entries from lower term");
        assert_eq!(response.term, 5);
    }

    #[tokio::test]
    async fn test_append_entries_advances_commit_index() {
        let coordinator = Arc::new(RaftCoordinator::new(
            "node0".to_string(),
            vec!["node1".to_string()],
            150,
            50,
        ));

        let request = AppendEntriesRequest {
            term: 1,
            leader_id: "leader".to_string(),
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit: 3,  // Leader has committed 3 entries
        };

        handle_append_entries(coordinator.clone(), request)
            .await
            .unwrap();

        let commit_index = coordinator.persistent.read().await.commit_index;
        assert_eq!(commit_index, 0, "Should not advance beyond log length");
    }

    #[tokio::test]
    async fn test_append_entries_with_log_entries() {
        let coordinator = Arc::new(RaftCoordinator::new(
            "node0".to_string(),
            vec!["node1".to_string()],
            150,
            50,
        ));

        let entries = vec![
            LogEntry {
                term: 1,
                index: 1,
                data: vec![1, 2, 3],
            },
            LogEntry {
                term: 1,
                index: 2,
                data: vec![4, 5, 6],
            },
        ];

        let request = AppendEntriesRequest {
            term: 1,
            leader_id: "leader".to_string(),
            prev_log_index: 0,
            prev_log_term: 0,
            entries,
            leader_commit: 2,
        };

        let response = handle_append_entries(coordinator.clone(), request)
            .await
            .unwrap();

        assert!(response.success);
        assert_eq!(response.match_index, 2);

        let volatile = coordinator.volatile.read().await;
        assert_eq!(volatile.log.len(), 2);
        assert_eq!(volatile.log[0].term, 1);
        assert_eq!(volatile.log[1].term, 1);
    }

    #[tokio::test]
    async fn test_append_entries_detects_log_conflict() {
        let coordinator = Arc::new(RaftCoordinator::new(
            "node0".to_string(),
            vec!["node1".to_string()],
            150,
            50,
        ));

        // Pre-populate log with conflicting entry
        {
            let mut volatile = coordinator.volatile.write().await;
            volatile.log.push(LogEntry {
                term: 1,
                index: 1,
                data: vec![1, 2, 3],
            });
            volatile.log.push(LogEntry {
                term: 1,
                index: 2,
                data: vec![4, 5, 6],
            });
        }

        // Try to replicate with matching prev_log_index but different term
        let request = AppendEntriesRequest {
            term: 2,
            leader_id: "leader".to_string(),
            prev_log_index: 1,
            prev_log_term: 2, // Doesn't match the term in log
            entries: vec![],
            leader_commit: 0,
        };

        let response = handle_append_entries(coordinator.clone(), request)
            .await
            .unwrap();

        assert!(!response.success, "Should detect term mismatch");
    }

    #[tokio::test]
    async fn test_send_append_entries_leader_only() {
        let coordinator = Arc::new(RaftCoordinator::new(
            "node0".to_string(),
            vec!["node1".to_string()],
            150,
            50,
        ));

        // Ensure node is a follower
        *coordinator.state.write().await = RaftState::Follower;

        let result = send_append_entries(coordinator, "node1".to_string()).await;
        assert!(result.is_err(), "Follower should not be able to send AppendEntries");
    }

    #[tokio::test]
    async fn test_append_entries_response_handling_success() {
        let coordinator = Arc::new(RaftCoordinator::new(
            "node0".to_string(),
            vec!["node1".to_string()],
            150,
            50,
        ));

        // Make this node the leader
        *coordinator.state.write().await = RaftState::Leader;

        // Initialize leader state
        {
            let mut leader_state = coordinator.leader_state.write().await;
            let mut ls = RaftLeaderState {
                next_index: std::collections::HashMap::new(),
                match_index: std::collections::HashMap::new(),
            };
            ls.next_index.insert("node1".to_string(), 1);
            ls.match_index.insert("node1".to_string(), 0);
            *leader_state = Some(ls);
        }

        let response = AppendEntriesResponse {
            term: 1,
            success: true,
            match_index: 3,
        };

        handle_append_entries_response(coordinator.clone(), "node1".to_string(), response)
            .await
            .unwrap();

        let leader_state = coordinator.leader_state.read().await;
        if let Some(ls) = leader_state.as_ref() {
            let match_idx = ls.match_index.get("node1").copied().unwrap_or(0);
            assert_eq!(match_idx, 3, "Should update match_index on success");
            
            let next_idx = ls.next_index.get("node1").copied().unwrap_or(1);
            assert_eq!(next_idx, 4, "Should update next_index to match_index + 1");
        }
    }

    #[tokio::test]
    async fn test_append_entries_response_handling_failure() {
        let coordinator = Arc::new(RaftCoordinator::new(
            "node0".to_string(),
            vec!["node1".to_string()],
            150,
            50,
        ));

        // Make this node the leader
        *coordinator.state.write().await = RaftState::Leader;

        // Initialize leader state
        {
            let mut leader_state = coordinator.leader_state.write().await;
            let mut ls = RaftLeaderState {
                next_index: std::collections::HashMap::new(),
                match_index: std::collections::HashMap::new(),
            };
            ls.next_index.insert("node1".to_string(), 5);
            ls.match_index.insert("node1".to_string(), 0);
            *leader_state = Some(ls);
        }

        let response = AppendEntriesResponse {
            term: 1,
            success: false,
            match_index: 0,
        };

        handle_append_entries_response(coordinator.clone(), "node1".to_string(), response)
            .await
            .unwrap();

        let leader_state = coordinator.leader_state.read().await;
        if let Some(ls) = leader_state.as_ref() {
            let next_idx = ls.next_index.get("node1").copied().unwrap_or(1);
            assert_eq!(next_idx, 4, "Should decrement next_index on failure");
        }
    }
}
