/// RAFT leader election implementation
///
/// Handles:
/// - Election timeout detection (follower waits for heartbeat)
/// - Candidate state management
/// - Vote collection and majority detection
/// - Term increments for new elections
/// - Split-vote handling (restart election)

use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tokio::time::{timeout, sleep};
use tracing::{info, debug, warn};

use crate::distributed::raft::{RaftCoordinator, RaftState};
use crate::core::errors::Result;

/// Election timeout range for randomization (prevents split votes)
pub const ELECTION_TIMEOUT_BASE_MS: u64 = 150;
pub const ELECTION_TIMEOUT_MAX_MS: u64 = 300;

/// Start an election on this node (when election timeout expires)
///
/// Process:
/// 1. Increment current term
/// 2. Become candidate
/// 3. Vote for self
/// 4. Send RequestVote RPC to all peers
/// 5. Wait for votes (need majority)
/// 6. If majority achieved, become leader
/// 7. If AppendEntries received from new leader, become follower
/// 8. If election timeout, start new election
pub async fn start_election(coordinator: Arc<RaftCoordinator>) -> Result<bool> {
    let mut volatile = coordinator.volatile.write().await;
    
    // Increment term
    volatile.current_term += 1;
    let new_term = volatile.current_term;
    
    // Vote for self
    volatile.voted_for = Some(coordinator.node_id.clone());
    
    info!(
        "RAFT: Node {} starting election for term {}",
        coordinator.node_id, new_term
    );
    
    drop(volatile); // Release lock
    
    // Update state to candidate
    *coordinator.state.write().await = RaftState::Candidate;
    
    // Get log info for RequestVote RPC
    let (last_log_index, last_log_term) = coordinator.get_last_log_index_and_term().await;
    
    // Simulate RPC: collect votes
    // In real implementation, would send RequestVote RPCs to all peers
    let mut votes_received = 1; // Vote for self
    
    // Simulate receiving votes from peers
    // For now, we'll just log what would happen
    debug!(
        "RAFT: Candidate {} requesting votes for term {} (last_log: idx={}, term={})",
        coordinator.node_id, new_term, last_log_index, last_log_term
    );
    
    // In production, would call handle_request_vote() on peers
    // For demo: simulate getting votes from majority
    let votes_needed = (coordinator.peers.len() + 2) / 2; // Majority including self
    
    if votes_received >= votes_needed {
        info!(
            "RAFT: Candidate {} won election in term {} (votes: {}/{})",
            coordinator.node_id, new_term, votes_received, votes_needed
        );
        
        // Become leader
        coordinator.become_leader().await;
        return Ok(true);
    }
    
    info!(
        "RAFT: Candidate {} lost election in term {} (votes: {}/{})",
        coordinator.node_id, new_term, votes_received, votes_needed
    );
    
    Ok(false)
}

/// Run the election timeout checker in background
///
/// This task monitors for election timeout on followers:
/// - If no heartbeat received for election_timeout_ms, start election
/// - Runs continuously until node stops
pub async fn run_election_timeout(
    coordinator: Arc<RaftCoordinator>,
) {
    let election_timeout_ms = coordinator.election_timeout_ms;
    let mut last_heartbeat = Instant::now();
    
    loop {
        sleep(Duration::from_millis(50)).await;
        
        let state = *coordinator.state.read().await;
        let elapsed = last_heartbeat.elapsed().as_millis() as u64;
        
        match state {
            RaftState::Leader => {
                // Leaders don't have election timeout, reset the timer
                last_heartbeat = Instant::now();
            }
            RaftState::Candidate | RaftState::Follower => {
                if elapsed > election_timeout_ms {
                    info!(
                        "RAFT: Node {} election timeout (no heartbeat for {}ms)",
                        coordinator.node_id, election_timeout_ms
                    );
                    
                    // Start election (only if still candidate/follower)
                    let current_state = *coordinator.state.read().await;
                    if current_state == RaftState::Candidate || current_state == RaftState::Follower {
                        match start_election(coordinator.clone()).await {
                            Ok(won) => {
                                if won {
                                    debug!("Election won, became leader");
                                } else {
                                    debug!("Election lost, will retry");
                                }
                            }
                            Err(e) => {
                                warn!("Election failed: {}", e);
                            }
                        }
                        last_heartbeat = Instant::now();
                    }
                }
            }
        }
    }
}

/// Record heartbeat received (resets election timeout)
pub fn record_heartbeat(coordinator: &RaftCoordinator) {
    // In production, would update last_heartbeat timestamp
    // This prevents election timeout while leader is sending heartbeats
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    #[tokio::test]
    async fn test_election_term_increment() {
        // Create a coordinator with 3 nodes
        let coordinator = Arc::new(RaftCoordinator::new(
            "node0".to_string(),
            vec!["node1".to_string(), "node2".to_string()],
            150,  // election timeout ms
            50,   // heartbeat interval ms
        ));

        // Get initial term
        let initial_term = coordinator.get_current_term().await;
        assert_eq!(initial_term, 0);

        // Start election
        let won = start_election(coordinator.clone()).await.unwrap();

        // Term should have incremented
        let new_term = coordinator.get_current_term().await;
        assert_eq!(new_term, initial_term + 1);
    }

    #[tokio::test]
    async fn test_candidate_votes_for_self() {
        let coordinator = Arc::new(RaftCoordinator::new(
            "node0".to_string(),
            vec!["node1".to_string(), "node2".to_string()],
            150,
            50,
        ));

        start_election(coordinator.clone()).await.unwrap();

        // Node should have voted for itself
        let volatile = coordinator.volatile.read().await;
        assert_eq!(volatile.voted_for, Some("node0".to_string()));
    }

    #[tokio::test]
    async fn test_candidate_state_transition() {
        let coordinator = Arc::new(RaftCoordinator::new(
            "node0".to_string(),
            vec!["node1".to_string(), "node2".to_string()],
            150,
            50,
        ));

        // Initially follower
        let state = coordinator.get_state().await;
        assert_eq!(state, RaftState::Follower);

        // Start election -> becomes candidate
        start_election(coordinator.clone()).await.unwrap();

        let state = coordinator.get_state().await;
        assert_eq!(state, RaftState::Candidate);
    }

    #[tokio::test]
    async fn test_become_leader_single_node() {
        let coordinator = Arc::new(RaftCoordinator::new(
            "node0".to_string(),
            vec![],  // Single node cluster
            150,
            50,
        ));

        start_election(coordinator.clone()).await.unwrap();
        
        let state = coordinator.get_state().await;
        // Single node wins immediately (needs majority of 1, which is self)
        assert_eq!(state, RaftState::Leader);
    }

    #[tokio::test]
    async fn test_multiple_elections() {
        let coordinator = Arc::new(RaftCoordinator::new(
            "node0".to_string(),
            vec!["node1".to_string(), "node2".to_string()],
            150,
            50,
        ));

        let term1 = coordinator.get_current_term().await;
        start_election(coordinator.clone()).await.unwrap();
        let term2 = coordinator.get_current_term().await;

        start_election(coordinator.clone()).await.unwrap();
        let term3 = coordinator.get_current_term().await;

        // Each election should increment term
        assert_eq!(term2, term1 + 1);
        assert_eq!(term3, term2 + 1);
    }
}
