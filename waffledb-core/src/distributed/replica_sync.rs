/// Replica Synchronization Module
/// 
/// Handles synchronization of replicas with the leader:
/// - Snapshot installation for large log gaps
/// - Incremental log replication
/// - Replica health checks
/// - Network partition resilience

use std::collections::HashMap;
use std::time::Instant;
use serde::{Serialize, Deserialize};
use crate::core::errors::{Result, WaffleError, ErrorCode};

/// Configuration for replica synchronization
#[derive(Debug, Clone)]
pub struct SyncConfig {
    /// Max entries to send per AppendEntries RPC
    pub max_entries_per_batch: usize,
    /// Snapshot transfer size (in number of entries per chunk)
    pub snapshot_chunk_size: usize,
    /// Timeout for sync operations in milliseconds
    pub sync_timeout_ms: u64,
    /// Max retries for failed sync operations
    pub max_retries: u32,
    /// Enable incremental catch-up after snapshot
    pub enable_incremental_catchup: bool,
}

impl Default for SyncConfig {
    fn default() -> Self {
        Self {
            max_entries_per_batch: 100,
            snapshot_chunk_size: 1000,
            sync_timeout_ms: 10000,
            max_retries: 3,
            enable_incremental_catchup: true,
        }
    }
}

/// Status of replica synchronization
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SyncStatus {
    /// Replica is in sync with leader
    InSync,
    /// Replica is catching up on log entries
    CatchingUpLog,
    /// Replica needs snapshot installation
    NeedsSnapshot,
    /// Snapshot installation in progress
    InstallingSnapshot,
    /// Sync failed temporarily
    Failed,
    /// Replica is unreachable
    Unreachable,
}

/// Per-replica synchronization state
#[derive(Debug, Clone)]
pub struct ReplicaSyncState {
    /// Replica node ID
    pub replica_id: String,
    /// Current sync status
    pub status: SyncStatus,
    /// Last log index replicated to this replica
    pub replicated_index: u64,
    /// Last known match index for this replica
    pub match_index: u64,
    /// Current term
    pub term: u64,
    /// Snapshot being installed
    pub snapshot_id: Option<String>,
    /// Snapshot installation progress (0-100)
    pub snapshot_progress: u8,
    /// Number of failed sync attempts
    pub failed_attempts: u32,
    /// Last successful sync time
    pub last_sync_time: Option<Instant>,
}

impl ReplicaSyncState {
    /// Create new replica sync state
    pub fn new(replica_id: String) -> Self {
        Self {
            replica_id,
            status: SyncStatus::CatchingUpLog,
            replicated_index: 0,
            match_index: 0,
            term: 0,
            snapshot_id: None,
            snapshot_progress: 0,
            failed_attempts: 0,
            last_sync_time: None,
        }
    }

    /// Mark sync as successful
    pub fn mark_success(&mut self, replicated_index: u64) {
        self.replicated_index = replicated_index;
        if self.replicated_index == self.match_index {
            self.status = SyncStatus::InSync;
        }
        self.last_sync_time = Some(Instant::now());
        self.failed_attempts = 0;
    }

    /// Mark sync as failed
    pub fn mark_failed(&mut self) {
        self.failed_attempts += 1;
        self.status = SyncStatus::Failed;
    }

    /// Check if replica needs snapshot
    pub fn needs_snapshot(&self, leader_last_snapshot: u64) -> bool {
        self.replicated_index < leader_last_snapshot
    }

    /// Check if replica is in sync
    pub fn is_in_sync(&self) -> bool {
        self.status == SyncStatus::InSync && self.failed_attempts == 0
    }

    /// Check if sync has timed out
    pub fn sync_timeout(&self, timeout_ms: u64) -> bool {
        match self.last_sync_time {
            Some(last_time) => last_time.elapsed().as_millis() as u64 > timeout_ms,
            None => true, // Never synced = timed out
        }
    }
}

/// Request for snapshot installation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstallSnapshotRequest {
    /// Leader's current term
    pub term: u64,
    /// Leader node ID
    pub leader_id: String,
    /// Snapshot metadata
    pub snapshot_id: String,
    /// Index of last included entry in snapshot
    pub last_included_index: u64,
    /// Term of last included entry in snapshot
    pub last_included_term: u64,
    /// Snapshot data chunk
    pub data: Vec<u8>,
    /// True if more chunks coming
    pub more_chunks: bool,
    /// Offset for this chunk (used for reassembly)
    pub offset: u64,
}

/// Response to snapshot installation request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstallSnapshotResponse {
    /// Current term (for leader to update itself)
    pub term: u64,
    /// True if snapshot chunk accepted
    pub accepted: bool,
}

/// Replica synchronization manager
pub struct ReplicaSyncManager {
    config: SyncConfig,
    /// Per-replica sync state
    replicas: HashMap<String, ReplicaSyncState>,
    /// Leader's last snapshot info
    leader_last_snapshot_index: u64,
    leader_last_snapshot_term: u64,
}

impl ReplicaSyncManager {
    /// Create new sync manager
    pub fn new(config: SyncConfig) -> Self {
        Self {
            config,
            replicas: HashMap::new(),
            leader_last_snapshot_index: 0,
            leader_last_snapshot_term: 0,
        }
    }

    /// Register a new replica
    pub fn register_replica(&mut self, replica_id: String) {
        self.replicas
            .entry(replica_id.clone())
            .or_insert_with(|| ReplicaSyncState::new(replica_id));
    }

    /// Get sync state for a replica
    pub fn get_replica_state(&self, replica_id: &str) -> Option<&ReplicaSyncState> {
        self.replicas.get(replica_id)
    }

    /// Get mutable sync state for a replica
    pub fn get_replica_state_mut(&mut self, replica_id: &str) -> Option<&mut ReplicaSyncState> {
        self.replicas.get_mut(replica_id)
    }

    /// Update replica with successful sync
    pub fn mark_replica_synced(&mut self, replica_id: &str, replicated_index: u64) -> Result<()> {
        let state = self
            .replicas
            .get_mut(replica_id)
            .ok_or_else(|| WaffleError::WithCode {
                code: ErrorCode::DistributedModeError,
                message: format!("Replica {} not registered", replica_id),
            })?;

        state.mark_success(replicated_index);
        Ok(())
    }

    /// Update replica with failed sync
    pub fn mark_replica_failed(&mut self, replica_id: &str) -> Result<()> {
        let state = self
            .replicas
            .get_mut(replica_id)
            .ok_or_else(|| WaffleError::WithCode {
                code: ErrorCode::DistributedModeError,
                message: format!("Replica {} not registered", replica_id),
            })?;

        state.mark_failed();
        Ok(())
    }

    /// Determine next sync action for a replica
    /// 
    /// Returns the action leader should take:
    /// - CatchingUpLog: send log entries via AppendEntries
    /// - NeedsSnapshot: send snapshot via InstallSnapshot
    /// - InSync: no action needed
    pub fn determine_next_action(&self, replica_id: &str) -> Result<SyncStatus> {
        let state = self
            .replicas
            .get(replica_id)
            .ok_or_else(|| WaffleError::WithCode {
                code: ErrorCode::DistributedModeError,
                message: format!("Replica {} not registered", replica_id),
            })?;

        // Check if needs snapshot
        if state.needs_snapshot(self.leader_last_snapshot_index) {
            return Ok(SyncStatus::NeedsSnapshot);
        }

        // Check if in sync
        if state.is_in_sync() {
            return Ok(SyncStatus::InSync);
        }

        // Default: catch up on log
        Ok(SyncStatus::CatchingUpLog)
    }

    /// Calculate how many entries to send in next AppendEntries
    pub fn entries_to_send(&self, replica_id: &str, leader_last_index: u64) -> Result<usize> {
        let state = self
            .replicas
            .get(replica_id)
            .ok_or_else(|| WaffleError::WithCode {
                code: ErrorCode::DistributedModeError,
                message: format!("Replica {} not registered", replica_id),
            })?;

        let remaining = leader_last_index.saturating_sub(state.replicated_index);
        Ok(std::cmp::min(
            remaining as usize,
            self.config.max_entries_per_batch,
        ))
    }

    /// Update leader snapshot info
    pub fn set_leader_snapshot(&mut self, snapshot_index: u64, snapshot_term: u64) {
        self.leader_last_snapshot_index = snapshot_index;
        self.leader_last_snapshot_term = snapshot_term;
    }

    /// Get all replicas that are in sync
    pub fn get_in_sync_replicas(&self) -> Vec<String> {
        self.replicas
            .iter()
            .filter(|(_, state)| state.is_in_sync())
            .map(|(id, _)| id.clone())
            .collect()
    }

    /// Get all replicas that need attention
    pub fn get_unhealthy_replicas(&self) -> Vec<String> {
        self.replicas
            .iter()
            .filter(|(_, state)| !state.is_in_sync())
            .map(|(id, _)| id.clone())
            .collect()
    }

    /// Check if quorum is in sync (majority including leader)
    pub fn is_quorum_in_sync(&self, total_nodes: usize) -> bool {
        let in_sync_count = self
            .replicas
            .values()
            .filter(|state| state.is_in_sync())
            .count();

        // Quorum = ceil((total_nodes + 1) / 2)
        // In sync count includes leader + replicas
        let quorum_size = (total_nodes + 2) / 2;
        in_sync_count + 1 >= quorum_size // +1 for leader itself
    }

    /// Step function for periodic sync checks
    /// 
    /// Returns list of replicas that need attention
    pub fn sync_step(&mut self, timeout_ms: u64) -> Result<Vec<String>> {
        let mut need_attention = Vec::new();

        for (replica_id, state) in &mut self.replicas {
            if state.sync_timeout(timeout_ms) {
                need_attention.push(replica_id.clone());
            }
        }

        Ok(need_attention)
    }

    /// Install snapshot on replica
    pub fn start_snapshot_installation(
        &mut self,
        replica_id: &str,
        snapshot_id: String,
        last_included_index: u64,
    ) -> Result<()> {
        let state = self
            .replicas
            .get_mut(replica_id)
            .ok_or_else(|| WaffleError::WithCode {
                code: ErrorCode::DistributedModeError,
                message: format!("Replica {} not registered", replica_id),
            })?;

        state.status = SyncStatus::InstallingSnapshot;
        state.snapshot_id = Some(snapshot_id);
        state.snapshot_progress = 0;
        state.replicated_index = last_included_index;

        Ok(())
    }

    /// Update snapshot installation progress
    pub fn update_snapshot_progress(
        &mut self,
        replica_id: &str,
        progress: u8,
    ) -> Result<()> {
        let state = self
            .replicas
            .get_mut(replica_id)
            .ok_or_else(|| WaffleError::WithCode {
                code: ErrorCode::DistributedModeError,
                message: format!("Replica {} not registered", replica_id),
            })?;

        state.snapshot_progress = std::cmp::min(progress, 100);

        if progress == 100 {
            state.status = SyncStatus::CatchingUpLog;
        }

        Ok(())
    }

    /// Handle snapshot installation response
    pub fn handle_snapshot_response(
        &mut self,
        replica_id: &str,
        response: &InstallSnapshotResponse,
    ) -> Result<()> {
        let state = self
            .replicas
            .get_mut(replica_id)
            .ok_or_else(|| WaffleError::WithCode {
                code: ErrorCode::DistributedModeError,
                message: format!("Replica {} not registered", replica_id),
            })?;

        if response.accepted {
            state.mark_success(state.replicated_index);
        } else {
            state.mark_failed();
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_replica_sync_state_creation() {
        let state = ReplicaSyncState::new("replica1".to_string());
        assert_eq!(state.replica_id, "replica1");
        assert_eq!(state.status, SyncStatus::CatchingUpLog);
        assert_eq!(state.replicated_index, 0);
        assert_eq!(state.failed_attempts, 0);
    }

    #[test]
    fn test_mark_replica_success() {
        let mut state = ReplicaSyncState::new("replica1".to_string());
        state.match_index = 10;

        state.mark_success(10);
        assert_eq!(state.status, SyncStatus::InSync);
        assert_eq!(state.replicated_index, 10);
        assert_eq!(state.failed_attempts, 0);
    }

    #[test]
    fn test_mark_replica_failed() {
        let mut state = ReplicaSyncState::new("replica1".to_string());

        state.mark_failed();
        assert_eq!(state.status, SyncStatus::Failed);
        assert_eq!(state.failed_attempts, 1);

        state.mark_failed();
        assert_eq!(state.failed_attempts, 2);
    }

    #[test]
    fn test_needs_snapshot() {
        let state = ReplicaSyncState::new("replica1".to_string());
        assert!(state.needs_snapshot(10)); // Last snapshot at 10, replica at 0

        let mut state2 = ReplicaSyncState::new("replica2".to_string());
        state2.replicated_index = 15;
        assert!(!state2.needs_snapshot(10)); // Replica is ahead of snapshot
    }

    #[test]
    fn test_replica_sync_manager_register() {
        let config = SyncConfig::default();
        let mut manager = ReplicaSyncManager::new(config);

        manager.register_replica("replica1".to_string());
        manager.register_replica("replica2".to_string());

        assert!(manager.get_replica_state("replica1").is_some());
        assert!(manager.get_replica_state("replica2").is_some());
    }

    #[test]
    fn test_mark_replica_synced() {
        let config = SyncConfig::default();
        let mut manager = ReplicaSyncManager::new(config);
        manager.register_replica("replica1".to_string());

        manager.mark_replica_synced("replica1", 50).unwrap();

        let state = manager.get_replica_state("replica1").unwrap();
        assert_eq!(state.replicated_index, 50);
    }

    #[test]
    fn test_determine_next_action() {
        let config = SyncConfig::default();
        let mut manager = ReplicaSyncManager::new(config);
        manager.register_replica("replica1".to_string());

        // Default: catching up on log
        let action = manager.determine_next_action("replica1").unwrap();
        assert_eq!(action, SyncStatus::CatchingUpLog);

        // After syncing to match_index
        let state = manager.get_replica_state_mut("replica1").unwrap();
        state.match_index = 100;
        state.mark_success(100);

        let action = manager.determine_next_action("replica1").unwrap();
        assert_eq!(action, SyncStatus::InSync);

        // With snapshot gap
        manager.set_leader_snapshot(200, 5);
        let state = manager.get_replica_state_mut("replica1").unwrap();
        state.replicated_index = 50;
        state.status = SyncStatus::CatchingUpLog;

        let action = manager.determine_next_action("replica1").unwrap();
        assert_eq!(action, SyncStatus::NeedsSnapshot);
    }

    #[test]
    fn test_entries_to_send() {
        let config = SyncConfig::default();
        let max_batch = config.max_entries_per_batch;
        let mut manager = ReplicaSyncManager::new(config);
        manager.register_replica("replica1".to_string());

        // Need to send 50 entries (less than max batch)
        let entries = manager.entries_to_send("replica1", 50).unwrap();
        assert_eq!(entries, 50);

        // Sync the replica
        manager.mark_replica_synced("replica1", 50).unwrap();

        // No more entries to send
        let entries = manager.entries_to_send("replica1", 50).unwrap();
        assert_eq!(entries, 0);

        // Need to send many entries (limited by max batch)
        let entries = manager.entries_to_send("replica1", 500).unwrap();
        assert_eq!(entries, max_batch);
    }

    #[test]
    fn test_get_in_sync_replicas() {
        let config = SyncConfig::default();
        let mut manager = ReplicaSyncManager::new(config);
        manager.register_replica("replica1".to_string());
        manager.register_replica("replica2".to_string());
        manager.register_replica("replica3".to_string());

        // Sync replica1 and replica2
        manager.mark_replica_synced("replica1", 10).unwrap();
        manager.mark_replica_synced("replica2", 10).unwrap();

        let in_sync = manager.get_in_sync_replicas();
        assert_eq!(in_sync.len(), 0); // match_index not set, so not truly in sync
    }

    #[test]
    fn test_quorum_in_sync() {
        let config = SyncConfig::default();
        let mut manager = ReplicaSyncManager::new(config);

        // 3-node cluster
        manager.register_replica("replica1".to_string());
        manager.register_replica("replica2".to_string());

        // Need 2 out of 3 in sync (including leader)
        // Initially: 0 replicas in sync, so 1 (leader) out of 3 = not quorum
        assert!(!manager.is_quorum_in_sync(3));

        // Get replica1 in sync
        let state = manager.get_replica_state_mut("replica1").unwrap();
        state.match_index = 10;
        state.mark_success(10);

        // Now 1 replica + leader = 2 out of 3 = quorum
        assert!(manager.is_quorum_in_sync(3));
    }

    #[test]
    fn test_start_snapshot_installation() {
        let config = SyncConfig::default();
        let mut manager = ReplicaSyncManager::new(config);
        manager.register_replica("replica1".to_string());

        manager
            .start_snapshot_installation("replica1", "snap1".to_string(), 100)
            .unwrap();

        let state = manager.get_replica_state("replica1").unwrap();
        assert_eq!(state.status, SyncStatus::InstallingSnapshot);
        assert_eq!(state.snapshot_id, Some("snap1".to_string()));
        assert_eq!(state.snapshot_progress, 0);
        assert_eq!(state.replicated_index, 100);
    }

    #[test]
    fn test_update_snapshot_progress() {
        let config = SyncConfig::default();
        let mut manager = ReplicaSyncManager::new(config);
        manager.register_replica("replica1".to_string());

        manager
            .start_snapshot_installation("replica1", "snap1".to_string(), 100)
            .unwrap();

        manager.update_snapshot_progress("replica1", 50).unwrap();
        let state = manager.get_replica_state("replica1").unwrap();
        assert_eq!(state.snapshot_progress, 50);

        manager.update_snapshot_progress("replica1", 100).unwrap();
        let state = manager.get_replica_state("replica1").unwrap();
        assert_eq!(state.snapshot_progress, 100);
        assert_eq!(state.status, SyncStatus::CatchingUpLog);
    }

    #[test]
    fn test_sync_step_detects_timeout() {
        let config = SyncConfig::default();
        let mut manager = ReplicaSyncManager::new(config);
        manager.register_replica("replica1".to_string());

        // Initially no timeout (last_sync_time is None)
        let need_attention = manager.sync_step(1000).unwrap();
        assert_eq!(need_attention.len(), 1); // "replica1" needs attention

        // After sync
        manager.mark_replica_synced("replica1", 10).unwrap();

        // Still no timeout if just synced
        let need_attention = manager.sync_step(1000).unwrap();
        assert_eq!(need_attention.len(), 0);
    }

    #[test]
    fn test_unhealthy_replicas() {
        let config = SyncConfig::default();
        let mut manager = ReplicaSyncManager::new(config);
        manager.register_replica("replica1".to_string());
        manager.register_replica("replica2".to_string());

        // Both healthy
        let unhealthy = manager.get_unhealthy_replicas();
        assert_eq!(unhealthy.len(), 2); // Both catching up = unhealthy

        // Mark one as healthy
        let state = manager.get_replica_state_mut("replica1").unwrap();
        state.match_index = 10;
        state.mark_success(10);

        let unhealthy = manager.get_unhealthy_replicas();
        assert_eq!(unhealthy.len(), 1);
        assert_eq!(unhealthy[0], "replica2");
    }
}
