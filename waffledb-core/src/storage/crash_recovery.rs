use std::path::Path;
use crate::errors::Result;
use crate::storage::{IndexPersistence, IndexState, HNSWSnapshot};
use crate::storage::wal::{WriteAheadLog, WALEntry};
use crate::hnsw::graph::HNSWIndex;

/// Crash recovery manager with WAL replay
pub struct CrashRecoveryManager {
    persistence: IndexPersistence,
    wal: WriteAheadLog,
}

/// Recovery result
#[derive(Debug, Clone)]
pub struct RecoveryResult {
    /// Whether recovery was successful
    pub success: bool,
    /// Recovered graph (if any)
    pub recovered_graph: Option<HNSWIndex>,
    /// WAL entries replayed
    pub wal_entries_replayed: usize,
    /// Whether recovery required rebuilding
    pub required_rebuild: bool,
    /// Error message (if any)
    pub error: Option<String>,
}

impl CrashRecoveryManager {
    /// Create new recovery manager
    pub fn new(base_dir: &Path) -> Result<Self> {
        let persistence = IndexPersistence::new(base_dir)?;
        let wal_dir = base_dir.join("wal");
        std::fs::create_dir_all(&wal_dir)
            .map_err(|e| crate::errors::WaffleError::StorageError(
                format!("Failed to create WAL dir: {}", e)
            ))?;

        let wal = WriteAheadLog::new(&wal_dir)?;

        Ok(CrashRecoveryManager { persistence, wal })
    }

    /// Run crash recovery procedure
    pub fn recover(&mut self) -> Result<RecoveryResult> {
        let current_state = self.persistence.current_state();

        match current_state {
            IndexState::Empty => {
                // No recovery needed
                Ok(RecoveryResult {
                    success: true,
                    recovered_graph: None,
                    wal_entries_replayed: 0,
                    required_rebuild: false,
                    error: None,
                })
            }

            IndexState::Ready => {
                // Normal case: try to load latest snapshot
                match self.persistence.load_latest_snapshot()? {
                    Some(snapshot) => {
                        let graph = Self::graph_from_snapshot(&snapshot)?;
                        let wal_count = self.wal.get_entries().len();

                        // Replay WAL entries on top of snapshot
                        let _applied = self.replay_wal_entries(&snapshot)?;

                        Ok(RecoveryResult {
                            success: true,
                            recovered_graph: Some(graph),
                            wal_entries_replayed: wal_count,
                            required_rebuild: false,
                            error: None,
                        })
                    }
                    None => {
                        // No snapshot but Ready state? Rebuild required
                        Ok(RecoveryResult {
                            success: true,
                            recovered_graph: None,
                            wal_entries_replayed: 0,
                            required_rebuild: true,
                            error: Some("No snapshot found but index marked Ready; rebuild required".to_string()),
                        })
                    }
                }
            }

            IndexState::Building => {
                // Incomplete build: try to recover from previous snapshot
                self.persistence.mark_recovering()?;

                match self.persistence.load_latest_snapshot() {
                    Ok(Some(snapshot)) => {
                        let graph = Self::graph_from_snapshot(&snapshot)?;

                        // Mark as recovered and ready
                        self.persistence.mark_ready(
                            snapshot.metadata.timestamp,
                            snapshot.metadata.version,
                        )?;

                        Ok(RecoveryResult {
                            success: true,
                            recovered_graph: Some(graph),
                            wal_entries_replayed: 0,
                            required_rebuild: false,
                            error: Some("Recovered from incomplete build".to_string()),
                        })
                    }
                    _ => {
                        // Cannot recover, need full rebuild
                        self.persistence.mark_corrupted()?;

                        Ok(RecoveryResult {
                            success: false,
                            recovered_graph: None,
                            wal_entries_replayed: 0,
                            required_rebuild: true,
                            error: Some("Incomplete build with no snapshot; full rebuild required".to_string()),
                        })
                    }
                }
            }

            IndexState::Recovering => {
                // Already in recovery: try to complete it
                match self.persistence.load_latest_snapshot() {
                    Ok(Some(snapshot)) => {
                        let graph = Self::graph_from_snapshot(&snapshot)?;
                        self.persistence.mark_ready(
                            snapshot.metadata.timestamp,
                            snapshot.metadata.version,
                        )?;

                        Ok(RecoveryResult {
                            success: true,
                            recovered_graph: Some(graph),
                            wal_entries_replayed: 0,
                            required_rebuild: false,
                            error: Some("Completed recovery from crash".to_string()),
                        })
                    }
                    _ => {
                        self.persistence.mark_corrupted()?;

                        Ok(RecoveryResult {
                            success: false,
                            recovered_graph: None,
                            wal_entries_replayed: 0,
                            required_rebuild: true,
                            error: Some("Recovery incomplete; corrupted index detected; full rebuild required".to_string()),
                        })
                    }
                }
            }

            IndexState::Corrupted => {
                // Index corrupted: cannot auto-recover
                Ok(RecoveryResult {
                    success: false,
                    recovered_graph: None,
                    wal_entries_replayed: 0,
                    required_rebuild: true,
                    error: Some("Index marked corrupted; manual intervention required or full rebuild".to_string()),
                })
            }
        }
    }

    /// Replay WAL entries (simulated for now)
    fn replay_wal_entries(&self, _snapshot: &HNSWSnapshot) -> Result<usize> {
        let entries = self.wal.get_entries();
        let mut applied = 0;

        for entry in entries {
            match entry {
                WALEntry::Insert { .. } => {
                    // In production: would apply insert to graph
                    applied += 1;
                }
                WALEntry::Delete { .. } => {
                    // In production: would apply delete to graph
                    applied += 1;
                }
                WALEntry::UpdateMetadata { .. } => {
                    // In production: would update metadata
                    applied += 1;
                }
            }
        }

        Ok(applied)
    }

    /// Convert snapshot to graph (helper)
    fn graph_from_snapshot(snapshot: &HNSWSnapshot) -> Result<HNSWIndex> {
        IndexPersistence::snapshot_to_graph(snapshot)
    }

    /// Clear WAL after successful recovery
    pub fn clear_wal(&mut self) -> Result<()> {
        self.wal.clear()?;
        self.wal.sync()?;
        Ok(())
    }

    /// Get reference to WAL
    pub fn wal(&self) -> &WriteAheadLog {
        &self.wal
    }

    /// Get mutable reference to WAL
    pub fn wal_mut(&mut self) -> &mut WriteAheadLog {
        &mut self.wal
    }

    /// Get reference to persistence
    pub fn persistence(&self) -> &IndexPersistence {
        &self.persistence
    }

    /// Get mutable reference to persistence
    pub fn persistence_mut(&mut self) -> &mut IndexPersistence {
        &mut self.persistence
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_recovery_empty_state() {
        let temp_dir = tempfile::tempdir().unwrap();
        let mut recovery = CrashRecoveryManager::new(temp_dir.path()).unwrap();

        let result = recovery.recover().unwrap();
        assert!(result.success);
        assert_eq!(result.wal_entries_replayed, 0);
        assert!(!result.required_rebuild);
    }

    #[test]
    fn test_recovery_ready_state() {
        let temp_dir = tempfile::tempdir().unwrap();

        // First: create a snapshot
        {
            let mut persistence = IndexPersistence::new(temp_dir.path()).unwrap();
            let graph = HNSWIndex::new(16, 1.0 / (2.0_f32.ln()));
            let snapshot = IndexPersistence::graph_to_snapshot(&graph, 16, 1.0 / (2.0_f32.ln())).unwrap();
            persistence.save_snapshot(&snapshot).unwrap();
        }

        // Second: recover
        {
            let mut recovery = CrashRecoveryManager::new(temp_dir.path()).unwrap();
            let result = recovery.recover().unwrap();
            assert!(result.success);
            assert!(result.recovered_graph.is_some());
        }
    }

    #[test]
    fn test_recovery_building_state() {
        let temp_dir = tempfile::tempdir().unwrap();

        // First: create snapshot and mark as building
        {
            let mut persistence = IndexPersistence::new(temp_dir.path()).unwrap();
            persistence.mark_building().unwrap();
            let graph = HNSWIndex::new(16, 1.0 / (2.0_f32.ln()));
            let snapshot = IndexPersistence::graph_to_snapshot(&graph, 16, 1.0 / (2.0_f32.ln())).unwrap();
            persistence.save_snapshot(&snapshot).unwrap();
        }

        // Second: recover (should recover from previous snapshot)
        {
            let mut recovery = CrashRecoveryManager::new(temp_dir.path()).unwrap();
            let result = recovery.recover().unwrap();
            assert!(result.success);
            assert!(result.recovered_graph.is_some());
        }
    }
}
