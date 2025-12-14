/// TombstoneManager: Efficient vector deletion with TTL support
///
/// Rather than immediately removing vectors (expensive in HNSW),
/// we mark them as "tombstoned" (deleted). During compaction,
/// tombstoned vectors are skipped.
///
/// Key properties:
/// - O(1) deletion (just mark as tombstoned)
/// - Cleanup happens during normal compaction
/// - Supports TTL-based automatic expiration
/// - Tracks deletion reasons and timestamps

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use parking_lot::RwLock;
use crate::core::errors::Result;

/// Reason for tombstoning a vector
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TombstoneReason {
    /// Explicitly deleted by user
    UserDelete,
    
    /// Expired due to TTL
    TTLExpired,
    
    /// Overwritten by newer version
    UpdateOverwrite,
}

/// A single tombstone entry
#[derive(Debug, Clone)]
pub struct TombstoneEntry {
    pub id: String,
    pub reason: TombstoneReason,
    pub timestamp: u64,  // When it was tombstoned
}

/// TombstoneManager: Manages vector deletion state
pub struct TombstoneManager {
    /// Tombstoned vectors: id -> TombstoneEntry
    tombstones: Arc<RwLock<HashMap<String, TombstoneEntry>>>,
    
    /// Stats
    total_tombstones: Arc<RwLock<u64>>,
    total_cleaned: Arc<RwLock<u64>>,
}

impl TombstoneManager {
    /// Create new tombstone manager
    pub fn new() -> Self {
        TombstoneManager {
            tombstones: Arc::new(RwLock::new(HashMap::new())),
            total_tombstones: Arc::new(RwLock::new(0)),
            total_cleaned: Arc::new(RwLock::new(0)),
        }
    }

    /// Mark a vector as deleted (O(1) operation)
    pub fn tombstone(&self, id: String, reason: TombstoneReason) -> Result<()> {
        let timestamp = Self::current_timestamp();
        
        let mut tombstones = self.tombstones.write();
        tombstones.insert(
            id,
            TombstoneEntry {
                id: String::new(), // Will be filled by key
                reason,
                timestamp,
            },
        );

        // Update stats
        *self.total_tombstones.write() += 1;

        Ok(())
    }

    /// Check if a vector is tombstoned
    pub fn is_tombstoned(&self, id: &str) -> bool {
        self.tombstones.read().contains_key(id)
    }

    /// Get tombstone entry for a vector (if exists)
    pub fn get_tombstone(&self, id: &str) -> Option<TombstoneEntry> {
        self.tombstones.read().get(id).cloned()
    }

    /// Bulk check for tombstones (for batch operations)
    pub fn filter_tombstones(&self, ids: &[String]) -> Vec<String> {
        let tombstones = self.tombstones.read();
        ids.iter()
            .filter(|id| !tombstones.contains_key(*id))
            .cloned()
            .collect()
    }

    /// Clean up tombstones that are older than cutoff_time
    /// 
    /// This is called during compaction to permanently remove old deletes.
    /// We keep tombstones around briefly for:
    /// - Replication lag tolerance
    /// - Cross-tenant visibility windows
    /// - Audit trail requirements
    pub fn cleanup_old_tombstones(&self, cutoff_timestamp: u64) -> usize {
        let mut tombstones = self.tombstones.write();
        let before_len = tombstones.len();
        
        tombstones.retain(|_, entry| entry.timestamp > cutoff_timestamp);
        
        let removed = before_len - tombstones.len();
        *self.total_cleaned.write() += removed as u64;
        
        removed
    }

    /// Get current tombstone count
    pub fn len(&self) -> usize {
        self.tombstones.read().len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.tombstones.read().is_empty()
    }

    /// Get all tombstones (for backup/recovery)
    pub fn get_all(&self) -> Vec<(String, TombstoneEntry)> {
        self.tombstones
            .read()
            .iter()
            .map(|(id, entry)| (id.clone(), entry.clone()))
            .collect()
    }

    /// Clear all tombstones (dangerous—use with care)
    pub fn clear_all(&self) {
        self.tombstones.write().clear();
    }

    /// Get statistics
    pub fn get_stats(&self) -> (u64, u64, usize) {
        (
            *self.total_tombstones.read(),  // Total tombstones ever created
            *self.total_cleaned.read(),     // Total tombstones cleaned
            self.len(),                     // Current tombstone count
        )
    }

    /// Current timestamp in milliseconds
    fn current_timestamp() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0)
    }
}

impl Default for TombstoneManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tombstone_manager_new() {
        let manager = TombstoneManager::new();
        assert!(manager.is_empty());
        assert_eq!(manager.len(), 0);
    }

    #[test]
    fn test_tombstone_single_vector() {
        let manager = TombstoneManager::new();
        
        assert!(manager.tombstone("v1".to_string(), TombstoneReason::UserDelete).is_ok());
        assert!(manager.is_tombstoned("v1"));
        assert!(!manager.is_tombstoned("v2"));
        
        assert_eq!(manager.len(), 1);
    }

    #[test]
    fn test_tombstone_multiple_vectors() {
        let manager = TombstoneManager::new();
        
        for i in 0..10 {
            assert!(
                manager.tombstone(format!("v{}", i), TombstoneReason::UserDelete).is_ok()
            );
        }
        
        assert_eq!(manager.len(), 10);
        
        for i in 0..10 {
            assert!(manager.is_tombstoned(&format!("v{}", i)));
        }
    }

    #[test]
    fn test_filter_tombstones() {
        let manager = TombstoneManager::new();
        
        // Tombstone v0, v2, v4
        for i in [0, 2, 4].iter() {
            let _ = manager.tombstone(format!("v{}", i), TombstoneReason::UserDelete);
        }
        
        // Filter: should keep v1, v3, v5
        let ids = vec!["v0", "v1", "v2", "v3", "v4", "v5"]
            .into_iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>();
        
        let filtered = manager.filter_tombstones(&ids);
        assert_eq!(filtered.len(), 3);
        assert!(filtered.contains(&"v1".to_string()));
        assert!(filtered.contains(&"v3".to_string()));
        assert!(filtered.contains(&"v5".to_string()));
    }

    #[test]
    fn test_get_tombstone() {
        let manager = TombstoneManager::new();
        
        let _ = manager.tombstone("v1".to_string(), TombstoneReason::TTLExpired);
        
        let entry = manager.get_tombstone("v1");
        assert!(entry.is_some());
        assert_eq!(entry.unwrap().reason, TombstoneReason::TTLExpired);
    }

    #[test]
    fn test_cleanup_old_tombstones() {
        let manager = TombstoneManager::new();
        
        let now = TombstoneManager::current_timestamp();
        
        // Add some tombstones
        for i in 0..5 {
            let _ = manager.tombstone(format!("v{}", i), TombstoneReason::UserDelete);
        }
        
        assert_eq!(manager.len(), 5);
        
        // Clean tombstones older than now (should remove all)
        let removed = manager.cleanup_old_tombstones(now);
        assert_eq!(removed, 5);
        assert!(manager.is_empty());
    }

    #[test]
    fn test_tombstone_reason_variants() {
        let manager = TombstoneManager::new();
        
        let _ = manager.tombstone("v1".to_string(), TombstoneReason::UserDelete);
        let _ = manager.tombstone("v2".to_string(), TombstoneReason::TTLExpired);
        let _ = manager.tombstone("v3".to_string(), TombstoneReason::UpdateOverwrite);
        
        assert_eq!(manager.get_tombstone("v1").unwrap().reason, TombstoneReason::UserDelete);
        assert_eq!(manager.get_tombstone("v2").unwrap().reason, TombstoneReason::TTLExpired);
        assert_eq!(manager.get_tombstone("v3").unwrap().reason, TombstoneReason::UpdateOverwrite);
    }

    #[test]
    fn test_tombstone_stats() {
        let manager = TombstoneManager::new();
        
        for i in 0..5 {
            let _ = manager.tombstone(format!("v{}", i), TombstoneReason::UserDelete);
        }
        
        let (total, cleaned, current) = manager.get_stats();
        assert_eq!(total, 5);
        assert_eq!(cleaned, 0);
        assert_eq!(current, 5);
        
        // Cleanup old tombstones
        let now = TombstoneManager::current_timestamp();
        let _ = manager.cleanup_old_tombstones(now);
        
        let (total, cleaned, current) = manager.get_stats();
        assert_eq!(total, 5);
        assert_eq!(cleaned, 5);
        assert_eq!(current, 0);
    }
}
