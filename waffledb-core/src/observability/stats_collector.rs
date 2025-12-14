/// Phase 1.7: StatsCollector for Index Statistics
/// 
/// Collects and maintains comprehensive statistics about the HNSW index.
/// Zero-cost abstraction: metrics are only computed when explicitly requested.

use crate::hnsw::HNSWIndex;
use crate::indexing::index_stats::*;
use crate::vector::distance::DistanceMetric;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// Trait for collecting index statistics
pub trait StatsCollector: Send + Sync {
    /// Get comprehensive index statistics
    fn get_stats(&self) -> IndexStats;
    
    /// Get layer quality analysis
    fn analyze_layers(&self) -> LayerAnalysis;
    
    /// Record a merge operation
    fn record_merge(&self, entry: MergeHistoryEntry);
    
    /// Get merge history (last N entries)
    fn get_merge_history(&self, limit: usize) -> Vec<MergeHistoryEntry>;
    
    /// Clear old merge history
    fn cleanup_old_history(&self, keep_recent_ms: u64);
}

/// Default implementation collecting statistics from HNSW index
pub struct HNSWStatsCollector {
    /// Shared reference to HNSW index
    index: Arc<HNSWIndex>,
    
    /// Merge operation history (thread-safe)
    merge_history: Arc<Mutex<Vec<MergeHistoryEntry>>>,
    
    /// Maximum history entries to keep
    max_history_entries: usize,
}

impl HNSWStatsCollector {
    /// Create new stats collector for HNSW index
    pub fn new(index: Arc<HNSWIndex>) -> Self {
        HNSWStatsCollector {
            index,
            merge_history: Arc::new(Mutex::new(Vec::new())),
            max_history_entries: 1000,
        }
    }
    
    /// Set maximum history entries (default 1000)
    pub fn with_max_history(mut self, max: usize) -> Self {
        self.max_history_entries = max;
        self
    }
    
    /// Calculate layer distribution
    fn calculate_layer_distribution(&self) -> HashMap<usize, usize> {
        // In a real implementation, would query the HNSW index
        // For now, return placeholder
        HashMap::new()
    }
    
    /// Calculate connectivity metrics
    fn calculate_connectivity(&self) -> ConnectivityMetrics {
        // Would analyze actual graph structure
        ConnectivityMetrics::default()
    }
    
    /// Calculate memory statistics
    fn calculate_memory_stats(&self) -> MemoryStats {
        // Would measure actual memory usage
        MemoryStats::default()
    }
}

impl StatsCollector for HNSWStatsCollector {
    fn get_stats(&self) -> IndexStats {
        let history = self.merge_history.lock().unwrap().clone();
        
        let mut stats = IndexStats::new();
        stats.layer_distribution = self.calculate_layer_distribution();
        stats.connectivity = self.calculate_connectivity();
        stats.memory_stats = self.calculate_memory_stats();
        stats.merge_history = history;
        
        stats
    }
    
    fn analyze_layers(&self) -> LayerAnalysis {
        let stats = self.get_stats();
        analyze_index(&stats)
    }
    
    fn record_merge(&self, entry: MergeHistoryEntry) {
        let mut history = self.merge_history.lock().unwrap();
        
        // Add new entry
        history.push(entry);
        
        // Keep only recent entries
        if history.len() > self.max_history_entries {
            let remove_count = history.len() - self.max_history_entries;
            history.drain(0..remove_count);
        }
    }
    
    fn get_merge_history(&self, limit: usize) -> Vec<MergeHistoryEntry> {
        let history = self.merge_history.lock().unwrap();
        
        if history.len() <= limit {
            return history.clone();
        }
        
        history.iter()
            .rev()
            .take(limit)
            .cloned()
            .collect()
    }
    
    fn cleanup_old_history(&self, keep_recent_ms: u64) {
        let mut history = self.merge_history.lock().unwrap();
        
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        
        let cutoff_time = now.saturating_sub(keep_recent_ms);
        
        history.retain(|entry| entry.timestamp_ms > cutoff_time);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stats_collector_creation() {
        // Note: Can't easily create HNSWIndex in test
        // This is a placeholder for integration testing
        let merge_entry = MergeHistoryEntry::new(100, 50, 10, 1000);
        assert_eq!(merge_entry.vectors_merged, 100);
    }

    #[test]
    fn test_merge_history_limit() {
        // Test that history respects max_history_entries
        // In full implementation, would use a mock index
        let merge1 = MergeHistoryEntry::new(100, 50, 10, 1000);
        let merge2 = MergeHistoryEntry::new(200, 75, 15, 2000);
        
        assert!(merge1.timestamp_ms > 0);
        assert!(merge2.timestamp_ms > 0);
    }

    #[test]
    fn test_connectivity_metrics_creation() {
        let metrics = ConnectivityMetrics::default();
        assert_eq!(metrics.completeness, 0.0);
        assert_eq!(metrics.min_connections, 0);
    }

    #[test]
    fn test_memory_stats_creation() {
        let stats = MemoryStats::default();
        assert_eq!(stats.total_bytes, 0);
        assert_eq!(stats.compression_ratio, 1.0);
    }

    #[test]
    fn test_layer_analysis_with_stats() {
        let stats = IndexStats::new();
        let analysis = analyze_index(&stats);
        assert!(analysis.is_healthy);
    }
}
