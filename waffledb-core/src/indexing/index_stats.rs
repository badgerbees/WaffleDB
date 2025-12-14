/// Phase 1.7: Index Statistics & Introspection
/// 
/// This module provides comprehensive introspection into HNSW index structure:
/// - Layer distribution and statistics
/// - Connectivity metrics and quality assessment
/// - Merge operation history and performance
/// - Query path tracing for debugging
/// - Memory efficiency analysis

use crate::Result;
use crate::vector::distance::DistanceMetric;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

/// Comprehensive index statistics
#[derive(Debug, Clone)]
pub struct IndexStats {
    /// Total vectors in index
    pub total_vectors: usize,
    
    /// Layer distribution: layer_id -> vector count
    pub layer_distribution: HashMap<usize, usize>,
    
    /// Total edges (connections) in graph
    pub total_edges: usize,
    
    /// Average connections per vector
    pub avg_connections_per_vector: f32,
    
    /// Connectivity metrics
    pub connectivity: ConnectivityMetrics,
    
    /// Memory footprint
    pub memory_stats: MemoryStats,
    
    /// Merge history
    pub merge_history: Vec<MergeHistoryEntry>,
    
    /// Layer quality metrics
    pub layer_quality: HashMap<usize, LayerQuality>,
}

impl IndexStats {
    /// Create new empty statistics
    pub fn new() -> Self {
        IndexStats {
            total_vectors: 0,
            layer_distribution: HashMap::new(),
            total_edges: 0,
            avg_connections_per_vector: 0.0,
            connectivity: ConnectivityMetrics::default(),
            memory_stats: MemoryStats::default(),
            merge_history: Vec::new(),
            layer_quality: HashMap::new(),
        }
    }
}

impl Default for IndexStats {
    fn default() -> Self {
        Self::new()
    }
}

/// Connectivity analysis
#[derive(Debug, Clone, Copy)]
pub struct ConnectivityMetrics {
    /// Minimum edges per vector
    pub min_connections: usize,
    
    /// Maximum edges per vector
    pub max_connections: usize,
    
    /// Median edges per vector
    pub median_connections: f32,
    
    /// Standard deviation of connection count
    pub connection_stddev: f32,
    
    /// Percentage of fully connected nodes
    pub fully_connected_percentage: f32,
    
    /// Graph completeness (0.0-1.0)
    pub completeness: f32,
}

impl Default for ConnectivityMetrics {
    fn default() -> Self {
        ConnectivityMetrics {
            min_connections: 0,
            max_connections: 0,
            median_connections: 0.0,
            connection_stddev: 0.0,
            fully_connected_percentage: 0.0,
            completeness: 0.0,
        }
    }
}

/// Memory efficiency metrics
#[derive(Debug, Clone, Copy)]
pub struct MemoryStats {
    /// Total vectors × dimensions × 4 bytes
    pub vector_data_bytes: u64,
    
    /// Approximate graph structure overhead
    pub graph_overhead_bytes: u64,
    
    /// Estimated index metadata
    pub metadata_bytes: u64,
    
    /// Total memory footprint
    pub total_bytes: u64,
    
    /// Bytes per vector (including all overhead)
    pub bytes_per_vector: f32,
    
    /// Compression ratio vs dense matrix
    pub compression_ratio: f32,
}

impl Default for MemoryStats {
    fn default() -> Self {
        MemoryStats {
            vector_data_bytes: 0,
            graph_overhead_bytes: 0,
            metadata_bytes: 0,
            total_bytes: 0,
            bytes_per_vector: 0.0,
            compression_ratio: 1.0,
        }
    }
}

/// Single layer quality assessment
#[derive(Debug, Clone, Copy)]
pub struct LayerQuality {
    /// Vectors in this layer
    pub vector_count: usize,
    
    /// Total edges in this layer
    pub edge_count: usize,
    
    /// Average edges in this layer
    pub avg_edges: f32,
    
    /// Is layer well-balanced (good connection distribution)
    pub is_balanced: bool,
    
    /// Quality score 0.0-1.0 (1.0 = perfect balance)
    pub quality_score: f32,
    
    /// Estimated search complexity for this layer
    pub estimated_search_complexity: f32,
}

/// Historical record of a merge operation
#[derive(Debug, Clone)]
pub struct MergeHistoryEntry {
    /// Timestamp of merge
    pub timestamp_ms: u64,
    
    /// Vectors merged in this operation
    pub vectors_merged: usize,
    
    /// New edges created
    pub new_edges_created: usize,
    
    /// Weak edges pruned (if enabled)
    pub weak_edges_pruned: usize,
    
    /// Merge duration in microseconds
    pub merge_time_us: u64,
    
    /// Which layer(s) were updated
    pub affected_layers: Vec<usize>,
    
    /// Status (success, partial, failed)
    pub status: MergeStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MergeStatus {
    Success,
    Partial,
    Failed,
}

impl MergeHistoryEntry {
    /// Create new merge history entry
    pub fn new(
        vectors_merged: usize,
        new_edges_created: usize,
        weak_edges_pruned: usize,
        merge_time_us: u64,
    ) -> Self {
        let timestamp_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);

        MergeHistoryEntry {
            timestamp_ms,
            vectors_merged,
            new_edges_created,
            weak_edges_pruned,
            merge_time_us,
            affected_layers: Vec::new(),
            status: MergeStatus::Success,
        }
    }
    
    /// Add affected layer to record
    pub fn add_affected_layer(&mut self, layer_id: usize) {
        self.affected_layers.push(layer_id);
    }
}

/// Query path tracing for debugging
#[derive(Debug, Clone)]
pub struct QueryPath {
    /// Starting layer (usually top)
    pub start_layer: usize,
    
    /// Nodes visited during search
    pub visited_nodes: Vec<QueryPathEntry>,
    
    /// Distance calculations performed
    pub distance_calculations: usize,
    
    /// Total query time in microseconds
    pub query_time_us: u64,
    
    /// Metric used for distance
    pub metric: String,
}

/// Single step in query path
#[derive(Debug, Clone)]
pub struct QueryPathEntry {
    /// Layer ID
    pub layer_id: usize,
    
    /// Node ID visited
    pub node_id: String,
    
    /// Distance to query vector
    pub distance: f32,
    
    /// Whether this node produced final result
    pub was_result: bool,
    
    /// Number of edges examined at this node
    pub edges_examined: usize,
}

/// Layer analysis for optimization recommendations
#[derive(Debug, Clone)]
pub struct LayerAnalysis {
    /// Current layer configuration
    pub layer_configs: HashMap<usize, LayerConfig>,
    
    /// Optimization recommendations
    pub recommendations: Vec<Recommendation>,
    
    /// Health check: all layers healthy?
    pub is_healthy: bool,
}

#[derive(Debug, Clone)]
pub struct LayerConfig {
    /// M value for this layer
    pub m: usize,
    
    /// M_max for this layer
    pub m_max: usize,
    
    /// ef_construction for insertion
    pub ef_construction: usize,
    
    /// ef_search for querying
    pub ef_search: usize,
    
    /// Connection reuse factor
    pub ml: f32,
}

#[derive(Debug, Clone)]
pub struct Recommendation {
    /// Type of recommendation
    pub recommendation_type: String,
    
    /// Affected layer (if any)
    pub layer_id: Option<usize>,
    
    /// Description of recommendation
    pub description: String,
    
    /// Estimated performance impact (as percentage)
    pub estimated_impact: f32,
    
    /// Severity: "info", "warning", "critical"
    pub severity: String,
}

impl Recommendation {
    /// Create info-level recommendation
    pub fn info(recommendation_type: String, description: String, impact: f32) -> Self {
        Recommendation {
            recommendation_type,
            layer_id: None,
            description,
            estimated_impact: impact,
            severity: "info".to_string(),
        }
    }
    
    /// Create warning-level recommendation
    pub fn warning(recommendation_type: String, description: String, impact: f32) -> Self {
        Recommendation {
            recommendation_type,
            layer_id: None,
            description,
            estimated_impact: impact,
            severity: "warning".to_string(),
        }
    }
    
    /// Create critical-level recommendation
    pub fn critical(recommendation_type: String, description: String, impact: f32) -> Self {
        Recommendation {
            recommendation_type,
            layer_id: None,
            description,
            estimated_impact: impact,
            severity: "critical".to_string(),
        }
    }
}

/// Analyze index structure for health and quality
pub fn analyze_index(stats: &IndexStats) -> LayerAnalysis {
    let mut recommendations = Vec::new();
    let mut is_healthy = true;
    
    // Empty index is healthy
    if stats.total_vectors == 0 {
        return LayerAnalysis {
            layer_configs: HashMap::new(),
            recommendations,
            is_healthy: true,
        };
    }
    
    // Check if any layer is unbalanced
    for (layer_id, quality) in &stats.layer_quality {
        if !quality.is_balanced && quality.vector_count > 10 {
            recommendations.push(Recommendation::warning(
                "UnbalancedLayer".to_string(),
                format!("Layer {} has uneven connection distribution (quality: {:.2})",
                    layer_id, quality.quality_score),
                -5.0,
            ));
            is_healthy = false;
        }
    }
    
    // Check graph completeness
    if stats.connectivity.completeness < 0.8 && stats.total_vectors > 10 {
        recommendations.push(Recommendation::warning(
            "LowCompleteness".to_string(),
            format!("Graph completeness is {:.2}%, target is 90%+",
                stats.connectivity.completeness * 100.0),
            -10.0,
        ));
        is_healthy = false;
    }
    
    // Check merge history for issues
    let recent_failures = stats.merge_history.iter()
        .rev()
        .take(10)
        .filter(|m| m.status != MergeStatus::Success)
        .count();
    
    if recent_failures > 2 {
        recommendations.push(Recommendation::critical(
            "FrequentMergeFailures".to_string(),
            format!("{} merge failures in last 10 operations", recent_failures),
            -25.0,
        ));
        is_healthy = false;
    }
    
    // Positive recommendation: good merge efficiency
    if let Some(last_merge) = stats.merge_history.last() {
        if last_merge.weak_edges_pruned > last_merge.new_edges_created / 2 {
            recommendations.push(Recommendation::info(
                "EffectivePruning".to_string(),
                "Merge pruning is removing significant weak edges, improving quality".to_string(),
                5.0,
            ));
        }
    }
    
    LayerAnalysis {
        layer_configs: HashMap::new(),
        recommendations,
        is_healthy,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_index_stats_creation() {
        let stats = IndexStats::new();
        assert_eq!(stats.total_vectors, 0);
        assert!(stats.layer_distribution.is_empty());
        assert_eq!(stats.total_edges, 0);
    }

    #[test]
    fn test_merge_history_entry_creation() {
        let entry = MergeHistoryEntry::new(100, 50, 10, 1000);
        assert_eq!(entry.vectors_merged, 100);
        assert_eq!(entry.new_edges_created, 50);
        assert_eq!(entry.weak_edges_pruned, 10);
        assert_eq!(entry.merge_time_us, 1000);
        assert_eq!(entry.status, MergeStatus::Success);
    }

    #[test]
    fn test_connectivity_metrics_default() {
        let metrics = ConnectivityMetrics::default();
        assert_eq!(metrics.min_connections, 0);
        assert_eq!(metrics.max_connections, 0);
        assert_eq!(metrics.completeness, 0.0);
    }

    #[test]
    fn test_recommendation_creation() {
        let rec = Recommendation::warning(
            "TestRec".to_string(),
            "Test description".to_string(),
            -5.0,
        );
        assert_eq!(rec.severity, "warning");
        assert_eq!(rec.estimated_impact, -5.0);
    }

    #[test]
    fn test_layer_analysis_healthy() {
        let stats = IndexStats::new();
        let analysis = analyze_index(&stats);
        assert!(analysis.is_healthy);
    }

    #[test]
    fn test_layer_analysis_low_completeness() {
        let mut stats = IndexStats::new();
        stats.total_vectors = 100;  // Non-empty index
        stats.connectivity.completeness = 0.5;
        let analysis = analyze_index(&stats);
        
        let has_warning = analysis.recommendations.iter()
            .any(|r| r.recommendation_type == "LowCompleteness" 
                && r.severity == "warning");
        assert!(has_warning);
    }

    #[test]
    fn test_merge_history_with_failures() {
        let mut stats = IndexStats::new();
        stats.total_vectors = 100;  // Non-empty index
        
        // Add some merge history
        let mut entry1 = MergeHistoryEntry::new(100, 50, 10, 1000);
        entry1.status = MergeStatus::Success;
        stats.merge_history.push(entry1);
        
        let mut entry2 = MergeHistoryEntry::new(50, 20, 5, 500);
        entry2.status = MergeStatus::Failed;
        stats.merge_history.push(entry2);
        
        let mut entry3 = MergeHistoryEntry::new(75, 30, 8, 750);
        entry3.status = MergeStatus::Failed;
        stats.merge_history.push(entry3);
        
        let mut entry4 = MergeHistoryEntry::new(60, 25, 6, 600);
        entry4.status = MergeStatus::Failed;
        stats.merge_history.push(entry4);
        
        let analysis = analyze_index(&stats);
        
        let has_critical = analysis.recommendations.iter()
            .any(|r| r.recommendation_type == "FrequentMergeFailures" 
                && r.severity == "critical");
        assert!(has_critical);
        assert!(!analysis.is_healthy);
    }

    #[test]
    fn test_effective_pruning_recommendation() {
        let mut stats = IndexStats::new();
        stats.total_vectors = 100;  // Non-empty index
        
        let mut entry = MergeHistoryEntry::new(100, 50, 30, 1000);
        entry.status = MergeStatus::Success;
        stats.merge_history.push(entry);
        
        let analysis = analyze_index(&stats);
        
        let has_info = analysis.recommendations.iter()
            .any(|r| r.recommendation_type == "EffectivePruning" 
                && r.severity == "info");
        assert!(has_info);
    }

    #[test]
    fn test_query_path_creation() {
        let path = QueryPath {
            start_layer: 5,
            visited_nodes: vec![],
            distance_calculations: 100,
            query_time_us: 5000,
            metric: "L2".to_string(),
        };
        
        assert_eq!(path.start_layer, 5);
        assert_eq!(path.distance_calculations, 100);
    }

    #[test]
    fn test_query_path_entry() {
        let entry = QueryPathEntry {
            layer_id: 0,
            node_id: "v123".to_string(),
            distance: 0.123,
            was_result: true,
            edges_examined: 16,
        };
        
        assert_eq!(entry.layer_id, 0);
        assert!(entry.was_result);
    }

    #[test]
    fn test_memory_stats_default() {
        let mem = MemoryStats::default();
        assert_eq!(mem.vector_data_bytes, 0);
        assert_eq!(mem.total_bytes, 0);
        assert_eq!(mem.compression_ratio, 1.0);
    }
}
