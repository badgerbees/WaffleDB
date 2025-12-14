/// Query Path Tracer for WaffleDB (Phase 1.7)
/// 
/// Captures detailed information about search operations including:
/// - Layer traversal paths
/// - Neighbor exploration at each layer
/// - Distance calculations
/// - Performance metrics
/// 
/// Used for debugging, performance analysis, and monitoring.

use std::sync::{Arc, Mutex};
use std::collections::VecDeque;
use crate::indexing::index_stats::QueryPath;

/// Configuration for query tracing
#[derive(Debug, Clone)]
pub struct QueryTracingConfig {
    /// Maximum number of query traces to keep
    pub max_traces: usize,
    
    /// Enable tracing for all queries
    pub enabled: bool,
    
    /// Sample rate (0.0-1.0, 1.0 = all queries traced)
    pub sample_rate: f32,
}

impl Default for QueryTracingConfig {
    fn default() -> Self {
        QueryTracingConfig {
            max_traces: 1000,
            enabled: true,
            sample_rate: 0.1, // Trace 10% of queries by default
        }
    }
}

/// Thread-safe query path tracer
pub struct QueryTracer {
    /// Configuration
    config: QueryTracingConfig,
    
    /// Stored query paths (circular buffer)
    traces: Arc<Mutex<VecDeque<QueryTrace>>>,
}

/// A captured query execution trace
#[derive(Debug, Clone)]
pub struct QueryTrace {
    /// Query path from index_stats
    pub path: QueryPath,
    
    /// Query vector dimension
    pub query_dim: usize,
    
    /// Collection name
    pub collection: String,
    
    /// Timestamp (unix microseconds)
    pub timestamp_us: u64,
    
    /// Whether this was a K-NN search (vs similarity search)
    pub is_knn: bool,
    
    /// K parameter (if K-NN)
    pub k: Option<usize>,
    
    /// EF parameter (if similarity)
    pub ef: Option<usize>,
    
    /// Results returned
    pub results_count: usize,
}

impl QueryTracer {
    /// Create new query tracer with config
    pub fn new(config: QueryTracingConfig) -> Self {
        let max_traces = config.max_traces;
        QueryTracer {
            config,
            traces: Arc::new(Mutex::new(VecDeque::with_capacity(max_traces))),
        }
    }
    
    /// Create query tracer with default config
    pub fn default() -> Self {
        Self::new(QueryTracingConfig::default())
    }
    
    /// Check if tracing is enabled and should trace this query
    pub fn should_trace(&self) -> bool {
        if !self.config.enabled {
            return false;
        }
        
        if self.config.sample_rate >= 1.0 {
            return true;
        }
        
        // Simple sampling: use thread-local randomness
        use std::cell::Cell;
        thread_local! {
            static RNG: Cell<u64> = Cell::new(12345);
        }
        
        RNG.with(|rng| {
            let mut seed = rng.get();
            seed = seed.wrapping_mul(1103515245).wrapping_add(12345);
            rng.set(seed);
            ((seed / 65536) % 100) as f32 / 100.0 < self.config.sample_rate
        })
    }
    
    /// Record a query execution trace
    pub fn record_trace(&self, trace: QueryTrace) {
        if let Ok(mut traces) = self.traces.lock() {
            // Remove oldest if at capacity
            if traces.len() >= self.config.max_traces {
                traces.pop_front();
            }
            traces.push_back(trace);
        }
    }
    
    /// Get all recorded traces
    pub fn get_traces(&self) -> Vec<QueryTrace> {
        self.traces
            .lock()
            .ok()
            .map(|t| t.iter().cloned().collect())
            .unwrap_or_default()
    }
    
    /// Get most recent N traces
    pub fn get_recent_traces(&self, limit: usize) -> Vec<QueryTrace> {
        self.traces
            .lock()
            .ok()
            .map(|t| t.iter().rev().take(limit).cloned().collect())
            .unwrap_or_default()
    }
    
    /// Get traces for specific collection
    pub fn get_collection_traces(&self, collection: &str) -> Vec<QueryTrace> {
        self.traces
            .lock()
            .ok()
            .map(|t| {
                t.iter()
                    .filter(|trace| trace.collection == collection)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default()
    }
    
    /// Get trace statistics
    pub fn get_stats(&self) -> QueryTraceStats {
        let traces = self.get_traces();
        
        if traces.is_empty() {
            return QueryTraceStats::default();
        }
        
        let total_time_us: u64 = traces.iter().map(|t| t.path.query_time_us).sum();
        let total_distance_calcs: u64 = traces.iter().map(|t| t.path.distance_calculations as u64).sum();
        let total_nodes_visited: u64 = traces.iter().map(|t| t.path.visited_nodes.len() as u64).sum();
        
        let avg_time_us = total_time_us / traces.len() as u64;
        let avg_distance_calcs = total_distance_calcs / traces.len() as u64;
        let avg_nodes_visited = total_nodes_visited / traces.len() as u64;
        
        let min_time_us = traces.iter().map(|t| t.path.query_time_us).min().unwrap_or(0);
        let max_time_us = traces.iter().map(|t| t.path.query_time_us).max().unwrap_or(0);
        
        QueryTraceStats {
            total_queries: traces.len(),
            avg_query_time_us: avg_time_us,
            min_query_time_us: min_time_us,
            max_query_time_us: max_time_us,
            avg_distance_calculations: avg_distance_calcs,
            avg_nodes_visited: avg_nodes_visited,
            total_traces_capacity: self.config.max_traces,
        }
    }
    
    /// Clear all traces
    pub fn clear(&self) {
        if let Ok(mut traces) = self.traces.lock() {
            traces.clear();
        }
    }
}

/// Statistics derived from query traces
#[derive(Debug, Clone)]
pub struct QueryTraceStats {
    /// Total number of queries traced
    pub total_queries: usize,
    
    /// Average query latency (microseconds)
    pub avg_query_time_us: u64,
    
    /// Minimum query latency observed
    pub min_query_time_us: u64,
    
    /// Maximum query latency observed
    pub max_query_time_us: u64,
    
    /// Average number of distance calculations
    pub avg_distance_calculations: u64,
    
    /// Average number of nodes visited
    pub avg_nodes_visited: u64,
    
    /// Total capacity for traces
    pub total_traces_capacity: usize,
}

impl Default for QueryTraceStats {
    fn default() -> Self {
        QueryTraceStats {
            total_queries: 0,
            avg_query_time_us: 0,
            min_query_time_us: 0,
            max_query_time_us: 0,
            avg_distance_calculations: 0,
            avg_nodes_visited: 0,
            total_traces_capacity: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_query_tracer_creation() {
        let tracer = QueryTracer::default();
        assert!(tracer.config.enabled);
        assert_eq!(tracer.config.max_traces, 1000);
    }
    
    #[test]
    fn test_record_and_retrieve_traces() {
        let tracer = QueryTracer::new(QueryTracingConfig {
            max_traces: 100,
            enabled: true,
            sample_rate: 1.0,
        });
        
        let trace = QueryTrace {
            path: QueryPath {
                start_layer: 2,
                visited_nodes: vec![],
                distance_calculations: 42,
                query_time_us: 1000,
                metric: "l2".to_string(),
            },
            query_dim: 768,
            collection: "test".to_string(),
            timestamp_us: 0,
            is_knn: true,
            k: Some(10),
            ef: None,
            results_count: 10,
        };
        
        tracer.record_trace(trace.clone());
        let traces = tracer.get_traces();
        
        assert_eq!(traces.len(), 1);
        assert_eq!(traces[0].query_dim, 768);
    }
    
    #[test]
    fn test_trace_capacity_limit() {
        let config = QueryTracingConfig {
            max_traces: 5,
            enabled: true,
            sample_rate: 1.0,
        };
        let tracer = QueryTracer::new(config);
        
        for i in 0..10 {
            let trace = QueryTrace {
                path: QueryPath {
                    start_layer: 2,
                    visited_nodes: vec![],
                    distance_calculations: i,
                    query_time_us: 1000,
                    metric: "l2".to_string(),
                },
                query_dim: 768,
                collection: "test".to_string(),
                timestamp_us: i as u64,
                is_knn: true,
                k: Some(10),
                ef: None,
                results_count: 10,
            };
            tracer.record_trace(trace);
        }
        
        let traces = tracer.get_traces();
        assert_eq!(traces.len(), 5);
        // Should have the last 5 (most recent)
        assert_eq!(traces[0].path.distance_calculations, 5);
        assert_eq!(traces[4].path.distance_calculations, 9);
    }
    
    #[test]
    fn test_collection_filter() {
        let tracer = QueryTracer::new(QueryTracingConfig {
            max_traces: 100,
            enabled: true,
            sample_rate: 1.0,
        });
        
        for i in 0..5 {
            let collection = if i % 2 == 0 { "col1" } else { "col2" };
            let trace = QueryTrace {
                path: QueryPath {
                    start_layer: 2,
                    visited_nodes: vec![],
                    distance_calculations: i,
                    query_time_us: 1000,
                    metric: "l2".to_string(),
                },
                query_dim: 768,
                collection: collection.to_string(),
                timestamp_us: i as u64,
                is_knn: true,
                k: Some(10),
                ef: None,
                results_count: 10,
            };
            tracer.record_trace(trace);
        }
        
        let col1_traces = tracer.get_collection_traces("col1");
        assert_eq!(col1_traces.len(), 3); // Indices 0, 2, 4
    }
    
    #[test]
    fn test_trace_statistics() {
        let tracer = QueryTracer::new(QueryTracingConfig {
            max_traces: 100,
            enabled: true,
            sample_rate: 1.0,
        });
        
        for i in 0..3 {
            let trace = QueryTrace {
                path: QueryPath {
                    start_layer: 2,
                    visited_nodes: vec![],
                    distance_calculations: 100 + i * 10,
                    query_time_us: 1000 + i as u64 * 100,
                    metric: "l2".to_string(),
                },
                query_dim: 768,
                collection: "test".to_string(),
                timestamp_us: i as u64,
                is_knn: true,
                k: Some(10),
                ef: None,
                results_count: 10,
            };
            tracer.record_trace(trace);
        }
        
        let stats = tracer.get_stats();
        assert_eq!(stats.total_queries, 3);
        assert!(stats.avg_query_time_us > 0);
        assert!(stats.avg_distance_calculations > 0);
    }
}
