/// Concurrent lock-free graph traversal for HNSW search.
/// 
/// This module provides lock-free atomic coordination for concurrent searches:
/// - Atomic result counter (no locks)
/// - Search completion flag (memory-efficient)
/// - Batch query processing utility

use std::sync::atomic::{AtomicUsize, AtomicBool, Ordering};
use std::sync::Arc;
use crate::hnsw::search::SearchResult;
use crate::vector::distance::DistanceMetric;

/// Shared state for concurrent search coordination
pub struct ConcurrentSearchState {
    /// Global result counter (lock-free)
    result_count: Arc<AtomicUsize>,
    /// Flag indicating search completion
    completed: Arc<AtomicBool>,
}

impl ConcurrentSearchState {
    /// Create new concurrent search state
    pub fn new() -> Self {
        ConcurrentSearchState {
            result_count: Arc::new(AtomicUsize::new(0)),
            completed: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Signal search completion
    pub fn mark_completed(&self) {
        self.completed.store(true, Ordering::Release);
    }

    /// Increment result counter atomically
    pub fn increment_results(&self) {
        self.result_count.fetch_add(1, Ordering::AcqRel);
    }

    /// Get current result count
    pub fn result_count(&self) -> usize {
        self.result_count.load(Ordering::Acquire)
    }

    /// Check if search completed
    pub fn is_completed(&self) -> bool {
        self.completed.load(Ordering::Acquire)
    }
}

impl Default for ConcurrentSearchState {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for ConcurrentSearchState {
    fn clone(&self) -> Self {
        ConcurrentSearchState {
            result_count: Arc::clone(&self.result_count),
            completed: Arc::clone(&self.completed),
        }
    }
}

/// Result of concurrent search
#[derive(Debug, Clone)]
pub struct ConcurrentSearchResult {
    pub results: Vec<SearchResult>,
    pub visited_count: usize,
}

/// Merge and sort multiple search results into final top-k
pub fn merge_search_results(results_vec: Vec<Vec<SearchResult>>, k: usize) -> ConcurrentSearchResult {
    let mut merged = Vec::new();
    let visited_count = results_vec.iter().map(|r| r.len()).sum();
    
    // Flatten all results
    for results in results_vec {
        merged.extend(results);
    }
    
    // Sort by distance ascending (nearest first)
    merged.sort_by(|a, b| {
        a.distance.partial_cmp(&b.distance).unwrap_or(std::cmp::Ordering::Equal)
    });
    
    // Keep top-k results
    merged.truncate(k);
    
    ConcurrentSearchResult {
        results: merged,
        visited_count,
    }
}

/// Batch search utility - process multiple queries
pub fn batch_search(
    queries: &[Vec<f32>],
    entry_point: usize,
    ef: usize,
    k: usize,
    get_vector: &dyn Fn(usize) -> Option<Vec<f32>>,
    get_neighbors: &dyn Fn(usize) -> Option<Vec<usize>>,
    metric: DistanceMetric,
) -> Vec<ConcurrentSearchResult> {
    queries
        .iter()
        .map(|query| {
            let results = super::search::search_layer(
                query,
                &[entry_point],
                ef,
                get_vector,
                get_neighbors,
                metric,
            );
            
            let visited = results.len();
            let merged: Vec<_> = results.into_iter().take(k).collect();
            
            ConcurrentSearchResult {
                results: merged,
                visited_count: visited,
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_concurrent_search_state() {
        let state = ConcurrentSearchState::new();
        assert_eq!(state.result_count(), 0);
        assert!(!state.is_completed());

        state.increment_results();
        assert_eq!(state.result_count(), 1);

        state.mark_completed();
        assert!(state.is_completed());
    }

    #[test]
    fn test_concurrent_search_state_atomic() {
        let state = ConcurrentSearchState::new();

        // Simulate multiple increments
        for _ in 0..100 {
            state.increment_results();
        }

        assert_eq!(state.result_count(), 100);
    }

    #[test]
    fn test_merge_results() {
        let result1 = vec![
            SearchResult { node_id: 0, distance: 0.1 },
            SearchResult { node_id: 1, distance: 0.5 },
        ];
        
        let result2 = vec![
            SearchResult { node_id: 2, distance: 0.3 },
        ];
        
        let merged = merge_search_results(vec![result1, result2], 2);
        assert_eq!(merged.results.len(), 2);
        assert_eq!(merged.results[0].distance, 0.1);
        assert_eq!(merged.results[1].distance, 0.3);
    }
}
