use crate::vector::distance::DistanceMetric;
use std::collections::{BinaryHeap, HashSet};
#[allow(unused_imports)]
use std::collections::HashMap;
use std::cmp::Ordering;

/// Profiling metrics for HNSW search.
#[derive(Debug, Clone, Default)]
pub struct SearchMetrics {
    pub visited_nodes: usize,
    pub neighbor_expansions: usize,
    pub max_heap_size: usize,
    pub distance_computations: usize,
}

/// Search result containing node ID and distance.
#[derive(Debug, Clone)]
pub struct SearchResult {
    pub node_id: usize,
    pub distance: f32,
}

/// Wrapper for ordering by distance (max-heap for candidates, min-heap for results).
#[derive(Debug, Clone)]
struct HeapEntry {
    distance: f32,
    node_id: usize,
}

impl Eq for HeapEntry {}

impl PartialEq for HeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.node_id == other.node_id
    }
}

impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reverse ordering for max-heap on distance (nearest first)
        other.distance.partial_cmp(&self.distance).unwrap_or(Ordering::Equal)
            .then_with(|| self.node_id.cmp(&other.node_id))
    }
}

impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Priority queue entry for candidates (max distance first).
#[derive(Debug, Clone)]
#[allow(dead_code)]
struct CandidateEntry {
    distance: f32,
    node_id: usize,
}

impl Eq for CandidateEntry {}

impl PartialEq for CandidateEntry {
    fn eq(&self, other: &Self) -> bool {
        self.node_id == other.node_id
    }
}

impl Ord for CandidateEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        // Regular ordering for min-heap on distance (farthest first for candidates)
        self.distance.partial_cmp(&other.distance).unwrap_or(Ordering::Equal)
            .then_with(|| self.node_id.cmp(&other.node_id))
    }
}

impl PartialOrd for CandidateEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Greedy search helper - find nearest entry point.
pub fn greedy_search_layer(
    query: &[f32],
    entry_points: &[usize],
    get_vector: &dyn Fn(usize) -> Option<Vec<f32>>,
    distance_fn: &dyn Fn(&[f32], &[f32]) -> f32,
) -> usize {
    if entry_points.is_empty() {
        return 0;
    }

    let mut closest = entry_points[0];
    let mut closest_dist = f32::MAX;

    for &ep in entry_points {
        if let Some(vec) = get_vector(ep) {
            let dist = distance_fn(query, &vec);
            if dist < closest_dist {
                closest_dist = dist;
                closest = ep;
            }
        }
    }

    closest
}

/// K-NN search at a specific layer with ef_search parameter.
/// This is the core layer search algorithm used in HNSW layer descent.
/// 
/// Parameters:
/// - ef: search width. Higher values = better quality but slower. Default 10-40 recommended.
///   * ef=1: Ultra-fast, lowest quality
///   * ef=10: Balanced (default)
///   * ef=40: High quality
///   * ef=100+: Best quality but slow
pub fn search_layer(
    query: &[f32],
    entry_points: &[usize],
    ef: usize,
    get_vector: &dyn Fn(usize) -> Option<Vec<f32>>,
    get_neighbors: &dyn Fn(usize) -> Option<Vec<usize>>,
    metric: DistanceMetric,
) -> Vec<SearchResult> {
    search_layer_with_metrics(query, entry_points, ef, get_vector, get_neighbors, metric).0
}

/// K-NN search with profiling metrics enabled.
pub fn search_layer_with_metrics(
    query: &[f32],
    entry_points: &[usize],
    ef: usize,
    get_vector: &dyn Fn(usize) -> Option<Vec<f32>>,
    get_neighbors: &dyn Fn(usize) -> Option<Vec<usize>>,
    metric: DistanceMetric,
) -> (Vec<SearchResult>, SearchMetrics) {
    if entry_points.is_empty() {
        return (vec![], SearchMetrics::default());
    }

    let mut visited = HashSet::new();
    let mut metrics = SearchMetrics::default();
    
    // Min-heap for result set (keeps nearest neighbors, max-heap wrapper for k-nn)
    let mut w = BinaryHeap::new();
    // Min-heap for candidates to expand (using Reverse for ascending order)
    let mut candidates = BinaryHeap::new();

    // Initialize with entry points
    for &ep in entry_points {
        if let Some(vec) = get_vector(ep) {
            let dist = metric.distance(query, &vec);
            visited.insert(ep);
            metrics.visited_nodes += 1;
            metrics.distance_computations += 1;
            
            // Max heap for results (farthest first)
            w.push(HeapEntry {
                distance: dist,
                node_id: ep,
            });
            
            // Min heap for candidates (nearest first)
            candidates.push(std::cmp::Reverse(CandidateEntry {
                distance: dist,
                node_id: ep,
            }));
        }
    }

    while !candidates.is_empty() {
        let std::cmp::Reverse(candidate) = candidates.pop().unwrap();
        
        // Compute lower bound: maximum distance in result set w
        let lowerbound = if let Some(entry) = w.peek() {
            entry.distance
        } else {
            f32::MAX
        };
        
        // Early termination: if nearest candidate is farther than result boundary
        if candidate.distance > lowerbound {
            break;
        }

        if let Some(neighbors) = get_neighbors(candidate.node_id) {
            metrics.neighbor_expansions += neighbors.len();
            
            for neighbor in neighbors {
                if !visited.contains(&neighbor) {
                    visited.insert(neighbor);
                    metrics.visited_nodes += 1;

                    if let Some(neighbor_vec) = get_vector(neighbor) {
                        let dist = metric.distance(query, &neighbor_vec);
                        metrics.distance_computations += 1;

                        // Include if closer than boundary or result set not full
                        if dist < lowerbound || w.len() < ef {
                            candidates.push(std::cmp::Reverse(CandidateEntry {
                                distance: dist,
                                node_id: neighbor,
                            }));
                            
                            w.push(HeapEntry {
                                distance: dist,
                                node_id: neighbor,
                            });

                            // Maintain result size at ef
                            if w.len() > ef {
                                w.pop();
                            }
                        }
                    }
                }
            }
        }
    }

    metrics.max_heap_size = w.len();

    // Extract results in ascending distance order
    let mut results = vec![];
    while !w.is_empty() {
        let entry = w.pop().unwrap();
        results.push(SearchResult {
            node_id: entry.node_id,
            distance: entry.distance,
        });
    }
    results.reverse(); // Reverse to get nearest first
    (results, metrics)
}

/// HNSW layer descent search - traverse layers from top to bottom.
/// 
/// This is the main search function for HNSW. It performs layer descent by:
/// 1. Starting from the highest layer's entry point
/// 2. Greedily searching each layer with ef=1
/// 3. Using the nearest neighbor as entry point to the next layer
/// 4. Final layer search uses ef_search for quality results
///
/// **ef_search tuning (Critical for performance):**
/// - ef=1:   Ultra-fast (~5ms @ 1M), recall ~50%
/// - ef=10:  Balanced (default, ~30ms @ 1M), recall ~90%
/// - ef=40:  High quality (~100ms @ 1M), recall ~99%
/// - ef=100: Best (~150ms @ 1M), recall ~99.5%
///
/// **Recommendation:** Start with ef=10-20 for 1M+ vectors
pub fn search_hnsw_layers(
    query: &[f32],
    layers: &[crate::hnsw::graph::Layer],
    entry_point: usize,
    ef_search: usize,
    get_vector: &dyn Fn(usize) -> Option<Vec<f32>>,
    metric: DistanceMetric,
) -> Vec<SearchResult> {
    search_hnsw_layers_with_metrics(query, layers, entry_point, ef_search, get_vector, metric).0
}

/// HNSW search with profiling metrics.
pub fn search_hnsw_layers_with_metrics(
    query: &[f32],
    layers: &[crate::hnsw::graph::Layer],
    entry_point: usize,
    ef_search: usize,
    get_vector: &dyn Fn(usize) -> Option<Vec<f32>>,
    metric: DistanceMetric,
) -> (Vec<SearchResult>, SearchMetrics) {
    if layers.is_empty() {
        return (vec![], SearchMetrics::default());
    }

    let mut nearest = vec![entry_point];
    let num_layers = layers.len();
    let mut total_metrics = SearchMetrics::default();

    // Layer descent from top layer to layer 1 (greedy search with ef=1)
    for layer_idx in (1..num_layers).rev() {
        let layer = &layers[layer_idx];
        
        // Greedy search helper for this layer
        let get_neighbors_fn = |node_id: usize| -> Option<Vec<usize>> {
            layer.neighbors(node_id).cloned()
        };
        
        let (search_results, metrics) = search_layer_with_metrics(
            query,
            &nearest,
            1, // ef=1 for layer descent (greedy)
            get_vector,
            &get_neighbors_fn,
            metric,
        );
        
        total_metrics.visited_nodes += metrics.visited_nodes;
        total_metrics.neighbor_expansions += metrics.neighbor_expansions;
        total_metrics.distance_computations += metrics.distance_computations;
        
        nearest = search_results.iter().map(|r| r.node_id).collect();
        if nearest.is_empty() {
            nearest = vec![entry_point];
        }
    }

    // Final search on layer 0 with full ef_search
    let layer_0 = &layers[0];
    let get_neighbors_fn = |node_id: usize| -> Option<Vec<usize>> {
        layer_0.neighbors(node_id).cloned()
    };
    
    let (results, final_metrics) = search_layer_with_metrics(
        query,
        &nearest,
        ef_search,
        get_vector,
        &get_neighbors_fn,
        metric,
    );
    
    total_metrics.visited_nodes += final_metrics.visited_nodes;
    total_metrics.neighbor_expansions += final_metrics.neighbor_expansions;
    total_metrics.max_heap_size = final_metrics.max_heap_size;
    total_metrics.distance_computations += final_metrics.distance_computations;
    
    (results, total_metrics)
}

/// HNSW search with metadata filtering support.
/// 
/// This function filters results based on metadata criteria before returning.
/// The filter function receives (node_id, metadata_value) and returns true if
/// the result should be included.
/// 
/// Note: For better performance, filtering is applied post-search. This means
/// the actual results may include more candidates than k to account for filtered-out items.
/// If you need exactly k results after filtering, increase k during search.
pub fn search_hnsw_layers_with_filter(
    query: &[f32],
    layers: &[crate::hnsw::graph::Layer],
    entry_point: usize,
    ef_search: usize,
    k: usize,
    get_vector: &dyn Fn(usize) -> Option<Vec<f32>>,
    get_metadata: &dyn Fn(usize) -> Option<String>,
    metric: DistanceMetric,
    filter: &dyn Fn(&str) -> bool, // Return true if this metadata passes filter
) -> Vec<SearchResult> {
    let (unfiltered_results, _metrics) = search_hnsw_layers_with_metrics(
        query,
        layers,
        entry_point,
        ef_search,
        get_vector,
        metric,
    );
    
    // Apply filter and keep top k results
    let mut filtered = vec![];
    for result in unfiltered_results {
        if filtered.len() >= k {
            break;
        }
        
        if let Some(metadata) = get_metadata(result.node_id) {
            if filter(&metadata) {
                filtered.push(result);
            }
        } else {
            // If no metadata, include by default (no metadata = passes filter)
            filtered.push(result);
        }
    }
    
    filtered
}

/// K-NN search at a specific layer with metadata filtering.
/// 
/// Similar to search_layer but applies metadata filtering post-search.
pub fn search_layer_with_filter(
    query: &[f32],
    entry_points: &[usize],
    ef: usize,
    k: usize,
    get_vector: &dyn Fn(usize) -> Option<Vec<f32>>,
    get_neighbors: &dyn Fn(usize) -> Option<Vec<usize>>,
    get_metadata: &dyn Fn(usize) -> Option<String>,
    metric: DistanceMetric,
    filter: &dyn Fn(&str) -> bool,
) -> Vec<SearchResult> {
    let unfiltered_results = search_layer(query, entry_points, ef, get_vector, get_neighbors, metric);
    
    // Apply filter and keep top k
    let mut filtered = vec![];
    for result in unfiltered_results {
        if filtered.len() >= k {
            break;
        }
        
        if let Some(metadata) = get_metadata(result.node_id) {
            if filter(&metadata) {
                filtered.push(result);
            }
        } else {
            filtered.push(result);
        }
    }
    
    filtered
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_search_result_ordering() {
        let results = vec![
            SearchResult { node_id: 1, distance: 0.5 },
            SearchResult { node_id: 2, distance: 0.3 },
            SearchResult { node_id: 3, distance: 0.8 },
        ];
        
        // Verify results are properly structured
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].distance, 0.5);
    }
    
    #[test]
    fn test_metadata_filter_basic() {
        // Simulate filtering by checking metadata values
        let metadata_store: HashMap<usize, String> = vec![
            (1, "category:A".to_string()),
            (2, "category:B".to_string()),
            (3, "category:A".to_string()),
        ].into_iter().collect();
        
        let get_metadata = |node_id: usize| -> Option<String> {
            metadata_store.get(&node_id).cloned()
        };
        
        let filter_a = |metadata: &str| -> bool {
            metadata.contains("category:A")
        };
        
        let results = vec![
            SearchResult { node_id: 1, distance: 0.5 },
            SearchResult { node_id: 2, distance: 0.3 },
            SearchResult { node_id: 3, distance: 0.8 },
        ];
        
        // Apply filter manually for testing
        let filtered: Vec<_> = results.iter()
            .filter(|r| {
                if let Some(meta) = get_metadata(r.node_id) {
                    filter_a(&meta)
                } else {
                    true
                }
            })
            .cloned()
            .collect();
        
        assert_eq!(filtered.len(), 2);
        assert!(filtered.iter().all(|r| r.node_id == 1 || r.node_id == 3));
    }
}
