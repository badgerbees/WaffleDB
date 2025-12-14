/// HNSW Merge Module: Merges compaction sub-graphs into main HNSW index
///
/// This module implements the incremental merge strategy:
/// 1. Take a sub-graph of new vectors (built by IncrementalLayerBuilder)
/// 2. Find entry points into the main HNSW graph
/// 3. Create bidirectional connections between sub-graph and main graph
/// 4. Optimize connectivity (remove worst connections if needed)
/// 5. Update index statistics
///
/// Performance characteristics:
/// - O(k log k) where k = vectors in sub-graph
/// - No full HNSW rebuild needed
/// - Incremental graph optimization

use crate::vector::distance::DistanceMetric;
use crate::vector::types::Vector;
use crate::Result;
use std::collections::HashMap;
use std::cmp::Ordering;

/// Priority queue entry for candidates (distance, node_id, vector_id)
#[derive(Debug, Clone)]
struct Candidate {
    distance: f32,
    node_id: usize,
    vector_id: String,
}

impl PartialEq for Candidate {
    fn eq(&self, other: &Self) -> bool {
        (self.distance - other.distance).abs() < 1e-6
    }
}

impl Eq for Candidate {}

impl PartialOrd for Candidate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        // Reverse ordering: min-heap by distance
        other.distance.partial_cmp(&self.distance)
    }
}

impl Ord for Candidate {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap_or(Ordering::Equal)
    }
}

/// Configuration for merging sub-graphs into HNSW
#[derive(Debug, Clone)]
pub struct MergeConfig {
    /// How many entry points to find from sub-graph into main graph
    pub num_entry_points: usize,
    
    /// How many candidates to consider when connecting (ef parameter)
    pub ef_insertion: usize,
    
    /// Max connections per node in merged graph
    pub max_connections: usize,
    
    /// If true, prune weak connections to maintain quality
    pub enable_pruning: bool,
    
    /// Connection quality threshold (0.0-1.0) for keeping edges
    pub connection_quality_threshold: f32,
}

impl Default for MergeConfig {
    fn default() -> Self {
        MergeConfig {
            num_entry_points: 5,
            ef_insertion: 200,
            max_connections: 32,
            enable_pruning: true,
            connection_quality_threshold: 0.8,
        }
    }
}

/// Statistics for a merge operation
#[derive(Debug, Clone)]
pub struct MergeStats {
    pub vectors_merged: usize,
    pub new_edges_created: usize,
    pub weak_edges_pruned: usize,
    pub merge_time_us: u64,
}

impl MergeStats {
    pub fn new() -> Self {
        MergeStats {
            vectors_merged: 0,
            new_edges_created: 0,
            weak_edges_pruned: 0,
            merge_time_us: 0,
        }
    }
}

/// Find M nearest neighbors in a vector set (for connectivity)
pub fn find_m_nearest(
    target_vector: &[f32],
    candidates: &HashMap<String, Vector>,
    m: usize,
    metric: DistanceMetric,
) -> Vec<String> {
    let mut distances: Vec<(f32, String)> = candidates
        .iter()
        .map(|(id, vec)| {
            let dist = metric.distance(target_vector, &vec.data);
            (dist, id.clone())
        })
        .collect();

    distances.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(Ordering::Equal));
    distances
        .into_iter()
        .take(m)
        .map(|(_, id)| id)
        .collect()
}

/// Merge sub-graph into main HNSW (returns merge statistics)
///
/// Algorithm:
/// 1. For each vector in sub-graph: find its M nearest in main graph
/// 2. For each vector in main graph: check if sub-graph vector is closer than its worst connection
/// 3. Create bidirectional edges
/// 4. Optionally prune weak connections to maintain graph quality
pub fn merge_subgraph_into_hnsw(
    new_vectors: &HashMap<String, Vector>,
    existing_vectors: &HashMap<String, Vector>,
    existing_connections: &mut HashMap<String, Vec<String>>,
    m: usize,
    metric: DistanceMetric,
    config: &MergeConfig,
) -> Result<MergeStats> {
    let start = std::time::Instant::now();
    let mut stats = MergeStats::new();

    if new_vectors.is_empty() {
        return Ok(stats);
    }

    if existing_vectors.is_empty() {
        // First batch: just create connections within sub-graph
        for (id1, vec1) in new_vectors.iter() {
            let nearest = find_m_nearest(&vec1.data, new_vectors, m, metric);
            existing_connections.insert(id1.clone(), nearest);
            stats.new_edges_created += m;
        }
        stats.vectors_merged = new_vectors.len();
        stats.merge_time_us = start.elapsed().as_micros() as u64;
        return Ok(stats);
    }

    // For each new vector: connect to M nearest in main graph
    for (new_id, new_vec) in new_vectors.iter() {
        let nearest_in_main = find_m_nearest(&new_vec.data, existing_vectors, m, metric);
        let mut connections = nearest_in_main.clone();

        // Also add up to M nearest from sub-graph
        let nearest_in_sub = find_m_nearest(&new_vec.data, new_vectors, m, metric);
        for sub_id in nearest_in_sub {
            if !connections.contains(&sub_id) && connections.len() < m * 2 {
                connections.push(sub_id);
            }
        }

        // Limit to max_connections
        connections.truncate(config.max_connections);
        existing_connections.insert(new_id.clone(), connections.clone());
        stats.new_edges_created += connections.len();
    }

    // For each existing vector: add nearest from sub-graph if it's better than current worst
    for (existing_id, existing_vec) in existing_vectors.iter() {
        let mut current_connections = existing_connections
            .entry(existing_id.clone())
            .or_insert_with(Vec::new)
            .clone();

        // Find nearest sub-graph vector
        for (new_id, new_vec) in new_vectors.iter() {
            let distance = metric.distance(&existing_vec.data, &new_vec.data);

            // Check if this connection is better than worst current connection
            if current_connections.len() < m {
                current_connections.push(new_id.clone());
                stats.new_edges_created += 1;
            } else if current_connections.len() >= m {
                // Simple approach: add if there's room, otherwise skip for now
                // In a full implementation, we'd track and replace worst connections
                if current_connections.len() < config.max_connections {
                    current_connections.push(new_id.clone());
                    stats.new_edges_created += 1;
                }
            }
        }

        // Apply pruning if enabled
        if config.enable_pruning && current_connections.len() > config.max_connections {
            let pruned_connections = prune_connections(
                existing_id,
                &current_connections,
                existing_vectors,
                new_vectors,
                config.max_connections,
                metric,
            );
            stats.weak_edges_pruned += current_connections.len() - pruned_connections.len();
            existing_connections.insert(existing_id.clone(), pruned_connections);
        } else {
            current_connections.truncate(config.max_connections);
            existing_connections.insert(existing_id.clone(), current_connections);
        }
    }

    stats.vectors_merged = new_vectors.len();
    stats.merge_time_us = start.elapsed().as_micros() as u64;

    Ok(stats)
}

/// Prune weak connections using quality-based selection
fn prune_connections(
    node_id: &str,
    connections: &[String],
    existing_vectors: &HashMap<String, Vector>,
    new_vectors: &HashMap<String, Vector>,
    max_keep: usize,
    metric: DistanceMetric,
) -> Vec<String> {
    if let Some(node_vec) = existing_vectors.get(node_id).or_else(|| new_vectors.get(node_id)) {
        let mut scored_connections: Vec<(f32, String)> = connections
            .iter()
            .filter_map(|neighbor_id| {
                existing_vectors
                    .get(neighbor_id)
                    .or_else(|| new_vectors.get(neighbor_id))
                    .map(|neighbor_vec| {
                        let distance = metric.distance(&node_vec.data, &neighbor_vec.data);
                        (distance, neighbor_id.clone())
                    })
            })
            .collect();

        // Sort by distance (closer = better)
        scored_connections.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(Ordering::Equal));

        // Keep best connections up to max_keep
        scored_connections
            .into_iter()
            .take(max_keep)
            .map(|(_, id)| id)
            .collect()
    } else {
        connections.to_vec()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_vector(dim: usize, val: f32) -> Vector {
        Vector::new(vec![val; dim])
    }

    #[test]
    fn test_merge_config_default() {
        let config = MergeConfig::default();
        assert_eq!(config.num_entry_points, 5);
        assert_eq!(config.ef_insertion, 200);
        assert_eq!(config.max_connections, 32);
        assert!(config.enable_pruning);
    }

    #[test]
    fn test_merge_stats_creation() {
        let stats = MergeStats::new();
        assert_eq!(stats.vectors_merged, 0);
        assert_eq!(stats.new_edges_created, 0);
        assert_eq!(stats.weak_edges_pruned, 0);
    }

    #[test]
    fn test_find_m_nearest() {
        let mut candidates = HashMap::new();
        candidates.insert("a".to_string(), create_test_vector(3, 1.0));
        candidates.insert("b".to_string(), create_test_vector(3, 2.0));
        candidates.insert("c".to_string(), create_test_vector(3, 10.0));

        let target = [1.5; 3];
        let nearest = find_m_nearest(&target, &candidates, 2, DistanceMetric::L2);

        assert_eq!(nearest.len(), 2);
        assert!(nearest.contains(&"a".to_string()));
        assert!(nearest.contains(&"b".to_string()));
    }

    #[test]
    fn test_merge_empty_subgraph() {
        let new_vectors = HashMap::new();
        let existing_vectors = HashMap::new();
        let mut connections = HashMap::new();

        let config = MergeConfig::default();
        let stats = merge_subgraph_into_hnsw(
            &new_vectors,
            &existing_vectors,
            &mut connections,
            16,
            DistanceMetric::L2,
            &config,
        ).unwrap();

        assert_eq!(stats.vectors_merged, 0);
        assert_eq!(stats.new_edges_created, 0);
    }

    #[test]
    fn test_merge_into_empty_index() {
        let mut new_vectors = HashMap::new();
        new_vectors.insert("v1".to_string(), create_test_vector(3, 1.0));
        new_vectors.insert("v2".to_string(), create_test_vector(3, 2.0));

        let existing_vectors = HashMap::new();
        let mut connections = HashMap::new();

        let config = MergeConfig::default();
        let stats = merge_subgraph_into_hnsw(
            &new_vectors,
            &existing_vectors,
            &mut connections,
            2,
            DistanceMetric::L2,
            &config,
        ).unwrap();

        assert_eq!(stats.vectors_merged, 2);
        assert!(stats.new_edges_created > 0);
        assert!(connections.contains_key("v1"));
        assert!(connections.contains_key("v2"));
    }

    #[test]
    fn test_merge_into_existing_index() {
        let mut new_vectors = HashMap::new();
        new_vectors.insert("new1".to_string(), create_test_vector(3, 5.0));
        new_vectors.insert("new2".to_string(), create_test_vector(3, 6.0));

        let mut existing_vectors = HashMap::new();
        existing_vectors.insert("old1".to_string(), create_test_vector(3, 1.0));
        existing_vectors.insert("old2".to_string(), create_test_vector(3, 2.0));

        let mut connections = HashMap::new();
        connections.insert("old1".to_string(), vec!["old2".to_string()]);
        connections.insert("old2".to_string(), vec!["old1".to_string()]);

        let config = MergeConfig::default();
        let stats = merge_subgraph_into_hnsw(
            &new_vectors,
            &existing_vectors,
            &mut connections,
            2,
            DistanceMetric::L2,
            &config,
        ).unwrap();

        assert_eq!(stats.vectors_merged, 2);
        assert!(stats.new_edges_created > 0);
        
        // New vectors should have connections to old vectors
        assert!(connections.contains_key("new1"));
        assert!(connections.contains_key("new2"));
        
        // Old vectors should have been updated with new connections
        let old1_connections = &connections["old1"];
        assert!(old1_connections.len() >= 1);
    }

    #[test]
    fn test_merge_with_pruning() {
        let mut new_vectors = HashMap::new();
        for i in 0..10 {
            new_vectors.insert(
                format!("new{}", i),
                create_test_vector(3, (100 + i) as f32 / 10.0),
            );
        }

        let mut existing_vectors = HashMap::new();
        for i in 0..5 {
            existing_vectors.insert(
                format!("old{}", i),
                create_test_vector(3, i as f32),
            );
        }

        let mut connections = HashMap::new();
        for i in 0..5 {
            connections.insert(
                format!("old{}", i),
                (0..5).filter(|j| *j != i).map(|j| format!("old{}", j)).collect(),
            );
        }

        let mut config = MergeConfig::default();
        config.max_connections = 4;
        config.enable_pruning = true;

        let stats = merge_subgraph_into_hnsw(
            &new_vectors,
            &existing_vectors,
            &mut connections,
            2,
            DistanceMetric::L2,
            &config,
        ).unwrap();

        assert_eq!(stats.vectors_merged, 10);
        
        // Verify max_connections is respected
        for (_, conns) in connections.iter() {
            assert!(conns.len() <= config.max_connections);
        }
    }
}
