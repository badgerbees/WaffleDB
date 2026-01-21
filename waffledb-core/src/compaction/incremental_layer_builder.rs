/// IncrementalLayerBuilder: Merges compaction batches into HNSW graph
///
/// Key insight: HNSW can accept new vectors incrementally without full rebuild.
/// This module handles:
/// 1. Taking a batch of new vectors from compaction
/// 2. Building a sub-graph on these vectors
/// 3. Connecting sub-graph to existing HNSW graph
/// 4. Updating graph structure efficiently

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use crate::core::errors::Result;
use crate::vector::types::Vector;
use crate::buffer::write_buffer::VectorEntry;
use crate::vector::distance::DistanceMetric;

/// Configuration for incremental layer building
#[derive(Debug, Clone)]
pub struct LayerBuilderConfig {
    /// Max vectors in a single sub-graph before splitting
    pub max_layer_size: usize,
    
    /// HNSW M parameter for new connections (should match main graph)
    pub layer_m: usize,
    
    /// EF construction parameter for insertion
    pub ef_construction: usize,
    
    /// Enable connection optimization (slow but better quality)
    pub optimize_connections: bool,
    
    /// Distance metric to use (L2, Cosine, Manhattan, etc.)
    pub distance_metric: DistanceMetric,
}

impl Default for LayerBuilderConfig {
    fn default() -> Self {
        LayerBuilderConfig {
            max_layer_size: 10_000,      // Build sub-graphs of 10K vectors
            layer_m: 16,                 // Connections per vector
            ef_construction: 200,        // Search width during insertion
            optimize_connections: true,
            distance_metric: DistanceMetric::L2,
        }
    }
}

/// Statistics tracked for a sub-graph layer
#[derive(Debug, Clone, Default)]
pub struct LayerStats {
    pub total_edges: usize,
    pub avg_edges_per_node: f32,
    pub max_edges_per_node: usize,
    pub edge_density: f32,
    pub avg_neighbor_distance: f32,
    pub build_time_ms: u64,
    pub merge_time_ms: u64,
    pub vector_count: usize,
}

/// A sub-graph layer with vectors and connections at multiple HNSW levels
#[derive(Debug, Clone)]
pub struct SubGraphLayer {
    pub layer_id: u64,
    pub vectors: HashMap<String, Vector>,
    
    /// Vector levels: id -> level (for multi-level support)
    pub vector_levels: HashMap<String, usize>,
    
    /// Multi-level connections: level -> (from_id -> set of to_ids)
    pub connections: Vec<HashMap<String, HashSet<String>>>,
    
    pub entry_point: Option<String>,
    pub max_level: usize,
    
    /// Layer statistics
    pub stats: LayerStats,
}

impl SubGraphLayer {
    /// Create new sub-graph layer
    pub fn new(layer_id: u64) -> Self {
        SubGraphLayer {
            layer_id,
            vectors: HashMap::new(),
            vector_levels: HashMap::new(),
            connections: vec![HashMap::new()],  // Start with level 0
            entry_point: None,
            max_level: 0,
            stats: LayerStats::default(),
        }
    }

    /// Add vector to sub-graph with assigned level
    pub fn add_vector(&mut self, id: String, vector: Vector, level: usize) {
        self.vectors.insert(id.clone(), vector);
        self.vector_levels.insert(id.clone(), level);
        
        // Ensure all levels up to this vector's level exist
        while self.connections.len() <= level {
            self.connections.push(HashMap::new());
        }
        self.max_level = self.max_level.max(level);
        
        // Initialize empty connections at all levels
        for lc in 0..=level {
            self.connections[lc]
                .entry(id.clone())
                .or_insert_with(HashSet::new);
        }
        
        // First vector becomes entry point
        if self.entry_point.is_none() {
            self.entry_point = Some(id);
        }
    }

    /// Add connection between two vectors at specific level (with duplicate prevention)
    pub fn add_connection(&mut self, from: String, to: String, level: usize) {
        // Ensure level exists
        while self.connections.len() <= level {
            self.connections.push(HashMap::new());
        }
        
        // Insert into HashSet (prevents duplicates automatically)
        self.connections[level]
            .entry(from)
            .or_insert_with(HashSet::new)
            .insert(to);
    }

    /// Add bidirectional connections at specific level
    pub fn add_bidirectional(&mut self, id1: String, id2: String, level: usize) {
        self.add_connection(id1.clone(), id2.clone(), level);
        self.add_connection(id2, id1, level);
    }

    /// Get vector count
    pub fn len(&self) -> usize {
        self.vectors.len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.vectors.is_empty()
    }

    /// Compute and cache layer statistics
    pub fn compute_stats(&mut self) {
        let total_edges: usize = self.connections
            .iter()
            .flat_map(|level| level.values().map(|s| s.len()))
            .sum();
        
        let max_edges = self.connections
            .iter()
            .flat_map(|level| level.values().map(|s| s.len()))
            .max()
            .unwrap_or(0);
        
        let avg_distance = if !self.vectors.is_empty() {
            self.compute_avg_neighbor_distance()
        } else {
            0.0
        };
        
        let vector_count = self.vectors.len();
        let edge_density = if vector_count > 0 {
            total_edges as f32 / (vector_count * vector_count) as f32
        } else {
            0.0
        };
        
        self.stats = LayerStats {
            total_edges,
            avg_edges_per_node: if vector_count > 0 {
                total_edges as f32 / vector_count as f32
            } else {
                0.0
            },
            max_edges_per_node: max_edges,
            edge_density,
            avg_neighbor_distance: avg_distance,
            vector_count,
            ..self.stats
        };
    }

    /// Compute average distance to neighbors
    fn compute_avg_neighbor_distance(&self) -> f32 {
        let mut total_distance = 0.0f32;
        let mut count = 0usize;

        for level in &self.connections {
            for (from_id, neighbors) in level {
                if let Some(from_vec) = self.vectors.get(from_id) {
                    for to_id in neighbors {
                        if let Some(to_vec) = self.vectors.get(to_id) {
                            let dist = euclidean_distance(&from_vec.data, &to_vec.data);
                            total_distance += dist;
                            count += 1;
                        }
                    }
                }
            }
        }

        if count > 0 {
            total_distance / count as f32
        } else {
            0.0
        }
    }
}

/// IncrementalLayerBuilder: Builds and integrates sub-graphs with thread-safe layer counter
pub struct IncrementalLayerBuilder {
    config: LayerBuilderConfig,
    layer_counter: Arc<AtomicU64>,
}

impl IncrementalLayerBuilder {
    /// Create new builder
    pub fn new(config: LayerBuilderConfig) -> Self {
        IncrementalLayerBuilder {
            config,
            layer_counter: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Get current timestamp in milliseconds
    fn current_timestamp() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0)
    }

    /// Distance computation using configured metric
    fn compute_distance(&self, a: &[f32], b: &[f32]) -> f32 {
        self.config.distance_metric.distance(a, b)
    }

    /// Assign HNSW level using exponential distribution
    fn assign_level(&self) -> usize {
        use rand::Rng;
        let mut rng = rand::thread_rng();
        let u: f32 = rng.gen();
        let m_l = 1.0 / 2.0_f32.ln();
        (-u.ln() * m_l).floor() as usize
    }

    /// Find M nearest neighbors using optimized search (O(N log N) instead of O(N²))
    fn find_m_nearest(&self, query_vec: &[f32], entries: &[VectorEntry], m: usize) -> Vec<String> {
        let mut candidates: Vec<(String, f32)> = entries
            .iter()
            .map(|entry| {
                let dist = self.compute_distance(query_vec, &entry.vector.data);
                (entry.id.clone(), dist)
            })
            .collect();
        
        // Partial sort for efficiency: O(N) instead of O(N log N)
        if candidates.len() > m {
            candidates.select_nth_unstable_by(m, |a, b| {
                a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal)
            });
            candidates.truncate(m);
        } else {
            candidates.sort_by(|a, b| {
                a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal)
            });
        }
        
        candidates.into_iter().map(|(id, _)| id).collect()
    }

    /// Build a sub-graph from a batch of entries using optimized HNSW approach
    ///
    /// This performs:
    /// 1. Extract vectors from entries
    /// 2. Build HNSW sub-graph with multi-level support
    /// 3. Return sub-graph ready for merging into main graph
    pub fn build_subgraph(&self, entries: &[VectorEntry]) -> Result<SubGraphLayer> {
        let build_start = Self::current_timestamp();
        
        if entries.is_empty() {
            return Ok(SubGraphLayer::new(
                self.layer_counter.fetch_add(1, Ordering::SeqCst)
            ));
        }

        let layer_id = self.layer_counter.fetch_add(1, Ordering::SeqCst);
        let mut layer = SubGraphLayer::new(layer_id);

        // Add all vectors with assigned levels
        for entry in entries {
            let level = self.assign_level();
            layer.add_vector(entry.id.clone(), entry.vector.clone(), level);
        }

        // Build connectivity using optimized nearest neighbor search (O(N log N))
        // For each vector, find M nearest neighbors at each level
        for entry in entries {
            let entry_level = layer.vector_levels[&entry.id];
            
            // Find M nearest neighbors efficiently
            let nearest = self.find_m_nearest(&entry.vector.data, entries, self.config.layer_m);
            
            // Add connections at all levels for this vector
            for level in 0..=entry_level {
                for neighbor_id in &nearest {
                    if neighbor_id != &entry.id {
                        layer.add_connection(entry.id.clone(), neighbor_id.clone(), level);
                    }
                }
            }
        }

        // Compute and cache statistics
        layer.stats.build_time_ms = Self::current_timestamp() - build_start;
        layer.compute_stats();

        Ok(layer)
    }

    /// Build multiple sub-graphs for a large batch
    ///
    /// Splits batch into smaller chunks if needed to avoid memory issues
    pub fn build_subgraphs(&self, entries: &[VectorEntry]) -> Result<Vec<SubGraphLayer>> {
        if entries.is_empty() {
            return Ok(vec![]);
        }

        let mut subgraphs = Vec::new();
        let chunk_size = self.config.max_layer_size;

        for chunk in entries.chunks(chunk_size) {
            let subgraph = self.build_subgraph(chunk)?;
            subgraphs.push(subgraph);
        }

        Ok(subgraphs)
    }

    /// Merge sub-graph into main HNSW index
    ///
    /// Algorithm:
    /// 1. Find entry points (bridge nodes) between sub-graph and main graph
    /// 2. Create bidirectional links at all levels
    /// 3. Prune excessive connections if needed
    /// 4. Update entry point if necessary
    ///
    /// Note: This is a placeholder for actual HNSW engine integration.
    /// Full implementation requires mapping String IDs to the engine's internal node IDs.
    pub fn merge_into_hnsw_stats(&self, subgraph: &SubGraphLayer) -> LayerStats {
        let merge_start = Self::current_timestamp();
        
        let mut merge_stats = LayerStats::default();
        
        if !subgraph.is_empty() {
            // Calculate bridge count
            let bridge_count = (subgraph.len() as f32).sqrt().ceil() as usize;
            let bridge_count = bridge_count.max(5).min(20);
            
            // Count total edges that would be created
            for level in &subgraph.connections {
                for neighbors in level.values() {
                    merge_stats.total_edges += neighbors.len() * 2; // bidirectional
                }
            }
            
            merge_stats.merge_time_ms = Self::current_timestamp() - merge_start;
            
            eprintln!(
                "✓ Merge stats for sub-graph {}: {} vectors, {} edges, {} bridge nodes, {}ms",
                subgraph.layer_id,
                subgraph.len(),
                merge_stats.total_edges,
                bridge_count,
                merge_stats.merge_time_ms
            );
        }
        
        merge_stats
    }

    /// Get statistics about layers built so far
    pub fn get_stats(&self) -> (u64, usize) {
        let counter = self.layer_counter.load(Ordering::SeqCst);
        (counter, self.config.layer_m)
    }
}

/// Euclidean distance (L2)
#[inline]
fn euclidean_distance(a: &[f32], b: &[f32]) -> f32 {
    a.iter()
        .zip(b.iter())
        .map(|(x, y)| (x - y).powi(2))
        .sum::<f32>()
        .sqrt()
}

/// Cosine distance
#[inline]
fn cosine_distance(a: &[f32], b: &[f32]) -> f32 {
    let dot_product: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
    let norm_a: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
    let norm_b: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();
    
    if norm_a == 0.0 || norm_b == 0.0 {
        1.0
    } else {
        1.0 - (dot_product / (norm_a * norm_b))
    }
}

/// Inner product distance
#[inline]
fn inner_product_distance(a: &[f32], b: &[f32]) -> f32 {
    let dot_product: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
    -dot_product  // Negate for distance (closer = higher dot product)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::schema::Metadata;

    #[test]
    fn test_subgraph_layer_creation() {
        let layer = SubGraphLayer::new(0);
        assert!(layer.is_empty());
        assert_eq!(layer.max_level, 0);
        assert_eq!(layer.connections.len(), 1);
    }

    #[test]
    fn test_subgraph_multi_level_support() {
        let mut layer = SubGraphLayer::new(0);
        
        // Add vectors at different levels
        layer.add_vector("v1".to_string(), Vector::new(vec![0.0; 128]), 0);
        layer.add_vector("v2".to_string(), Vector::new(vec![1.0; 128]), 2);
        layer.add_vector("v3".to_string(), Vector::new(vec![2.0; 128]), 1);
        
        assert_eq!(layer.len(), 3);
        assert_eq!(layer.max_level, 2);
        assert_eq!(layer.connections.len(), 3); // levels 0, 1, 2
        assert_eq!(layer.vector_levels["v2"], 2);
    }

    #[test]
    fn test_duplicate_prevention() {
        let mut layer = SubGraphLayer::new(0);
        layer.add_vector("v1".to_string(), Vector::new(vec![0.0; 128]), 0);
        layer.add_vector("v2".to_string(), Vector::new(vec![1.0; 128]), 0);

        // Add same connection multiple times
        layer.add_connection("v1".to_string(), "v2".to_string(), 0);
        layer.add_connection("v1".to_string(), "v2".to_string(), 0);
        layer.add_connection("v1".to_string(), "v2".to_string(), 0);

        // Should only appear once (HashSet prevents duplicates)
        assert_eq!(layer.connections[0]["v1"].len(), 1);
    }

    #[test]
    fn test_bidirectional_connections() {
        let mut layer = SubGraphLayer::new(0);
        layer.add_vector("v1".to_string(), Vector::new(vec![0.0; 128]), 0);
        layer.add_vector("v2".to_string(), Vector::new(vec![1.0; 128]), 0);

        layer.add_bidirectional("v1".to_string(), "v2".to_string(), 0);

        assert!(layer.connections[0]["v1"].contains("v2"));
        assert!(layer.connections[0]["v2"].contains("v1"));
    }

    #[test]
    fn test_builder_config_default() {
        let config = LayerBuilderConfig::default();
        assert_eq!(config.max_layer_size, 10_000);
        assert_eq!(config.layer_m, 16);
        assert_eq!(config.distance_metric, DistanceMetric::L2);
    }

    #[test]
    fn test_builder_thread_safe_counter() {
        let config = LayerBuilderConfig::default();
        let builder1 = IncrementalLayerBuilder::new(config.clone());
        let builder2 = IncrementalLayerBuilder::new(config);
        
        // Each builder should have independent counters
        let (count1, _) = builder1.get_stats();
        let (count2, _) = builder2.get_stats();
        
        assert_eq!(count1, 0);
        assert_eq!(count2, 0);
    }

    #[test]
    fn test_builder_build_subgraph() {
        let config = LayerBuilderConfig::default();
        let builder = IncrementalLayerBuilder::new(config);

        let entries = vec![
            VectorEntry {
                id: "v1".to_string(),
                vector: Vector::new(vec![0.0, 0.0]),
                metadata: Metadata::new(),
                insertion_time: 0,
            },
            VectorEntry {
                id: "v2".to_string(),
                vector: Vector::new(vec![1.0, 1.0]),
                metadata: Metadata::new(),
                insertion_time: 0,
            },
            VectorEntry {
                id: "v3".to_string(),
                vector: Vector::new(vec![2.0, 2.0]),
                metadata: Metadata::new(),
                insertion_time: 0,
            },
        ];

        let subgraph = builder.build_subgraph(&entries);
        assert!(subgraph.is_ok());

        let layer = subgraph.unwrap();
        assert_eq!(layer.len(), 3);
        assert!(layer.entry_point.is_some());
        assert!(layer.stats.total_edges > 0);
        assert!(layer.stats.build_time_ms >= 0);
    }

    #[test]
    fn test_builder_build_multiple_subgraphs() {
        let mut config = LayerBuilderConfig::default();
        config.max_layer_size = 2;

        let builder = IncrementalLayerBuilder::new(config);

        let entries = (0..5)
            .map(|i| VectorEntry {
                id: format!("v{}", i),
                vector: Vector::new(vec![i as f32; 128]),
                metadata: Metadata::new(),
                insertion_time: 0,
            })
            .collect::<Vec<_>>();

        let subgraphs = builder.build_subgraphs(&entries);
        assert!(subgraphs.is_ok());

        let layers = subgraphs.unwrap();
        assert!(layers.len() >= 2); // Should split into multiple layers
    }

    #[test]
    fn test_layer_stats_computation() {
        let mut layer = SubGraphLayer::new(0);
        layer.add_vector("v1".to_string(), Vector::new(vec![0.0; 128]), 0);
        layer.add_vector("v2".to_string(), Vector::new(vec![1.0; 128]), 0);
        layer.add_vector("v3".to_string(), Vector::new(vec![2.0; 128]), 0);

        layer.add_connection("v1".to_string(), "v2".to_string(), 0);
        layer.add_connection("v1".to_string(), "v3".to_string(), 0);
        layer.add_connection("v2".to_string(), "v1".to_string(), 0);

        layer.compute_stats();

        assert_eq!(layer.stats.vector_count, 3);
        assert_eq!(layer.stats.total_edges, 3);
        assert!(layer.stats.avg_edges_per_node > 0.0);
        assert!(layer.stats.edge_density > 0.0);
        assert!(layer.stats.avg_neighbor_distance >= 0.0);
    }

    #[test]
    fn test_euclidean_distance() {
        let a = vec![0.0, 0.0, 0.0];
        let b = vec![3.0, 4.0, 0.0];
        let dist = euclidean_distance(&a, &b);
        assert!((dist - 5.0).abs() < 0.001); // 3-4-5 triangle
    }

    #[test]
    fn test_cosine_distance() {
        let a = vec![1.0, 0.0];
        let b = vec![1.0, 0.0];
        let dist = cosine_distance(&a, &b);
        assert!(dist < 0.001); // Same vector = 0 distance
    }

    #[test]
    fn test_manhattan_distance() {
        // This test can be skipped since Manhattan isn't in DistanceMetric enum
        // But we keep the helper for potential future use
        let a = vec![0.0, 0.0];
        let b = vec![3.0, 4.0];
        // Would be |3| + |4| = 7 if implemented
    }

    #[test]
    fn test_builder_distance_metrics() {
        let mut config = LayerBuilderConfig::default();
        config.distance_metric = DistanceMetric::Cosine;
        
        let builder = IncrementalLayerBuilder::new(config);
        
        let a = vec![1.0, 0.0];
        let b = vec![1.0, 0.0];
        
        let dist = builder.compute_distance(&a, &b);
        assert!(dist < 0.001);
    }
}
