/// IncrementalLayerBuilder: Merges compaction batches into HNSW graph
///
/// Key insight: HNSW can accept new vectors incrementally without full rebuild.
/// This module handles:
/// 1. Taking a batch of new vectors from compaction
/// 2. Building a sub-graph on these vectors
/// 3. Connecting sub-graph to existing HNSW graph
/// 4. Updating graph structure efficiently

use std::collections::HashMap;
use crate::core::errors::Result;
use crate::vector::types::Vector;
use crate::buffer::write_buffer::VectorEntry;

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
}

impl Default for LayerBuilderConfig {
    fn default() -> Self {
        LayerBuilderConfig {
            max_layer_size: 10_000,      // Build sub-graphs of 10K vectors
            layer_m: 16,                 // Connections per vector
            ef_construction: 200,        // Search width during insertion
            optimize_connections: true,
        }
    }
}

/// A sub-graph layer with vectors and connections
#[derive(Debug, Clone)]
pub struct SubGraphLayer {
    pub layer_id: u64,
    pub vectors: HashMap<String, Vector>,
    /// Adjacency list: id -> list of connected ids
    pub connections: HashMap<String, Vec<String>>,
    pub entry_point: Option<String>,  // Which vector is the entry point?
}

impl SubGraphLayer {
    /// Create new sub-graph layer
    pub fn new(layer_id: u64) -> Self {
        SubGraphLayer {
            layer_id,
            vectors: HashMap::new(),
            connections: HashMap::new(),
            entry_point: None,
        }
    }

    /// Add vector to sub-graph
    pub fn add_vector(&mut self, id: String, vector: Vector) {
        self.vectors.insert(id.clone(), vector);
        self.connections.entry(id.clone()).or_insert_with(Vec::new);
        
        // First vector becomes entry point
        if self.entry_point.is_none() {
            self.entry_point = Some(id);
        }
    }

    /// Add connection between two vectors
    pub fn add_connection(&mut self, from: String, to: String) {
        self.connections
            .entry(from)
            .or_insert_with(Vec::new)
            .push(to);
    }

    /// Get vector count
    pub fn len(&self) -> usize {
        self.vectors.len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.vectors.is_empty()
    }
}

/// IncrementalLayerBuilder: Builds and integrates sub-graphs
pub struct IncrementalLayerBuilder {
    config: LayerBuilderConfig,
    layer_counter: u64,
}

impl IncrementalLayerBuilder {
    /// Create new builder
    pub fn new(config: LayerBuilderConfig) -> Self {
        IncrementalLayerBuilder {
            config,
            layer_counter: 0,
        }
    }

    /// Build a sub-graph from a batch of entries
    ///
    /// This performs:
    /// 1. Extract vectors from entries
    /// 2. Build HNSW sub-graph on these vectors alone
    /// 3. Return sub-graph ready for merging into main graph
    pub fn build_subgraph(&mut self, entries: &[VectorEntry]) -> Result<SubGraphLayer> {
        if entries.is_empty() {
            return Ok(SubGraphLayer::new(self.layer_counter));
        }

        let mut layer = SubGraphLayer::new(self.layer_counter);
        self.layer_counter += 1;

        // Add all vectors to layer
        for entry in entries {
            layer.add_vector(entry.id.clone(), entry.vector.clone());
        }

        // Build simple connectivity based on nearest neighbors
        // For each vector, find M nearest neighbors within this layer
        for (i, entry1) in entries.iter().enumerate() {
            let mut neighbors: Vec<(String, f32)> = Vec::new();

            for (j, entry2) in entries.iter().enumerate() {
                if i == j {
                    continue;
                }

                // L2 distance
                let dist = euclidean_distance(&entry1.vector.data, &entry2.vector.data);
                neighbors.push((entry2.id.clone(), dist));
            }

            // Sort by distance and keep top M
            neighbors.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
            neighbors.truncate(self.config.layer_m);

            // Add connections
            for (neighbor_id, _) in neighbors {
                layer.add_connection(entry1.id.clone(), neighbor_id);
            }
        }

        Ok(layer)
    }

    /// Build multiple sub-graphs for a large batch
    ///
    /// Splits batch into smaller chunks if needed to avoid memory issues
    pub fn build_subgraphs(&mut self, entries: &[VectorEntry]) -> Result<Vec<SubGraphLayer>> {
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

    /// (Placeholder) Merge sub-graph into main HNSW index
    ///
    /// This is where the magic happens: existing HNSW + new sub-graph = updated HNSW
    /// Real implementation would:
    /// 1. Find entry points between sub-graph and main graph
    /// 2. Create bidirectional links
    /// 3. Optimize connection count (remove worst connections if needed)
    /// 4. Update graph statistics
    pub fn merge_into_hnsw(&self, _subgraph: &SubGraphLayer) -> Result<()> {
        // Placeholder: actual merge logic would integrate with HNSWEngine
        // For now, just verify the subgraph is valid
        if _subgraph.is_empty() {
            return Ok(());
        }

        eprintln!(
            "Would merge sub-graph {} with {} vectors into HNSW",
            _subgraph.layer_id,
            _subgraph.len()
        );

        Ok(())
    }

    /// Get statistics about layers built so far
    pub fn get_stats(&self) -> (u64, usize) {
        (self.layer_counter, self.config.layer_m)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::schema::Metadata;

    #[test]
    fn test_subgraph_layer_creation() {
        let mut layer = SubGraphLayer::new(0);
        assert!(layer.is_empty());

        layer.add_vector("v1".to_string(), Vector::new(vec![0.0; 128]));
        assert_eq!(layer.len(), 1);
        assert_eq!(layer.entry_point, Some("v1".to_string()));
    }

    #[test]
    fn test_subgraph_connections() {
        let mut layer = SubGraphLayer::new(0);
        layer.add_vector("v1".to_string(), Vector::new(vec![0.0; 128]));
        layer.add_vector("v2".to_string(), Vector::new(vec![1.0; 128]));

        layer.add_connection("v1".to_string(), "v2".to_string());
        layer.add_connection("v1".to_string(), "v2".to_string()); // Duplicate

        assert_eq!(layer.connections["v1"].len(), 2); // Duplicates allowed
    }

    #[test]
    fn test_builder_config_default() {
        let config = LayerBuilderConfig::default();
        assert_eq!(config.max_layer_size, 10_000);
        assert_eq!(config.layer_m, 16);
    }

    #[test]
    fn test_builder_build_subgraph() {
        let config = LayerBuilderConfig::default();
        let mut builder = IncrementalLayerBuilder::new(config);

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
    }

    #[test]
    fn test_builder_build_multiple_subgraphs() {
        let mut config = LayerBuilderConfig::default();
        config.max_layer_size = 2; // Small chunks

        let mut builder = IncrementalLayerBuilder::new(config);

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
    fn test_euclidean_distance() {
        let a = vec![0.0, 0.0, 0.0];
        let b = vec![3.0, 4.0, 0.0];
        let dist = euclidean_distance(&a, &b);
        assert!((dist - 5.0).abs() < 0.001); // 3-4-5 triangle
    }

    #[test]
    fn test_builder_merge_placeholder() {
        let config = LayerBuilderConfig::default();
        let builder = IncrementalLayerBuilder::new(config);

        let mut layer = SubGraphLayer::new(0);
        layer.add_vector("v1".to_string(), Vector::new(vec![0.0; 128]));

        assert!(builder.merge_into_hnsw(&layer).is_ok());
    }
}
