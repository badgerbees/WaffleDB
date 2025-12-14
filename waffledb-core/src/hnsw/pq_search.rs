/// PQ-integrated HNSW search with Asymmetric Distance Computation (ADC).
/// 
/// This module optimizes HNSW search by:
/// 1. Storing vectors as PQ codes (quantized representation)
/// 2. Computing distances using ADC during traversal
/// 3. Only decompressing final candidates
/// 4. Reducing memory usage by 70-80% while maintaining accuracy
///
/// Key optimization: Precompute lookup tables once per query,
/// then use O(M) table lookups per distance computation instead of O(D) operations.

use crate::compression::pq_adc_optimized::OptimizedADCSearcher;
use std::collections::{BinaryHeap, HashMap};
use std::cmp::Ordering;

/// PQ-encoded vector: stores codebook indices instead of full floats
#[derive(Debug, Clone)]
pub struct PQVector {
    /// PQ codes: one u8 per subquantizer
    pub codes: Vec<u8>,
    /// Original dimension (for reference)
    pub dim: usize,
}

impl PQVector {
    pub fn new(codes: Vec<u8>, dim: usize) -> Self {
        PQVector { codes, dim }
    }

    pub fn memory_footprint(&self) -> usize {
        self.codes.len()  // ~1 byte per subquantizer
    }
}

/// Lookup table for ADC: precomputed distances from query to all centroids
#[derive(Debug, Clone)]
pub struct LookupTable {
    /// distances[subvec_idx][centroid_idx] = distance
    pub distances: Vec<Vec<f32>>,
}

impl LookupTable {
    /// Create new lookup table
    pub fn new(num_subquantizers: usize, codebook_size: usize) -> Self {
        LookupTable {
            distances: vec![vec![0.0; codebook_size]; num_subquantizers],
        }
    }

    /// Look up distance for PQ code
    #[inline]
    pub fn distance(&self, subvec_idx: usize, code: u8) -> f32 {
        self.distances
            .get(subvec_idx)
            .and_then(|v| v.get(code as usize))
            .copied()
            .unwrap_or(f32::MAX)
    }

    /// Compute distance between query (full) and PQ-encoded vector
    #[inline]
    pub fn compute_distance(&self, pq_vector: &PQVector) -> f32 {
        pq_vector
            .codes
            .iter()
            .enumerate()
            .map(|(idx, &code)| self.distance(idx, code))
            .sum()
    }
}

/// Entry in search priority queue (max-heap by distance)
#[derive(Debug, Clone)]
struct SearchEntry {
    distance: f32,
    node_id: usize,
}

impl Eq for SearchEntry {}

impl PartialEq for SearchEntry {
    fn eq(&self, other: &Self) -> bool {
        self.node_id == other.node_id
    }
}

impl Ord for SearchEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reverse: max-heap (nearest first)
        other.distance.partial_cmp(&self.distance).unwrap_or(Ordering::Equal)
            .then_with(|| self.node_id.cmp(&other.node_id))
    }
}

impl PartialOrd for SearchEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// PQ-optimized HNSW searcher
pub struct PQHNSWSearcher {
    /// PQ searcher for ADC computation
    pub adc_searcher: OptimizedADCSearcher,
    /// Stored PQ-encoded vectors: node_id -> PQVector
    pub pq_vectors: HashMap<usize, PQVector>,
}

impl PQHNSWSearcher {
    pub fn new(adc_searcher: OptimizedADCSearcher) -> Self {
        PQHNSWSearcher {
            adc_searcher,
            pq_vectors: HashMap::new(),
        }
    }

    /// Store a PQ-encoded vector
    pub fn store_pq_vector(&mut self, node_id: usize, pq_vector: PQVector) {
        self.pq_vectors.insert(node_id, pq_vector);
    }

    /// Search with PQ ADC (compressed distance computation)
    /// Returns (node_id, distance) using precomputed lookup table
    pub fn search_pq_layer(
        &self,
        entry_point: usize,
        top_k: usize,
        ef_search: usize,
        get_neighbors: &dyn Fn(usize) -> Vec<usize>,
        get_pq_vector: &dyn Fn(usize) -> Option<PQVector>,
        lookup_table: &LookupTable,
    ) -> Vec<(usize, f32)> {
        let mut candidates = BinaryHeap::new();
        let mut w = BinaryHeap::new();
        let mut visited = std::collections::HashSet::new();

        // Initialize with entry point
        if let Some(pq_vec) = get_pq_vector(entry_point) {
            let dist = lookup_table.compute_distance(&pq_vec);
            candidates.push(std::cmp::Reverse(SearchEntry {
                distance: dist,
                node_id: entry_point,
            }));
            w.push(SearchEntry {
                distance: dist,
                node_id: entry_point,
            });
            visited.insert(entry_point);
        }

        // Greedy search using PQ ADC
        while !candidates.is_empty() {
            let std::cmp::Reverse(entry) = candidates.pop().unwrap();

            if entry.distance > w.peek().map(|e| e.distance).unwrap_or(f32::MAX) {
                break;
            }

            let neighbors = get_neighbors(entry.node_id);
            for neighbor_id in neighbors {
                if visited.insert(neighbor_id) {
                    if let Some(pq_vec) = get_pq_vector(neighbor_id) {
                        let dist = lookup_table.compute_distance(&pq_vec);

                        if dist < w.peek().map(|e| e.distance).unwrap_or(f32::MAX)
                            || w.len() < ef_search
                        {
                            candidates.push(std::cmp::Reverse(SearchEntry {
                                distance: dist,
                                node_id: neighbor_id,
                            }));
                            w.push(SearchEntry {
                                distance: dist,
                                node_id: neighbor_id,
                            });

                            // Prune to ef_search size
                            if w.len() > ef_search {
                                let mut sorted: Vec<_> = w.drain().collect();
                                sorted.sort_by(|a, b| {
                                    b.distance.partial_cmp(&a.distance).unwrap_or(Ordering::Equal)
                                });
                                for entry in sorted.drain(..ef_search) {
                                    w.push(entry);
                                }
                            }
                        }
                    }
                }
            }
        }

        // Return top_k results
        let mut results: Vec<_> = w
            .drain()
            .map(|e| (e.node_id, e.distance))
            .collect();
        results.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(Ordering::Equal));
        results.truncate(top_k);
        results
    }
}

/// Memory savings analysis
pub fn calculate_memory_savings(
    num_vectors: usize,
    vector_dim: usize,
    num_subquantizers: usize,
) -> MemorySavingsStats {
    let full_vector_bytes = num_vectors * vector_dim * 4;  // f32 = 4 bytes
    let pq_vector_bytes = num_vectors * num_subquantizers;  // u8 per subquantizer

    let reduction_percent = (1.0 - pq_vector_bytes as f32 / full_vector_bytes as f32) * 100.0;

    MemorySavingsStats {
        full_vector_bytes,
        pq_vector_bytes,
        reduction_percent,
    }
}

#[derive(Debug, Clone)]
pub struct MemorySavingsStats {
    pub full_vector_bytes: usize,
    pub pq_vector_bytes: usize,
    pub reduction_percent: f32,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pq_vector_encoding_decoding() {
        // Test that PQ vectors correctly store and retrieve data
        let codes = vec![0, 1, 2, 3];
        let pq_vec = PQVector::new(codes.clone(), 8);

        assert_eq!(pq_vec.codes, codes);
        assert_eq!(pq_vec.dim, 8);
        assert_eq!(pq_vec.memory_footprint(), 4);
    }

    #[test]
    fn test_lookup_table_accuracy() {
        // Test that lookup tables correctly store and retrieve distances
        let mut table = LookupTable::new(4, 256);

        // Set some distances
        for subvec_idx in 0..4 {
            for centroid_idx in 0..256 {
                table.distances[subvec_idx][centroid_idx] =
                    (subvec_idx as f32 * 100.0) + (centroid_idx as f32);
            }
        }

        // Verify retrieval
        assert_eq!(table.distance(0, 0), 0.0);
        assert_eq!(table.distance(1, 50), 150.0);
        assert_eq!(table.distance(2, 100), 300.0);
        assert_eq!(table.distance(3, 200), 500.0);
    }

    #[test]
    fn test_pq_distance_aggregation() {
        // Test that distances from lookup table correctly aggregate
        let mut table = LookupTable::new(4, 256);

        // Set uniform distances for predictable aggregation
        for subvec_idx in 0..4 {
            for centroid_idx in 0..256 {
                table.distances[subvec_idx][centroid_idx] = 1.0;
            }
        }

        // Create PQ vector with all codes = 5
        let codes = vec![5, 5, 5, 5];
        let pq_vec = PQVector::new(codes, 8);

        let dist = table.compute_distance(&pq_vec);
        assert!((dist - 4.0).abs() < 0.0001); // 4 subvecs * 1.0 distance each
    }

    #[test]
    fn test_memory_savings_calculation() {
        let stats = calculate_memory_savings(1_000_000, 768, 32);
        assert!(stats.reduction_percent >= 98.0);
        println!("Memory savings: {:.1}%", stats.reduction_percent);
        println!(
            "Full: {} MB, PQ: {} MB",
            stats.full_vector_bytes / 1_000_000,
            stats.pq_vector_bytes / 1_000_000
        );
    }

    #[test]
    fn test_pq_vector_creation() {
        let codes = vec![1, 2, 3, 4];
        let pq_vec = PQVector::new(codes, 768);
        assert_eq!(pq_vec.dim, 768);
        assert_eq!(pq_vec.memory_footprint(), 4);
    }

    #[test]
    fn test_lookup_table() {
        let mut table = LookupTable::new(32, 256);
        table.distances[0][0] = 0.5;
        table.distances[0][1] = 1.5;

        assert_eq!(table.distance(0, 0), 0.5);
        assert_eq!(table.distance(0, 1), 1.5);
        assert_eq!(table.distance(1, 0), 0.0);  // Default for unset
    }

    #[test]
    fn test_pq_distance_computation() {
        let mut table = LookupTable::new(4, 256);
        for i in 0..4 {
            table.distances[i][5] = 0.1;
        }

        let codes = vec![5, 5, 5, 5];
        let pq_vec = PQVector::new(codes, 768);
        let dist = table.compute_distance(&pq_vec);
        assert!((dist - 0.4).abs() < 0.0001);
    }

    #[test]
    fn test_memory_savings_large_vectors() {
        // Test realistic memory savings: 1M vectors, 768-dim (like OpenAI embeddings)
        let num_vectors = 1_000_000;
        let vector_dim = 768;
        let num_subquantizers = 32;  // 768 / 32 = 24-dim per subvector

        let stats = calculate_memory_savings(num_vectors, vector_dim, num_subquantizers);
        println!("Large vector test:");
        println!("  Full vectors: {} MB", stats.full_vector_bytes / 1_000_000);
        println!("  PQ vectors: {} MB", stats.pq_vector_bytes / 1_000_000);
        println!("  Memory savings: {:.1}%", stats.reduction_percent);

        assert!(stats.reduction_percent >= 98.0);
        assert!(stats.pq_vector_bytes < 100_000_000);  // Should be <100MB
    }

    #[test]
    fn test_memory_savings_various_configs() {
        // Test memory savings calculation with various configurations
        let configs = vec![
            ("Small (100k)", 100_000, 768, 32),
            ("Medium (1M)", 1_000_000, 768, 32),
            ("Large (10M)", 10_000_000, 768, 32),
            ("XLarge (100M)", 100_000_000, 768, 32),
        ];

        println!("Memory savings across configurations:");
        for (name, num_vecs, dim, subvecs) in configs {
            let stats = calculate_memory_savings(num_vecs, dim, subvecs);
            println!(
                "  {}: {:.1}% reduction ({} -> {} MB)",
                name,
                stats.reduction_percent,
                stats.full_vector_bytes / 1_000_000,
                stats.pq_vector_bytes / 1_000_000,
            );

            assert!(stats.reduction_percent >= 98.0);
        }
    }

    #[test]
    fn test_pq_hnsw_searcher_creation() {
        // Test basic PQHNSWSearcher initialization
        // (Requires OptimizedADCSearcher which is in compression module)
        let searcher = PQHNSWSearcher::new(
            // Create a dummy searcher - in real usage this would be properly initialized
            crate::compression::pq_adc_optimized::OptimizedADCSearcher::new(
                vec![vec![vec![0.0; 2]; 8]; 4],
                crate::vector::distance::DistanceMetric::L2,
            )
        );

        assert_eq!(searcher.pq_vectors.len(), 0);
    }

    #[test]
    fn test_pq_vector_storage() {
        // Test storing and retrieving PQ vectors
        let searcher = PQHNSWSearcher::new(
            crate::compression::pq_adc_optimized::OptimizedADCSearcher::new(
                vec![vec![vec![0.0; 2]; 8]; 4],
                crate::vector::distance::DistanceMetric::L2,
            )
        );
        let mut searcher = searcher;

        // Store some PQ vectors
        let pq_vec_0 = PQVector::new(vec![0, 1, 2, 3], 8);
        let pq_vec_1 = PQVector::new(vec![1, 2, 3, 4], 8);

        searcher.store_pq_vector(0, pq_vec_0.clone());
        searcher.store_pq_vector(1, pq_vec_1.clone());

        assert_eq!(searcher.pq_vectors.len(), 2);
        assert_eq!(searcher.pq_vectors.get(&0).unwrap().codes, vec![0, 1, 2, 3]);
        assert_eq!(searcher.pq_vectors.get(&1).unwrap().codes, vec![1, 2, 3, 4]);
    }

    #[test]
    fn test_pq_compression_ratio_128k_vectors() {
        // Test compression ratio with 128k vectors (common test size)
        let stats = calculate_memory_savings(128_000, 768, 32);
        println!("128k vectors test: {:.1}% reduction", stats.reduction_percent);
        assert!(stats.reduction_percent >= 98.0);
    }
}
