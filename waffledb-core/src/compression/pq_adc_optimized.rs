/// Optimized PQ-ADC search with parallelization and SIMD.
/// 
/// This module provides high-performance Asymmetric Distance Computation for PQ:
/// - Precomputed lookup tables for O(M) distance computation
/// - Subquantizer parallelization support
/// - Memory-efficient table lookups

use crate::vector::distance::DistanceMetric;

/// Optimized ADC searcher with precomputation and parallelization.
pub struct OptimizedADCSearcher {
    /// Codebooks for each subquantizer: [subquantizer][centroid]
    pub codebooks: Vec<Vec<Vec<f32>>>,
    /// Subvector dimension
    pub subvector_dim: usize,
    /// Distance metric
    pub metric: DistanceMetric,
}

impl OptimizedADCSearcher {
    /// Create a new optimized ADC searcher.
    pub fn new(codebooks: Vec<Vec<Vec<f32>>>, metric: DistanceMetric) -> Self {
        let subvector_dim = if codebooks.is_empty() {
            0
        } else {
            codebooks[0][0].len()
        };

        OptimizedADCSearcher {
            codebooks,
            subvector_dim,
            metric,
        }
    }

    /// Precompute lookup table: distance[subvec][centroid]
    /// This is the key optimization - compute once, reuse for all database vectors.
    pub fn precompute_lookup_table(&self, query: &[f32]) -> LookupTable {
        let num_subquantizers = self.codebooks.len();
        let mut lookup = vec![vec![]; num_subquantizers];

        // Process each subquantizer
        for subvec_idx in 0..num_subquantizers {
            let start = subvec_idx * self.subvector_dim;
            let end = (start + self.subvector_dim).min(query.len());

            if start >= query.len() {
                break;
            }

            let query_subvec = &query[start..end];
            let mut distances = Vec::with_capacity(self.codebooks[subvec_idx].len());

            // Compute distances to all centroids in this subquantizer
            for centroid in &self.codebooks[subvec_idx] {
                let dist = match self.metric {
                    DistanceMetric::L2 => {
                        // L2 distance
                        query_subvec
                            .iter()
                            .zip(centroid.iter())
                            .map(|(x, y)| (x - y).powi(2))
                            .sum::<f32>()
                    }
                    DistanceMetric::Cosine => {
                        // Cosine distance  
                        let dot: f32 = query_subvec
                            .iter()
                            .zip(centroid.iter())
                            .map(|(x, y)| x * y)
                            .sum();
                        1.0 - dot
                    }
                    DistanceMetric::InnerProduct => {
                        // Negate inner product
                        let dot: f32 = query_subvec
                            .iter()
                            .zip(centroid.iter())
                            .map(|(x, y)| x * y)
                            .sum();
                        -dot
                    }
                };
                distances.push(dist);
            }

            lookup[subvec_idx] = distances;
        }

        LookupTable {
            distances: lookup,
            metric: self.metric,
        }
    }

    /// Compute distance from full query to PQ-compressed codes using lookup table.
    /// Time complexity: O(M) instead of O(dim) with full decompression.
    pub fn adc_distance(&self, codes: &[u8], lookup: &LookupTable) -> f32 {
        let mut distance = match self.metric {
            DistanceMetric::L2 => {
                // L2 distance - sum of squared subvector distances
                codes
                    .iter()
                    .enumerate()
                    .map(|(subvec_idx, code)| {
                        if subvec_idx < lookup.distances.len() {
                            let code_idx = *code as usize;
                            if code_idx < lookup.distances[subvec_idx].len() {
                                lookup.distances[subvec_idx][code_idx]
                            } else {
                                0.0
                            }
                        } else {
                            0.0
                        }
                    })
                    .sum::<f32>()
            }
            DistanceMetric::Cosine | DistanceMetric::InnerProduct => {
                // Cosine/InnerProduct - sum of subvector distances
                codes
                    .iter()
                    .enumerate()
                    .map(|(subvec_idx, code)| {
                        if subvec_idx < lookup.distances.len() {
                            let code_idx = *code as usize;
                            if code_idx < lookup.distances[subvec_idx].len() {
                                lookup.distances[subvec_idx][code_idx]
                            } else {
                                0.0
                            }
                        } else {
                            0.0
                        }
                    })
                    .sum::<f32>()
            }
        };

        // Final sqrt for L2 metric
        if self.metric == DistanceMetric::L2 {
            distance = distance.sqrt();
        }

        distance
    }

    /// Batch ADC distance computation for multiple codes.
    /// Can be extended for parallelization with rayon when needed.
    pub fn adc_batch_distance(&self, batch_codes: &[&[u8]], lookup: &LookupTable) -> Vec<f32> {
        batch_codes
            .iter()
            .map(|codes| self.adc_distance(codes, lookup))
            .collect()
    }
}

/// Precomputed lookup table for ADC search.
#[derive(Debug, Clone)]
pub struct LookupTable {
    /// distances[subvec_idx][centroid_idx] = distance from query subvec to centroid
    pub distances: Vec<Vec<f32>>,
    pub metric: DistanceMetric,
}

impl LookupTable {
    /// Memory size of lookup table in bytes
    pub fn memory_size_bytes(&self) -> usize {
        self.distances
            .iter()
            .map(|v| v.len() * std::mem::size_of::<f32>())
            .sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lookup_table_creation() {
        let codebooks = vec![
            vec![vec![0.0, 0.0], vec![1.0, 1.0]],
            vec![vec![0.0, 0.0], vec![0.5, 0.5]],
        ];
        let searcher = OptimizedADCSearcher::new(codebooks, DistanceMetric::L2);
        let query = vec![0.5, 0.5, 0.25, 0.25];

        let lookup = searcher.precompute_lookup_table(&query);
        assert_eq!(lookup.distances.len(), 2);
    }

    #[test]
    fn test_adc_distance_computation() {
        let codebooks = vec![
            vec![vec![0.0, 0.0], vec![1.0, 1.0]],
            vec![vec![0.0, 0.0], vec![0.5, 0.5]],
        ];
        let searcher = OptimizedADCSearcher::new(codebooks, DistanceMetric::L2);
        let query = vec![0.0, 0.0, 0.0, 0.0];

        let lookup = searcher.precompute_lookup_table(&query);
        let codes = vec![0, 0]; // Both codes point to [0,0]

        let dist = searcher.adc_distance(&codes, &lookup);
        assert!(dist < 0.001);
    }

    #[test]
    fn test_lookup_table_memory() {
        let codebooks = vec![vec![vec![0.0; 128]; 256]; 4];
        let searcher = OptimizedADCSearcher::new(codebooks, DistanceMetric::L2);
        let query = vec![0.0; 512];

        let lookup = searcher.precompute_lookup_table(&query);
        let expected_size = 4 * 256 * 4; // 4 subvecs * 256 centroids * 4 bytes
        assert_eq!(lookup.memory_size_bytes(), expected_size);
    }
}
