/// IncrementalVectorizer: Update PQ codebooks without full rebuild
///
/// Key optimization: PQ codebooks are typically 30-50MB for 1M vectors.
/// Rather than rebuilding on each compaction:
/// - Keep existing codebook
/// - Add new vectors to existing codebook (may increase precision slightly)
/// - Or use periodic incremental refinement (background batch updates)

use std::sync::Arc;
use parking_lot::RwLock;
use crate::vector::types::Vector;
use crate::core::errors::Result;

/// Configuration for incremental PQ updates
#[derive(Debug, Clone)]
pub struct IncrementalVectorizerConfig {
    /// Rebuild codebook after N new vectors accumulated
    pub codebook_rebuild_threshold: usize,
    
    /// Max codebook size (prevents unbounded growth)
    pub max_codebook_vectors: usize,
    
    /// Enable incremental refinement (slower but more accurate)
    pub enable_refinement: bool,
}

impl Default for IncrementalVectorizerConfig {
    fn default() -> Self {
        IncrementalVectorizerConfig {
            codebook_rebuild_threshold: 100_000,  // Rebuild codebook after 100K new vectors
            max_codebook_vectors: 500_000,        // Cap codebook at 500K vectors
            enable_refinement: true,
        }
    }
}

/// Represents a single code in the PQ codebook
#[derive(Debug, Clone)]
pub struct CodebookCode {
    pub id: u32,
    pub centroid: Vec<f32>,
    pub vector_count: u64,
}

/// IncrementalVectorizer: Manages PQ codebook updates
pub struct IncrementalVectorizer {
    config: IncrementalVectorizerConfig,
    
    /// Current codebook (immutable reference)
    codebook: Arc<RwLock<Vec<CodebookCode>>>,
    
    /// Vectors added since last codebook rebuild
    vectors_since_rebuild: Arc<RwLock<usize>>,
    
    /// Total vectors quantized with this codebook
    quantized_count: Arc<RwLock<u64>>,
}

impl IncrementalVectorizer {
    /// Create new incremental vectorizer with existing codebook
    pub fn new(codebook: Vec<CodebookCode>, config: IncrementalVectorizerConfig) -> Self {
        let quantized_count = codebook.iter().map(|c| c.vector_count).sum();

        IncrementalVectorizer {
            config,
            codebook: Arc::new(RwLock::new(codebook)),
            vectors_since_rebuild: Arc::new(RwLock::new(0)),
            quantized_count: Arc::new(RwLock::new(quantized_count)),
        }
    }

    /// Get current codebook (immutable)
    pub fn get_codebook(&self) -> Vec<CodebookCode> {
        self.codebook.read().clone()
    }

    /// Check if codebook should be rebuilt
    pub fn should_rebuild_codebook(&self) -> bool {
        *self.vectors_since_rebuild.read() >= self.config.codebook_rebuild_threshold
    }

    /// Add new vectors for incremental update (non-blocking check)
    pub fn add_vectors_for_update(&self, count: usize) -> Result<()> {
        let mut vectors = self.vectors_since_rebuild.write();
        *vectors += count;

        // Check bounds
        if *vectors > self.config.codebook_rebuild_threshold {
            // Will trigger rebuild in main engine
            eprintln!(
                "Codebook rebuild needed: {} vectors since rebuild",
                *vectors
            );
        }

        Ok(())
    }

    /// Perform incremental codebook refinement (background task)
    ///
    /// This refines the existing codebook by:
    /// 1. Taking accumulated vectors since last rebuild
    /// 2. Re-clustering them against existing centroids
    /// 3. Updating centroid positions slightly (don't change centroids drastically)
    /// 4. Resetting counter for next cycle
    ///
    /// Returns: new codebook (or None if skipped due to config)
    pub async fn refine_codebook(&self, new_vectors: &[Vector]) -> Result<Option<Vec<CodebookCode>>> {
        if !self.config.enable_refinement {
            return Ok(None);
        }

        if new_vectors.is_empty() {
            return Ok(None);
        }

        let mut codebook = self.codebook.write();

        // Simple refinement: assign new vectors to nearest centroids
        // and update centroid positions slightly
        for vector in new_vectors {
            // Find nearest centroid
            let mut nearest_idx = 0;
            let mut nearest_dist = f32::MAX;

            for (i, code) in codebook.iter().enumerate() {
                let dist = euclidean_distance(&vector.data, &code.centroid);
                if dist < nearest_dist {
                    nearest_dist = dist;
                    nearest_idx = i;
                }
            }

            // Update nearest centroid (slowly)
            // alpha = 0.01 means new centroids move only 1% toward new vector
            let alpha = 0.01;
            let centroid = &mut codebook[nearest_idx].centroid;
            for (j, v) in vector.data.iter().enumerate() {
                centroid[j] = centroid[j] * (1.0 - alpha) + v * alpha;
            }

            codebook[nearest_idx].vector_count += 1;
        }

        // Reset counter
        *self.vectors_since_rebuild.write() = 0;

        // Update quantized count
        *self.quantized_count.write() =
            codebook.iter().map(|c| c.vector_count).sum();

        Ok(Some(codebook.clone()))
    }

    /// Get codebook statistics
    pub fn get_stats(&self) -> (usize, u64, usize) {
        (
            self.codebook.read().len(),                      // codebook size
            *self.quantized_count.read(),                    // total quantized vectors
            *self.vectors_since_rebuild.read(),              // pending vectors
        )
    }
}

/// Euclidean distance between two vectors
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

    #[test]
    fn test_incremental_vectorizer_new() {
        let codebook = vec![
            CodebookCode {
                id: 0,
                centroid: vec![0.0; 128],
                vector_count: 1000,
            },
            CodebookCode {
                id: 1,
                centroid: vec![1.0; 128],
                vector_count: 1500,
            },
        ];

        let config = IncrementalVectorizerConfig::default();
        let vectorizer = IncrementalVectorizer::new(codebook, config);

        let (codebook_size, quantized, pending) = vectorizer.get_stats();
        assert_eq!(codebook_size, 2);
        assert_eq!(quantized, 2500);
        assert_eq!(pending, 0);
    }

    #[test]
    fn test_should_rebuild_codebook() {
        let codebook = vec![CodebookCode {
            id: 0,
            centroid: vec![0.0; 128],
            vector_count: 1000,
        }];

        let mut config = IncrementalVectorizerConfig::default();
        config.codebook_rebuild_threshold = 100;

        let vectorizer = IncrementalVectorizer::new(codebook, config);

        // Add vectors below threshold
        assert!(vectorizer.add_vectors_for_update(50).is_ok());
        assert!(!vectorizer.should_rebuild_codebook());

        // Add more to exceed threshold
        assert!(vectorizer.add_vectors_for_update(60).is_ok());
        assert!(vectorizer.should_rebuild_codebook());
    }

    #[test]
    fn test_euclidean_distance() {
        let a = vec![0.0, 0.0, 0.0];
        let b = vec![3.0, 4.0, 0.0];
        let dist = euclidean_distance(&a, &b);
        assert!((dist - 5.0).abs() < 0.001); // 3-4-5 triangle
    }

    #[tokio::test]
    async fn test_refine_codebook() {
        let codebook = vec![CodebookCode {
            id: 0,
            centroid: vec![0.0, 0.0],
            vector_count: 100,
        }];

        let config = IncrementalVectorizerConfig::default();
        let vectorizer = IncrementalVectorizer::new(codebook, config);

        let new_vectors = vec![
            Vector::new(vec![0.1, 0.1]),
            Vector::new(vec![0.2, 0.2]),
        ];

        let result = vectorizer.refine_codebook(&new_vectors).await;
        assert!(result.is_ok());
        assert!(result.unwrap().is_some());

        let (_, quantized, pending) = vectorizer.get_stats();
        assert_eq!(quantized, 102); // 100 + 2 new
        assert_eq!(pending, 0); // Counter reset
    }
}
