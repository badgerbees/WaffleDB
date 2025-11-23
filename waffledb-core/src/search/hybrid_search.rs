/// Hybrid search fusion: Combine dense + sparse + BM25
///
/// Algorithm:
/// 1. Dense search via HNSW: top 3K candidates
/// 2. BM25 search: top 3K candidates
/// 3. Sparse scoring: candidates from both
/// 4. Score fusion: weighted combination
/// 5. Apply filters: metadata, vector field
/// 6. Return top-k sorted by fused score
///
/// This enables enterprises to:
/// - Weight semantic relevance (dense)
/// - Weight lexical relevance (BM25)
/// - Weight sparse keyword matches
/// - Combine all signals into unified ranking

use std::collections::{HashMap, HashSet};
use crate::core::vector_type::VectorType;
use crate::indexing::sparse_vector::SparseVector;
use crate::core::errors::{WaffleError, Result, ErrorCode};
use crate::indexing::multi_vector::MultiVectorDocument;

/// Weights for fusion algorithm
#[derive(Debug, Clone, Copy)]
pub struct FusionWeights {
    /// Weight for dense semantic similarity (typically 0.5-0.8)
    pub dense_weight: f32,
    
    /// Weight for BM25 lexical relevance (typically 0.1-0.4)
    pub bm25_weight: f32,
    
    /// Weight for sparse keyword matching (typically 0.1-0.3)
    pub sparse_weight: f32,
}

impl FusionWeights {
    /// Create new fusion weights
    ///
    /// # Arguments
    /// * `dense_weight` - Semantic similarity weight (0-1)
    /// * `bm25_weight` - Lexical relevance weight (0-1)
    /// * `sparse_weight` - Keyword matching weight (0-1)
    ///
    /// Weights should sum to approximately 1.0 for interpretability,
    /// but the algorithm will normalize them automatically.
    pub fn new(dense_weight: f32, bm25_weight: f32, sparse_weight: f32) -> Result<Self> {
        // Validate weights are non-negative
        if dense_weight < 0.0 || bm25_weight < 0.0 || sparse_weight < 0.0 {
            return Err(WaffleError::WithCode {
                code: ErrorCode::ValidationFailed,
                message: "Weights must be non-negative".to_string(),
            });
        }

        // Validate at least one weight is non-zero
        let sum = dense_weight + bm25_weight + sparse_weight;
        if sum <= 0.0 {
            return Err(WaffleError::WithCode {
                code: ErrorCode::ValidationFailed,
                message: "At least one weight must be non-zero".to_string(),
            });
        }

        Ok(FusionWeights {
            dense_weight,
            bm25_weight,
            sparse_weight,
        })
    }

    /// Default weights: 70% dense, 20% BM25, 10% sparse
    pub fn default_hybrid() -> Self {
        FusionWeights {
            dense_weight: 0.7,
            bm25_weight: 0.2,
            sparse_weight: 0.1,
        }
    }

    /// Dense-focused weights: 80% dense, 15% BM25, 5% sparse
    pub fn dense_focused() -> Self {
        FusionWeights {
            dense_weight: 0.8,
            bm25_weight: 0.15,
            sparse_weight: 0.05,
        }
    }

    /// Balanced weights: 50% dense, 30% BM25, 20% sparse
    pub fn balanced() -> Self {
        FusionWeights {
            dense_weight: 0.5,
            bm25_weight: 0.3,
            sparse_weight: 0.2,
        }
    }

    /// BM25-focused weights: 40% dense, 50% BM25, 10% sparse
    pub fn bm25_focused() -> Self {
        FusionWeights {
            dense_weight: 0.4,
            bm25_weight: 0.5,
            sparse_weight: 0.1,
        }
    }

    /// Normalize weights to sum to 1.0
    pub fn normalize(&self) -> FusionWeights {
        let sum = self.dense_weight + self.bm25_weight + self.sparse_weight;
        FusionWeights {
            dense_weight: self.dense_weight / sum,
            bm25_weight: self.bm25_weight / sum,
            sparse_weight: self.sparse_weight / sum,
        }
    }
}

/// Hybrid search result
#[derive(Debug, Clone)]
pub struct HybridSearchResult {
    /// Document ID
    pub doc_id: String,
    
    /// Fused score (0-1 normalized)
    pub score: f32,
    
    /// Dense similarity score component (if available)
    pub dense_score: Option<f32>,
    
    /// BM25 score component (if available)
    pub bm25_score: Option<f32>,
    
    /// Sparse similarity score component (if available)
    pub sparse_score: Option<f32>,
}

/// Candidate accumulator for deduplication
struct HybridCandidate {
    doc_id: String,
    dense_score: Option<f32>,
    bm25_score: Option<f32>,
    sparse_score: Option<f32>,
}

impl HybridCandidate {
    /// Compute fused score from components
    fn fused_score(&self, weights: &FusionWeights) -> f32 {
        let weighted_sum = 
            self.dense_score.unwrap_or(0.0) * weights.dense_weight +
            self.bm25_score.unwrap_or(0.0) * weights.bm25_weight +
            self.sparse_score.unwrap_or(0.0) * weights.sparse_weight;

        // Normalize by sum of non-zero weights
        let weight_sum = 
            (if self.dense_score.is_some() { weights.dense_weight } else { 0.0 }) +
            (if self.bm25_score.is_some() { weights.bm25_weight } else { 0.0 }) +
            (if self.sparse_score.is_some() { weights.sparse_weight } else { 0.0 });

        if weight_sum > 0.0 {
            weighted_sum / weight_sum
        } else {
            0.0
        }
    }

    fn to_result(&self, weights: &FusionWeights) -> HybridSearchResult {
        HybridSearchResult {
            doc_id: self.doc_id.clone(),
            score: self.fused_score(weights),
            dense_score: self.dense_score,
            bm25_score: self.bm25_score,
            sparse_score: self.sparse_score,
        }
    }
}

/// Hybrid search pipeline: Fuse dense + sparse + BM25
pub struct HybridSearch {
    /// Fusion weights
    weights: FusionWeights,
    
    /// Candidate multiplier (fetch 3K instead of K for fusion)
    candidate_multiplier: usize,
}

impl HybridSearch {
    /// Create new hybrid search with default weights
    pub fn new() -> Self {
        HybridSearch {
            weights: FusionWeights::default_hybrid(),
            candidate_multiplier: 3,
        }
    }

    /// Create hybrid search with custom weights
    pub fn with_weights(weights: FusionWeights) -> Result<Self> {
        Ok(HybridSearch {
            weights: weights.normalize(),
            candidate_multiplier: 3,
        })
    }

    /// Set candidate multiplier (candidates_to_fetch = top_k * multiplier)
    pub fn set_candidate_multiplier(&mut self, multiplier: usize) -> Result<()> {
        if multiplier < 1 {
            return Err(WaffleError::WithCode {
                code: ErrorCode::ValidationFailed,
                message: "Candidate multiplier must be at least 1".to_string(),
            });
        }
        self.candidate_multiplier = multiplier;
        Ok(())
    }

    /// Fuse search results from three independent ranking signals
    ///
    /// # Arguments
    /// * `dense_results` - Vec<(doc_id, distance)> from HNSW (smaller = better, so convert to similarity)
    /// * `bm25_results` - Vec<(doc_id, score)> from BM25 (higher = better, already [0,1])
    /// * `sparse_results` - Vec<(doc_id, similarity)> from sparse scoring (higher = better, [0,1])
    /// * `top_k` - Number of final results to return
    ///
    /// # Returns
    /// Vector of fused results sorted by combined score (highest first)
    pub fn fuse(
        &self,
        dense_results: Vec<(String, f32)>,
        bm25_results: Vec<(String, f32)>,
        sparse_results: Vec<(String, f32)>,
        top_k: usize,
    ) -> Result<Vec<HybridSearchResult>> {
        if top_k == 0 {
            return Err(WaffleError::WithCode {
                code: ErrorCode::ValidationFailed,
                message: "top_k must be greater than 0".to_string(),
            });
        }

        // Deduplicate candidates
        let mut candidates: HashMap<String, HybridCandidate> = HashMap::new();

        // Process dense results (convert distance to similarity)
        // Distance: 0 = identical, 1 = max distance
        // Similarity: 1 = identical, 0 = no similarity
        for (doc_id, distance) in dense_results {
            let similarity = 1.0 - distance.clamp(0.0, 1.0);
            candidates
                .entry(doc_id.clone())
                .or_insert_with(|| HybridCandidate {
                    doc_id,
                    dense_score: None,
                    bm25_score: None,
                    sparse_score: None,
                })
                .dense_score = Some(similarity);
        }

        // Process BM25 results (already normalized)
        for (doc_id, score) in bm25_results {
            candidates
                .entry(doc_id.clone())
                .or_insert_with(|| HybridCandidate {
                    doc_id,
                    dense_score: None,
                    bm25_score: None,
                    sparse_score: None,
                })
                .bm25_score = Some(score);
        }

        // Process sparse results (already normalized)
        for (doc_id, score) in sparse_results {
            candidates
                .entry(doc_id.clone())
                .or_insert_with(|| HybridCandidate {
                    doc_id,
                    dense_score: None,
                    bm25_score: None,
                    sparse_score: None,
                })
                .sparse_score = Some(score);
        }

        // Convert to results with fused scores
        let mut results: Vec<HybridSearchResult> = candidates
            .into_values()
            .map(|candidate| candidate.to_result(&self.weights))
            .collect();

        // Sort by fused score (descending)
        results.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        // Truncate to top_k
        results.truncate(top_k);

        Ok(results)
    }

    /// Get current weights
    pub fn weights(&self) -> &FusionWeights {
        &self.weights
    }

    /// Get candidate multiplier
    pub fn candidate_multiplier(&self) -> usize {
        self.candidate_multiplier
    }
}

impl Default for HybridSearch {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fusion_weights_creation() {
        let weights = FusionWeights::new(0.7, 0.2, 0.1).unwrap();
        assert_eq!(weights.dense_weight, 0.7);
        assert_eq!(weights.bm25_weight, 0.2);
        assert_eq!(weights.sparse_weight, 0.1);
    }

    #[test]
    fn test_fusion_weights_negative() {
        assert!(FusionWeights::new(-0.1, 0.5, 0.5).is_err());
        assert!(FusionWeights::new(0.5, -0.1, 0.5).is_err());
        assert!(FusionWeights::new(0.5, 0.5, -0.1).is_err());
    }

    #[test]
    fn test_fusion_weights_all_zero() {
        assert!(FusionWeights::new(0.0, 0.0, 0.0).is_err());
    }

    #[test]
    fn test_fusion_weights_normalize() {
        let weights = FusionWeights::new(7.0, 2.0, 1.0).unwrap();
        let normalized = weights.normalize();
        
        let sum = normalized.dense_weight + normalized.bm25_weight + normalized.sparse_weight;
        assert!((sum - 1.0).abs() < 0.0001);
    }

    #[test]
    fn test_fusion_weights_presets() {
        let dense_focused = FusionWeights::dense_focused();
        assert!(dense_focused.dense_weight > dense_focused.bm25_weight);
        
        let balanced = FusionWeights::balanced();
        assert!(balanced.dense_weight >= balanced.sparse_weight);
        
        let bm25_focused = FusionWeights::bm25_focused();
        assert!(bm25_focused.bm25_weight > bm25_focused.sparse_weight);
    }

    #[test]
    fn test_hybrid_search_new() {
        let search = HybridSearch::new();
        assert_eq!(search.candidate_multiplier(), 3);
    }

    #[test]
    fn test_hybrid_search_fuse_single_source() {
        let search = HybridSearch::new();
        
        let dense_results = vec![
            ("doc1".to_string(), 0.1),  // High similarity
            ("doc2".to_string(), 0.5),
        ];
        
        let results = search.fuse(dense_results, vec![], vec![], 2).unwrap();
        
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].doc_id, "doc1");
        assert!(results[0].score > results[1].score);
    }

    #[test]
    fn test_hybrid_search_fuse_deduplication() {
        let search = HybridSearch::new();
        
        let dense_results = vec![("doc1".to_string(), 0.2)];
        let bm25_results = vec![("doc1".to_string(), 0.8)];
        let sparse_results = vec![("doc1".to_string(), 0.5)];
        
        let results = search.fuse(dense_results, bm25_results, sparse_results, 10).unwrap();
        
        // Should have single result with all three scores
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].doc_id, "doc1");
        assert!(results[0].dense_score.is_some());
        assert!(results[0].bm25_score.is_some());
        assert!(results[0].sparse_score.is_some());
    }

    #[test]
    fn test_hybrid_search_fuse_mixed_documents() {
        let search = HybridSearch::new();
        
        let dense_results = vec![
            ("doc1".to_string(), 0.1),
            ("doc2".to_string(), 0.2),
        ];
        let bm25_results = vec![
            ("doc2".to_string(), 0.9),
            ("doc3".to_string(), 0.7),
        ];
        let sparse_results = vec![];
        
        let results = search.fuse(dense_results, bm25_results, sparse_results, 10).unwrap();
        
        // Should have 3 unique documents
        assert_eq!(results.len(), 3);
        let doc_ids: Vec<_> = results.iter().map(|r| r.doc_id.clone()).collect();
        assert!(doc_ids.contains(&"doc1".to_string()));
        assert!(doc_ids.contains(&"doc2".to_string()));
        assert!(doc_ids.contains(&"doc3".to_string()));
    }

    #[test]
    fn test_hybrid_search_fuse_top_k() {
        let search = HybridSearch::new();
        
        let dense_results = vec![
            ("doc1".to_string(), 0.1),
            ("doc2".to_string(), 0.2),
            ("doc3".to_string(), 0.3),
            ("doc4".to_string(), 0.4),
        ];
        
        let results = search.fuse(dense_results, vec![], vec![], 2).unwrap();
        
        // Should return only top 2
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].doc_id, "doc1");
        assert_eq!(results[1].doc_id, "doc2");
    }

    #[test]
    fn test_hybrid_search_fuse_score_normalization() {
        let search = HybridSearch::new();
        
        // Dense result: distance 0.3 → similarity 0.7
        let dense_results = vec![("doc1".to_string(), 0.3)];
        
        let results = search.fuse(dense_results, vec![], vec![], 1).unwrap();
        
        assert_eq!(results.len(), 1);
        let expected_dense_similarity = 0.7;
        assert!((results[0].dense_score.unwrap() - expected_dense_similarity).abs() < 0.0001);
    }

    #[test]
    fn test_hybrid_search_fuse_invalid_top_k() {
        let search = HybridSearch::new();
        
        let result = search.fuse(vec![], vec![], vec![], 0);
        assert!(result.is_err());
    }

    #[test]
    fn test_hybrid_candidate_fused_score() {
        let candidate = HybridCandidate {
            doc_id: "doc1".to_string(),
            dense_score: Some(0.8),
            bm25_score: Some(0.6),
            sparse_score: Some(0.4),
        };
        
        let weights = FusionWeights::default_hybrid();
        let score = candidate.fused_score(&weights);
        
        // 0.8*0.7 + 0.6*0.2 + 0.4*0.1 = 0.56 + 0.12 + 0.04 = 0.72
        assert!((score - 0.72).abs() < 0.0001);
    }

    #[test]
    fn test_hybrid_candidate_partial_scores() {
        let candidate = HybridCandidate {
            doc_id: "doc1".to_string(),
            dense_score: Some(0.8),
            bm25_score: None,
            sparse_score: Some(0.4),
        };
        
        let weights = FusionWeights::default_hybrid();
        let score = candidate.fused_score(&weights);
        
        // Should normalize by only dense_weight + sparse_weight
        // (0.8*0.7 + 0.4*0.1) / (0.7 + 0.1) = 0.6 / 0.8 = 0.75
        assert!((score - 0.75).abs() < 0.0001);
    }

    #[test]
    fn test_hybrid_search_set_candidate_multiplier() {
        let mut search = HybridSearch::new();
        
        search.set_candidate_multiplier(5).unwrap();
        assert_eq!(search.candidate_multiplier(), 5);
        
        assert!(search.set_candidate_multiplier(0).is_err());
    }
}
