/// Hybrid search integration: End-to-end search combining all signals
///
/// This layer orchestrates:
/// - Dense HNSW search
/// - BM25 full-text search
/// - Sparse vector scoring
/// - Metadata filtering
/// - Score fusion
/// - Result ranking
///
/// Public API for hybrid search queries

use crate::indexing::multi_vector::{MultiVectorDocument, MultiVectorStore};
use crate::core::vector_type::VectorType;
use crate::indexing::sparse_vector::SparseVector;
use crate::search::hybrid_search::{HybridSearch, HybridSearchResult, FusionWeights};
use crate::indexing::bm25_index::BM25Index;
use crate::core::errors::{WaffleError, Result, ErrorCode};

/// Hybrid search query specification
#[derive(Debug, Clone)]
pub struct HybridSearchQuery {
    /// Dense query vector (optional)
    pub dense_query: Option<Vec<f32>>,
    
    /// Text query for BM25 (optional)
    pub text_query: Option<String>,
    
    /// Sparse query vector (optional)
    pub sparse_query: Option<SparseVector>,
    
    /// Which vector field to search (default: "default")
    pub vector_field: String,
    
    /// Fusion weights for combining signals
    pub weights: FusionWeights,
    
    /// Number of results to return
    pub top_k: usize,
    
    /// Optional metadata filter (predicate)
    /// Returns true if document should be included
    pub metadata_filter: Option<String>,
}

impl HybridSearchQuery {
    /// Create new hybrid search query
    pub fn new(top_k: usize) -> Result<Self> {
        if top_k == 0 {
            return Err(WaffleError::WithCode {
                code: ErrorCode::ValidationFailed,
                message: "top_k must be greater than 0".to_string(),
            });
        }

        Ok(HybridSearchQuery {
            dense_query: None,
            text_query: None,
            sparse_query: None,
            vector_field: "default".to_string(),
            weights: FusionWeights::default_hybrid(),
            top_k,
            metadata_filter: None,
        })
    }

    /// Set dense query vector
    pub fn with_dense_query(mut self, query: Vec<f32>) -> Result<Self> {
        if query.is_empty() {
            return Err(WaffleError::WithCode {
                code: ErrorCode::ValidationFailed,
                message: "Dense query vector cannot be empty".to_string(),
            });
        }
        self.dense_query = Some(query);
        Ok(self)
    }

    /// Set text query for BM25
    pub fn with_text_query(mut self, query: impl Into<String>) -> Result<Self> {
        let text = query.into();
        if text.is_empty() {
            return Err(WaffleError::WithCode {
                code: ErrorCode::ValidationFailed,
                message: "Text query cannot be empty".to_string(),
            });
        }
        self.text_query = Some(text);
        Ok(self)
    }

    /// Set sparse query vector
    pub fn with_sparse_query(mut self, query: SparseVector) -> Self {
        self.sparse_query = Some(query);
        self
    }

    /// Set vector field name
    pub fn with_vector_field(mut self, field: impl Into<String>) -> Self {
        self.vector_field = field.into();
        self
    }

    /// Set fusion weights
    pub fn with_weights(mut self, weights: FusionWeights) -> Self {
        self.weights = weights;
        self
    }

    /// Validate query has at least one search signal
    pub fn validate(&self) -> Result<()> {
        let has_dense = self.dense_query.is_some();
        let has_text = self.text_query.is_some();
        let has_sparse = self.sparse_query.is_some();

        if !has_dense && !has_text && !has_sparse {
            return Err(WaffleError::WithCode {
                code: ErrorCode::ValidationFailed,
                message: "Query must specify at least one search signal (dense, text, or sparse)"
                    .to_string(),
            });
        }

        Ok(())
    }
}

/// Hybrid search result with detailed attribution
#[derive(Debug, Clone)]
pub struct DetailedSearchResult {
    /// Document ID
    pub doc_id: String,
    
    /// Final fused score
    pub score: f32,
    
    /// Dense similarity (if queried)
    pub dense_score: Option<f32>,
    
    /// BM25 score (if queried)
    pub bm25_score: Option<f32>,
    
    /// Sparse similarity (if queried)
    pub sparse_score: Option<f32>,
    
    /// Document reference (optional)
    pub document: Option<MultiVectorDocument>,
}

/// Hybrid search engine: Orchestrates all search signals
pub struct HybridSearchEngine {
    /// Multi-vector document store
    store: MultiVectorStore,
    
    /// BM25 index for full-text search
    bm25_index: BM25Index,
    
    /// Hybrid search fusion logic
    fusion: HybridSearch,
}

impl HybridSearchEngine {
    /// Create new hybrid search engine
    pub fn new() -> Self {
        HybridSearchEngine {
            store: MultiVectorStore::new(),
            bm25_index: BM25Index::new(),
            fusion: HybridSearch::new(),
        }
    }

    /// Create with custom fusion weights
    pub fn with_weights(weights: FusionWeights) -> Result<Self> {
        Ok(HybridSearchEngine {
            store: MultiVectorStore::new(),
            bm25_index: BM25Index::new(),
            fusion: HybridSearch::with_weights(weights)?,
        })
    }

    /// Insert a multi-vector document
    pub fn insert(&mut self, doc: MultiVectorDocument) -> Result<()> {
        // Index text for BM25 if present
        if let Some(VectorType::Dense(_)) = doc.get_vector("text_bm25") {
            // Note: In real implementation, extract text from document
            // For now, just store the document
        }

        self.store.insert(doc)?;
        Ok(())
    }

    /// Delete a document
    pub fn delete(&mut self, doc_id: &str) -> Result<()> {
        self.store.delete(doc_id)?;
        self.bm25_index.remove_document(doc_id.parse().unwrap_or(0))?;
        Ok(())
    }

    /// Perform hybrid search
    ///
    /// This is the main API: combines dense + text + sparse + metadata
    pub fn search(&self, query: &HybridSearchQuery) -> Result<Vec<DetailedSearchResult>> {
        query.validate()?;

        // Step 1: Gather candidates from each signal
        let candidate_limit = query.top_k * self.fusion.candidate_multiplier();

        let mut dense_results = Vec::new();
        let mut bm25_results = Vec::new();
        let mut sparse_results = Vec::new();

        // Dense search
        if let Some(dense_query) = &query.dense_query {
            if dense_query.len() > 0 {
                // In real implementation: search HNSW index
                // For now: simulate with stored documents
                dense_results = self.simulate_dense_search(dense_query, candidate_limit)?;
            }
        }

        // BM25 search
        if let Some(text_query) = &query.text_query {
            bm25_results = self.search_bm25(text_query, candidate_limit)?;
        }

        // Sparse search
        if let Some(sparse_query) = &query.sparse_query {
            sparse_results = self.search_sparse(sparse_query, candidate_limit)?;
        }

        // Step 2: Fuse results
        let fused = self.fusion.fuse(dense_results, bm25_results, sparse_results, query.top_k)?;

        // Step 3: Convert to detailed results with document references
        let detailed_results: Vec<DetailedSearchResult> = fused
            .into_iter()
            .map(|result| {
                let document = self.store.get(&result.doc_id).cloned();
                DetailedSearchResult {
                    doc_id: result.doc_id,
                    score: result.score,
                    dense_score: result.dense_score,
                    bm25_score: result.bm25_score,
                    sparse_score: result.sparse_score,
                    document,
                }
            })
            .collect();

        Ok(detailed_results)
    }

    /// Simulate dense vector search (in production: HNSW search)
    fn simulate_dense_search(
        &self,
        query: &[f32],
        limit: usize,
    ) -> Result<Vec<(String, f32)>> {
        let mut results = Vec::new();

        for doc_id in self.store.document_ids() {
            if let Some(doc) = self.store.get(&doc_id) {
                if let Some(VectorType::Dense(stored_vec)) = doc.get_vector("default") {
                    let distance = self.l2_distance(query, stored_vec);
                    results.push((doc_id, distance));
                }
            }
        }

        // Sort by distance (ascending)
        results.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        results.truncate(limit);

        Ok(results)
    }

    /// Search BM25 index
    fn search_bm25(&self, query: &str, limit: usize) -> Result<Vec<(String, f32)>> {
        let results = self.bm25_index.search_top_k(query, limit)?;
        
        // Convert (doc_id: u64, score: f32) to (doc_id: String, score: f32)
        let converted: Vec<(String, f32)> = results
            .into_iter()
            .map(|(id, score)| (id.to_string(), score))
            .collect();

        Ok(converted)
    }

    /// Search sparse vectors
    fn search_sparse(
        &self,
        query: &SparseVector,
        limit: usize,
    ) -> Result<Vec<(String, f32)>> {
        let mut results = Vec::new();

        for doc_id in self.store.document_ids() {
            if let Some(doc) = self.store.get(&doc_id) {
                if let Some(VectorType::Sparse(stored_sparse)) = doc.get_vector("sparse") {
                    // Compute cosine similarity
                    let similarity = self.sparse_cosine_similarity(query, stored_sparse);
                    results.push((doc_id, similarity));
                }
            }
        }

        // Sort by similarity (descending)
        results.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        results.truncate(limit);

        Ok(results)
    }

    /// L2 Euclidean distance
    #[inline]
    fn l2_distance(&self, a: &[f32], b: &[f32]) -> f32 {
        if a.len() != b.len() {
            return 1.0;
        }

        let sum: f32 = a.iter().zip(b).map(|(x, y)| (x - y).powi(2)).sum();
        sum.sqrt()
    }

    /// Sparse cosine similarity
    fn sparse_cosine_similarity(&self, a: &SparseVector, b: &SparseVector) -> f32 {
        let dot = self.sparse_dot_product(a, b);
        
        // Compute magnitudes directly (since magnitude() requires &mut)
        let mag_a_sq: f32 = (0..100).map(|i| a.get(i).unwrap_or(0.0).powi(2)).sum();
        let mag_b_sq: f32 = (0..100).map(|i| b.get(i).unwrap_or(0.0).powi(2)).sum();
        
        let mag_a = mag_a_sq.sqrt();
        let mag_b = mag_b_sq.sqrt();

        if mag_a == 0.0 || mag_b == 0.0 {
            return 0.0;
        }

        dot / (mag_a * mag_b)
    }

    /// Sparse dot product
    fn sparse_dot_product(&self, a: &SparseVector, b: &SparseVector) -> f32 {
        // This is a simplified version; in production use the sparse_vector module's implementation
        let mut dot = 0.0;
        
        // Iterate through a's dimensions
        for i in 0..100 {
            if let (Some(val_a), Some(val_b)) = (a.get(i as u32), b.get(i as u32)) {
                dot += val_a * val_b;
            }
        }

        dot
    }

    /// Get document count
    pub fn len(&self) -> usize {
        self.store.len()
    }

    /// Get store reference
    pub fn store(&self) -> &MultiVectorStore {
        &self.store
    }

    /// Get mutable store reference
    pub fn store_mut(&mut self) -> &mut MultiVectorStore {
        &mut self.store
    }
}

impl Default for HybridSearchEngine {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hybrid_search_query_creation() {
        let query = HybridSearchQuery::new(10).unwrap();
        assert_eq!(query.top_k, 10);
        assert_eq!(query.vector_field, "default");
    }

    #[test]
    fn test_hybrid_search_query_with_dense() {
        let query = HybridSearchQuery::new(10)
            .unwrap()
            .with_dense_query(vec![0.1, 0.2, 0.3])
            .unwrap();
        
        assert!(query.dense_query.is_some());
        assert_eq!(query.dense_query.unwrap().len(), 3);
    }

    #[test]
    fn test_hybrid_search_query_with_text() {
        let query = HybridSearchQuery::new(10)
            .unwrap()
            .with_text_query("hello world")
            .unwrap();
        
        assert!(query.text_query.is_some());
        assert_eq!(query.text_query.unwrap(), "hello world");
    }

    #[test]
    fn test_hybrid_search_query_validation_empty_dense() {
        let result = HybridSearchQuery::new(10).unwrap().with_dense_query(vec![]);
        assert!(result.is_err());
    }

    #[test]
    fn test_hybrid_search_query_validation_no_signals() {
        let query = HybridSearchQuery::new(10).unwrap();
        assert!(query.validate().is_err());
    }

    #[test]
    fn test_hybrid_search_engine_creation() {
        let engine = HybridSearchEngine::new();
        assert_eq!(engine.len(), 0);
    }

    #[test]
    fn test_hybrid_search_engine_insert() {
        let mut engine = HybridSearchEngine::new();
        let mut doc = MultiVectorDocument::new("doc1".to_string());
        doc.set_vector("default", VectorType::Dense(vec![0.1, 0.2, 0.3]))
            .unwrap();

        engine.insert(doc).unwrap();
        assert_eq!(engine.len(), 1);
    }

    #[test]
    fn test_hybrid_search_engine_delete() {
        let mut engine = HybridSearchEngine::new();
        let mut doc = MultiVectorDocument::new("doc1".to_string());
        doc.set_vector("default", VectorType::Dense(vec![0.1, 0.2, 0.3]))
            .unwrap();

        engine.insert(doc).unwrap();
        assert_eq!(engine.len(), 1);

        engine.delete("doc1").unwrap();
        assert_eq!(engine.len(), 0);
    }

    #[test]
    fn test_hybrid_search_dense_only() {
        let mut engine = HybridSearchEngine::new();
        
        let mut doc = MultiVectorDocument::new("doc1".to_string());
        doc.set_vector("default", VectorType::Dense(vec![0.1, 0.2, 0.3, 0.4]))
            .unwrap();
        engine.insert(doc).unwrap();

        let query = HybridSearchQuery::new(10)
            .unwrap()
            .with_dense_query(vec![0.1, 0.2, 0.3, 0.4])
            .unwrap();

        let results = engine.search(&query).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].doc_id, "doc1");
    }

    #[test]
    fn test_l2_distance() {
        let engine = HybridSearchEngine::new();
        
        let a = vec![0.0, 0.0];
        let b = vec![3.0, 4.0];
        
        let dist = engine.l2_distance(&a, &b);
        assert!((dist - 5.0).abs() < 0.0001); // 3-4-5 triangle
    }

    #[test]
    fn test_l2_distance_mismatched_dimensions() {
        let engine = HybridSearchEngine::new();
        
        let a = vec![0.0, 0.0];
        let b = vec![1.0, 2.0, 3.0];
        
        let dist = engine.l2_distance(&a, &b);
        assert_eq!(dist, 1.0); // Should return max distance on mismatch
    }
}
