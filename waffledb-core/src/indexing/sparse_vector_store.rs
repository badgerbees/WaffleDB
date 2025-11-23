/// Sparse Vector Storage and Retrieval
/// 
/// Stores sparse vectors separately from dense vectors.
/// Indexed by doc_id for fast lookup and scoring.

use std::collections::HashMap;
use crate::core::errors::{Result, WaffleError, ErrorCode};
use crate::indexing::sparse_vector::SparseVector;

/// Sparse vector store: doc_id -> SparseVector
pub struct SparseVectorStore {
    vectors: HashMap<u64, SparseVector>,
    // Track statistics
    total_nnz: usize,  // total non-zero elements
}

impl SparseVectorStore {
    pub fn new() -> Self {
        SparseVectorStore {
            vectors: HashMap::new(),
            total_nnz: 0,
        }
    }
    
    /// Insert sparse vector for document
    pub fn insert(&mut self, doc_id: u64, vector: SparseVector) -> Result<()> {
        if vector.nnz() == 0 {
            return Err(WaffleError::WithCode {
                code: ErrorCode::ValidationFailed,
                message: "Cannot store empty sparse vector".to_string(),
            });
        }
        
        // Update statistics
        if let Some(old) = self.vectors.get(&doc_id) {
            self.total_nnz = self.total_nnz.saturating_sub(old.nnz());
        }
        
        self.total_nnz += vector.nnz();
        self.vectors.insert(doc_id, vector);
        
        Ok(())
    }
    
    /// Get sparse vector for document
    pub fn get(&self, doc_id: u64) -> Option<&SparseVector> {
        self.vectors.get(&doc_id)
    }
    
    /// Delete sparse vector
    pub fn delete(&mut self, doc_id: u64) -> Result<()> {
        if let Some(vector) = self.vectors.remove(&doc_id) {
            self.total_nnz = self.total_nnz.saturating_sub(vector.nnz());
        }
        Ok(())
    }
    
    /// Check if document has sparse vector
    pub fn contains(&self, doc_id: u64) -> bool {
        self.vectors.contains_key(&doc_id)
    }
    
    /// Get all document IDs
    pub fn doc_ids(&self) -> Vec<u64> {
        self.vectors.keys().copied().collect()
    }
    
    /// Number of stored sparse vectors
    pub fn len(&self) -> usize {
        self.vectors.len()
    }
    
    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.vectors.is_empty()
    }
    
    /// Total number of non-zero elements across all vectors
    pub fn total_nnz(&self) -> usize {
        self.total_nnz
    }
    
    /// Average sparsity: average non-zero elements per vector
    pub fn avg_nnz(&self) -> f64 {
        if self.vectors.is_empty() {
            0.0
        } else {
            self.total_nnz as f64 / self.vectors.len() as f64
        }
    }
    
    /// Get top K results by sparse score
    pub fn score_candidates(
        &self,
        query: &SparseVector,
        candidate_ids: &[u64],
        k: usize,
        score_fn: fn(&SparseVector, &SparseVector) -> f32,
    ) -> Result<Vec<(u64, f32)>> {
        let mut scores = Vec::new();
        
        for &doc_id in candidate_ids {
            if let Some(doc_vec) = self.get(doc_id) {
                let score = score_fn(query, doc_vec);
                if score > 0.0 {  // Only include positive scores
                    scores.push((doc_id, score));
                }
            }
        }
        
        // Sort by score descending
        scores.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        scores.truncate(k);
        
        Ok(scores)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    
    fn create_test_vector(dims: &[(u32, f32)]) -> SparseVector {
        let mut indices = HashMap::new();
        for (dim, val) in dims {
            indices.insert(*dim, *val);
        }
        SparseVector::new(indices).unwrap()
    }
    
    #[test]
    fn test_sparse_store_insert_and_get() {
        let mut store = SparseVectorStore::new();
        let vec = create_test_vector(&[(0, 1.0), (1, 2.0)]);
        
        store.insert(1, vec.clone()).unwrap();
        
        assert_eq!(store.len(), 1);
        assert!(store.contains(1));
        assert_eq!(store.get(1).unwrap().nnz(), 2);
    }
    
    #[test]
    fn test_sparse_store_delete() {
        let mut store = SparseVectorStore::new();
        let vec = create_test_vector(&[(0, 1.0), (1, 2.0)]);
        
        store.insert(1, vec).unwrap();
        assert_eq!(store.len(), 1);
        
        store.delete(1).unwrap();
        assert_eq!(store.len(), 0);
        assert!(!store.contains(1));
    }
    
    #[test]
    fn test_sparse_store_statistics() {
        let mut store = SparseVectorStore::new();
        let vec1 = create_test_vector(&[(0, 1.0), (1, 2.0)]);
        let vec2 = create_test_vector(&[(2, 3.0), (3, 4.0), (4, 5.0)]);
        
        store.insert(1, vec1).unwrap();
        store.insert(2, vec2).unwrap();
        
        assert_eq!(store.len(), 2);
        assert_eq!(store.total_nnz(), 5); // 2 + 3
        assert!((store.avg_nnz() - 2.5).abs() < 0.01); // 5 / 2
    }
    
    #[test]
    fn test_sparse_store_doc_ids() {
        let mut store = SparseVectorStore::new();
        let vec = create_test_vector(&[(0, 1.0)]);
        
        store.insert(1, vec.clone()).unwrap();
        store.insert(3, vec.clone()).unwrap();
        store.insert(5, vec).unwrap();
        
        let mut ids = store.doc_ids();
        ids.sort();
        assert_eq!(ids, vec![1, 3, 5]);
    }
    
    #[test]
    fn test_sparse_store_empty_vector_rejected() {
        let mut store = SparseVectorStore::new();
        let empty_vec = SparseVector::empty();
        
        let result = store.insert(1, empty_vec);
        assert!(result.is_err());
        assert_eq!(store.len(), 0);
    }
    
    #[test]
    fn test_sparse_store_score_candidates() {
        use crate::indexing::sparse_vector::sparse_distance::dot_product;
        
        let mut store = SparseVectorStore::new();
        let vec1 = create_test_vector(&[(0, 1.0), (1, 2.0)]);
        let vec2 = create_test_vector(&[(0, 1.0), (2, 1.0)]);
        let vec3 = create_test_vector(&[(1, 1.0), (2, 1.0)]);
        
        store.insert(1, vec1).unwrap();
        store.insert(2, vec2).unwrap();
        store.insert(3, vec3).unwrap();
        
        let query = create_test_vector(&[(0, 1.0), (1, 1.0)]);
        let candidates = vec![1, 2, 3];
        
        let results = store.score_candidates(&query, &candidates, 10, |q, d| dot_product(q, d)).unwrap();
        
        // Results should be sorted by score descending
        assert!(!results.is_empty());
        for i in 0..results.len()-1 {
            assert!(results[i].1 >= results[i+1].1);
        }
    }
}
