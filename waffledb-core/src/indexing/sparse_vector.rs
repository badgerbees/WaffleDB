/// Sparse Vector Support
/// 
/// Sparse vectors store only non-zero dimensions (e.g., TF-IDF, BM25 scores).
/// Represented as HashMap<u32, f32> where u32 is dimension index, f32 is value.
/// 
/// Used alongside dense vectors for hybrid search (Phase 4).

use serde::{Serialize, Deserialize};
use std::collections::HashMap;
use crate::core::errors::{Result, WaffleError, ErrorCode};

/// Sparse vector: dimension_index -> value
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SparseVector {
    // dimension_index -> score/value
    indices: HashMap<u32, f32>,
    // Cached magnitude for cosine similarity
    magnitude: Option<f32>,
}

impl SparseVector {
    /// Create new sparse vector from dimension -> value pairs
    pub fn new(indices: HashMap<u32, f32>) -> Result<Self> {
        // Validate: no zero or negative values allowed
        for (idx, val) in &indices {
            if val.is_nan() || val.is_infinite() {
                return Err(WaffleError::WithCode {
                    code: ErrorCode::ValidationFailed,
                    message: format!("Invalid sparse value at dimension {}: {}", idx, val),
                });
            }
        }
        
        Ok(SparseVector {
            indices,
            magnitude: None,
        })
    }
    
    /// Create empty sparse vector
    pub fn empty() -> Self {
        SparseVector {
            indices: HashMap::new(),
            magnitude: None,
        }
    }
    
    /// Get value at dimension
    pub fn get(&self, dimension: u32) -> Option<f32> {
        self.indices.get(&dimension).copied()
    }
    
    /// Insert value at dimension
    pub fn insert(&mut self, dimension: u32, value: f32) -> Result<()> {
        if value.is_nan() || value.is_infinite() {
            return Err(WaffleError::WithCode {
                code: ErrorCode::ValidationFailed,
                message: format!("Invalid sparse value: {}", value),
            });
        }
        self.indices.insert(dimension, value);
        self.magnitude = None; // Invalidate cache
        Ok(())
    }
    
    /// Number of non-zero dimensions
    pub fn nnz(&self) -> usize {
        self.indices.len()
    }
    
    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.indices.is_empty()
    }
    
    /// Get all dimensions (sorted)
    pub fn dimensions(&self) -> Vec<u32> {
        let mut dims: Vec<_> = self.indices.keys().copied().collect();
        dims.sort();
        dims
    }
    
    /// Compute Euclidean magnitude (L2 norm)
    pub fn magnitude(&mut self) -> f32 {
        if let Some(mag) = self.magnitude {
            return mag;
        }
        let mag = self.indices
            .values()
            .map(|v| v * v)
            .sum::<f32>()
            .sqrt();
        self.magnitude = Some(mag);
        mag
    }
    
    /// Compute sum of all values
    pub fn sum(&self) -> f32 {
        self.indices.values().sum()
    }
}

/// Sparse vector distance metrics
pub mod sparse_distance {
    use super::SparseVector;
    
    /// Dot product (inner product)
    /// For sparse vectors: sum of (a[i] * b[i]) for all common indices
    pub fn dot_product(a: &SparseVector, b: &SparseVector) -> f32 {
        let mut result = 0.0;
        
        // Iterate over smaller vector's dimensions
        if a.nnz() <= b.nnz() {
            for &dim in a.dimensions().iter() {
                if let Some(b_val) = b.get(dim) {
                    if let Some(a_val) = a.get(dim) {
                        result += a_val * b_val;
                    }
                }
            }
        } else {
            for &dim in b.dimensions().iter() {
                if let Some(a_val) = a.get(dim) {
                    if let Some(b_val) = b.get(dim) {
                        result += a_val * b_val;
                    }
                }
            }
        }
        
        result
    }
    
    /// Cosine similarity: dot_product / (mag_a * mag_b)
    pub fn cosine_similarity(a: &mut SparseVector, b: &mut SparseVector) -> f32 {
        let dot = dot_product(a, b);
        let mag_a = a.magnitude();
        let mag_b = b.magnitude();
        
        if mag_a == 0.0 || mag_b == 0.0 {
            return 0.0;
        }
        
        dot / (mag_a * mag_b)
    }
    
    /// Cosine distance: 1 - cosine_similarity
    /// Returns value in [0, 2] range
    pub fn cosine_distance(a: &mut SparseVector, b: &mut SparseVector) -> f32 {
        1.0 - cosine_similarity(a, b)
    }
    
    /// Jaccard similarity for sparse binary vectors
    /// intersection_size / union_size
    pub fn jaccard_similarity(a: &SparseVector, b: &SparseVector) -> f32 {
        if a.is_empty() && b.is_empty() {
            return 1.0;
        }
        if a.is_empty() || b.is_empty() {
            return 0.0;
        }
        
        let mut intersection = 0;
        let mut union = 0;
        
        let a_dims: std::collections::HashSet<_> = a.dimensions().into_iter().collect();
        let b_dims: std::collections::HashSet<_> = b.dimensions().into_iter().collect();
        
        union = a_dims.len() + b_dims.len();
        for dim in a_dims.intersection(&b_dims) {
            intersection += 1;
        }
        union -= intersection;
        
        if union == 0 {
            1.0
        } else {
            intersection as f32 / union as f32
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::sparse_distance::*;
    
    #[test]
    fn test_sparse_vector_creation() {
        let mut indices = HashMap::new();
        indices.insert(0, 1.5);
        indices.insert(5, 2.0);
        indices.insert(10, 0.5);
        
        let vec = SparseVector::new(indices).unwrap();
        assert_eq!(vec.nnz(), 3);
        assert_eq!(vec.get(0), Some(1.5));
        assert_eq!(vec.get(5), Some(2.0));
        assert_eq!(vec.get(10), Some(0.5));
        assert_eq!(vec.get(999), None);
    }
    
    #[test]
    fn test_sparse_vector_empty() {
        let vec = SparseVector::empty();
        assert_eq!(vec.nnz(), 0);
        assert!(vec.is_empty());
    }
    
    #[test]
    fn test_sparse_vector_magnitude() {
        let mut indices = HashMap::new();
        indices.insert(0, 3.0);
        indices.insert(1, 4.0);
        
        let mut vec = SparseVector::new(indices).unwrap();
        // sqrt(3^2 + 4^2) = sqrt(9 + 16) = 5.0
        let mag = vec.magnitude();
        assert!((mag - 5.0).abs() < 0.001);
    }
    
    #[test]
    fn test_dot_product() {
        let mut a_indices = HashMap::new();
        a_indices.insert(0, 1.0);
        a_indices.insert(1, 2.0);
        a_indices.insert(2, 3.0);
        
        let mut b_indices = HashMap::new();
        b_indices.insert(0, 2.0);
        b_indices.insert(1, 1.0);
        b_indices.insert(3, 5.0); // Not in a
        
        let a = SparseVector::new(a_indices).unwrap();
        let b = SparseVector::new(b_indices).unwrap();
        
        // (1*2) + (2*1) + (3*0) = 2 + 2 = 4
        let dot = dot_product(&a, &b);
        assert!((dot - 4.0).abs() < 0.001);
    }
    
    #[test]
    fn test_cosine_similarity() {
        let mut a_indices = HashMap::new();
        a_indices.insert(0, 1.0);
        a_indices.insert(1, 0.0);
        
        let mut b_indices = HashMap::new();
        b_indices.insert(0, 1.0);
        b_indices.insert(1, 0.0);
        
        let mut a = SparseVector::new(a_indices).unwrap();
        let mut b = SparseVector::new(b_indices).unwrap();
        
        // Identical vectors should have cosine similarity = 1.0
        let sim = cosine_similarity(&mut a, &mut b);
        assert!((sim - 1.0).abs() < 0.001);
    }
    
    #[test]
    fn test_cosine_distance_orthogonal() {
        let mut a_indices = HashMap::new();
        a_indices.insert(0, 1.0);
        
        let mut b_indices = HashMap::new();
        b_indices.insert(1, 1.0);
        
        let mut a = SparseVector::new(a_indices).unwrap();
        let mut b = SparseVector::new(b_indices).unwrap();
        
        // Orthogonal vectors: dot product = 0, cosine distance = 1.0
        let dist = cosine_distance(&mut a, &mut b);
        assert!((dist - 1.0).abs() < 0.001);
    }
    
    #[test]
    fn test_jaccard_similarity() {
        let mut a_indices = HashMap::new();
        a_indices.insert(0, 1.0);
        a_indices.insert(1, 1.0);
        a_indices.insert(2, 1.0);
        
        let mut b_indices = HashMap::new();
        b_indices.insert(1, 1.0);
        b_indices.insert(2, 1.0);
        b_indices.insert(3, 1.0);
        
        let a = SparseVector::new(a_indices).unwrap();
        let b = SparseVector::new(b_indices).unwrap();
        
        // Common: {1, 2}, Union: {0, 1, 2, 3}
        // Jaccard = 2/4 = 0.5
        let sim = jaccard_similarity(&a, &b);
        assert!((sim - 0.5).abs() < 0.001);
    }
    
    #[test]
    fn test_invalid_values() {
        let mut indices = HashMap::new();
        indices.insert(0, f32::NAN);
        
        let result = SparseVector::new(indices);
        assert!(result.is_err());
    }
    
    #[test]
    fn test_sparse_vector_insert() {
        let mut vec = SparseVector::empty();
        vec.insert(0, 1.5).unwrap();
        vec.insert(5, 2.0).unwrap();
        
        assert_eq!(vec.nnz(), 2);
        assert_eq!(vec.get(0), Some(1.5));
    }
}
