/// Unified Vector Type - supports Dense and Sparse
/// 
/// Allows documents to have either dense vectors (for HNSW),
/// sparse vectors (for BM25/TF-IDF), or both (for hybrid search).

use crate::indexing::sparse_vector::SparseVector;
use crate::core::errors::{Result, WaffleError, ErrorCode};
use serde::{Serialize, Deserialize};

/// Unified vector type
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum VectorType {
    /// Dense vector for HNSW indexing
    Dense(Vec<f32>),
    
    /// Sparse vector for BM25/TF-IDF scoring
    Sparse(SparseVector),
}

impl VectorType {
    /// Create dense vector
    pub fn dense(vec: Vec<f32>) -> Result<Self> {
        if vec.is_empty() {
            return Err(WaffleError::WithCode {
                code: ErrorCode::VectorDimensionInvalid,
                message: "Vector cannot be empty".to_string(),
            });
        }
        
        for (i, val) in vec.iter().enumerate() {
            if val.is_nan() || val.is_infinite() {
                return Err(WaffleError::WithCode {
                    code: ErrorCode::ValidationFailed,
                    message: format!("Invalid value at dimension {}: {}", i, val),
                });
            }
        }
        
        Ok(VectorType::Dense(vec))
    }
    
    /// Create sparse vector
    pub fn sparse(vec: SparseVector) -> Result<Self> {
        if vec.is_empty() {
            return Err(WaffleError::WithCode {
                code: ErrorCode::ValidationFailed,
                message: "Sparse vector cannot be empty".to_string(),
            });
        }
        Ok(VectorType::Sparse(vec))
    }
    
    /// Get as dense vector (if applicable)
    pub fn as_dense(&self) -> Option<&Vec<f32>> {
        match self {
            VectorType::Dense(v) => Some(v),
            _ => None,
        }
    }
    
    /// Get as sparse vector (if applicable)
    pub fn as_sparse(&self) -> Option<&SparseVector> {
        match self {
            VectorType::Sparse(v) => Some(v),
            _ => None,
        }
    }
    
    /// Check if dense
    pub fn is_dense(&self) -> bool {
        matches!(self, VectorType::Dense(_))
    }
    
    /// Check if sparse
    pub fn is_sparse(&self) -> bool {
        matches!(self, VectorType::Sparse(_))
    }
    
    /// Get dimension count (for dense) or non-zero count (for sparse)
    pub fn dimension(&self) -> usize {
        match self {
            VectorType::Dense(v) => v.len(),
            VectorType::Sparse(v) => v.nnz(),
        }
    }
}

/// Document with unified vectors (can have both dense and sparse)
#[derive(Debug, Clone)]
pub struct VectorDocument {
    pub id: u64,
    pub dense: Option<VectorType>,
    pub sparse: Option<VectorType>,
}

impl VectorDocument {
    pub fn new(id: u64) -> Self {
        VectorDocument {
            id,
            dense: None,
            sparse: None,
        }
    }
    
    pub fn with_dense(id: u64, dense: VectorType) -> Result<Self> {
        if !dense.is_dense() {
            return Err(WaffleError::WithCode {
                code: ErrorCode::ValidationFailed,
                message: "Expected dense vector".to_string(),
            });
        }
        Ok(VectorDocument {
            id,
            dense: Some(dense),
            sparse: None,
        })
    }
    
    pub fn with_sparse(id: u64, sparse: VectorType) -> Result<Self> {
        if !sparse.is_sparse() {
            return Err(WaffleError::WithCode {
                code: ErrorCode::ValidationFailed,
                message: "Expected sparse vector".to_string(),
            });
        }
        Ok(VectorDocument {
            id,
            dense: None,
            sparse: Some(sparse),
        })
    }
    
    pub fn add_dense(&mut self, dense: VectorType) -> Result<()> {
        if !dense.is_dense() {
            return Err(WaffleError::WithCode {
                code: ErrorCode::ValidationFailed,
                message: "Expected dense vector".to_string(),
            });
        }
        self.dense = Some(dense);
        Ok(())
    }
    
    pub fn add_sparse(&mut self, sparse: VectorType) -> Result<()> {
        if !sparse.is_sparse() {
            return Err(WaffleError::WithCode {
                code: ErrorCode::ValidationFailed,
                message: "Expected sparse vector".to_string(),
            });
        }
        self.sparse = Some(sparse);
        Ok(())
    }
    
    /// Check if has any vector
    pub fn has_vector(&self) -> bool {
        self.dense.is_some() || self.sparse.is_some()
    }
    
    /// Check if hybrid (has both dense and sparse)
    pub fn is_hybrid(&self) -> bool {
        self.dense.is_some() && self.sparse.is_some()
    }
    
    /// Check if dense-only
    pub fn is_dense_only(&self) -> bool {
        self.dense.is_some() && self.sparse.is_none()
    }
    
    /// Check if sparse-only
    pub fn is_sparse_only(&self) -> bool {
        self.dense.is_none() && self.sparse.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    
    #[test]
    fn test_vector_type_dense() {
        let vec = VectorType::dense(vec![0.1, 0.2, 0.3]).unwrap();
        assert!(vec.is_dense());
        assert!(!vec.is_sparse());
        assert_eq!(vec.dimension(), 3);
        assert!(vec.as_dense().is_some());
        assert!(vec.as_sparse().is_none());
    }
    
    #[test]
    fn test_vector_type_sparse() {
        let mut indices = HashMap::new();
        indices.insert(0, 1.5);
        indices.insert(5, 2.0);
        
        let sparse_vec = SparseVector::new(indices).unwrap();
        let vec = VectorType::sparse(sparse_vec).unwrap();
        
        assert!(vec.is_sparse());
        assert!(!vec.is_dense());
        assert_eq!(vec.dimension(), 2);
        assert!(vec.as_sparse().is_some());
        assert!(vec.as_dense().is_none());
    }
    
    #[test]
    fn test_vector_type_dense_empty_rejected() {
        let result = VectorType::dense(vec![]);
        assert!(result.is_err());
    }
    
    #[test]
    fn test_vector_type_dense_invalid_value() {
        let result = VectorType::dense(vec![0.1, f32::NAN, 0.3]);
        assert!(result.is_err());
    }
    
    #[test]
    fn test_document_dense_only() {
        let vec = VectorType::dense(vec![0.1, 0.2, 0.3]).unwrap();
        let doc = VectorDocument::with_dense(1, vec).unwrap();
        
        assert_eq!(doc.id, 1);
        assert!(doc.is_dense_only());
        assert!(!doc.is_sparse_only());
        assert!(!doc.is_hybrid());
        assert!(doc.has_vector());
    }
    
    #[test]
    fn test_document_sparse_only() {
        let mut indices = HashMap::new();
        indices.insert(0, 1.0);
        let sparse_vec = SparseVector::new(indices).unwrap();
        let vec = VectorType::sparse(sparse_vec).unwrap();
        let doc = VectorDocument::with_sparse(1, vec).unwrap();
        
        assert_eq!(doc.id, 1);
        assert!(doc.is_sparse_only());
        assert!(!doc.is_dense_only());
        assert!(!doc.is_hybrid());
        assert!(doc.has_vector());
    }
    
    #[test]
    fn test_document_hybrid() {
        let mut doc = VectorDocument::new(1);
        
        let dense = VectorType::dense(vec![0.1, 0.2]).unwrap();
        let mut indices = HashMap::new();
        indices.insert(0, 1.0);
        let sparse = VectorType::sparse(SparseVector::new(indices).unwrap()).unwrap();
        
        doc.add_dense(dense).unwrap();
        doc.add_sparse(sparse).unwrap();
        
        assert!(doc.is_hybrid());
        assert!(!doc.is_dense_only());
        assert!(!doc.is_sparse_only());
        assert!(doc.has_vector());
    }
}
