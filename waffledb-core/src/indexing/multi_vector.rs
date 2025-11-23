/// Multi-vector storage: Support multiple embeddings per document
///
/// Architecture:
/// - HashMap<String, VectorType> per document (named vector slots)
/// - Default slot for backward compatibility
/// - Supports Dense, Sparse, and Hybrid vectors simultaneously
/// - Storage-efficient: sparse vectors use compact representation
///
/// Example:
/// ```ignore
/// let mut doc = MultiVectorDocument::new("doc123".to_string());
/// doc.set_vector("body", VectorType::Dense(body_vec))?;
/// doc.set_vector("title", VectorType::Dense(title_vec))?;
/// doc.set_vector("keywords", VectorType::Sparse(sparse_vec))?;
/// ```

use std::collections::HashMap;
use crate::core::vector_type::VectorType;
use crate::metadata::schema::Metadata;
use crate::core::errors::{WaffleError, Result, ErrorCode};

/// Document with multiple vector slots
#[derive(Debug, Clone)]
pub struct MultiVectorDocument {
    /// Unique document identifier
    pub id: String,
    
    /// Named vector slots (e.g., "body", "title", "code")
    /// HashMap<vector_name, VectorType>
    vectors: HashMap<String, VectorType>,
    
    /// Optional metadata (filtering, source info, etc.)
    pub metadata: Option<Metadata>,
    
    /// Insertion timestamp in milliseconds
    pub insertion_time: u64,
}

impl MultiVectorDocument {
    /// Create a new multi-vector document
    pub fn new(id: String) -> Self {
        MultiVectorDocument {
            id,
            vectors: HashMap::new(),
            metadata: None,
            insertion_time: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0),
        }
    }

    /// Set a named vector for this document
    ///
    /// Allows storing multiple embeddings:
    /// - "default" for backward compatibility
    /// - "body", "title", "summary" for text variants
    /// - "keywords" for sparse vectors
    /// - "text_bm25" for BM25 indexed text
    pub fn set_vector(&mut self, name: impl Into<String>, vector: VectorType) -> Result<()> {
        let name = name.into();
        
        // Validate name (alphanumeric + underscore)
        if name.is_empty() || name.len() > 128 {
            return Err(WaffleError::WithCode {
                code: ErrorCode::ValidationFailed,
                message: "Vector name must be 1-128 characters".to_string(),
            });
        }
        
        if !name.chars().all(|c| c.is_alphanumeric() || c == '_') {
            return Err(WaffleError::WithCode {
                code: ErrorCode::ValidationFailed,
                message: "Vector name must contain only alphanumeric characters and underscores".to_string(),
            });
        }

        self.vectors.insert(name, vector);
        Ok(())
    }

    /// Get a vector by name
    pub fn get_vector(&self, name: &str) -> Option<&VectorType> {
        self.vectors.get(name)
    }

    /// Get mutable reference to a vector
    pub fn get_vector_mut(&mut self, name: &str) -> Option<&mut VectorType> {
        self.vectors.get_mut(name)
    }

    /// Check if document has a vector with given name
    pub fn has_vector(&self, name: &str) -> bool {
        self.vectors.contains_key(name)
    }

    /// Get all vector names in this document
    pub fn vector_names(&self) -> Vec<String> {
        self.vectors.keys().cloned().collect()
    }

    /// Get number of vectors in document
    pub fn vector_count(&self) -> usize {
        self.vectors.len()
    }

    /// Remove a vector by name
    pub fn remove_vector(&mut self, name: &str) -> Option<VectorType> {
        self.vectors.remove(name)
    }

    /// Get dimension for a named vector
    pub fn dimension(&self, name: &str) -> Option<usize> {
        self.vectors.get(name).map(|v| v.dimension())
    }

    /// Check if all vectors have consistent dimensions per type
    ///
    /// Returns (dense_dimension, sparse_dimensions)
    /// Returns error if vectors have mismatched dimensions
    pub fn validate_dimensions(&self) -> Result<(Option<usize>, Option<usize>)> {
        let mut dense_dim = None;
        let mut sparse_dim = None;

        for (_name, vec_type) in &self.vectors {
            match vec_type {
                VectorType::Dense(v) => {
                    if let Some(existing_dim) = dense_dim {
                        if existing_dim != v.len() {
                            return Err(WaffleError::VectorDimensionMismatch {
                                expected: existing_dim,
                                got: v.len(),
                            });
                        }
                    } else {
                        dense_dim = Some(v.len());
                    }
                }
                VectorType::Sparse(_) => {
                    // Sparse vectors can have flexible dimensions, just track presence
                    sparse_dim = Some(1); // Mark that we have sparse vectors
                }
            }
        }

        Ok((dense_dim, sparse_dim))
    }

    /// Get total bytes used by all vectors (approximate)
    pub fn estimate_size_bytes(&self) -> usize {
        let mut total = 64; // Base overhead
        
        for (name, vec_type) in &self.vectors {
            total += name.len(); // Key size
            total += match vec_type {
                VectorType::Dense(v) => v.len() * 4, // f32 = 4 bytes
                VectorType::Sparse(s) => s.nnz() * 8, // (u32, f32) = 8 bytes per entry
            };
        }
        
        total
    }

    /// Set metadata for this document
    pub fn set_metadata(&mut self, metadata: Metadata) {
        self.metadata = Some(metadata);
    }

    /// Get metadata reference
    pub fn get_metadata(&self) -> Option<&Metadata> {
        self.metadata.as_ref()
    }

    /// Get all vectors as references
    pub fn all_vectors(&self) -> &HashMap<String, VectorType> {
        &self.vectors
    }
}

/// Multi-vector store: Persistent storage for multi-vector documents
#[derive(Debug)]
pub struct MultiVectorStore {
    /// HashMap<doc_id, MultiVectorDocument>
    documents: HashMap<String, MultiVectorDocument>,
    
    /// Statistics tracking
    stats: MultiVectorStats,
}

#[derive(Debug, Clone, Default)]
pub struct MultiVectorStats {
    pub total_documents: u64,
    pub total_vectors: u64,
    pub total_dense_vectors: u64,
    pub total_sparse_vectors: u64,
    pub total_size_bytes: u64,
}

impl MultiVectorStore {
    /// Create new multi-vector store
    pub fn new() -> Self {
        MultiVectorStore {
            documents: HashMap::new(),
            stats: MultiVectorStats::default(),
        }
    }

    /// Insert a multi-vector document
    pub fn insert(&mut self, doc: MultiVectorDocument) -> Result<()> {
        if doc.id.is_empty() {
            return Err(WaffleError::WithCode {
                code: ErrorCode::ValidationFailed,
                message: "Document ID cannot be empty".to_string(),
            });
        }

        // Validate dimensions before insert
        let (dense_dim, sparse_dim) = doc.validate_dimensions()?;

        // Update statistics
        let vector_count = doc.vector_count() as u64;
        let dense_count = if dense_dim.is_some() { 1 } else { 0 };
        let sparse_count = if sparse_dim.is_some() { 1 } else { 0 };
        let doc_size = doc.estimate_size_bytes() as u64;

        // Check if replacing existing document
        if let Some(old_doc) = self.documents.get(&doc.id) {
            self.stats.total_size_bytes -= old_doc.estimate_size_bytes() as u64;
            self.stats.total_vectors -= old_doc.vector_count() as u64;
        } else {
            self.stats.total_documents += 1;
        }

        self.documents.insert(doc.id.clone(), doc);
        
        self.stats.total_vectors += vector_count;
        self.stats.total_dense_vectors += dense_count;
        self.stats.total_sparse_vectors += sparse_count;
        self.stats.total_size_bytes += doc_size;

        Ok(())
    }

    /// Get a document by ID
    pub fn get(&self, id: &str) -> Option<&MultiVectorDocument> {
        self.documents.get(id)
    }

    /// Get mutable reference to document
    pub fn get_mut(&mut self, id: &str) -> Option<&mut MultiVectorDocument> {
        self.documents.get_mut(id)
    }

    /// Delete a document by ID
    pub fn delete(&mut self, id: &str) -> Result<()> {
        if let Some(doc) = self.documents.remove(id) {
            self.stats.total_documents -= 1;
            self.stats.total_vectors -= doc.vector_count() as u64;
            self.stats.total_size_bytes -= doc.estimate_size_bytes() as u64;
            Ok(())
        } else {
            Err(WaffleError::NotFound(format!("Document '{}' not found", id)))
        }
    }

    /// Check if document exists
    pub fn contains(&self, id: &str) -> bool {
        self.documents.contains_key(id)
    }

    /// Get all document IDs
    pub fn document_ids(&self) -> Vec<String> {
        self.documents.keys().cloned().collect()
    }

    /// Get number of documents
    pub fn len(&self) -> usize {
        self.documents.len()
    }

    /// Check if store is empty
    pub fn is_empty(&self) -> bool {
        self.documents.is_empty()
    }

    /// Get statistics
    pub fn stats(&self) -> &MultiVectorStats {
        &self.stats
    }

    /// Clear all documents
    pub fn clear(&mut self) {
        self.documents.clear();
        self.stats = MultiVectorStats::default();
    }

    /// Find documents by vector name (for validation)
    pub fn documents_with_vector(&self, name: &str) -> Vec<String> {
        self.documents
            .iter()
            .filter(|(_, doc)| doc.has_vector(name))
            .map(|(id, _)| id.clone())
            .collect()
    }

    /// Validate that all documents have consistent vector dimensions
    pub fn validate_all_dimensions(&self) -> Result<()> {
        for (_, doc) in &self.documents {
            doc.validate_dimensions()?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SparseVector;
    use std::collections::HashMap;

    #[test]
    fn test_create_multi_vector_document() {
        let doc = MultiVectorDocument::new("doc123".to_string());
        assert_eq!(doc.id, "doc123");
        assert_eq!(doc.vector_count(), 0);
        assert!(doc.metadata.is_none());
    }

    #[test]
    fn test_set_and_get_dense_vector() {
        let mut doc = MultiVectorDocument::new("doc123".to_string());
        let vec = vec![0.1, 0.2, 0.3, 0.4];
        
        doc.set_vector("body", VectorType::Dense(vec.clone())).unwrap();
        
        assert_eq!(doc.vector_count(), 1);
        assert!(doc.has_vector("body"));
        
        let retrieved = doc.get_vector("body").unwrap();
        match retrieved {
            VectorType::Dense(v) => assert_eq!(v, &vec),
            _ => panic!("Expected dense vector"),
        }
    }

    #[test]
    fn test_multiple_vector_slots() {
        let mut doc = MultiVectorDocument::new("doc123".to_string());
        
        doc.set_vector("body", VectorType::Dense(vec![0.1, 0.2, 0.3])).unwrap();
        doc.set_vector("title", VectorType::Dense(vec![0.4, 0.5, 0.6])).unwrap();
        
        let mut sparse_indices = HashMap::new();
        sparse_indices.insert(0u32, 1.0);
        let sparse = SparseVector::new(sparse_indices).unwrap();
        doc.set_vector("keywords", VectorType::Sparse(sparse)).unwrap();
        
        assert_eq!(doc.vector_count(), 3);
        assert_eq!(doc.vector_names().len(), 3);
    }

    #[test]
    fn test_invalid_vector_name() {
        let mut doc = MultiVectorDocument::new("doc123".to_string());
        
        // Empty name
        assert!(doc.set_vector("", VectorType::Dense(vec![0.1])).is_err());
        
        // Invalid characters
        assert!(doc.set_vector("body-text", VectorType::Dense(vec![0.1])).is_err());
        
        // Too long
        let long_name = "a".repeat(200);
        assert!(doc.set_vector(long_name, VectorType::Dense(vec![0.1])).is_err());
    }

    #[test]
    fn test_validate_dimensions_consistent() {
        let mut doc = MultiVectorDocument::new("doc123".to_string());
        
        doc.set_vector("body", VectorType::Dense(vec![0.1, 0.2, 0.3])).unwrap();
        doc.set_vector("title", VectorType::Dense(vec![0.4, 0.5, 0.6])).unwrap();
        
        let (dense_dim, sparse_dim) = doc.validate_dimensions().unwrap();
        assert_eq!(dense_dim, Some(3));
        assert!(sparse_dim.is_none());
    }

    #[test]
    fn test_validate_dimensions_mismatch() {
        let mut doc = MultiVectorDocument::new("doc123".to_string());
        
        doc.set_vector("body", VectorType::Dense(vec![0.1, 0.2, 0.3])).unwrap();
        doc.set_vector("title", VectorType::Dense(vec![0.4, 0.5])).unwrap();
        
        assert!(doc.validate_dimensions().is_err());
    }

    #[test]
    fn test_multi_vector_store_insert_get() {
        let mut store = MultiVectorStore::new();
        let mut doc = MultiVectorDocument::new("doc123".to_string());
        doc.set_vector("body", VectorType::Dense(vec![0.1, 0.2, 0.3])).unwrap();
        
        store.insert(doc).unwrap();
        
        assert_eq!(store.len(), 1);
        assert!(store.contains("doc123"));
        
        let retrieved = store.get("doc123").unwrap();
        assert_eq!(retrieved.id, "doc123");
        assert_eq!(retrieved.vector_count(), 1);
    }

    #[test]
    fn test_multi_vector_store_delete() {
        let mut store = MultiVectorStore::new();
        let mut doc = MultiVectorDocument::new("doc123".to_string());
        doc.set_vector("body", VectorType::Dense(vec![0.1, 0.2, 0.3])).unwrap();
        
        store.insert(doc).unwrap();
        assert_eq!(store.len(), 1);
        
        store.delete("doc123").unwrap();
        assert_eq!(store.len(), 0);
    }

    #[test]
    fn test_estimate_size_bytes() {
        let mut doc = MultiVectorDocument::new("doc123".to_string());
        doc.set_vector("body", VectorType::Dense(vec![0.1, 0.2, 0.3, 0.4])).unwrap();
        
        let size = doc.estimate_size_bytes();
        // At least 64 (base) + 4 (name) + 4*4 (vector)
        assert!(size >= 80);
    }

    #[test]
    fn test_store_statistics() {
        let mut store = MultiVectorStore::new();
        
        let mut doc1 = MultiVectorDocument::new("doc1".to_string());
        doc1.set_vector("body", VectorType::Dense(vec![0.1, 0.2, 0.3])).unwrap();
        
        let mut doc2 = MultiVectorDocument::new("doc2".to_string());
        doc2.set_vector("body", VectorType::Dense(vec![0.4, 0.5, 0.6])).unwrap();
        doc2.set_vector("title", VectorType::Dense(vec![0.7, 0.8, 0.9])).unwrap();
        
        store.insert(doc1).unwrap();
        store.insert(doc2).unwrap();
        
        assert_eq!(store.stats().total_documents, 2);
        assert_eq!(store.stats().total_vectors, 3);
    }

    #[test]
    fn test_documents_with_vector() {
        let mut store = MultiVectorStore::new();
        
        let mut doc1 = MultiVectorDocument::new("doc1".to_string());
        doc1.set_vector("body", VectorType::Dense(vec![0.1, 0.2])).unwrap();
        
        let mut doc2 = MultiVectorDocument::new("doc2".to_string());
        doc2.set_vector("title", VectorType::Dense(vec![0.1, 0.2])).unwrap();
        
        store.insert(doc1).unwrap();
        store.insert(doc2).unwrap();
        
        let docs_with_body = store.documents_with_vector("body");
        assert_eq!(docs_with_body.len(), 1);
        assert!(docs_with_body.contains(&"doc1".to_string()));
    }
}
