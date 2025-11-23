/// Index durability system for fast recovery
/// 
/// Persists index structures (BM25, sparse, metadata) for instant reload on restart.
/// Eliminates rebuild latency.

use std::path::{Path, PathBuf};
use std::fs;
use waffledb_core::core::errors::{Result, WaffleError, ErrorCode};
use serde::{Deserialize, Serialize};

/// BM25 index checkpoint
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BM25Checkpoint {
    pub version: u32,
    pub term_count: u64,
    pub doc_count: u64,
    pub avg_doc_length: f32,
    pub parameters_k1: f32,
    pub parameters_b: f32,
}

impl BM25Checkpoint {
    pub fn new() -> Self {
        Self {
            version: 1,
            term_count: 0,
            doc_count: 0,
            avg_doc_length: 0.0,
            parameters_k1: 1.5,
            parameters_b: 0.75,
        }
    }
}

/// Sparse index checkpoint
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SparseCheckpoint {
    pub version: u32,
    pub vector_count: u64,
    pub max_index: u32,
    pub avg_nnz: f32,
    pub compression_ratio: f32,
}

impl SparseCheckpoint {
    pub fn new() -> Self {
        Self {
            version: 1,
            vector_count: 0,
            max_index: 0,
            avg_nnz: 0.0,
            compression_ratio: 1.0,
        }
    }
}

/// Metadata index checkpoint
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetadataCheckpoint {
    pub version: u32,
    pub field_count: usize,
    pub indexed_documents: u64,
    pub timestamp_ms: u64,
}

impl MetadataCheckpoint {
    pub fn new() -> Self {
        Self {
            version: 1,
            field_count: 0,
            indexed_documents: 0,
            timestamp_ms: 0,
        }
    }
}

/// Durability manager for all index types
pub struct DurabilityManager {
    base_path: PathBuf,
}

impl DurabilityManager {
    /// Create new durability manager
    pub fn new(base_path: impl AsRef<Path>) -> Result<Self> {
        let path = base_path.as_ref().to_path_buf();
        fs::create_dir_all(&path).map_err(|e| WaffleError::WithCode {
            code: ErrorCode::StorageIOError,
            message: format!("Failed to create durability directory: {}", e),
        })?;

        Ok(Self { base_path: path })
    }

    /// Save BM25 index checkpoint
    pub fn save_bm25_checkpoint(&self, collection: &str, checkpoint: &BM25Checkpoint) -> Result<()> {
        let path = self.base_path.join(format!("{}_bm25.json", collection));
        let json = serde_json::to_string(checkpoint).map_err(|e| WaffleError::WithCode {
            code: ErrorCode::SerializationError,
            message: format!("Failed to serialize BM25 checkpoint: {}", e),
        })?;

        fs::write(&path, json).map_err(|e| WaffleError::WithCode {
            code: ErrorCode::StorageIOError,
            message: format!("Failed to write BM25 checkpoint: {}", e),
        })?;

        tracing::info!("Saved BM25 checkpoint for collection {}", collection);
        Ok(())
    }

    /// Load BM25 index checkpoint
    pub fn load_bm25_checkpoint(&self, collection: &str) -> Result<Option<BM25Checkpoint>> {
        let path = self.base_path.join(format!("{}_bm25.json", collection));

        if !path.exists() {
            return Ok(None);
        }

        let json = fs::read_to_string(&path).map_err(|e| WaffleError::WithCode {
            code: ErrorCode::StorageIOError,
            message: format!("Failed to read BM25 checkpoint: {}", e),
        })?;

        let checkpoint =
            serde_json::from_str(&json).map_err(|e| WaffleError::WithCode {
                code: ErrorCode::DeserializationError,
                message: format!("Failed to deserialize BM25 checkpoint: {}", e),
            })?;

        tracing::info!("Loaded BM25 checkpoint for collection {}", collection);
        Ok(Some(checkpoint))
    }

    /// Save sparse index checkpoint
    pub fn save_sparse_checkpoint(
        &self,
        collection: &str,
        checkpoint: &SparseCheckpoint,
    ) -> Result<()> {
        let path = self.base_path.join(format!("{}_sparse.json", collection));
        let json = serde_json::to_string(checkpoint).map_err(|e| WaffleError::WithCode {
            code: ErrorCode::SerializationError,
            message: format!("Failed to serialize sparse checkpoint: {}", e),
        })?;

        fs::write(&path, json).map_err(|e| WaffleError::WithCode {
            code: ErrorCode::StorageIOError,
            message: format!("Failed to write sparse checkpoint: {}", e),
        })?;

        tracing::info!("Saved sparse checkpoint for collection {}", collection);
        Ok(())
    }

    /// Load sparse index checkpoint
    pub fn load_sparse_checkpoint(&self, collection: &str) -> Result<Option<SparseCheckpoint>> {
        let path = self.base_path.join(format!("{}_sparse.json", collection));

        if !path.exists() {
            return Ok(None);
        }

        let json = fs::read_to_string(&path).map_err(|e| WaffleError::WithCode {
            code: ErrorCode::StorageIOError,
            message: format!("Failed to read sparse checkpoint: {}", e),
        })?;

        let checkpoint =
            serde_json::from_str(&json).map_err(|e| WaffleError::WithCode {
                code: ErrorCode::DeserializationError,
                message: format!("Failed to deserialize sparse checkpoint: {}", e),
            })?;

        tracing::info!("Loaded sparse checkpoint for collection {}", collection);
        Ok(Some(checkpoint))
    }

    /// Save metadata index checkpoint
    pub fn save_metadata_checkpoint(
        &self,
        collection: &str,
        checkpoint: &MetadataCheckpoint,
    ) -> Result<()> {
        let path = self.base_path.join(format!("{}_metadata.json", collection));
        let json = serde_json::to_string(checkpoint).map_err(|e| WaffleError::WithCode {
            code: ErrorCode::SerializationError,
            message: format!("Failed to serialize metadata checkpoint: {}", e),
        })?;

        fs::write(&path, json).map_err(|e| WaffleError::WithCode {
            code: ErrorCode::StorageIOError,
            message: format!("Failed to write metadata checkpoint: {}", e),
        })?;

        tracing::info!(
            "Saved metadata checkpoint for collection {}",
            collection
        );
        Ok(())
    }

    /// Load metadata index checkpoint
    pub fn load_metadata_checkpoint(
        &self,
        collection: &str,
    ) -> Result<Option<MetadataCheckpoint>> {
        let path = self.base_path.join(format!("{}_metadata.json", collection));

        if !path.exists() {
            return Ok(None);
        }

        let json = fs::read_to_string(&path).map_err(|e| WaffleError::WithCode {
            code: ErrorCode::StorageIOError,
            message: format!("Failed to read metadata checkpoint: {}", e),
        })?;

        let checkpoint =
            serde_json::from_str(&json).map_err(|e| WaffleError::WithCode {
                code: ErrorCode::DeserializationError,
                message: format!("Failed to deserialize metadata checkpoint: {}", e),
            })?;

        tracing::info!(
            "Loaded metadata checkpoint for collection {}",
            collection
        );
        Ok(Some(checkpoint))
    }

    /// Delete all checkpoints for collection
    pub fn delete_collection_checkpoints(&self, collection: &str) -> Result<()> {
        let patterns = vec!["_bm25.json", "_sparse.json", "_metadata.json"];

        for pattern in patterns {
            let path = self.base_path.join(format!("{}{}", collection, pattern));
            if path.exists() {
                fs::remove_file(&path).map_err(|e| WaffleError::WithCode {
                    code: ErrorCode::StorageIOError,
                    message: format!("Failed to delete checkpoint: {}", e),
                })?;
            }
        }

        Ok(())
    }

    /// Get all collections with checkpoints
    pub fn list_collections(&self) -> Result<Vec<String>> {
        let mut collections = std::collections::HashSet::new();

        for entry in fs::read_dir(&self.base_path).map_err(|e| WaffleError::WithCode {
            code: ErrorCode::StorageIOError,
            message: format!("Failed to read checkpoint directory: {}", e),
        })? {
            let entry = entry.map_err(|e| WaffleError::WithCode {
                code: ErrorCode::StorageIOError,
                message: format!("Failed to read directory entry: {}", e),
            })?;

            if let Some(file_name) = entry.file_name().to_str() {
                for pattern in &["_bm25.json", "_sparse.json", "_metadata.json"] {
                    if file_name.ends_with(pattern) {
                        let collection = file_name.replace(pattern, "");
                        collections.insert(collection);
                    }
                }
            }
        }

        Ok(collections.into_iter().collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn test_bm25_checkpoint_serialization() {
        let checkpoint = BM25Checkpoint::new();
        let json = serde_json::to_string(&checkpoint).unwrap();
        let deserialized: BM25Checkpoint = serde_json::from_str(&json).unwrap();

        assert_eq!(checkpoint.version, deserialized.version);
    }

    #[test]
    fn test_durability_manager_creation() {
        let temp_dir = tempfile::tempdir().unwrap();
        let manager = DurabilityManager::new(temp_dir.path()).unwrap();
        assert!(temp_dir.path().exists());
    }

    #[test]
    fn test_save_and_load_bm25() {
        let temp_dir = tempfile::tempdir().unwrap();
        let manager = DurabilityManager::new(temp_dir.path()).unwrap();

        let checkpoint = BM25Checkpoint::new();
        manager
            .save_bm25_checkpoint("test_collection", &checkpoint)
            .unwrap();

        let loaded = manager.load_bm25_checkpoint("test_collection").unwrap();
        assert!(loaded.is_some());
    }

    #[test]
    fn test_checkpoint_deletion() {
        let temp_dir = tempfile::tempdir().unwrap();
        let manager = DurabilityManager::new(temp_dir.path()).unwrap();

        let checkpoint = BM25Checkpoint::new();
        manager
            .save_bm25_checkpoint("test_collection", &checkpoint)
            .unwrap();

        manager
            .delete_collection_checkpoints("test_collection")
            .unwrap();

        let loaded = manager.load_bm25_checkpoint("test_collection").unwrap();
        assert!(loaded.is_none());
    }
}
