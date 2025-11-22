use waffledb_core::vector::types::Vector;
use waffledb_core::metadata::schema::Metadata;
use std::collections::HashMap;
use std::sync::{Arc, RwLock, atomic::{AtomicU64, AtomicUsize}};
use serde::{Serialize, Deserialize};
use crate::engines::EngineWrapper;

/// Engine state enumeration
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum EngineHealthState {
    Empty = 0,
    Building = 1,
    Ready = 2,
    Recovering = 3,
    Error = 4,
}

/// Engine metrics tracking (shared via Arc)
pub struct EngineMetrics {
    pub total_requests: AtomicU64,
    pub total_errors: AtomicU64,
    pub total_inserts: AtomicU64,
    pub total_searches: AtomicU64,
    pub insert_latency_total_ms: AtomicU64,
    pub search_latency_total_ms: AtomicU64,
    pub total_collections: AtomicUsize,
    pub state: AtomicU64,
}

impl EngineMetrics {
    pub fn new() -> Arc<Self> {
        Arc::new(EngineMetrics {
            total_requests: AtomicU64::new(0),
            total_errors: AtomicU64::new(0),
            total_inserts: AtomicU64::new(0),
            total_searches: AtomicU64::new(0),
            insert_latency_total_ms: AtomicU64::new(0),
            search_latency_total_ms: AtomicU64::new(0),
            total_collections: AtomicUsize::new(0),
            state: AtomicU64::new(EngineHealthState::Empty as u64),
        })
    }
}

/// Collection metadata
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CollectionMetadata {
    pub name: String,
    pub dimension: usize,
    pub created_at: u64,
    pub updated_at: u64,
    pub vector_count: usize,
    pub duplicate_policy: String,
}

/// Per-collection storage with pluggable VectorEngine
pub struct Collection {
    pub metadata: CollectionMetadata,
    /// Pluggable vector engine (HNSW, IVF, etc.)
    pub engine: Arc<EngineWrapper>,
    /// Metadata storage (separate from vectors)
    pub vector_metadata: Arc<RwLock<HashMap<String, Metadata>>>,
}

/// Global engine state with multi-collection support, thread-safe.
pub struct EngineState {
    pub collections: Arc<RwLock<HashMap<String, Arc<Collection>>>>,
    pub metrics: Arc<EngineMetrics>,
}

impl EngineState {
    /// Create new engine state.
    pub fn new() -> Self {
        EngineState {
            collections: Arc::new(RwLock::new(HashMap::new())),
            metrics: EngineMetrics::new(),
        }
    }

    /// Create a new collection with specified engine type and duplicate policy
    pub fn create_collection_with_engine(&self, name: String, dimension: usize, engine_type: crate::engines::EngineType, duplicate_policy: String) -> waffledb_core::Result<()> {
        let mut collections = self.collections.write().unwrap();
        if collections.contains_key(&name) {
            return Err(waffledb_core::WaffleError::StorageError(format!("Collection '{}' already exists", name)));
        }

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let engine = Arc::new(EngineWrapper::new(engine_type)?);

        let collection = Collection {
            metadata: CollectionMetadata {
                name: name.clone(),
                dimension,
                created_at: now,
                updated_at: now,
                vector_count: 0,
                duplicate_policy,
            },
            engine,
            vector_metadata: Arc::new(RwLock::new(HashMap::new())),
        };

        collections.insert(name, Arc::new(collection));
        Ok(())
    }

    /// Create a new collection (default: HNSW engine with overwrite duplicate policy)
    pub fn create_collection(&self, name: String, dimension: usize) -> waffledb_core::Result<()> {
        self.create_collection_with_engine(name, dimension, crate::engines::EngineType::HNSW, "overwrite".to_string())
    }

    /// Delete a collection
    pub fn delete_collection(&self, name: &str) -> waffledb_core::Result<()> {
        let mut collections = self.collections.write().unwrap();
        if !collections.contains_key(name) {
            return Err(waffledb_core::WaffleError::StorageError(format!("Collection '{}' not found", name)));
        }
        collections.remove(name);
        Ok(())
    }

    /// Get collection metadata
    pub fn get_collection(&self, name: &str) -> waffledb_core::Result<CollectionMetadata> {
        let collections = self.collections.read().unwrap();
        let collection = collections
            .get(name)
            .ok_or_else(|| waffledb_core::WaffleError::StorageError(format!("Collection '{}' not found", name)))?;

        let mut metadata = collection.metadata.clone();
        metadata.vector_count = collection.engine.len();

        Ok(metadata)
    }

    /// List all collections
    pub fn list_collections(&self) -> waffledb_core::Result<Vec<CollectionMetadata>> {
        let collections = self.collections.read().unwrap();
        let mut result = Vec::new();

        for collection in collections.values() {
            let mut metadata = collection.metadata.clone();
            metadata.vector_count = collection.engine.len();
            result.push(metadata);
        }

        Ok(result)
    }

    /// Set engine state
    pub fn set_state(&self, state: EngineHealthState) {
        use std::sync::atomic::Ordering;
        self.metrics.state.store(state as u64, Ordering::Relaxed);
    }

    /// Get engine state
    pub fn get_state(&self) -> EngineHealthState {
        use std::sync::atomic::Ordering;
        let state_val = self.metrics.state.load(Ordering::Relaxed);
        match state_val {
            0 => EngineHealthState::Empty,
            1 => EngineHealthState::Building,
            2 => EngineHealthState::Ready,
            3 => EngineHealthState::Recovering,
            4 => EngineHealthState::Error,
            _ => EngineHealthState::Empty,
        }
    }

    /// Insert a vector into a collection using the pluggable engine
    pub fn insert(
        &self,
        collection_name: &str,
        id: String,
        vector: Vector,
        metadata: Option<Metadata>,
    ) -> waffledb_core::Result<()> {
        let collections = self.collections.read().unwrap();
        let collection = collections
            .get(collection_name)
            .ok_or_else(|| waffledb_core::WaffleError::StorageError(format!("Collection '{}' not found", collection_name)))?;

        // Insert into the engine (REAL HNSW with graph connectivity)
        collection.engine.insert(id.clone(), vector)?;

        // Store metadata separately
        if let Some(meta) = metadata {
            let mut metadata_store = collection.vector_metadata.write().unwrap();
            metadata_store.insert(id, meta);
        }

        Ok(())
    }

    /// Get a vector from a collection
    pub fn get_vector(&self, collection_name: &str, id: &str) -> waffledb_core::Result<Vector> {
        let collections = self.collections.read().unwrap();
        let collection = collections
            .get(collection_name)
            .ok_or_else(|| waffledb_core::WaffleError::StorageError(format!("Collection '{}' not found", collection_name)))?;

        collection.engine.get(id).ok_or(waffledb_core::WaffleError::NotFound)
    }

    /// Get metadata from a collection
    pub fn get_metadata(&self, collection_name: &str, id: &str) -> waffledb_core::Result<Metadata> {
        let collections = self.collections.read().unwrap();
        let collection = collections
            .get(collection_name)
            .ok_or_else(|| waffledb_core::WaffleError::StorageError(format!("Collection '{}' not found", collection_name)))?;

        let metadata = collection.vector_metadata.read().unwrap();
        metadata
            .get(id)
            .cloned()
            .ok_or_else(|| waffledb_core::WaffleError::NotFound)
    }

    /// Delete a vector from a collection
    pub fn delete(&self, collection_name: &str, id: &str) -> waffledb_core::Result<()> {
        let collections = self.collections.read().unwrap();
        let collection = collections
            .get(collection_name)
            .ok_or_else(|| waffledb_core::WaffleError::StorageError(format!("Collection '{}' not found", collection_name)))?;

        // Delete from engine
        collection.engine.delete(id)?;

        // Remove from metadata
        let mut metadata = collection.vector_metadata.write().unwrap();
        metadata.remove(id);

        Ok(())
    }

    /// Insert with duplicate handling policy
    pub fn insert_with_policy(
        &self,
        collection_name: &str,
        id: String,
        vector: Vector,
        metadata: Option<Metadata>,
    ) -> waffledb_core::Result<()> {
        let collections = self.collections.read().unwrap();
        let collection = collections
            .get(collection_name)
            .ok_or_else(|| waffledb_core::WaffleError::StorageError(format!("Collection '{}' not found", collection_name)))?;

        let duplicate_policy = collection.metadata.duplicate_policy.as_str();
        
        // Check if vector already exists
        let exists = collection.engine.get(&id).is_some();
        
        if exists && duplicate_policy == "reject" {
            return Err(waffledb_core::WaffleError::StorageError(format!("Vector '{}' already exists (duplicate policy: reject)", id)));
        }

        // Delete existing if overwrite policy
        if exists && duplicate_policy == "overwrite" {
            let _ = collection.engine.delete(&id);
            let mut metadata_store = collection.vector_metadata.write().unwrap();
            metadata_store.remove(&id);
        }

        // Insert into the engine
        collection.engine.insert(id.clone(), vector)?;

        // Store metadata
        if let Some(meta) = metadata {
            let mut metadata_store = collection.vector_metadata.write().unwrap();
            metadata_store.insert(id, meta);
        }

        Ok(())
    }

    /// Update vector with new embedding (reindexes)
    pub fn update_vector(
        &self,
        collection_name: &str,
        id: String,
        vector: Vector,
        metadata: Option<Metadata>,
    ) -> waffledb_core::Result<()> {
        let collections = self.collections.read().unwrap();
        let collection = collections
            .get(collection_name)
            .ok_or_else(|| waffledb_core::WaffleError::StorageError(format!("Collection '{}' not found", collection_name)))?;

        // Delete old vector
        collection.engine.delete(&id)?;
        let mut metadata_store = collection.vector_metadata.write().unwrap();
        metadata_store.remove(&id);

        // Insert new vector
        collection.engine.insert(id.clone(), vector)?;

        // Update metadata if provided
        if let Some(meta) = metadata {
            metadata_store.insert(id, meta);
        }

        Ok(())
    }

    /// Patch (merge) metadata for a vector without changing embedding
    pub fn patch_metadata(
        &self,
        collection_name: &str,
        id: &str,
        partial_metadata: Metadata,
    ) -> waffledb_core::Result<()> {
        let collections = self.collections.read().unwrap();
        let collection = collections
            .get(collection_name)
            .ok_or_else(|| waffledb_core::WaffleError::StorageError(format!("Collection '{}' not found", collection_name)))?;

        let mut metadata_store = collection.vector_metadata.write().unwrap();
        
        // For now, replace metadata (full update)
        // TODO: Implement proper merge once Metadata type supports iteration
        metadata_store.insert(id.to_string(), partial_metadata);
        Ok(())
    }

    /// Search for similar vectors using the engine's search implementation (REAL HNSW)
    pub fn search(
        &self,
        collection_name: &str,
        query: &[f32],
        top_k: usize,
    ) -> waffledb_core::Result<Vec<(String, f32)>> {
        let collections = self.collections.read().unwrap();
        let collection = collections
            .get(collection_name)
            .ok_or_else(|| waffledb_core::WaffleError::StorageError(format!("Collection '{}' not found", collection_name)))?;

        // Use the engine's search implementation (REAL HNSW with layer descent)
        let results = collection.engine.search(query, top_k)?;

        // Convert from EngineSearchResult to (String, f32) tuples for backwards compatibility
        let converted: Vec<(String, f32)> = results
            .into_iter()
            .map(|r| (r.id, r.distance))
            .collect();

        Ok(converted)
    }
}
