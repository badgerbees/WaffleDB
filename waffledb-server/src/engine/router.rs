use crate::engine::state::EngineState;
use waffledb_core::vector::types::Vector;
use waffledb_core::metadata::schema::Metadata;

/// Central request router.
pub struct RequestRouter {
    pub engine: EngineState,
}

impl RequestRouter {
    pub fn new(engine: EngineState) -> Self {
        RequestRouter { engine }
    }

    /// Route insert request.
    pub async fn handle_insert(
        &self,
        collection_name: &str,
        id: String,
        vector: Vec<f32>,
        metadata: Option<Metadata>,
    ) -> waffledb_core::Result<String> {
        let vec = Vector::new(vector);
        self.engine.insert(collection_name, id.clone(), vec, metadata)?;
        Ok(id)
    }

    /// Route search request.
    pub async fn handle_search(
        &self,
        collection_name: &str,
        query: Vec<f32>,
        top_k: usize,
    ) -> waffledb_core::Result<Vec<(String, f32)>> {
        self.engine.search(collection_name, &query, top_k)
    }

    /// Route delete request.
    pub async fn handle_delete(&self, collection_name: &str, id: &str) -> waffledb_core::Result<()> {
        self.engine.delete(collection_name, id)
    }

    /// Route metadata update.
    pub async fn handle_update_metadata(
        &self,
        collection_name: &str,
        id: String,
        metadata: Metadata,
    ) -> waffledb_core::Result<()> {
        // Update metadata for the vector in a collection
        // This is handled by the engine's collection metadata store
        let collections = self.engine.collections.read().unwrap();
        let collection = collections
            .get(collection_name)
            .ok_or_else(|| waffledb_core::WaffleError::StorageError(format!("Collection '{}' not found", collection_name)))?;

        let mut meta_store = collection.vector_metadata.write().unwrap();
        meta_store.insert(id, metadata);
        Ok(())
    }
}
