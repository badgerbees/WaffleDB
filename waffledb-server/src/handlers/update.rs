use crate::engine::state::EngineState;
use crate::api::models::{UpdateVectorRequest, UpdateVectorResponse, UpdateMetadataRequest, UpdateMetadataResponse, PatchMetadataRequest};
use waffledb_core::vector::types::Vector;
use waffledb_core::metadata::schema::Metadata;
use std::time::Instant;
use tracing::{info, error, debug, instrument};
use crate::metrics;

/// Update a vector's embedding (reindexes)
#[instrument(skip(engine, req), fields(collection = %collection))]
pub async fn handle_update_vector(
    engine: &EngineState,
    collection: String,
    req: UpdateVectorRequest,
) -> waffledb_core::Result<UpdateVectorResponse> {
    let start = Instant::now();
    let id = req.id.clone();
    let vector_dim = req.vector.len();
    
    debug!(vector_id = %id, dimension = vector_dim, "Updating vector");

    let vector = Vector::new(req.vector);
    let metadata = req.metadata.and_then(|m| {
        // Convert serde_json::Value to waffledb_core::Metadata
        let mut meta = Metadata::new();
        if let Ok(obj) = serde_json::to_value(&m) {
            if let Some(map) = obj.as_object() {
                for (k, v) in map {
                    meta.insert(k.clone(), format!("{}", v));
                }
            }
        }
        Some(meta)
    });

    match engine.update_vector(&collection, id.clone(), vector, metadata) {
        Ok(()) => {
            let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
            metrics::TOTAL_REQUESTS.inc();
            
            info!(
                vector_id = %id,
                dimension = vector_dim,
                latency_ms = elapsed_ms,
                "UPDATE VECTOR completed successfully"
            );

            Ok(UpdateVectorResponse {
                status: "ok".to_string(),
                id,
            })
        }
        Err(e) => {
            metrics::TOTAL_ERRORS.inc();
            error!(
                vector_id = %id,
                error = %e,
                "UPDATE VECTOR failed"
            );
            Err(e)
        }
    }
}

/// Update only metadata without reindexing
#[instrument(skip(engine, req), fields(collection = %collection))]
pub async fn handle_update_metadata(
    engine: &EngineState,
    collection: String,
    req: UpdateMetadataRequest,
) -> waffledb_core::Result<UpdateMetadataResponse> {
    let start = Instant::now();
    let id = req.id.clone();
    
    debug!(vector_id = %id, "Updating metadata");

    // Convert serde_json::Value to Metadata
    let mut meta = Metadata::new();
    if let Some(obj) = req.metadata.as_object() {
        for (k, v) in obj {
            meta.insert(k.clone(), format!("{}", v));
        }
    }

    match engine.patch_metadata(&collection, &id, meta) {
        Ok(()) => {
            let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
            metrics::TOTAL_REQUESTS.inc();
            
            info!(
                vector_id = %id,
                latency_ms = elapsed_ms,
                "UPDATE METADATA completed successfully"
            );

            Ok(UpdateMetadataResponse {
                status: "ok".to_string(),
                id,
            })
        }
        Err(e) => {
            metrics::TOTAL_ERRORS.inc();
            error!(
                vector_id = %id,
                error = %e,
                "UPDATE METADATA failed"
            );
            Err(e)
        }
    }
}

/// Patch metadata (merge with existing)
#[instrument(skip(engine, req), fields(collection = %collection))]
pub async fn handle_patch_metadata(
    engine: &EngineState,
    collection: String,
    req: PatchMetadataRequest,
) -> waffledb_core::Result<UpdateMetadataResponse> {
    let start = Instant::now();
    let id = req.id.clone();
    
    debug!(vector_id = %id, "Patching metadata");

    // Convert serde_json::Value to Metadata
    let mut meta = Metadata::new();
    if let Some(obj) = req.metadata.as_object() {
        for (k, v) in obj {
            meta.insert(k.clone(), format!("{}", v));
        }
    }

    match engine.patch_metadata(&collection, &id, meta) {
        Ok(()) => {
            let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
            metrics::TOTAL_REQUESTS.inc();
            
            info!(
                vector_id = %id,
                latency_ms = elapsed_ms,
                "PATCH METADATA completed successfully"
            );

            Ok(UpdateMetadataResponse {
                status: "ok".to_string(),
                id,
            })
        }
        Err(e) => {
            metrics::TOTAL_ERRORS.inc();
            error!(
                vector_id = %id,
                error = %e,
                "PATCH METADATA failed"
            );
            Err(e)
        }
    }
}
