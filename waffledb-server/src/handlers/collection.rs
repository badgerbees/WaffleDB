use crate::engine::state::EngineState;
use crate::api::models::*;
use crate::engines::EngineType;
use tracing::{info, error, instrument};

/// Handle create collection with tenant isolation
#[instrument(skip(engine, req), fields(tenant = %tenant_id))]
pub async fn handle_create_collection(
    engine: &EngineState,
    tenant_id: &str,
    req: CreateCollectionRequest,
) -> Result<CreateCollectionResponse, String> {
    // Parse engine type from request (default to Hybrid)
    let engine_type = match req.engine.as_deref() {
        Some("hnsw") => EngineType::HNSW,
        Some("hybrid") | _ => EngineType::Hybrid,
    };

    // Scoped collection name includes tenant_id for isolation
    let scoped_name = format!("{}:{}", tenant_id, req.name);
    
    match engine.create_collection_with_engine(scoped_name.clone(), req.dimension, engine_type, req.duplicate_policy.clone()) {
        Ok(()) => {
            info!(
                name = %req.name,
                dimension = req.dimension,
                metric = %req.metric,
                engine_type = engine_type.as_str(),
                tenant = %tenant_id,
                "Collection created"
            );
            Ok(CreateCollectionResponse {
                name: req.name,
                dimension: req.dimension,
                metric: req.metric,
                engine: engine_type.as_str().to_string(),
                status: "created".to_string(),
            })
        }
        Err(e) => {
            error!(
                name = %req.name,
                tenant = %tenant_id,
                error = %e,
                "Collection creation failed"
            );
            Err(format!("{}", e))
        }
    }
}

/// Handle delete collection with tenant isolation
#[instrument(skip(engine, req), fields(tenant = %tenant_id))]
pub async fn handle_delete_collection(
    engine: &EngineState,
    tenant_id: &str,
    req: DeleteCollectionRequest,
) -> Result<DeleteCollectionResponse, String> {
    match engine.delete_collection_for_tenant(tenant_id, &req.name) {
        Ok(()) => {
            info!(name = %req.name, tenant = %tenant_id, "Collection deleted");
            Ok(DeleteCollectionResponse {
                status: "deleted".to_string(),
            })
        }
        Err(e) => {
            error!(
                name = %req.name,
                tenant = %tenant_id,
                error = %e,
                "Collection deletion failed"
            );
            Err(format!("{}", e))
        }
    }
}

/// Handle get collection with tenant isolation
pub async fn handle_get_collection(
    engine: &EngineState,
    tenant_id: &str,
    name: String,
) -> Result<CollectionInfo, String> {
    let scoped_name = format!("{}:{}", tenant_id, name);
    let metadata = engine.get_collection(&scoped_name).map_err(|e| format!("{}", e))?;

    Ok(CollectionInfo {
        name: metadata.name,
        dimension: metadata.dimension,
        metric: "l2".to_string(),
        vector_count: metadata.vector_count,
        size_bytes: 0,
    })
}

/// Handle list collections
pub async fn handle_list_collections(
    engine: &EngineState,
) -> Result<Vec<CollectionInfo>, String> {
    let metadata_list = engine.list_collections().map_err(|e| format!("{}", e))?;

    Ok(metadata_list
        .into_iter()
        .map(|m| CollectionInfo {
            name: m.name,
            dimension: m.dimension,
            metric: "l2".to_string(),
            vector_count: m.vector_count,
            size_bytes: 0,
        })
        .collect())
}
