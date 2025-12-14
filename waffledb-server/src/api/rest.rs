use actix_web::{web, HttpResponse, Responder};
use crate::engine::state::EngineState;
use crate::api::models::*;
use crate::handlers::*;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

// ============================================================================
// ERROR HANDLING HELPER
// ============================================================================

/// Convert error messages to detailed error responses
fn handle_error(error: &str, status_code: u32) -> HttpResponse {
    // Try to detect common error patterns and provide better suggestions
    let error_lower = error.to_lowercase();
    
    if error_lower.contains("json") || error_lower.contains("deserialize") {
        if error_lower.contains("integer") && error_lower.contains("expected a string") {
            return HttpResponse::BadRequest().json(
                DetailedErrorResponse::deserialization_error(
                    "vector field",
                    "string",
                    "integer",
                    "Convert vector IDs to strings. Example: { \"id\": \"vec_123\" }"
                )
            );
        }
        if error_lower.contains("expected") {
            return HttpResponse::BadRequest().json(
                DetailedErrorResponse::deserialization_error(
                    "request body",
                    "valid JSON",
                    "invalid format",
                    "Ensure all required fields are present and properly formatted"
                )
            );
        }
    }
    
    if error_lower.contains("not found") || error_lower.contains("doesn't exist") {
        return HttpResponse::NotFound().json(
            DetailedErrorResponse::not_found("Collection or vector")
        );
    }
    
    // Fallback for internal errors
    if status_code >= 500 {
        return HttpResponse::InternalServerError().json(
            DetailedErrorResponse::internal_error(error)
        );
    }
    
    HttpResponse::BadRequest().json(
        DetailedErrorResponse::validation_error(
            "request",
            error,
            "Check the error details above and adjust your request"
        )
    )
}

// ============================================================================
// COLLECTION MANAGEMENT
// ============================================================================

/// POST /collections - Create a new collection
pub async fn create_collection(
    engine: web::Data<Arc<EngineState>>,
    req: web::Json<CreateCollectionRequest>,
) -> impl Responder {
    match collection::handle_create_collection(engine.as_ref().as_ref(), req.into_inner()).await {
        Ok(response) => HttpResponse::Created().json(response),
        Err(e) => handle_error(&format!("{}", e), 400),
    }
}

/// DELETE /collections/{name} - Delete a collection
pub async fn delete_collection(
    engine: web::Data<Arc<EngineState>>,
    name: web::Path<String>,
) -> impl Responder {
    let req = DeleteCollectionRequest {
        name: name.into_inner(),
    };
    match collection::handle_delete_collection(engine.as_ref().as_ref(), req).await {
        Ok(response) => HttpResponse::Ok().json(response),
        Err(e) => handle_error(&format!("{}", e), 400),
    }
}

/// GET /collections/{name} - Get collection metadata
pub async fn get_collection(
    engine: web::Data<Arc<EngineState>>,
    name: web::Path<String>,
) -> impl Responder {
    match collection::handle_get_collection(engine.as_ref().as_ref(), name.into_inner()).await {
        Ok(info) => HttpResponse::Ok().json(info),
        Err(e) => handle_error(&format!("{}", e), 404),
    }
}

/// GET /collections - List all collections
pub async fn list_collections(
    engine: web::Data<Arc<EngineState>>,
) -> impl Responder {
    match collection::handle_list_collections(engine.as_ref().as_ref()).await {
        Ok(collections) => HttpResponse::Ok().json(serde_json::json!({"collections": collections})),
        Err(e) => handle_error(&format!("{}", e), 500),
    }
}

// ============================================================================
// INSERT OPERATIONS
// ============================================================================

/// POST /collections/{name}/insert - Insert a single vector
pub async fn insert(
    engine: web::Data<Arc<EngineState>>,
    name: web::Path<String>,
    req: web::Json<InsertRequest>,
) -> impl Responder {
    match insert::handle_insert(engine.as_ref().as_ref(), name.into_inner(), req.into_inner()).await {
        Ok(response) => HttpResponse::Ok().json(response),
        Err(e) => handle_error(&format!("{}", e), 400),
    }
}

/// POST /collections/{name}/batch_insert - Batch insert vectors
pub async fn batch_insert(
    engine: web::Data<Arc<EngineState>>,
    name: web::Path<String>,
    req: web::Json<BatchInsertRequest>,
) -> impl Responder {
    match insert::handle_batch_insert(engine.as_ref().as_ref(), name.into_inner(), req.into_inner()).await {
        Ok(response) => HttpResponse::Ok().json(response),
        Err(e) => handle_error(&format!("{}", e), 400),
    }
}

// ============================================================================
// SEARCH OPERATIONS
// ============================================================================

/// POST /collections/{name}/search - Single search query
pub async fn search(
    engine: web::Data<Arc<EngineState>>,
    name: web::Path<String>,
    req: web::Json<SearchRequest>,
) -> impl Responder {
    match search::handle_search(engine.as_ref().as_ref(), name.into_inner(), req.into_inner()).await {
        Ok(response) => HttpResponse::Ok().json(response),
        Err(e) => handle_error(&format!("{}", e), 400),
    }
}

/// POST /collections/{name}/batch_search - Batch search queries
pub async fn batch_search(
    engine: web::Data<Arc<EngineState>>,
    name: web::Path<String>,
    req: web::Json<BatchSearchRequest>,
) -> impl Responder {
    match search::handle_batch_search(engine.as_ref().as_ref(), name.into_inner(), req.into_inner()).await {
        Ok(response) => HttpResponse::Ok().json(response),
        Err(e) => handle_error(&format!("{}", e), 400),
    }
}

// ============================================================================
// DELETE OPERATIONS
// ============================================================================

/// DELETE /collections/{name}/vectors/{id} - Delete a vector
pub async fn delete(
    engine: web::Data<Arc<EngineState>>,
    path: web::Path<(String, String)>,
) -> impl Responder {
    let (collection, id) = path.into_inner();
    let req = DeleteRequest { id };
    match delete::handle_delete(engine.as_ref().as_ref(), collection, req).await {
        Ok(response) => HttpResponse::Ok().json(response),
        Err(e) => handle_error(&format!("{}", e), 400),
    }
}

// ============================================================================
// METADATA OPERATIONS
// ============================================================================

/// PUT /collections/{name}/vectors/{id} - Update vector embedding and/or metadata
pub async fn update_vector(
    engine: web::Data<Arc<EngineState>>,
    path: web::Path<(String, String)>,
    req: web::Json<UpdateVectorRequest>,
) -> impl Responder {
    let (collection, id) = path.into_inner();
    let mut update_req = req.into_inner();
    update_req.id = id;
    match update::handle_update_vector(engine.as_ref().as_ref(), collection, update_req).await {
        Ok(response) => HttpResponse::Ok().json(response),
        Err(e) => handle_error(&format!("{}", e), 400),
    }
}

/// PATCH /collections/{name}/vectors/{id}/metadata - Patch metadata (partial update)
pub async fn patch_metadata(
    engine: web::Data<Arc<EngineState>>,
    path: web::Path<(String, String)>,
    req: web::Json<PatchMetadataRequest>,
) -> impl Responder {
    let (collection, id) = path.into_inner();
    let mut patch_req = req.into_inner();
    patch_req.id = id;
    match update::handle_patch_metadata(engine.as_ref().as_ref(), collection, patch_req).await {
        Ok(response) => HttpResponse::Ok().json(response),
        Err(e) => handle_error(&format!("{}", e), 400),
    }
}

/// PUT /collections/{name}/vectors/{id}/metadata - Update vector metadata
pub async fn update_metadata(
    engine: web::Data<Arc<EngineState>>,
    path: web::Path<(String, String)>,
    req: web::Json<UpdateMetadataRequest>,
) -> impl Responder {
    let (collection, id) = path.into_inner();
    let mut update_req = req.into_inner();
    update_req.id = id;
    match update::handle_update_metadata(engine.as_ref().as_ref(), collection, update_req).await {
        Ok(response) => HttpResponse::Ok().json(response),
        Err(e) => handle_error(&format!("{}", e), 400),
    }
}

// ============================================================================
// SNAPSHOT OPERATIONS (PERSISTENCE)
// ============================================================================

/// POST /collections/{name}/snapshot - Create index snapshot
pub async fn create_snapshot(
    engine: web::Data<Arc<EngineState>>,
    name: web::Path<String>,
) -> impl Responder {
    match snapshot::handle_create_snapshot(engine.as_ref().as_ref(), name.into_inner()).await {
        Ok(response) => HttpResponse::Ok().json(response),
        Err(e) => handle_error(&format!("{}", e), 400),
    }
}

// ============================================================================
// STATISTICS & MONITORING
// ============================================================================

/// GET /collections/{name}/stats - Get collection statistics
pub async fn get_stats(
    engine: web::Data<Arc<EngineState>>,
    name: web::Path<String>,
) -> impl Responder {
    match stats::handle_get_stats(engine.as_ref().as_ref(), name.into_inner()).await {
        Ok(stats_resp) => HttpResponse::Ok().json(stats_resp),
        Err(e) => handle_error(&format!("{}", e), 404),
    }
}

/// GET /collections/{name}/stats/index - Get detailed index statistics
pub async fn get_index_stats(
    engine: web::Data<Arc<EngineState>>,
    name: web::Path<String>,
) -> impl Responder {
    match stats::handle_get_index_stats(engine.as_ref().as_ref(), name.into_inner()).await {
        Ok(stats_resp) => HttpResponse::Ok().json(stats_resp),
        Err(e) => handle_error(&format!("{}", e), 404),
    }
}

/// GET /collections/{name}/stats/analysis - Analyze collection health
pub async fn analyze_index(
    engine: web::Data<Arc<EngineState>>,
    name: web::Path<String>,
) -> impl Responder {
    match stats::handle_analyze_index(engine.as_ref().as_ref(), name.into_inner()).await {
        Ok(analysis_resp) => HttpResponse::Ok().json(analysis_resp),
        Err(e) => handle_error(&format!("{}", e), 404),
    }
}

/// GET /collections/{name}/stats/merge-history - Get merge operation history
pub async fn get_merge_history(
    engine: web::Data<Arc<EngineState>>,
    name: web::Path<String>,
    query: web::Query<std::collections::HashMap<String, String>>,
) -> impl Responder {
    let limit = query.get("limit")
        .and_then(|l| l.parse::<usize>().ok())
        .map(Some)
        .unwrap_or(Some(100));

    match stats::handle_get_merge_history(engine.as_ref().as_ref(), name.into_inner(), limit).await {
        Ok(history_resp) => HttpResponse::Ok().json(history_resp),
        Err(e) => handle_error(&format!("{}", e), 404),
    }
}

/// GET /collections/{name}/stats/traces - Get query execution traces
pub async fn get_query_traces(
    engine: web::Data<Arc<EngineState>>,
    name: web::Path<String>,
    query: web::Query<std::collections::HashMap<String, String>>,
) -> impl Responder {
    let limit = query.get("limit")
        .and_then(|l| l.parse::<usize>().ok())
        .map(Some)
        .unwrap_or(Some(50));

    match stats::handle_get_query_traces(engine.as_ref().as_ref(), name.into_inner(), limit).await {
        Ok(traces_resp) => HttpResponse::Ok().json(traces_resp),
        Err(e) => handle_error(&format!("{}", e), 404),
    }
}

/// GET /collections/{name}/stats/traces/stats - Get query trace statistics
pub async fn get_query_trace_stats(
    engine: web::Data<Arc<EngineState>>,
    name: web::Path<String>,
) -> impl Responder {
    match stats::handle_get_query_trace_stats(engine.as_ref().as_ref(), name.into_inner()).await {
        Ok(stats_resp) => HttpResponse::Ok().json(stats_resp),
        Err(e) => handle_error(&format!("{}", e), 404),
    }
}

/// GET /health - Server health check
pub async fn health() -> impl Responder {
    let uptime_seconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);

    HttpResponse::Ok().json(HealthResponse {
        status: "healthy".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        uptime_seconds,
    })
}

/// GET /liveness - Kubernetes liveness probe endpoint
pub async fn liveness() -> impl Responder {
    HttpResponse::Ok().json(serde_json::json!({"status": "alive"}))
}

/// GET /metrics - Prometheus metrics endpoint
pub async fn metrics(
    _engine: web::Data<Arc<EngineState>>,
) -> impl Responder {
    let prometheus_output = crate::metrics::encode_metrics();
    HttpResponse::Ok()
        .content_type("text/plain; charset=utf-8")
        .body(prometheus_output)
}

/// GET /ready - Readiness probe (for Kubernetes, etc.)
pub async fn ready(
    engine: web::Data<Arc<EngineState>>,
) -> impl Responder {
    match stats::handle_is_ready(engine.as_ref().as_ref()).await {
        Ok(true) => HttpResponse::Ok().json(serde_json::json!({"ready": true})),
        _ => HttpResponse::ServiceUnavailable().json(serde_json::json!({"ready": false})),
    }
}

// ============================================================================
// SIMPLE API SHORTCUTS - Auto-create + Insert/Search/Delete (Zero Friction)
// ============================================================================

/// POST /collections/{name}/add - Add vectors (auto-creates collection if needed)
pub async fn add(
    engine: web::Data<Arc<EngineState>>,
    name: web::Path<String>,
    req: web::Json<BatchInsertRequest>,
) -> impl Responder {
    let collection_name = name.into_inner();
    
    // Validate request
    if req.vectors.is_empty() {
        return handle_error(
            "No vectors provided. Include at least one vector with 'id' and 'vector' fields.",
            400,
        );
    }
    
    // Auto-create collection if it doesn't exist
    // Infer dimension from first vector
    let dimension = req.vectors[0].vector.len() as u32;
    
    if dimension == 0 {
        return handle_error(
            "Vector cannot be empty. Provide a non-empty embedding vector.",
            400,
        );
    }
    
    // Validate all vectors have same dimension
    for (i, vec) in req.vectors.iter().enumerate() {
        if vec.vector.len() as u32 != dimension {
            return handle_error(
                &format!(
                    "Dimension mismatch at vector {}: expected {} but got {}",
                    i,
                    dimension,
                    vec.vector.len()
                ),
                400,
            );
        }
    }
    
    // Try to create, ignore if already exists
    let create_req = CreateCollectionRequest {
        name: collection_name.clone(),
        dimension: dimension as usize,
        metric: "l2".to_string(),
        engine: Some("hnsw".to_string()),
        m: None,
        m_l: None,
        duplicate_policy: "overwrite".to_string(),
    };
    
    let _ = collection::handle_create_collection(engine.as_ref().as_ref(), create_req).await;
    
    // Now insert normally
    match insert::handle_batch_insert(engine.as_ref().as_ref(), collection_name.clone(), req.into_inner()).await {
        Ok(response) => HttpResponse::Ok().json(response),
        Err(e) => handle_error(&format!("{}", e), 400),
    }
}

/// POST /collections/{name}/search - Search for similar vectors (Simple API)
pub async fn simple_search(
    engine: web::Data<Arc<EngineState>>,
    name: web::Path<String>,
    req: web::Json<SimpleSearchRequest>,
) -> impl Responder {
    // Validate request
    if req.embedding.is_empty() {
        return handle_error(
            "Embedding cannot be empty. Provide a non-empty query vector in the 'embedding' field.",
            400,
        );
    }
    
    if let Some(limit) = req.limit {
        if limit <= 0 {
            return handle_error(
                &format!("Limit must be > 0, got {}", limit),
                400,
            );
        }
    }
    
    let search_req = SearchRequest {
        vector: req.embedding.clone(),
        top_k: req.limit.unwrap_or(5),
        ef_search: None,
        filter: req.filter.as_ref().map(|f| serde_json::to_value(f).unwrap_or(serde_json::json!({}))),
        include_metadata: true,
    };
    
    match search::handle_search(engine.as_ref().as_ref(), name.into_inner(), search_req).await {
        Ok(response) => HttpResponse::Ok().json(response),
        Err(e) => handle_error(&format!("{}", e), 400),
    }
}

/// POST /collections/{name}/delete - Delete vectors (Simple API - Batch)
pub async fn simple_delete(
    engine: web::Data<Arc<EngineState>>,
    name: web::Path<String>,
    req: web::Json<SimpleDeleteRequest>,
) -> impl Responder {
    // Validate request
    if req.ids.is_empty() {
        return handle_error(
            "No IDs provided. Include at least one ID in the 'ids' field to delete.",
            400,
        );
    }
    
    let collection = name.into_inner();
    let mut errors = Vec::new();
    let mut deleted_count = 0;
    
    for id in &req.ids {
        let delete_req = DeleteRequest { id: id.clone() };
        match delete::handle_delete(engine.as_ref().as_ref(), collection.clone(), delete_req).await {
            Ok(_) => deleted_count += 1,
            Err(e) => errors.push(format!("{}: {}", id, e)),
        }
    }
    
    let result = serde_json::json!({
        "deleted_count": deleted_count,
        "total_requested": req.ids.len(),
        "errors": if errors.is_empty() { None } else { Some(errors) }
    });
    
    HttpResponse::Ok().json(result)
}

// ============================================================================
// HELPER FUNCTIONS
// ============================================================================