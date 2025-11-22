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
// HELPER FUNCTIONS
// ============================================================================