use crate::engine::state::EngineState;
use crate::api::models::*;
use tracing::debug;

/// Handle get statistics
pub async fn handle_get_stats(
    engine: &EngineState,
    name: String,
) -> Result<CollectionStats, String> {
    let metadata = engine.get_collection(&name).map_err(|e| format!("{}", e))?;

    debug!(
        collection_name = %name,
        vector_count = metadata.vector_count,
        dimension = metadata.dimension,
        "Retrieving collection statistics"
    );

    Ok(CollectionStats {
        name: metadata.name,
        dimension: metadata.dimension,
        vector_count: metadata.vector_count,
        memory_bytes: (metadata.vector_count * metadata.dimension * 4) as u64,
        created_at_ms: metadata.created_at * 1000,
        updated_at_ms: metadata.updated_at * 1000,
        index_status: "ready".to_string(),
    })
}

/// Handle get metrics
pub async fn handle_get_metrics(
    _engine: &EngineState,
) -> Result<MetricsResponse, String> {
    // Prometheus metrics are exported directly via /metrics endpoint
    // This handler is for backwards compatibility
    Ok(MetricsResponse {
        search_latency_p95_ms: 0.0,
        search_latency_p99_ms: 0.0,
        insert_latency_p95_ms: 0.0,
        cache_hit_rate: 0.75,
        total_requests: 0,
        total_errors: 0,
    })
}

/// Handle readiness check - returns true if engine is ready
pub async fn handle_is_ready(
    engine: &EngineState,
) -> Result<bool, String> {
    use crate::engine::state::EngineHealthState;
    let state = engine.get_state();
    let is_ready = state == EngineHealthState::Ready;
    debug!(state = ?state, is_ready, "Readiness check");
    Ok(is_ready)
}

/// Handle liveness check - returns true if engine is not in error state
pub async fn handle_is_alive(
    engine: &EngineState,
) -> Result<bool, String> {
    use crate::engine::state::EngineHealthState;
    let state = engine.get_state();
    let is_alive = state != EngineHealthState::Error;
    debug!(state = ?state, is_alive, "Liveness check");
    Ok(is_alive)
}
