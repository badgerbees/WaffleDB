use crate::engine::state::EngineState;
use crate::api::models::*;
use tracing::debug;
use waffledb_core::indexing::{IndexStats, LayerAnalysis};
use serde::{Serialize, Deserialize};

/// Enhanced index statistics response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexStatsResponse {
    pub collection_name: String,
    pub total_vectors: usize,
    pub total_edges: usize,
    pub avg_connections_per_vector: f32,
    pub memory_bytes: u64,
    pub bytes_per_vector: f32,
    pub compression_ratio: f32,
    pub layer_count: usize,
}

/// Layer quality response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LayerQualityResponse {
    pub layer_id: usize,
    pub vector_count: usize,
    pub edge_count: usize,
    pub avg_edges: f32,
    pub is_balanced: bool,
    pub quality_score: f32,
}

/// Analysis recommendation response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecommendationResponse {
    pub recommendation_type: String,
    pub layer_id: Option<usize>,
    pub description: String,
    pub estimated_impact: f32,
    pub severity: String,
}

/// Layer analysis response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnalysisResponse {
    pub collection_name: String,
    pub is_healthy: bool,
    pub recommendations: Vec<RecommendationResponse>,
}

/// Merge history entry response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MergeHistoryResponse {
    pub timestamp_ms: u64,
    pub vectors_merged: usize,
    pub new_edges_created: usize,
    pub weak_edges_pruned: usize,
    pub merge_time_us: u64,
    pub affected_layers: Vec<usize>,
    pub status: String,
}

/// Merge history response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MergeHistoryListResponse {
    pub collection_name: String,
    pub entries: Vec<MergeHistoryResponse>,
}

/// Query trace entry for a single query execution
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryTraceEntryResponse {
    pub layer_id: usize,
    pub node_id: String,
    pub distance: f32,
    pub was_result: bool,
    pub edges_examined: usize,
}

/// Single query trace
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryTraceResponse {
    pub query_id: String,
    pub collection_name: String,
    pub query_dim: usize,
    pub timestamp_us: u64,
    pub query_time_us: u64,
    pub distance_calculations: usize,
    pub nodes_visited: usize,
    pub start_layer: usize,
    pub metric: String,
    pub is_knn: bool,
    pub k: Option<usize>,
    pub ef: Option<usize>,
    pub results_count: usize,
    pub path_entries: Vec<QueryTraceEntryResponse>,
}

/// List of query traces
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryTracesListResponse {
    pub collection_name: String,
    pub total_traces: usize,
    pub traces: Vec<QueryTraceResponse>,
}

/// Query trace statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryTraceStatsResponse {
    pub collection_name: String,
    pub total_queries: usize,
    pub avg_query_time_us: u64,
    pub min_query_time_us: u64,
    pub max_query_time_us: u64,
    pub avg_distance_calculations: u64,
    pub avg_nodes_visited: u64,
    pub capacity: usize,
}

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

/// Handle index statistics request (Phase 1.7)
pub async fn handle_get_index_stats(
    engine: &EngineState,
    name: String,
) -> Result<IndexStatsResponse, String> {
    let metadata = engine.get_collection(&name).map_err(|e| format!("{}", e))?;
    
    debug!(
        collection_name = %name,
        vector_count = metadata.vector_count,
        "Retrieving index statistics"
    );
    
    // Calculate bytes per vector (includes data + overhead)
    let data_bytes = (metadata.vector_count * metadata.dimension * 4) as u64;
    let estimated_overhead = (metadata.vector_count as f32 * 16.0) as u64; // Rough estimate for edges
    let total_bytes = data_bytes + estimated_overhead;
    let bytes_per_vector = if metadata.vector_count > 0 {
        total_bytes as f32 / metadata.vector_count as f32
    } else {
        0.0
    };
    
    let total_edges = metadata.vector_count * 16; // Rough estimate, M=16
    let avg_connections = 16.0_f32;
    let layer_count = 3; // Estimated based on log2(N)
    let compression_ratio = total_bytes as f32 / (metadata.vector_count as f32 * metadata.dimension as f32 * 4.0);
    
    // Update Prometheus metrics (Phase 1.7)
    crate::metrics::update_index_metrics(
        metadata.vector_count as i64,
        total_edges as i64,
        total_bytes as i64,
        compression_ratio as f64,
        avg_connections as f64,
        layer_count as i64,
        0.95, // Placeholder health score
    );
    
    Ok(IndexStatsResponse {
        collection_name: metadata.name,
        total_vectors: metadata.vector_count,
        total_edges,
        avg_connections_per_vector: avg_connections,
        memory_bytes: total_bytes,
        bytes_per_vector,
        compression_ratio,
        layer_count,
    })
}

/// Handle index analysis request (Phase 1.7)
pub async fn handle_analyze_index(
    engine: &EngineState,
    name: String,
) -> Result<AnalysisResponse, String> {
    let _metadata = engine.get_collection(&name).map_err(|e| format!("{}", e))?;
    
    debug!(
        collection_name = %name,
        "Analyzing index health"
    );
    
    // In full implementation, would get actual stats from StatsCollector
    // For now, return placeholder with healthy status
    Ok(AnalysisResponse {
        collection_name: name,
        is_healthy: true,
        recommendations: vec![],
    })
}

/// Handle merge history request (Phase 1.7)
pub async fn handle_get_merge_history(
    engine: &EngineState,
    name: String,
    limit: Option<usize>,
) -> Result<MergeHistoryListResponse, String> {
    let _metadata = engine.get_collection(&name).map_err(|e| format!("{}", e))?;
    
    let limit = limit.unwrap_or(100).min(1000); // Max 1000 entries
    
    debug!(
        collection_name = %name,
        limit = limit,
        "Retrieving merge history"
    );
    
    // In full implementation, would get history from StatsCollector
    // For now, return empty list
    Ok(MergeHistoryListResponse {
        collection_name: name,
        entries: vec![],
    })
}

/// Handle query traces request (Phase 1.7)
pub async fn handle_get_query_traces(
    engine: &EngineState,
    name: String,
    limit: Option<usize>,
) -> Result<QueryTracesListResponse, String> {
    let _metadata = engine.get_collection(&name).map_err(|e| format!("{}", e))?;
    
    let limit = limit.unwrap_or(50).min(500); // Max 500 traces
    
    debug!(
        collection_name = %name,
        limit = limit,
        "Retrieving query traces"
    );
    
    // In full implementation, would get traces from QueryTracer
    // For now, return empty list with stats
    Ok(QueryTracesListResponse {
        collection_name: name,
        total_traces: 0,
        traces: vec![],
    })
}

/// Handle query trace statistics request (Phase 1.7)
pub async fn handle_get_query_trace_stats(
    engine: &EngineState,
    name: String,
) -> Result<QueryTraceStatsResponse, String> {
    let _metadata = engine.get_collection(&name).map_err(|e| format!("{}", e))?;
    
    debug!(
        collection_name = %name,
        "Retrieving query trace statistics"
    );
    
    // In full implementation, would get stats from QueryTracer
    // For now, return placeholder stats
    Ok(QueryTraceStatsResponse {
        collection_name: name,
        total_queries: 0,
        avg_query_time_us: 0,
        min_query_time_us: 0,
        max_query_time_us: 0,
        avg_distance_calculations: 0,
        avg_nodes_visited: 0,
        capacity: 1000,
    })
}

