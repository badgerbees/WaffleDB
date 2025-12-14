use crate::engine::state::EngineState;
use crate::api::models::{SearchRequest, SearchResponse, SearchResultItem, BatchSearchRequest, BatchSearchResponse};
use std::time::Instant;
use rayon::prelude::*;
use tracing::{info, error, debug, instrument};
use crate::metrics;

/// Handle single search request
#[instrument(skip(engine, req), fields(collection = %collection))]
pub async fn handle_search(
    engine: &EngineState,
    collection: String,
    req: SearchRequest,
) -> waffledb_core::Result<SearchResponse> {
    let start = Instant::now();
    let query_dim = req.vector.len();
    
    debug!(top_k = req.top_k, query_dim, "Searching vectors");

    match engine.search(&collection, &req.vector, req.top_k) {
        Ok(results) => {
            let latency_ms = start.elapsed().as_secs_f64() * 1000.0;
            let latency_secs = latency_ms / 1000.0;

            metrics::TOTAL_SEARCHES.inc();
            metrics::TOTAL_REQUESTS.inc();
            metrics::SEARCH_LATENCY.observe(latency_secs);

            info!(
                top_k = req.top_k,
                query_dim,
                results_count = results.len(),
                latency_ms,
                "SEARCH completed successfully"
            );

            let items = results
                .into_iter()
                .map(|(id, distance)| {
                    SearchResultItem {
                        id,
                        score: distance,
                        metadata: None,
                    }
                })
                .collect();

            Ok(SearchResponse {
                results: items,
                query_latency_ms: latency_ms,
            })
        }
        Err(e) => {
            metrics::TOTAL_ERRORS.inc();
            error!(
                query_dim,
                error = %e,
                "SEARCH failed"
            );
            Err(e)
        }
    }
}

/// Handle batch search request (parallel execution)
pub async fn handle_batch_search(
    engine: &EngineState,
    collection: String,
    req: BatchSearchRequest,
) -> waffledb_core::Result<BatchSearchResponse> {
    let start = Instant::now();
    let batch_size = req.queries.len();

    info!(batch_size, collection = %collection, "Starting parallel batch search");

    // Use rayon for parallel search execution
    let all_results = req
        .queries
        .into_par_iter()
        .map(|query| {
            // Execute search for each query in parallel
            let query_vec = query.vector.clone();
            match engine.search(&collection, &query_vec, query.top_k) {
                Ok(results) => results
                    .into_iter()
                    .map(|(id, distance)| SearchResultItem {
                        id,
                        score: distance,
                        metadata: None,
                    })
                    .collect(),
                Err(_) => vec![],
            }
        })
        .collect();

    let total_latency_ms = start.elapsed().as_secs_f64() * 1000.0;

    for _ in 0..batch_size {
        metrics::TOTAL_SEARCHES.inc();
    }
    metrics::TOTAL_REQUESTS.inc();
    metrics::SEARCH_LATENCY.observe(total_latency_ms / 1000.0);

    info!(
        batch_size,
        total_latency_ms,
        avg_latency_ms = total_latency_ms / batch_size as f64,
        "Batch search completed"
    );

    Ok(BatchSearchResponse {
        results: all_results,
        total_latency_ms,
    })
}
