use crate::engine::state::EngineState;
use crate::api::models::{InsertRequest, InsertResponse, BatchInsertRequest, BatchInsertResponse};
use waffledb_core::vector::types::Vector;
use std::time::Instant;
use rayon::prelude::*;
use tracing::{info, error, debug, instrument};
use crate::metrics;
use crate::utils::filters::normalize_metadata;

/// Handle single insert request
#[instrument(skip(engine, req), fields(collection = %collection))]
pub async fn handle_insert(
    engine: &EngineState,
    collection: String,
    req: InsertRequest,
) -> waffledb_core::Result<InsertResponse> {
    let start = Instant::now();
    
    let id = req.id.unwrap_or_else(|| {
        use std::time::{SystemTime, UNIX_EPOCH};
        let duration = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        format!("vec_{}", duration.as_nanos())
    });

    let vector_dim = req.vector.len();
    let metadata_size = req.metadata.as_ref().map(|m| m.len()).unwrap_or(0);
    let vector = Vector::new(req.vector);

    // Normalize metadata: convert all values (numbers, bools, etc.) to strings automatically
    let metadata = normalize_metadata(req.metadata);

    debug!(vector_id = %id, dimension = vector_dim, metadata_size, "Inserting vector");

    match engine.insert_with_policy(&collection, id.clone(), vector, metadata) {
        Ok(()) => {
            let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
            let elapsed_secs = elapsed_ms / 1000.0;
            
            metrics::TOTAL_INSERTS.inc();
            metrics::TOTAL_REQUESTS.inc();
            metrics::INSERT_LATENCY.observe(elapsed_secs);
            
            info!(
                vector_id = %id,
                dimension = vector_dim,
                metadata_size,
                latency_ms = elapsed_ms,
                "INSERT completed successfully"
            );

            Ok(InsertResponse {
                id,
                status: "ok".to_string(),
            })
        }
        Err(e) => {
            metrics::TOTAL_ERRORS.inc();
            error!(
                vector_id = %id,
                dimension = vector_dim,
                error = %e,
                "INSERT failed"
            );
            Err(e)
        }
    }
}

/// Handle batch insert request with deterministic processing
/// 
/// Maintains insertion order for consistency with duplicate policies
/// Parallel vector preparation while maintaining sequential insertion
pub async fn handle_batch_insert(
    engine: &EngineState,
    collection: String,
    req: BatchInsertRequest,
) -> waffledb_core::Result<BatchInsertResponse> {
    let batch_size = req.vectors.len();
    info!(batch_size, collection = %collection, "Starting batch insert (deterministic)");
    let start = Instant::now();
    
    // Prepare vectors in parallel (non-blocking)
    let prepared: Vec<_> = req
        .vectors
        .into_par_iter()
        .enumerate()
        .map(|(idx, insert_req)| {
            let id = insert_req.id.unwrap_or_else(|| {
                use std::time::{SystemTime, UNIX_EPOCH};
                let duration = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default();
                // Include original index to maintain deterministic order
                format!("vec_{}_{}", idx, duration.as_nanos())
            });

            let vector = Vector::new(insert_req.vector);
            let metadata = normalize_metadata(insert_req.metadata);

            (idx, id, vector, metadata)
        })
        .collect();
    
    // Sort by original index to maintain deterministic insertion order
    let mut sorted = prepared;
    sorted.sort_by_key(|item| item.0);

    let mut inserted = 0;
    let mut errors = vec![];

    // Process all vectors sequentially for determinism
    // This ensures duplicate policies (reject/overwrite/skip) work consistently
    for (_idx, id, vector, metadata) in sorted {
        debug!(vector_id = %id, "Inserting vector from batch");
        match engine.insert(&collection, id.clone(), vector, metadata) {
            Ok(()) => {
                inserted += 1;
                debug!(vector_id = %id, "Vector inserted successfully");
            }
            Err(e) => {
                let error_msg = format!("Vector {}: {}", id, e);
                errors.push(error_msg);
                error!(vector_id = %id, error = %e, "Failed to insert vector");
            }
        }
    }

    let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
    let avg_latency_per_vector = if batch_size > 0 { elapsed_ms / batch_size as f64 } else { 0.0 };
    
    for _ in 0..inserted {
        metrics::TOTAL_INSERTS.inc();
    }
    metrics::TOTAL_REQUESTS.inc();
    
    info!(
        batch_size,
        inserted_count = inserted,
        failed_count = errors.len(),
        latency_ms = elapsed_ms,
        avg_per_vector_ms = avg_latency_per_vector,
        "Batch insert completed with parallel processing"
    );

    Ok(BatchInsertResponse {
        inserted_count: inserted,
        failed_count: errors.len(),
        errors,
    })
}
