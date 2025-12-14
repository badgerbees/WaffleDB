mod config;
mod api;
mod handlers;
mod engine;
mod engines;
mod engine_config;
mod logging;
mod middleware;
mod metrics;
mod utils;
mod rpc;

// Note: Enterprise features (auth, multi-tenancy, quotas, audit) are intentionally separate.
// This is a clean OSS foundation - projects can implement their own auth layer on top.

use actix_web::{web, App, HttpServer};
use actix_cors::Cors;
use config::ServerConfig;
use engine::EngineState;
use engine::state::EngineHealthState;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tracing::{info, warn, error};
use tokio::signal;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    logging::init_logging();
    metrics::init_metrics();

    let config = ServerConfig::from_env();
    info!("Starting WaffleDB Server with config: {:?}", config);

    let engine = Arc::new(EngineState::new());
    
    // INTEGRATION: Initialize RAFT Coordinator for distributed mode
    if config.cluster_mode {
        use waffledb_core::distributed::raft::RaftCoordinator;
        
        let raft = RaftCoordinator::new(
            config.node_id.clone(),
            config.peers.clone(),
            config.election_timeout_ms,
            config.heartbeat_interval_ms,
        );
        engine.set_raft_coordinator(raft);
        info!("✅ Distributed mode enabled (cluster node: {})", config.node_id);
    } else {
        info!("ℹ️  Single-node mode (set WAFFLEDB_CLUSTER_MODE=true for distributed)");
    }
    
    // INTEGRATION: Initialize production features (BatchWAL, Snapshots, Repair)
    if let Err(e) = engine.initialize_production_features().await {
        warn!("Production feature initialization failed: {:?}", e);
        // Continue anyway - graceful degradation to single-node mode
    }
    
    // INTEGRATION: Run crash recovery on startup
    if let Err(e) = engine.startup_recovery().await {
        error!("Crash recovery failed: {:?}", e);
        // Mark engine in error state but continue
        engine.set_state(EngineHealthState::Error);
    }
    
    // Graceful shutdown flag
    let shutdown = Arc::new(AtomicBool::new(false));
    let shutdown_clone = shutdown.clone();

    // Spawn signal handler for graceful shutdown (cross-platform using Ctrl+C)
    tokio::spawn(async move {
        if signal::ctrl_c().await.is_ok() {
            info!("Received shutdown signal, initiating graceful shutdown...");
            shutdown_clone.store(true, Ordering::Release);
        }
    });

    let listen_addr = format!("{}:{}", config.host, config.http_port);
    info!("Starting HTTP server on {}", listen_addr);
    info!("Press Ctrl+C to gracefully shutdown");

    let mut server = HttpServer::new(move || {
        let cors = Cors::default()
            .allow_any_origin()
            .allow_any_method()
            .allow_any_header();

        App::new()
            .wrap(cors)
            .wrap(middleware::RequestLogger)
            .app_data(web::Data::new(engine.clone()))
            // ===== Monitoring =====
            .route("/health", web::get().to(api::rest::health))
            .route("/liveness", web::get().to(api::rest::liveness))
            .route("/metrics", web::get().to(api::rest::metrics))
            .route("/ready", web::get().to(api::rest::ready))
            
            // ===== Collection Management =====
            .route("/collections", web::post().to(api::rest::create_collection))
            .route("/collections", web::get().to(api::rest::list_collections))
            .route("/collections/{name}", web::get().to(api::rest::get_collection))
            .route("/collections/{name}", web::delete().to(api::rest::delete_collection))
            
            // ===== Insert Operations (Simple API - PRIMARY) =====
            .route("/collections/{name}/add", web::post().to(api::rest::add))
            .route("/collections/{name}/insert", web::post().to(api::rest::insert))  // Legacy
            .route("/collections/{name}/batch_insert", web::post().to(api::rest::batch_insert))
            
            // ===== Search Operations (Simple API - PRIMARY) =====
            .route("/collections/{name}/search", web::post().to(api::rest::simple_search))
            .route("/collections/{name}/batch_search", web::post().to(api::rest::batch_search))
            
            // ===== Delete Operations (Simple API - PRIMARY) =====
            .route("/collections/{name}/delete", web::post().to(api::rest::simple_delete))
            
            // ===== Metadata & Update Operations =====
            .route("/collections/{name}/vectors/{id}", web::put().to(api::rest::update_vector))
            .route("/collections/{name}/vectors/{id}/metadata", web::put().to(api::rest::update_metadata))
            .route("/collections/{name}/vectors/{id}/metadata", web::patch().to(api::rest::patch_metadata))
            
            // ===== Snapshots =====
            .route("/collections/{name}/snapshot", web::post().to(api::rest::create_snapshot))
            
            // ===== Statistics =====
            .route("/collections/{name}/stats", web::get().to(api::rest::get_stats))
            .route("/collections/{name}/stats/index", web::get().to(api::rest::get_index_stats))
            .route("/collections/{name}/stats/analysis", web::get().to(api::rest::analyze_index))
            .route("/collections/{name}/stats/merge-history", web::get().to(api::rest::get_merge_history))
            .route("/collections/{name}/stats/traces", web::get().to(api::rest::get_query_traces))
            .route("/collections/{name}/stats/traces/stats", web::get().to(api::rest::get_query_trace_stats))
            
            // ===== Inter-Node RPC (Distributed Mode) =====
            // RAFT consensus RPCs
            .route("/raft/append", web::post().to(rpc::handlers::handle_append_entries))
            .route("/raft/vote", web::post().to(rpc::handlers::handle_request_vote))
            .route("/raft/snapshot", web::post().to(rpc::handlers::handle_install_snapshot))
            
            // Data forwarding RPCs
            .route("/internal/forward/insert", web::post().to(rpc::handlers::handle_forward_insert))
            .route("/internal/forward/search", web::post().to(rpc::handlers::handle_forward_search))
            .route("/internal/forward/delete", web::post().to(rpc::handlers::handle_forward_delete))
            
            // Cluster management RPCs
            .route("/internal/shard/status", web::post().to(rpc::handlers::handle_get_shard_status))
            .route("/internal/heartbeat", web::post().to(rpc::handlers::handle_heartbeat))
    })
    .bind(&listen_addr)?;

    // Configure graceful shutdown: 30 second timeout for in-flight requests
    server = server.shutdown_timeout(30);

    let server = server.run();
    
    // Monitor shutdown flag for clean exit
    let shutdown_monitor = {
        let shutdown = shutdown.clone();
        tokio::spawn(async move {
            loop {
                if shutdown.load(Ordering::Acquire) {
                    info!("Shutdown signal detected, waiting for in-flight requests (max 30s)...");
                    break;
                }
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            }
        })
    };

    // Run server until shutdown signal received
    tokio::select! {
        result = server => {
            match result {
                Ok(_) => info!("Server stopped"),
                Err(e) => {
                    tracing::error!("Server error: {}", e);
                    return Err(std::io::Error::new(std::io::ErrorKind::Other, e));
                }
            }
        }
        _ = shutdown_monitor => {
            info!("Graceful shutdown initiated");
        }
    }

    info!("WaffleDB Server shut down gracefully");
    Ok(())
}
