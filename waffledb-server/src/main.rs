mod config;
mod api;
mod handlers;
mod engine;
mod engines;
mod engine_config;
mod logging;
mod middleware;
mod metrics;
mod snapshot;
mod utils;

use actix_web::{web, App, HttpServer};
use config::ServerConfig;
use engine::EngineState;
use std::sync::Arc;
use tracing::info;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    logging::init_logging();
    metrics::init_metrics();

    let config = ServerConfig::from_env();
    info!("Starting WaffleDB Server with config: {:?}", config);

    let engine = Arc::new(EngineState::new());

    let listen_addr = format!("{}:{}", config.host, config.http_port);
    info!("Starting HTTP server on {}", listen_addr);

    HttpServer::new(move || {
        App::new()
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
            
            // ===== Insert Operations =====
            .route("/collections/{name}/insert", web::post().to(api::rest::insert))
            .route("/collections/{name}/batch_insert", web::post().to(api::rest::batch_insert))
            
            // ===== Search Operations =====
            .route("/collections/{name}/search", web::post().to(api::rest::search))
            .route("/collections/{name}/batch_search", web::post().to(api::rest::batch_search))
            
            // ===== Delete Operations =====
            .route("/collections/{name}/delete", web::post().to(api::rest::delete))
            
            // ===== Metadata & Update Operations =====
            .route("/collections/{name}/vectors/{id}", web::put().to(api::rest::update_vector))
            .route("/collections/{name}/vectors/{id}/metadata", web::put().to(api::rest::update_metadata))
            .route("/collections/{name}/vectors/{id}/metadata", web::patch().to(api::rest::patch_metadata))
            
            // ===== Snapshots =====
            .route("/collections/{name}/snapshot", web::post().to(api::rest::create_snapshot))
            
            // ===== Statistics =====
            .route("/collections/{name}/stats", web::get().to(api::rest::get_stats))
    })
    .bind(&listen_addr)?
    .run()
    .await
}
