/// OpenTelemetry Integration for WaffleDB
///
/// Provides:
/// - Prometheus metrics export
/// - Request instrumentation
/// - Performance monitoring
///
/// Configuration via environment:
/// - PROMETHEUS_EXPORTER_PORT (default: 9090)

use std::time::Instant;
use tracing::info_span;

pub mod metrics {
    use lazy_static::lazy_static;
    use prometheus::{Counter as PromCounter, Histogram as PromHistogram, HistogramOpts, Counter, Registry, Gauge as PromGauge};

    lazy_static! {
        pub static ref REGISTRY: Registry = Registry::new();
    }

    lazy_static! {
        pub static ref INSERT_TOTAL: PromCounter = PromCounter::new("waffledb_insert_total", "Total insert operations")
            .expect("Failed to create insert_total counter");
        pub static ref INSERT_LATENCY: PromHistogram = PromHistogram::with_opts(HistogramOpts::new("waffledb_insert_latency_ms", "Insert operation latency in ms"))
            .expect("Failed to create insert_latency histogram");
        pub static ref SEARCH_TOTAL: PromCounter = PromCounter::new("waffledb_search_total", "Total search operations")
            .expect("Failed to create search_total counter");
        pub static ref SEARCH_LATENCY: PromHistogram = PromHistogram::with_opts(HistogramOpts::new("waffledb_search_latency_ms", "Search operation latency in ms"))
            .expect("Failed to create search_latency histogram");
        pub static ref DELETE_TOTAL: PromCounter = PromCounter::new("waffledb_delete_total", "Total delete operations")
            .expect("Failed to create delete_total counter");
        pub static ref HNSW_BUILD_LATENCY: PromHistogram = PromHistogram::with_opts(HistogramOpts::new("waffledb_hnsw_build_latency_ms", "HNSW index build latency in ms"))
            .expect("Failed to create hnsw_build_latency histogram");
        pub static ref PQ_COMPRESSION_RATIO: PromHistogram = PromHistogram::with_opts(HistogramOpts::new("waffledb_pq_compression_ratio", "PQ compression ratio (%)")) 
            .expect("Failed to create pq_compression_ratio histogram");
        pub static ref VECTORS_TOTAL: PromGauge = PromGauge::new("waffledb_vectors_total", "Total vectors in index")
            .expect("Failed to create vectors_total gauge");
        pub static ref MEMORY_USAGE_BYTES: PromGauge = PromGauge::new("waffledb_memory_usage_bytes", "Memory usage in bytes")
            .expect("Failed to create memory_usage_bytes gauge");
        pub static ref CONCURRENT_OPERATIONS: PromGauge = PromGauge::new("waffledb_concurrent_operations", "Concurrent operations")
            .expect("Failed to create concurrent_operations gauge");
        pub static ref ERRORS_TOTAL: PromCounter = PromCounter::new("waffledb_errors_total", "Total errors")
            .expect("Failed to create errors_total counter");
    }

    pub fn register_all() -> Result<(), prometheus::Error> {
        REGISTRY.register(Box::new(INSERT_TOTAL.clone()))?;
        REGISTRY.register(Box::new(INSERT_LATENCY.clone()))?;
        REGISTRY.register(Box::new(SEARCH_TOTAL.clone()))?;
        REGISTRY.register(Box::new(SEARCH_LATENCY.clone()))?;
        REGISTRY.register(Box::new(DELETE_TOTAL.clone()))?;
        REGISTRY.register(Box::new(HNSW_BUILD_LATENCY.clone()))?;
        REGISTRY.register(Box::new(PQ_COMPRESSION_RATIO.clone()))?;
        REGISTRY.register(Box::new(VECTORS_TOTAL.clone()))?;
        REGISTRY.register(Box::new(MEMORY_USAGE_BYTES.clone()))?;
        REGISTRY.register(Box::new(CONCURRENT_OPERATIONS.clone()))?;
        REGISTRY.register(Box::new(ERRORS_TOTAL.clone()))?;
        Ok(())
    }
}

/// Initialize telemetry (simplified version without Jaeger)
pub fn init_telemetry() -> Result<(), Box<dyn std::error::Error>> {
    // Register Prometheus metrics
    metrics::register_all()?;

    // Initialize tracing subscriber
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;

    tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer()
                .with_target(true)
                .with_level(true)
        )
        .init();

    Ok(())
}

/// Get Prometheus metrics in text format
pub fn get_metrics() -> Result<String, prometheus::Error> {
    use prometheus::Encoder;
    
    let encoder = prometheus::TextEncoder::new();
    let metric_families = metrics::REGISTRY.gather();
    
    let mut buffer = Vec::new();
    encoder.encode(&metric_families, &mut buffer)?;
    
    Ok(String::from_utf8(buffer).unwrap_or_default())
}

/// Instrumentation helper: Automatically track latency
pub struct InstrumentedOperation {
    name: &'static str,
    start: Instant,
}

impl InstrumentedOperation {
    pub fn new(name: &'static str) -> Self {
        let _span = info_span!(
            "operation",
            name = name,
            component = "waffledb"
        );

        InstrumentedOperation {
            name,
            start: Instant::now(),
        }
    }

    pub fn record_success(&self, doc_count: usize) {
        let elapsed_ms = self.start.elapsed().as_millis() as f64;

        match self.name {
            "insert" => {
                metrics::INSERT_TOTAL.inc();
                metrics::INSERT_LATENCY.observe(elapsed_ms);
                tracing::info!(
                    name = "insert",
                    doc_count = doc_count,
                    latency_ms = elapsed_ms,
                    "operation completed successfully"
                );
            }
            "search" => {
                metrics::SEARCH_TOTAL.inc();
                metrics::SEARCH_LATENCY.observe(elapsed_ms);
                tracing::info!(
                    name = "search",
                    result_count = doc_count,
                    latency_ms = elapsed_ms,
                    "operation completed successfully"
                );
            }
            "delete" => {
                metrics::DELETE_TOTAL.inc();
                tracing::info!(
                    name = "delete",
                    doc_count = doc_count,
                    latency_ms = elapsed_ms,
                    "operation completed successfully"
                );
            }
            _ => {}
        }
    }

    pub fn record_error(&self, error: &str) {
        let elapsed_ms = self.start.elapsed().as_millis() as f64;
        metrics::ERRORS_TOTAL.inc();

        tracing::error!(
            name = self.name,
            error = error,
            latency_ms = elapsed_ms,
            "operation failed"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_instrumented_operation_insert() {
        metrics::register_all().ok(); // Register first
        let op = InstrumentedOperation::new("insert");
        op.record_success(100);
        
        let metrics_str = get_metrics().unwrap();
        assert!(metrics_str.len() > 0);
    }

    #[test]
    fn test_instrumented_operation_search() {
        metrics::register_all().ok(); // Register first
        let op = InstrumentedOperation::new("search");
        op.record_success(5);
        
        let metrics_str = get_metrics().unwrap();
        assert!(metrics_str.len() > 0);
    }

    #[test]
    fn test_metrics_registry() {
        metrics::register_all().ok(); // May already be registered from other tests
        
        metrics::INSERT_TOTAL.inc();
        metrics::VECTORS_TOTAL.set(1000.0);
        
        let metrics_str = get_metrics().unwrap();
        assert!(metrics_str.len() > 0);
    }

    #[test]
    fn test_latency_histogram() {
        metrics::register_all().ok(); // Register first
        metrics::INSERT_LATENCY.observe(50.0);
        metrics::INSERT_LATENCY.observe(100.0);
        metrics::INSERT_LATENCY.observe(150.0);
        
        let metrics_str = get_metrics().unwrap();
        assert!(metrics_str.len() > 0);
    }
}
