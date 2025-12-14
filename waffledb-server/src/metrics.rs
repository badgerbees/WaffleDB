use prometheus::{Counter, Histogram, IntGauge, Gauge, Registry, Encoder, TextEncoder};
use lazy_static::lazy_static;

lazy_static! {
    pub static ref METRICS_REGISTRY: Registry = Registry::new();
    
    // Counters
    pub static ref TOTAL_REQUESTS: Counter = Counter::new("waffledb_requests_total", "Total HTTP requests").expect("counter creation");
    pub static ref TOTAL_INSERTS: Counter = Counter::new("waffledb_inserts_total", "Total insert operations").expect("counter creation");
    pub static ref TOTAL_SEARCHES: Counter = Counter::new("waffledb_searches_total", "Total search operations").expect("counter creation");
    pub static ref TOTAL_ERRORS: Counter = Counter::new("waffledb_errors_total", "Total errors").expect("counter creation");
    pub static ref TOTAL_MERGES: Counter = Counter::new("waffledb_merges_total", "Total merge operations").expect("counter creation");
    
    // Histograms
    pub static ref INSERT_LATENCY: Histogram = Histogram::with_opts(
        prometheus::HistogramOpts::new("waffledb_insert_latency_seconds", "Insert operation latency")
            .buckets(vec![0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0]),
    ).expect("histogram creation");
    
    pub static ref SEARCH_LATENCY: Histogram = Histogram::with_opts(
        prometheus::HistogramOpts::new("waffledb_search_latency_seconds", "Search operation latency")
            .buckets(vec![0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0]),
    ).expect("histogram creation");
    
    pub static ref MERGE_LATENCY: Histogram = Histogram::with_opts(
        prometheus::HistogramOpts::new("waffledb_merge_latency_seconds", "Merge operation latency")
            .buckets(vec![0.01, 0.05, 0.1, 0.5, 1.0, 2.0, 5.0, 10.0]),
    ).expect("histogram creation");
    
    // Gauges
    pub static ref COLLECTION_COUNT: IntGauge = IntGauge::new("waffledb_collection_count", "Number of collections").expect("gauge creation");
    pub static ref INDEX_STATE: IntGauge = IntGauge::new("waffledb_index_state", "Index state (0=Empty, 1=Building, 2=Ready, 3=Recovering, 4=Error)").expect("gauge creation");
    
    // Statistics Gauges (Phase 1.7)
    pub static ref TOTAL_VECTORS: IntGauge = IntGauge::new("waffledb_vectors_total", "Total vectors in index").expect("gauge creation");
    pub static ref TOTAL_EDGES: IntGauge = IntGauge::new("waffledb_edges_total", "Total edges in HNSW graph").expect("gauge creation");
    pub static ref MEMORY_BYTES: IntGauge = IntGauge::new("waffledb_memory_bytes", "Memory used by index").expect("gauge creation");
    pub static ref COMPRESSION_RATIO: Gauge = Gauge::new("waffledb_compression_ratio", "Data compression ratio").expect("gauge creation");
    pub static ref AVG_CONNECTIONS: Gauge = Gauge::new("waffledb_avg_connections_per_vector", "Average connections per vector").expect("gauge creation");
    pub static ref LAYER_COUNT: IntGauge = IntGauge::new("waffledb_layer_count", "Number of layers in HNSW").expect("gauge creation");
    pub static ref INDEX_HEALTH: Gauge = Gauge::new("waffledb_index_health", "Index health score (0.0-1.0)").expect("gauge creation");
}

pub fn init_metrics() {
    METRICS_REGISTRY
        .register(Box::new(TOTAL_REQUESTS.clone()))
        .expect("register");
    METRICS_REGISTRY
        .register(Box::new(TOTAL_INSERTS.clone()))
        .expect("register");
    METRICS_REGISTRY
        .register(Box::new(TOTAL_SEARCHES.clone()))
        .expect("register");
    METRICS_REGISTRY
        .register(Box::new(TOTAL_ERRORS.clone()))
        .expect("register");
    METRICS_REGISTRY
        .register(Box::new(TOTAL_MERGES.clone()))
        .expect("register");
    METRICS_REGISTRY
        .register(Box::new(INSERT_LATENCY.clone()))
        .expect("register");
    METRICS_REGISTRY
        .register(Box::new(SEARCH_LATENCY.clone()))
        .expect("register");
    METRICS_REGISTRY
        .register(Box::new(MERGE_LATENCY.clone()))
        .expect("register");
    METRICS_REGISTRY
        .register(Box::new(COLLECTION_COUNT.clone()))
        .expect("register");
    METRICS_REGISTRY
        .register(Box::new(INDEX_STATE.clone()))
        .expect("register");
    METRICS_REGISTRY
        .register(Box::new(TOTAL_VECTORS.clone()))
        .expect("register");
    METRICS_REGISTRY
        .register(Box::new(TOTAL_EDGES.clone()))
        .expect("register");
    METRICS_REGISTRY
        .register(Box::new(MEMORY_BYTES.clone()))
        .expect("register");
    METRICS_REGISTRY
        .register(Box::new(COMPRESSION_RATIO.clone()))
        .expect("register");
    METRICS_REGISTRY
        .register(Box::new(AVG_CONNECTIONS.clone()))
        .expect("register");
    METRICS_REGISTRY
        .register(Box::new(LAYER_COUNT.clone()))
        .expect("register");
    METRICS_REGISTRY
        .register(Box::new(INDEX_HEALTH.clone()))
        .expect("register");
}

pub fn encode_metrics() -> String {
    let encoder = TextEncoder::new();
    let mut buffer = Vec::new();
    let _ = encoder.encode(&METRICS_REGISTRY.gather(), &mut buffer);
    String::from_utf8(buffer).unwrap_or_else(|_| "# ERROR encoding metrics\n".to_string())
}

/// Update metrics from index statistics
pub fn update_index_metrics(
    total_vectors: i64,
    total_edges: i64,
    memory_bytes: i64,
    compression_ratio: f64,
    avg_connections: f64,
    layer_count: i64,
    health_score: f64,
) {
    TOTAL_VECTORS.set(total_vectors);
    TOTAL_EDGES.set(total_edges);
    MEMORY_BYTES.set(memory_bytes);
    COMPRESSION_RATIO.set(compression_ratio);
    AVG_CONNECTIONS.set(avg_connections);
    LAYER_COUNT.set(layer_count);
    INDEX_HEALTH.set(health_score);
}
