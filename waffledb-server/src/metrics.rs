use prometheus::{Counter, Histogram, IntGauge, Registry, Encoder, TextEncoder};
use lazy_static::lazy_static;

lazy_static! {
    pub static ref METRICS_REGISTRY: Registry = Registry::new();
    
    // Counters
    pub static ref TOTAL_REQUESTS: Counter = Counter::new("waffledb_requests_total", "Total HTTP requests").expect("counter creation");
    pub static ref TOTAL_INSERTS: Counter = Counter::new("waffledb_inserts_total", "Total insert operations").expect("counter creation");
    pub static ref TOTAL_SEARCHES: Counter = Counter::new("waffledb_searches_total", "Total search operations").expect("counter creation");
    pub static ref TOTAL_ERRORS: Counter = Counter::new("waffledb_errors_total", "Total errors").expect("counter creation");
    
    // Histograms
    pub static ref INSERT_LATENCY: Histogram = Histogram::with_opts(
        prometheus::HistogramOpts::new("waffledb_insert_latency_seconds", "Insert operation latency")
            .buckets(vec![0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0]),
    ).expect("histogram creation");
    
    pub static ref SEARCH_LATENCY: Histogram = Histogram::with_opts(
        prometheus::HistogramOpts::new("waffledb_search_latency_seconds", "Search operation latency")
            .buckets(vec![0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0]),
    ).expect("histogram creation");
    
    // Gauges
    pub static ref COLLECTION_COUNT: IntGauge = IntGauge::new("waffledb_collection_count", "Number of collections").expect("gauge creation");
    pub static ref INDEX_STATE: IntGauge = IntGauge::new("waffledb_index_state", "Index state (0=Empty, 1=Building, 2=Ready, 3=Recovering, 4=Error)").expect("gauge creation");
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
        .register(Box::new(INSERT_LATENCY.clone()))
        .expect("register");
    METRICS_REGISTRY
        .register(Box::new(SEARCH_LATENCY.clone()))
        .expect("register");
    METRICS_REGISTRY
        .register(Box::new(COLLECTION_COUNT.clone()))
        .expect("register");
    METRICS_REGISTRY
        .register(Box::new(INDEX_STATE.clone()))
        .expect("register");
}

pub fn encode_metrics() -> String {
    let encoder = TextEncoder::new();
    let mut buffer = Vec::new();
    let _ = encoder.encode(&METRICS_REGISTRY.gather(), &mut buffer);
    String::from_utf8(buffer).unwrap_or_else(|_| "# ERROR encoding metrics\n".to_string())
}
