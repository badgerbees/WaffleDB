/// Server configuration.
#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub host: String,
    pub http_port: u16,
    pub grpc_port: u16,
    pub storage_path: String,
    pub hnsw_m: usize,
    pub hnsw_ef_construction: usize,
    pub hnsw_ef_search: usize,
}

impl Default for ServerConfig {
    fn default() -> Self {
        ServerConfig {
            host: "0.0.0.0".to_string(),
            http_port: 8080,
            grpc_port: 9090,
            storage_path: "/tmp/waffledb".to_string(),
            hnsw_m: 16,
            hnsw_ef_construction: 200,
            hnsw_ef_search: 200,
        }
    }
}

impl ServerConfig {
    /// Load from environment variables.
    pub fn from_env() -> Self {
        let mut config = ServerConfig::default();

        if let Ok(host) = std::env::var("WAFFLEDB_HOST") {
            config.host = host;
        }
        if let Ok(port) = std::env::var("WAFFLEDB_HTTP_PORT") {
            config.http_port = port.parse().unwrap_or(8080);
        }
        if let Ok(port) = std::env::var("WAFFLEDB_GRPC_PORT") {
            config.grpc_port = port.parse().unwrap_or(9090);
        }
        if let Ok(path) = std::env::var("WAFFLEDB_STORAGE_PATH") {
            config.storage_path = path;
        }

        config
    }
}
