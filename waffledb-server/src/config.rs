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
    
    // Clustering (RAFT Distributed Mode)
    pub cluster_mode: bool,           // Enable RAFT consensus
    pub node_id: String,              // Unique node identifier
    pub raft_port: u16,               // Port for inter-node RAFT RPC
    pub peers: Vec<String>,           // List of "host:port" for other nodes
    pub election_timeout_ms: u64,     // Milliseconds before starting election
    pub heartbeat_interval_ms: u64,   // Milliseconds between heartbeats
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
            cluster_mode: false,
            node_id: "node1".to_string(),
            raft_port: 9090,
            peers: vec![],
            election_timeout_ms: 150,
            heartbeat_interval_ms: 50,
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
        
        // Clustering options
        if let Ok(enabled) = std::env::var("WAFFLEDB_CLUSTER_MODE") {
            config.cluster_mode = enabled.to_lowercase() == "true";
        }
        if let Ok(node_id) = std::env::var("WAFFLEDB_NODE_ID") {
            config.node_id = node_id;
        }
        if let Ok(port) = std::env::var("WAFFLEDB_RAFT_PORT") {
            config.raft_port = port.parse().unwrap_or(9090);
        }
        if let Ok(peers_str) = std::env::var("WAFFLEDB_PEERS") {
            config.peers = peers_str
                .split(',')
                .map(|p| p.trim().to_string())
                .filter(|p| !p.is_empty())
                .collect();
        }
        if let Ok(timeout) = std::env::var("WAFFLEDB_ELECTION_TIMEOUT_MS") {
            config.election_timeout_ms = timeout.parse().unwrap_or(150);
        }
        if let Ok(interval) = std::env::var("WAFFLEDB_HEARTBEAT_INTERVAL_MS") {
            config.heartbeat_interval_ms = interval.parse().unwrap_or(50);
        }

        config
    }
}
