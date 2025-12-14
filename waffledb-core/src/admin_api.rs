/// Admin API for cluster management, rebalancing, and diagnostics
///
/// Implements:
/// - Cluster health monitoring
/// - Shard rebalancing and migration
/// - Snapshot management endpoints
/// - Configuration management
/// - Token-based authentication
/// - Admin CLI tool support

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::net::SocketAddr;

/// Authentication token
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdminToken {
    pub token: String,
    pub permissions: Vec<String>,
}

impl AdminToken {
    pub fn new(token: String, permissions: Vec<String>) -> Self {
        Self { token, permissions }
    }

    pub fn has_permission(&self, permission: &str) -> bool {
        self.permissions.contains(&permission.to_string())
    }
}

/// Cluster health status
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HealthStatus {
    Healthy,
    Degraded,
    Critical,
}

/// Node status in cluster
#[derive(Debug, Clone)]
pub struct NodeStatus {
    pub node_id: String,
    pub address: String,
    pub is_leader: bool,
    pub vector_count: u64,
    pub shard_count: usize,
    pub memory_mb: u64,
    pub replication_lag_ms: u64,
}

impl NodeStatus {
    pub fn new(node_id: String, address: String) -> Self {
        Self {
            node_id,
            address,
            is_leader: false,
            vector_count: 0,
            shard_count: 0,
            memory_mb: 0,
            replication_lag_ms: 0,
        }
    }
}

/// Cluster health information
#[derive(Debug, Clone)]
pub struct ClusterHealth {
    pub status: HealthStatus,
    pub nodes: Vec<NodeStatus>,
    pub total_vectors: u64,
    pub total_shards: usize,
    pub rebalancing_active: bool,
}

impl ClusterHealth {
    pub fn new() -> Self {
        Self {
            status: HealthStatus::Healthy,
            nodes: Vec::new(),
            total_vectors: 0,
            total_shards: 0,
            rebalancing_active: false,
        }
    }
}

impl Default for ClusterHealth {
    fn default() -> Self {
        Self::new()
    }
}

/// Rebalancing operation
#[derive(Debug, Clone)]
pub struct RebalanceOperation {
    pub operation_id: String,
    pub source_node: String,
    pub destination_node: String,
    pub shard_id: usize,
    pub vector_count: u64,
    pub progress_percent: u32,
    pub status: String,
}

impl RebalanceOperation {
    pub fn new(
        source_node: String,
        destination_node: String,
        shard_id: usize,
        vector_count: u64,
    ) -> Self {
        Self {
            operation_id: format!("rebal_{}_{}", shard_id, rand_seed()),
            source_node,
            destination_node,
            shard_id,
            vector_count,
            progress_percent: 0,
            status: "pending".to_string(),
        }
    }

    pub fn start(&mut self) {
        self.status = "running".to_string();
        self.progress_percent = 1;
    }

    pub fn complete(&mut self) {
        self.status = "completed".to_string();
        self.progress_percent = 100;
    }

    pub fn fail(&mut self, reason: String) {
        self.status = format!("failed: {}", reason);
        self.progress_percent = 0;
    }
}

/// Configuration option
#[derive(Debug, Clone)]
pub struct ConfigOption {
    pub key: String,
    pub value: String,
    pub data_type: String,
    pub description: String,
    pub read_only: bool,
}

impl ConfigOption {
    pub fn new(key: String, value: String, data_type: String, description: String) -> Self {
        Self {
            key,
            value,
            data_type,
            description,
            read_only: false,
        }
    }
}

/// Admin API server
pub struct AdminApiServer {
    tokens: Arc<RwLock<HashMap<String, AdminToken>>>,
    cluster_health: Arc<RwLock<ClusterHealth>>,
    rebalance_operations: Arc<RwLock<HashMap<String, RebalanceOperation>>>,
    config: Arc<RwLock<HashMap<String, ConfigOption>>>,
    server_addr: SocketAddr,
}

impl AdminApiServer {
    pub fn new(addr: SocketAddr) -> Self {
        Self {
            tokens: Arc::new(RwLock::new(HashMap::new())),
            cluster_health: Arc::new(RwLock::new(ClusterHealth::new())),
            rebalance_operations: Arc::new(RwLock::new(HashMap::new())),
            config: Arc::new(RwLock::new(HashMap::new())),
            server_addr: addr,
        }
    }

    /// Register an admin token
    pub fn register_token(&self, token: AdminToken) -> Result<(), String> {
        let mut tokens = self.tokens.write().unwrap();
        if tokens.contains_key(&token.token) {
            return Err("Token already registered".to_string());
        }
        tokens.insert(token.token.clone(), token);
        Ok(())
    }

    /// Verify token and check permission
    pub fn verify_token(&self, token_str: &str, required_permission: &str) -> Result<(), String> {
        let tokens = self.tokens.read().unwrap();
        tokens
            .get(token_str)
            .ok_or_else(|| "Invalid token".to_string())?
            .has_permission(required_permission)
            .then_some(())
            .ok_or_else(|| "Insufficient permissions".to_string())
    }

    /// Get cluster health
    pub fn get_health(&self) -> ClusterHealth {
        self.cluster_health.read().unwrap().clone()
    }

    /// Update cluster health
    pub fn update_health<F>(&self, f: F)
    where
        F: FnOnce(&mut ClusterHealth),
    {
        let mut health = self.cluster_health.write().unwrap();
        f(&mut health);
    }

    /// Add node to cluster
    pub fn add_node(&self, node: NodeStatus) -> Result<(), String> {
        let mut health = self.cluster_health.write().unwrap();
        if health.nodes.iter().any(|n| n.node_id == node.node_id) {
            return Err("Node already exists".to_string());
        }
        health.nodes.push(node);
        Ok(())
    }

    /// Remove node from cluster
    pub fn remove_node(&self, node_id: &str) -> Result<(), String> {
        let mut health = self.cluster_health.write().unwrap();
        let initial_len = health.nodes.len();
        health.nodes.retain(|n| n.node_id != node_id);
        if health.nodes.len() < initial_len {
            Ok(())
        } else {
            Err("Node not found".to_string())
        }
    }

    /// Start a rebalancing operation
    pub fn start_rebalance(
        &self,
        source: String,
        destination: String,
        shard_id: usize,
        vector_count: u64,
    ) -> Result<String, String> {
        let mut rebal = RebalanceOperation::new(source, destination, shard_id, vector_count);
        rebal.start();
        let op_id = rebal.operation_id.clone();

        let mut ops = self.rebalance_operations.write().unwrap();
        ops.insert(op_id.clone(), rebal);

        Ok(op_id)
    }

    /// Get rebalancing operation
    pub fn get_rebalance_op(&self, op_id: &str) -> Option<RebalanceOperation> {
        self.rebalance_operations.read().unwrap().get(op_id).cloned()
    }

    /// Update rebalancing operation progress
    pub fn update_rebalance_progress(&self, op_id: &str, progress: u32) -> Result<(), String> {
        let mut ops = self.rebalance_operations.write().unwrap();
        ops.get_mut(op_id)
            .ok_or_else(|| "Operation not found".to_string())?
            .progress_percent = progress;
        Ok(())
    }

    /// Complete rebalancing operation
    pub fn complete_rebalance(&self, op_id: &str) -> Result<(), String> {
        let mut ops = self.rebalance_operations.write().unwrap();
        ops.get_mut(op_id)
            .ok_or_else(|| "Operation not found".to_string())?
            .complete();
        Ok(())
    }

    /// List active rebalancing operations
    pub fn list_rebalance_ops(&self) -> Vec<RebalanceOperation> {
        self.rebalance_operations
            .read()
            .unwrap()
            .values()
            .cloned()
            .collect()
    }

    /// Set configuration option
    pub fn set_config(&self, key: String, value: String) -> Result<(), String> {
        let mut config = self.config.write().unwrap();
        if let Some(opt) = config.get(&key) {
            if opt.read_only {
                return Err("Configuration is read-only".to_string());
            }
        }
        config.insert(key.clone(), ConfigOption::new(key, value, "string".to_string(), "".to_string()));
        Ok(())
    }

    /// Get configuration option
    pub fn get_config(&self, key: &str) -> Option<ConfigOption> {
        self.config.read().unwrap().get(key).cloned()
    }

    /// Get all configuration
    pub fn list_config(&self) -> Vec<ConfigOption> {
        self.config
            .read()
            .unwrap()
            .values()
            .cloned()
            .collect()
    }

    /// Get server address
    pub fn server_addr(&self) -> SocketAddr {
        self.server_addr
    }
}

// Helper for test IDs
fn rand_seed() -> u64 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    
    let mut hasher = DefaultHasher::new();
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos().hash(&mut hasher);
    hasher.finish() % 100000
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn test_admin_token_creation() {
        let token = AdminToken::new("test_token".to_string(), vec!["read".to_string()]);
        assert_eq!(token.token, "test_token");
        assert_eq!(token.permissions.len(), 1);
    }

    #[test]
    fn test_admin_token_permission_check() {
        let token = AdminToken::new(
            "test_token".to_string(),
            vec!["read".to_string(), "write".to_string()],
        );
        assert!(token.has_permission("read"));
        assert!(token.has_permission("write"));
        assert!(!token.has_permission("admin"));
    }

    #[test]
    fn test_node_status_creation() {
        let node = NodeStatus::new("node1".to_string(), "127.0.0.1:8000".to_string());
        assert_eq!(node.node_id, "node1");
        assert_eq!(node.address, "127.0.0.1:8000");
        assert!(!node.is_leader);
    }

    #[test]
    fn test_cluster_health_creation() {
        let health = ClusterHealth::new();
        assert_eq!(health.status, HealthStatus::Healthy);
        assert_eq!(health.nodes.len(), 0);
    }

    #[test]
    fn test_rebalance_operation_creation() {
        let rebal = RebalanceOperation::new(
            "node1".to_string(),
            "node2".to_string(),
            0,
            1000,
        );
        assert_eq!(rebal.source_node, "node1");
        assert_eq!(rebal.destination_node, "node2");
        assert_eq!(rebal.shard_id, 0);
        assert_eq!(rebal.vector_count, 1000);
        assert_eq!(rebal.status, "pending");
    }

    #[test]
    fn test_rebalance_operation_start() {
        let mut rebal = RebalanceOperation::new(
            "node1".to_string(),
            "node2".to_string(),
            0,
            1000,
        );
        rebal.start();
        assert_eq!(rebal.status, "running");
        assert!(rebal.progress_percent > 0);
    }

    #[test]
    fn test_rebalance_operation_complete() {
        let mut rebal = RebalanceOperation::new(
            "node1".to_string(),
            "node2".to_string(),
            0,
            1000,
        );
        rebal.start();
        rebal.complete();
        assert_eq!(rebal.status, "completed");
        assert_eq!(rebal.progress_percent, 100);
    }

    #[test]
    fn test_rebalance_operation_fail() {
        let mut rebal = RebalanceOperation::new(
            "node1".to_string(),
            "node2".to_string(),
            0,
            1000,
        );
        rebal.fail("timeout".to_string());
        assert!(rebal.status.contains("failed"));
        assert_eq!(rebal.progress_percent, 0);
    }

    #[test]
    fn test_config_option_creation() {
        let config = ConfigOption::new(
            "max_vectors".to_string(),
            "1000000".to_string(),
            "integer".to_string(),
            "Maximum vectors per collection".to_string(),
        );
        assert_eq!(config.key, "max_vectors");
        assert_eq!(config.value, "1000000");
        assert!(!config.read_only);
    }

    #[test]
    fn test_admin_api_server_creation() {
        let addr = "127.0.0.1:9000".parse().unwrap();
        let server = AdminApiServer::new(addr);
        assert_eq!(server.server_addr(), addr);
    }

    #[test]
    fn test_admin_api_register_token() {
        let addr = "127.0.0.1:9000".parse().unwrap();
        let server = AdminApiServer::new(addr);
        let token = AdminToken::new("secret".to_string(), vec!["admin".to_string()]);
        
        assert!(server.register_token(token).is_ok());
    }

    #[test]
    fn test_admin_api_register_duplicate_token() {
        let addr = "127.0.0.1:9000".parse().unwrap();
        let server = AdminApiServer::new(addr);
        let token = AdminToken::new("secret".to_string(), vec!["admin".to_string()]);
        
        assert!(server.register_token(token.clone()).is_ok());
        assert!(server.register_token(token).is_err());
    }

    #[test]
    fn test_admin_api_verify_token() {
        let addr = "127.0.0.1:9000".parse().unwrap();
        let server = AdminApiServer::new(addr);
        let token = AdminToken::new("secret".to_string(), vec!["admin".to_string()]);
        
        assert!(server.register_token(token).is_ok());
        assert!(server.verify_token("secret", "admin").is_ok());
        assert!(server.verify_token("secret", "read").is_err());
        assert!(server.verify_token("invalid", "admin").is_err());
    }

    #[test]
    fn test_admin_api_get_health() {
        let addr = "127.0.0.1:9000".parse().unwrap();
        let server = AdminApiServer::new(addr);
        let health = server.get_health();
        assert_eq!(health.status, HealthStatus::Healthy);
    }

    #[test]
    fn test_admin_api_update_health() {
        let addr = "127.0.0.1:9000".parse().unwrap();
        let server = AdminApiServer::new(addr);
        
        server.update_health(|health| {
            health.status = HealthStatus::Degraded;
            health.total_vectors = 1000;
        });

        let health = server.get_health();
        assert_eq!(health.status, HealthStatus::Degraded);
        assert_eq!(health.total_vectors, 1000);
    }

    #[test]
    fn test_admin_api_add_node() {
        let addr = "127.0.0.1:9000".parse().unwrap();
        let server = AdminApiServer::new(addr);
        let node = NodeStatus::new("node1".to_string(), "127.0.0.1:8000".to_string());
        
        assert!(server.add_node(node).is_ok());
        
        let health = server.get_health();
        assert_eq!(health.nodes.len(), 1);
    }

    #[test]
    fn test_admin_api_add_duplicate_node() {
        let addr = "127.0.0.1:9000".parse().unwrap();
        let server = AdminApiServer::new(addr);
        let node = NodeStatus::new("node1".to_string(), "127.0.0.1:8000".to_string());
        
        assert!(server.add_node(node.clone()).is_ok());
        assert!(server.add_node(node).is_err());
    }

    #[test]
    fn test_admin_api_remove_node() {
        let addr = "127.0.0.1:9000".parse().unwrap();
        let server = AdminApiServer::new(addr);
        let node = NodeStatus::new("node1".to_string(), "127.0.0.1:8000".to_string());
        
        assert!(server.add_node(node).is_ok());
        assert!(server.remove_node("node1").is_ok());
        
        let health = server.get_health();
        assert_eq!(health.nodes.len(), 0);
    }

    #[test]
    fn test_admin_api_remove_non_existent_node() {
        let addr = "127.0.0.1:9000".parse().unwrap();
        let server = AdminApiServer::new(addr);
        
        assert!(server.remove_node("node1").is_err());
    }

    #[test]
    fn test_admin_api_start_rebalance() {
        let addr = "127.0.0.1:9000".parse().unwrap();
        let server = AdminApiServer::new(addr);
        
        let result = server.start_rebalance(
            "node1".to_string(),
            "node2".to_string(),
            0,
            1000,
        );
        assert!(result.is_ok());
        
        let op_id = result.unwrap();
        let op = server.get_rebalance_op(&op_id);
        assert!(op.is_some());
        assert_eq!(op.unwrap().status, "running");
    }

    #[test]
    fn test_admin_api_get_rebalance_op() {
        let addr = "127.0.0.1:9000".parse().unwrap();
        let server = AdminApiServer::new(addr);
        
        let result = server.start_rebalance(
            "node1".to_string(),
            "node2".to_string(),
            0,
            1000,
        );
        let op_id = result.unwrap();
        
        let op = server.get_rebalance_op(&op_id);
        assert!(op.is_some());
    }

    #[test]
    fn test_admin_api_update_rebalance_progress() {
        let addr = "127.0.0.1:9000".parse().unwrap();
        let server = AdminApiServer::new(addr);
        
        let result = server.start_rebalance(
            "node1".to_string(),
            "node2".to_string(),
            0,
            1000,
        );
        let op_id = result.unwrap();
        
        assert!(server.update_rebalance_progress(&op_id, 50).is_ok());
        let op = server.get_rebalance_op(&op_id).unwrap();
        assert_eq!(op.progress_percent, 50);
    }

    #[test]
    fn test_admin_api_complete_rebalance() {
        let addr = "127.0.0.1:9000".parse().unwrap();
        let server = AdminApiServer::new(addr);
        
        let result = server.start_rebalance(
            "node1".to_string(),
            "node2".to_string(),
            0,
            1000,
        );
        let op_id = result.unwrap();
        
        assert!(server.complete_rebalance(&op_id).is_ok());
        let op = server.get_rebalance_op(&op_id).unwrap();
        assert_eq!(op.status, "completed");
    }

    #[test]
    fn test_admin_api_list_rebalance_ops() {
        let addr = "127.0.0.1:9000".parse().unwrap();
        let server = AdminApiServer::new(addr);
        
        let _op1 = server.start_rebalance(
            "node1".to_string(),
            "node2".to_string(),
            0,
            1000,
        );
        let _op2 = server.start_rebalance(
            "node2".to_string(),
            "node3".to_string(),
            1,
            1000,
        );
        
        let ops = server.list_rebalance_ops();
        assert_eq!(ops.len(), 2);
    }

    #[test]
    fn test_admin_api_set_config() {
        let addr = "127.0.0.1:9000".parse().unwrap();
        let server = AdminApiServer::new(addr);
        
        assert!(server.set_config("max_vectors".to_string(), "1000000".to_string()).is_ok());
        
        let config = server.get_config("max_vectors");
        assert!(config.is_some());
        assert_eq!(config.unwrap().value, "1000000");
    }

    #[test]
    fn test_admin_api_get_config() {
        let addr = "127.0.0.1:9000".parse().unwrap();
        let server = AdminApiServer::new(addr);
        
        server.set_config("max_vectors".to_string(), "1000000".to_string()).unwrap();
        
        let config = server.get_config("max_vectors");
        assert!(config.is_some());
    }

    #[test]
    fn test_admin_api_list_config() {
        let addr = "127.0.0.1:9000".parse().unwrap();
        let server = AdminApiServer::new(addr);
        
        server.set_config("max_vectors".to_string(), "1000000".to_string()).unwrap();
        server.set_config("max_shards".to_string(), "100".to_string()).unwrap();
        
        let config = server.list_config();
        assert_eq!(config.len(), 2);
    }

    #[test]
    fn test_health_status_enum() {
        assert_eq!(HealthStatus::Healthy, HealthStatus::Healthy);
        assert_ne!(HealthStatus::Healthy, HealthStatus::Degraded);
    }
}
