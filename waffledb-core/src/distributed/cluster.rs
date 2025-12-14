/// Cluster management for multi-node enterprise deployments
///
/// Provides:
/// - Node discovery (static config + health checks)
/// - Cluster membership tracking
/// - Health-check coordination
/// - Zero overhead in single-node (OSS) mode

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Instant, Duration};
use parking_lot::RwLock;

/// Node identity in the cluster
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct NodeId(pub String);

impl NodeId {
    pub fn new(id: impl Into<String>) -> Self {
        NodeId(id.into())
    }
}

/// Health status of a node in the cluster
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeHealth {
    /// Node is responding to health checks
    Healthy,
    /// Node is not responding (timeout on health check)
    Unreachable,
    /// Node was healthy, now suspected to be down
    Suspect,
}

/// Information about a peer node in the cluster
#[derive(Debug, Clone)]
pub struct NodeInfo {
    pub node_id: NodeId,
    pub address: String,
    pub health: NodeHealth,
    pub last_heartbeat: Instant,
    pub raft_term: u64,
}

/// Cluster membership and node tracking
///
/// In OSS (single-node) mode, this remains a no-op.
/// In Enterprise mode, tracks all peers and their health.
#[derive(Debug)]
pub struct ClusterManager {
    /// This node's ID
    self_id: NodeId,
    
    /// All known peers (excluding self)
    peers: Arc<RwLock<HashMap<NodeId, NodeInfo>>>,
    
    /// Whether multi-node mode is enabled
    multi_node_mode: bool,
}

impl ClusterManager {
    /// Create a new cluster manager
    pub fn new(self_id: impl Into<String>) -> Self {
        ClusterManager {
            self_id: NodeId::new(self_id),
            peers: Arc::new(RwLock::new(HashMap::new())),
            multi_node_mode: false,
        }
    }

    /// Initialize from static config (list of node addresses)
    ///
    /// Example:
    /// ```
    /// // env: CLUSTER_NODES=node1:8000,node2:8000,node3:8000
    /// // env: NODE_ID=node1
    /// ```
    pub fn init_from_static_config(
        self_id: impl Into<String>,
        peer_addresses: Vec<String>,
    ) -> Self {
        let self_id = NodeId::new(self_id);
        let mut peers = HashMap::new();

        for addr in peer_addresses {
            let node_id = NodeId::new(extract_node_id_from_addr(&addr));
            if node_id != self_id {
                peers.insert(
                    node_id.clone(),
                    NodeInfo {
                        node_id,
                        address: addr,
                        health: NodeHealth::Suspect,
                        last_heartbeat: Instant::now(),
                        raft_term: 0,
                    },
                );
            }
        }

        ClusterManager {
            self_id,
            peers: Arc::new(RwLock::new(peers)),
            multi_node_mode: true,
        }
    }

    /// Get this node's ID
    pub fn self_id(&self) -> &NodeId {
        &self.self_id
    }

    /// Get all healthy peers
    pub fn healthy_peers(&self) -> Vec<NodeInfo> {
        self.peers
            .read()
            .values()
            .filter(|info| info.health == NodeHealth::Healthy)
            .cloned()
            .collect()
    }

    /// Get all peers (regardless of health)
    pub fn all_peers(&self) -> Vec<NodeInfo> {
        self.peers.read().values().cloned().collect()
    }

    /// Update node health status
    pub fn update_node_health(&self, node_id: &NodeId, health: NodeHealth) {
        if let Some(info) = self.peers.write().get_mut(node_id) {
            info.health = health;
            info.last_heartbeat = Instant::now();
        }
    }

    /// Mark node as unreachable (failed health check)
    pub fn mark_unreachable(&self, node_id: &NodeId) {
        self.update_node_health(node_id, NodeHealth::Unreachable);
    }

    /// Mark node as healthy (passed health check)
    pub fn mark_healthy(&self, node_id: &NodeId) {
        self.update_node_health(node_id, NodeHealth::Healthy);
    }

    /// Mark node as suspect (health check timeout but not yet unreachable)
    pub fn mark_suspect(&self, node_id: &NodeId) {
        self.update_node_health(node_id, NodeHealth::Suspect);
    }

    /// Get information about a specific peer
    pub fn get_peer(&self, node_id: &NodeId) -> Option<NodeInfo> {
        self.peers.read().get(node_id).cloned()
    }

    /// Get node information (alias for get_peer)
    pub fn get_node_info(&self, node_id: &NodeId) -> Option<NodeInfo> {
        self.get_peer(node_id)
    }

    /// Number of healthy peers
    pub fn healthy_peer_count(&self) -> usize {
        self.peers
            .read()
            .values()
            .filter(|info| info.health == NodeHealth::Healthy)
            .count()
    }

    /// Total cluster size (including self)
    pub fn cluster_size(&self) -> usize {
        self.peers.read().len() + 1
    }

    /// Check if we have quorum (at least half + 1 of cluster is healthy)
    pub fn has_quorum(&self) -> bool {
        let total = self.cluster_size();
        let healthy = self.healthy_peer_count() + 1; // +1 for self (always healthy)
        healthy > total / 2
    }
}

/// Extract node ID from address string
/// Example: "node1:8000" -> "node1"
fn extract_node_id_from_addr(addr: &str) -> String {
    addr.split(':').next().unwrap_or("unknown").to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cluster_manager_single_node() {
        let cluster = ClusterManager::new("node1");
        assert_eq!(cluster.self_id().0, "node1");
        assert_eq!(cluster.cluster_size(), 1);
        assert_eq!(cluster.healthy_peers().len(), 0);
    }

    #[cfg(feature = "enterprise")]
    #[test]
    fn test_cluster_manager_multi_node() {
        let peers = vec![
            "node2:8000".to_string(),
            "node3:8000".to_string(),
        ];
        let cluster = ClusterManager::init_from_static_config("node1", peers);

        assert_eq!(cluster.self_id().0, "node1");
        assert_eq!(cluster.cluster_size(), 3);
        assert_eq!(cluster.all_peers().len(), 2);
        assert_eq!(cluster.healthy_peers().len(), 0); // All start as suspect
    }

    #[cfg(feature = "enterprise")]
    #[test]
    fn test_health_tracking() {
        let peers = vec!["node2:8000".to_string()];
        let cluster = ClusterManager::init_from_static_config("node1", peers);

        let node2 = NodeId::new("node2");
        assert_eq!(cluster.get_peer(&node2).unwrap().health, NodeHealth::Suspect);

        cluster.mark_healthy(&node2);
        assert_eq!(cluster.get_peer(&node2).unwrap().health, NodeHealth::Healthy);

        cluster.mark_unreachable(&node2);
        assert_eq!(cluster.get_peer(&node2).unwrap().health, NodeHealth::Unreachable);
    }

    #[cfg(feature = "enterprise")]
    #[test]
    fn test_quorum_calculation() {
        let peers = vec![
            "node2:8000".to_string(),
            "node3:8000".to_string(),
            "node4:8000".to_string(),
        ];
        let cluster = ClusterManager::init_from_static_config("node1", peers);

        // 4 total nodes, need 3 for quorum (self + 2 others)
        assert!(!cluster.has_quorum()); // 0 healthy peers + self = 1

        cluster.mark_healthy(&NodeId::new("node2"));
        assert!(!cluster.has_quorum()); // 1 healthy + self = 2

        cluster.mark_healthy(&NodeId::new("node3"));
        assert!(cluster.has_quorum()); // 2 healthy + self = 3 ✓
    }
}
