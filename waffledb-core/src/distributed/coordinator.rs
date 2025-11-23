/// Cluster coordinator
/// 
/// Manages cluster state, handles failover, coordinates replica placement.
/// Lightweight - no gossip, just simple HTTP endpoints.

use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};
use crate::core::errors::{Result, WaffleError, ErrorCode};
use crate::distributed::node::{DistributedNode, NodeConfig};
use crate::distributed::sharding::{ShardId, HashSharding, ShardingStrategy};

/// Coordinator configuration
#[derive(Debug, Clone)]
pub struct CoordinatorConfig {
    /// Number of shards in cluster
    pub num_shards: u32,
    /// Replication factor
    pub replication_factor: u32,
    /// Nodes in cluster
    pub nodes: Vec<NodeConfig>,
}

/// Cluster health status
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClusterHealth {
    /// All nodes healthy
    Healthy,
    /// Some nodes down but quorum maintained
    Degraded,
    /// Quorum lost, not safe to accept writes
    Unhealthy,
}

/// Replica assignment for a shard
#[derive(Debug, Clone)]
pub struct ReplicaAssignment {
    pub shard_id: ShardId,
    pub primary_node: String,
    pub replicas: Vec<String>,
}

/// Cluster coordinator
pub struct Coordinator {
    config: CoordinatorConfig,
    sharding: Box<dyn ShardingStrategy>,
    nodes: HashMap<String, NodeConfig>,
    replica_assignments: HashMap<ShardId, ReplicaAssignment>,
    last_heartbeat: HashMap<String, u64>,
}

impl std::fmt::Debug for Coordinator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Coordinator")
            .field("config", &self.config)
            .field("nodes", &self.nodes)
            .field("replica_assignments", &self.replica_assignments)
            .field("last_heartbeat", &self.last_heartbeat)
            .finish()
    }
}

impl Coordinator {
    /// Create new coordinator
    pub fn new(config: CoordinatorConfig) -> Result<Self> {
        if config.num_shards == 0 {
            return Err(WaffleError::WithCode {
                code: ErrorCode::ValidationFailed,
                message: "num_shards must be > 0".to_string(),
            });
        }

        if config.nodes.is_empty() {
            return Err(WaffleError::WithCode {
                code: ErrorCode::ValidationFailed,
                message: "nodes cannot be empty".to_string(),
            });
        }

        let sharding = Box::new(HashSharding::new(config.num_shards, config.replication_factor)?);

        let mut nodes = HashMap::new();
        for node_config in &config.nodes {
            nodes.insert(node_config.node_id.clone(), node_config.clone());
        }

        let mut replica_assignments = HashMap::new();
        let mut node_list: Vec<_> = nodes.keys().cloned().collect();
        node_list.sort();

        // Assign replicas to nodes
        for shard_id in sharding.all_shards() {
            let primary_idx = (shard_id.0 as usize) % node_list.len();
            let primary_node = node_list[primary_idx].clone();

            let mut replicas = Vec::new();
            let replica_ids = sharding.get_replicas(shard_id);

            for (_i, replica_shard_id) in replica_ids.iter().enumerate() {
                let replica_idx = (replica_shard_id.0 as usize) % node_list.len();
                if replica_idx != primary_idx {
                    replicas.push(node_list[replica_idx].clone());
                }
            }

            replica_assignments.insert(
                shard_id,
                ReplicaAssignment {
                    shard_id,
                    primary_node,
                    replicas,
                },
            );
        }

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let mut coordinator = Self {
            config,
            sharding,
            nodes,
            replica_assignments,
            last_heartbeat: HashMap::new(),
        };

        for node_id in coordinator.nodes.keys() {
            coordinator.last_heartbeat.insert(node_id.clone(), now);
        }

        Ok(coordinator)
    }

    /// Get replica assignment for a shard
    pub fn get_replica_assignment(&self, shard_id: ShardId) -> Option<&ReplicaAssignment> {
        self.replica_assignments.get(&shard_id)
    }

    /// Get all replica assignments
    pub fn all_replica_assignments(&self) -> Vec<&ReplicaAssignment> {
        self.replica_assignments.values().collect()
    }

    /// Record heartbeat from node
    pub fn record_heartbeat(&mut self, node_id: &str) -> Result<()> {
        if !self.nodes.contains_key(node_id) {
            return Err(WaffleError::WithCode {
                code: ErrorCode::ValidationFailed,
                message: format!("Unknown node: {}", node_id),
            });
        }

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        self.last_heartbeat.insert(node_id.to_string(), now);
        Ok(())
    }

    /// Get node health (based on heartbeat timeout)
    pub fn is_node_healthy(&self, node_id: &str, timeout_secs: u64) -> bool {
        if let Some(last_beat) = self.last_heartbeat.get(node_id) {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs();
            now - last_beat < timeout_secs
        } else {
            false
        }
    }

    /// Get cluster health
    pub fn cluster_health(&self, timeout_secs: u64) -> ClusterHealth {
        let total_nodes = self.nodes.len();
        let healthy_nodes = self
            .nodes
            .keys()
            .filter(|node_id| self.is_node_healthy(node_id, timeout_secs))
            .count();

        if healthy_nodes == total_nodes {
            ClusterHealth::Healthy
        } else if healthy_nodes > total_nodes / 2 {
            ClusterHealth::Degraded
        } else {
            ClusterHealth::Unhealthy
        }
    }

    /// Get all node IDs
    pub fn all_nodes(&self) -> Vec<&str> {
        self.nodes.keys().map(|s| s.as_str()).collect()
    }

    /// Get shard distribution across nodes
    pub fn shard_distribution(&self) -> HashMap<String, Vec<ShardId>> {
        let mut distribution: HashMap<String, Vec<ShardId>> = HashMap::new();

        for (shard_id, assignment) in &self.replica_assignments {
            distribution
                .entry(assignment.primary_node.clone())
                .or_insert_with(Vec::new)
                .push(*shard_id);

            for replica_node in &assignment.replicas {
                distribution
                    .entry(replica_node.clone())
                    .or_insert_with(Vec::new)
                    .push(*shard_id);
            }
        }

        distribution
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distributed::replication::ReplicationConfig;

    fn create_test_coordinator() -> Coordinator {
        let config = CoordinatorConfig {
            num_shards: 16,
            replication_factor: 3,
            nodes: vec![
                NodeConfig {
                    node_id: "node0".to_string(),
                    shard_ids: vec![],
                    replication: ReplicationConfig::default(),
                    max_memory: 1_000_000_000,
                },
                NodeConfig {
                    node_id: "node1".to_string(),
                    shard_ids: vec![],
                    replication: ReplicationConfig::default(),
                    max_memory: 1_000_000_000,
                },
                NodeConfig {
                    node_id: "node2".to_string(),
                    shard_ids: vec![],
                    replication: ReplicationConfig::default(),
                    max_memory: 1_000_000_000,
                },
            ],
        };

        Coordinator::new(config).unwrap()
    }

    #[test]
    fn test_coordinator_creation() {
        let coordinator = create_test_coordinator();
        assert_eq!(coordinator.all_nodes().len(), 3);
    }

    #[test]
    fn test_replica_assignments() {
        let coordinator = create_test_coordinator();
        let assignments = coordinator.all_replica_assignments();
        assert_eq!(assignments.len(), 16);

        for assignment in assignments {
            assert!(!assignment.primary_node.is_empty());
            // With 3 nodes and 16 shards, we may get 1-2 replicas per shard
            assert!(assignment.replicas.len() >= 1 && assignment.replicas.len() <= 2);
        }
    }

    #[test]
    fn test_heartbeat_tracking() {
        let mut coordinator = create_test_coordinator();

        coordinator.record_heartbeat("node0").unwrap();
        assert!(coordinator.is_node_healthy("node0", 10));
    }

    #[test]
    fn test_shard_distribution() {
        let coordinator = create_test_coordinator();
        let distribution = coordinator.shard_distribution();

        // Each shard should be distributed across nodes
        // With 3 nodes and 16 shards, total should be around 16 + (16-dup_count)
        let total_replicas: usize = distribution.values().map(|v| v.len()).sum();
        // Should have all primary nodes (16) plus most replicas (allowing for dupes)
        assert!(total_replicas >= 16 && total_replicas <= 48);
    }

    #[test]
    fn test_cluster_health_all_healthy() {
        let mut coordinator = create_test_coordinator();

        for node_id in ["node0", "node1", "node2"] {
            coordinator.record_heartbeat(node_id).ok();
        }

        assert_eq!(coordinator.cluster_health(10), ClusterHealth::Healthy);
    }
}
