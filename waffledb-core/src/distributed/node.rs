/// Individual distributed node
/// 
/// A single node in the WaffleDB cluster.
/// Runs local HNSW + metadata, receives replication from leader.

use crate::core::errors::{Result, WaffleError, ErrorCode};
use crate::distributed::sharding::ShardId;
use crate::distributed::replication::{ReplicationManager, ReplicationConfig, ReplicationState};

/// Node configuration
#[derive(Debug, Clone)]
pub struct NodeConfig {
    /// Unique node ID in cluster
    pub node_id: String,
    /// Shards this node is responsible for
    pub shard_ids: Vec<ShardId>,
    /// Replication configuration
    pub replication: ReplicationConfig,
    /// Max memory for local index (bytes)
    pub max_memory: u64,
}

/// Node runtime state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeState {
    /// Node is starting up
    Starting,
    /// Node is ready for queries
    Ready,
    /// Node is syncing with leader
    Syncing,
    /// Node encountered an error
    Failed,
}

/// A distributed node instance
#[derive(Debug)]
pub struct DistributedNode {
    config: NodeConfig,
    state: NodeState,
    replication_manager: ReplicationManager,
    current_memory: u64,
}

impl DistributedNode {
    /// Create a new distributed node
    pub fn new(config: NodeConfig) -> Result<Self> {
        if config.node_id.is_empty() {
            return Err(WaffleError::WithCode {
                code: ErrorCode::ValidationFailed,
                message: "node_id cannot be empty".to_string(),
            });
        }

        if config.shard_ids.is_empty() {
            return Err(WaffleError::WithCode {
                code: ErrorCode::ValidationFailed,
                message: "shard_ids cannot be empty".to_string(),
            });
        }

        let replication_manager = ReplicationManager::new(config.replication.clone())?;

        Ok(Self {
            config,
            state: NodeState::Starting,
            replication_manager,
            current_memory: 0,
        })
    }

    /// Get node ID
    pub fn node_id(&self) -> &str {
        &self.config.node_id
    }

    /// Get responsible shard IDs
    pub fn shard_ids(&self) -> &[ShardId] {
        &self.config.shard_ids
    }

    /// Get current node state
    pub fn state(&self) -> NodeState {
        self.state
    }

    /// Transition node to ready state
    pub fn become_ready(&mut self) -> Result<()> {
        self.state = NodeState::Ready;
        Ok(())
    }

    /// Transition node to syncing state
    pub fn start_sync(&mut self) -> Result<()> {
        self.state = NodeState::Syncing;
        Ok(())
    }

    /// Get replication manager (mutable)
    pub fn replication_manager_mut(&mut self) -> &mut ReplicationManager {
        &mut self.replication_manager
    }

    /// Get replication manager (immutable)
    pub fn replication_manager(&self) -> &ReplicationManager {
        &self.replication_manager
    }

    /// Get current memory usage
    pub fn current_memory(&self) -> u64 {
        self.current_memory
    }

    /// Check if node has capacity for more data
    pub fn has_capacity(&self, required_bytes: u64) -> bool {
        self.current_memory + required_bytes <= self.config.max_memory
    }

    /// Allocate memory
    pub fn allocate_memory(&mut self, bytes: u64) -> Result<()> {
        if !self.has_capacity(bytes) {
            return Err(WaffleError::WithCode {
                code: ErrorCode::StorageIOError,
                message: format!(
                    "Insufficient memory: {} + {} > {}",
                    self.current_memory, bytes, self.config.max_memory
                ),
            });
        }

        self.current_memory += bytes;
        Ok(())
    }

    /// Deallocate memory
    pub fn deallocate_memory(&mut self, bytes: u64) {
        self.current_memory = self.current_memory.saturating_sub(bytes);
    }

    /// Get replication state
    pub fn replication_state(&self) -> ReplicationState {
        self.replication_manager.state()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_node() -> DistributedNode {
        let config = NodeConfig {
            node_id: "node0".to_string(),
            shard_ids: vec![ShardId(0), ShardId(1)],
            replication: ReplicationConfig::default(),
            max_memory: 1_000_000_000,
        };

        DistributedNode::new(config).unwrap()
    }

    #[test]
    fn test_node_creation() {
        let node = create_test_node();
        assert_eq!(node.node_id(), "node0");
        assert_eq!(node.shard_ids().len(), 2);
        assert_eq!(node.state(), NodeState::Starting);
    }

    #[test]
    fn test_node_become_ready() {
        let mut node = create_test_node();
        node.become_ready().unwrap();
        assert_eq!(node.state(), NodeState::Ready);
    }

    #[test]
    fn test_node_memory_allocation() {
        let mut node = create_test_node();

        node.allocate_memory(100_000).unwrap();
        assert_eq!(node.current_memory(), 100_000);

        assert!(node.has_capacity(500_000_000));
    }

    #[test]
    fn test_node_memory_overflow() {
        let mut node = DistributedNode::new(NodeConfig {
            node_id: "node0".to_string(),
            shard_ids: vec![ShardId(0)],
            replication: ReplicationConfig::default(),
            max_memory: 1000,
        })
        .unwrap();

        assert!(node.allocate_memory(2000).is_err());
    }

    #[test]
    fn test_node_invalid_config() {
        let config = NodeConfig {
            node_id: String::new(),
            shard_ids: vec![ShardId(0)],
            replication: ReplicationConfig::default(),
            max_memory: 1_000_000,
        };

        assert!(DistributedNode::new(config).is_err());
    }
}
