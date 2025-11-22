use super::hashring::HashRing;
use super::strategy::ShardingStrategy;

/// Sharding coordinator for cluster-wide key distribution.
pub struct ShardingCoordinator {
    hash_ring: HashRing,
    strategy: ShardingStrategy,
    num_shards: usize,
}

impl ShardingCoordinator {
    pub fn new(num_shards: usize, strategy: ShardingStrategy) -> Self {
        ShardingCoordinator {
            hash_ring: HashRing::new(3), // 3 replicas
            strategy,
            num_shards,
        }
    }

    /// Add a node to the cluster.
    pub fn add_node(&mut self, node_id: String) {
        self.hash_ring.add_node(node_id);
    }

    /// Remove a node from the cluster.
    pub fn remove_node(&mut self, node_id: &str) {
        self.hash_ring.remove_node(node_id);
    }

    /// Determine which shard a key belongs to.
    pub fn get_shard(&self, key: &str) -> usize {
        self.strategy.get_shard(key, self.num_shards)
    }

    /// Get the responsible node for a key.
    pub fn get_node(&self, key: &str) -> Option<String> {
        self.hash_ring.get_node(key)
    }
}
