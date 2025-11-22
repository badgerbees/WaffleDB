/// Sharding strategy for distributing vectors across nodes.
pub enum ShardingStrategy {
    Range,
    Hash,
    Consistent,
}

impl ShardingStrategy {
    /// Get shard ID for a given key.
    pub fn get_shard(&self, key: &str, num_shards: usize) -> usize {
        match self {
            ShardingStrategy::Range => {
                // Simple range-based sharding
                key.len() % num_shards
            }
            ShardingStrategy::Hash => {
                // Simple hash-based sharding
                let hash = key
                    .chars()
                    .fold(0u32, |acc, c| acc.wrapping_mul(31).wrapping_add(c as u32));
                (hash as usize) % num_shards
            }
            ShardingStrategy::Consistent => {
                // Consistent hashing
                key.len() % num_shards
            }
        }
    }
}
