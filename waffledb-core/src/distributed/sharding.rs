/// Hash-based sharding strategy
/// 
/// Deterministic consistent hashing without coordination overhead.
/// Uses a simple hash ring to assign documents to shards.

use std::collections::HashMap;
use crate::core::errors::{Result, WaffleError, ErrorCode};

/// Unique identifier for a shard
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ShardId(pub u32);

/// Key used for sharding decisions
#[derive(Debug, Clone)]
pub struct ShardKey {
    pub doc_id: String,
    pub hash: u64,
}

impl ShardKey {
    /// Create a new shard key with deterministic hash
    pub fn new(doc_id: String) -> Self {
        let hash = Self::compute_hash(&doc_id);
        Self { doc_id, hash }
    }

    /// Consistent hash function (FNV-1a)
    fn compute_hash(doc_id: &str) -> u64 {
        const FNV_OFFSET_BASIS: u64 = 14695981039346656037;
        const FNV_PRIME: u64 = 1099511628211;

        let mut hash = FNV_OFFSET_BASIS;
        for byte in doc_id.as_bytes() {
            hash ^= *byte as u64;
            hash = hash.wrapping_mul(FNV_PRIME);
        }
        hash
    }
}

/// Sharding strategy trait
pub trait ShardingStrategy: Send + Sync {
    /// Get shard ID for a given key
    fn get_shard(&self, key: &ShardKey) -> ShardId;

    /// Get all shard IDs
    fn all_shards(&self) -> Vec<ShardId>;

    /// Get replica shards for a given shard (for replication)
    fn get_replicas(&self, shard_id: ShardId) -> Vec<ShardId>;
}

/// Hash-based sharding: simple modulo-based distribution
#[derive(Debug, Clone)]
pub struct HashSharding {
    num_shards: u32,
    replication_factor: u32,
}

impl HashSharding {
    /// Create new hash sharding with given shard count
    pub fn new(num_shards: u32, replication_factor: u32) -> Result<Self> {
        if num_shards == 0 {
            return Err(WaffleError::WithCode {
                code: ErrorCode::ValidationFailed,
                message: "num_shards must be > 0".to_string(),
            });
        }
        if replication_factor == 0 || replication_factor > num_shards {
            return Err(WaffleError::WithCode {
                code: ErrorCode::ValidationFailed,
                message: "replication_factor must be > 0 and <= num_shards".to_string(),
            });
        }

        Ok(Self {
            num_shards,
            replication_factor,
        })
    }

    /// Get primary shard for a key
    pub fn get_primary(&self, key: &ShardKey) -> ShardId {
        ShardId(((key.hash as u32) % self.num_shards) as u32)
    }
}

impl ShardingStrategy for HashSharding {
    fn get_shard(&self, key: &ShardKey) -> ShardId {
        self.get_primary(key)
    }

    fn all_shards(&self) -> Vec<ShardId> {
        (0..self.num_shards).map(ShardId).collect()
    }

    fn get_replicas(&self, shard_id: ShardId) -> Vec<ShardId> {
        // Replicate to next (replication_factor - 1) shards in the ring
        (1..self.replication_factor)
            .map(|i| ShardId((shard_id.0 + i) % self.num_shards))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_shard_key_hash_consistency() {
        let key1 = ShardKey::new("doc123".to_string());
        let key2 = ShardKey::new("doc123".to_string());
        assert_eq!(key1.hash, key2.hash);
    }

    #[test]
    fn test_shard_key_hash_different() {
        let key1 = ShardKey::new("doc123".to_string());
        let key2 = ShardKey::new("doc456".to_string());
        assert_ne!(key1.hash, key2.hash);
    }

    #[test]
    fn test_hash_sharding_creates() {
        let sharding = HashSharding::new(16, 3).unwrap();
        assert_eq!(sharding.all_shards().len(), 16);
    }

    #[test]
    fn test_hash_sharding_deterministic() {
        let sharding = HashSharding::new(16, 3).unwrap();
        let key = ShardKey::new("doc123".to_string());
        let shard1 = sharding.get_shard(&key);
        let shard2 = sharding.get_shard(&key);
        assert_eq!(shard1, shard2);
    }

    #[test]
    fn test_hash_sharding_distribution() {
        let sharding = HashSharding::new(16, 3).unwrap();
        let mut distribution = HashMap::new();

        for i in 0..1000 {
            let key = ShardKey::new(format!("doc{}", i));
            let shard = sharding.get_shard(&key);
            *distribution.entry(shard).or_insert(0) += 1;
        }

        // Should distribute roughly evenly
        let avg = 1000 / 16;
        for count in distribution.values() {
            assert!(*count > avg / 2 && *count < avg * 2);
        }
    }

    #[test]
    fn test_hash_sharding_replicas() {
        let sharding = HashSharding::new(16, 3).unwrap();
        let primary = ShardId(5);
        let replicas = sharding.get_replicas(primary);

        assert_eq!(replicas.len(), 2); // replication_factor - 1
        assert_eq!(replicas[0], ShardId(6));
        assert_eq!(replicas[1], ShardId(7));
    }

    #[test]
    fn test_hash_sharding_invalid_config() {
        assert!(HashSharding::new(0, 3).is_err());
        assert!(HashSharding::new(16, 0).is_err());
        assert!(HashSharding::new(16, 20).is_err());
    }
}
