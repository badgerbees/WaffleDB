/// Consistent hashing for shard assignment
///
/// Replaces simple modulo hashing to support dynamic node addition/removal.
/// Uses jump hash algorithm for minimal rebalancing.

use std::collections::BTreeMap;
use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;

/// Virtual node count per physical node (for better distribution)
const VIRTUAL_NODES: usize = 160;

/// Consistent hash ring for shard assignment
#[derive(Debug, Clone)]
pub struct ConsistentHashRing {
    /// Map of hash value -> shard ID
    ring: BTreeMap<u64, usize>,
    /// Number of physical shards
    num_shards: usize,
}

impl ConsistentHashRing {
    /// Create a new consistent hash ring with N shards
    pub fn new(num_shards: usize) -> Self {
        if num_shards == 0 {
            panic!("num_shards must be > 0");
        }

        let mut ring = BTreeMap::new();

        // Add virtual nodes for each shard
        for shard_id in 0..num_shards {
            for vnode in 0..VIRTUAL_NODES {
                let key = format!("shard-{}-vnode-{}", shard_id, vnode);
                let hash = Self::hash_key(&key);
                ring.insert(hash, shard_id);
            }
        }

        ConsistentHashRing { ring, num_shards }
    }

    /// Get the shard ID for a given key
    pub fn get_shard(&self, key: &str) -> usize {
        let hash = Self::hash_key(key);
        
        // Find the first shard >= hash (ring wrap-around)
        if let Some((_, shard_id)) = self.ring.range(hash..).next() {
            *shard_id
        } else if let Some((_, shard_id)) = self.ring.iter().next() {
            *shard_id
        } else {
            0 // Fallback (shouldn't happen)
        }
    }

    /// Hash a key to u64 using DefaultHasher
    fn hash_key(key: &str) -> u64 {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        hasher.finish()
    }

    /// Number of shards in this ring
    pub fn num_shards(&self) -> usize {
        self.num_shards
    }
}

/// Jump hash algorithm for simpler, faster hashing
/// (alternative to consistent hashing, used when nodes are static)
pub struct JumpHash;

impl JumpHash {
    /// Assign key to bucket using jump hash algorithm
    /// 
    /// Properties:
    /// - Fast O(log N) time
    /// - Minimal remapping when bucket count changes
    /// - Good distribution
    pub fn hash(key: &str, num_buckets: u64) -> u64 {
        if num_buckets == 0 {
            return 0;
        }

        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        let mut h = hasher.finish();

        let mut bucket: i64 = -1;
        let mut range = num_buckets as i64;

        while range > 0 {
            // Jump to next bucket
            bucket += (((h ^ (h >> 33).wrapping_mul(0xff51afd7ed558ccd)) >> 33) as i64) + 1;
            h = h.wrapping_mul(0x85ebca6b);

            if bucket < range {
                break;
            }

            range = ((range as u64 * 2654435761 % 2u64.pow(32)) as i64);
        }

        bucket.max(0) as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_consistent_hash_ring() {
        let ring = ConsistentHashRing::new(4);

        // Same key should always map to same shard
        let shard1 = ring.get_shard("doc123");
        let shard2 = ring.get_shard("doc123");
        assert_eq!(shard1, shard2);

        // Different keys may map to different shards
        let shard_a = ring.get_shard("doc_a");
        let shard_b = ring.get_shard("doc_b");
        // Can map to same or different shard, just verify it's in range
        assert!(shard_a < 4);
        assert!(shard_b < 4);
    }

    #[test]
    fn test_consistent_hash_distribution() {
        let ring = ConsistentHashRing::new(4);

        // Count distribution across many keys
        let mut counts = vec![0; 4];
        for i in 0..1000 {
            let key = format!("key_{}", i);
            let shard = ring.get_shard(&key);
            counts[shard] += 1;
        }

        // Each shard should get roughly 250 keys (1000 / 4)
        // Allow ±20% variance
        for count in counts {
            assert!(count > 200 && count < 300, "Unbalanced: {}", count);
        }
    }

    #[test]
    fn test_jump_hash() {
        // Same key -> same bucket
        assert_eq!(JumpHash::hash("doc123", 4), JumpHash::hash("doc123", 4));

        // Within bounds
        assert!(JumpHash::hash("doc_a", 4) < 4);
        assert!(JumpHash::hash("doc_b", 10) < 10);
    }

    #[test]
    fn test_jump_hash_distribution() {
        let num_buckets = 10;
        let mut counts = vec![0; num_buckets as usize];

        for i in 0..1000 {
            let key = format!("key_{}", i);
            let bucket = JumpHash::hash(&key, num_buckets) as usize;
            counts[bucket] += 1;
        }

        // Each bucket should get roughly 100 keys
        for count in counts {
            assert!(count > 80 && count < 120, "Unbalanced: {}", count);
        }
    }
}
