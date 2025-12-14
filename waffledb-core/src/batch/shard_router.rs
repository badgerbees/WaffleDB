/// Shard-aware batch processing for parallel multi-shard writes.
///
/// This module enables:
/// - Batch request routing to appropriate shards
/// - Parallel write execution across shards
/// - Per-shard consolidation and buffering
/// - Sharding strategy abstraction

use std::collections::HashMap;
use std::sync::Arc;

/// Sharding strategy for distributing vectors across shards.
pub trait ShardingStrategy: Send + Sync {
    /// Determine which shard a vector belongs to.
    fn get_shard(&self, vector_id: &str, num_shards: usize) -> usize;
    
    /// Determine shard for a collection.
    fn get_shard_for_collection(&self, collection_id: &str, num_shards: usize) -> usize;
}

/// Hash-based sharding strategy.
#[derive(Debug, Clone)]
pub struct HashShardingStrategy;

impl ShardingStrategy for HashShardingStrategy {
    fn get_shard(&self, vector_id: &str, num_shards: usize) -> usize {
        if num_shards == 0 {
            return 0;
        }
        
        // Simple hash function
        let mut hash = 5381u64;
        for byte in vector_id.as_bytes() {
            hash = hash.wrapping_mul(33).wrapping_add(*byte as u64);
        }
        
        (hash as usize) % num_shards
    }

    fn get_shard_for_collection(&self, collection_id: &str, num_shards: usize) -> usize {
        self.get_shard(collection_id, num_shards)
    }
}

/// Range-based sharding strategy.
#[derive(Debug, Clone)]
pub struct RangeShardingStrategy {
    shard_ranges: Vec<(String, String)>, // (start, end) for each shard
}

impl RangeShardingStrategy {
    pub fn new(shard_ranges: Vec<(String, String)>) -> Self {
        Self { shard_ranges }
    }

    pub fn from_shard_count(num_shards: usize) -> Self {
        let mut ranges = Vec::new();
        for i in 0..num_shards {
            let start = format!("{:032x}", (i as u128) << 120);
            let end = format!("{:032x}", ((i + 1) as u128) << 120);
            ranges.push((start, end));
        }
        Self { shard_ranges: ranges }
    }
}

impl ShardingStrategy for RangeShardingStrategy {
    fn get_shard(&self, vector_id: &str, num_shards: usize) -> usize {
        for (shard_idx, (start, end)) in self.shard_ranges.iter().enumerate() {
            if vector_id >= start.as_str() && vector_id <= end.as_str() {
                return shard_idx;
            }
        }
        num_shards.saturating_sub(1)
    }

    fn get_shard_for_collection(&self, _collection_id: &str, _num_shards: usize) -> usize {
        0
    }
}

/// Shard-aware batch routing result.
#[derive(Debug, Clone)]
pub struct ShardRoute {
    pub shard_id: usize,
    pub vector_ids: Vec<String>,
    pub indices: Vec<usize>, // Original indices in batch
}

/// Router for distributing batch requests across shards.
pub struct BatchShardRouter {
    num_shards: usize,
    strategy: Arc<dyn ShardingStrategy>,
}

impl BatchShardRouter {
    pub fn new(num_shards: usize, strategy: Arc<dyn ShardingStrategy>) -> Self {
        Self { num_shards, strategy }
    }

    /// Route batch insert requests to appropriate shards.
    pub fn route_inserts(&self, collection_id: &str, vector_ids: &[String]) -> Vec<ShardRoute> {
        let mut shard_map: HashMap<usize, (Vec<String>, Vec<usize>)> = HashMap::new();

        for (idx, vector_id) in vector_ids.iter().enumerate() {
            let shard = self.strategy.get_shard(vector_id, self.num_shards);
            let entry = shard_map.entry(shard).or_insert((Vec::new(), Vec::new()));
            entry.0.push(vector_id.clone());
            entry.1.push(idx);
        }

        shard_map
            .into_iter()
            .map(|(shard_id, (vector_ids, indices))| ShardRoute {
                shard_id,
                vector_ids,
                indices,
            })
            .collect()
    }

    /// Route batch delete requests to appropriate shards.
    pub fn route_deletes(&self, collection_id: &str, ids: &[String]) -> Vec<ShardRoute> {
        self.route_inserts(collection_id, ids)
    }

    /// Get number of shards.
    pub fn num_shards(&self) -> usize {
        self.num_shards
    }

    /// Get reference to sharding strategy.
    pub fn strategy(&self) -> &dyn ShardingStrategy {
        &*self.strategy
    }
}

/// Per-shard batch processor.
pub struct ShardBatchProcessor {
    shard_id: usize,
    batch_size: usize,
    pending_operations: Arc<std::sync::Mutex<Vec<String>>>,
}

impl ShardBatchProcessor {
    pub fn new(shard_id: usize, batch_size: usize) -> Self {
        Self {
            shard_id,
            batch_size,
            pending_operations: Arc::new(std::sync::Mutex::new(Vec::new())),
        }
    }

    /// Add vector ID to pending operations.
    pub fn add_operation(&self, vector_id: String) -> bool {
        let mut pending = self.pending_operations.lock().unwrap();
        pending.push(vector_id);
        pending.len() >= self.batch_size
    }

    /// Get and clear pending operations.
    pub fn get_pending(&self) -> Vec<String> {
        let mut pending = self.pending_operations.lock().unwrap();
        let ops = pending.clone();
        pending.clear();
        ops
    }

    /// Get pending count.
    pub fn pending_count(&self) -> usize {
        self.pending_operations.lock().unwrap().len()
    }

    /// Get shard ID.
    pub fn shard_id(&self) -> usize {
        self.shard_id
    }
}

/// Parallel shard write coordinator.
pub struct ParallelShardWriter {
    processors: HashMap<usize, ShardBatchProcessor>,
    batch_size: usize,
}

impl ParallelShardWriter {
    pub fn new(num_shards: usize, batch_size: usize) -> Self {
        let mut processors = HashMap::new();
        for shard_id in 0..num_shards {
            processors.insert(shard_id, ShardBatchProcessor::new(shard_id, batch_size));
        }

        Self {
            processors,
            batch_size,
        }
    }

    /// Execute writes in parallel across shards.
    pub fn execute_parallel_writes(
        &self,
        routes: &[ShardRoute],
    ) -> HashMap<usize, Vec<String>> {
        let mut results = HashMap::new();

        for route in routes {
            if let Some(processor) = self.processors.get(&route.shard_id) {
                // Simulate parallel write by collecting operations
                for vector_id in &route.vector_ids {
                    processor.add_operation(vector_id.clone());
                }

                // Get pending operations for this shard
                let pending = processor.get_pending();
                if !pending.is_empty() {
                    results.insert(route.shard_id, pending);
                }
            }
        }

        results
    }

    /// Get processor for shard.
    pub fn get_processor(&self, shard_id: usize) -> Option<&ShardBatchProcessor> {
        self.processors.get(&shard_id)
    }

    /// Get all processors.
    pub fn processors(&self) -> &HashMap<usize, ShardBatchProcessor> {
        &self.processors
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hash_sharding_strategy() {
        let strategy = HashShardingStrategy;
        
        let shard1 = strategy.get_shard("id1", 10);
        let shard2 = strategy.get_shard("id2", 10);
        
        assert!(shard1 < 10);
        assert!(shard2 < 10);
    }

    #[test]
    fn test_hash_sharding_consistency() {
        let strategy = HashShardingStrategy;
        
        let shard1 = strategy.get_shard("same_id", 10);
        let shard2 = strategy.get_shard("same_id", 10);
        
        assert_eq!(shard1, shard2);
    }

    #[test]
    fn test_hash_sharding_distribution() {
        let strategy = HashShardingStrategy;
        let num_shards = 10;
        let mut shard_counts = vec![0; num_shards];

        for i in 0..1000 {
            let shard = strategy.get_shard(&format!("id{}", i), num_shards);
            shard_counts[shard] += 1;
        }

        // Check that we get reasonable distribution (not all in one shard)
        let non_zero = shard_counts.iter().filter(|&&c| c > 0).count();
        assert!(non_zero > 1, "Should distribute across multiple shards");
    }

    #[test]
    fn test_range_sharding_strategy() {
        let strategy = RangeShardingStrategy::from_shard_count(4);
        
        let shard1 = strategy.get_shard("00000000000000000000000000000000", 4);
        let shard2 = strategy.get_shard("40000000000000000000000000000000", 4);
        
        assert!(shard1 < 4);
        assert!(shard2 < 4);
    }

    #[test]
    fn test_batch_shard_router_creation() {
        let strategy = Arc::new(HashShardingStrategy);
        let router = BatchShardRouter::new(10, strategy);
        
        assert_eq!(router.num_shards(), 10);
    }

    #[test]
    fn test_batch_shard_router_route_inserts() {
        let strategy = Arc::new(HashShardingStrategy);
        let router = BatchShardRouter::new(4, strategy);
        
        let ids = vec![
            "id1".to_string(),
            "id2".to_string(),
            "id3".to_string(),
            "id4".to_string(),
        ];

        let routes = router.route_inserts("col1", &ids);
        
        // Should route to at most 4 shards
        assert!(routes.len() <= 4);
        
        // Total vectors should match input
        let total: usize = routes.iter().map(|r| r.vector_ids.len()).sum();
        assert_eq!(total, 4);
    }

    #[test]
    fn test_batch_shard_router_route_deletes() {
        let strategy = Arc::new(HashShardingStrategy);
        let router = BatchShardRouter::new(4, strategy);
        
        let ids = vec![
            "id1".to_string(),
            "id2".to_string(),
            "id3".to_string(),
        ];

        let routes = router.route_deletes("col1", &ids);
        
        assert!(routes.len() <= 4);
        let total: usize = routes.iter().map(|r| r.vector_ids.len()).sum();
        assert_eq!(total, 3);
    }

    #[test]
    fn test_shard_batch_processor_creation() {
        let processor = ShardBatchProcessor::new(0, 100);
        assert_eq!(processor.shard_id(), 0);
        assert_eq!(processor.pending_count(), 0);
    }

    #[test]
    fn test_shard_batch_processor_add_operation() {
        let processor = ShardBatchProcessor::new(0, 10);
        
        for i in 0..5 {
            let should_flush = processor.add_operation(format!("id{}", i));
            assert!(!should_flush);
        }

        assert_eq!(processor.pending_count(), 5);
    }

    #[test]
    fn test_shard_batch_processor_batch_threshold() {
        let processor = ShardBatchProcessor::new(0, 5);
        
        for i in 0..4 {
            let should_flush = processor.add_operation(format!("id{}", i));
            assert!(!should_flush);
        }

        let should_flush = processor.add_operation("id4".to_string());
        assert!(should_flush);
    }

    #[test]
    fn test_shard_batch_processor_get_pending() {
        let processor = ShardBatchProcessor::new(0, 100);
        
        for i in 0..10 {
            processor.add_operation(format!("id{}", i));
        }

        let pending = processor.get_pending();
        assert_eq!(pending.len(), 10);
        assert_eq!(processor.pending_count(), 0);
    }

    #[test]
    fn test_parallel_shard_writer_creation() {
        let writer = ParallelShardWriter::new(4, 100);
        assert_eq!(writer.processors().len(), 4);
    }

    #[test]
    fn test_parallel_shard_writer_execute() {
        let writer = ParallelShardWriter::new(4, 100);
        
        let routes = vec![
            ShardRoute {
                shard_id: 0,
                vector_ids: vec!["id1".to_string(), "id2".to_string()],
                indices: vec![0, 1],
            },
            ShardRoute {
                shard_id: 1,
                vector_ids: vec!["id3".to_string()],
                indices: vec![2],
            },
        ];

        let results = writer.execute_parallel_writes(&routes);
        
        // Should have results for each shard that had operations
        assert!(results.contains_key(&0) || results.contains_key(&1));
    }

    #[test]
    fn test_shard_route_structure() {
        let route = ShardRoute {
            shard_id: 2,
            vector_ids: vec!["id1".to_string(), "id2".to_string()],
            indices: vec![0, 1],
        };

        assert_eq!(route.shard_id, 2);
        assert_eq!(route.vector_ids.len(), 2);
        assert_eq!(route.indices.len(), 2);
    }

    #[test]
    fn test_large_batch_routing() {
        let strategy = Arc::new(HashShardingStrategy);
        let router = BatchShardRouter::new(16, strategy);
        
        let mut ids = Vec::new();
        for i in 0..10000 {
            ids.push(format!("id{}", i));
        }

        let routes = router.route_inserts("col1", &ids);
        
        let total: usize = routes.iter().map(|r| r.vector_ids.len()).sum();
        assert_eq!(total, 10000);
    }

    #[test]
    fn test_hash_sharding_empty_id() {
        let strategy = HashShardingStrategy;
        let shard = strategy.get_shard("", 10);
        assert!(shard < 10);
    }

    #[test]
    fn test_get_processor() {
        let writer = ParallelShardWriter::new(4, 100);
        
        let processor = writer.get_processor(0);
        assert!(processor.is_some());
        assert_eq!(processor.unwrap().shard_id(), 0);
        
        let processor = writer.get_processor(10);
        assert!(processor.is_none());
    }

    #[test]
    fn test_hash_sharding_same_collection() {
        let strategy = HashShardingStrategy;
        
        let shard = strategy.get_shard_for_collection("col1", 10);
        assert!(shard < 10);
    }

    #[test]
    fn test_batch_router_indices_preserved() {
        let strategy = Arc::new(HashShardingStrategy);
        let router = BatchShardRouter::new(4, strategy);
        
        let ids = vec!["id1".to_string(), "id2".to_string(), "id3".to_string()];
        let routes = router.route_inserts("col1", &ids);
        
        // All original indices should be present across routes
        let all_indices: Vec<usize> = routes.iter().flat_map(|r| r.indices.clone()).collect();
        assert_eq!(all_indices.len(), 3);
    }
}
