/// Query routing across shards
/// 
/// Routes queries to appropriate shards, merges results, and handles failover.

use std::collections::HashMap;
use crate::core::errors::{Result, WaffleError, ErrorCode};
use crate::distributed::sharding::{ShardId, ShardKey, ShardingStrategy};

/// A query routed to one or more shards
#[derive(Debug, Clone)]
pub struct RoutedQuery {
    pub shard_ids: Vec<ShardId>,
    pub query_type: QueryType,
    pub consistency_level: ConsistencyLevel,
}

/// Types of queries
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryType {
    /// Point lookup by doc_id
    PointGet,
    /// Range query across all shards
    RangeQuery,
    /// Search by vector
    VectorSearch,
    /// Metadata filter scan
    MetadataFilter,
}

/// Consistency levels for queries
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConsistencyLevel {
    /// Read from any replica (fastest, may be stale)
    Eventual,
    /// Read from primary only (strongly consistent)
    Strong,
    /// Read from quorum of replicas (eventually consistent with bounds)
    Quorum,
}

/// Result from a single shard
#[derive(Debug, Clone)]
pub struct ShardResult {
    pub shard_id: ShardId,
    pub doc_ids: Vec<String>,
    pub scores: Vec<f32>,
}

/// Merged result from all shards
#[derive(Debug, Clone)]
pub struct MergedResult {
    pub doc_ids: Vec<String>,
    pub scores: Vec<f32>,
    pub shard_sources: HashMap<String, ShardId>,
}

impl MergedResult {
    /// Create empty result
    pub fn empty() -> Self {
        Self {
            doc_ids: Vec::new(),
            scores: Vec::new(),
            shard_sources: HashMap::new(),
        }
    }

    /// Merge multiple shard results with deduplication and sorting
    pub fn merge(results: Vec<ShardResult>, limit: usize) -> Self {
        let mut merged: HashMap<String, (f32, ShardId)> = HashMap::new();

        // Merge results with deduplication (keep best score)
        for result in results {
            for (doc_id, score) in result.doc_ids.iter().zip(result.scores.iter()) {
                merged
                    .entry(doc_id.clone())
                    .and_modify(|(existing_score, _)| {
                        if score > existing_score {
                            *existing_score = *score;
                        }
                    })
                    .or_insert((*score, result.shard_id));
            }
        }

        // Sort by score descending
        let mut sorted: Vec<_> = merged.into_iter().collect();
        sorted.sort_by(|a, b| b.1 .0.partial_cmp(&a.1 .0).unwrap());

        // Truncate to limit
        sorted.truncate(limit);

        let mut doc_ids = Vec::new();
        let mut scores = Vec::new();
        let mut shard_sources = HashMap::new();

        for (doc_id, (score, shard_id)) in sorted {
            doc_ids.push(doc_id.clone());
            scores.push(score);
            shard_sources.insert(doc_id, shard_id);
        }

        Self {
            doc_ids,
            scores,
            shard_sources,
        }
    }
}

/// Query router for distributed queries
pub struct QueryRouter {
    sharding: Box<dyn ShardingStrategy>,
}

impl QueryRouter {
    /// Create new query router with sharding strategy
    pub fn new(sharding: Box<dyn ShardingStrategy>) -> Self {
        Self { sharding }
    }

    /// Route a point lookup query
    pub fn route_point_get(&self, doc_id: &str, consistency: ConsistencyLevel) -> Result<RoutedQuery> {
        let key = ShardKey::new(doc_id.to_string());
        let shard_id = self.sharding.get_shard(&key);

        let mut shard_ids = vec![shard_id];

        // For quorum or strong consistency, include replicas
        match consistency {
            ConsistencyLevel::Strong => {
                shard_ids.extend(self.sharding.get_replicas(shard_id));
            }
            ConsistencyLevel::Quorum => {
                let replicas = self.sharding.get_replicas(shard_id);
                let quorum_size = (replicas.len() / 2) + 1;
                shard_ids.extend(replicas.iter().take(quorum_size).copied());
            }
            ConsistencyLevel::Eventual => {}
        }

        Ok(RoutedQuery {
            shard_ids,
            query_type: QueryType::PointGet,
            consistency_level: consistency,
        })
    }

    /// Route a range query (goes to all shards)
    pub fn route_range_query(&self, consistency: ConsistencyLevel) -> Result<RoutedQuery> {
        Ok(RoutedQuery {
            shard_ids: self.sharding.all_shards(),
            query_type: QueryType::RangeQuery,
            consistency_level: consistency,
        })
    }

    /// Route a vector search query (goes to all shards, merges results)
    pub fn route_vector_search(&self, consistency: ConsistencyLevel) -> Result<RoutedQuery> {
        Ok(RoutedQuery {
            shard_ids: self.sharding.all_shards(),
            query_type: QueryType::VectorSearch,
            consistency_level: consistency,
        })
    }

    /// Route a metadata filter query (goes to all shards)
    pub fn route_metadata_filter(&self, consistency: ConsistencyLevel) -> Result<RoutedQuery> {
        Ok(RoutedQuery {
            shard_ids: self.sharding.all_shards(),
            query_type: QueryType::MetadataFilter,
            consistency_level: consistency,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct MockSharding {
        num_shards: u32,
    }

    impl ShardingStrategy for MockSharding {
        fn get_shard(&self, key: &ShardKey) -> ShardId {
            ShardId(((key.hash as u32) % self.num_shards) as u32)
        }

        fn all_shards(&self) -> Vec<ShardId> {
            (0..self.num_shards).map(ShardId).collect()
        }

        fn get_replicas(&self, shard_id: ShardId) -> Vec<ShardId> {
            vec![
                ShardId((shard_id.0 + 1) % self.num_shards),
                ShardId((shard_id.0 + 2) % self.num_shards),
            ]
        }
    }

    #[test]
    fn test_query_router_point_get() {
        let sharding = Box::new(MockSharding { num_shards: 16 });
        let router = QueryRouter::new(sharding);

        let routed = router.route_point_get("doc1", ConsistencyLevel::Eventual).unwrap();
        assert_eq!(routed.shard_ids.len(), 1);
        assert_eq!(routed.query_type, QueryType::PointGet);
    }

    #[test]
    fn test_query_router_range_query() {
        let sharding = Box::new(MockSharding { num_shards: 16 });
        let router = QueryRouter::new(sharding);

        let routed = router.route_range_query(ConsistencyLevel::Eventual).unwrap();
        assert_eq!(routed.shard_ids.len(), 16);
        assert_eq!(routed.query_type, QueryType::RangeQuery);
    }

    #[test]
    fn test_merged_result_deduplication() {
        let result1 = ShardResult {
            shard_id: ShardId(0),
            doc_ids: vec!["doc1".to_string(), "doc2".to_string()],
            scores: vec![0.9, 0.8],
        };

        let result2 = ShardResult {
            shard_id: ShardId(1),
            doc_ids: vec!["doc1".to_string(), "doc3".to_string()],
            scores: vec![0.7, 0.95],
        };

        let merged = MergedResult::merge(vec![result1, result2], 10);

        // Should deduplicate doc1 and keep highest score (0.9)
        assert!(merged.doc_ids.contains(&"doc1".to_string()));
        assert_eq!(merged.shard_sources.get("doc1").unwrap(), &ShardId(0));
    }

    #[test]
    fn test_merged_result_sorted() {
        let result1 = ShardResult {
            shard_id: ShardId(0),
            doc_ids: vec!["doc1".to_string(), "doc2".to_string()],
            scores: vec![0.5, 0.7],
        };

        let result2 = ShardResult {
            shard_id: ShardId(1),
            doc_ids: vec!["doc3".to_string()],
            scores: vec![0.9],
        };

        let merged = MergedResult::merge(vec![result1, result2], 10);

        // Should be sorted: doc3 (0.9), doc2 (0.7), doc1 (0.5)
        assert_eq!(merged.doc_ids[0], "doc3");
        assert_eq!(merged.doc_ids[1], "doc2");
        assert_eq!(merged.doc_ids[2], "doc1");
    }

    #[test]
    fn test_merged_result_limit() {
        let result = ShardResult {
            shard_id: ShardId(0),
            doc_ids: vec![
                "doc1".to_string(),
                "doc2".to_string(),
                "doc3".to_string(),
                "doc4".to_string(),
                "doc5".to_string(),
            ],
            scores: vec![0.9, 0.8, 0.7, 0.6, 0.5],
        };

        let merged = MergedResult::merge(vec![result], 3);
        assert_eq!(merged.doc_ids.len(), 3);
    }
}
