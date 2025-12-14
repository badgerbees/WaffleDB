/// Scatter-Gather Distributed Search
///
/// Implements parallel fan-out search across shards with:
/// - Parallel query execution on multiple shards
/// - Result merging with consistent scoring
/// - Replica fallback on shard failure
/// - Timeout handling for slow shards

use std::collections::HashMap;
use std::time::{Duration, Instant};
use serde::{Serialize, Deserialize};
use crate::core::errors::{Result, WaffleError, ErrorCode};
use crate::distributed::query_router::{ShardResult, MergedResult, QueryType, ConsistencyLevel};
use crate::distributed::sharding::ShardId;

/// Configuration for scatter-gather execution
#[derive(Debug, Clone)]
pub struct ScatterGatherConfig {
    /// Timeout per shard in milliseconds
    pub shard_timeout_ms: u64,
    /// Maximum number of parallel requests
    pub max_parallel: usize,
    /// Enable replica fallback on failure
    pub enable_fallback: bool,
    /// Minimum replicas to succeed (for quorum consistency)
    pub min_replicas_for_quorum: usize,
}

impl Default for ScatterGatherConfig {
    fn default() -> Self {
        Self {
            shard_timeout_ms: 5000,
            max_parallel: 32,
            enable_fallback: true,
            min_replicas_for_quorum: 2,
        }
    }
}

/// Result from a scatter-gather operation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScatterGatherResult {
    /// Merged result from all shards
    pub merged: MergedResult,
    /// Number of shards contacted
    pub shards_contacted: usize,
    /// Number of shards that responded successfully
    pub shards_succeeded: usize,
    /// Number of shards that timed out or failed
    pub shards_failed: usize,
    /// Time taken in milliseconds
    pub duration_ms: u64,
    /// Per-shard status
    pub shard_status: HashMap<ShardId, ShardStatus>,
}

/// Status of a single shard in scatter-gather
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum ShardStatus {
    /// Shard responded successfully
    Success,
    /// Shard timed out
    Timeout,
    /// Shard returned an error
    Error,
    /// Replica was used due to primary failure
    ReplicaUsed,
}

/// Request to execute scatter-gather search
#[derive(Debug, Clone)]
pub struct ScatterGatherRequest {
    /// Shard IDs to query
    pub shard_ids: Vec<ShardId>,
    /// Replica shards for fallback
    pub replica_shards: HashMap<ShardId, Vec<ShardId>>,
    /// Query type being executed
    pub query_type: QueryType,
    /// Consistency level required
    pub consistency_level: ConsistencyLevel,
    /// Result limit (top-k)
    pub limit: usize,
}

/// Executor for scatter-gather queries
pub struct ScatterGatherExecutor {
    config: ScatterGatherConfig,
}

impl ScatterGatherExecutor {
    /// Create new scatter-gather executor
    pub fn new(config: ScatterGatherConfig) -> Self {
        Self { config }
    }

    /// Execute scatter-gather search across shards
    /// 
    /// This simulates parallel execution across shards with:
    /// - Timeout handling per shard
    /// - Replica fallback on failure
    /// - Result merging and deduplication
    /// 
    /// In production, this would:
    /// 1. Send parallel RPC requests to all shards
    /// 2. Wait for responses with timeout
    /// 3. Fall back to replicas on failure
    /// 4. Merge results from all successful shards
    pub fn execute(
        &self,
        request: ScatterGatherRequest,
        shard_results: HashMap<ShardId, Option<ShardResult>>,
    ) -> Result<ScatterGatherResult> {
        let start = Instant::now();
        let mut merged_results = Vec::new();
        let mut shard_status = HashMap::new();
        let mut shards_succeeded = 0;
        let mut shards_failed = 0;

        // Process primary shards
        for shard_id in &request.shard_ids {
            match shard_results.get(shard_id) {
                Some(Some(result)) => {
                    // Shard returned result
                    merged_results.push(result.clone());
                    shard_status.insert(*shard_id, ShardStatus::Success);
                    shards_succeeded += 1;
                }
                Some(None) => {
                    // Shard failed - try replica fallback
                    if self.config.enable_fallback {
                        if let Some(replicas) = request.replica_shards.get(shard_id) {
                            let mut found = false;
                            for replica_id in replicas {
                                if let Some(Some(result)) = shard_results.get(replica_id) {
                                    merged_results.push(result.clone());
                                    shard_status.insert(*shard_id, ShardStatus::ReplicaUsed);
                                    shards_succeeded += 1;
                                    found = true;
                                    break;
                                }
                            }
                            if !found {
                                shard_status.insert(*shard_id, ShardStatus::Error);
                                shards_failed += 1;
                            }
                        } else {
                            shard_status.insert(*shard_id, ShardStatus::Error);
                            shards_failed += 1;
                        }
                    } else {
                        shard_status.insert(*shard_id, ShardStatus::Timeout);
                        shards_failed += 1;
                    }
                }
                None => {
                    // Shard not in results (timeout)
                    shard_status.insert(*shard_id, ShardStatus::Timeout);
                    shards_failed += 1;
                }
            }
        }

        // Check consistency requirements
        if request.consistency_level == ConsistencyLevel::Strong && shards_failed > 0 {
            return Err(WaffleError::WithCode {
                code: ErrorCode::DistributedModeError,
                message: format!(
                    "Strong consistency required but {} shards failed",
                    shards_failed
                ),
            });
        }

        if request.consistency_level == ConsistencyLevel::Quorum {
            let successful_replicas = shard_status
                .values()
                .filter(|s| **s == ShardStatus::Success || **s == ShardStatus::ReplicaUsed)
                .count();

            if successful_replicas < self.config.min_replicas_for_quorum {
                return Err(WaffleError::WithCode {
                    code: ErrorCode::DistributedModeError,
                    message: format!(
                        "Quorum consistency required but only {} replicas succeeded",
                        successful_replicas
                    ),
                });
            }
        }

        // Merge results from all successful shards
        let merged = MergedResult::merge(merged_results, request.limit);

        let duration_ms = start.elapsed().as_millis() as u64;

        Ok(ScatterGatherResult {
            merged,
            shards_contacted: request.shard_ids.len(),
            shards_succeeded,
            shards_failed,
            duration_ms,
            shard_status,
        })
    }

    /// Get timeout duration
    pub fn get_timeout(&self) -> Duration {
        Duration::from_millis(self.config.shard_timeout_ms)
    }

    /// Check if request meets consistency requirements given failures
    pub fn check_consistency(
        &self,
        consistency_level: ConsistencyLevel,
        shards_contacted: usize,
        shards_succeeded: usize,
    ) -> bool {
        match consistency_level {
            ConsistencyLevel::Eventual => true, // Always OK
            ConsistencyLevel::Strong => shards_succeeded == shards_contacted, // All must succeed
            ConsistencyLevel::Quorum => shards_succeeded >= self.config.min_replicas_for_quorum,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_scatter_gather_all_success() {
        let config = ScatterGatherConfig::default();
        let executor = ScatterGatherExecutor::new(config);

        let request = ScatterGatherRequest {
            shard_ids: vec![ShardId(0), ShardId(1), ShardId(2)],
            replica_shards: HashMap::new(),
            query_type: QueryType::VectorSearch,
            consistency_level: ConsistencyLevel::Eventual,
            limit: 10,
        };

        let mut shard_results = HashMap::new();
        shard_results.insert(
            ShardId(0),
            Some(ShardResult {
                shard_id: ShardId(0),
                doc_ids: vec!["doc0".to_string()],
                scores: vec![0.9],
            }),
        );
        shard_results.insert(
            ShardId(1),
            Some(ShardResult {
                shard_id: ShardId(1),
                doc_ids: vec!["doc1".to_string()],
                scores: vec![0.8],
            }),
        );
        shard_results.insert(
            ShardId(2),
            Some(ShardResult {
                shard_id: ShardId(2),
                doc_ids: vec!["doc2".to_string()],
                scores: vec![0.7],
            }),
        );

        let result = executor.execute(request, shard_results).unwrap();

        assert_eq!(result.shards_contacted, 3);
        assert_eq!(result.shards_succeeded, 3);
        assert_eq!(result.shards_failed, 0);
        assert_eq!(result.merged.doc_ids.len(), 3);
        // Should be sorted: doc0 (0.9), doc1 (0.8), doc2 (0.7)
        assert_eq!(result.merged.doc_ids[0], "doc0");
    }

    #[test]
    fn test_scatter_gather_with_timeout() {
        let mut config = ScatterGatherConfig::default();
        config.enable_fallback = false; // Disable fallback to see timeout status
        let executor = ScatterGatherExecutor::new(config);

        let request = ScatterGatherRequest {
            shard_ids: vec![ShardId(0), ShardId(1), ShardId(2)],
            replica_shards: HashMap::new(),
            query_type: QueryType::VectorSearch,
            consistency_level: ConsistencyLevel::Eventual,
            limit: 10,
        };

        let mut shard_results = HashMap::new();
        shard_results.insert(
            ShardId(0),
            Some(ShardResult {
                shard_id: ShardId(0),
                doc_ids: vec!["doc0".to_string()],
                scores: vec![0.9],
            }),
        );
        // ShardId(1) times out (None)
        shard_results.insert(ShardId(1), None);
        shard_results.insert(
            ShardId(2),
            Some(ShardResult {
                shard_id: ShardId(2),
                doc_ids: vec!["doc2".to_string()],
                scores: vec![0.7],
            }),
        );

        let result = executor.execute(request, shard_results).unwrap();

        assert_eq!(result.shards_contacted, 3);
        assert_eq!(result.shards_succeeded, 2);
        assert_eq!(result.shards_failed, 1);
        assert_eq!(result.shard_status[&ShardId(1)], ShardStatus::Timeout);
    }

    #[test]
    fn test_scatter_gather_replica_fallback() {
        let config = ScatterGatherConfig::default();
        let executor = ScatterGatherExecutor::new(config);

        let mut replica_shards = HashMap::new();
        replica_shards.insert(ShardId(0), vec![ShardId(3), ShardId(4)]);

        let request = ScatterGatherRequest {
            shard_ids: vec![ShardId(0), ShardId(1)],
            replica_shards,
            query_type: QueryType::VectorSearch,
            consistency_level: ConsistencyLevel::Eventual,
            limit: 10,
        };

        let mut shard_results = HashMap::new();
        // Primary shard 0 fails
        shard_results.insert(ShardId(0), None);
        // But replica 3 succeeds
        shard_results.insert(
            ShardId(3),
            Some(ShardResult {
                shard_id: ShardId(3),
                doc_ids: vec!["doc0".to_string()],
                scores: vec![0.9],
            }),
        );
        shard_results.insert(
            ShardId(1),
            Some(ShardResult {
                shard_id: ShardId(1),
                doc_ids: vec!["doc1".to_string()],
                scores: vec![0.8],
            }),
        );

        let result = executor.execute(request, shard_results).unwrap();

        assert_eq!(result.shards_succeeded, 2);
        assert_eq!(result.shard_status[&ShardId(0)], ShardStatus::ReplicaUsed);
        assert_eq!(result.merged.doc_ids.len(), 2);
    }

    #[test]
    fn test_scatter_gather_strong_consistency_failure() {
        let config = ScatterGatherConfig::default();
        let executor = ScatterGatherExecutor::new(config);

        let request = ScatterGatherRequest {
            shard_ids: vec![ShardId(0), ShardId(1)],
            replica_shards: HashMap::new(),
            query_type: QueryType::VectorSearch,
            consistency_level: ConsistencyLevel::Strong,
            limit: 10,
        };

        let mut shard_results = HashMap::new();
        shard_results.insert(
            ShardId(0),
            Some(ShardResult {
                shard_id: ShardId(0),
                doc_ids: vec!["doc0".to_string()],
                scores: vec![0.9],
            }),
        );
        // Shard 1 failed
        shard_results.insert(ShardId(1), None);

        let result = executor.execute(request, shard_results);
        assert!(result.is_err());
    }

    #[test]
    fn test_scatter_gather_quorum_consistency() {
        let mut config = ScatterGatherConfig::default();
        config.min_replicas_for_quorum = 2;
        let executor = ScatterGatherExecutor::new(config);

        let request = ScatterGatherRequest {
            shard_ids: vec![ShardId(0), ShardId(1), ShardId(2)],
            replica_shards: HashMap::new(),
            query_type: QueryType::VectorSearch,
            consistency_level: ConsistencyLevel::Quorum,
            limit: 10,
        };

        let mut shard_results = HashMap::new();
        shard_results.insert(
            ShardId(0),
            Some(ShardResult {
                shard_id: ShardId(0),
                doc_ids: vec!["doc0".to_string()],
                scores: vec![0.9],
            }),
        );
        shard_results.insert(
            ShardId(1),
            Some(ShardResult {
                shard_id: ShardId(1),
                doc_ids: vec!["doc1".to_string()],
                scores: vec![0.8],
            }),
        );
        // Shard 2 failed
        shard_results.insert(ShardId(2), None);

        let result = executor.execute(request, shard_results).unwrap();
        assert_eq!(result.shards_succeeded, 2);
    }

    #[test]
    fn test_scatter_gather_result_deduplication() {
        let config = ScatterGatherConfig::default();
        let executor = ScatterGatherExecutor::new(config);

        let request = ScatterGatherRequest {
            shard_ids: vec![ShardId(0), ShardId(1)],
            replica_shards: HashMap::new(),
            query_type: QueryType::VectorSearch,
            consistency_level: ConsistencyLevel::Eventual,
            limit: 10,
        };

        let mut shard_results = HashMap::new();
        shard_results.insert(
            ShardId(0),
            Some(ShardResult {
                shard_id: ShardId(0),
                doc_ids: vec!["doc1".to_string(), "doc2".to_string()],
                scores: vec![0.9, 0.7],
            }),
        );
        shard_results.insert(
            ShardId(1),
            Some(ShardResult {
                shard_id: ShardId(1),
                doc_ids: vec!["doc1".to_string(), "doc3".to_string()],
                scores: vec![0.8, 0.6],
            }),
        );

        let result = executor.execute(request, shard_results).unwrap();

        // doc1 should be deduplicated with highest score (0.9)
        assert!(result.merged.doc_ids.contains(&"doc1".to_string()));
        let doc1_score = result
            .merged
            .scores
            .iter()
            .zip(&result.merged.doc_ids)
            .find(|(_, id)| *id == "doc1")
            .map(|(score, _)| *score);
        assert_eq!(doc1_score, Some(0.9));
    }

    #[test]
    fn test_scatter_gather_respects_limit() {
        let config = ScatterGatherConfig::default();
        let executor = ScatterGatherExecutor::new(config);

        let request = ScatterGatherRequest {
            shard_ids: vec![ShardId(0), ShardId(1)],
            replica_shards: HashMap::new(),
            query_type: QueryType::VectorSearch,
            consistency_level: ConsistencyLevel::Eventual,
            limit: 2,
        };

        let mut shard_results = HashMap::new();
        shard_results.insert(
            ShardId(0),
            Some(ShardResult {
                shard_id: ShardId(0),
                doc_ids: vec!["doc1".to_string(), "doc2".to_string()],
                scores: vec![0.9, 0.7],
            }),
        );
        shard_results.insert(
            ShardId(1),
            Some(ShardResult {
                shard_id: ShardId(1),
                doc_ids: vec!["doc3".to_string()],
                scores: vec![0.8],
            }),
        );

        let result = executor.execute(request, shard_results).unwrap();

        assert_eq!(result.merged.doc_ids.len(), 2);
        // Should keep top 2: doc1 (0.9) and doc3 (0.8)
        assert_eq!(result.merged.doc_ids[0], "doc1");
        assert_eq!(result.merged.doc_ids[1], "doc3");
    }

    #[test]
    fn test_consistency_check_methods() {
        let config = ScatterGatherConfig::default();
        let executor = ScatterGatherExecutor::new(config);

        // Eventual consistency - always passes
        assert!(executor.check_consistency(ConsistencyLevel::Eventual, 3, 2));

        // Strong consistency - requires all
        assert!(executor.check_consistency(ConsistencyLevel::Strong, 3, 3));
        assert!(!executor.check_consistency(ConsistencyLevel::Strong, 3, 2));

        // Quorum - requires min_replicas_for_quorum (default 2)
        assert!(executor.check_consistency(ConsistencyLevel::Quorum, 3, 2));
        assert!(!executor.check_consistency(ConsistencyLevel::Quorum, 3, 1));
    }
}
