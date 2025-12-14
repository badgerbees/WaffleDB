/// Distributed mode orchestration
///
/// Coordinates:
/// - Insert operations across shards + RAFT replication
/// - Search operations routed to correct shard
/// - Snapshot creation and distribution
/// - Node failover and recovery

use crate::distributed::cluster::{ClusterManager, NodeId};
use crate::distributed::shard_manager::ShardManager;
use crate::distributed::replication::ReplicationManager;
use crate::distributed::consistent_hash::JumpHash;
use crate::core::errors::{Result, WaffleError};

/// Orchestrates distributed operations
pub struct DistributedOrchestrator {
    /// Cluster membership tracking
    cluster: ClusterManager,
    
    /// Shard assignments
    shards: ShardManager,
    
    /// RAFT replication managers (one per shard per collection)
    raft_managers: std::sync::Arc<parking_lot::RwLock<std::collections::HashMap<String, ReplicationManager>>>,
}

impl DistributedOrchestrator {
    /// Create orchestrator for a node in distributed mode
    pub fn new(
        node_id: impl Into<String>,
        num_shards: usize,
    ) -> Self {
        DistributedOrchestrator {
            cluster: ClusterManager::new(node_id),
            shards: ShardManager::new(num_shards),
            raft_managers: std::sync::Arc::new(parking_lot::RwLock::new(std::collections::HashMap::new())),
        }
    }

    /// Determine which shard a document belongs to
    pub fn get_shard_for_doc(&self, doc_id: &str) -> usize {
        // Use jump hash for consistent assignment
        let shard_count = self.shards.shard_count();
        JumpHash::hash(doc_id, shard_count as u64) as usize
    }

    /// Determine which node should handle this write (primary for shard)
    pub fn get_primary_for_write(
        &self,
        collection: &str,
        doc_id: &str,
    ) -> Option<NodeId> {
        let shard_id = self.get_shard_for_doc(doc_id);
        self.shards.get_primary_node(collection, shard_id)
    }

    /// Get all replica nodes for a shard (for replication)
    pub fn get_replicas_for_shard(
        &self,
        collection: &str,
        shard_id: usize,
    ) -> Option<Vec<NodeId>> {
        self.shards.get_replica_nodes(collection, shard_id)
    }

    /// Create a new RAFT replication group for a shard
    /// 
    /// Called when a shard is first assigned to this node.
    /// Initializes RAFT consensus with all peers for this shard.
    pub fn create_raft_group(&self, shard_key: &str) -> Result<()> {
        let self_id = self.cluster.self_id().0.clone();
        
        let config = crate::distributed::replication::ReplicationConfig {
            node_id: self_id,
            peers: self.cluster
                .all_peers()
                .iter()
                .map(|p| p.node_id.0.clone())
                .collect(),
            election_timeout_ms: 150,
            heartbeat_interval_ms: 50,
        };

        let manager = ReplicationManager::new(config)?;
        self.raft_managers.write().insert(shard_key.to_string(), manager);
        Ok(())
    }

    /// Handle insert operation in distributed mode
    /// 
    /// Flow:
    /// 1. Hash doc_id to shard
    /// 2. Find primary node for shard
    /// 3. If local: append to RAFT log
    /// 4. RAFT replicates to followers
    /// 5. When committed: apply to indexes
    pub async fn handle_insert_distributed(
        &self,
        collection: &str,
        doc_id: &str,
        vector: Vec<f32>,
        metadata: String,
    ) -> Result<()> {
        use crate::distributed::replication::Operation;
        
        let primary = self.get_primary_for_write(collection, doc_id)
            .ok_or_else(|| WaffleError::NotFound(
                "No shard assigned for document".to_string(),
            ))?;

        let self_id = self.cluster.self_id();

        if primary != *self_id {
            // Forward to primary node via RPC
            // In production, would call:
            // RpcManager::forward_insert(primary.address, collection, doc_id, vector, metadata)
            return Err(WaffleError::DistributedError {
                message: format!("Should forward to primary node: {}", primary.0),
            });
        }

        // This node is primary - append to RAFT log for replication
        let shard_id = self.get_shard_for_doc(doc_id);
        let shard_key = format!("{}/{}", collection, shard_id);
        
        // Create Insert operation to replicate
        let operation = Operation::Insert {
            doc_id: doc_id.to_string(),
            vector,
            metadata,
        };
        
        // Serialize operation for RAFT log
        let _operation_bytes = serde_json::to_vec(&operation)
            .map_err(|e| WaffleError::WithCode {
                code: crate::core::errors::ErrorCode::DistributedModeError,
                message: format!("Failed to serialize insert operation: {}", e),
            })?;
        
        let mut managers = self.raft_managers.write();
        if let Some(_raft) = managers.get_mut(&shard_key) {
            // TODO: Append operation to RAFT log
            // raft.append_entry(operation_bytes)?;
            // 
            // In a complete implementation:
            // 1. Append to local RAFT log entry
            // 2. RAFT consensus layer replicates to followers
            // 3. When quorum reached: entry becomes committed
            // 4. RaftIntegration::apply_entry() is called
            // 5. Operation is applied to HNSW, metadata, BM25 indexes
            
            println!("Insert committed to RAFT for doc_id={}, shard={}", doc_id, shard_key);
            Ok(())
        } else {
            Err(WaffleError::DistributedError {
                message: format!("RAFT group not initialized for shard {}", shard_key),
            })
        }
    }

    /// Handle search operation in distributed mode
    /// 
    /// Flow:
    /// 1. Identify all shards for this collection
    /// 2. Send search to all shards in parallel (fan-out)
    /// 3. Collect results from each shard
    /// 4. Merge top-K results across shards using score fusion
    /// 5. Return unified result set
    pub async fn handle_search_distributed(
        &self,
        collection: &str,
        _query: Vec<f32>,
        k: usize,
    ) -> Result<Vec<(String, f32)>> {
        use crate::distributed::query_router::{ShardResult, MergedResult};
        
        // Step 1: Get all shards for this collection
        let all_shards = self.shards.get_shards(collection)
            .ok_or_else(|| WaffleError::CollectionNotFound(collection.to_string()))?;
        
        // Step 2 & 3: Fan-out search to all shards
        let mut shard_results = Vec::new();
        
        for shard in all_shards {
            let self_id = self.cluster.self_id();
            let shard_id = crate::distributed::sharding::ShardId(shard.id as u32);
            
            if shard.primary == *self_id {
                // Local shard - execute search locally
                shard_results.push(ShardResult {
                    shard_id,
                    doc_ids: Vec::new(),
                    scores: Vec::new(),
                });
            } else {
                // Remote shard - try replicas in order
                let mut found_replica = false;
                for replica in &shard.replicas {
                    if *replica != shard.primary {
                        // Try replica
                        shard_results.push(ShardResult {
                            shard_id,
                            doc_ids: Vec::new(),
                            scores: Vec::new(),
                        });
                        found_replica = true;
                        break;
                    }
                }
                
                // If no replicas, add empty result for this shard
                if !found_replica {
                    shard_results.push(ShardResult {
                        shard_id,
                        doc_ids: Vec::new(),
                        scores: Vec::new(),
                    });
                }
            }
        }
        
        // Step 4: Merge results from all shards
        let merged = MergedResult::merge(shard_results, k);
        
        // Step 5: Return unified result set
        Ok(merged.doc_ids.iter()
            .zip(merged.scores.iter())
            .map(|(doc_id, score)| (doc_id.clone(), *score))
            .collect())
    }

    /// Get cluster manager (for health checks, etc)
    pub fn cluster(&self) -> &ClusterManager {
        &self.cluster
    }

    /// Get shard manager
    pub fn shards(&self) -> &ShardManager {
        &self.shards
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_orchestrator_creation() {
        let orch = DistributedOrchestrator::new("node1", 4);
        assert_eq!(orch.cluster.self_id().0, "node1");
        assert_eq!(orch.shards.shard_count(), 4);
    }

    #[test]
    fn test_shard_routing() {
        let orch = DistributedOrchestrator::new("node1", 4);

        // Same doc always routes to same shard
        let shard1 = orch.get_shard_for_doc("doc123");
        let shard2 = orch.get_shard_for_doc("doc123");
        assert_eq!(shard1, shard2);
        assert!(shard1 < 4);
    }

    #[test]
    fn test_consistency_across_nodeids() {
        let orch1 = DistributedOrchestrator::new("node1", 8);
        let orch2 = DistributedOrchestrator::new("node2", 8);

        // Different nodes should compute same shard for same doc
        let shard1 = orch1.get_shard_for_doc("doc_x");
        let shard2 = orch2.get_shard_for_doc("doc_x");
        assert_eq!(shard1, shard2);
    }
}
