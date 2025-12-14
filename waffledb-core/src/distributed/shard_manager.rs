/// Shard management and mapping
///
/// Tracks:
/// - Which collections exist
/// - How collections map to shards
/// - Replica distribution across nodes
/// - Rebalancing on node changes

use crate::distributed::cluster::NodeId;

#[cfg(feature = "enterprise")]
use std::collections::HashMap;
#[cfg(not(feature = "enterprise"))]
use std::collections::HashMap;

use std::sync::Arc;
use parking_lot::RwLock;

/// A shard is a partition of a collection across replicas
#[derive(Debug, Clone)]
pub struct Shard {
    /// Shard ID (0..num_shards-1)
    pub id: usize,
    
    /// Collection this shard belongs to
    pub collection: String,
    
    /// Primary replica node
    pub primary: NodeId,
    
    /// Secondary replicas (followers)
    pub replicas: Vec<NodeId>,
}

/// Maps collections to shards and tracks replica placement
#[derive(Debug)]
pub struct ShardManager {
    /// Collection -> shard assignments
    shards: Arc<RwLock<HashMap<String, Vec<Shard>>>>,
    
    /// Number of shards per collection
    num_shards: usize,
}

impl ShardManager {
    /// Create a new shard manager
    pub fn new(num_shards: usize) -> Self {
        ShardManager {
            shards: Arc::new(RwLock::new(HashMap::new())),
            num_shards,
        }
    }

    /// Assign shards for a collection
    /// 
    /// Creates num_shards shards, each with a primary and replicas
    pub fn assign_collection(
        &self,
        collection: &str,
        primary_nodes: Vec<NodeId>,
        replication_factor: usize,
    ) -> Result<(), String> {
        if primary_nodes.is_empty() {
            return Err("Need at least 1 node".to_string());
        }

        let mut shards = vec![];
        
        for shard_id in 0..self.num_shards {
            // Round-robin assign primary
            let primary_idx = shard_id % primary_nodes.len();
            let primary = primary_nodes[primary_idx].clone();

            // Round-robin assign replicas
            let mut replicas = vec![];
            for i in 1..replication_factor {
                let replica_idx = (shard_id + i) % primary_nodes.len();
                if replica_idx != primary_idx {
                    replicas.push(primary_nodes[replica_idx].clone());
                }
            }

            shards.push(Shard {
                id: shard_id,
                collection: collection.to_string(),
                primary,
                replicas,
            });
        }

        self.shards.write().insert(collection.to_string(), shards);
        Ok(())
    }

    /// Get shard assignments for a collection
    pub fn get_shards(&self, collection: &str) -> Option<Vec<Shard>> {
        self.shards.read().get(collection).cloned()
    }

    /// Get the shard for a document
    pub fn get_shard_for_doc(&self, collection: &str, doc_id: &str) -> Option<Shard> {
        let shards = self.shards.read().get(collection).cloned()?;
        
        // Hash doc_id to shard
        let shard_idx = {
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            use std::hash::{Hash, Hasher};
            doc_id.hash(&mut hasher);
            (hasher.finish() as usize) % self.num_shards
        };

        shards.iter().find(|s| s.id == shard_idx).cloned()
    }

    /// Find which node is primary for a shard
    pub fn get_primary_node(
        &self,
        collection: &str,
        shard_id: usize,
    ) -> Option<NodeId> {
        self.shards
            .read()
            .get(collection)?
            .iter()
            .find(|s| s.id == shard_id)
            .map(|s| s.primary.clone())
    }

    /// Find replica nodes for a shard
    pub fn get_replica_nodes(
        &self,
        collection: &str,
        shard_id: usize,
    ) -> Option<Vec<NodeId>> {
        self.shards
            .read()
            .get(collection)?
            .iter()
            .find(|s| s.id == shard_id)
            .map(|s| s.replicas.clone())
    }

    /// Rebalance shards when a node is removed
    /// (Simple: redistribute to remaining nodes)
    pub fn rebalance_on_node_removal(
        &self,
        collection: &str,
        removed_node: &NodeId,
        replacement_nodes: Vec<NodeId>,
    ) -> Result<(), String> {
        if replacement_nodes.is_empty() {
            return Err("Need at least 1 replacement node".to_string());
        }

        let mut shards = self.shards.write();
        if let Some(col_shards) = shards.get_mut(collection) {
            for shard in col_shards {
                // If this was the primary, reassign
                if &shard.primary == removed_node {
                    shard.primary = replacement_nodes[shard.id % replacement_nodes.len()].clone();
                }

                // Remove from replicas and replace
                shard.replicas.retain(|n| n != removed_node);
                if shard.replicas.len() < replacement_nodes.len() - 1 {
                    for replacement in &replacement_nodes {
                        if replacement != &shard.primary && !shard.replicas.contains(replacement) {
                            shard.replicas.push(replacement.clone());
                            break;
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Number of shards per collection
    pub fn shard_count(&self) -> usize {
        self.num_shards
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_shard_assignment() {
        let manager = ShardManager::new(4);
        let nodes = vec![
            NodeId::new("node1"),
            NodeId::new("node2"),
            NodeId::new("node3"),
        ];

        let result = manager.assign_collection("vectors", nodes, 2);
        assert!(result.is_ok());

        let shards = manager.get_shards("vectors").unwrap();
        assert_eq!(shards.len(), 4);

        // Each shard should have a primary
        for shard in shards {
            assert!(!shard.primary.0.is_empty());
        }
    }

    #[test]
    fn test_get_shard_for_doc() {
        let manager = ShardManager::new(4);
        let nodes = vec![NodeId::new("node1"), NodeId::new("node2")];

        manager
            .assign_collection("vectors", nodes, 1)
            .unwrap();

        // Same doc always maps to same shard
        let shard1 = manager.get_shard_for_doc("vectors", "doc123").unwrap();
        let shard2 = manager.get_shard_for_doc("vectors", "doc123").unwrap();
        assert_eq!(shard1.id, shard2.id);

        // In range
        assert!(shard1.id < 4);
    }

    #[test]
    fn test_rebalancing() {
        let manager = ShardManager::new(3);
        let nodes = vec![
            NodeId::new("node1"),
            NodeId::new("node2"),
            NodeId::new("node3"),
        ];

        manager
            .assign_collection("vectors", nodes.clone(), 2)
            .unwrap();

        let shards_before = manager.get_shards("vectors").unwrap();
        let has_node1_before = shards_before
            .iter()
            .any(|s| s.primary == NodeId::new("node1") || s.replicas.contains(&NodeId::new("node1")));
        assert!(has_node1_before);

        // Remove node1
        let replacement = vec![NodeId::new("node4")];
        manager
            .rebalance_on_node_removal("vectors", &NodeId::new("node1"), replacement)
            .unwrap();

        let shards_after = manager.get_shards("vectors").unwrap();
        let has_node1_after = shards_after
            .iter()
            .any(|s| s.primary == NodeId::new("node1") || s.replicas.contains(&NodeId::new("node1")));
        assert!(!has_node1_after);
    }
}
