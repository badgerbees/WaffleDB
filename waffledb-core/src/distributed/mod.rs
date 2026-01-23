/// Distributed layer for WaffleDB
/// 
/// Provides:
/// - Hash-based sharding for horizontal scaling
/// - Lightweight replication with RAFT
/// - Query routing and result merging
/// - Multi-node coordination
/// - Automatic failover
///
/// Enterprise-only features (feature = "enterprise"):
/// - Cluster discovery and health checks
/// - Consistent hashing for dynamic rebalancing
/// - Distributed orchestration
/// - Snapshot sync across cluster
/// - Disaster recovery via S3

pub mod sharding;
pub mod replication;
pub mod coordinator;
pub mod node;
pub mod query_router;
pub mod scatter_gather;
pub mod replica_sync;
pub mod raft;  // RAFT consensus implementation
pub mod apply_entry;  // Apply RAFT entries to engine
pub mod leader_election;  // RAFT leader election with timeouts
pub mod log_replication;  // RAFT log replication via AppendEntries RPC

// Multi-node distributed layer (OSS + Enterprise)
pub mod cluster;
pub mod consistent_hash;
pub mod orchestrator;
pub mod snapshot_integration;
pub mod shard_manager;
pub mod rpc;

pub use sharding::{ShardKey, ShardingStrategy, HashSharding};
pub use replication::{ReplicationConfig, ReplicationState, ReplicationManager};
pub use coordinator::{Coordinator, CoordinatorConfig};
pub use node::{DistributedNode, NodeConfig, NodeState};
pub use query_router::{QueryRouter, RoutedQuery, MergedResult};

// Re-export distributed modules (available for all multi-node setups)
pub use cluster::{ClusterManager, NodeId, NodeHealth};
pub use consistent_hash::{ConsistentHashRing, JumpHash};
pub use orchestrator::DistributedOrchestrator;
pub use snapshot_integration::{EngineSnapshot, SnapshotMetadata, SnapshotOperations};
pub use shard_manager::ShardManager;
pub use rpc::{RpcManager, RpcError, AppendEntriesRequest, AppendEntriesResponse, ForwardInsertRequest, ForwardSearchRequest};
