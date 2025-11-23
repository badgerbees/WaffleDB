/// Distributed layer for WaffleDB
/// 
/// Provides:
/// - Hash-based sharding for horizontal scaling
/// - Lightweight replication with RAFT
/// - Query routing and result merging
/// - Multi-node coordination
/// - Automatic failover

pub mod sharding;
pub mod replication;
pub mod coordinator;
pub mod node;
pub mod query_router;

pub use sharding::{ShardKey, ShardingStrategy, HashSharding};
pub use replication::{ReplicationConfig, ReplicationState, ReplicationManager};
pub use coordinator::{Coordinator, CoordinatorConfig};
pub use node::{DistributedNode, NodeConfig, NodeState};
pub use query_router::{QueryRouter, RoutedQuery, MergedResult};
