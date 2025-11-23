// ============================================================================
// CORE TYPES & UTILITIES
// ============================================================================
pub mod core;

// ============================================================================
// INDEXING & SEARCH ALGORITHMS
// ============================================================================
pub mod indexing;
pub mod vector;
pub mod hnsw;

// ============================================================================
// SEARCH ENGINES & QUERY EXECUTION
// ============================================================================
pub mod search;

// ============================================================================
// STORAGE & PERSISTENCE
// ============================================================================
pub mod compression;
pub mod storage;
pub mod buffer;

// ============================================================================
// METADATA & MAIN ENGINE
// ============================================================================
pub mod metadata;
pub mod engine;

// ============================================================================
// DISTRIBUTED & CLUSTERING
// ============================================================================
pub mod distributed;
pub mod multitenancy;

// Re-export commonly used types
pub use core::{Result, WaffleError, VectorType, VectorDocument, PerformanceGates};
pub use indexing::{SparseVector, SparseVectorStore, BM25Index, Tokenizer, MultiVectorDocument, MultiVectorStore};
pub use search::{
    HybridSearch, HybridSearchResult, FusionWeights,
    HybridSearchEngine, HybridSearchQuery, DetailedSearchResult,
    FilteredSearchResult, search_with_filter, count_matching,
    MetadataIndex, MetadataValue, FilterExpression
};
pub use vector::types::Vector;
pub use hnsw::{HNSWBuilder, HNSWIndex};
pub use buffer::{WriteBuffer, MultiLayerSearcher, BuildStatus, SearchResult};
pub use engine::{VectorEngine, EngineSearchResult, EngineStats};
pub use distributed::{
    sharding::ShardId,
    sharding::ShardingStrategy,
    sharding::HashSharding,
    replication::ReplicationConfig,
    replication::ReplicationManager,
    replication::ReplicationState,
    coordinator::Coordinator,
    coordinator::CoordinatorConfig,
    node::DistributedNode,
    node::NodeConfig,
    node::NodeState,
    query_router::QueryRouter,
    query_router::ConsistencyLevel,
    query_router::MergedResult
};
pub use multitenancy::{
    tenant::{Tenant, TenantConfig, TenantId},
    namespace::{Namespace, NamespaceConfig},
    quota::{QuotaManager, DiskQuota}
};
