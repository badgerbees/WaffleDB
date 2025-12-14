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
pub mod hnsw_merge;

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
pub mod compaction;
pub mod batch;
pub mod recovery;

// ============================================================================
// ADMIN & OPERATIONS
// ============================================================================
pub mod admin;

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

// ============================================================================
// OBSERVABILITY & TELEMETRY
// ============================================================================
pub mod observability;

// ============================================================================
// TESTING & BENCHMARKS
// ============================================================================
#[cfg(test)]
mod chaos_tests;

#[cfg(test)]
mod production_benchmarks;

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
pub use batch::{BatchInsertRequest, BatchDeleteRequest, BatchShardRouter};
pub use recovery::{Snapshot, SnapshotManager};
pub use admin::{AdminToken, HealthStatus};
pub use observability::{QueryTracer, QueryTrace, HNSWStatsCollector};
pub use engine::{VectorEngine, EngineSearchResult, EngineStats};
