/// Buffer module: Hot data layer for ultra-fast inserts
/// 
/// This module provides the hybrid architecture implementation:
/// - WriteBuffer: O(1) amortized inserts, brute-force search
/// - MultiLayerSearcher: Queries multiple storage layers
/// - BuildStatus: Tracks async HNSW building progress

pub mod write_buffer;

pub use write_buffer::{
    WriteBuffer, VectorEntry, BuildStatus, MultiLayerSearcher, SearchResult,
    SearcherStats, HNSWLayer, ArchiveLayer
};
