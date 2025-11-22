use crate::vector::types::Vector;
use crate::Result;

/// Search result from engine.
#[derive(Debug, Clone)]
pub struct EngineSearchResult {
    pub id: String,
    pub distance: f32,
}

/// Engine statistics.
#[derive(Debug, Clone)]
pub struct EngineStats {
    pub total_inserts: u64,
    pub total_searches: u64,
    pub total_deletes: u64,
    pub avg_insert_ms: f64,
    pub avg_search_ms: f64,
}

/// Core vector engine trait.
/// 
/// Implementations provide insert, search, and delete operations
/// over a vector index. This trait enables pluggable engines (HNSW, IVF, etc.)
/// while keeping the core optimizations (SIMD, PQ compression) in place.
pub trait VectorEngine: Send + Sync {
    /// Insert a vector with given ID.
    fn insert(&mut self, id: String, vector: Vector) -> Result<()>;

    /// Search for top-k nearest neighbors to query vector.
    fn search(&self, query: &[f32], top_k: usize) -> Result<Vec<EngineSearchResult>>;

    /// Delete a vector by ID.
    fn delete(&mut self, id: &str) -> Result<()>;

    /// Retrieve a stored vector by ID.
    fn get(&self, id: &str) -> Option<Vector>;

    /// Get the number of vectors in the engine.
    fn len(&self) -> usize;

    /// Get engine statistics.
    fn stats(&self) -> EngineStats;
}
