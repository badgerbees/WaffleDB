/// WriteBuffer: Hot data layer for ultra-fast inserts
/// 
/// Architecture:
/// - Append-only vector storage (no HNSW overhead)
/// - Serves brute-force searches until HNSW build completes
/// - Automatically triggers async HNSW building when full
/// - Rotates to new buffer after build starts

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};
use parking_lot::RwLock;
use crate::vector::types::Vector;
use crate::metadata::schema::Metadata;
use crate::errors::{WaffleError, Result};

/// A single vector entry in the buffer
#[derive(Debug, Clone)]
pub struct VectorEntry {
    pub id: String,
    pub vector: Vector,
    pub metadata: Metadata,
    pub insertion_time: u64,  // Unix timestamp in ms for TTL tracking
}

/// WriteBuffer: High-performance append-only buffer
/// 
/// Properties:
/// - O(1) amortized inserts
/// - Supports brute-force search for <50K vectors
/// - Automatically triggers HNSW build when full
/// - Thread-safe via RwLock and Arc
pub struct WriteBuffer {
    /// Raw vectors (not compressed)
    entries: Arc<RwLock<Vec<VectorEntry>>>,
    
    /// Configuration
    capacity: usize,
    buffer_id: u64,
    
    /// State tracking with atomic operations for lock-free reads
    is_building: Arc<AtomicBool>,
    entry_count: Arc<AtomicU64>,
    build_start_time: Arc<RwLock<Option<u64>>>,
}

impl WriteBuffer {
    /// Create new buffer with specified capacity
    pub fn new(capacity: usize, buffer_id: u64) -> Self {
        WriteBuffer {
            entries: Arc::new(RwLock::new(Vec::with_capacity(capacity))),
            capacity,
            buffer_id,
            is_building: Arc::new(AtomicBool::new(false)),
            entry_count: Arc::new(AtomicU64::new(0)),
            build_start_time: Arc::new(RwLock::new(None)),
        }
    }

    /// Get current timestamp in milliseconds
    #[inline]
    fn current_timestamp() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0)
    }

    /// Ultra-fast append to buffer (O(1) amortized)
    /// 
    /// Returns:
    /// - Ok(()) if successfully inserted
    /// - Err(BufferFull) if buffer is at capacity
    /// - Err(BuildInProgress) if buffer is currently being built
    pub fn push(&self, id: String, vector: Vector, metadata: Metadata) -> Result<()> {
        // Fast check without locking if we're building
        if self.is_building.load(Ordering::Acquire) {
            return Err(WaffleError::StorageError("Buffer is currently building HNSW".to_string()));
        }

        // Acquire write lock
        let mut entries = self.entries.write();

        // Check capacity
        if entries.len() >= self.capacity {
            return Err(WaffleError::StorageError("Buffer at capacity".to_string()));
        }

        entries.push(VectorEntry {
            id,
            vector,
            metadata,
            insertion_time: Self::current_timestamp(),
        });

        // Update entry count atomically
        self.entry_count.fetch_add(1, Ordering::Release);

        Ok(())
    }

    /// Get current buffer size
    #[inline]
    pub fn len(&self) -> usize {
        self.entry_count.load(Ordering::Acquire) as usize
    }

    /// Check if buffer is empty
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Check if buffer should trigger HNSW build
    #[inline]
    pub fn should_build(&self) -> bool {
        let is_building = self.is_building.load(Ordering::Acquire);
        let len = self.len();
        
        len >= self.capacity && !is_building
    }

    /// Mark buffer as building (called when async build starts)
    pub fn mark_building(&self) {
        self.is_building.store(true, Ordering::Release);
        *self.build_start_time.write() = Some(Self::current_timestamp());
    }

    /// Mark buffer as complete (called when async build finishes)
    pub fn mark_complete(&self) {
        self.is_building.store(false, Ordering::Release);
    }

    /// Brute-force search on buffer (fast for <50K vectors)
    /// 
    /// # Arguments
    /// * `query` - Query vector
    /// * `top_k` - Number of results to return
    /// * `distance_fn` - Distance metric function
    /// 
    /// # Returns
    /// Vector of (id, distance) tuples sorted by distance ascending
    pub fn search(
        &self,
        query: &[f32],
        top_k: usize,
        distance_fn: fn(&[f32], &[f32]) -> f32,
    ) -> Vec<(String, f32)> {
        let entries = self.entries.read();

        if entries.is_empty() {
            return vec![];
        }

        let mut results: Vec<(String, f32)> = entries
            .iter()
            .map(|entry| {
                let dist = distance_fn(query, &entry.vector.data);
                (entry.id.clone(), dist)
            })
            .collect();

        // Sort by distance (ascending, best matches first)
        results.sort_by(|a, b| {
            a.1.partial_cmp(&b.1)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        // Truncate to top_k
        results.truncate(top_k);

        results
    }

    /// Search with metadata filtering
    /// 
    /// Returns only vectors where the filter predicate returns true for metadata
    pub fn search_with_filter<F>(
        &self,
        query: &[f32],
        top_k: usize,
        distance_fn: fn(&[f32], &[f32]) -> f32,
        filter: F,
    ) -> Vec<(String, f32)>
    where
        F: Fn(&Metadata) -> bool,
    {
        let entries = self.entries.read();

        if entries.is_empty() {
            return vec![];
        }

        let mut results: Vec<(String, f32)> = entries
            .iter()
            .filter(|entry| filter(&entry.metadata))
            .map(|entry| {
                let dist = distance_fn(query, &entry.vector.data);
                (entry.id.clone(), dist)
            })
            .collect();

        // Sort by distance
        results.sort_by(|a, b| {
            a.1.partial_cmp(&b.1)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        results.truncate(top_k);

        results
    }

    /// Drain buffer for HNSW build (takes ownership, empties buffer)
    /// 
    /// This is called when starting an async HNSW build. The returned
    /// entries should be used to build the HNSW index.
    pub fn drain(&self) -> Vec<VectorEntry> {
        let mut entries = self.entries.write();
        let drained = entries.drain(..).collect();
        
        // Reset count
        self.entry_count.store(0, Ordering::Release);
        
        drained
    }

    /// Get all entries (immutable, for iteration)
    pub fn get_entries(&self) -> Vec<VectorEntry> {
        self.entries
            .read()
            .iter()
            .cloned()
            .collect()
    }

    /// Check if build is in progress
    #[inline]
    pub fn is_building(&self) -> bool {
        self.is_building.load(Ordering::Acquire)
    }

    /// Get build status with detailed information
    pub fn build_status(&self) -> BuildStatus {
        let size = self.len();
        let is_building = self.is_building.load(Ordering::Acquire);
        let build_start = *self.build_start_time.read();

        let elapsed_ms = build_start
            .map(|start| Self::current_timestamp() - start)
            .unwrap_or(0);

        let estimated_complete_ms = if is_building && size > 0 {
            // Estimate: 30µs per vector to build HNSW (conservative)
            Some((size as u64) * 30 / 1000)
        } else {
            None
        };

        BuildStatus {
            is_building,
            buffer_size: size,
            buffer_capacity: self.capacity,
            elapsed_ms,
            estimated_complete_ms,
            buffer_id: self.buffer_id,
        }
    }

    /// Get buffer ID for tracking
    pub fn buffer_id(&self) -> u64 {
        self.buffer_id
    }

    /// Get the capacity
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Clone the internal Arc for use in async tasks
    pub fn clone_inner(&self) -> Arc<RwLock<Vec<VectorEntry>>> {
        Arc::clone(&self.entries)
    }
}

/// Build status information
#[derive(Debug, Clone)]
pub struct BuildStatus {
    pub is_building: bool,
    pub buffer_size: usize,
    pub buffer_capacity: usize,
    pub elapsed_ms: u64,
    pub estimated_complete_ms: Option<u64>,
    pub buffer_id: u64,
}

/// Search result from multi-layer search
#[derive(Debug, Clone)]
pub struct SearchResult {
    pub id: String,
    pub distance: f32,
    pub layer: String,  // "buffer", "hnsw", or "archive"
}

/// Multi-layer search router
/// 
/// Queries multiple storage layers in parallel and merges results.
/// This enables fast searches while asynchronously building HNSW.
pub struct MultiLayerSearcher {
    pub buffer: Arc<WriteBuffer>,
    pub hnsw_layer: Option<Arc<HNSWLayer>>,
    pub archive_layer: Option<Arc<ArchiveLayer>>,
}

/// Placeholder for HNSW layer (will be integrated in Phase 2)
pub struct HNSWLayer {
    pub id: u64,
    // Will contain actual HNSW index data
}

/// Placeholder for Archive layer (will be integrated later)
pub struct ArchiveLayer {
    pub id: u64,
    // Will contain persistent storage
}

impl MultiLayerSearcher {
    /// Create new multi-layer searcher
    pub fn new(buffer: Arc<WriteBuffer>) -> Self {
        MultiLayerSearcher {
            buffer,
            hnsw_layer: None,
            archive_layer: None,
        }
    }

    /// Search across all layers
    /// 
    /// Strategy:
    /// 1. Query buffer layer (fast brute-force if not empty)
    /// 2. Query HNSW layer (if exists)
    /// 3. Merge results with deduplication
    /// 4. Re-rank and truncate to top_k
    pub fn search(
        &self,
        query: &[f32],
        top_k: usize,
        distance_fn: fn(&[f32], &[f32]) -> f32,
    ) -> Vec<SearchResult> {
        let mut all_results: HashMap<String, SearchResult> = HashMap::new();

        // Phase 1: Search hot buffer (if not empty)
        if self.buffer.len() > 0 {
            let buffer_results = self.buffer.search(query, top_k * 2, distance_fn);
            for (id, dist) in buffer_results {
                all_results.insert(
                    id.clone(),
                    SearchResult {
                        id,
                        distance: dist,
                        layer: "buffer".to_string(),
                    },
                );
            }
        }

        // Phase 2: Search HNSW layer (if exists and not building)
        // NOTE: In Phase 2, this will query the actual HNSW index
        if let Some(_hnsw) = &self.hnsw_layer {
            // Placeholder: will implement actual HNSW search in Phase 2
            // let hnsw_results = hnsw.search(query, top_k * 2, distance_fn);
            // for (id, dist) in hnsw_results {
            //     all_results.entry(id.clone())
            //         .or_insert(SearchResult {
            //             id,
            //             distance: dist,
            //             layer: "hnsw".to_string(),
            //         });
            // }
        }

        // Phase 3: Convert to sorted results
        let mut final_results: Vec<SearchResult> = all_results.into_values().collect();
        final_results.sort_by(|a, b| {
            a.distance.partial_cmp(&b.distance)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        final_results.truncate(top_k);

        final_results
    }

    /// Check if any layer is building
    pub fn is_building(&self) -> bool {
        self.buffer.is_building()
    }

    /// Get system statistics
    pub fn stats(&self) -> SearcherStats {
        SearcherStats {
            buffer_size: self.buffer.len(),
            buffer_capacity: self.buffer.capacity(),
            is_building: self.buffer.is_building(),
            has_hnsw: self.hnsw_layer.is_some(),
            has_archive: self.archive_layer.is_some(),
        }
    }
}

/// Statistics for multi-layer searcher
#[derive(Debug, Clone)]
pub struct SearcherStats {
    pub buffer_size: usize,
    pub buffer_capacity: usize,
    pub is_building: bool,
    pub has_hnsw: bool,
    pub has_archive: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn euclidean_distance(a: &[f32], b: &[f32]) -> f32 {
        a.iter()
            .zip(b.iter())
            .map(|(x, y)| (x - y).powi(2))
            .sum::<f32>()
            .sqrt()
    }

    #[test]
    fn test_buffer_creation() {
        let buffer = WriteBuffer::new(100, 1);
        assert_eq!(buffer.len(), 0);
        assert!(buffer.is_empty());
        assert!(!buffer.is_building());
    }

    #[test]
    fn test_buffer_insert() {
        let buffer = WriteBuffer::new(100, 1);

        for i in 0..10 {
            let id = format!("vec_{}", i);
            let vector = Vector::new(vec![0.1; 128]);
            let metadata = Metadata::new();

            let result = buffer.push(id, vector, metadata);
            assert!(result.is_ok());
        }

        assert_eq!(buffer.len(), 10);
    }

    #[test]
    fn test_buffer_capacity() {
        let buffer = WriteBuffer::new(5, 1);

        // Fill the buffer
        for i in 0..5 {
            let id = format!("vec_{}", i);
            let vector = Vector::new(vec![0.1; 128]);
            let metadata = Metadata::new();

            let result = buffer.push(id, vector, metadata);
            assert!(result.is_ok());
        }

        // Next insert should fail
        let result = buffer.push(
            "vec_overflow".to_string(),
            Vector::new(vec![0.1; 128]),
            Metadata::new(),
        );

        assert!(result.is_err());
    }

    #[test]
    fn test_buffer_search() {
        let buffer = WriteBuffer::new(100, 1);

        // Insert vectors with varying first dimension
        for i in 0..10 {
            let mut data = vec![0.0; 128];
            data[0] = i as f32 / 10.0;

            let id = format!("vec_{}", i);
            let vector = Vector::new(data);
            let metadata = Metadata::new();

            buffer.push(id, vector, metadata).unwrap();
        }

        // Search with query vector
        let query = vec![0.5; 128];
        let results = buffer.search(&query, 5, euclidean_distance);

        assert_eq!(results.len(), 5);
        // Results should be sorted by distance
        for i in 1..results.len() {
            assert!(results[i].1 >= results[i - 1].1);
        }
    }

    #[test]
    fn test_buffer_search_empty() {
        let buffer = WriteBuffer::new(100, 1);
        let query = vec![0.5; 128];
        let results = buffer.search(&query, 5, euclidean_distance);

        assert!(results.is_empty());
    }

    #[test]
    fn test_buffer_drain() {
        let buffer = WriteBuffer::new(100, 1);

        // Insert vectors
        for i in 0..10 {
            let id = format!("vec_{}", i);
            let vector = Vector::new(vec![0.1; 128]);
            let metadata = Metadata::new();

            buffer.push(id, vector, metadata).unwrap();
        }

        assert_eq!(buffer.len(), 10);

        // Drain
        let entries = buffer.drain();
        assert_eq!(entries.len(), 10);
        assert_eq!(buffer.len(), 0);

        // Verify entries are valid
        for (i, entry) in entries.iter().enumerate() {
            assert_eq!(entry.id, format!("vec_{}", i));
        }
    }

    #[test]
    fn test_buffer_build_status() {
        let buffer = WriteBuffer::new(100, 42);

        assert!(!buffer.is_building());
        
        let status = buffer.build_status();
        assert_eq!(status.buffer_id, 42);
        assert!(!status.is_building);
        assert_eq!(status.buffer_size, 0);
        assert_eq!(status.buffer_capacity, 100);

        // Add some vectors so size > 0
        for i in 0..10 {
            let id = format!("vec_{}", i);
            let vector = Vector::new(vec![0.1; 128]);
            let metadata = Metadata::new();
            buffer.push(id, vector, metadata).unwrap();
        }

        // Mark as building
        buffer.mark_building();
        assert!(buffer.is_building());

        let status = buffer.build_status();
        assert!(status.is_building);
        assert!(status.estimated_complete_ms.is_some());
        assert_eq!(status.buffer_size, 10);

        // Mark as complete
        buffer.mark_complete();
        assert!(!buffer.is_building());
    }

    #[test]
    fn test_multi_layer_searcher() {
        let buffer = Arc::new(WriteBuffer::new(100, 1));

        // Insert vectors
        for i in 0..10 {
            let mut data = vec![0.0; 128];
            data[0] = i as f32 / 10.0;

            let id = format!("vec_{}", i);
            let vector = Vector::new(data);
            let metadata = Metadata::new();

            buffer.push(id, vector, metadata).unwrap();
        }

        // Create searcher
        let searcher = MultiLayerSearcher::new(buffer);

        // Search
        let query = vec![0.5; 128];
        let results = searcher.search(&query, 5, euclidean_distance);

        assert_eq!(results.len(), 5);
        for result in &results {
            assert_eq!(result.layer, "buffer");
        }
    }

    #[test]
    fn test_multi_layer_stats() {
        let buffer = Arc::new(WriteBuffer::new(100, 1));

        // Insert vectors
        for i in 0..25 {
            let id = format!("vec_{}", i);
            let vector = Vector::new(vec![0.1; 128]);
            let metadata = Metadata::new();

            buffer.push(id, vector, metadata).unwrap();
        }

        let searcher = MultiLayerSearcher::new(buffer);
        let stats = searcher.stats();

        assert_eq!(stats.buffer_size, 25);
        assert_eq!(stats.buffer_capacity, 100);
        assert!(!stats.is_building);
        assert!(!stats.has_hnsw);
        assert!(!stats.has_archive);
    }
}

