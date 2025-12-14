/// CompactionManager: Orchestrates background merging of WriteBuffer → HNSW
///
/// Key insight: HNSW can accept new vectors incrementally without full rebuild.
/// CompactionManager handles:
/// 1. Detecting when WriteBuffer is full
/// 2. Draining WriteBuffer and building incremental HNSW layer
/// 3. Merging with existing HNSW index
/// 4. Updating PQ codebooks incrementally (add-only)
/// 5. Tracking tombstones for deleted vectors

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::collections::{HashMap, VecDeque};
use std::time::{SystemTime, UNIX_EPOCH};
use parking_lot::RwLock;
use crate::core::errors::{Result, WaffleError, ErrorCode};
use crate::vector::types::Vector;
use crate::metadata::schema::Metadata;
use crate::buffer::write_buffer::{WriteBuffer, VectorEntry};

/// Compaction configuration
#[derive(Debug, Clone)]
pub struct CompactionConfig {
    /// Trigger compaction when WriteBuffer reaches this size
    pub buffer_threshold: usize,
    
    /// Max vectors to compact in single batch (limits memory)
    pub batch_size: usize,
    
    /// Enable TTL-based cleanup
    pub enable_ttl: bool,
    
    /// TTL in seconds (0 = no expiry)
    pub default_ttl_secs: u64,
    
    /// Max compaction workers to run in parallel
    pub max_parallel_compactions: usize,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        CompactionConfig {
            buffer_threshold: 50_000,  // Compact when 50K vectors in buffer
            batch_size: 10_000,        // Process 10K at a time
            enable_ttl: true,
            default_ttl_secs: 0,       // No TTL by default
            max_parallel_compactions: 2,
        }
    }
}

/// Metrics tracked during compaction
#[derive(Debug, Clone, Default)]
pub struct CompactionStats {
    /// Total vectors compacted
    pub vectors_compacted: u64,
    
    /// Vectors dropped due to TTL
    pub vectors_expired: u64,
    
    /// Vectors deleted (tombstoned)
    pub vectors_deleted: u64,
    
    /// Number of compaction runs
    pub compaction_runs: u64,
    
    /// Total compaction time in ms
    pub total_compaction_time_ms: u64,
    
    /// Average HNSW build time per compaction
    pub avg_hnsw_build_time_ms: u64,
    
    /// Last compaction timestamp
    pub last_compaction_time: Option<u64>,
}

/// Compaction result for a batch
#[derive(Debug)]
pub struct CompactionBatch {
    pub batch_id: u64,
    pub entries: Vec<VectorEntry>,
    pub expired_count: usize,
    pub deleted_count: usize,
    pub valid_count: usize,
}

/// CompactionManager: Orchestrates WriteBuffer → HNSW merging
pub struct CompactionManager {
    /// Configuration
    config: CompactionConfig,
    
    /// Write buffer reference
    write_buffer: Arc<WriteBuffer>,
    
    /// Compaction statistics
    stats: Arc<RwLock<CompactionStats>>,
    
    /// Queue of pending compaction batches
    pending_batches: Arc<RwLock<VecDeque<CompactionBatch>>>,
    
    /// Is a compaction currently running?
    is_compacting: Arc<AtomicBool>,
    
    /// Batch ID counter
    batch_id_counter: Arc<AtomicU64>,
}

impl CompactionManager {
    /// Create new compaction manager
    pub fn new(write_buffer: Arc<WriteBuffer>, config: CompactionConfig) -> Self {
        CompactionManager {
            config,
            write_buffer,
            stats: Arc::new(RwLock::new(CompactionStats::default())),
            pending_batches: Arc::new(RwLock::new(VecDeque::new())),
            is_compacting: Arc::new(AtomicBool::new(false)),
            batch_id_counter: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Check if compaction should trigger
    pub fn should_compact(&self) -> bool {
        let buffer_size = self.write_buffer.len();
        !self.is_compacting.load(Ordering::Acquire) && buffer_size >= self.config.buffer_threshold
    }

    /// Get current compaction stats
    pub fn get_stats(&self) -> CompactionStats {
        self.stats.read().clone()
    }

    /// Start background compaction (non-blocking)
    ///
    /// This is intended to be called from an async task or thread pool.
    /// Returns immediately while compaction happens in background.
    pub async fn compact(&self) -> Result<()> {
        // Already compacting?
        if self.is_compacting.swap(true, Ordering::AcqRel) {
            return Err(WaffleError::StorageError {
                code: ErrorCode::StorageIOError,
                message: "Compaction already in progress".to_string(),
            });
        }

        let start_time = Self::current_timestamp();

        match self.do_compact().await {
            Ok(batch) => {
                // Queue the batch for merging into HNSW
                self.pending_batches.write().push_back(batch);

                // Update stats
                let elapsed = Self::current_timestamp() - start_time;
                let mut stats = self.stats.write();
                stats.compaction_runs += 1;
                stats.total_compaction_time_ms += elapsed;
                stats.avg_hnsw_build_time_ms =
                    stats.total_compaction_time_ms / stats.compaction_runs;
                stats.last_compaction_time = Some(Self::current_timestamp());
            }
            Err(e) => {
                eprintln!("Compaction failed: {}", e);
            }
        }

        // Mark compaction complete
        self.is_compacting.store(false, Ordering::Release);
        Ok(())
    }

    /// Internal: Actually perform the compaction
    async fn do_compact(&self) -> Result<CompactionBatch> {
        // 1. Drain write buffer
        let entries = self.write_buffer.drain();

        // 2. Filter out expired/deleted entries
        let mut valid_entries = Vec::new();
        let mut expired_count = 0;
        let mut deleted_count = 0;

        let now = Self::current_timestamp();

        for entry in entries {
            // Check TTL
            if self.config.enable_ttl && self.config.default_ttl_secs > 0 {
                let age_secs = (now - entry.insertion_time) / 1000;
                if age_secs > self.config.default_ttl_secs {
                    expired_count += 1;
                    continue;
                }
            }

            // Check tombstone (would check TombstoneManager here)
            // For now, just track; actual deletion logic in next PR
            if self.is_tombstoned(&entry.id) {
                deleted_count += 1;
                continue;
            }

            valid_entries.push(entry);
        }

        // 3. Create compaction batch
        let batch_id = self.batch_id_counter.fetch_add(1, Ordering::Release);
        let batch = CompactionBatch {
            batch_id,
            entries: valid_entries,
            expired_count,
            deleted_count,
            valid_count: 0, // Will be set below
        };

        let mut batch = batch;
        batch.valid_count = batch.entries.len();

        // 4. Update stats
        let mut stats = self.stats.write();
        stats.vectors_compacted += batch.valid_count as u64;
        stats.vectors_expired += expired_count as u64;
        stats.vectors_deleted += deleted_count as u64;

        Ok(batch)
    }

    /// Drain pending batches for HNSW merging
    pub fn drain_pending_batches(&self) -> Vec<CompactionBatch> {
        self.pending_batches.write().drain(..).collect()
    }

    /// Get current timestamp in milliseconds
    fn current_timestamp() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0)
    }

    /// Check if vector is tombstoned (placeholder for now)
    fn is_tombstoned(&self, _id: &str) -> bool {
        false // Will integrate with TombstoneManager
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compaction_config_default() {
        let config = CompactionConfig::default();
        assert_eq!(config.buffer_threshold, 50_000);
        assert_eq!(config.batch_size, 10_000);
        assert_eq!(config.max_parallel_compactions, 2);
    }

    #[test]
    fn test_compaction_stats_tracking() {
        let config = CompactionConfig::default();
        let write_buffer = Arc::new(WriteBuffer::new(100_000, 1));
        let manager = CompactionManager::new(write_buffer, config);

        let stats = manager.get_stats();
        assert_eq!(stats.vectors_compacted, 0);
        assert_eq!(stats.compaction_runs, 0);
    }

    #[test]
    fn test_should_compact_trigger() {
        let mut config = CompactionConfig::default();
        config.buffer_threshold = 10;
        let write_buffer = Arc::new(WriteBuffer::new(100, 1));

        // Add vectors until we should compact
        for i in 0..10 {
            let _ = write_buffer.push(
                format!("v{}", i),
                Vector::new(vec![0.5; 128]),
                Metadata::new(),
            );
        }

        let manager = CompactionManager::new(write_buffer, config);
        assert!(manager.should_compact());
    }

    #[test]
    fn test_should_not_compact_when_full() {
        let mut config = CompactionConfig::default();
        config.buffer_threshold = 10;
        let write_buffer = Arc::new(WriteBuffer::new(100, 1));

        for i in 0..10 {
            let _ = write_buffer.push(
                format!("v{}", i),
                Vector::new(vec![0.5; 128]),
                Metadata::new(),
            );
        }

        let manager = CompactionManager::new(write_buffer, config);
        
        // Mark compaction as in progress
        manager.is_compacting.store(true, Ordering::Release);
        
        // Should NOT trigger while compacting
        assert!(!manager.should_compact());
    }
}
