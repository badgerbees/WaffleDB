/// CompactionScheduler: Background task orchestration for compaction
///
/// Responsibilities:
/// 1. Monitor WriteBuffer fullness
/// 2. Trigger async compaction when threshold reached
/// 3. Handle compaction batches for HNSW integration
/// 4. Manage concurrent compactions (configurable max workers)
/// 5. Track compaction metrics and health

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;
use parking_lot::RwLock;
use tokio::task::JoinHandle;
use tokio::time::sleep;
use crate::core::errors::{Result, WaffleError, ErrorCode};
use crate::compaction::{CompactionManager, CompactionConfig, CompactionStats};
use crate::buffer::write_buffer::WriteBuffer;

/// Configuration for the compaction scheduler
#[derive(Debug, Clone)]
pub struct SchedulerConfig {
    /// How often to check if compaction is needed (ms)
    pub check_interval_ms: u64,
    
    /// Max concurrent compaction tasks
    pub max_workers: usize,
    
    /// Enable scheduler (default: true)
    pub enabled: bool,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        SchedulerConfig {
            check_interval_ms: 1000,  // Check every 1 second
            max_workers: 2,           // At most 2 concurrent compactions
            enabled: true,
        }
    }
}

/// CompactionScheduler: Manages background compaction tasks
pub struct CompactionScheduler {
    config: SchedulerConfig,
    manager: Arc<CompactionManager>,
    
    /// Is scheduler running?
    is_running: Arc<AtomicBool>,
    
    /// Current worker count
    active_workers: Arc<AtomicU64>,
    
    /// Handle to background task (if any)
    background_task: Arc<RwLock<Option<JoinHandle<()>>>>,
}

impl CompactionScheduler {
    /// Create new scheduler
    pub fn new(manager: Arc<CompactionManager>, config: SchedulerConfig) -> Self {
        CompactionScheduler {
            config,
            manager,
            is_running: Arc::new(AtomicBool::new(false)),
            active_workers: Arc::new(AtomicU64::new(0)),
            background_task: Arc::new(RwLock::new(None)),
        }
    }

    /// Start the background scheduler
    ///
    /// This spawns a long-running task that periodically checks if compaction
    /// is needed and triggers it asynchronously.
    pub fn start(&self) -> Result<()> {
        if !self.config.enabled {
            return Ok(());
        }

        // Already running?
        if self.is_running.swap(true, Ordering::AcqRel) {
            return Err(WaffleError::StorageError {
                code: ErrorCode::StorageIOError,
                message: "Scheduler already running".to_string(),
            });
        }

        let manager = self.manager.clone();
        let check_interval = Duration::from_millis(self.config.check_interval_ms);
        let max_workers = self.config.max_workers;
        let active_workers = self.active_workers.clone();
        let is_running = self.is_running.clone();

        let task = tokio::spawn(async move {
            while is_running.load(Ordering::Acquire) {
                sleep(check_interval).await;

                // Check if we should compact
                if manager.should_compact() {
                    // Check worker limit
                    let current_workers =
                        active_workers.load(Ordering::Acquire) as usize;
                    if current_workers >= max_workers {
                        continue; // Skip this cycle, too many workers
                    }

                    // Spawn compaction task
                    active_workers.fetch_add(1, Ordering::Release);
                    let manager_clone = manager.clone();
                    let workers_clone = active_workers.clone();

                    tokio::spawn(async move {
                        match manager_clone.compact().await {
                            Ok(_) => {
                                eprintln!("Compaction completed successfully");
                            }
                            Err(e) => {
                                eprintln!("Compaction failed: {}", e);
                            }
                        }
                        workers_clone.fetch_sub(1, Ordering::Release);
                    });
                }
            }
        });

        *self.background_task.write() = Some(task);
        Ok(())
    }

    /// Stop the scheduler
    pub async fn stop(&self) -> Result<()> {
        self.is_running.store(false, Ordering::Release);

        // Wait for background task to finish
        if let Some(task) = self.background_task.write().take() {
            task.await.ok();
        }

        Ok(())
    }

    /// Get current active worker count
    pub fn active_workers(&self) -> usize {
        self.active_workers.load(Ordering::Acquire) as usize
    }

    /// Check if scheduler is running
    pub fn is_running(&self) -> bool {
        self.is_running.load(Ordering::Acquire)
    }

    /// Get compaction stats
    pub fn get_stats(&self) -> CompactionStats {
        self.manager.get_stats()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compaction::CompactionManager;
    use crate::buffer::write_buffer::WriteBuffer;
    use std::sync::Arc;

    #[test]
    fn test_scheduler_config_default() {
        let config = SchedulerConfig::default();
        assert_eq!(config.check_interval_ms, 1000);
        assert_eq!(config.max_workers, 2);
        assert!(config.enabled);
    }

    #[test]
    fn test_scheduler_creation() {
        let write_buffer = Arc::new(WriteBuffer::new(100, 1));
        let compaction_config = CompactionConfig::default();
        let manager = Arc::new(CompactionManager::new(write_buffer, compaction_config));
        let config = SchedulerConfig::default();

        let scheduler = CompactionScheduler::new(manager, config);
        assert!(!scheduler.is_running());
        assert_eq!(scheduler.active_workers(), 0);
    }

    #[tokio::test]
    async fn test_scheduler_start_stop() {
        let write_buffer = Arc::new(WriteBuffer::new(100, 1));
        let compaction_config = CompactionConfig::default();
        let manager = Arc::new(CompactionManager::new(write_buffer, compaction_config));
        let config = SchedulerConfig::default();

        let scheduler = CompactionScheduler::new(manager, config);

        // Start scheduler
        assert!(scheduler.start().is_ok());
        assert!(scheduler.is_running());

        // Stop scheduler
        assert!(scheduler.stop().await.is_ok());
        assert!(!scheduler.is_running());
    }

    #[tokio::test]
    async fn test_scheduler_prevents_double_start() {
        let write_buffer = Arc::new(WriteBuffer::new(100, 1));
        let compaction_config = CompactionConfig::default();
        let manager = Arc::new(CompactionManager::new(write_buffer, compaction_config));
        let config = SchedulerConfig::default();

        let scheduler = CompactionScheduler::new(manager, config);

        // Start once
        assert!(scheduler.start().is_ok());

        // Try to start again
        assert!(scheduler.start().is_err());

        scheduler.stop().await.ok();
    }

    #[test]
    fn test_scheduler_disabled() {
        let write_buffer = Arc::new(WriteBuffer::new(100, 1));
        let compaction_config = CompactionConfig::default();
        let manager = Arc::new(CompactionManager::new(write_buffer, compaction_config));
        let mut config = SchedulerConfig::default();
        config.enabled = false;

        let scheduler = CompactionScheduler::new(manager, config);

        // Should succeed but not actually start
        assert!(scheduler.start().is_ok());
        assert!(!scheduler.is_running());
    }
}
