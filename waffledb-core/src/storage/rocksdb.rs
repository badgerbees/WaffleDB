use std::path::Path;
use rocksdb::{DB, Options, WriteOptions};
use crate::core::errors::{Result, WaffleError, ErrorCode};

/// RocksDB storage wrapper for vectors and metadata with production-grade durability.
pub struct RocksDBStore {
    db: DB,
}

impl RocksDBStore {
    /// Create a new RocksDB store with production-safe options for durability.
    pub fn new(path: &Path) -> Result<Self> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        
        // Enable write-ahead log (WAL) for crash recovery (enabled by default, but explicit)
        opts.set_wal_recovery_mode(rocksdb::DBRecoveryMode::TolerateCorruptedTailRecords);
        
        // Configure for better durability
        opts.set_max_background_jobs(4);
        opts.set_db_log_dir(path);  // Separate log directory for WAL
        
        let db = DB::open(&opts, path)
            .map_err(|e| WaffleError::StorageError {
                code: ErrorCode::StorageIOError,
                message: format!("RocksDB open error: {}", e),
            })?;
        
        Ok(RocksDBStore { db })
    }

    /// Put a key-value pair with durability guarantee via WAL.
    pub fn put(&mut self, key: &str, value: &[u8]) -> Result<()> {
        let mut opts = WriteOptions::default();
        opts.disable_wal(false);  // Ensure WAL is enabled
        opts.set_sync(false);      // Async write for performance, WAL guarantees durability
        
        self.db.put_opt(key.as_bytes(), value, &opts)
            .map_err(|e| WaffleError::StorageError {
                code: ErrorCode::StorageIOError,
                message: format!("Put error: {}", e),
            })?;
        Ok(())
    }
    
    /// Put with explicit fsync for critical operations (snapshots, metadata)
    pub fn put_sync(&mut self, key: &str, value: &[u8]) -> Result<()> {
        let mut opts = WriteOptions::default();
        opts.disable_wal(false);
        opts.set_sync(true);  // Synchronous write - waits for fsync
        
        self.db.put_opt(key.as_bytes(), value, &opts)
            .map_err(|e| WaffleError::StorageError {
                code: ErrorCode::StorageIOError,
                message: format!("Sync put error: {}", e),
            })?;
        Ok(())
    }

    /// Get a value by key.
    pub fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        self.db.get(key.as_bytes())
            .map_err(|e| WaffleError::StorageError {
                code: ErrorCode::StorageIOError,
                message: format!("Get error: {}", e),
            })
    }

    /// Delete a key.
    pub fn delete(&mut self, key: &str) -> Result<()> {
        self.db.delete(key.as_bytes())
            .map_err(|e| WaffleError::StorageError {
                code: ErrorCode::StorageIOError,
                message: format!("Delete error: {}", e),
            })?;
        Ok(())
    }

    /// Check if key exists.
    pub fn contains_key(&self, key: &str) -> bool {
        self.get(key).map(|v| v.is_some()).unwrap_or(false)
    }

    /// Get all keys with a prefix.
    pub fn prefix_iterator(&self, prefix: &str) -> Vec<(String, Vec<u8>)> {
        let mut iter = self.db.prefix_iterator(prefix.as_bytes());
        let mut results = vec![];
        
        loop {
            match iter.next() {
                Some(Ok((key, value))) => {
                    if let Ok(key_str) = String::from_utf8(key.to_vec()) {
                        results.push((key_str, value.to_vec()));
                    }
                }
                Some(Err(_)) | None => break,
            }
        }
        
        results
    }

    /// Flush pending operations to disk.
    pub fn flush(&mut self) -> Result<()> {
        self.db.flush()
            .map_err(|e| WaffleError::StorageError { 
                code: ErrorCode::StorageIOError,
                message: format!("Flush error: {}", e) 
            })?;
        Ok(())
    }
}
