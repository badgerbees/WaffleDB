use std::path::Path;
use rocksdb::{DB, Options};

/// RocksDB storage wrapper for vectors and metadata.
pub struct RocksDBStore {
    db: DB,
}

impl RocksDBStore {
    /// Create a new RocksDB store.
    pub fn new(path: &Path) -> crate::errors::Result<Self> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        
        let db = DB::open(&opts, path)
            .map_err(|e| crate::errors::WaffleError::StorageError(format!("RocksDB open error: {}", e)))?;
        
        Ok(RocksDBStore { db })
    }

    /// Put a key-value pair.
    pub fn put(&mut self, key: &str, value: &[u8]) -> crate::errors::Result<()> {
        self.db.put(key.as_bytes(), value)
            .map_err(|e| crate::errors::WaffleError::StorageError(format!("Put error: {}", e)))?;
        Ok(())
    }

    /// Get a value by key.
    pub fn get(&self, key: &str) -> crate::errors::Result<Option<Vec<u8>>> {
        self.db.get(key.as_bytes())
            .map_err(|e| crate::errors::WaffleError::StorageError(format!("Get error: {}", e)))
    }

    /// Delete a key.
    pub fn delete(&mut self, key: &str) -> crate::errors::Result<()> {
        self.db.delete(key.as_bytes())
            .map_err(|e| crate::errors::WaffleError::StorageError(format!("Delete error: {}", e)))?;
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
    pub fn flush(&mut self) -> crate::errors::Result<()> {
        self.db.flush()
            .map_err(|e| crate::errors::WaffleError::StorageError(format!("Flush error: {}", e)))?;
        Ok(())
    }
}
