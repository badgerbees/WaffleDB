use std::path::Path;
use std::fs::{self, File};
use std::io::Write;

/// Snapshot metadata.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SnapshotMetadata {
    pub timestamp: u64,
    pub version: u32,
    pub num_vectors: usize,
}

/// Snapshot manager for periodic backups.
pub struct SnapshotManager {
    path: std::path::PathBuf,
}

impl SnapshotManager {
    /// Create a new snapshot manager.
    pub fn new(path: &Path) -> crate::errors::Result<Self> {
        // Create snapshot directory if it doesn't exist
        fs::create_dir_all(path)
            .map_err(|e| crate::errors::WaffleError::StorageError(format!("Dir creation error: {}", e)))?;
        
        Ok(SnapshotManager {
            path: path.to_path_buf(),
        })
    }

    /// Create a snapshot.
    pub fn create_snapshot(
        &self,
        metadata: SnapshotMetadata,
        data: &[u8],
    ) -> crate::errors::Result<String> {
        let filename = format!(
            "snapshot_{}_{}.bin",
            metadata.timestamp, metadata.version
        );
        let filepath = self.path.join(&filename);

        // Write metadata as JSON header
        let metadata_json = serde_json::to_string(&metadata)
            .map_err(|e| crate::errors::WaffleError::StorageError(format!("JSON error: {}", e)))?;
        
        let mut file = File::create(&filepath)
            .map_err(|e| crate::errors::WaffleError::StorageError(format!("File create error: {}", e)))?;
        
        // Write metadata length first, then metadata, then data
        let meta_bytes = metadata_json.as_bytes();
        file.write_all(&(meta_bytes.len() as u32).to_le_bytes())
            .map_err(|e| crate::errors::WaffleError::StorageError(format!("Write error: {}", e)))?;
        file.write_all(meta_bytes)
            .map_err(|e| crate::errors::WaffleError::StorageError(format!("Write error: {}", e)))?;
        file.write_all(data)
            .map_err(|e| crate::errors::WaffleError::StorageError(format!("Write error: {}", e)))?;
        
        file.sync_all()
            .map_err(|e| crate::errors::WaffleError::StorageError(format!("Sync error: {}", e)))?;

        Ok(filename)
    }

    /// List all snapshots.
    pub fn list_snapshots(&self) -> crate::errors::Result<Vec<SnapshotMetadata>> {
        let mut snapshots = vec![];
        
        if let Ok(entries) = fs::read_dir(&self.path) {
            for entry in entries {
                if let Ok(entry) = entry {
                    let path = entry.path();
                    if path.is_file() && path.extension().map_or(false, |ext| ext == "bin") {
                        if let Ok(data) = fs::read(&path) {
                            if let Ok(metadata) = Self::extract_metadata(&data) {
                                snapshots.push(metadata);
                            }
                        }
                    }
                }
            }
        }
        
        Ok(snapshots)
    }

    /// Load a snapshot by filename.
    pub fn load_snapshot(&self, filename: &str) -> crate::errors::Result<Vec<u8>> {
        let filepath = self.path.join(filename);
        let data = fs::read(&filepath)
            .map_err(|e| crate::errors::WaffleError::StorageError(format!("Read error: {}", e)))?;
        
        // Skip metadata header and return just the data
        if data.len() >= 4 {
            let meta_len = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
            if data.len() >= 4 + meta_len {
                return Ok(data[4 + meta_len..].to_vec());
            }
        }
        
        Ok(data)
    }

    /// Delete a snapshot.
    pub fn delete_snapshot(&self, filename: &str) -> crate::errors::Result<()> {
        let filepath = self.path.join(filename);
        fs::remove_file(&filepath)
            .map_err(|e| crate::errors::WaffleError::StorageError(format!("Delete error: {}", e)))?;
        Ok(())
    }

    /// Extract metadata from binary snapshot.
    fn extract_metadata(data: &[u8]) -> crate::errors::Result<SnapshotMetadata> {
        if data.len() < 4 {
            return Err(crate::errors::WaffleError::StorageError("Invalid snapshot format".to_string()));
        }
        
        let meta_len = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
        if data.len() < 4 + meta_len {
            return Err(crate::errors::WaffleError::StorageError("Invalid snapshot format".to_string()));
        }
        
        let meta_json = std::str::from_utf8(&data[4..4 + meta_len])
            .map_err(|_| crate::errors::WaffleError::StorageError("Invalid UTF-8".to_string()))?;
        
        serde_json::from_str(meta_json)
            .map_err(|e| crate::errors::WaffleError::StorageError(format!("JSON parse error: {}", e)))
    }
}
