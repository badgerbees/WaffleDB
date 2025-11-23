use std::path::Path;
use std::fs::{File, OpenOptions};
use std::io::{Write, Read};
use crate::core::errors::{Result, WaffleError, ErrorCode};

/// Write-Ahead Log entry.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum WALEntry {
    Insert {
        id: String,
        vector: Vec<f32>,
        metadata: Option<String>,
    },
    Delete {
        id: String,
    },
    UpdateMetadata {
        id: String,
        metadata: String,
    },
}

/// Write-Ahead Log for durability.
pub struct WriteAheadLog {
    entries: Vec<WALEntry>,
    path: std::path::PathBuf,
}

impl WriteAheadLog {
    /// Create a new WAL.
    pub fn new(path: &Path) -> Result<Self> {
        // If path is a directory, create wal.json inside it
        let file_path = if path.is_dir() {
            path.join("wal.json")
        } else {
            path.to_path_buf()
        };

        // Try to load existing WAL from disk
        let entries = if file_path.exists() {
            match Self::load_from_disk(&file_path) {
                Ok(entries) => entries,
                Err(_) => vec![], // Start fresh if corrupted
            }
        } else {
            vec![]
        };

        Ok(WriteAheadLog {
            entries,
            path: file_path,
        })
    }

    /// Append an entry to the WAL.
    pub fn append(&mut self, entry: WALEntry) -> Result<()> {
        self.entries.push(entry);
        Ok(())
    }

    /// Get all entries.
    pub fn get_entries(&self) -> &[WALEntry] {
        &self.entries
    }

    /// Clear the WAL.
    pub fn clear(&mut self) -> Result<()> {
        self.entries.clear();
        Ok(())
    }

    /// Sync to disk (writes all entries to file).
    pub fn sync(&self) -> Result<()> {
        let json = serde_json::to_string(&self.entries)
            .map_err(|e| WaffleError::StorageError { code: ErrorCode::StorageIOError, message: format!("JSON error: {}", e) })?;
        
        // Write directly to file with retry logic for Windows file locking
        for attempt in 0..3 {
            match OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&self.path)
            {
                Ok(mut file) => {
                    if let Ok(_) = file.write_all(json.as_bytes()) {
                        if let Ok(_) = file.sync_all() {
                            return Ok(());
                        }
                    }
                    drop(file);
                }
                Err(_e) if attempt < 2 => {
                    // Retry after a small delay
                    std::thread::sleep(std::time::Duration::from_millis(10));
                    continue;
                }
                Err(e) => {
                    return Err(WaffleError::StorageError {
                        code: ErrorCode::StorageIOError,
                        message: format!("File error after {} attempts: {}", attempt + 1, e)
                    });
                }
            }
        }
        
        Ok(())
    }

    /// Load WAL from disk.
    fn load_from_disk(path: &Path) -> Result<Vec<WALEntry>> {
        let mut file = File::open(path)
            .map_err(|e| WaffleError::StorageError { code: ErrorCode::StorageIOError, message: format!("File open error: {}", e) })?;
        
        let mut contents = String::new();
        file.read_to_string(&mut contents)
            .map_err(|e| WaffleError::StorageError { code: ErrorCode::StorageIOError, message: format!("Read error: {}", e) })?;
        
        serde_json::from_str(&contents)
            .map_err(|e| WaffleError::StorageError { code: ErrorCode::StorageIOError, message: format!("JSON parse error: {}", e) })
    }
}
