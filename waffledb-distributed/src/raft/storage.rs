use super::log::LogEntry;
use std::fs::{File, OpenOptions};
use std::io::{Write, Read};
use std::path::{Path, PathBuf};

/// Persistent Raft state storage.
pub struct RaftStorage {
    /// Current term
    pub current_term: u64,
    /// Voted for in current term
    pub voted_for: Option<String>,
    /// Log entries
    pub log: Vec<LogEntry>,
    /// Committed index
    pub committed_index: usize,
    /// Path to persistence directory
    path: PathBuf,
}

impl RaftStorage {
    pub fn new(storage_path: Option<&Path>) -> Self {
        let path = storage_path
            .map(|p| p.to_path_buf())
            .unwrap_or_else(|| PathBuf::from(".raft_state"));
        
        RaftStorage {
            current_term: 0,
            voted_for: None,
            log: vec![],
            committed_index: 0,
            path,
        }
    }

    /// Save state to persistent storage.
    pub fn persist(&self) -> std::io::Result<()> {
        // Create state file with JSON serialization
        let state = serde_json::json!({
            "current_term": self.current_term,
            "voted_for": self.voted_for,
            "committed_index": self.committed_index,
            "log": self.log,
        });
        
        let json = serde_json::to_string(&state)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&self.path)?;
        
        file.write_all(json.as_bytes())?;
        file.sync_all()?;
        
        Ok(())
    }

    /// Load state from persistent storage.
    pub fn load(storage_path: Option<&Path>) -> std::io::Result<Self> {
        let path = storage_path
            .map(|p| p.to_path_buf())
            .unwrap_or_else(|| PathBuf::from(".raft_state"));
        
        if !path.exists() {
            return Ok(RaftStorage::new(storage_path));
        }
        
        let mut file = File::open(&path)?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)?;
        
        let state: serde_json::Value = serde_json::from_str(&contents)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        
        Ok(RaftStorage {
            current_term: state["current_term"].as_u64().unwrap_or(0),
            voted_for: state["voted_for"].as_str().map(|s| s.to_string()),
            log: serde_json::from_value(state["log"].clone()).unwrap_or_default(),
            committed_index: state["committed_index"].as_u64().unwrap_or(0) as usize,
            path,
        })
    }

    /// Store term and voted_for.
    pub fn save_state(&mut self, term: u64, voted_for: Option<String>) {
        self.current_term = term;
        self.voted_for = voted_for;
    }

    /// Append log entries.
    pub fn append_entries(&mut self, entries: Vec<LogEntry>) {
        self.log.extend(entries);
    }
}
