use serde::{Deserialize, Serialize};

/// Raft log entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    pub term: u64,
    pub index: usize,
    pub command: Vec<u8>,
}

/// Raft log.
pub struct RaftLog {
    entries: Vec<LogEntry>,
}

impl RaftLog {
    /// Create a new log.
    pub fn new() -> Self {
        RaftLog {
            entries: vec![],
        }
    }

    /// Append an entry.
    pub fn append(&mut self, entry: LogEntry) {
        self.entries.push(entry);
    }

    /// Get entry at index.
    pub fn get(&self, index: usize) -> Option<&LogEntry> {
        if index == 0 {
            None
        } else {
            self.entries.get(index - 1)
        }
    }

    /// Get last entry.
    pub fn last(&self) -> Option<&LogEntry> {
        self.entries.last()
    }

    /// Get log length.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Truncate log at index.
    pub fn truncate(&mut self, index: usize) {
        if index > 0 && index <= self.entries.len() {
            self.entries.truncate(index);
        }
    }
}
