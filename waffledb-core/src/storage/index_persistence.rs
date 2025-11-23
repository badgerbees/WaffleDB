use std::path::{Path, PathBuf};
use std::fs::{self, File};
use std::io::{Write, Read, BufReader, BufWriter};
use serde::{Serialize, Deserialize};
use crate::core::errors::{Result, WaffleError, ErrorCode};
use crate::hnsw::graph::HNSWIndex;

/// Index state machine for crash-safe recovery
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum IndexState {
    /// No index exists
    Empty,
    /// Index is being built (in-progress)
    Building,
    /// Index is ready for queries
    Ready,
    /// Index has detected corruption
    Corrupted,
    /// Index is recovering from crash
    Recovering,
}

/// Serializable HNSW graph snapshot
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HNSWSnapshot {
    /// Index metadata
    pub metadata: IndexMetadata,
    /// Graph layers
    pub layers: Vec<GraphLayer>,
    /// Entry point node ID
    pub entry_point: Option<usize>,
    /// HNSW parameters
    pub m: usize,
    pub m_l: f32,
}

/// Index metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexMetadata {
    /// Timestamp of snapshot
    pub timestamp: u64,
    /// Version number
    pub version: u32,
    /// Total nodes in index
    pub num_nodes: usize,
    /// Total layers
    pub num_layers: usize,
    /// Index state
    pub state: IndexState,
}

/// Serializable graph layer
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphLayer {
    /// Adjacency list: node_id -> neighbors
    pub adjacencies: Vec<(usize, Vec<usize>)>,
}

/// State file tracking index persistence
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateFile {
    /// Current index state
    pub state: IndexState,
    /// Last successful snapshot timestamp
    pub last_snapshot_timestamp: Option<u64>,
    /// Last snapshot version
    pub last_snapshot_version: Option<u32>,
    /// WAL entries since last snapshot
    pub wal_entries_count: usize,
}

/// Index persistence manager
pub struct IndexPersistence {
    /// Base directory for all persistence files
    #[allow(dead_code)]
    base_dir: PathBuf,
    /// Current state file path
    state_file: PathBuf,
    /// Snapshots directory
    snapshots_dir: PathBuf,
    /// Current state
    state: IndexState,
}

impl IndexPersistence {
    /// Create a new persistence manager
    pub fn new(base_dir: &Path) -> Result<Self> {
        // Create base directory and subdirectories
        fs::create_dir_all(base_dir)
            .map_err(|e| WaffleError::StorageError {
                code: ErrorCode::StorageIOError,
                message: format!("Failed to create base dir: {}", e)
            })?;

        let snapshots_dir = base_dir.join("snapshots");
        fs::create_dir_all(&snapshots_dir)
            .map_err(|e| WaffleError::StorageError {
                code: ErrorCode::StorageIOError,
                message: format!("Failed to create snapshots dir: {}", e)
            })?;

        let state_file = base_dir.join("state.json");

        // Load existing state or create new
        let state = if state_file.exists() {
            let existing_state = Self::load_state_file(&state_file)?;
            existing_state.state
        } else {
            IndexState::Empty
        };

        Ok(IndexPersistence {
            base_dir: base_dir.to_path_buf(),
            state_file,
            snapshots_dir,
            state,
        })
    }

    /// Get current index state
    pub fn current_state(&self) -> IndexState {
        self.state
    }

    /// Mark index as building
    pub fn mark_building(&mut self) -> Result<()> {
        self.state = IndexState::Building;
        self.save_state_file(IndexState::Building, None, None, 0)?;
        Ok(())
    }

    /// Mark index as ready
    pub fn mark_ready(&mut self, snapshot_timestamp: u64, snapshot_version: u32) -> Result<()> {
        self.state = IndexState::Ready;
        self.save_state_file(
            IndexState::Ready,
            Some(snapshot_timestamp),
            Some(snapshot_version),
            0,
        )?;
        Ok(())
    }

    /// Mark index as corrupted
    pub fn mark_corrupted(&mut self) -> Result<()> {
        self.state = IndexState::Corrupted;
        self.save_state_file(IndexState::Corrupted, None, None, 0)?;
        Ok(())
    }

    /// Mark index as recovering
    pub fn mark_recovering(&mut self) -> Result<()> {
        self.state = IndexState::Recovering;
        self.save_state_file(IndexState::Recovering, None, None, 0)?;
        Ok(())
    }

    /// Save HNSW graph snapshot to disk
    pub fn save_snapshot(&mut self, snapshot: &HNSWSnapshot) -> Result<(u64, u32)> {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|e| WaffleError::StorageError {
                code: ErrorCode::StorageIOError,
                message: format!("Failed to get timestamp: {}", e)
            })?
            .as_secs();

        let version = self.get_next_version()?;
        let filename = format!("snapshot_{}_{}.bin", timestamp, version);
        let filepath = self.snapshots_dir.join(&filename);

        // Write snapshot to file
        let file = File::create(&filepath)
            .map_err(|e| WaffleError::StorageError {
                code: ErrorCode::StorageIOError,
                message: format!("Failed to create snapshot file: {}", e)
            })?;

        let writer = BufWriter::new(file);
        bincode::serialize_into(writer, snapshot)
            .map_err(|e| WaffleError::StorageError {
                code: ErrorCode::StorageIOError,
                message: format!("Failed to serialize snapshot: {}", e)
            })?;

        // Update state file
        self.mark_ready(timestamp, version)?;

        Ok((timestamp, version))
    }

    /// Load latest snapshot
    pub fn load_latest_snapshot(&self) -> Result<Option<HNSWSnapshot>> {
        // Get state file
        if !self.state_file.exists() {
            return Ok(None);
        }

        let state_data = Self::load_state_file(&self.state_file)?;

        // If no snapshot recorded, return None
        let (Some(timestamp), Some(version)) = (state_data.last_snapshot_timestamp, state_data.last_snapshot_version) else {
            return Ok(None);
        };

        let filename = format!("snapshot_{}_{}.bin", timestamp, version);
        let filepath = self.snapshots_dir.join(&filename);

        if !filepath.exists() {
            return Ok(None);
        }

        // Load and deserialize snapshot
        let file = File::open(&filepath)
            .map_err(|e| WaffleError::StorageError {
                code: ErrorCode::StorageIOError,
                message: format!("Failed to open snapshot: {}", e)
            })?;

        let reader = BufReader::new(file);
        let snapshot = bincode::deserialize_from(reader)
            .map_err(|e| WaffleError::StorageError {
                code: ErrorCode::StorageIOError,
                message: format!("Failed to deserialize snapshot: {}", e)
            })?;

        Ok(Some(snapshot))
    }

    /// Load all snapshots metadata (for recovery/cleanup)
    pub fn list_snapshots(&self) -> Result<Vec<(u64, u32, PathBuf)>> {
        let mut snapshots = vec![];

        if !self.snapshots_dir.exists() {
            return Ok(snapshots);
        }

        let entries = fs::read_dir(&self.snapshots_dir)
            .map_err(|e| WaffleError::StorageError {
                code: ErrorCode::StorageIOError,
                message: format!("Failed to read snapshots dir: {}", e)
            })?;

        for entry in entries {
            let entry = entry
                .map_err(|e| WaffleError::StorageError {
                    code: ErrorCode::StorageIOError,
                    message: format!("Failed to read dir entry: {}", e)
                })?;

            let path = entry.path();
            if path.is_file() && path.extension().map_or(false, |ext| ext == "bin") {
                if let Some(filename) = path.file_name().and_then(|n| n.to_str()) {
                    if filename.starts_with("snapshot_") {
                        // Parse timestamp and version from filename
                        if let Some(name_without_ext) = filename.strip_suffix(".bin") {
                            let parts: Vec<&str> = name_without_ext.split('_').collect();
                            if parts.len() >= 3 {
                                if let (Ok(timestamp), Ok(version)) = (parts[1].parse::<u64>(), parts[2].parse::<u32>()) {
                                    snapshots.push((timestamp, version, path));
                                }
                            }
                        }
                    }
                }
            }
        }

        snapshots.sort_by(|a, b| b.0.cmp(&a.0)); // Sort by timestamp descending
        Ok(snapshots)
    }

    /// Cleanup old snapshots (keep only last N)
    pub fn cleanup_old_snapshots(&self, keep_count: usize) -> Result<()> {
        let snapshots = self.list_snapshots()?;

        if snapshots.len() <= keep_count {
            return Ok(());
        }

        // Remove oldest snapshots
        for (_timestamp, _version, path) in snapshots.iter().skip(keep_count) {
            fs::remove_file(path)
                .map_err(|e| WaffleError::StorageError {
                    code: ErrorCode::StorageIOError,
                    message: format!("Failed to remove old snapshot: {}", e)
                })?;
        }

        Ok(())
    }

    /// Convert HNSW graph to snapshot format
    pub fn graph_to_snapshot(graph: &HNSWIndex, m: usize, m_l: f32) -> Result<HNSWSnapshot> {
        let num_nodes = graph.layers.iter().map(|l| l.size()).max().unwrap_or(0);
        let num_layers = graph.layers.len();

        let layers = graph
            .layers
            .iter()
            .map(|layer| {
                let mut adjacencies = layer.graph.iter()
                    .map(|(node_id, neighbors)| (*node_id, neighbors.clone()))
                    .collect::<Vec<_>>();
                adjacencies.sort_by_key(|a| a.0);

                GraphLayer { adjacencies }
            })
            .collect();

        let metadata = IndexMetadata {
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_err(|e| WaffleError::StorageError {
                    code: ErrorCode::StorageIOError,
                    message: format!("Failed to get timestamp: {}", e)
                })?
                .as_secs(),
            version: 1,
            num_nodes,
            num_layers,
            state: IndexState::Ready,
        };

        Ok(HNSWSnapshot {
            metadata,
            layers,
            entry_point: graph.entry_point,
            m,
            m_l,
        })
    }

    /// Restore HNSW graph from snapshot
    pub fn snapshot_to_graph(snapshot: &HNSWSnapshot) -> Result<HNSWIndex> {
        let mut graph = HNSWIndex::new(snapshot.m, snapshot.m_l);
        graph.entry_point = snapshot.entry_point;

        // Rebuild layers
        for (layer_idx, layer_snapshot) in snapshot.layers.iter().enumerate() {
            graph.get_or_create_layer(layer_idx);

            for (node_id, neighbors) in &layer_snapshot.adjacencies {
                // Ensure node exists
                if layer_idx >= graph.layers.len() {
                    graph.get_or_create_layer(layer_idx);
                }

                let layer = &mut graph.layers[layer_idx];
                layer.graph.insert(*node_id, neighbors.clone());
            }
        }

        Ok(graph)
    }

    /// Helper: Load state file
    fn load_state_file(path: &Path) -> Result<StateFile> {
        let mut file = File::open(path)
            .map_err(|e| WaffleError::StorageError {
                code: ErrorCode::StorageIOError,
                message: format!("Failed to open state file: {}", e)
            })?;

        let mut contents = String::new();
        file.read_to_string(&mut contents)
            .map_err(|e| WaffleError::StorageError {
                code: ErrorCode::StorageIOError,
                message: format!("Failed to read state file: {}", e)
            })?;

        serde_json::from_str(&contents)
            .map_err(|e| WaffleError::StorageError {
                code: ErrorCode::StorageIOError,
                message: format!("Failed to parse state file: {}", e)
            })
    }

    /// Helper: Save state file
    fn save_state_file(
        &self,
        state: IndexState,
        snapshot_timestamp: Option<u64>,
        snapshot_version: Option<u32>,
        wal_entries: usize,
    ) -> Result<()> {
        let state_data = StateFile {
            state,
            last_snapshot_timestamp: snapshot_timestamp,
            last_snapshot_version: snapshot_version,
            wal_entries_count: wal_entries,
        };

        let json = serde_json::to_string(&state_data)
            .map_err(|e| WaffleError::StorageError {
                code: ErrorCode::StorageIOError,
                message: format!("Failed to serialize state: {}", e)
            })?;

        // Write with fsync for durability
        let file = File::create(&self.state_file)
            .map_err(|e| WaffleError::StorageError {
                code: ErrorCode::StorageIOError,
                message: format!("Failed to create state file: {}", e)
            })?;

        let mut file = file;
        file.write_all(json.as_bytes())
            .map_err(|e| WaffleError::StorageError {
                code: ErrorCode::StorageIOError,
                message: format!("Failed to write state file: {}", e)
            })?;

        file.sync_all()
            .map_err(|e| WaffleError::StorageError {
                code: ErrorCode::StorageIOError,
                message: format!("Failed to sync state file: {}", e)
            })?;

        Ok(())
    }

    /// Helper: Get next snapshot version
    fn get_next_version(&self) -> Result<u32> {
        let snapshots = self.list_snapshots()?;
        Ok(snapshots.iter().map(|(_, v, _)| *v).max().unwrap_or(0) + 1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_index_state_transitions() {
        let temp_dir = tempfile::tempdir().unwrap();
        let mut persistence = IndexPersistence::new(temp_dir.path()).unwrap();

        assert_eq!(persistence.current_state(), IndexState::Empty);

        persistence.mark_building().unwrap();
        assert_eq!(persistence.current_state(), IndexState::Building);

        persistence.mark_ready(12345, 1).unwrap();
        assert_eq!(persistence.current_state(), IndexState::Ready);

        persistence.mark_corrupted().unwrap();
        assert_eq!(persistence.current_state(), IndexState::Corrupted);
    }

    #[test]
    fn test_state_file_persistence() {
        let temp_dir = tempfile::tempdir().unwrap();
        let mut persistence1 = IndexPersistence::new(temp_dir.path()).unwrap();

        persistence1.mark_building().unwrap();
        persistence1.mark_ready(67890, 2).unwrap();

        // Create new persistence instance from same directory
        let persistence2 = IndexPersistence::new(temp_dir.path()).unwrap();
        assert_eq!(persistence2.current_state(), IndexState::Ready);
    }
}
