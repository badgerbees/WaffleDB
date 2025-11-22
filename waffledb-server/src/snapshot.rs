use std::path::{Path, PathBuf};
use std::fs;
use std::io::{self, Write};
use tracing::info;

pub struct SnapshotManager {
    snapshot_dir: PathBuf,
}

impl SnapshotManager {
    pub fn new(base_dir: &str) -> io::Result<Self> {
        let snapshot_dir = Path::new(base_dir).join("snapshots");
        fs::create_dir_all(&snapshot_dir)?;
        
        info!(path = %snapshot_dir.display(), "Initialized snapshot manager");
        
        Ok(SnapshotManager { snapshot_dir })
    }

    pub fn create_snapshot(
        &self,
        collection_name: &str,
        vector_count: usize,
        dimension: usize,
    ) -> io::Result<SnapshotInfo> {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let snapshot_id = format!("{}_{:?}_{}", collection_name, dimension, timestamp);
        let collection_dir = self.snapshot_dir.join(&snapshot_id);
        
        fs::create_dir_all(&collection_dir)?;

        // Write metadata
        let metadata_path = collection_dir.join("metadata.json");
        let metadata = serde_json::json!({
            "collection": collection_name,
            "vector_count": vector_count,
            "dimension": dimension,
            "timestamp": timestamp,
            "snapshot_id": snapshot_id,
        });

        let mut file = fs::File::create(&metadata_path)?;
        file.write_all(metadata.to_string().as_bytes())?;
        file.sync_all()?;

        let size_bytes = (vector_count * dimension * 4) as u64;

        info!(
            collection_name,
            snapshot_id = %snapshot_id,
            vector_count,
            dimension,
            size_bytes,
            path = %collection_dir.display(),
            "Snapshot created"
        );

        Ok(SnapshotInfo {
            snapshot_id,
            path: collection_dir,
            timestamp,
            vector_count,
            size_bytes,
        })
    }

    pub fn list_snapshots(&self) -> io::Result<Vec<SnapshotInfo>> {
        let mut snapshots = vec![];
        
        for entry in fs::read_dir(&self.snapshot_dir)? {
            let entry = entry?;
            let path = entry.path();
            
            if path.is_dir() {
                if let Ok(metadata_str) = fs::read_to_string(path.join("metadata.json")) {
                    if let Ok(metadata) = serde_json::from_str::<serde_json::Value>(&metadata_str) {
                        snapshots.push(SnapshotInfo {
                            snapshot_id: metadata["snapshot_id"]
                                .as_str()
                                .unwrap_or("unknown")
                                .to_string(),
                            path: path.clone(),
                            timestamp: metadata["timestamp"]
                                .as_u64()
                                .unwrap_or(0),
                            vector_count: metadata["vector_count"]
                                .as_u64()
                                .unwrap_or(0) as usize,
                            size_bytes: (metadata["vector_count"].as_u64().unwrap_or(0)
                                * metadata["dimension"].as_u64().unwrap_or(0)
                                * 4) as u64,
                        });
                    }
                }
            }
        }
        
        Ok(snapshots)
    }

    pub fn delete_snapshot(&self, snapshot_id: &str) -> io::Result<()> {
        let snapshot_path = self.snapshot_dir.join(snapshot_id);
        
        if snapshot_path.exists() {
            fs::remove_dir_all(&snapshot_path)?;
            info!(snapshot_id, "Snapshot deleted");
        }
        
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct SnapshotInfo {
    pub snapshot_id: String,
    pub path: PathBuf,
    pub timestamp: u64,
    pub vector_count: usize,
    pub size_bytes: u64,
}
