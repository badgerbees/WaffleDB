use crate::engine::state::EngineState;
use crate::api::models::*;
use tracing::{info, error, instrument};
use crate::snapshot::SnapshotManager;

thread_local! {
    static SNAPSHOT_MANAGER: SnapshotManager = SnapshotManager::new("./data").expect("snapshot manager init");
}

/// Handle snapshot creation with real persistence
#[instrument(skip(engine))]
pub async fn handle_create_snapshot(
    engine: &EngineState,
    name: String,
) -> Result<SnapshotResponse, String> {
    match engine.get_collection(&name) {
        Ok(metadata) => {
            SNAPSHOT_MANAGER.with(|mgr| {
                match mgr.create_snapshot(&name, metadata.vector_count, metadata.dimension) {
                    Ok(snap_info) => {
                        info!(
                            collection_name = %name,
                            snapshot_id = %snap_info.snapshot_id,
                            vector_count = metadata.vector_count,
                            dimension = metadata.dimension,
                            size_bytes = snap_info.size_bytes,
                            path = %snap_info.path.display(),
                            "Snapshot created successfully"
                        );

                        Ok(SnapshotResponse {
                            snapshot_id: snap_info.snapshot_id,
                            timestamp: snap_info.timestamp,
                            vector_count: snap_info.vector_count,
                            size_bytes: snap_info.size_bytes,
                        })
                    }
                    Err(e) => {
                        error!(
                            collection_name = %name,
                            error = %e,
                            "Snapshot creation failed"
                        );
                        Err(format!("Failed to create snapshot: {}", e))
                    }
                }
            })
        }
        Err(e) => {
            error!(
                collection_name = %name,
                error = %e,
                "Collection not found for snapshot"
            );
            Err(format!("{}", e))
        }
    }
}
