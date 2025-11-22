use crate::engine::state::EngineState;
use crate::api::models::{UpdateMetadataRequest, UpdateMetadataResponse};

/// Handle metadata update request (deprecated - use update.rs instead)
pub async fn handle_update_metadata(
    engine: &EngineState,
    collection: String,
    req: UpdateMetadataRequest,
) -> waffledb_core::Result<UpdateMetadataResponse> {
    // Verify collection exists
    engine.get_collection(&collection)?;

    Ok(UpdateMetadataResponse {
        status: "ok".to_string(),
        id: req.id,
    })
}