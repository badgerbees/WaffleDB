use crate::engine::state::EngineState;
use crate::api::models::{DeleteRequest, DeleteResponse};

/// Handle delete request
pub async fn handle_delete(
    engine: &EngineState,
    collection: String,
    req: DeleteRequest,
) -> waffledb_core::Result<DeleteResponse> {
    engine.delete(&collection, &req.id)?;

    Ok(DeleteResponse {
        status: "ok".to_string(),
    })
}
