use crate::engine::state::EngineState;
use crate::api::models::{DeleteRequest, DeleteResponse};

/// Handle delete request with tenant isolation
pub async fn handle_delete(
    engine: &EngineState,
    tenant_id: &str,
    collection: String,
    req: DeleteRequest,
) -> waffledb_core::Result<DeleteResponse> {
    engine.delete_vector_for_tenant(tenant_id, &collection, &req.id)?;

    Ok(DeleteResponse {
        status: "ok".to_string(),
    })
}
