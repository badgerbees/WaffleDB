// Shared utility functions for WaffleDB Core

/// Generate a unique ID.
pub fn generate_id() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    format!("vec_{}", duration.as_nanos())
}
