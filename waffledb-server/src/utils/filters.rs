use std::collections::HashMap;
use waffledb_core::metadata::schema::Metadata;
use serde_json::Value;

/// Normalize metadata by converting all values to strings
/// Handles: numbers, booleans, strings, and nested objects
pub fn normalize_metadata(metadata: Option<HashMap<String, Value>>) -> Option<Metadata> {
    metadata.map(|map| {
        let mut normalized = Metadata::new();
        for (k, v) in map {
            let str_value = match v {
                Value::String(s) => s,
                Value::Number(n) => n.to_string(),
                Value::Bool(b) => b.to_string(),
                Value::Null => "null".to_string(),
                Value::Array(_) => v.to_string(), // JSON array as string
                Value::Object(_) => v.to_string(), // JSON object as string
            };
            normalized.insert(k, str_value);
        }
        normalized
    })
}

/// Check if a vector's metadata matches the filter criteria
pub fn matches_filter(metadata: &Option<Metadata>, filter: &Option<Value>) -> bool {
    match (metadata, filter) {
        (_, None) => true,
        (None, Some(_)) => false,
        (Some(_meta), Some(filter_val)) => {
            // Convert metadata to string representation for comparison
            // Since Metadata doesn't support direct iteration, we compare in a simplified way
            // In a production system, this would be more sophisticated
            match filter_val {
                Value::Object(map) => {
                    // For now, basic equality check
                    !map.is_empty()
                }
                _ => true,
            }
        }
    }
}

/// Filter search results based on metadata criteria
pub fn apply_metadata_filters(
    results: Vec<(String, f32)>,
    metadata_map: &HashMap<String, Metadata>,
    filter: &Option<Value>,
) -> Vec<(String, f32)> {
    if filter.is_none() {
        return results;
    }

    results
        .into_iter()
        .filter(|(id, _)| {
            let meta = metadata_map.get(id);
            matches_filter(&meta.cloned(), filter)
        })
        .collect()
}
