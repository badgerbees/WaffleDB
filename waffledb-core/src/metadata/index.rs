use std::collections::HashMap;
use super::schema::Metadata;

/// Metadata index for fast lookups.
pub struct MetadataIndex {
    /// Inverted index: field_value -> vector_ids
    pub inverted: HashMap<String, Vec<String>>,
}

impl MetadataIndex {
    pub fn new() -> Self {
        MetadataIndex {
            inverted: HashMap::new(),
        }
    }

    /// Index a metadata entry.
    pub fn insert(&mut self, vector_id: &str, metadata: &Metadata) {
        for (key, value) in &metadata.fields {
            let index_key = format!("{}={}", key, value);
            self.inverted
                .entry(index_key)
                .or_insert_with(Vec::new)
                .push(vector_id.to_string());
        }
    }

    /// Remove a metadata entry.
    pub fn remove(&mut self, vector_id: &str, metadata: &Metadata) {
        for (key, value) in &metadata.fields {
            let index_key = format!("{}={}", key, value);
            if let Some(ids) = self.inverted.get_mut(&index_key) {
                ids.retain(|id| id != vector_id);
            }
        }
    }

    /// Search by metadata field.
    pub fn search(&self, key: &str, value: &str) -> Vec<String> {
        let index_key = format!("{}={}", key, value);
        self.inverted
            .get(&index_key)
            .map(|ids| ids.clone())
            .unwrap_or_default()
    }
}
