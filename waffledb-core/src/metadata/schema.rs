use std::collections::HashMap;

/// Metadata schema for vectors.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Metadata {
    pub fields: HashMap<String, String>,
}

impl Metadata {
    /// Create empty metadata.
    pub fn new() -> Self {
        Metadata {
            fields: HashMap::new(),
        }
    }

    /// Insert a field.
    pub fn insert(&mut self, key: String, value: String) {
        self.fields.insert(key, value);
    }

    /// Get a field.
    pub fn get(&self, key: &str) -> Option<&str> {
        self.fields.get(key).map(|s| s.as_str())
    }

    /// Remove a field.
    pub fn remove(&mut self, key: &str) -> Option<String> {
        self.fields.remove(key)
    }

    /// Merge metadata.
    pub fn merge(&mut self, other: &Metadata) {
        for (k, v) in &other.fields {
            self.fields.insert(k.clone(), v.clone());
        }
    }

    /// Convert to JSON string.
    pub fn to_json_string(&self) -> String {
        serde_json::to_string(&self.fields).unwrap_or_default()
    }
}
