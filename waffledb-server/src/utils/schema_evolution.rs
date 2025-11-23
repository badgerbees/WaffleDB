/// Schema evolution and versioning support
/// 
/// Enables safe schema changes and migrations over time
/// Maintains backward compatibility with existing data

use serde::{Serialize, Deserialize};
use std::collections::HashMap;

/// Schema version tracking
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaVersion {
    /// Version number (starts at 1)
    pub version: u32,
    /// Timestamp when version was created (Unix epoch seconds)
    pub created_at: u64,
    /// Fields and their types in this version
    pub fields: HashMap<String, FieldSchema>,
    /// Migration notes from previous version
    pub migration_notes: Option<String>,
}

/// Field schema definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldSchema {
    /// Field name
    pub name: String,
    /// Field type (string, number, boolean, json)
    pub field_type: FieldType,
    /// Whether field is required
    pub required: bool,
    /// Default value if missing (optional)
    pub default: Option<String>,
    /// Whether field is indexed (for filtering)
    pub indexed: bool,
}

/// Supported field types
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FieldType {
    String,
    Number,
    Boolean,
    Json,
}

impl FieldType {
    pub fn as_str(&self) -> &'static str {
        match self {
            FieldType::String => "string",
            FieldType::Number => "number",
            FieldType::Boolean => "boolean",
            FieldType::Json => "json",
        }
    }
}

/// Schema manager for handling schema evolution
pub struct SchemaManager {
    current_version: u32,
    versions: HashMap<u32, SchemaVersion>,
}

impl SchemaManager {
    /// Create new schema manager
    pub fn new() -> Self {
        SchemaManager {
            current_version: 0,
            versions: HashMap::new(),
        }
    }
    
    /// Register a new schema version
    pub fn register_version(&mut self, version_num: u32, schema: SchemaVersion) -> Result<(), String> {
        if version_num <= self.current_version && self.current_version > 0 {
            return Err("Version number must be greater than current version".to_string());
        }
        
        self.versions.insert(version_num, schema);
        self.current_version = version_num;
        Ok(())
    }
    
    /// Get schema for specific version
    pub fn get_version(&self, version: u32) -> Option<&SchemaVersion> {
        self.versions.get(&version)
    }
    
    /// Get current schema
    pub fn current_schema(&self) -> Option<&SchemaVersion> {
        self.versions.get(&self.current_version)
    }
    
    /// Migrate value from old version to new version
    /// Applies default values for new required fields
    pub fn migrate_value(&self, old_version: u32, new_version: u32, mut value: HashMap<String, String>) 
        -> Result<HashMap<String, String>, String> {
        
        let new_schema = self.get_version(new_version)
            .ok_or("Target schema version not found")?;
        
        // Add default values for missing required fields
        for (field_name, field_schema) in &new_schema.fields {
            if !value.contains_key(field_name) {
                if field_schema.required {
                    if let Some(default) = &field_schema.default {
                        value.insert(field_name.clone(), default.clone());
                    } else {
                        return Err(format!("Missing required field: {}", field_name));
                    }
                }
            }
        }
        
        // Remove fields that no longer exist in new schema
        value.retain(|k, _| new_schema.fields.contains_key(k));
        
        Ok(value)
    }
    
    /// Get current version number
    pub fn current_version_number(&self) -> u32 {
        self.current_version
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_schema_manager_basic() {
        let mut manager = SchemaManager::new();
        
        let mut fields = HashMap::new();
        fields.insert("name".to_string(), FieldSchema {
            name: "name".to_string(),
            field_type: FieldType::String,
            required: true,
            default: None,
            indexed: true,
        });
        
        let schema = SchemaVersion {
            version: 1,
            created_at: 0,
            fields,
            migration_notes: None,
        };
        
        assert!(manager.register_version(1, schema).is_ok());
        assert_eq!(manager.current_version_number(), 1);
    }
    
    #[test]
    fn test_schema_migration() {
        let mut manager = SchemaManager::new();
        
        let mut fields_v1 = HashMap::new();
        fields_v1.insert("name".to_string(), FieldSchema {
            name: "name".to_string(),
            field_type: FieldType::String,
            required: true,
            default: None,
            indexed: true,
        });
        
        let schema_v1 = SchemaVersion {
            version: 1,
            created_at: 0,
            fields: fields_v1,
            migration_notes: None,
        };
        
        manager.register_version(1, schema_v1).unwrap();
        
        let mut fields_v2 = HashMap::new();
        fields_v2.insert("name".to_string(), FieldSchema {
            name: "name".to_string(),
            field_type: FieldType::String,
            required: true,
            default: None,
            indexed: true,
        });
        fields_v2.insert("age".to_string(), FieldSchema {
            name: "age".to_string(),
            field_type: FieldType::Number,
            required: false,
            default: Some("0".to_string()),
            indexed: true,
        });
        
        let schema_v2 = SchemaVersion {
            version: 2,
            created_at: 1,
            fields: fields_v2,
            migration_notes: Some("Added age field".to_string()),
        };
        
        manager.register_version(2, schema_v2).unwrap();
        
        let old_data = {
            let mut m = HashMap::new();
            m.insert("name".to_string(), "Alice".to_string());
            m
        };
        
        let migrated = manager.migrate_value(1, 2, old_data).unwrap();
        assert_eq!(migrated.get("name"), Some(&"Alice".to_string()));
        assert_eq!(migrated.get("age"), Some(&"0".to_string()));
    }
}
