/// Schema validation system for WaffleDB
/// 
/// Ensures type safety and prevents dimension mismatches.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use waffledb_core::core::errors::{Result, WaffleError, ErrorCode};

/// Supported vector types
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum VectorType {
    Dense,
    Sparse,
    Text,
}

/// Compression method for vectors
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum CompressionType {
    None,
    PQ8,
    PQ4,
}

/// Vector field definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorFieldSchema {
    pub vector_type: VectorType,
    pub dimension: Option<u32>,
    pub compression: Option<CompressionType>,
}

impl VectorFieldSchema {
    /// Validate vector field definition
    pub fn validate(&self) -> Result<()> {
        match self.vector_type {
            VectorType::Dense => {
                if self.dimension.is_none() || self.dimension == Some(0) {
                    return Err(WaffleError::WithCode {
                        code: ErrorCode::ValidationFailed,
                        message: "Dense vectors must specify dimension > 0".to_string(),
                    });
                }
            }
            VectorType::Sparse => {
                // Sparse vectors don't need dimension
            }
            VectorType::Text => {
                // Text vectors validated during encoding
            }
        }
        Ok(())
    }
}

/// Collection schema defining all fields
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CollectionSchema {
    pub name: String,
    pub vectors: HashMap<String, VectorFieldSchema>,
    pub metadata_fields: Option<HashMap<String, String>>,
}

impl CollectionSchema {
    /// Create new collection schema
    pub fn new(name: String) -> Self {
        Self {
            name,
            vectors: HashMap::new(),
            metadata_fields: None,
        }
    }

    /// Add vector field to schema
    pub fn add_vector_field(mut self, name: String, field: VectorFieldSchema) -> Result<Self> {
        field.validate()?;
        self.vectors.insert(name, field);
        Ok(self)
    }

    /// Validate entire schema
    pub fn validate(&self) -> Result<()> {
        if self.name.is_empty() {
            return Err(WaffleError::WithCode {
                code: ErrorCode::ValidationFailed,
                message: "Collection name cannot be empty".to_string(),
            });
        }

        if self.vectors.is_empty() {
            return Err(WaffleError::WithCode {
                code: ErrorCode::ValidationFailed,
                message: "Collection must have at least one vector field".to_string(),
            });
        }

        for (field_name, field_schema) in &self.vectors {
            field_schema.validate().map_err(|_| WaffleError::WithCode {
                code: ErrorCode::ValidationFailed,
                message: format!("Invalid schema for field '{}': dimension required for dense vectors", field_name),
            })?;
        }

        Ok(())
    }
}

/// Schema registry for managing collection schemas
pub struct SchemaRegistry {
    schemas: HashMap<String, CollectionSchema>,
}

impl SchemaRegistry {
    pub fn new() -> Self {
        Self {
            schemas: HashMap::new(),
        }
    }

    /// Register a new collection schema
    pub fn register(&mut self, schema: CollectionSchema) -> Result<()> {
        schema.validate()?;
        self.schemas.insert(schema.name.clone(), schema);
        Ok(())
    }

    /// Get schema for collection
    pub fn get(&self, collection: &str) -> Option<&CollectionSchema> {
        self.schemas.get(collection)
    }

    /// Validate vector dimensions match schema
    pub fn validate_vector_dims(
        &self,
        collection: &str,
        vector_name: &str,
        dim: usize,
    ) -> Result<()> {
        let schema = self.get(collection).ok_or_else(|| WaffleError::WithCode {
            code: ErrorCode::ValidationFailed,
            message: format!("Schema not found for collection '{}'", collection),
        })?;

        let field = schema.vectors.get(vector_name).ok_or_else(|| WaffleError::WithCode {
            code: ErrorCode::ValidationFailed,
            message: format!("Vector field '{}' not found in schema", vector_name),
        })?;

        if let Some(expected_dim) = field.dimension {
            if dim != expected_dim as usize {
                return Err(WaffleError::WithCode {
                    code: ErrorCode::ValidationFailed,
                    message: format!(
                        "Dimension mismatch for field '{}': expected {}, got {}",
                        vector_name, expected_dim, dim
                    ),
                });
            }
        }

        Ok(())
    }
}

impl Default for SchemaRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schema_creation() {
        let schema = CollectionSchema::new("test_collection".to_string());
        assert_eq!(schema.name, "test_collection");
        assert!(schema.vectors.is_empty());
    }

    #[test]
    fn test_dense_field_validation() {
        let field = VectorFieldSchema {
            vector_type: VectorType::Dense,
            dimension: Some(384),
            compression: Some(CompressionType::PQ8),
        };
        assert!(field.validate().is_ok());
    }

    #[test]
    fn test_dense_field_missing_dimension() {
        let field = VectorFieldSchema {
            vector_type: VectorType::Dense,
            dimension: None,
            compression: None,
        };
        assert!(field.validate().is_err());
    }

    #[test]
    fn test_schema_add_vector_field() {
        let schema = CollectionSchema::new("test".to_string())
            .add_vector_field(
                "embedding".to_string(),
                VectorFieldSchema {
                    vector_type: VectorType::Dense,
                    dimension: Some(384),
                    compression: None,
                },
            )
            .unwrap();

        assert_eq!(schema.vectors.len(), 1);
        assert!(schema.vectors.contains_key("embedding"));
    }

    #[test]
    fn test_schema_registry() {
        let mut registry = SchemaRegistry::new();
        let schema = CollectionSchema::new("test".to_string())
            .add_vector_field(
                "emb".to_string(),
                VectorFieldSchema {
                    vector_type: VectorType::Dense,
                    dimension: Some(384),
                    compression: None,
                },
            )
            .unwrap();

        assert!(registry.register(schema).is_ok());
        assert!(registry.get("test").is_some());
    }

    #[test]
    fn test_dimension_validation() {
        let mut registry = SchemaRegistry::new();
        let schema = CollectionSchema::new("test".to_string())
            .add_vector_field(
                "emb".to_string(),
                VectorFieldSchema {
                    vector_type: VectorType::Dense,
                    dimension: Some(384),
                    compression: None,
                },
            )
            .unwrap();

        registry.register(schema).unwrap();
        assert!(registry.validate_vector_dims("test", "emb", 384).is_ok());
        assert!(registry.validate_vector_dims("test", "emb", 512).is_err());
    }
}
