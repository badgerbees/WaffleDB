/// Namespace management
/// 
/// Namespaces provide logical grouping within a tenant.

use crate::core::errors::{Result, WaffleError, ErrorCode};
use crate::multitenancy::tenant::TenantId;

/// Namespace configuration
#[derive(Debug, Clone)]
pub struct NamespaceConfig {
    /// Namespace name (unique within tenant)
    pub name: String,
    /// Owner tenant
    pub tenant_id: TenantId,
    /// Max indexes in this namespace
    pub max_indexes: Option<usize>,
}

/// Namespace runtime state
#[derive(Debug)]
pub struct Namespace {
    config: NamespaceConfig,
    index_count: usize,
}

impl Namespace {
    /// Create new namespace
    pub fn new(config: NamespaceConfig) -> Result<Self> {
        if config.name.is_empty() {
            return Err(WaffleError::WithCode {
                code: ErrorCode::ValidationFailed,
                message: "namespace name cannot be empty".to_string(),
            });
        }

        Ok(Self {
            config,
            index_count: 0,
        })
    }

    /// Get namespace name
    pub fn name(&self) -> &str {
        &self.config.name
    }

    /// Get owner tenant
    pub fn tenant_id(&self) -> &TenantId {
        &self.config.tenant_id
    }

    /// Get current index count
    pub fn index_count(&self) -> usize {
        self.index_count
    }

    /// Check if can add more indexes
    pub fn can_add_index(&self) -> bool {
        match self.config.max_indexes {
            Some(max) => self.index_count < max,
            None => true,
        }
    }

    /// Increment index count
    pub fn add_index(&mut self) -> Result<()> {
        if !self.can_add_index() {
            return Err(WaffleError::WithCode {
                code: ErrorCode::ValidationFailed,
                message: format!(
                    "Max indexes exceeded: {}",
                    self.config.max_indexes.unwrap_or(0)
                ),
            });
        }

        self.index_count += 1;
        Ok(())
    }

    /// Decrement index count
    pub fn remove_index(&mut self) {
        self.index_count = self.index_count.saturating_sub(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_namespace_creation() {
        let config = NamespaceConfig {
            name: "ns1".to_string(),
            tenant_id: TenantId::new("tenant1".to_string()),
            max_indexes: Some(100),
        };

        let ns = Namespace::new(config).unwrap();
        assert_eq!(ns.name(), "ns1");
        assert_eq!(ns.index_count(), 0);
    }

    #[test]
    fn test_namespace_add_index() {
        let config = NamespaceConfig {
            name: "ns1".to_string(),
            tenant_id: TenantId::new("tenant1".to_string()),
            max_indexes: Some(2),
        };

        let mut ns = Namespace::new(config).unwrap();
        ns.add_index().unwrap();
        ns.add_index().unwrap();

        assert!(ns.add_index().is_err());
    }
}
