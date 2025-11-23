/// Tenant management
/// 
/// Each tenant has isolated indexes, metadata, and quotas.

use crate::core::errors::{Result, WaffleError, ErrorCode};

/// Unique tenant identifier
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TenantId(pub String);

impl TenantId {
    /// Create new tenant ID
    pub fn new(id: String) -> Self {
        Self(id)
    }
}

/// Tenant configuration
#[derive(Debug, Clone)]
pub struct TenantConfig {
    /// Tenant ID
    pub tenant_id: TenantId,
    /// Max disk quota in bytes (None = unlimited)
    pub max_disk_bytes: Option<u64>,
    /// Namespaces this tenant owns
    pub namespaces: Vec<String>,
}

/// Tenant runtime information
#[derive(Debug)]
pub struct Tenant {
    config: TenantConfig,
    current_disk_bytes: u64,
}

impl Tenant {
    /// Create new tenant
    pub fn new(config: TenantConfig) -> Result<Self> {
        if config.tenant_id.0.is_empty() {
            return Err(WaffleError::WithCode {
                code: ErrorCode::ValidationFailed,
                message: "tenant_id cannot be empty".to_string(),
            });
        }

        Ok(Self {
            config,
            current_disk_bytes: 0,
        })
    }

    /// Get tenant ID
    pub fn tenant_id(&self) -> &TenantId {
        &self.config.tenant_id
    }

    /// Get current disk usage
    pub fn disk_usage(&self) -> u64 {
        self.current_disk_bytes
    }

    /// Check if tenant has remaining quota
    pub fn has_quota(&self, required_bytes: u64) -> bool {
        match self.config.max_disk_bytes {
            Some(max) => self.current_disk_bytes + required_bytes <= max,
            None => true,
        }
    }

    /// Allocate disk space
    pub fn allocate_disk(&mut self, bytes: u64) -> Result<()> {
        if !self.has_quota(bytes) {
            let max = self.config.max_disk_bytes.unwrap_or(0);
            return Err(WaffleError::WithCode {
                code: ErrorCode::ValidationFailed,
                message: format!(
                    "Disk quota exceeded: {} + {} > {}",
                    self.current_disk_bytes, bytes, max
                ),
            });
        }

        self.current_disk_bytes += bytes;
        Ok(())
    }

    /// Deallocate disk space
    pub fn deallocate_disk(&mut self, bytes: u64) {
        self.current_disk_bytes = self.current_disk_bytes.saturating_sub(bytes);
    }

    /// Get namespaces
    pub fn namespaces(&self) -> &[String] {
        &self.config.namespaces
    }

    /// Add namespace
    pub fn add_namespace(&mut self, namespace: String) -> Result<()> {
        if self.config.namespaces.contains(&namespace) {
            return Err(WaffleError::WithCode {
                code: ErrorCode::ValidationFailed,
                message: format!("Namespace already exists: {}", namespace),
            });
        }

        self.config.namespaces.push(namespace);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tenant_creation() {
        let config = TenantConfig {
            tenant_id: TenantId::new("tenant1".to_string()),
            max_disk_bytes: Some(1_000_000_000),
            namespaces: vec![],
        };

        let tenant = Tenant::new(config).unwrap();
        assert_eq!(tenant.tenant_id().0, "tenant1");
        assert_eq!(tenant.disk_usage(), 0);
    }

    #[test]
    fn test_tenant_disk_quota() {
        let config = TenantConfig {
            tenant_id: TenantId::new("tenant1".to_string()),
            max_disk_bytes: Some(1000),
            namespaces: vec![],
        };

        let mut tenant = Tenant::new(config).unwrap();
        assert!(tenant.has_quota(500));
        assert!(!tenant.has_quota(1500));
    }

    #[test]
    fn test_tenant_allocate_disk() {
        let config = TenantConfig {
            tenant_id: TenantId::new("tenant1".to_string()),
            max_disk_bytes: Some(1000),
            namespaces: vec![],
        };

        let mut tenant = Tenant::new(config).unwrap();
        tenant.allocate_disk(500).unwrap();
        assert_eq!(tenant.disk_usage(), 500);

        assert!(tenant.allocate_disk(600).is_err());
    }

    #[test]
    fn test_tenant_add_namespace() {
        let config = TenantConfig {
            tenant_id: TenantId::new("tenant1".to_string()),
            max_disk_bytes: None,
            namespaces: vec![],
        };

        let mut tenant = Tenant::new(config).unwrap();
        tenant.add_namespace("ns1".to_string()).unwrap();
        assert_eq!(tenant.namespaces().len(), 1);
    }
}
