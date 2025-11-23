/// Quota management for multi-tenancy
/// 
/// Tracks and enforces per-tenant quotas.

use std::collections::HashMap;
use crate::multitenancy::tenant::TenantId;

/// Disk quota for a tenant
#[derive(Debug, Clone)]
pub struct DiskQuota {
    pub tenant_id: TenantId,
    pub max_bytes: u64,
    pub used_bytes: u64,
}

impl DiskQuota {
    /// Check if quota allows allocation
    pub fn can_allocate(&self, bytes: u64) -> bool {
        self.used_bytes + bytes <= self.max_bytes
    }

    /// Get remaining quota
    pub fn remaining_bytes(&self) -> u64 {
        self.max_bytes.saturating_sub(self.used_bytes)
    }
}

/// Quota manager for all tenants
#[derive(Debug)]
pub struct QuotaManager {
    quotas: HashMap<TenantId, DiskQuota>,
}

impl QuotaManager {
    /// Create new quota manager
    pub fn new() -> Self {
        Self {
            quotas: HashMap::new(),
        }
    }

    /// Register tenant quota
    pub fn register_quota(&mut self, tenant_id: TenantId, max_bytes: u64) {
        self.quotas.insert(
            tenant_id.clone(),
            DiskQuota {
                tenant_id,
                max_bytes,
                used_bytes: 0,
            },
        );
    }

    /// Get quota for tenant
    pub fn get_quota(&self, tenant_id: &TenantId) -> Option<&DiskQuota> {
        self.quotas.get(tenant_id)
    }

    /// Update quota usage
    pub fn update_usage(&mut self, tenant_id: &TenantId, bytes_delta: i64) -> bool {
        if let Some(quota) = self.quotas.get_mut(tenant_id) {
            if bytes_delta > 0 {
                let required = bytes_delta as u64;
                if quota.can_allocate(required) {
                    quota.used_bytes += required;
                    return true;
                }
                return false;
            } else {
                quota.used_bytes = quota.used_bytes.saturating_sub((-bytes_delta) as u64);
                return true;
            }
        }
        false
    }

    /// Get total usage across all tenants
    pub fn total_usage(&self) -> u64 {
        self.quotas.values().map(|q| q.used_bytes).sum()
    }
}

impl Default for QuotaManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_disk_quota_can_allocate() {
        let quota = DiskQuota {
            tenant_id: TenantId::new("tenant1".to_string()),
            max_bytes: 1000,
            used_bytes: 600,
        };

        assert!(quota.can_allocate(400));
        assert!(!quota.can_allocate(500));
    }

    #[test]
    fn test_quota_manager_register() {
        let mut manager = QuotaManager::new();
        let tenant_id = TenantId::new("tenant1".to_string());

        manager.register_quota(tenant_id.clone(), 1000);
        assert!(manager.get_quota(&tenant_id).is_some());
    }

    #[test]
    fn test_quota_manager_update_usage() {
        let mut manager = QuotaManager::new();
        let tenant_id = TenantId::new("tenant1".to_string());

        manager.register_quota(tenant_id.clone(), 1000);
        assert!(manager.update_usage(&tenant_id, 500));
        assert_eq!(manager.get_quota(&tenant_id).unwrap().used_bytes, 500);

        assert!(!manager.update_usage(&tenant_id, 600));
    }
}
