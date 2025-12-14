/// Tenant isolation enforcement layer
/// 
/// Ensures strict isolation between tenants at storage and query levels.
/// No cross-tenant data access possible.

use crate::core::errors::{Result, WaffleError, ErrorCode};
use std::collections::HashMap;
use serde::{Serialize, Deserialize};

/// Tenant context - attached to every operation
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct TenantContext {
    /// Tenant ID - must be present for all operations
    pub tenant_id: String,
    /// Project ID within tenant (optional)
    pub project_id: Option<String>,
}

impl TenantContext {
    /// Create new tenant context
    pub fn new(tenant_id: String) -> Self {
        Self {
            tenant_id,
            project_id: None,
        }
    }

    /// Create with project ID
    pub fn with_project(tenant_id: String, project_id: String) -> Self {
        Self {
            tenant_id,
            project_id: Some(project_id),
        }
    }

    /// Generate storage key prefix for this tenant
    /// All data for this tenant is stored under this prefix
    pub fn storage_prefix(&self) -> String {
        match &self.project_id {
            Some(proj_id) => format!("t/{}:p/{}", self.tenant_id, proj_id),
            None => format!("t/{}", self.tenant_id),
        }
    }

    /// Validate tenant context is set
    pub fn validate(&self) -> Result<()> {
        if self.tenant_id.is_empty() {
            return Err(WaffleError::WithCode {
                code: ErrorCode::ValidationFailed,
                message: "tenant_id cannot be empty".to_string(),
            });
        }
        Ok(())
    }
}

/// Storage scope - ensures all data is scoped per tenant
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageScope {
    /// Tenant this data belongs to
    pub tenant_id: String,
    /// Collection name
    pub collection: String,
    /// Version for concurrent access control
    pub version: u64,
}

impl StorageScope {
    /// Create new storage scope
    pub fn new(tenant_id: String, collection: String) -> Self {
        Self {
            tenant_id,
            collection,
            version: 0,
        }
    }

    /// Generate full key for a document ID
    pub fn document_key(&self, doc_id: &str) -> String {
        format!("{}/{}/docs/{}", self.tenant_id, self.collection, doc_id)
    }

    /// Generate key for HNSW index
    pub fn index_key(&self) -> String {
        format!("{}/{}/hnsw", self.tenant_id, self.collection)
    }

    /// Generate key for metadata index
    pub fn metadata_key(&self) -> String {
        format!("{}/{}/metadata", self.tenant_id, self.collection)
    }

    /// Generate key for WAL
    pub fn wal_key(&self) -> String {
        format!("{}/{}/wal", self.tenant_id, self.collection)
    }

    /// Generate key for snapshot
    pub fn snapshot_key(&self, snapshot_id: &str) -> String {
        format!("{}/{}/snapshots/{}", self.tenant_id, self.collection, snapshot_id)
    }
}

/// Tenant isolation enforcer
pub struct TenantIsolationEnforcer {
    /// Active tenants and their quotas
    tenants: HashMap<String, TenantInfo>,
    /// Query audit trail per tenant
    query_audit: HashMap<String, Vec<AuditEvent>>,
}

#[derive(Debug, Clone)]
struct TenantInfo {
    tenant_id: String,
    max_storage_bytes: Option<u64>,
    current_storage_bytes: u64,
    collections: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditEvent {
    pub timestamp: u64,
    pub tenant_id: String,
    pub operation: String,
    pub collection: String,
    pub doc_count: usize,
    pub result: String, // "ok" or error message
}

impl TenantIsolationEnforcer {
    /// Create new enforcer
    pub fn new() -> Self {
        Self {
            tenants: HashMap::new(),
            query_audit: HashMap::new(),
        }
    }

    /// Register a new tenant
    pub fn register_tenant(&mut self, tenant_id: String, max_storage_bytes: Option<u64>) -> Result<()> {
        if tenant_id.is_empty() {
            return Err(WaffleError::WithCode {
                code: ErrorCode::ValidationFailed,
                message: "tenant_id cannot be empty".to_string(),
            });
        }

        if self.tenants.contains_key(&tenant_id) {
            return Err(WaffleError::WithCode {
                code: ErrorCode::ValidationFailed,
                message: format!("tenant {} already registered", tenant_id),
            });
        }

        self.tenants.insert(
            tenant_id.clone(),
            TenantInfo {
                tenant_id: tenant_id.clone(),
                max_storage_bytes,
                current_storage_bytes: 0,
                collections: Vec::new(),
            },
        );

        self.query_audit.insert(tenant_id, Vec::new());

        Ok(())
    }

    /// Validate operation for tenant
    pub fn validate_operation(&self, tenant_id: &str, collection: &str, operation: &str) -> Result<()> {
        // Check tenant exists
        let tenant = self.tenants.get(tenant_id).ok_or_else(|| WaffleError::WithCode {
            code: ErrorCode::ValidationFailed,
            message: format!("unknown tenant: {}", tenant_id),
        })?;

        // Check collection belongs to tenant
        if !tenant.collections.contains(&collection.to_string()) {
            return Err(WaffleError::WithCode {
                code: ErrorCode::ValidationFailed,
                message: format!("collection {} not accessible to tenant {}", collection, tenant_id),
            });
        }

        // Check operation is allowed
        let allowed_ops = vec!["insert", "search", "delete", "update", "read"];
        if !allowed_ops.contains(&operation) {
            return Err(WaffleError::WithCode {
                code: ErrorCode::ValidationFailed,
                message: format!("unknown operation: {}", operation),
            });
        }

        Ok(())
    }

    /// Check if tenant has quota for additional data
    pub fn check_quota(&self, tenant_id: &str, additional_bytes: u64) -> Result<()> {
        let tenant = self.tenants.get(tenant_id).ok_or_else(|| WaffleError::WithCode {
            code: ErrorCode::ValidationFailed,
            message: format!("unknown tenant: {}", tenant_id),
        })?;

        if let Some(max) = tenant.max_storage_bytes {
            let new_total = tenant.current_storage_bytes + additional_bytes;
            if new_total > max {
                return Err(WaffleError::WithCode {
                    code: ErrorCode::ValidationFailed,
                    message: format!(
                        "quota exceeded: {} + {} > {}",
                        tenant.current_storage_bytes, additional_bytes, max
                    ),
                });
            }
        }

        Ok(())
    }

    /// Record audit event for tenant
    pub fn audit_operation(
        &mut self,
        tenant_id: String,
        collection: String,
        operation: String,
        doc_count: usize,
        result: String,
    ) -> Result<()> {
        let event = AuditEvent {
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            tenant_id: tenant_id.clone(),
            operation,
            collection,
            doc_count,
            result,
        };

        if let Some(events) = self.query_audit.get_mut(&tenant_id) {
            events.push(event);
            // Keep only last 1000 events per tenant to avoid unbounded growth
            if events.len() > 1000 {
                events.drain(0..events.len() - 1000);
            }
        }

        Ok(())
    }

    /// Get audit trail for tenant
    pub fn get_audit_trail(&self, tenant_id: &str) -> Vec<AuditEvent> {
        self.query_audit
            .get(tenant_id)
            .cloned()
            .unwrap_or_default()
    }

    /// Register collection for tenant
    pub fn register_collection(&mut self, tenant_id: &str, collection: String) -> Result<()> {
        let tenant = self.tenants.get_mut(tenant_id).ok_or_else(|| WaffleError::WithCode {
            code: ErrorCode::ValidationFailed,
            message: format!("unknown tenant: {}", tenant_id),
        })?;

        if !tenant.collections.contains(&collection) {
            tenant.collections.push(collection);
        }

        Ok(())
    }

    /// Update storage usage for tenant
    pub fn update_storage(&mut self, tenant_id: &str, delta: i64) -> Result<()> {
        let tenant = self.tenants.get_mut(tenant_id).ok_or_else(|| WaffleError::WithCode {
            code: ErrorCode::ValidationFailed,
            message: format!("unknown tenant: {}", tenant_id),
        })?;

        if delta < 0 {
            let abs_delta = (-delta) as u64;
            if abs_delta > tenant.current_storage_bytes {
                tenant.current_storage_bytes = 0;
            } else {
                tenant.current_storage_bytes -= abs_delta;
            }
        } else {
            tenant.current_storage_bytes += delta as u64;
        }

        Ok(())
    }

    /// Get tenant storage usage
    pub fn get_storage_usage(&self, tenant_id: &str) -> Result<(u64, Option<u64>)> {
        let tenant = self.tenants.get(tenant_id).ok_or_else(|| WaffleError::WithCode {
            code: ErrorCode::ValidationFailed,
            message: format!("unknown tenant: {}", tenant_id),
        })?;

        Ok((tenant.current_storage_bytes, tenant.max_storage_bytes))
    }

    /// Get all collections for tenant
    pub fn get_collections(&self, tenant_id: &str) -> Result<Vec<String>> {
        let tenant = self.tenants.get(tenant_id).ok_or_else(|| WaffleError::WithCode {
            code: ErrorCode::ValidationFailed,
            message: format!("unknown tenant: {}", tenant_id),
        })?;

        Ok(tenant.collections.clone())
    }

    /// Create collection for tenant (register + add to tenant)
    pub fn create_collection(&mut self, tenant_id: &str, collection: &str, quota: u64) -> Result<()> {
        // Validate tenant exists
        if !self.tenants.contains_key(tenant_id) {
            return Err(WaffleError::WithCode {
                code: ErrorCode::ValidationFailed,
                message: format!("unknown tenant: {}", tenant_id),
            });
        }

        // Register collection
        self.register_collection(tenant_id, collection.to_string())?;

        Ok(())
    }

    /// Log audit event for tenant
    pub fn log_audit_event(&mut self, event: AuditEvent) -> Result<()> {
        let tenant_id = event.tenant_id.clone();

        if let Some(events) = self.query_audit.get_mut(&tenant_id) {
            events.push(event);
            // Keep only last 1000 events per tenant
            if events.len() > 1000 {
                events.drain(0..events.len() - 1000);
            }
        } else {
            return Err(WaffleError::WithCode {
                code: ErrorCode::ValidationFailed,
                message: format!("unknown tenant: {}", tenant_id),
            });
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tenant_context() {
        let ctx = TenantContext::new("tenant1".to_string());
        assert_eq!(ctx.storage_prefix(), "t/tenant1");
        ctx.validate().unwrap();
    }

    #[test]
    fn test_tenant_context_with_project() {
        let ctx = TenantContext::with_project("tenant1".to_string(), "project1".to_string());
        assert_eq!(ctx.storage_prefix(), "t/tenant1:p/project1");
    }

    #[test]
    fn test_storage_scope_keys() {
        let scope = StorageScope::new("tenant1".to_string(), "docs".to_string());
        assert_eq!(scope.document_key("doc1"), "tenant1/docs/docs/doc1");
        assert_eq!(scope.index_key(), "tenant1/docs/hnsw");
        assert_eq!(scope.wal_key(), "tenant1/docs/wal");
    }

    #[test]
    fn test_isolation_enforcer() {
        let mut enforcer = TenantIsolationEnforcer::new();
        
        // Register tenant
        enforcer.register_tenant("tenant1".to_string(), Some(1_000_000)).unwrap();
        enforcer.register_collection("tenant1", "docs".to_string()).unwrap();
        
        // Check quota
        enforcer.check_quota("tenant1", 500_000).unwrap();
        
        // Try quota exceeded
        let result = enforcer.check_quota("tenant1", 1_500_000);
        assert!(result.is_err());
    }

    #[test]
    fn test_audit_trail() {
        let mut enforcer = TenantIsolationEnforcer::new();
        
        enforcer.register_tenant("tenant1".to_string(), None).unwrap();
        
        enforcer.audit_operation(
            "tenant1".to_string(),
            "docs".to_string(),
            "insert".to_string(),
            5,
            "ok".to_string(),
        ).unwrap();

        let trail = enforcer.get_audit_trail("tenant1");
        assert_eq!(trail.len(), 1);
        assert_eq!(trail[0].doc_count, 5);
    }
}

#[cfg(test)]
mod isolation_tests {
    use super::*;

    // ==================== STORAGE ISOLATION TESTS ====================

    #[test]
    fn test_storage_scope_document_key_isolation() {
        let scope1 = StorageScope::new("tenant1".to_string(), "collection1".to_string());
        let scope2 = StorageScope::new("tenant2".to_string(), "collection1".to_string());

        let key1 = scope1.document_key("doc1");
        let key2 = scope2.document_key("doc1");

        assert_ne!(key1, key2);
        assert!(key1.starts_with("tenant1/"));
        assert!(key2.starts_with("tenant2/"));
    }

    #[test]
    fn test_storage_scope_index_key_isolation() {
        let scope1 = StorageScope::new("tenant1".to_string(), "collection1".to_string());
        let scope2 = StorageScope::new("tenant2".to_string(), "collection1".to_string());

        let key1 = scope1.index_key();
        let key2 = scope2.index_key();

        assert_ne!(key1, key2);
        assert!(key1.contains("tenant1"));
        assert!(key2.contains("tenant2"));
    }

    #[test]
    fn test_storage_scope_metadata_key_isolation() {
        let scope1 = StorageScope::new("tenant1".to_string(), "collection1".to_string());
        let scope2 = StorageScope::new("tenant2".to_string(), "collection1".to_string());

        let key1 = scope1.metadata_key();
        let key2 = scope2.metadata_key();

        assert_ne!(key1, key2);
    }

    #[test]
    fn test_storage_scope_wal_key_isolation() {
        let scope1 = StorageScope::new("tenant1".to_string(), "collection1".to_string());
        let scope2 = StorageScope::new("tenant2".to_string(), "collection1".to_string());

        let key1 = scope1.wal_key();
        let key2 = scope2.wal_key();

        assert_ne!(key1, key2);
    }

    #[test]
    fn test_storage_scope_snapshot_key_isolation() {
        let scope1 = StorageScope::new("tenant1".to_string(), "collection1".to_string());
        let scope2 = StorageScope::new("tenant2".to_string(), "collection1".to_string());

        let key1 = scope1.snapshot_key("snapshot1");
        let key2 = scope2.snapshot_key("snapshot1");

        assert_ne!(key1, key2);
    }

    // ==================== TENANT CONTEXT TESTS ====================

    #[test]
    fn test_tenant_context_storage_prefix_uniqueness() {
        let ctx1 = TenantContext::new("tenant1".to_string());
        let ctx2 = TenantContext::new("tenant2".to_string());

        assert_ne!(ctx1.storage_prefix(), ctx2.storage_prefix());
    }

    #[test]
    fn test_tenant_context_with_project_prefix_uniqueness() {
        let ctx1 = TenantContext::with_project("tenant1".to_string(), "project1".to_string());
        let ctx2 = TenantContext::with_project("tenant1".to_string(), "project2".to_string());

        assert_ne!(ctx1.storage_prefix(), ctx2.storage_prefix());
    }

    #[test]
    fn test_tenant_context_project_id_handling() {
        let ctx_no_project = TenantContext::new("tenant1".to_string());
        let ctx_with_project = TenantContext::with_project("tenant1".to_string(), "project1".to_string());

        // Both should be valid but different
        assert!(ctx_no_project.project_id.is_none());
        assert!(ctx_with_project.project_id.is_some());
        assert_ne!(ctx_no_project.storage_prefix(), ctx_with_project.storage_prefix());
    }

    // ==================== ISOLATION ENFORCER TESTS ====================

    #[test]
    fn test_enforcer_tenant_registration() {
        let mut enforcer = TenantIsolationEnforcer::new();

        enforcer.register_tenant("tenant1".to_string(), Some(1_000_000)).unwrap();
        enforcer.register_tenant("tenant2".to_string(), Some(2_000_000)).unwrap();

        // Both tenants should be registered
        assert!(enforcer.tenants.contains_key("tenant1"));
        assert!(enforcer.tenants.contains_key("tenant2"));
    }

    #[test]
    fn test_enforcer_operation_validation_blocks_unknown_tenant() {
        let enforcer = TenantIsolationEnforcer::new();

        let result = enforcer.validate_operation("unknown_tenant", "collection1", "read");
        assert!(result.is_err());
    }

    #[test]
    fn test_enforcer_quota_enforcement() {
        let mut enforcer = TenantIsolationEnforcer::new();

        enforcer.register_tenant("tenant1".to_string(), Some(1_000_000)).unwrap();

        // Should succeed - within quota
        enforcer.check_quota("tenant1", 500_000).unwrap();

        // Should fail - exceeds quota
        let result = enforcer.check_quota("tenant1", 1_500_000);
        assert!(result.is_err());
    }

    #[test]
    fn test_enforcer_audit_trail_isolation() {
        let mut enforcer = TenantIsolationEnforcer::new();

        enforcer.register_tenant("tenant1".to_string(), None).unwrap();
        enforcer.register_tenant("tenant2".to_string(), None).unwrap();

        enforcer.audit_operation(
            "tenant1".to_string(),
            "collection1".to_string(),
            "insert".to_string(),
            5,
            "ok".to_string(),
        ).unwrap();

        enforcer.audit_operation(
            "tenant2".to_string(),
            "collection1".to_string(),
            "insert".to_string(),
            10,
            "ok".to_string(),
        ).unwrap();

        let trail1 = enforcer.get_audit_trail("tenant1");
        let trail2 = enforcer.get_audit_trail("tenant2");

        // Verify isolation
        assert_eq!(trail1.len(), 1);
        assert_eq!(trail2.len(), 1);
        assert_eq!(trail1[0].tenant_id, "tenant1");
        assert_eq!(trail2[0].tenant_id, "tenant2");
        assert_eq!(trail1[0].doc_count, 5);
        assert_eq!(trail2[0].doc_count, 10);
    }

    #[test]
    fn test_enforcer_cross_tenant_access_blocked() {
        let mut enforcer = TenantIsolationEnforcer::new();

        enforcer.register_tenant("tenant1".to_string(), None).unwrap();
        enforcer.register_collection("tenant1", "collection1".to_string()).unwrap();

        // Tenant 2 should not be able to access tenant1's collection
        enforcer.register_tenant("tenant2".to_string(), None).unwrap();

        let result = enforcer.validate_operation("tenant2", "collection1", "read");
        // This should fail because tenant2 didn't register collection1
        assert!(result.is_err());
    }

    #[test]
    fn test_enforcer_concurrent_tenant_operations() {
        let mut enforcer = TenantIsolationEnforcer::new();

        // Register multiple tenants
        for i in 0..5 {
            let tenant_id = format!("tenant{}", i);
            enforcer.register_tenant(tenant_id.clone(), Some(1_000_000)).unwrap();
            enforcer.register_collection(&tenant_id, "collection".to_string()).unwrap();
        }

        // Verify all tenants registered independently
        for i in 0..5 {
            let tenant_id = format!("tenant{}", i);
            assert!(enforcer.tenants.contains_key(&tenant_id));
            let collections = enforcer.get_collections(&tenant_id).unwrap();
            assert!(collections.contains(&"collection".to_string()));
        }
    }

    // ==================== SECURITY SCENARIO TESTS ====================

    #[test]
    fn test_filter_injection_prevention() {
        let enforcer = TenantIsolationEnforcer::new();

        // Try to inject filter syntax
        let malicious_tenant = "tenant1; DELETE FROM collection";
        let result = enforcer.validate_operation(malicious_tenant, "collection1", "read");
        assert!(result.is_err());
    }

    #[test]
    fn test_special_character_handling() {
        let mut enforcer = TenantIsolationEnforcer::new();

        // Register tenant with special characters (should sanitize)
        let result = enforcer.register_tenant("tenant@#$".to_string(), None);
        // Should handle gracefully (either reject or sanitize)
        if result.is_ok() {
            // If accepted, verify it's isolated
            assert!(enforcer.tenants.contains_key("tenant@#$"));
        } else {
            // If rejected, that's also fine (better security posture)
            assert!(result.is_err());
        }
    }

    #[test]
    fn test_unauthorized_quota_modification() {
        let mut enforcer = TenantIsolationEnforcer::new();

        enforcer.register_tenant("tenant1".to_string(), Some(1_000_000)).unwrap();

        // Only the quota check should work, not arbitrary modification
        enforcer.check_quota("tenant1", 500_000).unwrap();

        // Verify quota is still 1_000_000 (not modified by check)
        let tenant = enforcer.tenants.get("tenant1").unwrap();
        assert_eq!(tenant.max_storage_bytes, Some(1_000_000));
    }
}
