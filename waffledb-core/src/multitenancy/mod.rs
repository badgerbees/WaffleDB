/// Multi-tenancy support for WaffleDB
/// 
/// Provides:
/// - Tenant isolation (namespace-based)
/// - Per-tenant metadata and indexes
/// - Disk quotas
/// - Separate index management
/// - Strict storage-level isolation enforcement
/// - Per-tenant audit trails

pub mod tenant;
pub mod namespace;
pub mod quota;
pub mod isolation;

pub use tenant::{Tenant, TenantConfig, TenantId};
pub use namespace::{Namespace, NamespaceConfig};
pub use quota::{QuotaManager, DiskQuota};
pub use isolation::{TenantContext, StorageScope, TenantIsolationEnforcer, AuditEvent};

