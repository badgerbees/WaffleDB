/// Multi-tenancy support for WaffleDB
/// 
/// Provides:
/// - Tenant isolation (namespace-based)
/// - Per-tenant metadata and indexes
/// - Disk quotas
/// - Separate index management

pub mod tenant;
pub mod namespace;
pub mod quota;

pub use tenant::{Tenant, TenantConfig, TenantId};
pub use namespace::{Namespace, NamespaceConfig};
pub use quota::{QuotaManager, DiskQuota};
