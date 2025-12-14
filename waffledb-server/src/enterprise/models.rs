use serde::{Deserialize, Serialize};
use chrono::{DateTime, Utc};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TenantContext {
    pub org_id: String,
    pub project_id: String,
    pub user_id: String,
}

impl TenantContext {
    /// Generate a RocksDB key prefix for tenant isolation
    pub fn prefix(&self) -> String {
        format!("tenant:{}:proj:{}:", self.org_id, self.project_id)
    }

    /// Get full prefixed key for a resource
    pub fn prefixed_key(&self, resource: &str) -> String {
        format!("{}{}",self.prefix(), resource)
    }

    /// Parse tenant from Firebase custom claims
    pub fn from_firebase_claims(user_id: String, claims: &serde_json::Value) -> Result<Self, String> {
        let org_id = claims
            .get("org_id")
            .and_then(|v| v.as_str())
            .ok_or("Missing org_id in token claims")?
            .to_string();

        let project_id = claims
            .get("project_id")
            .and_then(|v| v.as_str())
            .ok_or("Missing project_id in token claims")?
            .to_string();

        Ok(TenantContext {
            org_id,
            project_id,
            user_id,
        })
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum ApiKeyRole {
    Admin,
    ReadWrite,
    ReadOnly,
}

impl std::str::FromStr for ApiKeyRole {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "admin" => Ok(ApiKeyRole::Admin),
            "read_write" | "readwrite" => Ok(ApiKeyRole::ReadWrite),
            "read_only" | "readonly" => Ok(ApiKeyRole::ReadOnly),
            _ => Err(format!("Invalid role: {}", s)),
        }
    }
}

impl ToString for ApiKeyRole {
    fn to_string(&self) -> String {
        match self {
            ApiKeyRole::Admin => "admin".to_string(),
            ApiKeyRole::ReadWrite => "read_write".to_string(),
            ApiKeyRole::ReadOnly => "read_only".to_string(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiKey {
    pub id: String,
    pub org_id: String,
    pub project_id: String,
    pub name: String,
    pub key_hash: String,
    pub role: ApiKeyRole,
    pub collections_access: Vec<String>, // "*" for all, or specific collection names
    pub created_at: DateTime<Utc>,
    pub last_used: Option<DateTime<Utc>>,
    pub is_active: bool,
}

impl ApiKey {
    pub fn new(
        org_id: String,
        project_id: String,
        name: String,
        role: ApiKeyRole,
        collections_access: Vec<String>,
    ) -> (Self, String) {
        let api_key = format!("wfdb_{}", Uuid::new_v4().to_string());
        let key_hash = Self::hash_key(&api_key);

        let api_key_obj = ApiKey {
            id: Uuid::new_v4().to_string(),
            org_id,
            project_id,
            name,
            key_hash,
            role,
            collections_access,
            created_at: Utc::now(),
            last_used: None,
            is_active: true,
        };

        (api_key_obj, api_key)
    }

    pub fn hash_key(key: &str) -> String {
        use sha2::{Sha256, Digest};
        let mut hasher = Sha256::new();
        hasher.update(key);
        format!("{:x}", hasher.finalize())
    }

    pub fn can_access_collection(&self, collection_name: &str) -> bool {
        self.collections_access.contains(&"*".to_string())
            || self.collections_access.contains(&collection_name.to_string())
    }

    pub fn can_read(&self) -> bool {
        matches!(self.role, ApiKeyRole::ReadWrite | ApiKeyRole::ReadOnly | ApiKeyRole::Admin)
    }

    pub fn can_write(&self) -> bool {
        matches!(self.role, ApiKeyRole::ReadWrite | ApiKeyRole::Admin)
    }

    pub fn can_admin(&self) -> bool {
        matches!(self.role, ApiKeyRole::Admin)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AuditAction {
    InsertVectors,
    DeleteVectors,
    SearchVectors,
    CreateCollection,
    DeleteCollection,
    ModifySchema,
    CreateSnapshot,
    RestoreSnapshot,
    CreateApiKey,
    DeleteApiKey,
}

impl ToString for AuditAction {
    fn to_string(&self) -> String {
        match self {
            AuditAction::InsertVectors => "insert_vectors",
            AuditAction::DeleteVectors => "delete_vectors",
            AuditAction::SearchVectors => "search_vectors",
            AuditAction::CreateCollection => "create_collection",
            AuditAction::DeleteCollection => "delete_collection",
            AuditAction::ModifySchema => "modify_schema",
            AuditAction::CreateSnapshot => "create_snapshot",
            AuditAction::RestoreSnapshot => "restore_snapshot",
            AuditAction::CreateApiKey => "create_api_key",
            AuditAction::DeleteApiKey => "delete_api_key",
        }
        .to_string()
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum AuditStatus {
    Success,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditLog {
    pub id: String,
    pub org_id: String,
    pub project_id: String,
    pub user_id: String,
    pub action: String, // Serialized AuditAction
    pub resource: String,
    pub timestamp: DateTime<Utc>,
    pub status: AuditStatus,
    pub details: serde_json::Value,
    pub error_message: Option<String>,
}

impl AuditLog {
    pub fn new(
        org_id: String,
        project_id: String,
        user_id: String,
        action: AuditAction,
        resource: String,
    ) -> Self {
        AuditLog {
            id: Uuid::new_v4().to_string(),
            org_id,
            project_id,
            user_id,
            action: action.to_string(),
            resource,
            timestamp: Utc::now(),
            status: AuditStatus::Success,
            details: serde_json::json!({}),
            error_message: None,
        }
    }

    pub fn with_details(mut self, details: serde_json::Value) -> Self {
        self.details = details;
        self
    }

    pub fn with_error(mut self, error: String) -> Self {
        self.status = AuditStatus::Failed;
        self.error_message = Some(error);
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TenantQuota {
    pub org_id: String,
    pub project_id: String,
    pub max_qps: u32,
    pub max_storage_bytes: u64,
    pub max_vectors_per_collection: u64,
    pub max_collections: u32,
    pub billing_tier: BillingTier,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum BillingTier {
    Free,
    Pro,
    Enterprise,
}

impl TenantQuota {
    pub fn free(org_id: String, project_id: String) -> Self {
        TenantQuota {
            org_id,
            project_id,
            max_qps: 100,
            max_storage_bytes: 1024 * 1024 * 1024, // 1GB
            max_vectors_per_collection: 1_000_000,
            max_collections: 10,
            billing_tier: BillingTier::Free,
            created_at: Utc::now(),
        }
    }

    pub fn pro(org_id: String, project_id: String) -> Self {
        TenantQuota {
            org_id,
            project_id,
            max_qps: 1000,
            max_storage_bytes: 100 * 1024 * 1024 * 1024, // 100GB
            max_vectors_per_collection: 100_000_000,
            max_collections: 100,
            billing_tier: BillingTier::Pro,
            created_at: Utc::now(),
        }
    }

    pub fn enterprise(org_id: String, project_id: String) -> Self {
        TenantQuota {
            org_id,
            project_id,
            max_qps: u32::MAX,
            max_storage_bytes: u64::MAX,
            max_vectors_per_collection: u64::MAX,
            max_collections: u32::MAX,
            billing_tier: BillingTier::Enterprise,
            created_at: Utc::now(),
        }
    }
}
