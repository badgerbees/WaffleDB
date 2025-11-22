use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use serde_json::Value;

// ============================================================================
// COLLECTION MANAGEMENT
// ============================================================================

/// Create collection request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateCollectionRequest {
    /// Collection name (unique)
    pub name: String,
    /// Vector dimension
    pub dimension: usize,
    /// Distance metric: "l2", "cosine", "inner_product"
    #[serde(default = "default_metric")]
    pub metric: String,
    /// Engine type: "hnsw" (default: "hnsw")
    #[serde(default = "default_engine")]
    pub engine: Option<String>,
    /// HNSW M parameter (default 16)
    #[serde(default = "default_m")]
    pub m: Option<usize>,
    /// HNSW M_l multiplier (default ln(2))
    #[serde(default)]
    pub m_l: Option<f32>,
    /// Duplicate handling policy: "overwrite", "reject" (default: "overwrite")
    #[serde(default = "default_duplicate_policy")]
    pub duplicate_policy: String,
}

fn default_duplicate_policy() -> String {
    "overwrite".to_string()
}

fn default_metric() -> String {
    "l2".to_string()
}

fn default_engine() -> Option<String> {
    Some("hybrid".to_string())
}

fn default_m() -> Option<usize> {
    Some(16)
}

/// Create collection response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateCollectionResponse {
    pub name: String,
    pub dimension: usize,
    pub metric: String,
    pub engine: String,
    pub status: String,
}

/// Collection metadata response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CollectionInfo {
    pub name: String,
    pub dimension: usize,
    pub metric: String,
    pub vector_count: usize,
    pub size_bytes: u64,
}

/// Delete collection request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteCollectionRequest {
    pub name: String,
}

/// Delete collection response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteCollectionResponse {
    pub status: String,
}

// ============================================================================
// INSERT OPERATIONS
// ============================================================================

/// Single insert request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InsertRequest {
    /// Auto-generated ID if not provided
    pub id: Option<String>,
    /// Vector embedding
    pub vector: Vec<f32>,
    /// Optional metadata (accepts any JSON value type, converts to strings)
    pub metadata: Option<HashMap<String, Value>>,
}

/// Batch insert request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchInsertRequest {
    /// Vectors to insert
    pub vectors: Vec<InsertRequest>,
}

/// Insert response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InsertResponse {
    pub id: String,
    pub status: String,
}

/// Batch insert response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchInsertResponse {
    pub inserted_count: usize,
    pub failed_count: usize,
    pub errors: Vec<String>,
}

// ============================================================================
// SEARCH OPERATIONS
// ============================================================================

/// Single search request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchRequest {
    pub vector: Vec<f32>,
    pub top_k: usize,
    /// ef_search parameter for HNSW (optional, overrides default)
    #[serde(default)]
    pub ef_search: Option<usize>,
    /// Filter metadata (simple key-value matching, supports contains)
    #[serde(default)]
    pub filter: Option<serde_json::Value>,
    /// Include metadata in results (default: true)
    #[serde(default = "default_include_metadata")]
    pub include_metadata: bool,
}

fn default_include_metadata() -> bool {
    true
}

/// Batch search request (multiple queries)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchSearchRequest {
    pub queries: Vec<SearchRequest>,
}

/// Single search result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchResultItem {
    pub id: String,
    pub distance: f32,
    pub metadata: Option<HashMap<String, String>>,
}

/// Search response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchResponse {
    pub results: Vec<SearchResultItem>,
    pub query_latency_ms: f64,
}

/// Batch search response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchSearchResponse {
    pub results: Vec<Vec<SearchResultItem>>,
    pub total_latency_ms: f64,
}

// ============================================================================
// DELETE OPERATIONS
// ============================================================================

/// Delete request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteRequest {
    pub id: String,
}

/// Delete response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteResponse {
    pub status: String,
}

// ============================================================================
// METADATA OPERATIONS
// ============================================================================

/// Update metadata request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateMetadataRequest {
    pub id: String,
    pub metadata: serde_json::Value,
}

/// Update metadata response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateMetadataResponse {
    pub status: String,
    pub id: String,
}

/// Patch metadata request (partial update)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PatchMetadataRequest {
    pub id: String,
    /// Partial metadata to merge with existing
    pub metadata: serde_json::Value,
}

/// Update vector request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateVectorRequest {
    pub id: String,
    pub vector: Vec<f32>,
    /// Optional new metadata
    #[serde(default)]
    pub metadata: Option<serde_json::Value>,
}

/// Update vector response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateVectorResponse {
    pub status: String,
    pub id: String,
}

// ============================================================================
// SNAPSHOT OPERATIONS
// ============================================================================

/// Snapshot creation response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotResponse {
    pub snapshot_id: String,
    pub timestamp: u64,
    pub vector_count: usize,
    pub size_bytes: u64,
}

// ============================================================================
// STATISTICS & MONITORING
// ============================================================================

/// Enhanced collection statistics with full introspection
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CollectionStats {
    pub name: String,
    pub dimension: usize,
    pub vector_count: usize,
    pub memory_bytes: u64,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
    pub index_status: String,
}

/// Server health status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthResponse {
    pub status: String,
    pub version: String,
    pub uptime_seconds: u64,
}

/// Metrics response (Prometheus format)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsResponse {
    pub search_latency_p95_ms: f64,
    pub search_latency_p99_ms: f64,
    pub insert_latency_p95_ms: f64,
    pub cache_hit_rate: f64,
    pub total_requests: u64,
    pub total_errors: u64,
}

// ============================================================================
// ERROR RESPONSE
// ============================================================================

/// Standard error response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorResponse {
    pub error: String,
    pub code: u32,
}

impl ErrorResponse {
    pub fn bad_request(msg: impl Into<String>) -> Self {
        ErrorResponse {
            error: msg.into(),
            code: 400,
        }
    }

    pub fn not_found(msg: impl Into<String>) -> Self {
        ErrorResponse {
            error: msg.into(),
            code: 404,
        }
    }

    pub fn internal_error(msg: impl Into<String>) -> Self {
        ErrorResponse {
            error: msg.into(),
            code: 500,
        }
    }
}

/// Detailed error response with actionable information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DetailedErrorResponse {
    /// Error code (same as HTTP status)
    pub code: u32,
    /// Error type: "ValidationError", "DeserializationError", "NotFound", etc
    pub error_type: String,
    /// Human-readable error message
    pub message: String,
    /// Field that caused the error (if applicable)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub field: Option<String>,
    /// What went wrong (e.g., "Expected string, got integer")
    #[serde(skip_serializing_if = "Option::is_none")]
    pub problem: Option<String>,
    /// Suggested fix for the problem
    #[serde(skip_serializing_if = "Option::is_none")]
    pub suggestion: Option<String>,
    /// Request ID for tracking/debugging
    pub request_id: String,
    /// Timestamp when error occurred
    pub timestamp_ms: u64,
}

impl DetailedErrorResponse {
    pub fn validation_error(
        field: impl Into<String>,
        problem: impl Into<String>,
        suggestion: impl Into<String>,
    ) -> Self {
        let field_str = field.into();
        let request_id = Self::generate_request_id();
        DetailedErrorResponse {
            code: 400,
            error_type: "ValidationError".to_string(),
            message: format!("Invalid field: {}", field_str),
            field: Some(field_str),
            problem: Some(problem.into()),
            suggestion: Some(suggestion.into()),
            request_id,
            timestamp_ms: Self::current_timestamp_ms(),
        }
    }

    pub fn deserialization_error(
        field: impl Into<String>,
        expected: impl Into<String>,
        got: impl Into<String>,
        suggestion: impl Into<String>,
    ) -> Self {
        let request_id = Self::generate_request_id();
        DetailedErrorResponse {
            code: 400,
            error_type: "DeserializationError".to_string(),
            message: format!("Failed to deserialize JSON"),
            field: Some(field.into()),
            problem: Some(format!("Expected {}, got {}", expected.into(), got.into())),
            suggestion: Some(suggestion.into()),
            request_id,
            timestamp_ms: Self::current_timestamp_ms(),
        }
    }

    pub fn not_found(resource: impl Into<String>) -> Self {
        let request_id = Self::generate_request_id();
        DetailedErrorResponse {
            code: 404,
            error_type: "NotFound".to_string(),
            message: format!("Resource not found: {}", resource.into()),
            field: None,
            problem: None,
            suggestion: Some("Check that the resource name is correct".to_string()),
            request_id,
            timestamp_ms: Self::current_timestamp_ms(),
        }
    }

    pub fn internal_error(msg: impl Into<String>) -> Self {
        let request_id = Self::generate_request_id();
        DetailedErrorResponse {
            code: 500,
            error_type: "InternalError".to_string(),
            message: msg.into(),
            field: None,
            problem: None,
            suggestion: Some(format!("Please report this issue with request ID: {}", request_id)),
            request_id,
            timestamp_ms: Self::current_timestamp_ms(),
        }
    }

    fn generate_request_id() -> String {
        use std::time::{SystemTime, UNIX_EPOCH};
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        format!("req_{:x}", nanos)
    }

    fn current_timestamp_ms() -> u64 {
        use std::time::{SystemTime, UNIX_EPOCH};
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
    }
}
