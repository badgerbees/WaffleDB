use std::fmt;

/// Error codes for programmatic error handling
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ErrorCode {
    /// 1000-1099: Dimension/Vector errors
    VectorDimensionMismatch = 1001,
    VectorDimensionInvalid = 1002,
    
    /// 1100-1199: Distance/Similarity errors
    InvalidDistanceMetric = 1101,
    DistanceComputationFailed = 1102,
    
    /// 1200-1299: Compression errors
    CompressionFailed = 1201,
    DecompressionFailed = 1202,
    InvalidCompressedVector = 1203,
    
    /// 1300-1399: Storage errors
    StorageIOError = 1301,
    SnapshotFailed = 1302,
    RecoveryFailed = 1303,
    WALReplayFailed = 1304,
    
    /// 1400-1499: Index/Search errors
    NotFound = 1401,
    IndexCorrupted = 1402,
    IndexBuildFailed = 1403,
    MergeFailed = 1404,
    
    /// 1500-1599: Collection/Database errors
    CollectionNotFound = 1501,
    CollectionAlreadyExists = 1502,
    DuplicateVectorID = 1503,
    
    /// 1600-1699: Concurrency errors
    LockPoisoned = 1601,
    DeadlockDetected = 1602,
    
    /// 1700-1799: Configuration/Validation errors
    InvalidConfiguration = 1701,
    ValidationFailed = 1702,
    
    /// 1800-1899: Distributed mode errors
    DistributedModeError = 1801,
    ConfigError = 1802,

    /// 9000: Unknown error
    Unknown = 9000,
}

impl ErrorCode {
    pub fn as_str(&self) -> &'static str {
        match self {
            ErrorCode::VectorDimensionMismatch => "VECTOR_DIMENSION_MISMATCH",
            ErrorCode::VectorDimensionInvalid => "VECTOR_DIMENSION_INVALID",
            ErrorCode::InvalidDistanceMetric => "INVALID_DISTANCE_METRIC",
            ErrorCode::DistanceComputationFailed => "DISTANCE_COMPUTATION_FAILED",
            ErrorCode::CompressionFailed => "COMPRESSION_FAILED",
            ErrorCode::DecompressionFailed => "DECOMPRESSION_FAILED",
            ErrorCode::InvalidCompressedVector => "INVALID_COMPRESSED_VECTOR",
            ErrorCode::StorageIOError => "STORAGE_IO_ERROR",
            ErrorCode::SnapshotFailed => "SNAPSHOT_FAILED",
            ErrorCode::RecoveryFailed => "RECOVERY_FAILED",
            ErrorCode::WALReplayFailed => "WAL_REPLAY_FAILED",
            ErrorCode::NotFound => "NOT_FOUND",
            ErrorCode::IndexCorrupted => "INDEX_CORRUPTED",
            ErrorCode::IndexBuildFailed => "INDEX_BUILD_FAILED",
            ErrorCode::MergeFailed => "MERGE_FAILED",
            ErrorCode::CollectionNotFound => "COLLECTION_NOT_FOUND",
            ErrorCode::CollectionAlreadyExists => "COLLECTION_ALREADY_EXISTS",
            ErrorCode::DuplicateVectorID => "DUPLICATE_VECTOR_ID",
            ErrorCode::LockPoisoned => "LOCK_POISONED",
            ErrorCode::DeadlockDetected => "DEADLOCK_DETECTED",
            ErrorCode::InvalidConfiguration => "INVALID_CONFIGURATION",
            ErrorCode::ValidationFailed => "VALIDATION_FAILED",
            ErrorCode::DistributedModeError => "DISTRIBUTED_MODE_ERROR",
            ErrorCode::ConfigError => "CONFIG_ERROR",
            ErrorCode::Unknown => "UNKNOWN_ERROR",
        }
    }
}

#[derive(Debug, Clone)]
pub enum WaffleError {
    /// Dimension mismatch between vectors and collection
    VectorDimensionMismatch { expected: usize, got: usize },
    /// Invalid vector dimension
    VectorDimensionInvalid(usize),
    /// Invalid distance metric
    InvalidDistance(String),
    /// Compression error with details
    CompressionError { code: ErrorCode, message: String },
    /// Storage error with details
    StorageError { code: ErrorCode, message: String },
    /// Item not found with context
    NotFound(String),
    /// Collection not found
    CollectionNotFound(String),
    /// Duplicate key
    DuplicateKey(String),
    /// Lock was poisoned (concurrent panic)
    LockPoisoned(String),
    /// Distributed mode error
    DistributedError { message: String },
    /// Configuration error
    ConfigError { message: String },
    /// Generic error with code
    WithCode { code: ErrorCode, message: String },
}

impl WaffleError {
    pub fn code(&self) -> ErrorCode {
        match self {
            WaffleError::VectorDimensionMismatch { .. } => ErrorCode::VectorDimensionMismatch,
            WaffleError::VectorDimensionInvalid(_) => ErrorCode::VectorDimensionInvalid,
            WaffleError::InvalidDistance(_) => ErrorCode::InvalidDistanceMetric,
            WaffleError::CompressionError { code, .. } => *code,
            WaffleError::StorageError { code, .. } => *code,
            WaffleError::NotFound(_) => ErrorCode::NotFound,
            WaffleError::CollectionNotFound(_) => ErrorCode::CollectionNotFound,
            WaffleError::DuplicateKey(_) => ErrorCode::DuplicateVectorID,
            WaffleError::LockPoisoned(_) => ErrorCode::LockPoisoned,
            WaffleError::DistributedError { .. } => ErrorCode::DistributedModeError,
            WaffleError::ConfigError { .. } => ErrorCode::ConfigError,
            WaffleError::WithCode { code, .. } => *code,
        }
    }
}

impl fmt::Display for WaffleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WaffleError::VectorDimensionMismatch { expected, got } => {
                write!(f, "[{}] Vector dimension mismatch: expected {}, got {}", 
                    self.code().as_str(), expected, got)
            }
            WaffleError::VectorDimensionInvalid(dim) => {
                write!(f, "[{}] Invalid vector dimension: {}", 
                    self.code().as_str(), dim)
            }
            WaffleError::InvalidDistance(msg) => {
                write!(f, "[{}] Invalid distance metric: {}", 
                    self.code().as_str(), msg)
            }
            WaffleError::CompressionError { code, message } => {
                write!(f, "[{}] Compression error: {}", code.as_str(), message)
            }
            WaffleError::StorageError { code, message } => {
                write!(f, "[{}] Storage error: {}", code.as_str(), message)
            }
            WaffleError::NotFound(context) => {
                write!(f, "[{}] Not found: {}", self.code().as_str(), context)
            }
            WaffleError::CollectionNotFound(name) => {
                write!(f, "[{}] Collection not found: {}", self.code().as_str(), name)
            }
            WaffleError::DuplicateKey(key) => {
                write!(f, "[{}] Duplicate key: {}", self.code().as_str(), key)
            }
            WaffleError::LockPoisoned(context) => {
                write!(f, "[{}] Lock poisoned (concurrent panic detected): {}", 
                    self.code().as_str(), context)
            }
            WaffleError::DistributedError { message } => {
                write!(f, "[{}] Distributed error: {}", self.code().as_str(), message)
            }
            WaffleError::ConfigError { message } => {
                write!(f, "[{}] Config error: {}", self.code().as_str(), message)
            }
            WaffleError::WithCode { code, message } => {
                write!(f, "[{}] {}", code.as_str(), message)
            }
        }
    }
}

impl std::error::Error for WaffleError {}

pub type Result<T> = std::result::Result<T, WaffleError>;
