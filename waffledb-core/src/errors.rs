use std::fmt;

#[derive(Debug, Clone)]
pub enum WaffleError {
    InvalidDimension,
    InvalidDistance,
    CompressionError(String),
    StorageError(String),
    NotFound,
}

impl fmt::Display for WaffleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WaffleError::InvalidDimension => write!(f, "Invalid vector dimension"),
            WaffleError::InvalidDistance => write!(f, "Invalid distance metric"),
            WaffleError::CompressionError(msg) => write!(f, "Compression error: {}", msg),
            WaffleError::StorageError(msg) => write!(f, "Storage error: {}", msg),
            WaffleError::NotFound => write!(f, "Item not found"),
        }
    }
}

impl std::error::Error for WaffleError {}

pub type Result<T> = std::result::Result<T, WaffleError>;
