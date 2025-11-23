/// Timeout middleware for request protection
/// 
/// Prevents long-running operations from holding resources indefinitely
/// Configurable per endpoint type

use std::time::Duration;

/// Operation timeout configuration
#[derive(Debug, Clone)]
pub struct TimeoutConfig {
    /// Timeout for insert operations (default: 30 seconds)
    pub insert_timeout: Duration,
    
    /// Timeout for search operations (default: 10 seconds)
    pub search_timeout: Duration,
    
    /// Timeout for delete operations (default: 5 seconds)
    pub delete_timeout: Duration,
    
    /// Timeout for batch operations (default: 60 seconds)
    pub batch_timeout: Duration,
    
    /// Timeout for snapshot operations (default: 120 seconds)
    pub snapshot_timeout: Duration,
}

impl Default for TimeoutConfig {
    fn default() -> Self {
        TimeoutConfig {
            insert_timeout: Duration::from_secs(30),
            search_timeout: Duration::from_secs(10),
            delete_timeout: Duration::from_secs(5),
            batch_timeout: Duration::from_secs(60),
            snapshot_timeout: Duration::from_secs(120),
        }
    }
}

impl TimeoutConfig {
    /// Load from environment variables
    /// 
    /// Environment variables (all optional, using defaults if not set):
    /// - WAFFLEDB_INSERT_TIMEOUT_SECS: default 30
    /// - WAFFLEDB_SEARCH_TIMEOUT_SECS: default 10
    /// - WAFFLEDB_DELETE_TIMEOUT_SECS: default 5
    /// - WAFFLEDB_BATCH_TIMEOUT_SECS: default 60
    /// - WAFFLEDB_SNAPSHOT_TIMEOUT_SECS: default 120
    pub fn from_env() -> Self {
        let mut config = Self::default();
        
        if let Ok(val) = std::env::var("WAFFLEDB_INSERT_TIMEOUT_SECS") {
            if let Ok(secs) = val.parse::<u64>() {
                config.insert_timeout = Duration::from_secs(secs);
            }
        }
        
        if let Ok(val) = std::env::var("WAFFLEDB_SEARCH_TIMEOUT_SECS") {
            if let Ok(secs) = val.parse::<u64>() {
                config.search_timeout = Duration::from_secs(secs);
            }
        }
        
        if let Ok(val) = std::env::var("WAFFLEDB_DELETE_TIMEOUT_SECS") {
            if let Ok(secs) = val.parse::<u64>() {
                config.delete_timeout = Duration::from_secs(secs);
            }
        }
        
        if let Ok(val) = std::env::var("WAFFLEDB_BATCH_TIMEOUT_SECS") {
            if let Ok(secs) = val.parse::<u64>() {
                config.batch_timeout = Duration::from_secs(secs);
            }
        }
        
        if let Ok(val) = std::env::var("WAFFLEDB_SNAPSHOT_TIMEOUT_SECS") {
            if let Ok(secs) = val.parse::<u64>() {
                config.snapshot_timeout = Duration::from_secs(secs);
            }
        }
        
        config
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_default_timeouts() {
        let config = TimeoutConfig::default();
        assert_eq!(config.insert_timeout.as_secs(), 30);
        assert_eq!(config.search_timeout.as_secs(), 10);
        assert_eq!(config.delete_timeout.as_secs(), 5);
        assert_eq!(config.batch_timeout.as_secs(), 60);
        assert_eq!(config.snapshot_timeout.as_secs(), 120);
    }
}
