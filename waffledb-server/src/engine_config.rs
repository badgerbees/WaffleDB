/// Engine configuration for tuning performance and memory usage
/// 
/// This module provides configurable parameters for vector engines,
/// allowing operators to tune behavior based on workload characteristics
/// and system resources.

use std::env;

/// Hybrid engine configuration
#[derive(Debug, Clone)]
pub struct HybridEngineConfig {
    /// Buffer capacity: number of vectors to buffer before triggering HNSW build
    /// Recommended: 10,000-50,000 depending on memory
    /// Larger = fewer builds, but higher search latency during buffering
    /// Smaller = more builds, lower memory, but CPU overhead
    pub buffer_capacity: usize,
    
    /// HNSW ef_construction: quality vs. speed tradeoff during index building
    /// Recommended: 50-200
    /// Higher = better quality, slower construction
    /// Lower = faster construction, lower quality
    pub ef_construction: usize,
    
    /// HNSW ef_search: quality vs. speed tradeoff during searches
    /// Recommended: 10-100
    /// Higher = better quality, slower searches
    /// Lower = faster searches, lower recall
    pub ef_search: usize,
    
    /// Maximum number of connections per node in HNSW graph
    /// Recommended: 12-32
    /// Higher = better connectivity, more memory
    /// Lower = faster operations, lower recall
    pub hnsw_m: usize,
}

impl Default for HybridEngineConfig {
    fn default() -> Self {
        HybridEngineConfig {
            buffer_capacity: 10_000,
            ef_construction: 100,
            ef_search: 50,
            hnsw_m: 16,
        }
    }
}

impl HybridEngineConfig {
    /// Load configuration from environment variables
    /// 
    /// Environment variables (with defaults):
    /// - WAFFLEDB_BUFFER_CAPACITY: 10000
    /// - WAFFLEDB_EF_CONSTRUCTION: 100
    /// - WAFFLEDB_EF_SEARCH: 50
    /// - WAFFLEDB_HNSW_M: 16
    pub fn from_env() -> Self {
        let mut config = Self::default();
        
        if let Ok(val) = env::var("WAFFLEDB_BUFFER_CAPACITY") {
            if let Ok(capacity) = val.parse() {
                config.buffer_capacity = capacity;
            }
        }
        
        if let Ok(val) = env::var("WAFFLEDB_EF_CONSTRUCTION") {
            if let Ok(ef) = val.parse() {
                config.ef_construction = ef;
            }
        }
        
        if let Ok(val) = env::var("WAFFLEDB_EF_SEARCH") {
            if let Ok(ef) = val.parse() {
                config.ef_search = ef;
            }
        }
        
        if let Ok(val) = env::var("WAFFLEDB_HNSW_M") {
            if let Ok(m) = val.parse() {
                config.hnsw_m = m;
            }
        }
        
        config
    }
    
    /// Validate configuration for sanity
    pub fn validate(&self) -> Result<(), String> {
        if self.buffer_capacity == 0 {
            return Err("buffer_capacity must be > 0".to_string());
        }
        if self.ef_construction == 0 {
            return Err("ef_construction must be > 0".to_string());
        }
        if self.ef_search == 0 {
            return Err("ef_search must be > 0".to_string());
        }
        if self.hnsw_m == 0 {
            return Err("hnsw_m must be > 0".to_string());
        }
        
        // Warn about extreme values (not errors, just warnings)
        if self.buffer_capacity > 1_000_000 {
            eprintln!("Warning: buffer_capacity is very large ({}), may cause memory issues", 
                self.buffer_capacity);
        }
        if self.ef_construction > 10_000 {
            eprintln!("Warning: ef_construction is very large ({}), builds will be slow", 
                self.ef_construction);
        }
        
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_default_config() {
        let config = HybridEngineConfig::default();
        assert_eq!(config.buffer_capacity, 10_000);
        assert_eq!(config.ef_construction, 100);
        assert_eq!(config.ef_search, 50);
        assert_eq!(config.hnsw_m, 16);
    }
    
    #[test]
    fn test_config_validation() {
        let mut config = HybridEngineConfig::default();
        assert!(config.validate().is_ok());
        
        config.buffer_capacity = 0;
        assert!(config.validate().is_err());
    }
}
