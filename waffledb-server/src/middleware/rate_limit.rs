/// Rate limiting and backpressure middleware
/// 
/// Protects server from being overwhelmed by requests
/// Uses token bucket algorithm for fair rate limiting

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Instant, Duration};

/// Rate limit configuration
#[derive(Debug, Clone)]
pub struct RateLimitConfig {
    /// Maximum requests per second globally
    pub global_rps_limit: u32,
    
    /// Maximum requests per second per client IP
    pub per_ip_rps_limit: u32,
    
    /// Maximum concurrent requests
    pub max_concurrent_requests: u32,
    
    /// Request queue size before rejecting
    pub request_queue_size: u32,
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        RateLimitConfig {
            global_rps_limit: 10_000,        // 10k RPS global limit
            per_ip_rps_limit: 1_000,         // 1k RPS per IP
            max_concurrent_requests: 1_000,  // 1k concurrent requests
            request_queue_size: 5_000,       // 5k request queue
        }
    }
}

impl RateLimitConfig {
    /// Load from environment variables
    pub fn from_env() -> Self {
        let mut config = Self::default();
        
        if let Ok(val) = std::env::var("WAFFLEDB_GLOBAL_RPS_LIMIT") {
            if let Ok(limit) = val.parse() {
                config.global_rps_limit = limit;
            }
        }
        
        if let Ok(val) = std::env::var("WAFFLEDB_PER_IP_RPS_LIMIT") {
            if let Ok(limit) = val.parse() {
                config.per_ip_rps_limit = limit;
            }
        }
        
        if let Ok(val) = std::env::var("WAFFLEDB_MAX_CONCURRENT_REQUESTS") {
            if let Ok(limit) = val.parse() {
                config.max_concurrent_requests = limit;
            }
        }
        
        if let Ok(val) = std::env::var("WAFFLEDB_REQUEST_QUEUE_SIZE") {
            if let Ok(size) = val.parse() {
                config.request_queue_size = size;
            }
        }
        
        config
    }
}

/// Token bucket for rate limiting
struct TokenBucket {
    tokens: f64,
    capacity: f64,
    refill_rate: f64,  // tokens per second
    last_refill: Instant,
}

impl TokenBucket {
    fn new(capacity: f64, refill_rate: f64) -> Self {
        TokenBucket {
            tokens: capacity,
            capacity,
            refill_rate,
            last_refill: Instant::now(),
        }
    }
    
    fn refill(&mut self) {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_refill).as_secs_f64();
        self.tokens = (self.tokens + elapsed * self.refill_rate).min(self.capacity);
        self.last_refill = now;
    }
    
    fn try_consume(&mut self, tokens: f64) -> bool {
        self.refill();
        if self.tokens >= tokens {
            self.tokens -= tokens;
            true
        } else {
            false
        }
    }
}

/// Rate limiter state
pub struct RateLimiter {
    config: RateLimitConfig,
    global_bucket: Arc<Mutex<TokenBucket>>,
    per_ip_buckets: Arc<Mutex<HashMap<String, TokenBucket>>>,
    concurrent_requests: Arc<Mutex<u32>>,
    queued_requests: Arc<Mutex<u32>>,
}

impl RateLimiter {
    pub fn new(config: RateLimitConfig) -> Self {
        let global_rps = config.global_rps_limit as f64;
        let per_ip_rps = config.per_ip_rps_limit as f64;
        
        RateLimiter {
            config,
            global_bucket: Arc::new(Mutex::new(TokenBucket::new(global_rps, global_rps))),
            per_ip_buckets: Arc::new(Mutex::new(HashMap::new())),
            concurrent_requests: Arc::new(Mutex::new(0)),
            queued_requests: Arc::new(Mutex::new(0)),
        }
    }
    
    /// Check if request can be processed
    pub fn allow_request(&self, client_ip: &str) -> bool {
        // Check concurrent request limit
        let mut concurrent = self.concurrent_requests.lock().unwrap();
        if *concurrent >= self.config.max_concurrent_requests {
            return false;
        }
        
        // Check queue size
        let mut queued = self.queued_requests.lock().unwrap();
        if *queued >= self.config.request_queue_size {
            return false;
        }
        
        // Check global rate limit
        let mut global = self.global_bucket.lock().unwrap();
        if !global.try_consume(1.0) {
            return false;
        }
        
        // Check per-IP rate limit
        let mut per_ip = self.per_ip_buckets.lock().unwrap();
        let per_ip_rps = self.config.per_ip_rps_limit as f64;
        let bucket = per_ip
            .entry(client_ip.to_string())
            .or_insert_with(|| TokenBucket::new(per_ip_rps, per_ip_rps));
        
        if !bucket.try_consume(1.0) {
            return false;
        }
        
        // Increment tracking
        *concurrent += 1;
        *queued += 1;
        
        true
    }
    
    /// Mark request as started (dequeue)
    pub fn start_request(&self) {
        if let Ok(mut queued) = self.queued_requests.lock() {
            if *queued > 0 {
                *queued -= 1;
            }
        }
    }
    
    /// Mark request as completed
    pub fn finish_request(&self) {
        if let Ok(mut concurrent) = self.concurrent_requests.lock() {
            if *concurrent > 0 {
                *concurrent -= 1;
            }
        }
    }
    
    /// Get current stats
    pub fn stats(&self) -> RateLimitStats {
        RateLimitStats {
            concurrent_requests: self.concurrent_requests.lock().unwrap().clone(),
            queued_requests: self.queued_requests.lock().unwrap().clone(),
            tracked_ips: self.per_ip_buckets.lock().unwrap().len() as u32,
        }
    }
}

#[derive(Debug, Clone)]
pub struct RateLimitStats {
    pub concurrent_requests: u32,
    pub queued_requests: u32,
    pub tracked_ips: u32,
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_token_bucket() {
        let mut bucket = TokenBucket::new(10.0, 10.0);
        assert!(bucket.try_consume(5.0));
        assert_eq!(bucket.tokens, 5.0);
        assert!(bucket.try_consume(5.0));
        assert!(!bucket.try_consume(1.0));
    }
    
    #[test]
    fn test_rate_limiter_basic() {
        let config = RateLimitConfig {
            global_rps_limit: 10,
            per_ip_rps_limit: 5,
            max_concurrent_requests: 2,
            request_queue_size: 10,
        };
        let limiter = RateLimiter::new(config);
        
        assert!(limiter.allow_request("127.0.0.1"));
        limiter.start_request();
        assert!(limiter.allow_request("127.0.0.1"));
        limiter.start_request();
        // Should now be at concurrent limit
        assert!(!limiter.allow_request("127.0.0.1"));
    }
}
