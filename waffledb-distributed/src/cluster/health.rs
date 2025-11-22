/// Cluster health monitoring.
pub struct HealthMonitor {
    pub heartbeat_interval: u64,
    pub heartbeat_timeout: u64,
}

impl HealthMonitor {
    pub fn new(heartbeat_interval: u64, heartbeat_timeout: u64) -> Self {
        HealthMonitor {
            heartbeat_interval,
            heartbeat_timeout,
        }
    }

    /// Check if a node is healthy based on last heartbeat.
    pub fn is_healthy(&self, last_heartbeat: u64, current_time: u64) -> bool {
        current_time - last_heartbeat <= self.heartbeat_timeout
    }

    /// Get next heartbeat deadline.
    pub fn next_heartbeat_deadline(&self, last_heartbeat: u64) -> u64 {
        last_heartbeat + self.heartbeat_interval
    }
}
