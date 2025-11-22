/// Follower replication logic.
pub struct FollowerState {
    pub last_heartbeat: u64,
    pub election_timeout: u64,
}

impl FollowerState {
    pub fn new(election_timeout: u64) -> Self {
        FollowerState {
            last_heartbeat: 0,
            election_timeout,
        }
    }

    /// Check if election timeout has elapsed.
    pub fn election_timeout_elapsed(&self, current_time: u64) -> bool {
        current_time - self.last_heartbeat > self.election_timeout
    }

    /// Update last heartbeat time.
    pub fn update_heartbeat(&mut self, current_time: u64) {
        self.last_heartbeat = current_time;
    }
}
