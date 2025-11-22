/// Raft node role.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RaftRole {
    Follower,
    Candidate,
    Leader,
}

/// Raft node state.
#[derive(Debug, Clone)]
pub struct RaftNode {
    pub id: String,
    pub role: RaftRole,
    pub current_term: u64,
    pub voted_for: Option<String>,
    pub commit_index: usize,
    pub last_applied: usize,
    pub peers: Vec<String>,
}

impl RaftNode {
    /// Create a new Raft node.
    pub fn new(id: String, peers: Vec<String>) -> Self {
        RaftNode {
            id,
            role: RaftRole::Follower,
            current_term: 0,
            voted_for: None,
            commit_index: 0,
            last_applied: 0,
            peers,
        }
    }

    /// Become a candidate.
    pub fn become_candidate(&mut self) {
        self.role = RaftRole::Candidate;
        self.current_term += 1;
        self.voted_for = Some(self.id.clone());
    }

    /// Become a leader.
    pub fn become_leader(&mut self) {
        self.role = RaftRole::Leader;
        self.voted_for = None;
    }

    /// Become a follower.
    pub fn become_follower(&mut self, term: u64) {
        if term > self.current_term {
            self.current_term = term;
            self.voted_for = None;
        }
        self.role = RaftRole::Follower;
    }

    /// Check if node is leader.
    pub fn is_leader(&self) -> bool {
        self.role == RaftRole::Leader
    }
}
