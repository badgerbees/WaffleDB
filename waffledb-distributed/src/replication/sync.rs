/// Replication synchronization.
pub struct ReplicationSyncState {
    pub synced_peers: Vec<String>,
    pub pending_peers: Vec<String>,
}

impl ReplicationSyncState {
    pub fn new() -> Self {
        ReplicationSyncState {
            synced_peers: vec![],
            pending_peers: vec![],
        }
    }

    /// Mark a peer as synced.
    pub fn mark_synced(&mut self, peer: &str) {
        if let Some(pos) = self.pending_peers.iter().position(|p| p == peer) {
            self.pending_peers.remove(pos);
            self.synced_peers.push(peer.to_string());
        }
    }

    /// Add a pending peer.
    pub fn add_pending(&mut self, peer: String) {
        self.pending_peers.push(peer);
    }

    /// Get sync progress.
    pub fn progress(&self) -> (usize, usize) {
        (self.synced_peers.len(), self.synced_peers.len() + self.pending_peers.len())
    }
}
