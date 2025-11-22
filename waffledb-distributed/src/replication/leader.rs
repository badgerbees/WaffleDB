use std::collections::HashMap;

/// Leader replication logic.
pub struct LeaderState {
    pub next_index: HashMap<String, usize>,
    pub match_index: HashMap<String, usize>,
}

impl LeaderState {
    pub fn new(peers: &[String]) -> Self {
        let mut next_index = HashMap::new();
        let mut match_index = HashMap::new();

        for peer in peers {
            next_index.insert(peer.clone(), 0);
            match_index.insert(peer.clone(), 0);
        }

        LeaderState {
            next_index,
            match_index,
        }
    }

    /// Update next index for a peer.
    pub fn update_next_index(&mut self, peer: &str, index: usize) {
        if let Some(entry) = self.next_index.get_mut(peer) {
            *entry = index;
        }
    }

    /// Update match index for a peer.
    pub fn update_match_index(&mut self, peer: &str, index: usize) {
        if let Some(entry) = self.match_index.get_mut(peer) {
            *entry = index;
        }
    }
}
