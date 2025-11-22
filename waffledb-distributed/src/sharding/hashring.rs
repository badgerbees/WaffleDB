use std::collections::BTreeMap;

/// Consistent hash ring for distributed sharding.
pub struct HashRing {
    ring: BTreeMap<u64, String>, // hash -> node_id
    replicas: usize,
}

impl HashRing {
    /// Create a new hash ring.
    pub fn new(replicas: usize) -> Self {
        HashRing {
            ring: BTreeMap::new(),
            replicas,
        }
    }

    /// Add a node to the ring.
    pub fn add_node(&mut self, node_id: String) {
        for i in 0..self.replicas {
            let key = format!("{}:{}", node_id, i);
            let hash = self.hash(&key);
            self.ring.insert(hash, node_id.clone());
        }
    }

    /// Remove a node from the ring.
    pub fn remove_node(&mut self, node_id: &str) {
        let mut to_remove = vec![];
        for (hash, nid) in &self.ring {
            if nid == node_id {
                to_remove.push(*hash);
            }
        }
        for hash in to_remove {
            self.ring.remove(&hash);
        }
    }

    /// Get the node responsible for a key.
    pub fn get_node(&self, key: &str) -> Option<String> {
        if self.ring.is_empty() {
            return None;
        }

        let hash = self.hash(key);

        // Find the first node with hash >= key hash
        self.ring
            .range(hash..)
            .next()
            .or_else(|| self.ring.iter().next())
            .map(|(_, node_id)| node_id.clone())
    }

    /// Hash function using FNV-1a.
    fn hash(&self, key: &str) -> u64 {
        let mut hash: u64 = 14695981039346656037;
        for byte in key.bytes() {
            hash ^= byte as u64;
            hash = hash.wrapping_mul(1099511628211);
        }
        hash
    }
}
