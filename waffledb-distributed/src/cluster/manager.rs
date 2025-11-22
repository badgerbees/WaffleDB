use std::collections::HashMap;

/// Node status in the cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeStatus {
    Healthy,
    Lagging,
    Offline,
}

/// Cluster member.
#[derive(Debug, Clone)]
pub struct ClusterMember {
    pub id: String,
    pub status: NodeStatus,
    pub last_heartbeat: u64,
}

/// Cluster manager.
pub struct ClusterManager {
    pub members: HashMap<String, ClusterMember>,
    pub leader: Option<String>,
}

impl ClusterManager {
    pub fn new() -> Self {
        ClusterManager {
            members: HashMap::new(),
            leader: None,
        }
    }

    /// Add a member to the cluster.
    pub fn add_member(&mut self, id: String) {
        self.members.insert(
            id.clone(),
            ClusterMember {
                id,
                status: NodeStatus::Healthy,
                last_heartbeat: 0,
            },
        );
    }

    /// Remove a member from the cluster.
    pub fn remove_member(&mut self, id: &str) {
        self.members.remove(id);
        if self.leader.as_deref() == Some(id) {
            self.leader = None;
        }
    }

    /// Set the cluster leader.
    pub fn set_leader(&mut self, id: String) {
        self.leader = Some(id);
    }

    /// Update member status.
    pub fn update_member_status(&mut self, id: &str, status: NodeStatus, heartbeat: u64) {
        if let Some(member) = self.members.get_mut(id) {
            member.status = status;
            member.last_heartbeat = heartbeat;
        }
    }

    /// Get healthy members.
    pub fn healthy_members(&self) -> Vec<String> {
        self.members
            .iter()
            .filter(|(_, m)| m.status == NodeStatus::Healthy)
            .map(|(id, _)| id.clone())
            .collect()
    }

    /// Get all members.
    pub fn all_members(&self) -> Vec<String> {
        self.members.keys().cloned().collect()
    }
}
