/// RPC integration layer - bridges ClusterManager with actual node communication
/// This module manages RPC client lifecycle and health tracking
/// Available in both OSS and Enterprise modes for multi-node support

use crate::distributed::cluster::{ClusterManager, NodeId};
use std::collections::HashMap;
use std::sync::Arc;
use parking_lot::RwLock;

/// RPC request/response types (shared with server)
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct AppendEntriesRequest {
    pub leader_id: String,
    pub term: u64,
    pub prev_log_index: u64,
    pub prev_log_term: u64,
    pub entries: Vec<Vec<u8>>,
    pub leader_commit: u64,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct AppendEntriesResponse {
    pub term: u64,
    pub success: bool,
    pub last_log_index: u64,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ForwardInsertRequest {
    pub collection: String,
    pub doc_id: String,
    pub vector: Vec<f32>,
    pub metadata: serde_json::Value,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ForwardInsertResponse {
    pub success: bool,
    pub error: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ForwardSearchRequest {
    pub collection: String,
    pub query_vector: Vec<f32>,
    pub top_k: usize,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SearchResult {
    pub doc_id: String,
    pub distance: f32,
    pub metadata: serde_json::Value,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ForwardSearchResponse {
    pub results: Vec<SearchResult>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct HeartbeatRequest {
    pub node_id: String,
    pub term: u64,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct HeartbeatResponse {
    pub healthy: bool,
    pub term: u64,
}

/// Manages RPC connections to remote nodes
/// Available for all multi-node deployments
pub struct RpcManager {
    cluster: Arc<RwLock<ClusterManager>>,
    /// HTTP client for making RPC calls (shared across all connections)
    http_client: reqwest::Client,
    /// Connection timeouts
    timeout_secs: u64,
}

impl RpcManager {
    /// Create a new RPC manager
    pub fn new(cluster: Arc<RwLock<ClusterManager>>) -> Self {
        let http_client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(5))
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());

        RpcManager {
            cluster,
            http_client,
            timeout_secs: 5,
        }
    }

    /// Send AppendEntries RPC to a specific node
    pub async fn send_append_entries(
        &self,
        node_id: &NodeId,
        request: &AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse, RpcError> {
        let node_info = {
            let cluster = self.cluster.read();
            cluster
                .get_node_info(node_id)
                .ok_or(RpcError::NodeNotFound(node_id.clone()))?
        };

        let url = format!("http://{}/raft/append", node_info.address);

        let result = self.http_client
            .post(&url)
            .json(request)
            .send()
            .await;

        match result {
            Ok(resp) if resp.status().is_success() => {
                self.cluster.write().mark_healthy(node_id);
                resp.json::<AppendEntriesResponse>()
                    .await
                    .map_err(|e| RpcError::DeserializationError(e.to_string()))
            }
            Ok(resp) => {
                Err(RpcError::ServerError(resp.status().to_string()))
            }
            Err(e) => {
                Err(RpcError::NetworkError(e.to_string()))
            }
        }
    }

    /// Forward insert to a remote node
    pub async fn forward_insert(
        &self,
        node_id: &NodeId,
        request: &ForwardInsertRequest,
    ) -> Result<ForwardInsertResponse, RpcError> {
        let node_info = {
            let cluster = self.cluster.read();
            cluster
                .get_node_info(node_id)
                .ok_or(RpcError::NodeNotFound(node_id.clone()))?
        };

        let url = format!("http://{}/internal/forward/insert", node_info.address);

        let result = self.http_client
            .post(&url)
            .json(request)
            .send()
            .await;

        match result {
            Ok(resp) if resp.status().is_success() => {
                self.cluster.write().mark_healthy(node_id);
                resp.json::<ForwardInsertResponse>()
                    .await
                    .map_err(|e| RpcError::DeserializationError(e.to_string()))
            }
            Ok(resp) => {
                self.cluster.write().mark_suspect(node_id);
                Err(RpcError::ServerError(resp.status().to_string()))
            }
            Err(e) => {
                self.cluster.write().mark_suspect(node_id);
                Err(RpcError::NetworkError(e.to_string()))
            }
        }
    }

    /// Forward search to a remote node
    pub async fn forward_search(
        &self,
        node_id: &NodeId,
        request: &ForwardSearchRequest,
    ) -> Result<ForwardSearchResponse, RpcError> {
        let node_info = {
            let cluster = self.cluster.read();
            cluster
                .get_node_info(node_id)
                .ok_or(RpcError::NodeNotFound(node_id.clone()))?
        };

        let url = format!("http://{}/internal/forward/search", node_info.address);

        let result = self.http_client
            .post(&url)
            .json(request)
            .send()
            .await;

        match result {
            Ok(resp) if resp.status().is_success() => {
                self.cluster.write().mark_healthy(node_id);
                resp.json::<ForwardSearchResponse>()
                    .await
                    .map_err(|e| RpcError::DeserializationError(e.to_string()))
            }
            Ok(resp) => {
                self.cluster.write().mark_suspect(node_id);
                Err(RpcError::ServerError(resp.status().to_string()))
            }
            Err(e) => {
                self.cluster.write().mark_suspect(node_id);
                Err(RpcError::NetworkError(e.to_string()))
            }
        }
    }

    /// Send heartbeat ping to check node health
    pub async fn heartbeat(
        &self,
        node_id: &NodeId,
        request: &HeartbeatRequest,
    ) -> Result<HeartbeatResponse, RpcError> {
        let node_info = {
            let cluster = self.cluster.read();
            cluster
                .get_node_info(node_id)
                .ok_or(RpcError::NodeNotFound(node_id.clone()))?
        };

        let url = format!("http://{}/internal/heartbeat", node_info.address);

        let result = self.http_client
            .post(&url)
            .json(request)
            .send()
            .await;

        match result {
            Ok(resp) if resp.status().is_success() => {
                self.cluster.write().mark_healthy(node_id);
                resp.json::<HeartbeatResponse>()
                    .await
                    .map_err(|e| RpcError::DeserializationError(e.to_string()))
            }
            Ok(resp) => {
                self.cluster.write().mark_suspect(node_id);
                Err(RpcError::ServerError(resp.status().to_string()))
            }
            Err(e) => {
                self.cluster.write().mark_unreachable(node_id);
                Err(RpcError::NetworkError(e.to_string()))
            }
        }
    }
}

/// RPC error types
#[derive(Debug, Clone)]
pub enum RpcError {
    NetworkError(String),
    DeserializationError(String),
    ServerError(String),
    NodeNotFound(NodeId),
    Timeout,
}

impl std::fmt::Display for RpcError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RpcError::NetworkError(msg) => write!(f, "Network error: {}", msg),
            RpcError::DeserializationError(msg) => write!(f, "Deserialization error: {}", msg),
            RpcError::ServerError(msg) => write!(f, "Server error: {}", msg),
            RpcError::NodeNotFound(node) => write!(f, "Node not found: {}", node.0),
            RpcError::Timeout => write!(f, "RPC call timed out"),
        }
    }
}

impl std::error::Error for RpcError {}
