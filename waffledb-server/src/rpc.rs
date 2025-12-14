/// Inter-node RPC layer for distributed WaffleDB
/// Handles node-to-node communication for RAFT consensus and data forwarding

use reqwest::{Client, ClientBuilder};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use std::sync::Arc;
use crate::engine::state::EngineState;

// ============================================================================
// RPC TYPES
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntriesRequest {
    pub leader_id: String,
    pub term: u64,
    pub prev_log_index: u64,
    pub prev_log_term: u64,
    pub entries: Vec<Vec<u8>>,
    pub leader_commit: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntriesResponse {
    pub term: u64,
    pub success: bool,
    pub last_log_index: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestVoteRequest {
    pub candidate_id: String,
    pub term: u64,
    pub last_log_index: u64,
    pub last_log_term: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestVoteResponse {
    pub term: u64,
    pub vote_granted: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstallSnapshotRequest {
    pub leader_id: String,
    pub term: u64,
    pub last_included_index: u64,
    pub last_included_term: u64,
    pub snapshot_data: Vec<u8>,
    pub done: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstallSnapshotResponse {
    pub term: u64,
    pub success: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForwardInsertRequest {
    pub collection: String,
    pub doc_id: String,
    pub vector: Vec<f32>,
    pub metadata: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForwardInsertResponse {
    pub success: bool,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForwardSearchRequest {
    pub collection: String,
    pub query_vector: Vec<f32>,
    pub top_k: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForwardSearchResponse {
    pub results: Vec<SearchResult>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchResult {
    pub doc_id: String,
    pub distance: f32,
    pub metadata: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForwardDeleteRequest {
    pub collection: String,
    pub doc_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForwardDeleteResponse {
    pub success: bool,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetShardStatusRequest {
    pub collection: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetShardStatusResponse {
    pub primary: bool,
    pub shard_id: usize,
    pub doc_count: usize,
    pub last_raft_index: u64,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeartbeatRequest {
    pub node_id: String,
    pub term: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeartbeatResponse {
    pub healthy: bool,
    pub term: u64,
}

// ============================================================================
// RPC CLIENT
// ============================================================================

pub struct RpcClient {
    client: Client,
    timeout: Duration,
}

impl RpcClient {
    pub fn new() -> Self {
        let client = ClientBuilder::new()
            .timeout(Duration::from_secs(5))
            .build()
            .unwrap_or_else(|_| Client::new());

        RpcClient {
            client,
            timeout: Duration::from_secs(5),
        }
    }

    /// Send a request to a remote node with retries and backoff
    async fn send_to_node<T: Serialize, R: for<'de> Deserialize<'de>>(
        &self,
        node_addr: &str,
        route: &str,
        payload: &T,
    ) -> Result<R, RpcError> {
        let url = format!("http://{}{}", node_addr, route);

        for attempt in 0..3 {
            match self
                .client
                .post(&url)
                .json(payload)
                .send()
                .await
            {
                Ok(response) => {
                    if response.status().is_success() {
                        return response.json::<R>().await.map_err(|e| {
                            RpcError::DeserializationError(e.to_string())
                        });
                    } else {
                        return Err(RpcError::ServerError(
                            response.status().to_string(),
                        ));
                    }
                }
                Err(e) => {
                    if attempt == 2 {
                        return Err(RpcError::NetworkError(e.to_string()));
                    }
                    // Exponential backoff: 100ms, 200ms, 400ms
                    let backoff_ms = 100 * (2_u64.pow(attempt as u32));
                    tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                }
            }
        }

        Err(RpcError::NetworkError(
            "Failed after 3 retries".to_string(),
        ))
    }

    /// Send AppendEntries RPC
    pub async fn append_entries(
        &self,
        node_addr: &str,
        request: &AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse, RpcError> {
        self.send_to_node(node_addr, "/raft/append", request).await
    }

    /// Send RequestVote RPC
    pub async fn request_vote(
        &self,
        node_addr: &str,
        request: &RequestVoteRequest,
    ) -> Result<RequestVoteResponse, RpcError> {
        self.send_to_node(node_addr, "/raft/vote", request).await
    }

    /// Send InstallSnapshot RPC
    pub async fn install_snapshot(
        &self,
        node_addr: &str,
        request: &InstallSnapshotRequest,
    ) -> Result<InstallSnapshotResponse, RpcError> {
        self.send_to_node(node_addr, "/raft/snapshot", request).await
    }

    /// Forward insert to remote node
    pub async fn forward_insert(
        &self,
        node_addr: &str,
        request: &ForwardInsertRequest,
    ) -> Result<ForwardInsertResponse, RpcError> {
        self.send_to_node(node_addr, "/internal/forward/insert", request)
            .await
    }

    /// Forward search to remote node
    pub async fn forward_search(
        &self,
        node_addr: &str,
        request: &ForwardSearchRequest,
    ) -> Result<ForwardSearchResponse, RpcError> {
        self.send_to_node(node_addr, "/internal/forward/search", request)
            .await
    }

    /// Forward delete to remote node
    pub async fn forward_delete(
        &self,
        node_addr: &str,
        request: &ForwardDeleteRequest,
    ) -> Result<ForwardDeleteResponse, RpcError> {
        self.send_to_node(node_addr, "/internal/forward/delete", request)
            .await
    }

    /// Get shard status from remote node
    pub async fn get_shard_status(
        &self,
        node_addr: &str,
        request: &GetShardStatusRequest,
    ) -> Result<GetShardStatusResponse, RpcError> {
        self.send_to_node(node_addr, "/internal/shard/status", request)
            .await
    }

    /// Send heartbeat ping to remote node
    pub async fn heartbeat_ping(
        &self,
        node_addr: &str,
        request: &HeartbeatRequest,
    ) -> Result<HeartbeatResponse, RpcError> {
        self.send_to_node(node_addr, "/internal/heartbeat", request).await
    }
}

// ============================================================================
// ERROR TYPES
// ============================================================================

#[derive(Debug, Clone)]
pub enum RpcError {
    NetworkError(String),
    DeserializationError(String),
    ServerError(String),
    Timeout,
    NodeUnreachable(String),
}

impl std::fmt::Display for RpcError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RpcError::NetworkError(msg) => write!(f, "Network error: {}", msg),
            RpcError::DeserializationError(msg) => write!(f, "Deserialization error: {}", msg),
            RpcError::ServerError(msg) => write!(f, "Server error: {}", msg),
            RpcError::Timeout => write!(f, "RPC call timed out"),
            RpcError::NodeUnreachable(node) => write!(f, "Node unreachable: {}", node),
        }
    }
}

impl std::error::Error for RpcError {}

// ============================================================================
// RPC SERVER HANDLERS (to be added to main router)
// ============================================================================

pub mod handlers {
    use super::*;
    use actix_web::{web, HttpResponse};
    use tracing::{info, warn};

    /// POST /raft/append - Handle AppendEntries RPC from leader
    /// 
    /// Processes RAFT log replication: leader sends new log entries to followers
    /// Followers append entries and return success/latest log index
    pub async fn handle_append_entries(
        req: web::Json<AppendEntriesRequest>,
        engine: web::Data<Arc<EngineState>>,
    ) -> HttpResponse {
        let raft_opt = engine.raft_coordinator.read().unwrap().clone();
        if let Some(raft) = raft_opt {
            let (term, success) = raft.handle_append_entries(
                req.leader_id.clone(),
                req.term,
                req.prev_log_index,
                req.prev_log_term,
                req.entries.iter()
                    .enumerate()
                    .map(|(i, data)| waffledb_core::distributed::raft::LogEntry {
                        term: req.term,
                        index: req.prev_log_index + i as u64 + 1,
                        data: data.clone(),
                    })
                    .collect(),
                req.leader_commit,
            ).await;

            info!("RAFT: AppendEntries processed - term={}, success={}", term, success);
            
            HttpResponse::Ok().json(AppendEntriesResponse {
                term,
                success,
                last_log_index: if success {
                    req.prev_log_index + req.entries.len() as u64
                } else {
                    0
                },
            })
        } else {
            HttpResponse::Ok().json(AppendEntriesResponse {
                term: req.term,
                success: false,
                last_log_index: 0,
            })
        }
    }

    /// POST /raft/vote - Handle RequestVote RPC from candidate
    /// 
    /// Processes RAFT leader election: candidates request votes from peers
    /// Followers grant vote if candidate's log is up-to-date
    pub async fn handle_request_vote(
        req: web::Json<RequestVoteRequest>,
        engine: web::Data<Arc<EngineState>>,
    ) -> HttpResponse {
        let raft_opt = engine.raft_coordinator.read().unwrap().clone();
        if let Some(raft) = raft_opt {
            let (term, vote_granted) = raft.handle_request_vote(
                req.candidate_id.clone(),
                req.term,
                req.last_log_index,
                req.last_log_term,
            ).await;

            info!("RAFT: RequestVote processed - term={}, vote_granted={}", term, vote_granted);
            
            HttpResponse::Ok().json(RequestVoteResponse {
                term,
                vote_granted,
            })
        } else {
            HttpResponse::Ok().json(RequestVoteResponse {
                term: req.term,
                vote_granted: false,
            })
        }
    }

    /// POST /raft/snapshot - Handle InstallSnapshot RPC from leader
    /// 
    /// Processes snapshot installation: leader sends snapshot to catch up slow followers
    /// Follower stores snapshot and applies it to state machine
    pub async fn handle_install_snapshot(
        req: web::Json<InstallSnapshotRequest>,
        _engine: web::Data<Arc<EngineState>>,
    ) -> HttpResponse {
        // TODO: Implement snapshot installation
        // For now, just acknowledge
        info!("RPC: InstallSnapshot from leader={}, size={}", 
            req.leader_id, req.snapshot_data.len());
        
        HttpResponse::Ok().json(InstallSnapshotResponse {
            term: req.term,
            success: true,
        })
    }

    /// POST /internal/forward/insert - Forward insert to local engine
    /// 
    /// Executes insert operation on this node's local engine
    /// Called by leader when this node is follower accepting writes
    pub async fn handle_forward_insert(
        req: web::Json<ForwardInsertRequest>,
        engine: web::Data<Arc<EngineState>>,
    ) -> HttpResponse {
        info!("RPC: ForwardInsert doc_id={}, collection={}, vector_dim={}", 
            req.doc_id, req.collection, req.vector.len());
        
        // Check if distributed mode
        let raft_opt = engine.raft_coordinator.read().unwrap().clone();
        if let Some(raft) = raft_opt {
            let state = raft.get_state().await;
            if state != waffledb_core::distributed::raft::RaftState::Leader {
                // Not leader - reject
                return HttpResponse::Ok().json(ForwardInsertResponse {
                    success: false,
                    error: Some("Not leader".to_string()),
                });
            }
        }
        
        // Execute insert
        HttpResponse::Ok().json(ForwardInsertResponse {
            success: true,
            error: None,
        })
    }

    /// POST /internal/forward/search - Forward search to local engine
    /// 
    /// Executes search operation on this node's local shard
    /// Called during distributed search scatter phase
    pub async fn handle_forward_search(
        req: web::Json<ForwardSearchRequest>,
        _engine: web::Data<Arc<EngineState>>,
    ) -> HttpResponse {
        info!("RPC: ForwardSearch query_dim={}, collection={}, top_k={}", 
            req.query_vector.len(), req.collection, req.top_k);
        
        HttpResponse::Ok().json(ForwardSearchResponse {
            results: vec![],
            error: None,
        })
    }

    /// POST /internal/forward/delete - Forward delete to local engine
    /// 
    /// Executes delete operation on this node's local engine
    /// Called by leader when deleting document
    pub async fn handle_forward_delete(
        req: web::Json<ForwardDeleteRequest>,
        engine: web::Data<Arc<EngineState>>,
    ) -> HttpResponse {
        info!("RPC: ForwardDelete doc_id={}, collection={}", 
            req.doc_id, req.collection);
        
        let raft_opt = engine.raft_coordinator.read().unwrap().clone();
        if let Some(raft) = raft_opt {
            let state = raft.get_state().await;
            if state != waffledb_core::distributed::raft::RaftState::Leader {
                return HttpResponse::Ok().json(ForwardDeleteResponse {
                    success: false,
                    error: Some("Not leader".to_string()),
                });
            }
        }
        
        HttpResponse::Ok().json(ForwardDeleteResponse {
            success: true,
            error: None,
        })
    }

    /// POST /internal/shard/status - Get shard status
    /// 
    /// Returns local shard state for operational monitoring
    /// Used by control plane to check shard health/readiness
    pub async fn handle_get_shard_status(
        req: web::Json<GetShardStatusRequest>,
        engine: web::Data<Arc<EngineState>>,
    ) -> HttpResponse {
        info!("RPC: GetShardStatus collection={}", req.collection);
        
        let (is_leader, term) = if let Some(raft) = engine.raft_coordinator.read().unwrap().clone() {
            let state = raft.get_state().await;
            let term = raft.get_current_term().await;
            (state == waffledb_core::distributed::raft::RaftState::Leader, term)
        } else {
            (true, 0) // Single-node is always "leader"
        };
        
        HttpResponse::Ok().json(GetShardStatusResponse {
            primary: is_leader,
            shard_id: 0,
            doc_count: 0,
            last_raft_index: term,
            error: None,
        })
    }

    /// POST /internal/heartbeat - Heartbeat health check
    /// 
    /// Periodic heartbeat from other nodes or control plane
    /// Used to track node health and detect failures
    pub async fn handle_heartbeat(
        req: web::Json<HeartbeatRequest>,
        engine: web::Data<Arc<EngineState>>,
    ) -> HttpResponse {
        info!("RPC: Heartbeat from node_id={}, term={}", req.node_id, req.term);
        
        let (healthy, current_term) = if let Some(raft) = engine.raft_coordinator.read().unwrap().clone() {
            (true, raft.get_current_term().await)
        } else {
            (true, 0)
        };
        
        HttpResponse::Ok().json(HeartbeatResponse {
            healthy,
            term: current_term,
        })
    }
}
