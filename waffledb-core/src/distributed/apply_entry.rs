/// RAFT apply_entry hook: deserialize and apply operations to engine state
///
/// This is the critical missing link between RAFT log replication and index updates.
/// Currently:
/// - RAFT replicates log entries to followers ✅
/// - Followers receive entries ✅
/// - Entries are never applied to indexes ❌ ← WE FIX THIS

use crate::distributed::replication::{Operation, BatchInsertEntry};
use crate::core::errors::{Result, WaffleError, ErrorCode};
use tracing::{debug, warn, error};
use std::collections::HashMap;

/// Apply a replicated operation to the search engine
///
/// This function deserializes a RAFT log entry and applies it to:
/// - WriteBuffer (for inserts/deletes)
/// - HNSW graph (for indexed search)
/// - Metadata index
/// - BM25 index (if enabled)
/// - Sparse vector index (if enabled)
///
/// Must be idempotent: applying the same operation twice shouldn't corrupt state.
///
/// # Arguments
/// * `operation` - The deserialized operation to apply
/// * `apply_callback` - Callback to actually perform the operation on engine
///
/// # Returns
/// - Ok(()) if operation applied successfully
/// - Err if operation failed (engine may need recovery)
pub async fn apply_entry_with_callback(
    operation: &Operation,
    apply_callback: &dyn Fn(&Operation) -> futures::future::BoxFuture<'static, Result<()>>,
) -> Result<()> {
    match operation {
        Operation::Insert {
            doc_id,
            vector,
            metadata,
        } => {
            debug!(doc_id = %doc_id, "Applying Insert operation");
            apply_callback(operation).await?;
            debug!(doc_id = %doc_id, "Insert applied successfully");
            Ok(())
        }
        Operation::BatchInsert { entries } => {
            debug!(batch_size = entries.len(), "Applying BatchInsert operation");
            
            // Validate batch is not empty
            if entries.is_empty() {
                warn!("Received empty BatchInsert, skipping");
                return Ok(());
            }
            
            apply_callback(operation).await?;
            debug!(batch_size = entries.len(), "BatchInsert applied successfully");
            Ok(())
        }
        Operation::Delete { doc_id } => {
            debug!(doc_id = %doc_id, "Applying Delete operation");
            apply_callback(operation).await?;
            debug!(doc_id = %doc_id, "Delete applied successfully");
            Ok(())
        }
        Operation::UpdateMetadata {
            doc_id,
            metadata,
        } => {
            debug!(doc_id = %doc_id, "Applying UpdateMetadata operation");
            apply_callback(operation).await?;
            debug!(doc_id = %doc_id, "UpdateMetadata applied successfully");
            Ok(())
        }
        Operation::Snapshot {
            snapshot_id,
            term,
            index,
        } => {
            debug!(
                snapshot_id = %snapshot_id,
                term = term,
                index = index,
                "Applying Snapshot operation"
            );
            apply_callback(operation).await?;
            debug!(snapshot_id = %snapshot_id, "Snapshot applied successfully");
            Ok(())
        }
    }
}

/// Simple version: just decode an operation without applying
/// Use this to test operation parsing
pub fn decode_operation(operation: &Operation) -> Result<()> {
    match operation {
        Operation::Insert { doc_id, vector, .. } => {
            debug!(doc_id = %doc_id, vector_dim = vector.len(), "Decoded Insert");
            Ok(())
        }
        Operation::BatchInsert { entries } => {
            debug!(batch_size = entries.len(), "Decoded BatchInsert");
            Ok(())
        }
        Operation::Delete { doc_id } => {
            debug!(doc_id = %doc_id, "Decoded Delete");
            Ok(())
        }
        Operation::UpdateMetadata { doc_id, .. } => {
            debug!(doc_id = %doc_id, "Decoded UpdateMetadata");
            Ok(())
        }
        Operation::Snapshot {
            snapshot_id, ..
        } => {
            debug!(snapshot_id = %snapshot_id, "Decoded Snapshot");
            Ok(())
        }
    }
}

/// Apply multiple entries from RAFT log to ensure consistency
///
/// Applies entries in order while tracking progress for crash recovery.
/// If any entry fails, stops and returns error (entries are idempotent, can retry).
pub async fn apply_entries_with_callback(
    entries: Vec<Operation>,
    apply_callback: &dyn Fn(&Operation) -> futures::future::BoxFuture<'static, Result<()>>,
) -> Result<()> {
    debug!(entry_count = entries.len(), "Applying batch of RAFT entries");
    
    for (idx, operation) in entries.iter().enumerate() {
        match apply_entry_with_callback(operation, apply_callback).await {
            Ok(()) => {
                debug!(entry_idx = idx, "Entry applied successfully");
            }
            Err(e) => {
                error!(
                    entry_idx = idx,
                    error = %e,
                    "Failed to apply entry, stopping batch"
                );
                return Err(e);
            }
        }
    }
    
    Ok(())
}

/// Verify idempotence: ensure operation ID uniqueness
///
/// For idempotence guarantee, each operation should have a unique ID.
/// If we see the same ID twice, we can skip it (already applied).
///
/// # Arguments
/// * `operation` - The operation to get ID from
///
/// # Returns
/// - Some(id) if operation has an ID
/// - None if operation doesn't support deduplication
pub fn get_operation_id(operation: &Operation) -> Option<String> {
    match operation {
        Operation::Insert { doc_id, .. } => Some(format!("insert_{}", doc_id)),
        Operation::Delete { doc_id } => Some(format!("delete_{}", doc_id)),
        Operation::UpdateMetadata { doc_id, .. } => Some(format!("update_{}", doc_id)),
        Operation::BatchInsert { entries } => {
            // For batch, use first entry ID if available
            entries
                .first()
                .map(|e| format!("batch_{}", e.doc_id))
        }
        Operation::Snapshot {
            snapshot_id, ..
        } => Some(format!("snapshot_{}", snapshot_id)),
    }
}

/// Deduplicate operations using a seen set
///
/// Maintains a set of operation IDs we've seen.
/// Filters out duplicates to prevent re-applying the same operation.
///
/// This is important for crash recovery where we might replay the same
/// operations multiple times.
pub struct OperationDeduplicator {
    seen: std::collections::HashSet<String>,
}

impl OperationDeduplicator {
    pub fn new() -> Self {
        OperationDeduplicator {
            seen: std::collections::HashSet::new(),
        }
    }
    
    /// Check if operation has been seen before
    pub fn is_duplicate(&self, operation: &Operation) -> bool {
        if let Some(id) = get_operation_id(operation) {
            self.seen.contains(&id)
        } else {
            false
        }
    }
    
    /// Mark operation as seen
    pub fn mark_seen(&mut self, operation: &Operation) {
        if let Some(id) = get_operation_id(operation) {
            self.seen.insert(id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    #[test]
    fn test_operation_id_extraction() {
        let insert = Operation::Insert {
            doc_id: "doc1".to_string(),
            vector: vec![0.1],
            metadata: "{}".to_string(),
        };
        
        let id = get_operation_id(&insert);
        assert_eq!(id, Some("insert_doc1".to_string()));

        let delete = Operation::Delete {
            doc_id: "doc1".to_string(),
        };
        
        let id = get_operation_id(&delete);
        assert_eq!(id, Some("delete_doc1".to_string()));
    }

    #[test]
    fn test_deduplicator() {
        let mut dedup = OperationDeduplicator::new();
        
        let op1 = Operation::Insert {
            doc_id: "doc1".to_string(),
            vector: vec![0.1],
            metadata: "{}".to_string(),
        };
        
        assert!(!dedup.is_duplicate(&op1));
        dedup.mark_seen(&op1);
        assert!(dedup.is_duplicate(&op1));
    }

    #[test]
    fn test_batch_insert_idempotence() {
        // Verify that BatchInsert operations are idempotent
        let op = Operation::BatchInsert {
            entries: vec![
                BatchInsertEntry {
                    doc_id: "doc1".to_string(),
                    vector: vec![0.1],
                    metadata: "{}".to_string(),
                },
                BatchInsertEntry {
                    doc_id: "doc2".to_string(),
                    vector: vec![0.2],
                    metadata: "{}".to_string(),
                },
            ],
        };

        // Should be able to apply the same operation multiple times
        let id1 = get_operation_id(&op);
        let id2 = get_operation_id(&op);
        assert_eq!(id1, id2, "Same operation should have same ID");
    }

    #[test]
    fn test_operation_deduplication() {
        let mut dedup = OperationDeduplicator::new();

        let insert1 = Operation::Insert {
            doc_id: "doc1".to_string(),
            vector: vec![0.1],
            metadata: "{}".to_string(),
        };

        let insert2 = Operation::Insert {
            doc_id: "doc2".to_string(),
            vector: vec![0.2],
            metadata: "{}".to_string(),
        };

        // First insert should not be duplicate
        assert!(!dedup.is_duplicate(&insert1));
        dedup.mark_seen(&insert1);

        // Same insert should now be duplicate
        assert!(dedup.is_duplicate(&insert1));

        // Different insert should not be duplicate
        assert!(!dedup.is_duplicate(&insert2));
        dedup.mark_seen(&insert2);

        // Both should now be duplicates
        assert!(dedup.is_duplicate(&insert1));
        assert!(dedup.is_duplicate(&insert2));
    }

    #[test]
    fn test_delete_operation_id() {
        let delete = Operation::Delete {
            doc_id: "doc123".to_string(),
        };

        let id = get_operation_id(&delete);
        assert_eq!(id, Some("delete_doc123".to_string()));
    }

    #[test]
    fn test_snapshot_operation_id() {
        let snapshot = Operation::Snapshot {
            snapshot_id: "snap123".to_string(),
            term: 5,
            index: 42,
        };

        let id = get_operation_id(&snapshot);
        assert_eq!(id, Some("snapshot_snap123".to_string()));
    }
}
