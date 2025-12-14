use crate::enterprise::models::{AuditLog, AuditAction, AuditStatus};
use rocksdb::DB;
use std::sync::Arc;

pub struct AuditLogger {
    db: Arc<DB>,
}

impl AuditLogger {
    pub fn new(db: Arc<DB>) -> Self {
        AuditLogger { db }
    }

    /// Log an audit event
    pub fn log(&self, mut audit_log: AuditLog) -> Result<(), Box<dyn std::error::Error>> {
        let key = format!(
            "audit:{}:{}:{}",
            audit_log.org_id, audit_log.project_id, audit_log.timestamp.timestamp_millis()
        );
        let value = serde_json::to_string(&audit_log)?;
        self.db.put(key.as_bytes(), value.as_bytes())?;

        // Also index by action for quick queries
        let action_key = format!(
            "audit_action:{}:{}:{}:{}",
            audit_log.org_id, audit_log.project_id, audit_log.action, audit_log.id
        );
        self.db.put(action_key.as_bytes(), key.as_bytes())?;

        Ok(())
    }

    /// Get audit logs for a project (limited to recent logs)
    pub fn get_recent(&self, org_id: &str, project_id: &str, limit: usize) -> Result<Vec<AuditLog>, Box<dyn std::error::Error>> {
        let prefix = format!("audit:{}:{}:", org_id, project_id);
        let iter = self.db.iterator(rocksdb::IteratorMode::From(prefix.as_bytes(), rocksdb::Direction::Reverse));
        
        let mut logs = Vec::new();
        for (_, v) in iter.take(limit) {
            if let Ok(audit_log) = serde_json::from_slice::<AuditLog>(&v) {
                logs.push(audit_log);
            }
        }
        
        Ok(logs)
    }

    /// Get logs by action
    pub fn get_by_action(
        &self,
        org_id: &str,
        project_id: &str,
        action: &str,
        limit: usize,
    ) -> Result<Vec<AuditLog>, Box<dyn std::error::Error>> {
        let prefix = format!("audit_action:{}:{}:{}:", org_id, project_id, action);
        let iter = self.db.iterator(rocksdb::IteratorMode::From(prefix.as_bytes(), rocksdb::Direction::Reverse));
        
        let mut logs = Vec::new();
        for (_, audit_key_data) in iter.take(limit) {
            let audit_key = String::from_utf8(audit_key_data.to_vec())?;
            if let Ok(Some(data)) = self.db.get(audit_key.as_bytes()) {
                if let Ok(audit_log) = serde_json::from_slice::<AuditLog>(&data) {
                    logs.push(audit_log);
                }
            }
        }
        
        Ok(logs)
    }

    /// Get logs by user
    pub fn get_by_user(
        &self,
        org_id: &str,
        user_id: &str,
        limit: usize,
    ) -> Result<Vec<AuditLog>, Box<dyn std::error::Error>> {
        let prefix = format!("audit:{}:", org_id);
        let iter = self.db.iterator(rocksdb::IteratorMode::From(prefix.as_bytes(), rocksdb::Direction::Reverse));
        
        let mut logs = Vec::new();
        for (_, v) in iter {
            if logs.len() >= limit {
                break;
            }
            if let Ok(audit_log) = serde_json::from_slice::<AuditLog>(&v) {
                if audit_log.user_id == user_id {
                    logs.push(audit_log);
                }
            }
        }
        
        Ok(logs)
    }

    /// Retention: delete logs older than days
    pub fn cleanup_old_logs(&self, org_id: &str, days: i64) -> Result<u64, Box<dyn std::error::Error>> {
        let cutoff = chrono::Utc::now() - chrono::Duration::days(days);
        let prefix = format!("audit:{}:", org_id);
        let iter = self.db.iterator(rocksdb::IteratorMode::From(prefix.as_bytes(), rocksdb::Direction::Forward));
        
        let mut deleted = 0u64;
        for (k, v) in iter {
            if let Ok(audit_log) = serde_json::from_slice::<AuditLog>(&v) {
                if audit_log.timestamp < cutoff {
                    self.db.delete(&k)?;
                    deleted += 1;
                }
            }
        }
        
        Ok(deleted)
    }
}
