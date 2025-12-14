use crate::enterprise::models::{ApiKey, ApiKeyRole, TenantContext};
use rocksdb::DB;
use std::sync::Arc;

pub struct ApiKeyManager {
    db: Arc<DB>,
}

impl ApiKeyManager {
    pub fn new(db: Arc<DB>) -> Self {
        ApiKeyManager { db }
    }

    /// Store API key in database
    pub fn create(&self, api_key: &ApiKey) -> Result<(), Box<dyn std::error::Error>> {
        let key = format!("apikey:{}:{}", api_key.org_id, api_key.id);
        let value = serde_json::to_string(&api_key)?;
        self.db.put(key.as_bytes(), value.as_bytes())?;

        // Also store by hash for lookup
        let hash_key = format!("apikey_hash:{}", api_key.key_hash);
        self.db.put(hash_key.as_bytes(), api_key.id.as_bytes())?;

        Ok(())
    }

    /// Get API key by ID
    pub fn get_by_id(&self, org_id: &str, key_id: &str) -> Result<Option<ApiKey>, Box<dyn std::error::Error>> {
        let key = format!("apikey:{}:{}", org_id, key_id);
        match self.db.get(key.as_bytes())? {
            Some(data) => {
                let api_key = serde_json::from_slice(&data)?;
                Ok(Some(api_key))
            }
            None => Ok(None),
        }
    }

    /// Validate API key by hash
    pub fn validate(&self, key_hash: &str) -> Result<Option<ApiKey>, Box<dyn std::error::Error>> {
        let hash_key = format!("apikey_hash:{}", key_hash);
        match self.db.get(hash_key.as_bytes())? {
            Some(key_id_data) => {
                let key_id = String::from_utf8(key_id_data)?;
                // Need org_id to fully retrieve - this is a limitation
                // Better approach: store hash -> (org_id, key_id) mapping
                Ok(None) // Placeholder
            }
            None => Ok(None),
        }
    }

    /// Validate with plaintext key (used in request handlers)
    pub fn validate_key(&self, plaintext_key: &str) -> Result<Option<ApiKey>, Box<dyn std::error::Error>> {
        let key_hash = ApiKey::hash_key(plaintext_key);
        
        // Scan all keys to find matching hash (inefficient, but works for MVP)
        // In production: use separate hash -> (org_id, key_id) index
        let iter = self.db.iterator(rocksdb::IteratorMode::From(b"apikey:", rocksdb::Direction::Forward));
        
        for (_, v) in iter {
            if let Ok(api_key) = serde_json::from_slice::<ApiKey>(&v) {
                if api_key.key_hash == key_hash && api_key.is_active {
                    // Update last_used
                    let updated = ApiKey {
                        last_used: Some(chrono::Utc::now()),
                        ..api_key
                    };
                    let _ = self.create(&updated); // Update in DB
                    return Ok(Some(updated));
                }
            }
        }
        
        Ok(None)
    }

    /// List all API keys for an org
    pub fn list_for_org(&self, org_id: &str) -> Result<Vec<ApiKey>, Box<dyn std::error::Error>> {
        let prefix = format!("apikey:{}:", org_id);
        let iter = self.db.iterator(rocksdb::IteratorMode::From(prefix.as_bytes(), rocksdb::Direction::Forward));
        
        let mut keys = Vec::new();
        for (k, v) in iter {
            let key_str = String::from_utf8(k.to_vec())?;
            if !key_str.starts_with(&prefix) {
                break;
            }
            if let Ok(api_key) = serde_json::from_slice::<ApiKey>(&v) {
                keys.push(api_key);
            }
        }
        
        Ok(keys)
    }

    /// Delete API key
    pub fn delete(&self, org_id: &str, key_id: &str) -> Result<(), Box<dyn std::error::Error>> {
        let key = format!("apikey:{}:{}", org_id, key_id);
        
        // Get the key first to retrieve hash
        if let Some(api_key) = self.get_by_id(org_id, key_id)? {
            let hash_key = format!("apikey_hash:{}", api_key.key_hash);
            self.db.delete(hash_key.as_bytes())?;
        }
        
        self.db.delete(key.as_bytes())?;
        Ok(())
    }

    /// Revoke API key (soft delete)
    pub fn revoke(&self, org_id: &str, key_id: &str) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(mut api_key) = self.get_by_id(org_id, key_id)? {
            api_key.is_active = false;
            self.create(&api_key)?;
        }
        Ok(())
    }
}
