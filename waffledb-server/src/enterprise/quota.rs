use crate::enterprise::models::{TenantQuota, BillingTier};
use rocksdb::DB;
use std::sync::Arc;

pub struct QuotaManager {
    db: Arc<DB>,
}

impl QuotaManager {
    pub fn new(db: Arc<DB>) -> Self {
        QuotaManager { db }
    }

    /// Get or create quota for tenant
    pub fn get_or_create(&self, org_id: &str, project_id: &str, tier: BillingTier) -> Result<TenantQuota, Box<dyn std::error::Error>> {
        let key = format!("quota:{}:{}", org_id, project_id);
        
        match self.db.get(key.as_bytes())? {
            Some(data) => {
                let quota = serde_json::from_slice(&data)?;
                Ok(quota)
            }
            None => {
                let quota = match tier {
                    BillingTier::Free => TenantQuota::free(org_id.to_string(), project_id.to_string()),
                    BillingTier::Pro => TenantQuota::pro(org_id.to_string(), project_id.to_string()),
                    BillingTier::Enterprise => TenantQuota::enterprise(org_id.to_string(), project_id.to_string()),
                };
                
                let value = serde_json::to_string(&quota)?;
                self.db.put(key.as_bytes(), value.as_bytes())?;
                
                Ok(quota)
            }
        }
    }

    /// Update quota for tenant
    pub fn update(&self, quota: &TenantQuota) -> Result<(), Box<dyn std::error::Error>> {
        let key = format!("quota:{}:{}", quota.org_id, quota.project_id);
        let value = serde_json::to_string(&quota)?;
        self.db.put(key.as_bytes(), value.as_bytes())?;
        Ok(())
    }

    /// Track storage usage
    pub fn get_storage_usage(&self, org_id: &str, project_id: &str) -> Result<u64, Box<dyn std::error::Error>> {
        let key = format!("usage:storage:{}:{}", org_id, project_id);
        match self.db.get(key.as_bytes())? {
            Some(data) => {
                let usage_str = String::from_utf8(data)?;
                Ok(usage_str.parse().unwrap_or(0))
            }
            None => Ok(0),
        }
    }

    /// Increment storage usage
    pub fn add_storage(&self, org_id: &str, project_id: &str, bytes: u64) -> Result<(), Box<dyn std::error::Error>> {
        let current = self.get_storage_usage(org_id, project_id)?;
        let key = format!("usage:storage:{}:{}", org_id, project_id);
        self.db.put(key.as_bytes(), (current + bytes).to_string().as_bytes())?;
        Ok(())
    }

    /// Check if within quota
    pub fn check_quota(&self, org_id: &str, project_id: &str, tier: BillingTier) -> Result<bool, Box<dyn std::error::Error>> {
        let quota = self.get_or_create(org_id, project_id, tier)?;
        let storage_usage = self.get_storage_usage(org_id, project_id)?;
        
        Ok(storage_usage <= quota.max_storage_bytes)
    }

    /// Estimate quota remaining (percentage)
    pub fn get_remaining_percentage(&self, org_id: &str, project_id: &str, tier: BillingTier) -> Result<u32, Box<dyn std::error::Error>> {
        let quota = self.get_or_create(org_id, project_id, tier)?;
        let storage_usage = self.get_storage_usage(org_id, project_id)?;
        
        if quota.max_storage_bytes == 0 {
            return Ok(100);
        }
        
        let used_percent = ((storage_usage as f64 / quota.max_storage_bytes as f64) * 100.0) as u32;
        Ok(100_u32.saturating_sub(used_percent))
    }
}

pub struct TenantUsageMetrics {
    pub org_id: String,
    pub project_id: String,
    pub storage_bytes: u64,
    pub vector_count: u64,
    pub collection_count: u32,
    pub searches_today: u64,
    pub inserts_today: u64,
}

impl TenantUsageMetrics {
    pub fn new(org_id: String, project_id: String) -> Self {
        TenantUsageMetrics {
            org_id,
            project_id,
            storage_bytes: 0,
            vector_count: 0,
            collection_count: 0,
            searches_today: 0,
            inserts_today: 0,
        }
    }

    /// Store metrics in DB
    pub fn save(&self, db: &DB) -> Result<(), Box<dyn std::error::Error>> {
        let key = format!("metrics:{}:{}", self.org_id, self.project_id);
        let value = serde_json::to_string(&self)?;
        db.put(key.as_bytes(), value.as_bytes())?;
        Ok(())
    }

    /// Load metrics from DB
    pub fn load(db: &DB, org_id: &str, project_id: &str) -> Result<Option<Self>, Box<dyn std::error::Error>> {
        let key = format!("metrics:{}:{}", org_id, project_id);
        match db.get(key.as_bytes())? {
            Some(data) => {
                let metrics = serde_json::from_slice(&data)?;
                Ok(Some(metrics))
            }
            None => Ok(None),
        }
    }
}

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
impl Serialize for TenantUsageMetrics {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeMap;
        let mut map = serializer.serialize_map(Some(7))?;
        map.serialize_entry("org_id", &self.org_id)?;
        map.serialize_entry("project_id", &self.project_id)?;
        map.serialize_entry("storage_bytes", &self.storage_bytes)?;
        map.serialize_entry("vector_count", &self.vector_count)?;
        map.serialize_entry("collection_count", &self.collection_count)?;
        map.serialize_entry("searches_today", &self.searches_today)?;
        map.serialize_entry("inserts_today", &self.inserts_today)?;
        map.end()
    }
}

impl<'de> Deserialize<'de> for TenantUsageMetrics {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::{self, MapAccess, Visitor};
        use std::fmt;

        struct MetricsVisitor;

        impl<'de> Visitor<'de> for MetricsVisitor {
            type Value = TenantUsageMetrics;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("struct TenantUsageMetrics")
            }

            fn visit_map<M>(self, mut map: M) -> Result<TenantUsageMetrics, M::Error>
            where
                M: MapAccess<'de>,
            {
                let mut org_id = None;
                let mut project_id = None;
                let mut storage_bytes = None;
                let mut vector_count = None;
                let mut collection_count = None;
                let mut searches_today = None;
                let mut inserts_today = None;

                while let Some(key) = map.next_key()? {
                    match key {
                        "org_id" => org_id = Some(map.next_value()?),
                        "project_id" => project_id = Some(map.next_value()?),
                        "storage_bytes" => storage_bytes = Some(map.next_value()?),
                        "vector_count" => vector_count = Some(map.next_value()?),
                        "collection_count" => collection_count = Some(map.next_value()?),
                        "searches_today" => searches_today = Some(map.next_value()?),
                        "inserts_today" => inserts_today = Some(map.next_value()?),
                        _ => {
                            let _ = map.next_value::<de::IgnoredAny>()?;
                        }
                    }
                }

                Ok(TenantUsageMetrics {
                    org_id: org_id.ok_or_else(|| de::Error::missing_field("org_id"))?,
                    project_id: project_id.ok_or_else(|| de::Error::missing_field("project_id"))?,
                    storage_bytes: storage_bytes.ok_or_else(|| de::Error::missing_field("storage_bytes"))?,
                    vector_count: vector_count.ok_or_else(|| de::Error::missing_field("vector_count"))?,
                    collection_count: collection_count.ok_or_else(|| de::Error::missing_field("collection_count"))?,
                    searches_today: searches_today.ok_or_else(|| de::Error::missing_field("searches_today"))?,
                    inserts_today: inserts_today.ok_or_else(|| de::Error::missing_field("inserts_today"))?,
                })
            }
        }

        deserializer.deserialize_map(MetricsVisitor)
    }
}
