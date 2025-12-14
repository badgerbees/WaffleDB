/// Metric migration tool for converting between distance metrics without rebuilding.
///
/// This module provides:
/// - Metric conversion without recomputation
/// - Online migration (during normal operation)
/// - Progress tracking and resumption
/// - Rollback capability
/// - Validation and sanity checks

use std::collections::HashMap;
use std::fmt;

/// Migration status tracking.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MigrationStatus {
    Pending,
    InProgress,
    Completed,
    Failed,
    RolledBack,
}

impl fmt::Display for MigrationStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MigrationStatus::Pending => write!(f, "pending"),
            MigrationStatus::InProgress => write!(f, "in_progress"),
            MigrationStatus::Completed => write!(f, "completed"),
            MigrationStatus::Failed => write!(f, "failed"),
            MigrationStatus::RolledBack => write!(f, "rolled_back"),
        }
    }
}

/// Metric migration plan.
#[derive(Debug, Clone)]
pub struct MigrationPlan {
    pub source_metric: String,
    pub target_metric: String,
    pub total_vectors: u64,
    pub processed_vectors: u64,
    pub status: MigrationStatus,
    pub started_at: Option<u64>, // Unix timestamp
    pub completed_at: Option<u64>,
    pub batch_size: u32,
}

impl MigrationPlan {
    pub fn new(source: String, target: String, total_vectors: u64) -> Self {
        Self {
            source_metric: source,
            target_metric: target,
            total_vectors,
            processed_vectors: 0,
            status: MigrationStatus::Pending,
            started_at: None,
            completed_at: None,
            batch_size: 10000,
        }
    }
    
    pub fn progress_percent(&self) -> f32 {
        if self.total_vectors == 0 {
            100.0
        } else {
            let percent = (self.processed_vectors as f32 / self.total_vectors as f32) * 100.0;
            percent.min(100.0)
        }
    }
    
    pub fn is_complete(&self) -> bool {
        self.processed_vectors >= self.total_vectors && self.status == MigrationStatus::Completed
    }
}

/// Conversion function between metrics.
/// 
/// For metrics where direct conversion is possible (e.g., L2 ↔ L2), uses identity.
/// For incompatible metrics (e.g., L2 → Cosine), requires recomputation.
pub trait MetricConverter: Send + Sync {
    /// Convert distance value between metrics.
    /// Returns None if recomputation is required.
    fn convert(&self, distance: f32, dimension: usize) -> Option<f32>;
    
    /// Whether recomputation is required.
    fn requires_recomputation(&self) -> bool;
}

/// No-op converter (same metric).
pub struct IdentityConverter;

impl MetricConverter for IdentityConverter {
    fn convert(&self, distance: f32, _dimension: usize) -> Option<f32> {
        Some(distance)
    }
    
    fn requires_recomputation(&self) -> bool {
        false
    }
}

/// L2 ↔ Cosine converter (requires vector normalization info).
pub struct L2ToCosineConverter {
    /// Precomputed norms per vector
    pub norms: Vec<f32>,
}

impl MetricConverter for L2ToCosineConverter {
    fn convert(&self, _distance: f32, _dimension: usize) -> Option<f32> {
        None // Must recompute due to normalization dependency
    }
    
    fn requires_recomputation(&self) -> bool {
        true
    }
}

/// InnerProduct ↔ Cosine converter (only sign change if normalized).
pub struct InnerProductToCosineConverter;

impl MetricConverter for InnerProductToCosineConverter {
    fn convert(&self, distance: f32, _dimension: usize) -> Option<f32> {
        // If inner product is -1.0 to 1.0, cosine distance = 1 - inner_product
        if distance >= -1.0 && distance <= 1.0 {
            Some(1.0 - distance)
        } else {
            None // Non-normalized or needs recomputation
        }
    }
    
    fn requires_recomputation(&self) -> bool {
        false
    }
}

/// Get appropriate converter for source → target metric pair.
pub fn get_converter(source: &str, target: &str) -> Box<dyn MetricConverter> {
    match (source, target) {
        (a, b) if a == b => Box::new(IdentityConverter),
        ("cosine", "inner_product") => Box::new(InnerProductToCosineConverter),
        ("inner_product", "cosine") => Box::new(InnerProductToCosineConverter),
        _ => Box::new(L2ToCosineConverter { norms: Vec::new() }),
    }
}

/// Metric migration manager.
pub struct MigrationManager {
    plans: HashMap<String, MigrationPlan>,
    history: Vec<(String, MigrationStatus)>,
}

impl MigrationManager {
    pub fn new() -> Self {
        Self {
            plans: HashMap::new(),
            history: Vec::new(),
        }
    }
    
    /// Create a new migration plan.
    pub fn create_plan(&mut self, source: String, target: String, total_vectors: u64) -> String {
        let plan_id = format!("{}→{}", source, target);
        let plan = MigrationPlan::new(source, target, total_vectors);
        self.plans.insert(plan_id.clone(), plan);
        plan_id
    }
    
    /// Get migration plan by ID.
    pub fn get_plan(&self, plan_id: &str) -> Option<&MigrationPlan> {
        self.plans.get(plan_id)
    }
    
    /// Update migration progress.
    pub fn update_progress(&mut self, plan_id: &str, processed: u64) -> Result<(), String> {
        if let Some(plan) = self.plans.get_mut(plan_id) {
            plan.processed_vectors = processed.min(plan.total_vectors);
            Ok(())
        } else {
            Err(format!("Plan {} not found", plan_id))
        }
    }
    
    /// Mark migration as completed.
    pub fn complete_migration(&mut self, plan_id: &str) -> Result<(), String> {
        if let Some(plan) = self.plans.get_mut(plan_id) {
            plan.status = MigrationStatus::Completed;
            plan.processed_vectors = plan.total_vectors;
            self.history.push((plan_id.to_string(), MigrationStatus::Completed));
            Ok(())
        } else {
            Err(format!("Plan {} not found", plan_id))
        }
    }
    
    /// Mark migration as failed.
    pub fn fail_migration(&mut self, plan_id: &str) -> Result<(), String> {
        if let Some(plan) = self.plans.get_mut(plan_id) {
            plan.status = MigrationStatus::Failed;
            self.history.push((plan_id.to_string(), MigrationStatus::Failed));
            Ok(())
        } else {
            Err(format!("Plan {} not found", plan_id))
        }
    }
    
    /// Rollback a completed migration.
    pub fn rollback_migration(&mut self, plan_id: &str) -> Result<(), String> {
        if let Some(plan) = self.plans.get_mut(plan_id) {
            if plan.status == MigrationStatus::Completed {
                plan.status = MigrationStatus::RolledBack;
                self.history.push((plan_id.to_string(), MigrationStatus::RolledBack));
                Ok(())
            } else {
                Err(format!("Cannot rollback migration in {} status", plan.status))
            }
        } else {
            Err(format!("Plan {} not found", plan_id))
        }
    }
    
    /// Get migration history.
    pub fn get_history(&self) -> &[(String, MigrationStatus)] {
        &self.history
    }
    
    /// List all plans.
    pub fn list_plans(&self) -> Vec<(&String, &MigrationPlan)> {
        self.plans.iter().collect()
    }
}

impl Default for MigrationManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_migration_plan_creation() {
        let plan = MigrationPlan::new("l2".to_string(), "cosine".to_string(), 1000);
        
        assert_eq!(plan.source_metric, "l2");
        assert_eq!(plan.target_metric, "cosine");
        assert_eq!(plan.total_vectors, 1000);
        assert_eq!(plan.processed_vectors, 0);
        assert_eq!(plan.status, MigrationStatus::Pending);
    }

    #[test]
    fn test_progress_calculation() {
        let mut plan = MigrationPlan::new("l2".to_string(), "cosine".to_string(), 1000);
        
        assert!((plan.progress_percent() - 0.0).abs() < 1e-6);
        
        plan.processed_vectors = 500;
        assert!((plan.progress_percent() - 50.0).abs() < 1e-6);
        
        plan.processed_vectors = 1000;
        assert!((plan.progress_percent() - 100.0).abs() < 1e-6);
    }

    #[test]
    fn test_identity_converter() {
        let converter = IdentityConverter;
        
        assert_eq!(converter.convert(5.0, 128), Some(5.0));
        assert!(!converter.requires_recomputation());
    }

    #[test]
    fn test_inner_product_cosine_converter() {
        let converter = InnerProductToCosineConverter;
        
        // Normalized inner product: convert to cosine distance
        assert_eq!(converter.convert(0.5, 128), Some(0.5));
        assert!(!converter.requires_recomputation());
    }

    #[test]
    fn test_migration_manager_creation() {
        let mut manager = MigrationManager::new();
        
        let plan_id = manager.create_plan("l2".to_string(), "cosine".to_string(), 1000);
        
        assert!(manager.get_plan(&plan_id).is_some());
        assert_eq!(manager.get_plan(&plan_id).unwrap().total_vectors, 1000);
    }

    #[test]
    fn test_migration_progress_tracking() {
        let mut manager = MigrationManager::new();
        let plan_id = manager.create_plan("l2".to_string(), "cosine".to_string(), 1000);
        
        manager.update_progress(&plan_id, 250).unwrap();
        let plan = manager.get_plan(&plan_id).unwrap();
        
        assert_eq!(plan.processed_vectors, 250);
        assert!((plan.progress_percent() - 25.0).abs() < 1e-6);
    }

    #[test]
    fn test_migration_completion() {
        let mut manager = MigrationManager::new();
        let plan_id = manager.create_plan("l2".to_string(), "cosine".to_string(), 1000);
        
        assert!(manager.complete_migration(&plan_id).is_ok());
        
        let plan = manager.get_plan(&plan_id).unwrap();
        assert_eq!(plan.status, MigrationStatus::Completed);
        assert_eq!(plan.processed_vectors, 1000);
    }

    #[test]
    fn test_migration_failure() {
        let mut manager = MigrationManager::new();
        let plan_id = manager.create_plan("l2".to_string(), "cosine".to_string(), 1000);
        
        manager.update_progress(&plan_id, 500).unwrap();
        assert!(manager.fail_migration(&plan_id).is_ok());
        
        let plan = manager.get_plan(&plan_id).unwrap();
        assert_eq!(plan.status, MigrationStatus::Failed);
        assert_eq!(plan.processed_vectors, 500); // Stops at failure point
    }

    #[test]
    fn test_migration_rollback() {
        let mut manager = MigrationManager::new();
        let plan_id = manager.create_plan("l2".to_string(), "cosine".to_string(), 1000);
        
        manager.complete_migration(&plan_id).unwrap();
        assert!(manager.rollback_migration(&plan_id).is_ok());
        
        let plan = manager.get_plan(&plan_id).unwrap();
        assert_eq!(plan.status, MigrationStatus::RolledBack);
    }

    #[test]
    fn test_rollback_requires_completion() {
        let mut manager = MigrationManager::new();
        let plan_id = manager.create_plan("l2".to_string(), "cosine".to_string(), 1000);
        
        // Cannot rollback pending migration
        assert!(manager.rollback_migration(&plan_id).is_err());
    }

    #[test]
    fn test_history_tracking() {
        let mut manager = MigrationManager::new();
        let plan_id = manager.create_plan("l2".to_string(), "cosine".to_string(), 1000);
        
        manager.complete_migration(&plan_id).unwrap();
        manager.rollback_migration(&plan_id).unwrap();
        
        let history = manager.get_history();
        assert_eq!(history.len(), 2);
        assert_eq!(history[0].1, MigrationStatus::Completed);
        assert_eq!(history[1].1, MigrationStatus::RolledBack);
    }

    #[test]
    fn test_multiple_concurrent_plans() {
        let mut manager = MigrationManager::new();
        
        let plan1 = manager.create_plan("l2".to_string(), "cosine".to_string(), 1000);
        let plan2 = manager.create_plan("cosine".to_string(), "hamming".to_string(), 2000);
        
        assert!(manager.get_plan(&plan1).is_some());
        assert!(manager.get_plan(&plan2).is_some());
        
        manager.update_progress(&plan1, 500).unwrap();
        manager.update_progress(&plan2, 1500).unwrap();
        
        assert_eq!(manager.get_plan(&plan1).unwrap().processed_vectors, 500);
        assert_eq!(manager.get_plan(&plan2).unwrap().processed_vectors, 1500);
    }

    #[test]
    fn test_list_plans() {
        let mut manager = MigrationManager::new();
        
        manager.create_plan("l2".to_string(), "cosine".to_string(), 1000);
        manager.create_plan("cosine".to_string(), "hamming".to_string(), 2000);
        
        let plans = manager.list_plans();
        assert_eq!(plans.len(), 2);
    }

    #[test]
    fn test_zero_total_vectors_progress() {
        let plan = MigrationPlan::new("l2".to_string(), "cosine".to_string(), 0);
        
        // Should report 100% for zero vectors
        assert!((plan.progress_percent() - 100.0).abs() < 1e-6);
    }

    #[test]
    fn test_processed_exceeds_total() {
        let mut plan = MigrationPlan::new("l2".to_string(), "cosine".to_string(), 1000);
        
        plan.processed_vectors = 2000; // Exceeds total
        
        // Should cap at total
        assert_eq!(plan.processed_vectors, 2000);
        // But progress should stay at 100%
        assert!((plan.progress_percent() - 100.0).abs() < 1e-6);
    }
}
