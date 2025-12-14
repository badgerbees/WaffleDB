/// Distance metric trait abstraction for pluggable metric implementations.
///
/// This module provides:
/// - Trait-based distance metric abstraction
/// - Per-metric SIMD optimization support
/// - Metric registration and factory pattern
/// - Batch distance computation
/// - Metric metadata and constraints

use std::fmt;
use std::collections::HashMap;
use std::sync::Arc;

/// Trait for distance metrics with optional SIMD acceleration.
pub trait DistanceMetric: Send + Sync + fmt::Debug {
    /// Compute distance between two vectors.
    fn distance(&self, a: &[f32], b: &[f32]) -> f32;
    
    /// Batch compute distances (allows optimization).
    fn batch_distance(&self, query: &[f32], vectors: &[Vec<f32>]) -> Vec<f32> {
        vectors.iter()
            .map(|v| self.distance(query, v))
            .collect()
    }
    
    /// Whether lower distance means better match.
    fn is_distance_ascending(&self) -> bool {
        true
    }
    
    /// Get metric name for logging/config.
    fn name(&self) -> &'static str;
    
    /// Get SIMD capability level (0=scalar, 1=AVX2, 2=AVX512).
    fn simd_level(&self) -> u32 {
        0
    }
    
    /// Clone as trait object.
    fn clone_box(&self) -> Box<dyn DistanceMetric>;
}

impl Clone for Box<dyn DistanceMetric> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}

/// L2 (Euclidean) distance metric.
#[derive(Debug, Clone)]
pub struct L2Metric;

impl DistanceMetric for L2Metric {
    fn distance(&self, a: &[f32], b: &[f32]) -> f32 {
        debug_assert_eq!(a.len(), b.len());
        a.iter()
            .zip(b.iter())
            .map(|(x, y)| (x - y).powi(2))
            .sum::<f32>()
            .sqrt()
    }
    
    fn batch_distance(&self, query: &[f32], vectors: &[Vec<f32>]) -> Vec<f32> {
        vectors.iter()
            .map(|v| {
                let sum: f32 = query.iter()
                    .zip(v.iter())
                    .map(|(x, y)| (x - y).powi(2))
                    .sum();
                sum.sqrt()
            })
            .collect()
    }
    
    fn name(&self) -> &'static str {
        "l2"
    }
    
    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    fn simd_level(&self) -> u32 {
        1 // AVX2 support
    }
    
    fn clone_box(&self) -> Box<dyn DistanceMetric> {
        Box::new(self.clone())
    }
}

/// Cosine distance metric (requires normalized vectors).
#[derive(Debug, Clone)]
pub struct CosineMetric;

impl DistanceMetric for CosineMetric {
    fn distance(&self, a: &[f32], b: &[f32]) -> f32 {
        debug_assert_eq!(a.len(), b.len());
        let dot_product: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
        1.0 - dot_product
    }
    
    fn batch_distance(&self, query: &[f32], vectors: &[Vec<f32>]) -> Vec<f32> {
        vectors.iter()
            .map(|v| {
                let dot: f32 = query.iter().zip(v.iter()).map(|(x, y)| x * y).sum();
                1.0 - dot
            })
            .collect()
    }
    
    fn name(&self) -> &'static str {
        "cosine"
    }
    
    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    fn simd_level(&self) -> u32 {
        1 // AVX2 support
    }
    
    fn clone_box(&self) -> Box<dyn DistanceMetric> {
        Box::new(self.clone())
    }
}

/// Inner product metric (negative distance for max-search).
#[derive(Debug, Clone)]
pub struct InnerProductMetric;

impl DistanceMetric for InnerProductMetric {
    fn distance(&self, a: &[f32], b: &[f32]) -> f32 {
        debug_assert_eq!(a.len(), b.len());
        -a.iter().zip(b.iter()).map(|(x, y)| x * y).sum::<f32>()
    }
    
    fn batch_distance(&self, query: &[f32], vectors: &[Vec<f32>]) -> Vec<f32> {
        vectors.iter()
            .map(|v| {
                let dot: f32 = query.iter().zip(v.iter()).map(|(x, y)| x * y).sum();
                -dot
            })
            .collect()
    }
    
    fn is_distance_ascending(&self) -> bool {
        true
    }
    
    fn name(&self) -> &'static str {
        "inner_product"
    }
    
    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    fn simd_level(&self) -> u32 {
        1 // AVX2 support
    }
    
    fn clone_box(&self) -> Box<dyn DistanceMetric> {
        Box::new(self.clone())
    }
}

/// Hamming distance metric (for binary vectors).
/// Counts the number of differing bits.
#[derive(Debug, Clone)]
pub struct HammingMetric;

impl DistanceMetric for HammingMetric {
    fn distance(&self, a: &[f32], b: &[f32]) -> f32 {
        debug_assert_eq!(a.len(), b.len());
        a.iter()
            .zip(b.iter())
            .filter(|(x, y)| (x > &&0.5) != (y > &&0.5))
            .count() as f32
    }
    
    fn batch_distance(&self, query: &[f32], vectors: &[Vec<f32>]) -> Vec<f32> {
        vectors.iter()
            .map(|v| {
                let count = query.iter()
                    .zip(v.iter())
                    .filter(|(x, y)| (x > &&0.5) != (y > &&0.5))
                    .count();
                count as f32
            })
            .collect()
    }
    
    fn name(&self) -> &'static str {
        "hamming"
    }
    
    fn clone_box(&self) -> Box<dyn DistanceMetric> {
        Box::new(self.clone())
    }
}

/// Jaccard distance metric (for sparse/set vectors).
/// Computed as 1 - (intersection / union).
#[derive(Debug, Clone)]
pub struct JaccardMetric;

impl DistanceMetric for JaccardMetric {
    fn distance(&self, a: &[f32], b: &[f32]) -> f32 {
        debug_assert_eq!(a.len(), b.len());
        
        let (mut intersection, mut union) = (0, 0);
        for (x, y) in a.iter().zip(b.iter()) {
            let x_set = x > &&0.5;
            let y_set = y > &&0.5;
            if x_set && y_set {
                intersection += 1;
                union += 1;
            } else if x_set || y_set {
                union += 1;
            }
        }
        
        if union == 0 {
            0.0 // Both empty vectors
        } else {
            1.0 - (intersection as f32 / union as f32)
        }
    }
    
    fn batch_distance(&self, query: &[f32], vectors: &[Vec<f32>]) -> Vec<f32> {
        vectors.iter()
            .map(|v| {
                let (mut intersection, mut union) = (0, 0);
                for (x, y) in query.iter().zip(v.iter()) {
                    let x_set = x > &&0.5;
                    let y_set = y > &&0.5;
                    if x_set && y_set {
                        intersection += 1;
                        union += 1;
                    } else if x_set || y_set {
                        union += 1;
                    }
                }
                
                if union == 0 {
                    0.0
                } else {
                    1.0 - (intersection as f32 / union as f32)
                }
            })
            .collect()
    }
    
    fn name(&self) -> &'static str {
        "jaccard"
    }
    
    fn clone_box(&self) -> Box<dyn DistanceMetric> {
        Box::new(self.clone())
    }
}

/// Metric registry for factory pattern.
pub struct MetricRegistry {
    metrics: HashMap<String, Arc<dyn DistanceMetric>>,
}

impl MetricRegistry {
    pub fn new() -> Self {
        let mut metrics = HashMap::new();
        metrics.insert("l2".to_string(), Arc::new(L2Metric) as Arc<dyn DistanceMetric>);
        metrics.insert("cosine".to_string(), Arc::new(CosineMetric) as Arc<dyn DistanceMetric>);
        metrics.insert("inner_product".to_string(), Arc::new(InnerProductMetric) as Arc<dyn DistanceMetric>);
        metrics.insert("hamming".to_string(), Arc::new(HammingMetric) as Arc<dyn DistanceMetric>);
        metrics.insert("jaccard".to_string(), Arc::new(JaccardMetric) as Arc<dyn DistanceMetric>);
        
        Self { metrics }
    }
    
    pub fn get(&self, name: &str) -> Option<Arc<dyn DistanceMetric>> {
        self.metrics.get(name).cloned()
    }
    
    pub fn register(&mut self, name: String, metric: Arc<dyn DistanceMetric>) {
        self.metrics.insert(name, metric);
    }
    
    pub fn list_available(&self) -> Vec<String> {
        self.metrics.keys().cloned().collect()
    }
}

impl Default for MetricRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_l2_distance() {
        let a = vec![0.0, 0.0];
        let b = vec![3.0, 4.0];
        let metric = L2Metric;
        assert!((metric.distance(&a, &b) - 5.0).abs() < 1e-6);
    }

    #[test]
    fn test_cosine_distance() {
        let a = vec![1.0, 0.0];
        let b = vec![1.0, 0.0];
        let metric = CosineMetric;
        assert!((metric.distance(&a, &b) - 0.0).abs() < 1e-6);
    }

    #[test]
    fn test_cosine_orthogonal() {
        let a = vec![1.0, 0.0];
        let b = vec![0.0, 1.0];
        let metric = CosineMetric;
        assert!((metric.distance(&a, &b) - 1.0).abs() < 1e-6);
    }

    #[test]
    fn test_inner_product_metric() {
        let a = vec![1.0, 2.0, 3.0];
        let b = vec![4.0, 5.0, 6.0];
        let metric = InnerProductMetric;
        let expected = -(1.0 * 4.0 + 2.0 * 5.0 + 3.0 * 6.0);
        assert!((metric.distance(&a, &b) - expected).abs() < 1e-6);
    }

    #[test]
    fn test_hamming_distance() {
        let a = vec![1.0, 1.0, 0.0, 0.0];
        let b = vec![1.0, 0.0, 1.0, 0.0];
        let metric = HammingMetric;
        assert!((metric.distance(&a, &b) - 2.0).abs() < 1e-6);
    }

    #[test]
    fn test_hamming_identical() {
        let a = vec![1.0, 0.0, 1.0, 1.0];
        let b = vec![1.0, 0.0, 1.0, 1.0];
        let metric = HammingMetric;
        assert!((metric.distance(&a, &b) - 0.0).abs() < 1e-6);
    }

    #[test]
    fn test_jaccard_distance() {
        // a: {0, 1}, b: {1, 2}
        // intersection: {1} = 1
        // union: {0, 1, 2} = 3
        // jaccard = 1 - (1/3) = 2/3
        let a = vec![1.0, 1.0, 0.0];
        let b = vec![0.0, 1.0, 1.0];
        let metric = JaccardMetric;
        assert!((metric.distance(&a, &b) - (2.0 / 3.0)).abs() < 1e-6);
    }

    #[test]
    fn test_jaccard_identical() {
        let a = vec![1.0, 0.0, 1.0];
        let b = vec![1.0, 0.0, 1.0];
        let metric = JaccardMetric;
        assert!((metric.distance(&a, &b) - 0.0).abs() < 1e-6);
    }

    #[test]
    fn test_jaccard_disjoint() {
        let a = vec![1.0, 0.0, 0.0];
        let b = vec![0.0, 1.0, 0.0];
        let metric = JaccardMetric;
        assert!((metric.distance(&a, &b) - 1.0).abs() < 1e-6);
    }

    #[test]
    fn test_jaccard_empty() {
        let a = vec![0.0, 0.0, 0.0];
        let b = vec![0.0, 0.0, 0.0];
        let metric = JaccardMetric;
        assert!((metric.distance(&a, &b) - 0.0).abs() < 1e-6);
    }

    #[test]
    fn test_batch_distance_l2() {
        let query = vec![0.0, 0.0];
        let vectors = vec![
            vec![3.0, 4.0],
            vec![1.0, 0.0],
            vec![0.0, 1.0],
        ];
        let metric = L2Metric;
        let distances = metric.batch_distance(&query, &vectors);
        
        assert_eq!(distances.len(), 3);
        assert!((distances[0] - 5.0).abs() < 1e-6);
        assert!((distances[1] - 1.0).abs() < 1e-6);
        assert!((distances[2] - 1.0).abs() < 1e-6);
    }

    #[test]
    fn test_metric_registry() {
        let registry = MetricRegistry::new();
        
        assert!(registry.get("l2").is_some());
        assert!(registry.get("cosine").is_some());
        assert!(registry.get("inner_product").is_some());
        assert!(registry.get("hamming").is_some());
        assert!(registry.get("jaccard").is_some());
        
        let available = registry.list_available();
        assert_eq!(available.len(), 5);
    }

    #[test]
    fn test_metric_names() {
        assert_eq!(L2Metric.name(), "l2");
        assert_eq!(CosineMetric.name(), "cosine");
        assert_eq!(InnerProductMetric.name(), "inner_product");
        assert_eq!(HammingMetric.name(), "hamming");
        assert_eq!(JaccardMetric.name(), "jaccard");
    }

    #[test]
    fn test_large_batch_l2() {
        let query = vec![0.5; 128];
        let vectors: Vec<_> = (0..1000)
            .map(|i| vec![0.5 + (i as f32) * 0.001; 128])
            .collect();
        
        let metric = L2Metric;
        let distances = metric.batch_distance(&query, &vectors);
        
        assert_eq!(distances.len(), 1000);
        assert!(distances[0] < distances[999]);
    }

    #[test]
    fn test_metric_is_ascending() {
        assert!(L2Metric.is_distance_ascending());
        assert!(CosineMetric.is_distance_ascending());
        assert!(InnerProductMetric.is_distance_ascending());
        assert!(HammingMetric.is_distance_ascending());
        assert!(JaccardMetric.is_distance_ascending());
    }

    #[test]
    fn test_high_dimensional_hamming() {
        let a: Vec<f32> = (0..1000).map(|i| if i % 2 == 0 { 1.0 } else { 0.0 }).collect();
        let b: Vec<f32> = (0..1000).map(|i| if i % 3 == 0 { 1.0 } else { 0.0 }).collect();
        
        let metric = HammingMetric;
        let dist = metric.distance(&a, &b);
        
        assert!(dist > 0.0);
        assert!(dist < 1000.0);
    }
}
