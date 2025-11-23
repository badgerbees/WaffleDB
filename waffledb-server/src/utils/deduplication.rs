/// Duplicate vector detection and handling
/// 
/// Provides content-based deduplication to detect duplicate vectors
/// and apply configured policies (reject, overwrite, skip)

use waffledb_core::vector::types::Vector;
use std::collections::HashSet;

/// Duplicate handling policies
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DuplicatePolicy {
    /// Reject insert if duplicate found
    Reject,
    /// Overwrite existing vector
    Overwrite,
    /// Skip this insert
    Skip,
}

impl DuplicatePolicy {
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "reject" => Some(DuplicatePolicy::Reject),
            "overwrite" => Some(DuplicatePolicy::Overwrite),
            "skip" => Some(DuplicatePolicy::Skip),
            _ => None,
        }
    }
}

/// Calculate vector content hash for deduplication
/// Uses a simple sum of components as fast hash (not cryptographic)
/// For production: use xxhash or similar
pub fn vector_hash(vector: &Vector) -> u64 {
    let mut hash: u64 = 5381;
    for &val in &vector.data {
        let bytes = val.to_le_bytes();
        for &byte in &bytes {
            hash = hash.wrapping_mul(33).wrapping_add(byte as u64);
        }
    }
    hash
}

/// Detect vectors that are identical (same dimension and values)
pub fn is_vector_duplicate(v1: &Vector, v2: &Vector) -> bool {
    if v1.dim() != v2.dim() {
        return false;
    }
    
    v1.data.iter().zip(v2.data.iter()).all(|(a, b)| (a - b).abs() < 1e-6)
}

/// Detect approximately duplicate vectors using cosine similarity
/// Returns true if similarity > threshold (0.99 = very similar)
pub fn is_approximate_duplicate(v1: &Vector, v2: &Vector, similarity_threshold: f32) -> bool {
    if v1.dim() != v2.dim() {
        return false;
    }
    
    // Calculate cosine similarity
    let mut dot_product = 0.0;
    let mut norm_v1 = 0.0;
    let mut norm_v2 = 0.0;
    
    for (a, b) in v1.data.iter().zip(v2.data.iter()) {
        dot_product += a * b;
        norm_v1 += a * a;
        norm_v2 += b * b;
    }
    
    let magnitude = (norm_v1 * norm_v2).sqrt();
    if magnitude == 0.0 {
        return norm_v1 == 0.0 && norm_v2 == 0.0;
    }
    
    let similarity = dot_product / magnitude;
    similarity >= similarity_threshold
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_exact_duplicate_detection() {
        let v1 = Vector::new(vec![1.0, 2.0, 3.0]);
        let v2 = Vector::new(vec![1.0, 2.0, 3.0]);
        let v3 = Vector::new(vec![1.0, 2.0, 3.1]);
        
        assert!(is_vector_duplicate(&v1, &v2));
        assert!(!is_vector_duplicate(&v1, &v3));
    }
    
    #[test]
    fn test_approximate_duplicate_detection() {
        let v1 = Vector::new(vec![1.0, 0.0, 0.0]);
        let v2 = Vector::new(vec![0.99999, 0.0, 0.0]);
        
        assert!(is_approximate_duplicate(&v1, &v2, 0.99));
    }
    
    #[test]
    fn test_duplicate_policy_parsing() {
        assert_eq!(DuplicatePolicy::from_str("reject"), Some(DuplicatePolicy::Reject));
        assert_eq!(DuplicatePolicy::from_str("overwrite"), Some(DuplicatePolicy::Overwrite));
        assert_eq!(DuplicatePolicy::from_str("skip"), Some(DuplicatePolicy::Skip));
        assert_eq!(DuplicatePolicy::from_str("invalid"), None);
    }
}
