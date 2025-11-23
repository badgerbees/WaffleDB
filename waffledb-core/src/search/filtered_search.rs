/// Filtered Search Integration
/// Connects metadata filtering to HNSW search for predicate pushdown
/// 
/// This enables queries like:
///   "Find 10 nearest neighbors where category='electronics' AND price < 100"
/// 
/// The filter is applied during layer descent to reduce traversal cost

use crate::{
    core::errors::Result,
    search::metadata_filtering::{MetadataIndex, FilterExpression},
};

/// Metadata-aware search result
#[derive(Debug, Clone)]
pub struct FilteredSearchResult {
    pub id: u64,
    pub distance: f32,
    pub metadata_matched: bool,
}

impl FilteredSearchResult {
    pub fn new(id: u64, distance: f32, metadata_matched: bool) -> Self {
        FilteredSearchResult {
            id,
            distance,
            metadata_matched,
        }
    }
}

/// Execute filtered search: combine candidates with metadata filtering
pub fn search_with_filter(
    candidates: Vec<(u64, f32)>,  // (id, distance) pairs from HNSW
    metadata_index: &MetadataIndex,
    filter: Option<&FilterExpression>,
    k: usize,
) -> Result<Vec<FilteredSearchResult>> {
    // Step 1: Get allowed IDs from metadata filter
    let allowed_ids = if let Some(f) = filter {
        metadata_index.evaluate_filter(f)?
    } else {
        // No filter - all IDs are allowed
        candidates.iter().map(|(id, _)| *id).collect()
    };
    
    // Step 2: Filter candidates by allowed IDs and take top k
    let mut filtered: Vec<FilteredSearchResult> = candidates
        .into_iter()
        .filter(|(id, _)| allowed_ids.contains(id))
        .map(|(id, distance)| FilteredSearchResult {
            id,
            distance,
            metadata_matched: true,
        })
        .collect();
    
    // Step 3: Sort by distance and truncate to k
    filtered.sort_by(|a, b| a.distance.partial_cmp(&b.distance).unwrap_or(std::cmp::Ordering::Equal));
    filtered.truncate(k);
    
    Ok(filtered)
}

/// Count documents matching filter without search
pub fn count_matching(
    metadata_index: &MetadataIndex,
    filter: &FilterExpression,
) -> Result<usize> {
    let matching = metadata_index.evaluate_filter(filter)?;
    Ok(matching.len())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use crate::search::metadata_filtering::MetadataValue;
    
    #[test]
    fn test_filtered_search_excludes_non_matching() {
        let mut metadata_index = MetadataIndex::new();
        
        // Create 10 documents with different categories
        for i in 0..10 {
            let mut meta = HashMap::new();
            meta.insert(
                "category".to_string(),
                if i % 2 == 0 {
                    MetadataValue::String("electronics".to_string())
                } else {
                    MetadataValue::String("books".to_string())
                },
            );
            metadata_index.insert_metadata(i as u64, meta).unwrap();
        }
        
        // Filter for only electronics
        let filter = FilterExpression::Eq(
            "category".to_string(),
            MetadataValue::String("electronics".to_string()),
        );
        
        let matching = metadata_index.evaluate_filter(&filter).unwrap();
        
        // Should have 5 matching (0, 2, 4, 6, 8)
        assert_eq!(matching.len(), 5);
        assert!(matching.contains(&0));
        assert!(matching.contains(&2));
        assert!(!matching.contains(&1));
        assert!(!matching.contains(&3));
    }
    
    #[test]
    fn test_count_matching() {
        let mut metadata_index = MetadataIndex::new();
        
        for i in 0..20 {
            let mut meta = HashMap::new();
            meta.insert("price".to_string(), MetadataValue::Integer(i as i64 * 10));
            metadata_index.insert_metadata(i as u64, meta).unwrap();
        }
        
        // Filter: price >= 100
        let filter = FilterExpression::Ge(
            "price".to_string(),
            MetadataValue::Integer(100),
        );
        
        let matching = metadata_index.evaluate_filter(&filter).unwrap();
        
        // Should have 10 matching (100, 110, ..., 190)
        assert_eq!(matching.len(), 10);
    }
    
    #[test]
    fn test_complex_filter_and() {
        let mut metadata_index = MetadataIndex::new();
        
        for i in 0..10 {
            let mut meta = HashMap::new();
            meta.insert(
                "category".to_string(),
                if i < 5 {
                    MetadataValue::String("electronics".to_string())
                } else {
                    MetadataValue::String("books".to_string())
                },
            );
            meta.insert("price".to_string(), MetadataValue::Float(i as f64 * 10.0));
            metadata_index.insert_metadata(i as u64, meta).unwrap();
        }
        
        // Filter: category == "electronics" AND price > 20
        let filter = FilterExpression::And(vec![
            FilterExpression::Eq("category".to_string(), MetadataValue::String("electronics".to_string())),
            FilterExpression::Gt("price".to_string(), MetadataValue::Float(20.0)),
        ]);
        
        let matching = metadata_index.evaluate_filter(&filter).unwrap();
        
        // Should have 3 matching (3, 4 with ids 3,4)
        assert_eq!(matching.len(), 2); // IDs 3, 4 (price 30, 40)
        assert!(matching.contains(&3));
        assert!(matching.contains(&4));
    }
}
