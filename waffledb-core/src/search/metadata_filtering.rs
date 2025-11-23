/// Metadata Filtering Module for WaffleDB
/// 
/// Provides:
/// - Inverted indices for string fields (fast exact/prefix matching)
/// - Range indices for numeric fields (fast range queries)
/// - Boolean expression combining (AND/OR/NOT)
/// - Predicate pushdown (filter before HNSW traversal to reduce search space)
/// 
/// This is the #1 feature that Qdrant users rely on for query expressiveness

use std::collections::{HashMap, BTreeMap, HashSet};
use crate::core::errors::{Result, WaffleError, ErrorCode};
use serde::{Serialize, Deserialize};

/// Metadata value type (supports all common types)
#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum MetadataValue {
    String(String),
    Integer(i64),
    Float(f64),
    Boolean(bool),
    StringList(Vec<String>),
}

impl MetadataValue {
    pub fn as_string(&self) -> Option<&str> {
        match self {
            MetadataValue::String(s) => Some(s),
            _ => None,
        }
    }
    
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            MetadataValue::Integer(i) => Some(*i),
            _ => None,
        }
    }
    
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            MetadataValue::Float(f) => Some(*f),
            MetadataValue::Integer(i) => Some(*i as f64), // Also support integers
            _ => None,
        }
    }
}

/// Filter expressions for combining predicates
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FilterExpression {
    /// Exact match: field == value
    Eq(String, MetadataValue),
    
    /// Not equal: field != value
    Ne(String, MetadataValue),
    
    /// Less than: field < value (numeric only)
    Lt(String, MetadataValue),
    
    /// Less than or equal: field <= value
    Le(String, MetadataValue),
    
    /// Greater than: field > value
    Gt(String, MetadataValue),
    
    /// Greater than or equal: field >= value
    Ge(String, MetadataValue),
    
    /// String prefix match: field starts_with prefix
    Prefix(String, String),
    
    /// Value in set: field in [values]
    In(String, Vec<MetadataValue>),
    
    /// String exists (non-null)
    Exists(String),
    
    /// Logical AND: all expressions must match
    And(Vec<FilterExpression>),
    
    /// Logical OR: at least one expression must match
    Or(Vec<FilterExpression>),
    
    /// Logical NOT: expression must not match
    Not(Box<FilterExpression>),
}

/// Metadata index for fast filtering
pub struct MetadataIndex {
    /// HashMap: field_name -> Map of (value -> set of document IDs)
    /// Used for exact match and range queries
    inverted_indices: HashMap<String, BTreeMap<String, HashSet<u64>>>,
    
    /// Numeric range index: field_name -> sorted vec of (value, doc_id)
    /// Used for range queries
    numeric_indices: HashMap<String, Vec<(f64, u64)>>,
    
    /// All metadata: doc_id -> field_name -> value
    all_metadata: HashMap<u64, HashMap<String, MetadataValue>>,
}

impl MetadataIndex {
    pub fn new() -> Self {
        MetadataIndex {
            inverted_indices: HashMap::new(),
            numeric_indices: HashMap::new(),
            all_metadata: HashMap::new(),
        }
    }
    
    /// Add metadata for a document
    pub fn insert_metadata(
        &mut self,
        doc_id: u64,
        metadata: HashMap<String, MetadataValue>,
    ) -> Result<()> {
        // Store full metadata
        self.all_metadata.insert(doc_id, metadata.clone());
        
        // Build indices
        for (field, value) in metadata {
            match &value {
                MetadataValue::String(s) => {
                    self.inverted_indices
                        .entry(field.clone())
                        .or_insert_with(BTreeMap::new)
                        .entry(s.clone())
                        .or_insert_with(HashSet::new)
                        .insert(doc_id);
                }
                MetadataValue::Integer(i) => {
                    let numeric_idx = self.numeric_indices
                        .entry(field.clone())
                        .or_insert_with(Vec::new);
                    numeric_idx.push((*i as f64, doc_id));
                    numeric_idx.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
                }
                MetadataValue::Float(f) => {
                    let numeric_idx = self.numeric_indices
                        .entry(field.clone())
                        .or_insert_with(Vec::new);
                    numeric_idx.push((*f, doc_id));
                    numeric_idx.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
                }
                MetadataValue::StringList(strings) => {
                    for s in strings {
                        self.inverted_indices
                            .entry(field.clone())
                            .or_insert_with(BTreeMap::new)
                            .entry(s.clone())
                            .or_insert_with(HashSet::new)
                            .insert(doc_id);
                    }
                }
                _ => {}
            }
        }
        
        Ok(())
    }
    
    /// Delete metadata for a document
    pub fn delete_metadata(&mut self, doc_id: u64) -> Result<()> {
        if let Some(metadata) = self.all_metadata.remove(&doc_id) {
            for (field, value) in metadata {
                match &value {
                    MetadataValue::String(s) => {
                        if let Some(field_index) = self.inverted_indices.get_mut(&field) {
                            if let Some(ids) = field_index.get_mut(s) {
                                ids.remove(&doc_id);
                            }
                        }
                    }
                    MetadataValue::StringList(strings) => {
                        for s in strings {
                            if let Some(field_index) = self.inverted_indices.get_mut(&field) {
                                if let Some(ids) = field_index.get_mut(s) {
                                    ids.remove(&doc_id);
                                }
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
        
        Ok(())
    }
    
    /// Evaluate filter expression to get matching document IDs
    pub fn evaluate_filter(&self, filter: &FilterExpression) -> Result<HashSet<u64>> {
        match filter {
            FilterExpression::Eq(field, value) => {
                self.evaluate_eq(field, value)
            }
            FilterExpression::Ne(field, value) => {
                self.evaluate_ne(field, value)
            }
            FilterExpression::Lt(field, value) => {
                self.evaluate_lt(field, value)
            }
            FilterExpression::Le(field, value) => {
                self.evaluate_le(field, value)
            }
            FilterExpression::Gt(field, value) => {
                self.evaluate_gt(field, value)
            }
            FilterExpression::Ge(field, value) => {
                self.evaluate_ge(field, value)
            }
            FilterExpression::Prefix(field, prefix) => {
                self.evaluate_prefix(field, prefix)
            }
            FilterExpression::In(field, values) => {
                self.evaluate_in(field, values)
            }
            FilterExpression::Exists(field) => {
                self.evaluate_exists(field)
            }
            FilterExpression::And(expressions) => {
                self.evaluate_and(expressions)
            }
            FilterExpression::Or(expressions) => {
                self.evaluate_or(expressions)
            }
            FilterExpression::Not(expr) => {
                self.evaluate_not(expr)
            }
        }
    }
    
    fn evaluate_eq(&self, field: &str, value: &MetadataValue) -> Result<HashSet<u64>> {
        match value {
            MetadataValue::String(s) => {
                if let Some(field_index) = self.inverted_indices.get(field) {
                    if let Some(ids) = field_index.get(s) {
                        return Ok(ids.clone());
                    }
                }
                Ok(HashSet::new())
            }
            _ => Err(WaffleError::WithCode {
                code: ErrorCode::ValidationFailed,
                message: "Can only perform exact match on string fields".to_string(),
            }),
        }
    }
    
    fn evaluate_ne(&self, field: &str, value: &MetadataValue) -> Result<HashSet<u64>> {
        // All docs except those matching the value
        let matching = self.evaluate_eq(field, value)?;
        let all: HashSet<u64> = self.all_metadata.keys().copied().collect();
        Ok(all.difference(&matching).copied().collect())
    }
    
    fn evaluate_lt(&self, field: &str, value: &MetadataValue) -> Result<HashSet<u64>> {
        let threshold = value.as_f64()
            .ok_or_else(|| WaffleError::WithCode {
                code: ErrorCode::ValidationFailed,
                message: "Range queries require numeric values".to_string(),
            })?;
        
        if let Some(numeric_idx) = self.numeric_indices.get(field) {
            return Ok(numeric_idx
                .iter()
                .filter(|(v, _)| v < &threshold)
                .map(|(_, id)| *id)
                .collect());
        }
        Ok(HashSet::new())
    }
    
    fn evaluate_le(&self, field: &str, value: &MetadataValue) -> Result<HashSet<u64>> {
        let threshold = value.as_f64()
            .ok_or_else(|| WaffleError::WithCode {
                code: ErrorCode::ValidationFailed,
                message: "Range queries require numeric values".to_string(),
            })?;
        
        if let Some(numeric_idx) = self.numeric_indices.get(field) {
            return Ok(numeric_idx
                .iter()
                .filter(|(v, _)| v <= &threshold)
                .map(|(_, id)| *id)
                .collect());
        }
        Ok(HashSet::new())
    }
    
    fn evaluate_gt(&self, field: &str, value: &MetadataValue) -> Result<HashSet<u64>> {
        let threshold = value.as_f64()
            .ok_or_else(|| WaffleError::WithCode {
                code: ErrorCode::ValidationFailed,
                message: "Range queries require numeric values".to_string(),
            })?;
        
        if let Some(numeric_idx) = self.numeric_indices.get(field) {
            return Ok(numeric_idx
                .iter()
                .filter(|(v, _)| v > &threshold)
                .map(|(_, id)| *id)
                .collect());
        }
        Ok(HashSet::new())
    }
    
    fn evaluate_ge(&self, field: &str, value: &MetadataValue) -> Result<HashSet<u64>> {
        let threshold = value.as_f64()
            .ok_or_else(|| WaffleError::WithCode {
                code: ErrorCode::ValidationFailed,
                message: "Range queries require numeric values".to_string(),
            })?;
        
        if let Some(numeric_idx) = self.numeric_indices.get(field) {
            return Ok(numeric_idx
                .iter()
                .filter(|(v, _)| v >= &threshold)
                .map(|(_, id)| *id)
                .collect());
        }
        Ok(HashSet::new())
    }
    
    fn evaluate_prefix(&self, field: &str, prefix: &str) -> Result<HashSet<u64>> {
        if let Some(field_index) = self.inverted_indices.get(field) {
            return Ok(field_index
                .iter()
                .filter(|(key, _)| key.starts_with(prefix))
                .flat_map(|(_, ids)| ids.iter().copied())
                .collect());
        }
        Ok(HashSet::new())
    }
    
    fn evaluate_in(&self, field: &str, values: &[MetadataValue]) -> Result<HashSet<u64>> {
        let mut result = HashSet::new();
        for value in values {
            result.extend(self.evaluate_eq(field, value)?);
        }
        Ok(result)
    }
    
    fn evaluate_exists(&self, field: &str) -> Result<HashSet<u64>> {
        Ok(self.all_metadata
            .iter()
            .filter(|(_, metadata)| metadata.contains_key(field))
            .map(|(id, _)| *id)
            .collect())
    }
    
    fn evaluate_and(&self, expressions: &[FilterExpression]) -> Result<HashSet<u64>> {
        if expressions.is_empty() {
            return Ok(self.all_metadata.keys().copied().collect());
        }
        
        let mut result = self.evaluate_filter(&expressions[0])?;
        for expr in &expressions[1..] {
            let matching = self.evaluate_filter(expr)?;
            result.retain(|id| matching.contains(id));
        }
        Ok(result)
    }
    
    fn evaluate_or(&self, expressions: &[FilterExpression]) -> Result<HashSet<u64>> {
        let mut result = HashSet::new();
        for expr in expressions {
            result.extend(self.evaluate_filter(expr)?);
        }
        Ok(result)
    }
    
    fn evaluate_not(&self, expr: &FilterExpression) -> Result<HashSet<u64>> {
        let matching = self.evaluate_filter(expr)?;
        let all: HashSet<u64> = self.all_metadata.keys().copied().collect();
        Ok(all.difference(&matching).copied().collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_string_exact_match() {
        let mut index = MetadataIndex::new();
        
        let mut meta1 = HashMap::new();
        meta1.insert("category".to_string(), MetadataValue::String("electronics".to_string()));
        index.insert_metadata(1, meta1).unwrap();
        
        let mut meta2 = HashMap::new();
        meta2.insert("category".to_string(), MetadataValue::String("books".to_string()));
        index.insert_metadata(2, meta2).unwrap();
        
        let filter = FilterExpression::Eq("category".to_string(), MetadataValue::String("electronics".to_string()));
        let results = index.evaluate_filter(&filter).unwrap();
        
        assert_eq!(results.len(), 1);
        assert!(results.contains(&1));
    }
    
    #[test]
    fn test_numeric_range_query() {
        let mut index = MetadataIndex::new();
        
        for i in 1..=5 {
            let mut meta = HashMap::new();
            meta.insert("price".to_string(), MetadataValue::Float(i as f64 * 10.0));
            index.insert_metadata(i as u64, meta).unwrap();
        }
        
        // price > 25
        let filter = FilterExpression::Gt("price".to_string(), MetadataValue::Float(25.0));
        let results = index.evaluate_filter(&filter).unwrap();
        
        assert_eq!(results.len(), 3); // 30, 40, 50
        assert!(results.contains(&3));
        assert!(results.contains(&4));
        assert!(results.contains(&5));
    }
    
    #[test]
    fn test_and_expression() {
        let mut index = MetadataIndex::new();
        
        for i in 1..=4 {
            let mut meta = HashMap::new();
            meta.insert("category".to_string(), if i % 2 == 0 {
                MetadataValue::String("electronics".to_string())
            } else {
                MetadataValue::String("books".to_string())
            });
            meta.insert("price".to_string(), MetadataValue::Float(i as f64 * 10.0));
            index.insert_metadata(i as u64, meta).unwrap();
        }
        
        // category == "electronics" AND price > 15
        let filter = FilterExpression::And(vec![
            FilterExpression::Eq("category".to_string(), MetadataValue::String("electronics".to_string())),
            FilterExpression::Gt("price".to_string(), MetadataValue::Float(15.0)),
        ]);
        
        let results = index.evaluate_filter(&filter).unwrap();
        
        assert_eq!(results.len(), 2); // IDs 2 and 4
        assert!(results.contains(&2));
        assert!(results.contains(&4));
    }
}
