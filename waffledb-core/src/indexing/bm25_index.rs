/// BM25 Text Indexing
/// 
/// Lightweight full-text search using BM25 ranking algorithm.
/// Generates sparse vectors from text (term -> TF-IDF/BM25 score).
/// 
/// Components:
/// - Simple tokenizer (lowercase, strip punctuation, split on whitespace)
/// - Postings lists (term -> [(doc_id, term_freq)])
/// - BM25 scoring with standard k1=1.5, b=0.75

use std::collections::{HashMap, HashSet};
use crate::core::errors::{Result, WaffleError, ErrorCode};

/// Simple deterministic tokenizer
pub struct Tokenizer;

impl Tokenizer {
    /// Tokenize text: lowercase, remove punctuation, split on whitespace
    pub fn tokenize(text: &str) -> Vec<String> {
        text.to_lowercase()
            .chars()
            .map(|c| if c.is_alphanumeric() { c } else { ' ' })
            .collect::<String>()
            .split_whitespace()
            .map(|s| s.to_string())
            .collect()
    }
}

/// BM25 text index
pub struct BM25Index {
    // term -> [(doc_id, term_frequency)]
    postings: HashMap<String, Vec<(u64, u32)>>,
    
    // doc_id -> document length (in tokens)
    doc_lengths: HashMap<u64, u32>,
    
    // Total number of documents
    doc_count: u32,
    
    // Average document length
    avg_doc_length: f32,
    
    // BM25 parameters
    k1: f32,  // Term frequency saturation (default 1.5)
    b: f32,   // Document length normalization (default 0.75)
}

impl BM25Index {
    pub fn new() -> Self {
        BM25Index {
            postings: HashMap::new(),
            doc_lengths: HashMap::new(),
            doc_count: 0,
            avg_doc_length: 0.0,
            k1: 1.5,
            b: 0.75,
        }
    }
    
    pub fn with_params(k1: f32, b: f32) -> Self {
        BM25Index {
            postings: HashMap::new(),
            doc_lengths: HashMap::new(),
            doc_count: 0,
            avg_doc_length: 0.0,
            k1,
            b,
        }
    }
    
    /// Index a document's text
    pub fn index_text(&mut self, doc_id: u64, text: &str) -> Result<()> {
        let tokens = Tokenizer::tokenize(text);
        let doc_length = tokens.len() as u32;
        
        // Track document length
        self.doc_lengths.insert(doc_id, doc_length);
        
        // Count unique terms
        let mut term_freqs: HashMap<String, u32> = HashMap::new();
        for token in tokens {
            *term_freqs.entry(token).or_insert(0) += 1;
        }
        
        // Update postings and document count
        for (term, freq) in term_freqs {
            self.postings
                .entry(term)
                .or_insert_with(Vec::new)
                .push((doc_id, freq));
        }
        
        self.doc_count += 1;
        self.recalculate_avg_length();
        
        Ok(())
    }
    
    /// Remove document from index
    pub fn remove_document(&mut self, doc_id: u64) -> Result<()> {
        // Remove from doc_lengths
        self.doc_lengths.remove(&doc_id);
        
        // Remove from postings
        for postings in self.postings.values_mut() {
            postings.retain(|(id, _)| *id != doc_id);
        }
        
        // Remove empty postings
        self.postings.retain(|_, postings| !postings.is_empty());
        
        self.doc_count = self.doc_count.saturating_sub(1);
        self.recalculate_avg_length();
        
        Ok(())
    }
    
    fn recalculate_avg_length(&mut self) {
        if self.doc_count == 0 {
            self.avg_doc_length = 0.0;
        } else {
            let total: u32 = self.doc_lengths.values().sum();
            self.avg_doc_length = total as f32 / self.doc_count as f32;
        }
    }
    
    /// Calculate IDF for a term
    fn idf(&self, term: &str) -> f32 {
        if self.doc_count == 0 {
            return 0.0;
        }
        
        let doc_freq = self.postings
            .get(term)
            .map(|p| p.len() as f32)
            .unwrap_or(0.0);
        
        if doc_freq == 0.0 {
            return 0.0;
        }
        
        ((self.doc_count as f32 - doc_freq + 0.5) / (doc_freq + 0.5) + 1.0).ln()
    }
    
    /// Calculate BM25 score for a document matching a query term
    fn bm25_score(&self, term: &str, _doc_id: u64, term_freq: u32, doc_length: u32) -> f32 {
        if self.avg_doc_length == 0.0 {
            return 0.0;
        }
        
        let idf = self.idf(term);
        let tf = term_freq as f32;
        
        let numerator = tf * (self.k1 + 1.0);
        let denominator = tf + self.k1 * (1.0 - self.b + self.b * (doc_length as f32 / self.avg_doc_length));
        
        idf * (numerator / denominator)
    }
    
    /// Search for documents matching query text, return BM25 scores
    /// Returns: Vec<(doc_id, bm25_score)> sorted by score descending
    pub fn search(&self, query: &str) -> Result<Vec<(u64, f32)>> {
        let query_terms = Tokenizer::tokenize(query);
        
        if query_terms.is_empty() {
            return Ok(Vec::new());
        }
        
        // Collect all matching documents
        let mut doc_scores: HashMap<u64, f32> = HashMap::new();
        
        for term in query_terms.iter() {
            if let Some(postings) = self.postings.get(term) {
                for (doc_id, term_freq) in postings {
                    if let Some(doc_length) = self.doc_lengths.get(doc_id) {
                        let score = self.bm25_score(term, *doc_id, *term_freq, *doc_length);
                        
                        // Aggregate total score
                        *doc_scores.entry(*doc_id).or_insert(0.0) += score;
                    }
                }
            }
        }
        
        // Sort by score (descending)
        let mut results: Vec<_> = doc_scores.into_iter().collect();
        results.sort_by(|a, b| {
            b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal)
        });
        
        Ok(results)
    }
    
    /// Get top K results for query
    pub fn search_top_k(&self, query: &str, k: usize) -> Result<Vec<(u64, f32)>> {
        let mut results = self.search(query)?;
        results.truncate(k);
        Ok(results)
    }
    
    /// Get document count
    pub fn doc_count(&self) -> u32 {
        self.doc_count
    }
    
    /// Get vocabulary size (unique terms)
    pub fn vocab_size(&self) -> usize {
        self.postings.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_tokenizer() {
        let tokens = Tokenizer::tokenize("Hello World! How are you?");
        assert_eq!(tokens, vec!["hello", "world", "how", "are", "you"]);
    }
    
    #[test]
    fn test_bm25_single_document() {
        let mut index = BM25Index::new();
        index.index_text(1, "hello world").unwrap();
        
        assert_eq!(index.doc_count(), 1);
        assert_eq!(index.vocab_size(), 2);
    }
    
    #[test]
    fn test_bm25_search_exact_match() {
        let mut index = BM25Index::new();
        index.index_text(1, "hello world").unwrap();
        index.index_text(2, "hello there").unwrap();
        index.index_text(3, "goodbye world").unwrap();
        
        let results = index.search_top_k("hello", 10).unwrap();
        
        // Should find documents 1 and 2 (both have "hello")
        assert_eq!(results.len(), 2);
        let result_ids: Vec<u64> = results.iter().map(|(id, _)| *id).collect();
        assert!(result_ids.contains(&1));
        assert!(result_ids.contains(&2));
        assert!(!result_ids.contains(&3));
    }
    
    #[test]
    fn test_bm25_multi_term_query() {
        let mut index = BM25Index::new();
        index.index_text(1, "the quick brown fox").unwrap();
        index.index_text(2, "the lazy dog").unwrap();
        index.index_text(3, "quick lazy fox").unwrap();
        
        let results = index.search_top_k("quick fox", 10).unwrap();
        
        // Doc 1 has both terms, Doc 3 has both terms, Doc 2 has neither
        assert!(results.len() <= 3);
        assert!(results.iter().any(|(id, _)| *id == 1));
    }
    
    #[test]
    fn test_bm25_remove_document() {
        let mut index = BM25Index::new();
        index.index_text(1, "hello").unwrap();
        index.index_text(2, "world").unwrap();
        
        assert_eq!(index.doc_count(), 2);
        
        index.remove_document(1).unwrap();
        assert_eq!(index.doc_count(), 1);
        
        let results = index.search_top_k("hello", 10).unwrap();
        assert_eq!(results.len(), 0); // Doc 1 is removed
    }
    
    #[test]
    fn test_bm25_empty_query() {
        let mut index = BM25Index::new();
        index.index_text(1, "hello world").unwrap();
        
        let results = index.search_top_k("", 10).unwrap();
        assert_eq!(results.len(), 0);
    }
    
    #[test]
    fn test_bm25_term_not_found() {
        let mut index = BM25Index::new();
        index.index_text(1, "hello world").unwrap();
        
        let results = index.search_top_k("xyz", 10).unwrap();
        assert_eq!(results.len(), 0);
    }
}
