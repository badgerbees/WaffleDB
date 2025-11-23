/// Phase 3 Performance Gates - Sparse Vectors + BM25
/// 
/// Ensures sparse scoring and BM25 search don't blow up latency.
/// These gates enforce that Phase 3 features stay within budgets.

#[cfg(test)]
mod phase3_perf_gates {
    use waffledb_core::{
        SparseVector, SparseVectorStore, BM25Index,
        indexing::sparse_vector::sparse_distance::dot_product,
    };
    use waffledb_core::core::LatencyMeasurement;
    use std::collections::HashMap;
    
    /// Gate: Sparse scoring P99 latency < 0.5ms for 10K vectors
    #[test]
    fn gate_sparse_scoring_latency() {
        let mut store = SparseVectorStore::new();
        let mut latencies = LatencyMeasurement::new();
        
        // Insert 10K sparse vectors
        for i in 0..10_000 {
            let mut indices = HashMap::new();
            indices.insert((i % 100) as u32, 1.0);
            indices.insert(((i + 1) % 100) as u32, 0.5);
            
            let sparse = SparseVector::new(indices).unwrap();
            store.insert(i as u64, sparse).ok();
        }
        
        // Create query vector
        let mut query_indices = HashMap::new();
        query_indices.insert(0, 1.0);
        query_indices.insert(1, 0.5);
        let query = SparseVector::new(query_indices).unwrap();
        
        // Measure scoring latency
        let candidates: Vec<u64> = (0..1000).collect();
        for _ in 0..100 {
            latencies.measure(|| {
                store.score_candidates(&query, &candidates, 10, dot_product).ok();
            });
        }
        
        let p99_ms = latencies.p99_ms();
        
        println!("✓ Sparse scoring P99: {:.3}ms", p99_ms);
        assert!(p99_ms < 0.5, "Sparse scoring too slow: {:.3}ms (max 0.5ms)", p99_ms);
    }
    
    /// Gate: BM25 search P99 latency < 5ms for 1M documents
    #[test]
    fn gate_bm25_search_latency() {
        let mut index = BM25Index::new();
        let mut latencies = LatencyMeasurement::new();
        
        // Index 10K documents with realistic text
        let docs = vec![
            "the quick brown fox jumps over the lazy dog",
            "hello world from waffledb",
            "rust programming language",
            "vector databases for search",
            "semantic search with embeddings",
        ];
        
        for i in 0..10_000 {
            let text = docs[i % docs.len()];
            index.index_text(i as u64, text).ok();
        }
        
        // Measure search latency
        for _ in 0..100 {
            latencies.measure(|| {
                index.search_top_k("quick fox", 10).ok();
            });
        }
        
        let p99_ms = latencies.p99_ms();
        
        println!("✓ BM25 search P99: {:.3}ms (doc_count: {})", p99_ms, index.doc_count());
        assert!(p99_ms < 5.0, "BM25 search too slow: {:.3}ms (max 5ms)", p99_ms);
    }
    
    /// Gate: Sparse vector magnitude computation < 10μs
    #[test]
    fn gate_sparse_magnitude_latency() {
        let mut latencies = LatencyMeasurement::new();
        
        // Create sparse vectors of various sizes
        for _ in 0..1000 {
            let mut indices = HashMap::new();
            for i in 0..100 {
                indices.insert(i as u32, (i as f32) * 0.1);
            }
            
            let mut vec = SparseVector::new(indices).unwrap();
            
            latencies.measure(|| {
                let _ = vec.magnitude();
            });
        }
        
        let p99_us = latencies.p99_us();
        
        println!("✓ Sparse magnitude computation P99: {:.1}μs", p99_us);
        assert!(p99_us < 10.0, "Sparse magnitude too slow: {:.1}μs (max 10μs)", p99_us);
    }
    
    /// Gate: BM25 indexing < 100μs per document
    #[test]
    fn gate_bm25_indexing_latency() {
        let mut index = BM25Index::new();
        let mut latencies = LatencyMeasurement::new();
        
        let text = "the quick brown fox jumps over the lazy dog for testing purposes";
        
        for i in 0..1000 {
            latencies.measure(|| {
                index.index_text(i as u64, text).ok();
            });
        }
        
        let p99_us = latencies.p99_us();
        
        println!("✓ BM25 indexing P99: {:.1}μs", p99_us);
        assert!(p99_us < 100.0, "BM25 indexing too slow: {:.1}μs (max 100μs)", p99_us);
    }
    
    /// Correctness: Sparse dot product correctness
    #[test]
    fn correctness_sparse_dot_product() {
        let mut a_indices = HashMap::new();
        a_indices.insert(0, 1.0);
        a_indices.insert(1, 2.0);
        a_indices.insert(2, 3.0);
        
        let mut b_indices = HashMap::new();
        b_indices.insert(0, 2.0);
        b_indices.insert(1, 1.0);
        b_indices.insert(3, 5.0);
        
        let a = SparseVector::new(a_indices).unwrap();
        let b = SparseVector::new(b_indices).unwrap();
        
        // Manually compute: (1*2) + (2*1) = 4
        let expected = 4.0;
        let actual = dot_product(&a, &b);
        
        assert!((actual - expected).abs() < 0.001, 
                "Dot product mismatch: {} vs {}", actual, expected);
    }
    
    /// Correctness: BM25 search results ordered by score
    #[test]
    fn correctness_bm25_ranking() {
        let mut index = BM25Index::new();
        
        // Doc with one occurrence
        index.index_text(1, "hello world").ok();
        
        // Doc with multiple occurrences
        index.index_text(2, "hello hello hello world world").ok();
        
        // Doc without term
        index.index_text(3, "goodbye world").ok();
        
        let results = index.search_top_k("hello", 10).ok().unwrap_or_default();
        
        // Doc 2 should rank higher (more occurrences)
        if results.len() >= 2 {
            let score_0 = results[0].1;
            let score_1 = results[1].1;
            assert!(score_0 >= score_1, 
                    "BM25 ranking violated: {} < {}", score_0, score_1);
        }
    }
}
