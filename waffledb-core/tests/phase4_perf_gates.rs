/// Phase 4 Performance Gates - Multi-Vector + Hybrid Search
/// 
/// Ensures multi-vector storage and hybrid search fusion remain performant.
/// These gates enforce that Phase 4 features stay within budgets.

#[cfg(test)]
mod phase4_perf_gates {
    use waffledb_core::{
        MultiVectorDocument, MultiVectorStore, HybridSearch, FusionWeights,
        VectorType, SparseVector,
    };
    use waffledb_core::core::LatencyMeasurement;
    use std::collections::HashMap;

    /// Gate: Multi-vector lookup < 50µs (document with 5 vectors)
    #[test]
    fn gate_multi_vector_lookup_latency() {
        let mut store = MultiVectorStore::new();
        let mut latencies = LatencyMeasurement::new();

        // Create 10K documents each with 5 vectors
        for i in 0..10_000 {
            let mut doc = MultiVectorDocument::new(format!("doc{}", i));
            
            // Add 5 different vector slots
            doc.set_vector("body", VectorType::Dense(vec![0.1, 0.2, 0.3, 0.4])).ok();
            doc.set_vector("title", VectorType::Dense(vec![0.5, 0.6, 0.7, 0.8])).ok();
            doc.set_vector("summary", VectorType::Dense(vec![0.2, 0.3, 0.4, 0.5])).ok();
            
            let mut sparse_indices = HashMap::new();
            sparse_indices.insert(0u32, 1.0);
            sparse_indices.insert(1u32, 0.5);
            let sparse = SparseVector::new(sparse_indices).ok();
            if let Some(s) = sparse {
                doc.set_vector("keywords", VectorType::Sparse(s)).ok();
            }
            
            doc.set_vector("metadata", VectorType::Dense(vec![0.1, 0.1, 0.1, 0.1])).ok();
            
            store.insert(doc).ok();
        }

        // Measure lookup latency
        let doc_ids: Vec<_> = (0..1000).map(|i| format!("doc{}", i)).collect();
        for doc_id in doc_ids {
            latencies.measure(|| {
                if let Some(doc) = store.get(&doc_id) {
                    let _ = doc.get_vector("body");
                    let _ = doc.get_vector("title");
                    let _ = doc.get_vector("keywords");
                }
            });
        }

        let p99_us = latencies.p99_us();
        println!("✓ Multi-vector lookup P99: {:.2}µs", p99_us);
        assert!(p99_us < 50.0, "Multi-vector lookup too slow: {:.2}µs (max 50µs)", p99_us);
    }

    /// Gate: Hybrid ranking P99 < 10ms (combining 3 result sets)
    #[test]
    fn gate_hybrid_ranking_latency() {
        let mut latencies = LatencyMeasurement::new();
        let search = HybridSearch::new();

        // Generate sample results
        let mut dense_results = Vec::new();
        let mut bm25_results = Vec::new();
        let mut sparse_results = Vec::new();

        for i in 0..3000 {
            dense_results.push((format!("doc{}", i), (i as f32) / 3000.0));
            if i % 2 == 0 {
                bm25_results.push((format!("doc{}", i), 1.0 - (i as f32) / 3000.0));
            }
            if i % 3 == 0 {
                sparse_results.push((format!("doc{}", i), 0.5 + (i as f32) / 6000.0));
            }
        }

        // Measure fusion latency
        for _ in 0..100 {
            latencies.measure(|| {
                search
                    .fuse(dense_results.clone(), bm25_results.clone(), sparse_results.clone(), 10)
                    .ok();
            });
        }

        let p99_ms = latencies.p99_ms();
        println!("✓ Hybrid ranking P99: {:.3}ms", p99_ms);
        assert!(p99_ms < 10.0, "Hybrid ranking too slow: {:.3}ms (max 10ms)", p99_ms);
    }

    /// Gate: Dense + BM25 combination < 6ms
    #[test]
    fn gate_dense_bm25_combination_latency() {
        let mut latencies = LatencyMeasurement::new();
        let search = HybridSearch::new();

        // Generate dense + BM25 results (sparse empty)
        let mut dense_results = Vec::new();
        let mut bm25_results = Vec::new();

        for i in 0..2000 {
            dense_results.push((format!("doc{}", i), (i as f32) / 2000.0));
            bm25_results.push((format!("doc{}", i + 1000), 1.0 - (i as f32) / 2000.0));
        }

        // Measure fusion with 2 sources
        for _ in 0..100 {
            latencies.measure(|| {
                search
                    .fuse(dense_results.clone(), bm25_results.clone(), vec![], 10)
                    .ok();
            });
        }

        let p99_ms = latencies.p99_ms();
        println!("✓ Dense + BM25 combination P99: {:.3}ms", p99_ms);
        assert!(p99_ms < 6.0, "Dense + BM25 too slow: {:.3}ms (max 6ms)", p99_ms);
    }

    /// Gate: Dense + Sparse + BM25 combination < 10ms (with 10K candidates)
    #[test]
    fn gate_full_triple_fusion_latency() {
        let mut latencies = LatencyMeasurement::new();
        let search = HybridSearch::new();

        // Generate all three result sets with 10K candidates
        let mut dense_results = Vec::new();
        let mut bm25_results = Vec::new();
        let mut sparse_results = Vec::new();

        for i in 0..10_000 {
            dense_results.push((format!("doc{}", i), (i as f32) / 10000.0));
            if i % 2 == 0 {
                bm25_results.push((format!("doc{}", i), 1.0 - (i as f32) / 10000.0));
            }
            if i % 3 == 0 {
                sparse_results.push((format!("doc{}", i), 0.5 + (i as f32) / 20000.0));
            }
        }

        // Measure full triple fusion
        for _ in 0..50 {
            latencies.measure(|| {
                search
                    .fuse(dense_results.clone(), bm25_results.clone(), sparse_results.clone(), 20)
                    .ok();
            });
        }

        let p99_ms = latencies.p99_ms();
        println!("✓ Full triple fusion P99: {:.3}ms (10K candidates)", p99_ms);
        assert!(p99_ms < 10.0, "Full triple fusion too slow: {:.3}ms (max 10ms)", p99_ms);
    }

    /// Correctness: Multi-vector dimension validation
    #[test]
    fn correctness_multi_vector_dimension_validation() {
        let mut doc = MultiVectorDocument::new("doc1".to_string());
        
        doc.set_vector("body", VectorType::Dense(vec![0.1, 0.2, 0.3])).unwrap();
        doc.set_vector("title", VectorType::Dense(vec![0.4, 0.5, 0.6])).unwrap();
        
        // Dimension validation should pass
        let (dense_dim, sparse_dim) = doc.validate_dimensions().unwrap();
        assert_eq!(dense_dim, Some(3));
        assert!(sparse_dim.is_none());

        // Adding mismatched dimension should fail
        assert!(doc
            .set_vector("bad", VectorType::Dense(vec![0.1, 0.2]))
            .is_ok()); // insertion succeeds
        
        assert!(doc.validate_dimensions().is_err()); // but validation fails
    }

    /// Correctness: Hybrid fusion score bounds [0,1]
    #[test]
    fn correctness_hybrid_fusion_bounds() {
        let search = HybridSearch::new();

        // Create results with various scores
        let dense_results = vec![
            ("doc1".to_string(), 0.0),   // Perfect match
            ("doc2".to_string(), 0.5),   // Medium
            ("doc3".to_string(), 1.0),   // No match
        ];
        let bm25_results = vec![
            ("doc1".to_string(), 1.0),
            ("doc2".to_string(), 0.0),
            ("doc3".to_string(), 0.5),
        ];

        let results = search.fuse(dense_results, bm25_results, vec![], 3).unwrap();

        // All fused scores should be in [0, 1]
        for result in results {
            assert!(result.score >= 0.0 && result.score <= 1.0,
                "Fused score out of bounds: {}", result.score);
            
            if let Some(s) = result.dense_score {
                assert!(s >= 0.0 && s <= 1.0, "Dense score out of bounds: {}", s);
            }
            if let Some(s) = result.bm25_score {
                assert!(s >= 0.0 && s <= 1.0, "BM25 score out of bounds: {}", s);
            }
            if let Some(s) = result.sparse_score {
                assert!(s >= 0.0 && s <= 1.0, "Sparse score out of bounds: {}", s);
            }
        }
    }

    /// Correctness: Hybrid fusion ranking order
    #[test]
    fn correctness_hybrid_fusion_ranking_order() {
        let weights = FusionWeights::balanced();
        let search = HybridSearch::with_weights(weights).unwrap();

        // Create results where doc1 should rank highest
        let dense_results = vec![
            ("doc1".to_string(), 0.0),   // Best dense match
            ("doc2".to_string(), 0.8),
        ];
        let bm25_results = vec![
            ("doc1".to_string(), 1.0),   // Best BM25 match
            ("doc2".to_string(), 0.1),
        ];

        let results = search.fuse(dense_results, bm25_results, vec![], 2).unwrap();

        // doc1 should rank first (has best scores in both signals)
        assert_eq!(results[0].doc_id, "doc1");
        assert!(results[0].score > results[1].score);
    }

    /// Correctness: Multi-vector store statistics
    #[test]
    fn correctness_multi_vector_store_stats() {
        let mut store = MultiVectorStore::new();

        // Insert documents with different numbers of vectors
        let mut doc1 = MultiVectorDocument::new("doc1".to_string());
        doc1.set_vector("v1", VectorType::Dense(vec![0.1])).ok();
        doc1.set_vector("v2", VectorType::Dense(vec![0.2])).ok();

        let mut doc2 = MultiVectorDocument::new("doc2".to_string());
        doc2.set_vector("v1", VectorType::Dense(vec![0.3])).ok();

        store.insert(doc1).ok();
        store.insert(doc2).ok();

        let stats = store.stats();
        assert_eq!(stats.total_documents, 2);
        assert_eq!(stats.total_vectors, 3); // 2 + 1
    }
}
