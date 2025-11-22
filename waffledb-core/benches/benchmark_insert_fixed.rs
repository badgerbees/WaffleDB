// FIXED INSERT THROUGHPUT BENCHMARK
// Uses REAL HNSWInserter logic (graph search) instead of brute force
// This measures what the server actually does, not an artificial bottleneck

use std::time::Instant;
use std::collections::HashMap;
use waffledb_core::hnsw::graph::HNSWIndex;
use waffledb_core::hnsw::builder::HNSWBuilder;
use waffledb_core::hnsw::insert::HNSWInserter;

/// Generate random normalized vectors for realistic benchmarking
fn generate_vectors(count: usize, dim: usize) -> Vec<Vec<f32>> {
    (0..count)
        .map(|_| {
            let data = (0..dim)
                .map(|_| (rand::random::<f32>() - 0.5) * 2.0)
                .collect::<Vec<_>>();
            
            // Normalize to unit sphere
            let norm = data.iter().map(|x| x * x).sum::<f32>().sqrt();
            if norm > 0.0 {
                data.iter().map(|x| x / norm).collect()
            } else {
                data
            }
        })
        .collect()
}

fn main() {
    println!("\n🔥 FIXED INSERT THROUGHPUT BENCHMARK");
    println!("══════════════════════════════════════════════");
    println!("Uses REAL HNSW graph search (not brute force)\n");
    
    let vector_counts = vec![100, 1_000, 5_000, 10_000];
    let dim = 128;
    
    for count in vector_counts {
        println!("\n📊 Testing with {} vectors", count);
        println!("─────────────────────────────────────");
        
        // Generate all vectors upfront
        let vectors = generate_vectors(count, dim);
        
        // Create builder and inserter
        let builder = HNSWBuilder::new()
            .with_m(16)
            .with_ef_construction(200)
            .with_ef_search(50);
        let inserter = HNSWInserter::new(builder);
        let mut index = HNSWIndex::new(16, 0.5);
        
        // Vector storage for lookups during insertion
        let mut vector_store = HashMap::new();
        
        let mut insert_times = Vec::new();
        let total_start = Instant::now();
        
        // Insert vectors one by one, measuring actual insert time
        for (idx, vector) in vectors.iter().enumerate() {
            let start = Instant::now();
            
            // Store vector
            vector_store.insert(idx, vector.clone());
            
            // Perform actual HNSW insertion with graph search
            let get_vector = |node_id: usize| -> Option<Vec<f32>> {
                vector_store.get(&node_id).cloned()
            };
            
            let distance_fn = |a: &[f32], b: &[f32]| -> f32 {
                a.iter()
                    .zip(b.iter())
                    .map(|(x, y)| (x - y).powi(2))
                    .sum::<f32>()
                    .sqrt()
            };
            
            inserter.insert(&mut index, idx, vector, &get_vector, &distance_fn);
            
            let elapsed_us = start.elapsed().as_micros() as f32;
            insert_times.push(elapsed_us);
        }
        
        let total_time = total_start.elapsed();
        let throughput = count as f32 / total_time.as_secs_f32();
        
        // Calculate percentiles
        insert_times.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let p50_idx = (insert_times.len() as f32 * 0.50) as usize;
        let p99_idx = (insert_times.len() as f32 * 0.99) as usize;
        let p95_idx = (insert_times.len() as f32 * 0.95) as usize;
        let avg = insert_times.iter().sum::<f32>() / insert_times.len() as f32;
        
        println!("  Total time:     {:.2}s", total_time.as_secs_f32());
        println!("  Throughput:     {:.0} inserts/sec", throughput);
        println!("  Avg latency:    {:.0} µs", avg);
        println!("  p50 latency:    {:.0} µs", insert_times[p50_idx]);
        println!("  p95 latency:    {:.0} µs", insert_times[p95_idx]);
        println!("  p99 latency:    {:.0} µs", insert_times[p99_idx]);
        println!("  Graph layers:   {}", index.layers.len());
        println!("  Entry point:    {:?}", index.entry_point);
    }
    
    println!("\n═══════════════════════════════════════════════");
    println!("KEY INSIGHT: This benchmark uses ACTUAL HNSW logic");
    println!("(graph traversal + neighbor selection)");
    println!("NOT brute-force distance computation to all vectors");
    println!("═══════════════════════════════════════════════\n");
}
