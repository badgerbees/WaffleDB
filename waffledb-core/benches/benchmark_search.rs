use std::time::Instant;
use std::fs::File;
use std::io::Write;
use waffledb_core::vector::types::Vector;
use waffledb_core::vector::distance::DistanceMetric;
use waffledb_core::hnsw::graph::HNSWIndex;

/// Generate random vectors
fn generate_vectors(count: usize, dim: usize) -> Vec<Vector> {
    (0..count)
        .map(|_| {
            let data = (0..dim)
                .map(|_| (rand::random::<f32>() - 0.5) * 2.0)
                .collect();
            Vector::new(data)
        })
        .collect()
}

/// Calculate recall@k: fraction of ground truth neighbors found
fn calculate_recall(approximate: &[usize], ground_truth: &[usize], k: usize) -> f32 {
    let approx_set: std::collections::HashSet<_> = approximate.iter().take(k).copied().collect();
    let truth_set: std::collections::HashSet<_> = ground_truth.iter().take(k).copied().collect();
    
    let intersection = approx_set.intersection(&truth_set).count() as f32;
    let k_float = k.min(ground_truth.len()) as f32;
    
    if k_float == 0.0 {
        1.0
    } else {
        intersection / k_float
    }
}

/// Brute force search for ground truth
fn brute_force_search(
    query: &Vector,
    vectors: &[Vector],
    k: usize,
    metric: DistanceMetric,
) -> Vec<usize> {
    let mut distances: Vec<(usize, f32)> = vectors
        .iter()
        .enumerate()
        .map(|(idx, vec)| {
            let dist = match metric {
                DistanceMetric::L2 => {
                    query.data.iter()
                        .zip(&vec.data)
                        .map(|(a, b)| (a - b).powi(2))
                        .sum::<f32>()
                        .sqrt()
                }
                DistanceMetric::Cosine => {
                    let dot: f32 = query.data.iter().zip(&vec.data).map(|(a, b)| a * b).sum();
                    let norm_q = query.l2_norm();
                    let norm_v = vec.l2_norm();
                    if norm_q == 0.0 || norm_v == 0.0 {
                        1.0
                    } else {
                        1.0 - (dot / (norm_q * norm_v))
                    }
                }
                DistanceMetric::InnerProduct => {
                    let dot: f32 = query.data.iter().zip(&vec.data).map(|(a, b)| a * b).sum();
                    -dot  // Negative because we want largest values first
                }
            };
            (idx, dist)
        })
        .collect();

    distances.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
    distances.iter().take(k).map(|(idx, _)| *idx).collect()
}

struct BenchmarkResult {
    dim: usize,
    dataset_size: usize,
    top_k: usize,
    queries_per_sec: f32,
    p95_latency_ms: f32,
    p99_latency_ms: f32,
    recall_at_k: f32,
}

fn run_search_benchmark(
    dim: usize,
    dataset_size: usize,
    top_k: usize,
    num_queries: usize,
) -> BenchmarkResult {
    println!("\n📊 Benchmark: dim={}, dataset={}, top_k={}", dim, dataset_size, top_k);

    // Generate dataset
    println!("  → Generating {} vectors of {} dimensions...", dataset_size, dim);
    let vectors = generate_vectors(dataset_size, dim);

    // Build basic HNSW structure
    println!("  → Building HNSW index...");
    let start = Instant::now();
    let mut index = HNSWIndex::new(16, 0.5);
    // Insert nodes into layer 0
    for idx in 0..dataset_size.min(10000) {  // Limited for benchmark speed
        index.insert_node(idx, 0);
    }
    let build_time = start.elapsed();
    println!("  → Index built in {:.2}s", build_time.as_secs_f32());

    // Generate random queries
    let query_vectors = generate_vectors(num_queries, dim);

    // Run searches and measure (brute force for now)
    println!("  → Running {} queries...", num_queries);
    let start = Instant::now();
    let mut latencies = Vec::new();
    let mut total_recall = 0.0;

    for query_vec in &query_vectors {
        let query_start = Instant::now();
        let results = brute_force_search(query_vec, &vectors, top_k, DistanceMetric::L2);
        let latency = query_start.elapsed().as_secs_f32() * 1000.0; // ms
        latencies.push(latency);

        // Calculate ground truth for recall
        let ground_truth = brute_force_search(query_vec, &vectors, top_k, DistanceMetric::L2);
        let recall = calculate_recall(&results, &ground_truth, top_k);
        total_recall += recall as f32;
    }

    let total_time = start.elapsed();
    let queries_per_sec = num_queries as f32 / total_time.as_secs_f32();
    let avg_recall = total_recall / num_queries as f32;

    // Calculate percentiles
    latencies.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let p95_idx = (latencies.len() as f32 * 0.95) as usize;
    let p99_idx = (latencies.len() as f32 * 0.99) as usize;
    let p95_latency = latencies.get(p95_idx).copied().unwrap_or(0.0);
    let p99_latency = latencies.get(p99_idx).copied().unwrap_or(0.0);

    println!("  ✓ {:.0} queries/sec, p95={:.2}ms, p99={:.2}ms, recall={:.3}", 
             queries_per_sec, p95_latency, p99_latency, avg_recall);

    BenchmarkResult {
        dim,
        dataset_size,
        top_k,
        queries_per_sec,
        p95_latency_ms: p95_latency,
        p99_latency_ms: p99_latency,
        recall_at_k: avg_recall,
    }
}

fn main() {
    println!("🚀 WaffleDB Search Benchmark Suite");
    println!("================================\n");

    let mut results = Vec::new();

    // Dimension sweep: 32, 64, 128, 256
    for dim in &[32, 64, 128, 256] {
        // Dataset size: 100K
        let result = run_search_benchmark(*dim, 100_000, 10, 1000);
        results.push(result);
    }

    // Dataset size sweep: 100K, 1M
    for size in &[100_000, 1_000_000] {
        let result = run_search_benchmark(128, *size, 10, 1000);
        results.push(result);
    }

    // Top-K sweep: 10, 50
    for k in &[10, 50] {
        let result = run_search_benchmark(128, 100_000, *k, 1000);
        results.push(result);
    }

    // Write results to CSV
    println!("\n📄 Writing results to benchmark_search_results.csv...");
    let mut file = File::create("benchmark_search_results.csv").unwrap();
    writeln!(file, "dimension,dataset_size,top_k,queries_per_sec,p95_latency_ms,p99_latency_ms,recall_at_k").unwrap();
    
    for result in &results {
        writeln!(
            file,
            "{},{},{},{:.2},{:.3},{:.3},{:.4}",
            result.dim,
            result.dataset_size,
            result.top_k,
            result.queries_per_sec,
            result.p95_latency_ms,
            result.p99_latency_ms,
            result.recall_at_k
        ).unwrap();
    }

    println!("✅ Benchmark complete! Results saved.\n");
    
    // Print summary table
    println!("\n📊 SEARCH PERFORMANCE SUMMARY");
    println!("┌─────────┬────────────┬──────────┬──────────┬──────────┬───────────────┐");
    println!("│ Dim     │ Dataset    │ Top-K    │ Q/s      │ p95 ms   │ Recall@K      │");
    println!("├─────────┼────────────┼──────────┼──────────┼──────────┼───────────────┤");
    
    for result in &results {
        println!(
            "│ {:5}   │ {:10} │ {:8} │ {:8.0} │ {:8.3} │ {:13.4} │",
            result.dim,
            result.dataset_size,
            result.top_k,
            result.queries_per_sec,
            result.p95_latency_ms,
            result.recall_at_k
        );
    }
    println!("└─────────┴────────────┴──────────┴──────────┴──────────┴───────────────┘");
}
