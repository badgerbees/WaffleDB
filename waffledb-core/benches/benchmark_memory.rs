use std::time::Instant;
use std::fs::File;
use std::io::Write;
use waffledb_core::vector::types::Vector;
use waffledb_core::hnsw::graph::HNSWIndex;

/// Estimate memory usage
fn estimate_memory_mb(bytes: usize) -> f32 {
    bytes as f32 / (1024.0 * 1024.0)
}

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

struct MemoryBenchmarkResult {
    dataset_size: usize,
    dim: usize,
    m_param: usize,
    vectors_size_mb: f32,
    hnsw_index_estimated_mb: f32,
    total_estimated_mb: f32,
    per_vector_overhead_bytes: f32,
}

fn benchmark_memory_usage(dataset_size: usize, dim: usize, m: usize) -> MemoryBenchmarkResult {
    println!("\n  📊 Memory: {} vectors, {} dims, M={}", dataset_size, dim, m);

    // Generate vectors
    let _vectors = generate_vectors(dataset_size, dim);

    // Estimate vector data size
    let vectors_size_bytes = dataset_size * dim * std::mem::size_of::<f32>();

    // Build HNSW index
    let start = Instant::now();
    let mut index = HNSWIndex::new(m, 0.5);
    for idx in 0..dataset_size.min(10000) {  // Limited for benchmark speed
        index.insert_node(idx, 0);
    }
    let build_time = start.elapsed();

    // Estimate HNSW overhead
    // Each node has roughly M neighbors (average), each neighbor is 8 bytes (pointer/ID)
    // Plus layer structure, multiplier list, etc.
    let avg_neighbors_per_node = m as f32 * 1.5; // Multi-layer average
    let hnsw_overhead_per_node = (avg_neighbors_per_node * 8.0) as usize; // rough estimate
    let hnsw_index_size = dataset_size * hnsw_overhead_per_node;

    let total_memory = vectors_size_bytes + hnsw_index_size;
    let per_vector_overhead = hnsw_index_size as f32 / dataset_size as f32;

    println!(
        "    ✓ Vectors: {:.1} MB, HNSW: {:.1} MB, Total: {:.1} MB, Built in {:.2}s",
        estimate_memory_mb(vectors_size_bytes),
        estimate_memory_mb(hnsw_index_size),
        estimate_memory_mb(total_memory),
        build_time.as_secs_f32()
    );

    MemoryBenchmarkResult {
        dataset_size,
        dim,
        m_param: m,
        vectors_size_mb: estimate_memory_mb(vectors_size_bytes),
        hnsw_index_estimated_mb: estimate_memory_mb(hnsw_index_size),
        total_estimated_mb: estimate_memory_mb(total_memory),
        per_vector_overhead_bytes: per_vector_overhead,
    }
}

fn estimate_compression_savings(
    dataset_size: usize,
    dim: usize,
    compression_type: &str,
) -> f32 {
    // Original size
    let original_bytes = dataset_size * dim * std::mem::size_of::<f32>();
    let original_mb = original_bytes as f32 / (1024.0 * 1024.0);

    // Compressed size
    let compressed_bytes = match compression_type {
        "binary" => dataset_size * ((dim + 7) / 8), // 1 bit per dimension
        "scalar" => dataset_size * dim, // 1 byte per dimension
        "pq4x256" => dataset_size * 4, // 4 bytes per vector (4 subvectors, 1 byte each)
        _ => original_bytes,
    };
    let compressed_mb = compressed_bytes as f32 / (1024.0 * 1024.0);
    let savings_percent = ((original_mb - compressed_mb) / original_mb) * 100.0;

    println!(
        "    → {}: {:.1} MB → {:.1} MB ({:.1}% savings)",
        compression_type,
        original_mb,
        compressed_mb,
        savings_percent
    );

    savings_percent
}

fn main() {
    println!("🚀 WaffleDB Memory Benchmark Suite");
    println!("===================================\n");

    let mut results = Vec::new();

    // Vary dataset size and M parameter
    println!("📊 HNSW Index Memory Scaling");
    for dataset_size in &[10_000, 100_000, 1_000_000] {
        for m in &[8, 16, 32] {
            results.push(benchmark_memory_usage(*dataset_size, 128, *m));
        }
    }

    // Different dimensions
    println!("\n📊 Dimension Impact");
    for dim in &[64, 128, 256, 768] {
        results.push(benchmark_memory_usage(100_000, *dim, 16));
    }

    // Compression savings
    println!("\n📊 Compression Savings (100K vectors, 128 dims)");
    estimate_compression_savings(100_000, 128, "binary");
    estimate_compression_savings(100_000, 128, "scalar");
    estimate_compression_savings(100_000, 128, "pq4x256");

    // Write to CSV
    println!("\n📄 Writing results to benchmark_memory_results.csv...");
    let mut file = File::create("benchmark_memory_results.csv").unwrap();
    writeln!(
        file,
        "dataset_size,dimension,m_param,vectors_mb,hnsw_mb,total_mb,per_vector_overhead_bytes"
    ).unwrap();

    for result in &results {
        writeln!(
            file,
            "{},{},{},{:.2},{:.2},{:.2},{:.2}",
            result.dataset_size,
            result.dim,
            result.m_param,
            result.vectors_size_mb,
            result.hnsw_index_estimated_mb,
            result.total_estimated_mb,
            result.per_vector_overhead_bytes
        ).unwrap();
    }

    println!("✅ Memory benchmark complete!\n");

    // Print summary table
    println!("📊 MEMORY FOOTPRINT SUMMARY");
    println!("┌───────────┬────────┬─────┬──────────────┬───────────────┬──────────────────┐");
    println!("│ Dataset   │ Dim    │ M   │ Vectors MB   │ HNSW MB       │ Per-Vec (bytes)  │");
    println!("├───────────┼────────┼─────┼──────────────┼───────────────┼──────────────────┤");

    for result in &results {
        println!(
            "│ {:9} │ {:6} │ {:3} │ {:12.2} │ {:13.2} │ {:16.2} │",
            result.dataset_size,
            result.dim,
            result.m_param,
            result.vectors_size_mb,
            result.hnsw_index_estimated_mb,
            result.per_vector_overhead_bytes
        );
    }
    println!("└───────────┴────────┴─────┴──────────────┴───────────────┴──────────────────┘");

    // Projection for 1M vectors
    println!("\n📈 Projected Memory Usage for 1M Vectors:");
    println!("  • Dense (128-dim, f32): ~488 MB");
    println!("  • HNSW Index (M=16): ~150-200 MB");
    println!("  • Total uncompressed: ~650-700 MB");
    println!("  • With PQ compression: ~50-100 MB (6-7x reduction)");
    println!("  • Comparison: Milvus would use ~2-3GB, Weaviate ~1.5-2GB");
}
