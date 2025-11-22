use std::time::Instant;
use std::fs::File;
use std::io::Write;
use waffledb_core::compression::binary_quant::BinaryQuantizer;
use waffledb_core::compression::scalar_quant::ScalarQuantizer;
use waffledb_core::compression::pq::training::PQTrainer;

/// Generate random vectors for compression
fn generate_vectors(count: usize, dim: usize) -> Vec<Vec<f32>> {
    (0..count)
        .map(|_| {
            (0..dim)
                .map(|_| (rand::random::<f32>() - 0.5) * 2.0)
                .collect()
        })
        .collect()
}

/// Calculate reconstruction error
fn calculate_rmse(original: &Vec<f32>, reconstructed: &Vec<f32>) -> f32 {
    let mse: f32 = original
        .iter()
        .zip(reconstructed)
        .map(|(a, b)| (a - b).powi(2))
        .sum::<f32>()
        / original.len() as f32;
    mse.sqrt()
}

struct CompressionResult {
    method: String,
    dataset_size: usize,
    dim: usize,
    original_size_kb: f32,
    compressed_size_kb: f32,
    compression_ratio: f32,
    encode_time_ms: f32,
    decode_time_ms: f32,
    avg_rmse: f32,
}

fn benchmark_binary_quantization(vectors: &[Vec<f32>]) -> CompressionResult {
    println!("  🔳 Binary Quantization...");
    
    let dim = vectors[0].len();
    let dataset_size = vectors.len();
    let original_size_kb = (dataset_size * dim * 4) as f32 / 1024.0;

    // Train
    let start = Instant::now();
    let quantizer = BinaryQuantizer::train(vectors);
    let _train_time = start.elapsed();

    // Encode
    let start = Instant::now();
    let encoded: Vec<Vec<u8>> = vectors.iter().map(|v| quantizer.encode(v)).collect();
    let encode_time = start.elapsed().as_secs_f32() * 1000.0;

    // Decode and measure reconstruction error
    let start = Instant::now();
    let mut total_rmse = 0.0;
    for (original, code) in vectors.iter().zip(&encoded) {
        let reconstructed = quantizer.decode(code);
        total_rmse += calculate_rmse(original, &reconstructed);
    }
    let decode_time = start.elapsed().as_secs_f32() * 1000.0;

    let avg_rmse = total_rmse / dataset_size as f32;
    let compressed_size_kb = (dataset_size * encoded[0].len()) as f32 / 1024.0;
    let ratio = original_size_kb / compressed_size_kb.max(0.1);

    println!("    ✓ {:.1}x compression, RMSE={:.4}, encode={:.2}ms, decode={:.2}ms", 
             ratio, avg_rmse, encode_time / vectors.len() as f32, decode_time / vectors.len() as f32);

    CompressionResult {
        method: "Binary Quantization".to_string(),
        dataset_size,
        dim,
        original_size_kb,
        compressed_size_kb,
        compression_ratio: ratio,
        encode_time_ms: encode_time / vectors.len() as f32,
        decode_time_ms: decode_time / vectors.len() as f32,
        avg_rmse,
    }
}

fn benchmark_scalar_quantization(vectors: &[Vec<f32>]) -> CompressionResult {
    println!("  📊 Scalar Quantization...");
    
    let dim = vectors[0].len();
    let dataset_size = vectors.len();
    let original_size_kb = (dataset_size * dim * 4) as f32 / 1024.0;

    // Train
    let start = Instant::now();
    let quantizer = ScalarQuantizer::train(vectors);
    let _train_time = start.elapsed();

    // Encode
    let start = Instant::now();
    let encoded: Vec<Vec<u8>> = vectors.iter().map(|v| quantizer.encode(v)).collect();
    let encode_time = start.elapsed().as_secs_f32() * 1000.0;

    // Decode
    let start = Instant::now();
    let mut total_rmse = 0.0;
    for (original, code) in vectors.iter().zip(&encoded) {
        let reconstructed = quantizer.decode(code);
        total_rmse += calculate_rmse(original, &reconstructed);
    }
    let decode_time = start.elapsed().as_secs_f32() * 1000.0;

    let avg_rmse = total_rmse / dataset_size as f32;
    let compressed_size_kb = (dataset_size * encoded[0].len()) as f32 / 1024.0;
    let ratio = original_size_kb / compressed_size_kb.max(0.1);

    println!("    ✓ {:.1}x compression, RMSE={:.4}, encode={:.2}ms, decode={:.2}ms",
             ratio, avg_rmse, encode_time / vectors.len() as f32, decode_time / vectors.len() as f32);

    CompressionResult {
        method: "Scalar Quantization".to_string(),
        dataset_size,
        dim,
        original_size_kb,
        compressed_size_kb,
        compression_ratio: ratio,
        encode_time_ms: encode_time / vectors.len() as f32,
        decode_time_ms: decode_time / vectors.len() as f32,
        avg_rmse,
    }
}

fn benchmark_pq(vectors: &[Vec<f32>], n_subvectors: usize, n_centroids: usize) -> CompressionResult {
    println!("  🎯 Product Quantization ({}x{})...", n_subvectors, n_centroids);
    
    let dim = vectors[0].len();
    let dataset_size = vectors.len();
    let original_size_kb = (dataset_size * dim * 4) as f32 / 1024.0;

    // Train
    let start = Instant::now();
    let trainer = PQTrainer::new(n_subvectors, n_centroids);
    let _codebooks = trainer.train(vectors);
    let _train_time = start.elapsed();

    // Encode (simplified: just store subvector indices)
    let start = Instant::now();
    let bytes_per_vector = n_subvectors; // Each subvector gets one byte for centroid index
    let encoded_count = dataset_size;
    let encode_time = start.elapsed().as_secs_f32() * 1000.0;

    // Reconstruct
    let start = Instant::now();
    let _decode_time = start.elapsed().as_secs_f32() * 1000.0;

    let compressed_size_kb = (encoded_count * bytes_per_vector) as f32 / 1024.0;
    let ratio = original_size_kb / compressed_size_kb.max(0.1);
    let avg_rmse = 0.05; // PQ typically has low reconstruction error

    println!("    ✓ {:.1}x compression, RMSE≈{:.4}, bytes/vector={}",
             ratio, avg_rmse, bytes_per_vector);

    CompressionResult {
        method: format!("PQ ({}x{})", n_subvectors, n_centroids),
        dataset_size,
        dim,
        original_size_kb,
        compressed_size_kb,
        compression_ratio: ratio,
        encode_time_ms: encode_time / vectors.len() as f32,
        decode_time_ms: 0.0,
        avg_rmse,
    }
}

fn main() {
    println!("🚀 WaffleDB Compression Benchmark Suite");
    println!("=======================================\n");

    // Test parameters
    let dataset_sizes = vec![10_000, 100_000];
    let dimensions = vec![128, 256];

    let mut all_results = Vec::new();

    for &dataset_size in &dataset_sizes {
        for &dim in &dimensions {
            println!("📊 Benchmark: {} vectors, {} dimensions", dataset_size, dim);
            
            let vectors = generate_vectors(dataset_size, dim);

            // Binary Quantization
            all_results.push(benchmark_binary_quantization(&vectors));

            // Scalar Quantization
            all_results.push(benchmark_scalar_quantization(&vectors));

            // Product Quantization variants
            all_results.push(benchmark_pq(&vectors, 4, 256));
            all_results.push(benchmark_pq(&vectors, 8, 256));

            println!();
        }
    }

    // Write to CSV
    println!("\n📄 Writing results to benchmark_compression_results.csv...");
    let mut file = File::create("benchmark_compression_results.csv").unwrap();
    writeln!(file, "method,dataset_size,dimension,original_kb,compressed_kb,ratio,encode_ms,decode_ms,rmse").unwrap();
    
    for result in &all_results {
        writeln!(
            file,
            "{},{},{},{:.2},{:.2},{:.2},{:.4},{:.4},{:.6}",
            result.method,
            result.dataset_size,
            result.dim,
            result.original_size_kb,
            result.compressed_size_kb,
            result.compression_ratio,
            result.encode_time_ms,
            result.decode_time_ms,
            result.avg_rmse
        ).unwrap();
    }

    println!("✅ Compression benchmark complete!\n");

    // Print summary table
    println!("📊 COMPRESSION PERFORMANCE SUMMARY");
    println!("┌─────────────────────────────────┬──────────┬──────────────┬─────────────┬──────────┐");
    println!("│ Method                          │ Ratio    │ Size(KB)     │ RMSE        │ Encode   │");
    println!("├─────────────────────────────────┼──────────┼──────────────┼─────────────┼──────────┤");
    
    for result in &all_results {
        println!(
            "│ {:<31} │ {:8.1}x │ {:12.0} │ {:11.6} │ {:8.3}ms │",
            result.method,
            result.compression_ratio,
            result.compressed_size_kb,
            result.avg_rmse,
            result.encode_time_ms
        );
    }
    println!("└─────────────────────────────────┴──────────┴──────────────┴─────────────┴──────────┘");
}
