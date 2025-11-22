// Product Quantization training data generation script

use rand::Rng;
use std::fs::File;
use std::io::Write;

fn main() {
    println!("=== PQ Training Data Generation ===\n");

    let num_vectors = 10_000;
    let vector_dim = 256;
    let output_file = "pq_training_data.bin";

    println!("Configuration:");
    println!("  Output file: {}", output_file);
    println!("  Vectors: {}", num_vectors);
    println!("  Dimension: {}", vector_dim);
    println!();

    let mut rng = rand::thread_rng();
    let mut file = File::create(output_file).expect("Failed to create file");

    // Write header
    let header = format!("{}\n{}\n", num_vectors, vector_dim);
    file.write_all(header.as_bytes()).expect("Failed to write header");

    println!("Generating training data...");
    for i in 0..num_vectors {
        for _ in 0..vector_dim {
            let value: f32 = rng.gen_range(0.0..1.0);
            let bytes = value.to_le_bytes();
            file.write_all(&bytes).expect("Failed to write data");
        }

        if (i + 1) % 1_000 == 0 {
            println!("  Generated {} vectors", i + 1);
        }
    }

    println!("\nTraining data generated successfully!");
    println!("File size: ~{:.2} MB", 
        (num_vectors * vector_dim * 4) as f64 / (1024.0 * 1024.0)
    );

    // Simulate PQ training
    println!("\nSimulating PQ training...");
    let n_subvectors = 16;
    let n_centroids = 256;

    println!("  Subvectors: {}", n_subvectors);
    println!("  Centroids per subvector: {}", n_centroids);

    let subvector_dim = vector_dim / n_subvectors;
    println!("  Subvector dimension: {}", subvector_dim);

    let compression_ratio = (vector_dim * 4) as f64 / n_subvectors as f64;
    println!("  Compression ratio: {:.1}x", compression_ratio);
}

