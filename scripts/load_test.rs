// Load testing script for WaffleDB

use std::time::Instant;
use rand::Rng;

fn main() {
    println!("=== WaffleDB Load Test ===\n");

    let num_vectors = 100_000;
    let vector_dim = 128;
    let batch_size = 1_000;

    println!("Configuration:");
    println!("  Vectors to insert: {}", num_vectors);
    println!("  Vector dimension: {}", vector_dim);
    println!("  Batch size: {}", batch_size);
    println!();

    let mut rng = rand::thread_rng();

    // Generate test vectors
    println!("Generating {} vectors of dimension {}...", num_vectors, vector_dim);
    let start = Instant::now();

    let mut vectors = vec![];
    for _ in 0..num_vectors {
        let vector: Vec<f32> = (0..vector_dim)
            .map(|_| rng.gen_range(0.0..1.0))
            .collect();
        vectors.push(vector);
    }

    let duration = start.elapsed();
    println!("Generated in {:.2}s", duration.as_secs_f64());
    println!("Generation rate: {:.0} vectors/sec\n", num_vectors as f64 / duration.as_secs_f64());

    // Simulate insertion
    println!("Simulating insertion in batches of {}...", batch_size);
    let start = Instant::now();

    let num_batches = (num_vectors + batch_size - 1) / batch_size;
    for batch_idx in 0..num_batches {
        let batch_start = batch_idx * batch_size;
        let batch_end = (batch_start + batch_size).min(num_vectors);
        let _batch = &vectors[batch_start..batch_end];
        
        if (batch_idx + 1) % 10 == 0 {
            println!("  Batch {}/{}", batch_idx + 1, num_batches);
        }
    }

    let duration = start.elapsed();
    println!("Insertion completed in {:.2}s", duration.as_secs_f64());
    println!("Insertion rate: {:.0} vectors/sec\n", num_vectors as f64 / duration.as_secs_f64());

    // Simulate search
    println!("Simulating search queries...");
    let num_queries = 1_000;
    let start = Instant::now();

    for _ in 0..num_queries {
        let query: Vec<f32> = (0..vector_dim)
            .map(|_| rng.gen_range(0.0..1.0))
            .collect();
        
        // Simulate linear scan (simple distance calculation)
        let _results: Vec<_> = vectors.iter()
            .enumerate()
            .map(|(idx, vec)| {
                let dist: f32 = query.iter()
                    .zip(vec.iter())
                    .map(|(x, y)| (x - y).powi(2))
                    .sum::<f32>()
                    .sqrt();
                (idx, dist)
            })
            .collect();
    }

    let duration = start.elapsed();
    println!("Completed {} queries in {:.2}s", num_queries, duration.as_secs_f64());
    println!("Query rate: {:.0} queries/sec\n", num_queries as f64 / duration.as_secs_f64());

    println!("=== Load Test Complete ===");
}

