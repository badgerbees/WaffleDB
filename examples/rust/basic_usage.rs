// Rust usage example

use waffledb_core::hnsw::{HNSWBuilder, HNSWIndex};
use waffledb_core::vector::types::Vector;
use waffledb_core::compression::binary_quant::BinaryQuantizer;
use waffledb_core::compression::scalar_quant::ScalarQuantizer;

fn main() {
    println!("=== WaffleDB Rust Core Example ===\n");
    
    // Create HNSW index
    let builder = HNSWBuilder::new()
        .with_m(16)
        .with_ef_construction(200)
        .with_ef_search(200);
    
    let mut index = HNSWIndex::new(builder.m, builder.m_l);
    println!("Created HNSW index with M=16");
    
    // Create some vectors
    let vectors = vec![
        vec![0.1, 0.2, 0.3, 0.4],
        vec![0.15, 0.25, 0.35, 0.45],
        vec![0.2, 0.3, 0.4, 0.5],
        vec![0.5, 0.4, 0.3, 0.2],
        vec![0.9, 0.8, 0.7, 0.6],
    ];
    
    println!("Created {} vectors with dimension 4\n", vectors.len());
    
    // Binary Quantization example
    println!("=== Binary Quantization ===");
    let bq = BinaryQuantizer::train(&vectors);
    for (i, vec) in vectors.iter().enumerate() {
        let encoded = bq.encode(vec);
        println!("Vector {}: {} bytes", i, encoded.len());
    }
    
    // Scalar Quantization example
    println!("\n=== Scalar Quantization ===");
    let sq = ScalarQuantizer::train(&vectors);
    for (i, vec) in vectors.iter().enumerate() {
        let encoded = sq.encode(vec);
        println!("Vector {}: {} bytes", i, encoded.len());
    }
    
    // Vector operations
    println!("\n=== Vector Operations ===");
    let v1 = Vector::new(vec![1.0, 0.0, 0.0]);
    let v2 = Vector::new(vec![0.0, 1.0, 0.0]);
    
    println!("Vector 1: {:?}", v1.data);
    println!("Vector 2: {:?}", v2.data);
    println!("L2 norm of V1: {:.4}", v1.l2_norm());
    
    let normalized = v1.normalize();
    println!("Normalized V1: {:?}", normalized.data);
    
    // Distance metrics
    use waffledb_core::vector::distance::{DistanceMetric, l2_distance, cosine_distance};
    
    println!("\n=== Distance Metrics ===");
    let d_l2 = l2_distance(&v1.data, &v2.data);
    let d_cos = cosine_distance(&v1.data, &v2.data);
    
    println!("L2 distance between V1 and V2: {:.4}", d_l2);
    println!("Cosine distance between V1 and V2: {:.4}", d_cos);
    
    println!("\n=== Example Complete ===");
}

