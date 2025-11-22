// Quick sanity check for HNSW search
use waffledb_core::hnsw::graph::HNSWIndex;
use waffledb_core::hnsw::search::search_hnsw_layers;
use waffledb_core::vector::distance::DistanceMetric;

fn main() {
    println!("Testing HNSW search connectivity...\n");
    
    // Create simple test vectors
    let vectors = vec![
        vec![1.0, 0.0, 0.0, 0.0],
        vec![0.9, 0.1, 0.0, 0.0],
        vec![0.8, 0.2, 0.0, 0.0],
        vec![0.0, 1.0, 0.0, 0.0],
        vec![0.0, 0.0, 1.0, 0.0],
    ];
    
    println!("Created {} test vectors", vectors.len());
    
    // Build HNSW index
    let mut index = HNSWIndex::new(16, 0.5);
    
    // Add nodes and edges to simulate real graph
    for i in 0..vectors.len() {
        index.get_or_create_layer(0);
        index.insert_node(i, 0);
        
        // Add edges to previous nodes (simulate bidirectional M-neighbors)
        for j in 0..i {
            index.add_edge(i, j, 0);
            index.add_edge(j, i, 0);
        }
    }
    
    println!("Built HNSW index with {} nodes", vectors.len());
    println!("Layer 0 has {} nodes\n", index.layers[0].size());
    
    // Test search
    let query = vec![1.0, 0.0, 0.0, 0.0]; // Should find node 0 or 1 or 2
    
    println!("Query vector: {:?}", query);
    println!("Running HNSW search with ef_search=10...\n");
    
    let results = search_hnsw_layers(
        &query,
        &index.layers,
        0, // entry point
        10,
        &|node_id| vectors.get(node_id).cloned(),
        DistanceMetric::L2,
    );
    
    println!("Search results ({} returned):", results.len());
    for (i, result) in results.iter().enumerate() {
        let dist_to_query = query.iter()
            .zip(&vectors[result.node_id])
            .map(|(a, b)| (a - b).powi(2))
            .sum::<f32>()
            .sqrt();
        println!("  {}. Node {}: distance={:.4} (verified={:.4})", 
                 i, result.node_id, result.distance, dist_to_query);
    }
    
    // Compare with brute force
    println!("\nBrute force ground truth (top 3):");
    let mut distances: Vec<(usize, f32)> = vectors
        .iter()
        .enumerate()
        .map(|(idx, vec)| {
            let dist = query.iter()
                .zip(vec.iter())
                .map(|(a, b)| (a - b).powi(2))
                .sum::<f32>()
                .sqrt();
            (idx, dist)
        })
        .collect();
    distances.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
    
    for (i, (node_id, dist)) in distances.iter().take(3).enumerate() {
        println!("  {}. Node {}: distance={:.4}", i, node_id, dist);
    }
}
