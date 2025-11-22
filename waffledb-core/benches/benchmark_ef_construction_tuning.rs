// Quick ef_construction tuning benchmark
// Tests how different ef_construction values affect insert speed

use waffledb_core::hnsw::builder::HNSWBuilder;
use waffledb_core::hnsw::insert::HNSWInserter;
use waffledb_core::hnsw::graph::HNSWIndex;
use std::collections::HashMap;
use std::time::Instant;

fn generate_vectors(count: usize, dim: usize) -> Vec<Vec<f32>> {
    (0..count)
        .map(|_| {
            let data = (0..dim)
                .map(|_| (rand::random::<f32>() - 0.5) * 2.0)
                .collect::<Vec<_>>();
            
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
    println!("\n⚙️  ef_construction TUNING");
    println!("══════════════════════════════════════════════");
    println!("Testing different ef_construction values for 5K vectors\n");
    
    let vectors = generate_vectors(5_000, 128);
    let ef_values = vec![50, 100, 150, 200, 250];
    
    for ef_construction in ef_values {
        let builder = HNSWBuilder::new()
            .with_m(16)
            .with_ef_construction(ef_construction)
            .with_ef_search(50);
        
        let inserter = HNSWInserter::new(builder);
        let mut index = HNSWIndex::new(16, 0.5);
        let mut vector_store = HashMap::new();
        
        let start = Instant::now();
        
        for (idx, vector) in vectors.iter().enumerate() {
            vector_store.insert(idx, vector.clone());
            
            let get_vector = |node_id: usize| vector_store.get(&node_id).cloned();
            let distance_fn = |a: &[f32], b: &[f32]| {
                a.iter()
                    .zip(b.iter())
                    .map(|(x, y)| (x - y).powi(2))
                    .sum::<f32>()
                    .sqrt()
            };
            
            inserter.insert(&mut index, idx, vector, &get_vector, &distance_fn);
        }
        
        let elapsed = start.elapsed().as_secs_f32();
        let throughput = 5000.0 / elapsed;
        
        println!("ef_construction={:<3}  →  {:.0} inserts/sec  ({:.2}s total)", 
                 ef_construction, throughput, elapsed);
    }
    
    println!("\n══════════════════════════════════════════════");
    println!("Lower ef_construction = faster inserts, slightly lower quality\n");
}
