use std::time::Instant;
use std::fs::File;
use std::io::Write;
use std::path::PathBuf;
use waffledb_core::vector::types::Vector;
use waffledb_core::storage::wal::{WriteAheadLog, WALEntry};
use waffledb_core::metadata::schema::Metadata;

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

struct InsertBenchmarkResult {
    insert_count: usize,
    batch_size: usize,
    vectors_per_sec: f32,
    avg_latency_us: f32,
    p95_latency_us: f32,
    total_time_sec: f32,
}

fn benchmark_wal_insert(
    insert_count: usize,
    vectors_per_sec_target: f32,
) -> InsertBenchmarkResult {
    println!("\n  📝 WAL Insert Benchmark: {} vectors", insert_count);

    // Create temp directory for WAL
    let temp_dir = PathBuf::from(format!(
        "target/bench_wal_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&temp_dir).ok();

    let mut wal = WriteAheadLog::new(&temp_dir).unwrap();
    let mut latencies = Vec::new();

    // Insert vectors
    let start = Instant::now();
    for i in 0..insert_count {
        let entry = WALEntry::Insert {
            id: format!("vec_{}", i),
            vector: vec![i as f32 % 10.0; 128],
            metadata: Some(format!("metadata_{}", i)),
        };

        let insert_start = Instant::now();
        wal.append(entry).ok();
        let latency_us = insert_start.elapsed().as_micros() as f32;
        latencies.push(latency_us);
    }
    let total_time = start.elapsed();

    // Sync to disk once
    let sync_start = Instant::now();
    wal.sync().ok();
    let sync_time = sync_start.elapsed();

    // Calculate stats
    latencies.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let p95_idx = (latencies.len() as f32 * 0.95) as usize;
    let avg_latency = latencies.iter().sum::<f32>() / latencies.len() as f32;
    let p95_latency = latencies.get(p95_idx).copied().unwrap_or(0.0);
    let vectors_per_sec = insert_count as f32 / total_time.as_secs_f32();

    println!(
        "    ✓ {:.0} vectors/sec, avg latency={:.2}µs, p95={:.2}µs, sync={:.2}ms",
        vectors_per_sec,
        avg_latency,
        p95_latency,
        sync_time.as_secs_f32() * 1000.0
    );

    // Cleanup
    let _ = std::fs::remove_dir_all(&temp_dir);

    InsertBenchmarkResult {
        insert_count,
        batch_size: 1,
        vectors_per_sec,
        avg_latency_us: avg_latency,
        p95_latency_us: p95_latency,
        total_time_sec: total_time.as_secs_f32(),
    }
}

fn benchmark_metadata_operations(vector_count: usize) -> InsertBenchmarkResult {
    println!("\n  🏷️  Metadata Operations: {} vectors", vector_count);

    let mut metadata_list = vec![];
    let mut latencies = Vec::new();

    let start = Instant::now();
    for i in 0..vector_count {
        let op_start = Instant::now();
        
        let mut meta = Metadata::new();
        meta.insert(
            "vector_id".to_string(),
            format!("vec_{}", i),
        );
        meta.insert(
            "category".to_string(),
            format!("cat_{}", i % 10),
        );
        meta.insert(
            "score".to_string(),
            format!("{}", (i as f32 % 100.0) as i32),
        );
        
        metadata_list.push(meta);
        
        let latency_us = op_start.elapsed().as_micros() as f32;
        latencies.push(latency_us);
    }
    let total_time = start.elapsed();

    latencies.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let p95_idx = (latencies.len() as f32 * 0.95) as usize;
    let avg_latency = latencies.iter().sum::<f32>() / latencies.len() as f32;
    let p95_latency = latencies.get(p95_idx).copied().unwrap_or(0.0);
    let ops_per_sec = vector_count as f32 / total_time.as_secs_f32();

    println!(
        "    ✓ {:.0} ops/sec, avg latency={:.2}µs, p95={:.2}µs",
        ops_per_sec,
        avg_latency,
        p95_latency
    );

    InsertBenchmarkResult {
        insert_count: vector_count,
        batch_size: 1,
        vectors_per_sec: ops_per_sec,
        avg_latency_us: avg_latency,
        p95_latency_us: p95_latency,
        total_time_sec: total_time.as_secs_f32(),
    }
}

fn main() {
    println!("🚀 WaffleDB Insert Benchmark Suite");
    println!("==================================\n");

    let mut results = Vec::new();

    // WAL insert benchmarks
    println!("📊 Write-Ahead Log (WAL) Performance");
    results.push(benchmark_wal_insert(1_000, 1000.0));
    results.push(benchmark_wal_insert(10_000, 10000.0));
    results.push(benchmark_wal_insert(100_000, 100000.0));

    // Metadata operation benchmarks
    println!("\n📊 Metadata Operations");
    results.push(benchmark_metadata_operations(1_000));
    results.push(benchmark_metadata_operations(10_000));
    results.push(benchmark_metadata_operations(100_000));

    // Write to CSV
    println!("\n📄 Writing results to benchmark_insert_results.csv...");
    let mut file = File::create("benchmark_insert_results.csv").unwrap();
    writeln!(file, "operation,count,vectors_per_sec,avg_latency_us,p95_latency_us,total_time_sec").unwrap();
    
    for result in &results {
        let op_type = if result.insert_count <= 100_000 { "WAL Insert" } else { "Metadata Ops" };
        writeln!(
            file,
            "{},{},{:.2},{:.2},{:.2},{:.3}",
            op_type,
            result.insert_count,
            result.vectors_per_sec,
            result.avg_latency_us,
            result.p95_latency_us,
            result.total_time_sec
        ).unwrap();
    }

    println!("✅ Insert benchmark complete!\n");

    // Print summary table
    println!("📊 INSERT PERFORMANCE SUMMARY");
    println!("┌────────────────┬──────────┬──────────┬──────────────┬──────────────┐");
    println!("│ Operation      │ Count    │ Vectors/ │ Avg(µs)      │ p95(µs)      │");
    println!("│                │          │ sec      │              │              │");
    println!("├────────────────┼──────────┼──────────┼──────────────┼──────────────┤");
    
    for result in &results {
        let op_type = if result.insert_count <= 100_000 { "WAL Insert" } else { "Metadata" };
        println!(
            "│ {:<14} │ {:8} │ {:8.0} │ {:12.2} │ {:12.2} │",
            op_type,
            result.insert_count,
            result.vectors_per_sec,
            result.avg_latency_us,
            result.p95_latency_us
        );
    }
    println!("└────────────────┴──────────┴──────────┴──────────────┴──────────────┘");
}
