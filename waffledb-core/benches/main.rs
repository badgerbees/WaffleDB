/// Master benchmark suite for WaffleDB
/// Runs all Phase 3 benchmarks and generates comprehensive report

use std::time::Instant;

mod batch_bench;
mod sharding_bench;
mod snapshots_bench;
mod load_testing;
mod generic_bench;
mod hnsw_core_bench;
mod vector_ops_bench;
mod search_bench;
// mod metadata_bench;  // DISABLED - Under development
// mod compression_bench;  // DISABLED - Under development
// mod replication_bench;  // DISABLED - Under development

fn main() {
    let overall_start = Instant::now();

    println!("\n");
    println!("{}", "=".repeat(70));
    println!("WAFFLEDB COMPREHENSIVE PERFORMANCE BENCHMARK SUITE");
    println!("Phase 3.3 - 3.5: Batch Ops, Sharding, Recovery, Load Testing");
    println!("{}", "=".repeat(70));
    println!();

    // Phase 3.3: Batch Operations
    println!("\n{}", "-".repeat(70));
    println!("PHASE 3.3: BATCH OPERATIONS BENCHMARKS");
    println!("{}", "-".repeat(70));
    println!();
    println!("Testing insertion throughput, latency, WAL consolidation, memory usage...");
    println!("Vector counts: 10K, 100K, 1M | Vector dimensions: 128, 256, 512");
    batch_bench::run_batch_benchmarks();

    // Phase 3.3: Shard Routing
    println!("\n{}", "-".repeat(70));
    println!("PHASE 3.3: SHARD ROUTING BENCHMARKS");
    println!("{}", "-".repeat(70));
    println!();
    println!("Testing routing latency, distribution uniformity, parallel throughput...");
    println!("Shard counts: 4, 8, 16, 32, 64 | Vector count: 1M");
    sharding_bench::run_sharding_benchmarks();

    // Phase 3.4: Snapshots & Recovery
    println!("\n{}", "-".repeat(70));
    println!("PHASE 3.4: SNAPSHOTS & RECOVERY BENCHMARKS");
    println!("{}", "-".repeat(70));
    println!();
    println!("Testing snapshot creation, restoration, compression, verification...");
    println!("Vector counts: 10K, 50K, 100K, 500K | Dimension: 512");
    snapshots_bench::run_snapshots_benchmarks();

    // Phase 3.5: Admin API
    println!("\n[Admin API benchmarks code removed - not implemented in this build]\n");

    // Load Testing
    println!("\n{}", "-".repeat(70));
    println!("LOAD TESTING: EXTREME STRESS SCENARIOS");
    println!("{}", "-".repeat(70));
    println!();
    println!("Testing sustained throughput, burst handling, memory pressure...");
    load_testing::run_load_tests();

    // Generic Benchmarks
    println!("\n{}", "-".repeat(70));
    println!("GENERIC BENCHMARKS: END-TO-END OPERATIONS");
    println!("{}", "-".repeat(70));
    println!();
    println!("Testing complete workflows, data integrity, memory efficiency...");
    generic_bench::run_generic_benchmarks();

    // HNSW Core
    println!("\n{}", "-".repeat(70));
    println!("HNSW CORE: INDEX OPERATIONS");
    println!("{}", "-".repeat(70));
    println!();
    println!("Testing index construction, insertion, deletion, search...");
    hnsw_core_bench::run_hnsw_benchmarks();

    // Vector Operations
    println!("\n{}", "-".repeat(70));
    println!("VECTOR OPS: DISTANCE METRICS, QUANTIZATION, TRANSFORMATIONS");
    println!("{}", "-".repeat(70));
    println!();
    println!("Testing distance metrics, normalization, quantization...");
    vector_ops_bench::run_vector_ops_benchmarks();

    // Search Algorithms
    println!("\n{}", "-".repeat(70));
    println!("SEARCH: HYBRID, FILTERED, AGGREGATED QUERIES");
    println!("{}", "-".repeat(70));
    println!();
    println!("Testing hybrid search, filtering, aggregation, complex queries...");
    search_bench::run_search_benchmarks();

    // Metadata Operations [DISABLED - FIX IN PROGRESS]
    // println!("\n{}", "-".repeat(70));
    // println!("METADATA: SCHEMA, INDEXING, FIELD OPERATIONS");
    // println!("{}", "-".repeat(70));
    // println!();
    // println!("Testing field indexing, schema evolution, validation...");
    // metadata_bench::run_metadata_benchmarks();

    // Compression [DISABLED - FIX IN PROGRESS]
    // println!("\n{}", "-".repeat(70));
    // println!("COMPRESSION: DELTA, CODECS, COMPRESSION RATIOS");
    // println!("{}", "-".repeat(70));
    // println!();
    // println!("Testing delta encoding, bit packing, compression ratios...");
    // compression_bench::run_compression_benchmarks();

    // Replication & HA [DISABLED - FIX IN PROGRESS]
    // println!("\n{}", "-".repeat(70));
    // println!("REPLICATION & HA: FAILOVER, CONSISTENCY, LAG");
    // println!("{}", "-".repeat(70));
    // println!();
    // println!("Testing replication throughput, failover, consistency...");
    // replication_bench::run_replication_benchmarks();

    // Summary
    let total_time_ms = overall_start.elapsed().as_millis();
    
    println!("\n{}", "=".repeat(70));
    println!("BENCHMARK EXECUTION COMPLETE");
    println!("{}", "=".repeat(70));
    println!();
    println!("Total Runtime: {}ms", total_time_ms);
    println!();
}
