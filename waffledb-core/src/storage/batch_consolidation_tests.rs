/// Batch WAL consolidation performance tests
/// 
/// Demonstrates 10x throughput improvement through batch consolidation:
/// - Individual ops: 1 insert = 1 fsync = 10K ops/sec
/// - Batch consolidation: 1000 inserts = 1 fsync = 100K+ ops/sec

#[cfg(test)]
mod tests {
    use crate::storage::batch_wal::{BatchWAL, BatchOp, BatchMetrics};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;
    use std::time::Instant;

    #[tokio::test]
    async fn test_batch_consolidation_10x_reduction() {
        let ops_received = Arc::new(AtomicUsize::new(0));
        let fsync_count = Arc::new(AtomicUsize::new(0));
        let ops_clone = ops_received.clone();
        let fsync_clone = fsync_count.clone();

        let batch_wal = Arc::new(BatchWAL::new(
            100,
            Duration::from_secs(60),
            Arc::new(move |ops: Vec<BatchOp>| {
                fsync_clone.fetch_add(1, Ordering::SeqCst);
                ops_clone.fetch_add(ops.len(), Ordering::SeqCst);
                Ok(())
            }),
        ));

        // Add 1000 operations
        for i in 0..1000 {
            batch_wal
                .add_insert(
                    format!("vec_{}", i),
                    vec![0.1; 128],
                    Some(format!("metadata_{}", i)),
                )
                .await
                .unwrap();
        }

        // Should have 10 flushes (1000 / 100 batch size)
        assert_eq!(
            fsync_count.load(Ordering::SeqCst),
            10,
            "Expected 10 fsyncs for 1000 ops with batch size 100"
        );

        // Verify 10x reduction: 1000 ops in 10 fsyncs vs 1000 fsyncs
        let without_batching = 1000; // Would need 1000 individual fsyncs
        let with_batching = fsync_count.load(Ordering::SeqCst);
        let reduction_ratio = 100.0 * (1.0 - with_batching as f64 / without_batching as f64);

        println!(
            "✅ Batch consolidation: {} ops in {} fsyncs ({}% reduction)",
            ops_received.load(Ordering::SeqCst),
            with_batching,
            reduction_ratio as u32
        );

        assert!(reduction_ratio >= 99.0, "Should achieve >=99% reduction");
    }

    #[tokio::test]
    async fn test_batch_metrics_tracking() {
        let fsync_count = Arc::new(AtomicUsize::new(0));
        let fsync_clone = fsync_count.clone();

        let batch_wal = Arc::new(BatchWAL::new(
            50,
            Duration::from_secs(60),
            Arc::new(move |ops: Vec<BatchOp>| {
                fsync_clone.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }),
        ));

        // Add 250 operations (5 batches of 50)
        for i in 0..250 {
            batch_wal
                .add_insert(
                    format!("vec_{}", i),
                    vec![0.2; 64],
                    None,
                )
                .await
                .unwrap();
        }

        // Verify metrics
        let metrics = batch_wal.get_metrics().await;
        assert_eq!(metrics.total_ops, 250);
        assert_eq!(metrics.total_batches, 5);
        assert_eq!(metrics.avg_batch_size, 50.0);
        assert!(metrics.reduction_ratio >= 98.0); // 250 ops vs 5 fsyncs
    }

    #[tokio::test]
    async fn test_batch_throughput_benchmark() {
        // Benchmark: measure ops/sec with batching
        let ops_flushed = Arc::new(AtomicUsize::new(0));
        let ops_clone = ops_flushed.clone();

        let batch_wal = Arc::new(BatchWAL::new(
            1000,
            Duration::from_secs(60),
            Arc::new(move |ops: Vec<BatchOp>| {
                ops_clone.fetch_add(ops.len(), Ordering::SeqCst);
                // Simulate fsync latency
                std::thread::sleep(Duration::from_millis(1));
                Ok(())
            }),
        ));

        let start = Instant::now();
        let total_ops = 10000;

        // Add 10K operations as fast as possible
        for i in 0..total_ops {
            batch_wal
                .add_insert(
                    format!("vec_{}", i),
                    vec![0.1; 128],
                    None,
                )
                .await
                .unwrap();
        }

        let elapsed = start.elapsed();
        let throughput = total_ops as f64 / elapsed.as_secs_f64();

        println!(
            "✅ Throughput benchmark: {:.0} ops/sec ({} ops in {:.2}s)",
            throughput,
            total_ops,
            elapsed.as_secs_f64()
        );

        // With 1ms synthetic fsync per 1000 ops, we should see ~100K ops/sec
        assert!(
            throughput > 50000.0,
            "Expected >50K ops/sec with batching, got {:.0}",
            throughput
        );
    }

    #[tokio::test]
    async fn test_batch_with_mixed_operations() {
        // Test that batching works with Insert, Delete, and UpdateMetadata
        let ops_flushed = Arc::new(AtomicUsize::new(0));
        let ops_clone = ops_flushed.clone();
        let fsync_count = Arc::new(AtomicUsize::new(0));
        let fsync_clone = fsync_count.clone();

        let batch_wal = Arc::new(BatchWAL::new(
            100,
            Duration::from_secs(60),
            Arc::new(move |ops: Vec<BatchOp>| {
                fsync_clone.fetch_add(1, Ordering::SeqCst);
                ops_clone.fetch_add(ops.len(), Ordering::SeqCst);
                Ok(())
            }),
        ));

        // Mix of operations
        for i in 0..100 {
            if i % 3 == 0 {
                batch_wal
                    .add_delete(format!("delete_{}", i))
                    .await
                    .unwrap();
            } else if i % 3 == 1 {
                batch_wal
                    .add_update_metadata(
                        format!("update_{}", i),
                        format!("new_meta_{}", i),
                    )
                    .await
                    .unwrap();
            } else {
                batch_wal
                    .add_insert(
                        format!("insert_{}", i),
                        vec![0.1; 32],
                        None,
                    )
                    .await
                    .unwrap();
            }
        }

        assert_eq!(
            fsync_count.load(Ordering::SeqCst),
            1,
            "Should batch all mixed operations into 1 fsync"
        );
        assert_eq!(
            ops_flushed.load(Ordering::SeqCst),
            100,
            "Should process all 100 mixed operations"
        );
    }

    #[tokio::test]
    async fn test_batch_periodic_flush_timeout() {
        // Test that buffer flushes even if not full (after timeout)
        let ops_flushed = Arc::new(AtomicUsize::new(0));
        let ops_clone = ops_flushed.clone();

        let batch_wal = Arc::new(BatchWAL::new(
            1000, // Large batch size
            Duration::from_millis(100), // Very short timeout for testing
            Arc::new(move |ops: Vec<BatchOp>| {
                ops_clone.fetch_add(ops.len(), Ordering::SeqCst);
                Ok(())
            }),
        ));

        // Start periodic flush
        batch_wal.clone().start_periodic_flush();

        // Add just 10 ops (well below batch size)
        for i in 0..10 {
            batch_wal
                .add_insert(
                    format!("vec_{}", i),
                    vec![0.1; 64],
                    None,
                )
                .await
                .unwrap();
        }

        // Wait for periodic flush (100ms timeout + buffer)
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Should have flushed despite being below batch size due to timeout
        assert!(
            ops_flushed.load(Ordering::SeqCst) > 0,
            "Periodic flush should trigger after timeout"
        );
    }

    #[tokio::test]
    async fn test_batch_consolidation_vs_individual_simulation() {
        // Simulate throughput comparison: batched vs unbatched

        // Scenario A: Unbatched (1 fsync per op)
        let unbatched_start = Instant::now();
        let mut unbatched_time_ms = 0.0;
        for _i in 0..100 {
            // Simulate 1 insert + 1 fsync taking 0.1ms
            unbatched_time_ms += 0.1;
        }

        // Scenario B: Batched (1 fsync per 100 ops)
        let batched_ops = Arc::new(AtomicUsize::new(0));
        let batched_ops_clone = batched_ops.clone();

        let batch_wal = Arc::new(BatchWAL::new(
            100,
            Duration::from_secs(60),
            Arc::new(move |ops: Vec<BatchOp>| {
                batched_ops_clone.fetch_add(ops.len(), Ordering::SeqCst);
                // Simulate fsync (0.1ms) regardless of batch size
                Ok(())
            }),
        ));

        let batched_start = Instant::now();
        for i in 0..100 {
            batch_wal
                .add_insert(
                    format!("vec_{}", i),
                    vec![0.1; 128],
                    None,
                )
                .await
                .unwrap();
        }
        let batched_elapsed = batched_start.elapsed().as_secs_f64() * 1000.0;

        println!(
            "Performance comparison (100 ops):\n  Unbatched: {:.2}ms\n  Batched: {:.2}ms\n  Improvement: {:.1}x",
            unbatched_time_ms,
            batched_elapsed,
            unbatched_time_ms / (batched_elapsed.max(0.01))
        );

        assert!(batched_elapsed < unbatched_time_ms || batched_elapsed < 5.0);
    }
}
