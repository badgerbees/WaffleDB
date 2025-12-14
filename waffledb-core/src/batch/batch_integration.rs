/// Integration tests for batch operations with sharding and memory pooling.
///
/// Tests the complete pipeline: batch request → shard routing → pipeline execution → memory pool reuse

use crate::batch::batch_operations::{
    BatchInsertPipeline, BatchDeletePipeline, BatchInsertRequest, BatchDeleteRequest,
    WriteBuffer, MemoryPool, OperationStatus,
};
use crate::batch::shard_router::{BatchShardRouter, HashShardingStrategy};
use std::sync::Arc;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_integration_batch_insert_with_sharding() {
        let strategy = Arc::new(HashShardingStrategy);
        let router = BatchShardRouter::new(4, strategy);
        let pipeline = BatchInsertPipeline::new(1000);

        // Create batch request with 100 vectors
        let mut request = BatchInsertRequest::new("col1".to_string());
        for i in 0..100 {
            request.add_vector(format!("id{}", i), vec![1.0, 2.0, 3.0], None);
        }

        // Route to shards
        let routes = router.route_inserts("col1", &request.ids);
        assert!(!routes.is_empty());

        // Process batch
        let results = pipeline.process_batch(&request);
        assert_eq!(results.len(), 100);
        assert!(results.iter().all(|s| s.is_success()));
    }

    #[test]
    fn test_integration_batch_delete_with_sharding() {
        let strategy = Arc::new(HashShardingStrategy);
        let router = BatchShardRouter::new(4, strategy);
        let pipeline = BatchDeletePipeline::new(1000);

        // Create batch delete request
        let mut request = BatchDeleteRequest::new("col1".to_string());
        for i in 0..50 {
            request.add_id(format!("id{}", i));
        }

        // Route to shards
        let routes = router.route_deletes("col1", &request.ids);
        assert!(!routes.is_empty());

        // Process batch
        let results = pipeline.process_batch(&request);
        assert_eq!(results.len(), 50);
        assert!(results.iter().all(|s| s.is_success()));
    }

    #[test]
    fn test_integration_memory_pool_reuse() {
        let pool = MemoryPool::new(100, 5);

        // Get and use buffers
        let mut buf1 = pool.get_vector_buffer();
        let mut buf2 = pool.get_id_buffer();

        buf1.push(vec![1.0, 2.0, 3.0]);
        buf2.push("id1".to_string());

        // Return to pool
        pool.return_vector_buffer(buf1);
        pool.return_id_buffer(buf2);

        // Get again - should be from pool
        let buf1_again = pool.get_vector_buffer();
        assert_eq!(buf1_again.capacity(), 100);

        let buf2_again = pool.get_id_buffer();
        assert_eq!(buf2_again.capacity(), 100);
    }

    #[test]
    fn test_integration_write_buffer_consolidation() {
        let buffer = WriteBuffer::new(100);

        // Simulate 200 writes (within buffer capacity)
        for i in 0..200 {
            let entry = crate::batch::batch_operations::WriteBufferEntry {
                operation_type: crate::batch::batch_operations::OperationType::Insert,
                vector_id: format!("id{}", i),
                collection_id: "col1".to_string(),
                timestamp: 1000 + i as u64,
            };

            // Buffer has capacity for batch_size * 2 = 200, so all should fit
            assert!(buffer.add_entry(entry));
        }

        let flushed = buffer.flush();
        
        // Should consolidate 200 writes into single batch
        assert_eq!(flushed.len(), 200);
    }

    #[test]
    fn test_integration_large_batch_end_to_end() {
        let strategy = Arc::new(HashShardingStrategy);
        let router = BatchShardRouter::new(16, strategy);
        let insert_pipeline = BatchInsertPipeline::new(10000);
        let pool = MemoryPool::new(10000, 10);

        // Create 1000-vector batch
        let mut request = BatchInsertRequest::new("col1".to_string());
        for i in 0..1000 {
            request.add_vector(
                format!("id{}", i),
                vec![1.0, 2.0, 3.0, 4.0, 5.0],
                None,
            );
        }

        // Route to shards
        let routes = router.route_inserts("col1", &request.ids);

        // Process batch
        let results = insert_pipeline.process_batch(&request);
        assert_eq!(results.len(), 1000);

        let success_count = results.iter().filter(|s| s.is_success()).count();
        assert_eq!(success_count, 1000);

        // Verify routing distributed across multiple shards
        assert!(routes.len() > 1);
    }

    #[test]
    fn test_integration_concurrent_pool_access() {
        let pool = Arc::new(MemoryPool::new(100, 5));
        let mut handles = vec![];

        // Simulate concurrent access
        for _ in 0..5 {
            let pool_clone = Arc::clone(&pool);
            let handle = std::thread::spawn(move || {
                let buf = pool_clone.get_vector_buffer();
                std::thread::sleep(std::time::Duration::from_millis(1));
                pool_clone.return_vector_buffer(buf);
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        // Pool should maintain size
        let (vecs, _) = pool.stats();
        assert!(vecs <= 5);
    }

    #[test]
    fn test_integration_batch_with_mixed_operations() {
        let insert_pipeline = BatchInsertPipeline::new(100);
        let delete_pipeline = BatchDeletePipeline::new(100);

        // Insert batch
        let mut insert_req = BatchInsertRequest::new("col1".to_string());
        for i in 0..50 {
            insert_req.add_vector(format!("id{}", i), vec![1.0, 2.0], None);
        }

        let insert_results = insert_pipeline.process_batch(&insert_req);
        assert_eq!(insert_results.len(), 50);

        // Delete batch
        let mut delete_req = BatchDeleteRequest::new("col1".to_string());
        for i in 0..25 {
            delete_req.add_id(format!("id{}", i));
        }

        let delete_results = delete_pipeline.process_batch(&delete_req);
        assert_eq!(delete_results.len(), 25);

        // All should succeed
        assert!(insert_results.iter().all(|s| s.is_success()));
        assert!(delete_results.iter().all(|s| s.is_success()));
    }

    #[test]
    fn test_integration_sharding_consistency() {
        // Create two identical strategies and routers
        let strategy1 = Arc::new(HashShardingStrategy) as Arc<dyn crate::batch::shard_router::ShardingStrategy>;
        let strategy2 = Arc::new(HashShardingStrategy) as Arc<dyn crate::batch::shard_router::ShardingStrategy>;
        
        let router1 = BatchShardRouter::new(4, strategy1);
        let router2 = BatchShardRouter::new(4, strategy2);

        let ids = vec!["id1".to_string(), "id2".to_string(), "id3".to_string()];

        let routes1 = router1.route_inserts("col1", &ids);
        let routes2 = router2.route_inserts("col1", &ids);

        // Same algorithm should route consistently
        // (routes may be in different order due to HashMap iteration, so check totals)
        let total1: usize = routes1.iter().map(|r| r.vector_ids.len()).sum();
        let total2: usize = routes2.iter().map(|r| r.vector_ids.len()).sum();
        
        assert_eq!(total1, total2);
        assert_eq!(total1, 3);
    }

    #[test]
    fn test_integration_memory_pool_capacity_limit() {
        let pool = MemoryPool::new(100, 2);

        // Get all available buffers
        let buf1 = pool.get_vector_buffer();
        let buf2 = pool.get_vector_buffer();
        let buf3 = pool.get_vector_buffer(); // Should allocate new

        assert_eq!(pool.stats().0, 0);

        // Return first two
        pool.return_vector_buffer(buf1);
        pool.return_vector_buffer(buf2);
        pool.return_vector_buffer(buf3);

        // Should cap at pool size
        assert_eq!(pool.stats().0, 2);
    }

    #[test]
    fn test_integration_batch_request_validation() {
        let mut request = BatchInsertRequest::new("col1".to_string());
        
        assert!(request.is_empty());
        assert_eq!(request.len(), 0);

        request.add_vector("id1".to_string(), vec![1.0, 2.0], None);
        assert!(!request.is_empty());
        assert_eq!(request.len(), 1);

        request.add_vector("id2".to_string(), vec![3.0, 4.0], Some("meta".to_string()));
        assert_eq!(request.len(), 2);
        assert_eq!(request.metadata[1], Some("meta".to_string()));
    }

    #[test]
    fn test_integration_throughput_simulation() {
        // Simulate 100k writes across 4 shards with consolidation
        let num_vectors = 100_000;
        let batch_size = 1000;
        let num_shards = 4;

        let strategy = Arc::new(HashShardingStrategy);
        let router = BatchShardRouter::new(num_shards, strategy);
        let pipeline = BatchInsertPipeline::new(batch_size);

        let mut total_successful = 0;

        for batch_num in 0..=(num_vectors / batch_size) {
            let start = batch_num * batch_size;
            let end = std::cmp::min(start + batch_size, num_vectors);
            
            if start >= num_vectors {
                break;
            }

            let mut request = BatchInsertRequest::new("col1".to_string());
            for i in start..end {
                request.add_vector(format!("id{}", i), vec![1.0, 2.0, 3.0], None);
            }

            let _routes = router.route_inserts("col1", &request.ids);
            let results = pipeline.process_batch(&request);

            total_successful += results.iter().filter(|s| s.is_success()).count();
        }

        assert_eq!(total_successful, num_vectors);
    }

    #[test]
    fn test_integration_write_buffer_auto_flush() {
        let buffer = WriteBuffer::new(10);

        // Add vectors up to batch size
        for i in 0..10 {
            let entry = crate::batch::batch_operations::WriteBufferEntry {
                operation_type: crate::batch::batch_operations::OperationType::Insert,
                vector_id: format!("id{}", i),
                collection_id: "col1".to_string(),
                timestamp: 1000 + i as u64,
            };
            buffer.add_entry(entry);
        }

        assert!(buffer.should_flush());

        let flushed = buffer.flush();
        assert_eq!(flushed.len(), 10);
        assert!(!buffer.should_flush());
    }
}
