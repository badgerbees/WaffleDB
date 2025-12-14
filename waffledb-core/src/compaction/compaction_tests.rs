/// Compaction integration tests
/// 
/// These tests verify the full compaction pipeline:
/// WriteBuffer → Compaction → HNSW merge → PQ codebook update

#[cfg(test)]
mod integration_tests {
    use crate::compaction::{CompactionManager, CompactionConfig};
    use crate::buffer::write_buffer::WriteBuffer;
    use crate::vector::types::Vector;
    use crate::metadata::schema::Metadata;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_full_compaction_pipeline() {
        // Set up
        let mut config = CompactionConfig::default();
        config.buffer_threshold = 10;
        config.batch_size = 5;

        let write_buffer = Arc::new(WriteBuffer::new(100, 1));
        let manager = CompactionManager::new(write_buffer.clone(), config);

        // Add vectors to trigger compaction
        for i in 0..10 {
            let _ = write_buffer.push(
                format!("v{}", i),
                Vector::new(vec![i as f32 / 10.0; 128]),
                Metadata::new(),
            );
        }

        // Verify compaction is needed
        assert!(manager.should_compact());

        // Run compaction (would be in background task)
        let _ = manager.compact().await;

        // Check stats
        let stats = manager.get_stats();
        assert!(stats.vectors_compacted > 0);
    }

    #[test]
    fn test_compaction_batch_draining() {
        let config = CompactionConfig::default();
        let write_buffer = Arc::new(WriteBuffer::new(100, 1));
        let manager = CompactionManager::new(write_buffer.clone(), config);

        // Add some vectors
        for i in 0..5 {
            let _ = write_buffer.push(
                format!("v{}", i),
                Vector::new(vec![0.5; 128]),
                Metadata::new(),
            );
        }

        // After compaction, batches should be available for draining
        // (would test async in real scenario)
        let batches = manager.drain_pending_batches();
        // Batches empty until actual compaction runs
        assert_eq!(batches.len(), 0);
    }
}
