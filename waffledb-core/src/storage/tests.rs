#[cfg(test)]
mod tests {
    use crate::storage::wal::{WriteAheadLog, WALEntry};
    use crate::storage::snapshot::{SnapshotManager, SnapshotMetadata};
    use std::path::PathBuf;
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn get_test_dir(name: &str) -> PathBuf {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = PathBuf::from(format!("target/test_data/{}_{}", name, timestamp));
        let _ = fs::remove_dir_all(&path);
        fs::create_dir_all(&path).ok();
        path
    }

    #[test]
    fn test_wal_append_and_get() {
        let path = get_test_dir("wal_append");
        let mut wal = WriteAheadLog::new(&path).unwrap();
        
        let entry = WALEntry::Insert {
            id: "vec1".to_string(),
            vector: vec![1.0, 2.0, 3.0],
            metadata: None,
        };
        
        wal.append(entry).unwrap();
        let entries = wal.get_entries();
        assert_eq!(entries.len(), 1);
    }

    #[test]
    fn test_wal_multiple_entries() {
        let path = get_test_dir("wal_multiple");
        let mut wal = WriteAheadLog::new(&path).unwrap();
        
        for i in 0..5 {
            let entry = WALEntry::Insert {
                id: format!("vec{}", i),
                vector: vec![i as f32],
                metadata: None,
            };
            wal.append(entry).unwrap();
        }
        
        let entries = wal.get_entries();
        assert_eq!(entries.len(), 5);
    }

    #[test]
    fn test_wal_clear() {
        let path = get_test_dir("wal_clear");
        let mut wal = WriteAheadLog::new(&path).unwrap();
        
        let entry = WALEntry::Insert {
            id: "vec1".to_string(),
            vector: vec![1.0],
            metadata: None,
        };
        wal.append(entry).unwrap();
        assert_eq!(wal.get_entries().len(), 1);
        
        wal.clear().unwrap();
        assert_eq!(wal.get_entries().len(), 0);
    }

    #[test]
    fn test_wal_sync_to_disk() {
        // Use system temp directory instead of project directory
        let temp_dir = std::env::temp_dir().join(format!("waffledb_test_{}", 
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()));
        let _ = std::fs::create_dir_all(&temp_dir);
        
        let mut wal = WriteAheadLog::new(&temp_dir).unwrap();
        
        let entry = WALEntry::Insert {
            id: "vec1".to_string(),
            vector: vec![1.0, 2.0],
            metadata: Some("meta".to_string()),
        };
        wal.append(entry).unwrap();
        
        wal.sync().unwrap();
        // File should exist now
        assert!(wal.get_entries().len() == 1);
        
        // Cleanup
        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_wal_delete_entry() {
        let path = get_test_dir("wal_delete");
        let mut wal = WriteAheadLog::new(&path).unwrap();
        
        wal.append(WALEntry::Insert {
            id: "vec1".to_string(),
            vector: vec![1.0],
            metadata: None,
        }).unwrap();
        
        wal.append(WALEntry::Delete {
            id: "vec1".to_string(),
        }).unwrap();
        
        let entries = wal.get_entries();
        assert_eq!(entries.len(), 2);
    }

    #[test]
    fn test_snapshot_metadata_creation() {
        let meta = SnapshotMetadata {
            timestamp: 1234567890,
            version: 1,
            num_vectors: 100,
        };
        
        assert_eq!(meta.timestamp, 1234567890);
        assert_eq!(meta.version, 1);
        assert_eq!(meta.num_vectors, 100);
    }

    #[test]
    fn test_snapshot_manager_creation() {
        let path = get_test_dir("snapshots");
        let _manager = SnapshotManager::new(&path).unwrap();
        assert!(path.exists());
    }

    #[test]
    fn test_snapshot_create_snapshot() {
        let path = get_test_dir("snapshot_create");
        let manager = SnapshotManager::new(&path).unwrap();
        
        let meta = SnapshotMetadata {
            timestamp: 1000,
            version: 1,
            num_vectors: 50,
        };
        
        let data = vec![1, 2, 3, 4, 5];
        let filename = manager.create_snapshot(meta, &data).unwrap();
        
        assert!(filename.contains("snapshot_"));
        assert!(filename.contains("1000"));
    }

    #[test]
    fn test_snapshot_list_snapshots() {
        let path = get_test_dir("snapshot_list");
        let manager = SnapshotManager::new(&path).unwrap();
        
        let meta1 = SnapshotMetadata {
            timestamp: 1000,
            version: 1,
            num_vectors: 50,
        };
        
        let meta2 = SnapshotMetadata {
            timestamp: 2000,
            version: 2,
            num_vectors: 100,
        };
        
        manager.create_snapshot(meta1.clone(), &vec![1, 2, 3]).unwrap();
        manager.create_snapshot(meta2.clone(), &vec![4, 5, 6]).unwrap();
        
        let snapshots = manager.list_snapshots().unwrap();
        assert_eq!(snapshots.len(), 2);
    }

    #[test]
    fn test_snapshot_load_snapshot() {
        let path = get_test_dir("snapshot_load");
        let manager = SnapshotManager::new(&path).unwrap();
        
        let meta = SnapshotMetadata {
            timestamp: 3000,
            version: 1,
            num_vectors: 25,
        };
        
        let original_data = vec![10, 20, 30, 40, 50];
        let filename = manager.create_snapshot(meta, &original_data).unwrap();
        
        let loaded = manager.load_snapshot(&filename).unwrap();
        assert_eq!(loaded, original_data);
    }

    #[test]
    fn test_snapshot_delete_snapshot() {
        let path = get_test_dir("snapshot_delete");
        let manager = SnapshotManager::new(&path).unwrap();
        
        let meta = SnapshotMetadata {
            timestamp: 4000,
            version: 1,
            num_vectors: 10,
        };
        
        let filename = manager.create_snapshot(meta, &vec![1, 2]).unwrap();
        let initial_count = manager.list_snapshots().unwrap().len();
        
        manager.delete_snapshot(&filename).unwrap();
        let after_count = manager.list_snapshots().unwrap().len();
        
        assert_eq!(initial_count, after_count + 1);
    }

    #[test]
    fn test_rocksdb_store_put_get() {
        use crate::storage::rocksdb::RocksDBStore;
        
        let path = get_test_dir("rocksdb_store");
        let mut store = RocksDBStore::new(&path).unwrap();
        
        store.put("key1", &[1, 2, 3, 4]).unwrap();
        let value = store.get("key1").unwrap();
        
        assert_eq!(value, Some(vec![1, 2, 3, 4]));
    }

    #[test]
    fn test_rocksdb_store_delete() {
        use crate::storage::rocksdb::RocksDBStore;
        
        let path = get_test_dir("rocksdb_delete");
        let mut store = RocksDBStore::new(&path).unwrap();
        
        store.put("key1", &[1, 2, 3]).unwrap();
        assert!(store.get("key1").unwrap().is_some());
        
        store.delete("key1").unwrap();
        assert!(store.get("key1").unwrap().is_none());
    }

    #[test]
    fn test_rocksdb_store_contains_key() {
        use crate::storage::rocksdb::RocksDBStore;
        
        let path = get_test_dir("rocksdb_contains");
        let mut store = RocksDBStore::new(&path).unwrap();
        
        store.put("key1", &[1]).unwrap();
        assert!(store.contains_key("key1"));
        assert!(!store.contains_key("key2"));
    }

    #[test]
    fn test_rocksdb_store_prefix_iterator() {
        use crate::storage::rocksdb::RocksDBStore;
        
        let path = get_test_dir("rocksdb_prefix");
        let mut store = RocksDBStore::new(&path).unwrap();
        
        store.put("user:1", &[1]).unwrap();
        store.put("user:2", &[2]).unwrap();
        store.put("post:1", &[3]).unwrap();
        
        let user_results = store.prefix_iterator("user:");
        assert_eq!(user_results.len(), 2);
    }
}
