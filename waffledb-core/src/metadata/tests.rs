#[cfg(test)]
mod tests {
    use crate::metadata::schema::Metadata;
    use crate::metadata::filter::{FilterOp, FilterBuilder};
    use crate::metadata::index::MetadataIndex;

    #[test]
    fn test_metadata_insert_get() {
        let mut meta = Metadata::new();
        meta.insert("name".to_string(), "vector1".to_string());
        meta.insert("category".to_string(), "image".to_string());
        
        assert_eq!(meta.get("name"), Some("vector1"));
        assert_eq!(meta.get("category"), Some("image"));
    }

    #[test]
    fn test_metadata_remove() {
        let mut meta = Metadata::new();
        meta.insert("key".to_string(), "value".to_string());
        assert_eq!(meta.get("key"), Some("value"));
        
        meta.remove("key");
        assert_eq!(meta.get("key"), None);
    }

    #[test]
    fn test_metadata_merge() {
        let mut meta1 = Metadata::new();
        meta1.insert("a".to_string(), "1".to_string());
        
        let mut meta2 = Metadata::new();
        meta2.insert("b".to_string(), "2".to_string());
        
        meta1.merge(&meta2);
        assert_eq!(meta1.get("a"), Some("1"));
        assert_eq!(meta1.get("b"), Some("2"));
    }

    #[test]
    fn test_metadata_to_json() {
        let mut meta = Metadata::new();
        meta.insert("field1".to_string(), "value1".to_string());
        meta.insert("field2".to_string(), "value2".to_string());
        
        let json = meta.to_json_string();
        assert!(json.contains("field1"));
        assert!(json.contains("value1"));
    }

    #[test]
    fn test_filter_equals() {
        let filter = FilterOp::Equals("type".to_string(), "image".to_string());
        
        let mut meta = Metadata::new();
        meta.insert("type".to_string(), "image".to_string());
        
        assert!(filter.matches(&meta));
    }

    #[test]
    fn test_filter_contains() {
        let filter = FilterOp::Contains("tags".to_string(), "red".to_string());
        
        let mut meta = Metadata::new();
        meta.insert("tags".to_string(), "red,blue,green".to_string());
        
        assert!(filter.matches(&meta));
    }

    #[test]
    fn test_filter_range() {
        let filter = FilterOp::Range("score".to_string(), "05".to_string(), "10".to_string());
        
        let mut meta = Metadata::new();
        meta.insert("score".to_string(), "07".to_string());
        
        assert!(filter.matches(&meta));
    }

    #[test]
    fn test_filter_builder() {
        let filter = FilterBuilder::new()
            .equals("status".to_string(), "active".to_string())
            .build();
        
        let mut meta = Metadata::new();
        meta.insert("status".to_string(), "active".to_string());
        
        assert!(filter.matches(&meta));
    }

    #[test]
    fn test_filter_multiple_conditions() {
        let filter = FilterBuilder::new()
            .equals("type".to_string(), "image".to_string())
            .range("size".to_string(), "1000".to_string(), "5000".to_string())
            .build();
        
        let mut meta = Metadata::new();
        meta.insert("type".to_string(), "image".to_string());
        meta.insert("size".to_string(), "2000".to_string());
        
        assert!(filter.matches(&meta));
    }

    #[test]
    fn test_metadata_index_insert() {
        let mut index = MetadataIndex::new();
        
        let mut meta = Metadata::new();
        meta.insert("category".to_string(), "electronics".to_string());
        
        index.insert("doc1", &meta);
        let results = index.search("category", "electronics");
        
        assert!(results.contains(&"doc1".to_string()));
    }

    #[test]
    fn test_metadata_index_multiple_documents() {
        let mut index = MetadataIndex::new();
        
        let mut meta1 = Metadata::new();
        meta1.insert("type".to_string(), "image".to_string());
        
        let mut meta2 = Metadata::new();
        meta2.insert("type".to_string(), "video".to_string());
        
        let mut meta3 = Metadata::new();
        meta3.insert("type".to_string(), "image".to_string());
        
        index.insert("doc1", &meta1);
        index.insert("doc2", &meta2);
        index.insert("doc3", &meta3);
        
        let image_results = index.search("type", "image");
        assert_eq!(image_results.len(), 2);
        assert!(image_results.contains(&"doc1".to_string()));
        assert!(image_results.contains(&"doc3".to_string()));
    }

    #[test]
    fn test_metadata_index_remove() {
        let mut index = MetadataIndex::new();
        
        let mut meta = Metadata::new();
        meta.insert("status".to_string(), "active".to_string());
        
        index.insert("doc1", &meta);
        let mut found = index.search("status", "active");
        assert_eq!(found.len(), 1);
        
        index.remove("doc1", &meta);
        found = index.search("status", "active");
        assert_eq!(found.len(), 0);
    }
}
