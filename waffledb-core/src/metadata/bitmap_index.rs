/// Bitmap indexes for fast categorical metadata filtering.
/// 
/// Instead of storing individual vector IDs for each value, we use bit vectors
/// to represent membership. This is 8-64× more space-efficient for large datasets.
///
/// Example: 100M vectors with 10 categories
/// - Traditional: 10 × Vec<String> × 20 bytes/string = 20GB
/// - Bitmap: 10 × Bitmap(100M bits) = 125MB
///
/// Tradeoff: Bitmaps are slower for small datasets (<1M vectors) but faster
/// for large datasets due to better cache locality and batch operations.

use std::collections::HashMap;
use std::sync::Arc;
use parking_lot::RwLock;

/// A 64-bit word for bitmap operations.
pub type Word = u64;
const WORD_SIZE: usize = 64;
const WORD_BITS: usize = 64;

/// Bitmap representation of which vectors have a specific metadata value.
#[derive(Clone, Debug)]
pub struct Bitmap {
    /// Vector of 64-bit words, one bit per vector ID.
    /// Vector ID N is represented by bit (N % 64) in word (N / 64).
    words: Vec<Word>,
    /// Total number of vectors tracked (determines bitmap size).
    total_vectors: u64,
}

impl Bitmap {
    /// Create a new bitmap for `total_vectors`.
    pub fn new(total_vectors: u64) -> Self {
        let num_words = ((total_vectors + WORD_BITS as u64 - 1) / WORD_BITS as u64) as usize;
        Bitmap {
            words: vec![0; num_words],
            total_vectors,
        }
    }

    /// Set bit for vector ID.
    pub fn set(&mut self, vector_id: u64) {
        if vector_id >= self.total_vectors {
            return; // Out of bounds, ignore
        }
        let word_idx = (vector_id / WORD_SIZE as u64) as usize;
        let bit_idx = (vector_id % WORD_SIZE as u64) as usize;
        self.words[word_idx] |= 1 << bit_idx;
    }

    /// Clear bit for vector ID.
    pub fn clear(&mut self, vector_id: u64) {
        if vector_id >= self.total_vectors {
            return;
        }
        let word_idx = (vector_id / WORD_SIZE as u64) as usize;
        let bit_idx = (vector_id % WORD_SIZE as u64) as usize;
        self.words[word_idx] &= !(1 << bit_idx);
    }

    /// Check if bit is set for vector ID.
    pub fn get(&self, vector_id: u64) -> bool {
        if vector_id >= self.total_vectors {
            return false;
        }
        let word_idx = (vector_id / WORD_SIZE as u64) as usize;
        let bit_idx = (vector_id % WORD_SIZE as u64) as usize;
        (self.words[word_idx] & (1 << bit_idx)) != 0
    }

    /// Count number of set bits (cardinality).
    pub fn count_set_bits(&self) -> u64 {
        self.words.iter().map(|w| w.count_ones() as u64).sum()
    }

    /// Bitwise AND with another bitmap (intersection).
    pub fn and(&self, other: &Bitmap) -> Bitmap {
        let num_words = self.words.len().min(other.words.len());
        let words = (0..num_words)
            .map(|i| self.words[i] & other.words[i])
            .collect();
        Bitmap {
            words,
            total_vectors: self.total_vectors.min(other.total_vectors),
        }
    }

    /// Bitwise OR with another bitmap (union).
    pub fn or(&self, other: &Bitmap) -> Bitmap {
        let num_words = self.words.len().max(other.words.len());
        let mut words = vec![0; num_words];
        for i in 0..self.words.len() {
            words[i] |= self.words[i];
        }
        for i in 0..other.words.len() {
            words[i] |= other.words[i];
        }
        Bitmap {
            words,
            total_vectors: self.total_vectors.max(other.total_vectors),
        }
    }

    /// Bitwise NOT (complement).
    pub fn not(&self) -> Bitmap {
        let words = self.words.iter().map(|w| !w).collect();
        Bitmap {
            words,
            total_vectors: self.total_vectors,
        }
    }

    /// Get all vector IDs where bit is set.
    pub fn iter_set_ids(&self) -> impl Iterator<Item = u64> + '_ {
        self.words
            .iter()
            .enumerate()
            .flat_map(|(word_idx, word)| {
                (0..WORD_SIZE).filter_map(move |bit_idx| {
                    if (word & (1 << bit_idx)) != 0 {
                        Some(word_idx as u64 * WORD_SIZE as u64 + bit_idx as u64)
                    } else {
                        None
                    }
                })
            })
    }

    /// Memory usage in bytes.
    pub fn memory_usage(&self) -> usize {
        self.words.len() * 8
    }
}

/// A bitmap index for a single metadata field (e.g., "category").
/// Maps each unique value to a bitmap of vector IDs.
#[derive(Clone, Debug)]
pub struct FieldBitmapIndex {
    /// Field name (e.g., "category", "region")
    field_name: String,
    /// Value -> Bitmap of vector IDs
    bitmaps: HashMap<String, Bitmap>,
    /// Total vectors tracked
    total_vectors: u64,
}

impl FieldBitmapIndex {
    /// Create new bitmap index for field with max vectors.
    pub fn new(field_name: String, total_vectors: u64) -> Self {
        FieldBitmapIndex {
            field_name,
            bitmaps: HashMap::new(),
            total_vectors,
        }
    }

    /// Add vector to bitmap for value.
    pub fn insert(&mut self, vector_id: u64, value: String) {
        let bitmap = self
            .bitmaps
            .entry(value)
            .or_insert_with(|| Bitmap::new(self.total_vectors));
        bitmap.set(vector_id);
    }

    /// Remove vector from bitmap for value.
    pub fn remove(&mut self, vector_id: u64, value: &str) {
        if let Some(bitmap) = self.bitmaps.get_mut(value) {
            bitmap.clear(vector_id);
        }
    }

    /// Get all vector IDs matching value (exact match).
    pub fn get(&self, value: &str) -> Option<Bitmap> {
        self.bitmaps.get(value).cloned()
    }

    /// Get all vector IDs NOT matching value.
    pub fn get_not(&self, value: &str) -> Option<Bitmap> {
        self.bitmaps.get(value).map(|bitmap| bitmap.not())
    }

    /// Get vectors matching any of the values (OR).
    pub fn get_any(&self, values: &[&str]) -> Option<Bitmap> {
        if values.is_empty() {
            return None;
        }

        let mut result = None;
        for value in values {
            if let Some(bitmap) = self.bitmaps.get(*value) {
                result = match result {
                    None => Some(bitmap.clone()),
                    Some(existing) => Some(existing.or(bitmap)),
                };
            }
        }
        result
    }

    /// Get vectors matching all values (AND).
    pub fn get_all(&self, values: &[&str]) -> Option<Bitmap> {
        if values.is_empty() {
            return None;
        }

        let mut result = None;
        for value in values {
            if let Some(bitmap) = self.bitmaps.get(*value) {
                result = match result {
                    None => Some(bitmap.clone()),
                    Some(existing) => Some(existing.and(bitmap)),
                };
            } else {
                // If any value is missing, result is empty
                return Some(Bitmap::new(self.total_vectors));
            }
        }
        result
    }

    /// Get memory usage of all bitmaps for this field.
    pub fn memory_usage(&self) -> usize {
        self.bitmaps.values().map(|b| b.memory_usage()).sum()
    }

    /// Get number of distinct values indexed.
    pub fn num_values(&self) -> usize {
        self.bitmaps.len()
    }
}

/// Complete bitmap index for all metadata fields.
pub struct BitmapMetadataIndex {
    /// Field name -> FieldBitmapIndex
    fields: Arc<RwLock<HashMap<String, FieldBitmapIndex>>>,
    /// Total vectors tracked
    total_vectors: u64,
}

impl BitmapMetadataIndex {
    /// Create new metadata bitmap index.
    pub fn new(total_vectors: u64) -> Self {
        BitmapMetadataIndex {
            fields: Arc::new(RwLock::new(HashMap::new())),
            total_vectors,
        }
    }

    /// Register a new field for indexing.
    pub fn register_field(&self, field_name: String) {
        let mut fields = self.fields.write();
        fields.insert(field_name.clone(), FieldBitmapIndex::new(field_name, self.total_vectors));
    }

    /// Add vector to bitmap for field=value.
    pub fn insert(&self, vector_id: u64, field_name: &str, value: String) {
        let mut fields = self.fields.write();
        let field_index = fields
            .entry(field_name.to_string())
            .or_insert_with(|| FieldBitmapIndex::new(field_name.to_string(), self.total_vectors));
        field_index.insert(vector_id, value);
    }

    /// Remove vector from bitmap for field=value.
    pub fn remove(&self, vector_id: u64, field_name: &str, value: &str) {
        let mut fields = self.fields.write();
        if let Some(field_index) = fields.get_mut(field_name) {
            field_index.remove(vector_id, value);
        }
    }

    /// Get vectors matching field=value (exact).
    pub fn get_exact(&self, field_name: &str, value: &str) -> Option<Bitmap> {
        let fields = self.fields.read();
        fields.get(field_name)?.get(value)
    }

    /// Get vectors NOT matching field=value.
    pub fn get_not_exact(&self, field_name: &str, value: &str) -> Option<Bitmap> {
        let fields = self.fields.read();
        fields.get(field_name)?.get_not(value)
    }

    /// Complex query: (field1=value1 OR field1=value2) AND (field2=value3)
    /// represented as a list of (field, values, is_and)
    pub fn query(&self, conditions: Vec<(String, Vec<String>, bool)>) -> Option<Bitmap> {
        let fields = self.fields.read();

        let mut result = None;

        for (field_name, values, is_and) in conditions {
            if let Some(field_index) = fields.get(&field_name) {
                let field_result = if is_and {
                    let values_refs: Vec<&str> = values.iter().map(|v| v.as_str()).collect();
                    field_index.get_all(&values_refs)?
                } else {
                    let values_refs: Vec<&str> = values.iter().map(|v| v.as_str()).collect();
                    field_index.get_any(&values_refs)?
                };

                result = match result {
                    None => Some(field_result),
                    Some(existing) => Some(existing.and(&field_result)),
                };
            }
        }

        result
    }

    /// Get memory usage of all field indexes.
    pub fn memory_usage(&self) -> usize {
        let fields = self.fields.read();
        fields.values().map(|f| f.memory_usage()).sum()
    }

    /// Get statistics about index.
    pub fn stats(&self) -> BitmapIndexStats {
        let fields = self.fields.read();
        let mut total_values = 0;
        let mut total_memory = 0;
        let mut field_stats = Vec::new();

        for (field_name, field_index) in fields.iter() {
            let num_values = field_index.num_values();
            let memory = field_index.memory_usage();
            total_values += num_values;
            total_memory += memory;

            field_stats.push(FieldIndexStats {
                field_name: field_name.clone(),
                num_values,
                memory_bytes: memory,
            });
        }

        BitmapIndexStats {
            total_fields: fields.len(),
            total_values,
            total_memory_bytes: total_memory,
            field_stats,
        }
    }
}

/// Statistics about bitmap index.
#[derive(Debug, Clone)]
pub struct BitmapIndexStats {
    pub total_fields: usize,
    pub total_values: usize,
    pub total_memory_bytes: usize,
    pub field_stats: Vec<FieldIndexStats>,
}

#[derive(Debug, Clone)]
pub struct FieldIndexStats {
    pub field_name: String,
    pub num_values: usize,
    pub memory_bytes: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bitmap_set_get() {
        let mut bitmap = Bitmap::new(100);
        bitmap.set(5);
        bitmap.set(50);

        assert!(bitmap.get(5));
        assert!(bitmap.get(50));
        assert!(!bitmap.get(6));
        assert!(!bitmap.get(49));
    }

    #[test]
    fn test_bitmap_clear() {
        let mut bitmap = Bitmap::new(100);
        bitmap.set(10);
        assert!(bitmap.get(10));

        bitmap.clear(10);
        assert!(!bitmap.get(10));
    }

    #[test]
    fn test_bitmap_count_set_bits() {
        let mut bitmap = Bitmap::new(100);
        bitmap.set(5);
        bitmap.set(10);
        bitmap.set(50);

        assert_eq!(bitmap.count_set_bits(), 3);
    }

    #[test]
    fn test_bitmap_and_intersection() {
        let mut bitmap1 = Bitmap::new(100);
        let mut bitmap2 = Bitmap::new(100);

        bitmap1.set(5);
        bitmap1.set(10);
        bitmap1.set(50);

        bitmap2.set(5);
        bitmap2.set(15);
        bitmap2.set(50);

        let result = bitmap1.and(&bitmap2);
        assert!(result.get(5)); // in both
        assert!(result.get(50)); // in both
        assert!(!result.get(10)); // only in bitmap1
        assert!(!result.get(15)); // only in bitmap2

        assert_eq!(result.count_set_bits(), 2);
    }

    #[test]
    fn test_bitmap_or_union() {
        let mut bitmap1 = Bitmap::new(100);
        let mut bitmap2 = Bitmap::new(100);

        bitmap1.set(5);
        bitmap1.set(10);

        bitmap2.set(15);
        bitmap2.set(50);

        let result = bitmap1.or(&bitmap2);
        assert!(result.get(5));
        assert!(result.get(10));
        assert!(result.get(15));
        assert!(result.get(50));
        assert_eq!(result.count_set_bits(), 4);
    }

    #[test]
    fn test_bitmap_not_complement() {
        let mut bitmap = Bitmap::new(10);
        bitmap.set(0);
        bitmap.set(5);

        let result = bitmap.not();
        assert!(!result.get(0));
        assert!(!result.get(5));
        assert!(result.get(1));
        assert!(result.get(9));
    }

    #[test]
    fn test_bitmap_iter_set_ids() {
        let mut bitmap = Bitmap::new(100);
        bitmap.set(5);
        bitmap.set(50);
        bitmap.set(99);

        let ids: Vec<u64> = bitmap.iter_set_ids().collect();
        assert_eq!(ids, vec![5, 50, 99]);
    }

    #[test]
    fn test_field_bitmap_index_insert_get() {
        let mut index = FieldBitmapIndex::new("category".to_string(), 100);

        index.insert(1, "red".to_string());
        index.insert(2, "red".to_string());
        index.insert(3, "blue".to_string());

        let red_bitmap = index.get("red").unwrap();
        assert!(red_bitmap.get(1));
        assert!(red_bitmap.get(2));
        assert!(!red_bitmap.get(3));
    }

    #[test]
    fn test_field_bitmap_index_get_any() {
        let mut index = FieldBitmapIndex::new("category".to_string(), 100);

        index.insert(1, "red".to_string());
        index.insert(2, "blue".to_string());
        index.insert(3, "green".to_string());

        let result = index.get_any(&["red", "blue"]).unwrap();
        assert!(result.get(1));
        assert!(result.get(2));
        assert!(!result.get(3));
        assert_eq!(result.count_set_bits(), 2);
    }

    #[test]
    fn test_field_bitmap_index_get_all() {
        let mut index1 = FieldBitmapIndex::new("color".to_string(), 100);
        let mut index2 = FieldBitmapIndex::new("size".to_string(), 100);

        index1.insert(1, "red".to_string());
        index1.insert(2, "red".to_string());
        index1.insert(3, "blue".to_string());

        index2.insert(1, "large".to_string());
        index2.insert(2, "small".to_string());

        // AND operation (should intersect)
        let red_bitmap = index1.get("red").unwrap();
        let large_bitmap = index2.get("large").unwrap();
        let result = red_bitmap.and(&large_bitmap);

        assert!(result.get(1)); // red AND large
        assert!(!result.get(2)); // red but not large
        assert_eq!(result.count_set_bits(), 1);
    }

    #[test]
    fn test_bitmap_metadata_index_insert_get() {
        let index = BitmapMetadataIndex::new(1000);
        index.register_field("category".to_string());

        index.insert(1, "category", "red".to_string());
        index.insert(2, "category", "red".to_string());
        index.insert(3, "category", "blue".to_string());

        let red_vectors = index.get_exact("category", "red").unwrap();
        assert!(red_vectors.get(1));
        assert!(red_vectors.get(2));
        assert!(!red_vectors.get(3));
    }

    #[test]
    fn test_bitmap_metadata_index_complex_query() {
        let index = BitmapMetadataIndex::new(1000);
        index.register_field("color".to_string());
        index.register_field("size".to_string());

        // Insert test data
        index.insert(1, "color", "red".to_string());
        index.insert(1, "size", "large".to_string());

        index.insert(2, "color", "red".to_string());
        index.insert(2, "size", "small".to_string());

        index.insert(3, "color", "blue".to_string());
        index.insert(3, "size", "large".to_string());

        // Query: color=red AND size=large
        let conditions = vec![
            ("color".to_string(), vec!["red".to_string()], true),
            ("size".to_string(), vec!["large".to_string()], true),
        ];
        let result = index.query(conditions).unwrap();

        assert!(result.get(1)); // red and large
        assert!(!result.get(2)); // red but small
        assert!(!result.get(3)); // blue, not red
    }

    #[test]
    fn test_bitmap_index_stats() {
        let index = BitmapMetadataIndex::new(1000);
        index.register_field("color".to_string());
        index.register_field("size".to_string());

        index.insert(1, "color", "red".to_string());
        index.insert(2, "color", "blue".to_string());
        index.insert(1, "size", "large".to_string());
        index.insert(2, "size", "small".to_string());

        let stats = index.stats();
        assert_eq!(stats.total_fields, 2);
        assert_eq!(stats.total_values, 4); // red, blue, large, small

        // Each bitmap is ~125 bytes for 1000 vectors
        assert!(stats.total_memory_bytes > 0);
    }

    #[test]
    fn test_bitmap_memory_efficiency() {
        // Compare memory usage: bitmap vs Vec<String>
        let num_vectors = 100_000;
        let num_values = 100;

        // Bitmap approach
        let mut bitmap = Bitmap::new(num_vectors as u64);
        for i in 0..num_vectors {
            bitmap.set(i as u64);
        }
        let bitmap_memory = bitmap.memory_usage();

        // Estimate Vec<String> approach
        // Each string ID is ~20 bytes on average
        let vec_memory_estimate = num_vectors * 20;

        // Bitmap should use ~8× less memory
        assert!(bitmap_memory < vec_memory_estimate / 8);
    }

    #[test]
    fn test_bitmap_large_dataset_simulation() {
        // Simulate 100M vectors with 10 categories
        let num_vectors = 100_000_000u64;
        let mut index = BitmapMetadataIndex::new(num_vectors);
        index.register_field("category".to_string());

        // Insert some sample vectors (we won't insert all 100M, just test structure)
        for i in 0..1000 {
            let category = format!("cat_{}", i % 10);
            index.insert(i, "category", category);
        }

        let stats = index.stats();
        assert_eq!(stats.total_fields, 1);
        assert_eq!(stats.total_values, 10); // 10 categories

        // Each bitmap for 100M vectors: ~12.5MB
        // Total: 10 × 12.5MB = 125MB (vs 20GB with Vec<String>)
        let expected_memory_per_field = (num_vectors / 8) as usize; // bits → bytes
        assert!(stats.total_memory_bytes > 0);
    }
}
