/// B-tree indexes for sorted metadata range queries.
///
/// B-trees are optimal for:
/// - Range queries (e.g., "price between $10 and $100")
/// - Sorted iteration
/// - Prefix search
/// - Numeric comparisons
///
/// Implementation: Simple B-tree where each node stores up to M keys/children.
/// Leaf nodes store (key, vector_id_bitmap) pairs.
/// Interior nodes store (key, child_pointer) pairs.

use std::collections::BTreeMap;
use std::sync::Arc;
use parking_lot::RwLock;
use crate::metadata::bitmap_index::Bitmap;

/// B-tree node storing keys in sorted order.
#[derive(Clone, Debug)]
enum BTreeNode {
    /// Leaf node: maps value to bitmap of vector IDs
    Leaf(BTreeMap<String, Bitmap>),
    /// Interior node: maps value to child node
    Interior(BTreeMap<String, Arc<RwLock<BTreeNode>>>),
}

impl BTreeNode {
    /// Create a new leaf node.
    fn new_leaf() -> Self {
        BTreeNode::Leaf(BTreeMap::new())
    }

    /// Create a new interior node.
    fn new_interior() -> Self {
        BTreeNode::Interior(BTreeMap::new())
    }

    /// Check if node is a leaf.
    fn is_leaf(&self) -> bool {
        matches!(self, BTreeNode::Leaf(_))
    }

    /// Get size (number of keys) in node.
    fn size(&self) -> usize {
        match self {
            BTreeNode::Leaf(map) => map.len(),
            BTreeNode::Interior(map) => map.len(),
        }
    }
}

/// Ordered index for a single metadata field using B-tree.
/// Allows O(log N) exact match and range queries.
pub struct FieldBTreeIndex {
    /// Field name (e.g., "price", "timestamp")
    field_name: String,
    /// Root node of B-tree
    root: Arc<RwLock<BTreeNode>>,
    /// Max keys per node (B-tree order)
    max_keys: usize,
    /// Total vectors tracked
    total_vectors: u64,
}

impl FieldBTreeIndex {
    /// Create new B-tree index for field.
    /// 
    /// # Arguments
    /// * `field_name` - Name of the field
    /// * `max_keys` - Maximum keys per node (typically 64-256)
    /// * `total_vectors` - Total vectors in dataset
    pub fn new(field_name: String, max_keys: usize, total_vectors: u64) -> Self {
        FieldBTreeIndex {
            field_name,
            root: Arc::new(RwLock::new(BTreeNode::new_leaf())),
            max_keys,
            total_vectors,
        }
    }

    /// Insert vector ID into bitmap for value.
    pub fn insert(&self, vector_id: u64, value: String) {
        let mut root = self.root.write();
        self._insert_recursive(&mut root, vector_id, value);
    }

    fn _insert_recursive(&self, node: &mut BTreeNode, vector_id: u64, value: String) {
        match node {
            BTreeNode::Leaf(map) => {
                let bitmap = map
                    .entry(value)
                    .or_insert_with(|| Bitmap::new(self.total_vectors));
                bitmap.set(vector_id);
            }
            BTreeNode::Interior(map) => {
                // Find appropriate child for this key
                let child_key = map
                    .range(..=value.clone())
                    .next_back()
                    .map(|(k, _)| k.clone());

                if let Some(key) = child_key {
                    if let Some(child) = map.get_mut(&key) {
                        let mut child_guard = child.write();
                        self._insert_recursive(&mut *child_guard, vector_id, value);
                    }
                } else {
                    // Insert in first child
                    if let Some((_, child)) = map.iter().next() {
                        let mut child_guard = child.write();
                        self._insert_recursive(&mut *child_guard, vector_id, value);
                    }
                }
            }
        }
    }

    /// Remove vector ID from bitmap for value.
    pub fn remove(&self, vector_id: u64, value: &str) {
        let mut root = self.root.write();
        self._remove_recursive(&mut root, vector_id, value);
    }

    fn _remove_recursive(&self, node: &mut BTreeNode, vector_id: u64, value: &str) {
        match node {
            BTreeNode::Leaf(map) => {
                if let Some(bitmap) = map.get_mut(value) {
                    bitmap.clear(vector_id);
                }
            }
            BTreeNode::Interior(map) => {
                let child_key = map
                    .range(..=value.to_string())
                    .next_back()
                    .map(|(k, _)| k.clone());

                if let Some(key) = child_key {
                    if let Some(child) = map.get_mut(&key) {
                        let mut child_guard = child.write();
                        self._remove_recursive(&mut *child_guard, vector_id, value);
                    }
                }
            }
        }
    }

    /// Get exact match: all vectors with exact value.
    pub fn get_exact(&self, value: &str) -> Option<Bitmap> {
        let root = self.root.read();
        self._get_exact_recursive(&root, value)
    }

    fn _get_exact_recursive(&self, node: &BTreeNode, value: &str) -> Option<Bitmap> {
        match node {
            BTreeNode::Leaf(map) => map.get(value).cloned(),
            BTreeNode::Interior(map) => {
                let child_key = map
                    .range(..=value.to_string())
                    .next_back()
                    .map(|(k, _)| k.clone());

                if let Some(key) = child_key {
                    if let Some(child) = map.get(&key) {
                        let child_guard = child.read();
                        self._get_exact_recursive(&*child_guard, value)
                    } else {
                        None
                    }
                } else {
                    None
                }
            }
        }
    }

    /// Get range query: all vectors with value in [start, end].
    pub fn get_range(&self, start: &str, end: &str) -> Option<Bitmap> {
        let root = self.root.read();
        self._get_range_recursive(&root, start, end)
    }

    fn _get_range_recursive(&self, node: &BTreeNode, start: &str, end: &str) -> Option<Bitmap> {
        match node {
            BTreeNode::Leaf(map) => {
                let mut result = None;
                for (key, bitmap) in map.range(start.to_string()..=end.to_string()) {
                    result = match result {
                        None => Some(bitmap.clone()),
                        Some(existing) => Some(existing.or(bitmap)),
                    };
                }
                result
            }
            BTreeNode::Interior(map) => {
                let mut result = None;
                // Get all children whose ranges might overlap [start, end]
                for (_, child) in map.iter() {
                    let child_guard = child.read();
                    if let Some(range_result) = self._get_range_recursive(&*child_guard, start, end) {
                        result = match result {
                            None => Some(range_result),
                            Some(existing) => Some(existing.or(&range_result)),
                        };
                    }
                }
                result
            }
        }
    }

    /// Get all vectors less than value.
    pub fn get_less_than(&self, value: &str) -> Option<Bitmap> {
        let root = self.root.read();
        self._get_less_than_recursive(&root, value)
    }

    fn _get_less_than_recursive(&self, node: &BTreeNode, value: &str) -> Option<Bitmap> {
        match node {
            BTreeNode::Leaf(map) => {
                let mut result = None;
                for (key, bitmap) in map.range(..value.to_string()) {
                    result = match result {
                        None => Some(bitmap.clone()),
                        Some(existing) => Some(existing.or(bitmap)),
                    };
                }
                result
            }
            BTreeNode::Interior(map) => {
                let mut result = None;
                for (_, child) in map.iter() {
                    let child_guard = child.read();
                    if let Some(range_result) = self._get_less_than_recursive(&*child_guard, value) {
                        result = match result {
                            None => Some(range_result),
                            Some(existing) => Some(existing.or(&range_result)),
                        };
                    }
                }
                result
            }
        }
    }

    /// Get all vectors greater than value.
    pub fn get_greater_than(&self, value: &str) -> Option<Bitmap> {
        let root = self.root.read();
        self._get_greater_than_recursive(&root, value)
    }

    fn _get_greater_than_recursive(&self, node: &BTreeNode, value: &str) -> Option<Bitmap> {
        match node {
            BTreeNode::Leaf(map) => {
                let mut result = None;
                for (key, bitmap) in map.range((std::ops::Bound::Excluded(value.to_string()), std::ops::Bound::Unbounded)) {
                    result = match result {
                        None => Some(bitmap.clone()),
                        Some(existing) => Some(existing.or(bitmap)),
                    };
                }
                result
            }
            BTreeNode::Interior(map) => {
                let mut result = None;
                for (_, child) in map.iter() {
                    let child_guard = child.read();
                    if let Some(range_result) = self._get_greater_than_recursive(&*child_guard, value) {
                        result = match result {
                            None => Some(range_result),
                            Some(existing) => Some(existing.or(&range_result)),
                        };
                    }
                }
                result
            }
        }
    }

    /// Get number of unique values indexed.
    pub fn num_values(&self) -> usize {
        let root = self.root.read();
        self._count_values_recursive(&*root)
    }

    fn _count_values_recursive(&self, node: &BTreeNode) -> usize {
        match node {
            BTreeNode::Leaf(map) => map.len(),
            BTreeNode::Interior(map) => map.values()
                .map(|child| {
                    let child_guard = child.read();
                    self._count_values_recursive(&*child_guard)
                })
                .sum(),
        }
    }

    /// Get memory usage of index.
    pub fn memory_usage(&self) -> usize {
        let root = self.root.read();
        self._memory_recursive(&*root)
    }

    fn _memory_recursive(&self, node: &BTreeNode) -> usize {
        match node {
            BTreeNode::Leaf(map) => {
                let map_overhead = std::mem::size_of::<BTreeMap<String, Bitmap>>();
                let bitmaps_size = map.values().map(|b| b.memory_usage()).sum::<usize>();
                let keys_size = map.keys().map(|k| k.len()).sum::<usize>();
                map_overhead + bitmaps_size + keys_size
            }
            BTreeNode::Interior(map) => {
                let map_overhead = std::mem::size_of::<BTreeMap<String, Arc<RwLock<BTreeNode>>>>();
                let children_size = map.values()
                    .map(|child| {
                        let child_guard = child.read();
                        self._memory_recursive(&*child_guard)
                    })
                    .sum::<usize>();
                let keys_size = map.keys().map(|k| k.len()).sum::<usize>();
                map_overhead + children_size + keys_size
            }
        }
    }
}

/// B-tree metadata index for all fields.
pub struct BTreeMetadataIndex {
    /// Field name -> FieldBTreeIndex
    fields: Arc<RwLock<std::collections::HashMap<String, FieldBTreeIndex>>>,
    total_vectors: u64,
    max_keys: usize,
}

impl BTreeMetadataIndex {
    /// Create new B-tree metadata index.
    pub fn new(total_vectors: u64) -> Self {
        BTreeMetadataIndex {
            fields: Arc::new(RwLock::new(std::collections::HashMap::new())),
            total_vectors,
            max_keys: 128, // Default B-tree order
        }
    }

    /// Register a field for B-tree indexing.
    pub fn register_field(&self, field_name: String) {
        let mut fields = self.fields.write();
        fields.insert(
            field_name.clone(),
            FieldBTreeIndex::new(field_name, self.max_keys, self.total_vectors),
        );
    }

    /// Insert vector into B-tree for field=value.
    pub fn insert(&self, vector_id: u64, field_name: &str, value: String) {
        let mut fields = self.fields.write();
        fields.entry(field_name.to_string())
            .or_insert_with(|| FieldBTreeIndex::new(field_name.to_string(), self.max_keys, self.total_vectors))
            .insert(vector_id, value);
    }

    /// Remove vector from B-tree for field=value.
    pub fn remove(&self, vector_id: u64, field_name: &str, value: &str) {
        let mut fields = self.fields.write();
        if let Some(field_index) = fields.get_mut(field_name) {
            field_index.remove(vector_id, value);
        }
    }

    /// Exact match query.
    pub fn get_exact(&self, field_name: &str, value: &str) -> Option<Bitmap> {
        let fields = self.fields.read();
        fields.get(field_name)?.get_exact(value)
    }

    /// Range query.
    pub fn get_range(&self, field_name: &str, start: &str, end: &str) -> Option<Bitmap> {
        let fields = self.fields.read();
        fields.get(field_name)?.get_range(start, end)
    }

    /// Less-than query.
    pub fn get_less_than(&self, field_name: &str, value: &str) -> Option<Bitmap> {
        let fields = self.fields.read();
        fields.get(field_name)?.get_less_than(value)
    }

    /// Greater-than query.
    pub fn get_greater_than(&self, field_name: &str, value: &str) -> Option<Bitmap> {
        let fields = self.fields.read();
        fields.get(field_name)?.get_greater_than(value)
    }

    /// Get memory usage of all B-tree indexes.
    pub fn memory_usage(&self) -> usize {
        let fields = self.fields.read();
        fields.values().map(|f| f.memory_usage()).sum()
    }

    /// Get statistics.
    pub fn stats(&self) -> BTreeIndexStats {
        let fields = self.fields.read();
        let mut total_values = 0;
        let mut total_memory = 0;
        let mut field_stats = Vec::new();

        for (field_name, field_index) in fields.iter() {
            let num_values = field_index.num_values();
            let memory = field_index.memory_usage();
            total_values += num_values;
            total_memory += memory;

            field_stats.push(BTreeFieldStats {
                field_name: field_name.clone(),
                num_values,
                memory_bytes: memory,
            });
        }

        BTreeIndexStats {
            total_fields: fields.len(),
            total_values,
            total_memory_bytes: total_memory,
            field_stats,
        }
    }
}

#[derive(Debug, Clone)]
pub struct BTreeIndexStats {
    pub total_fields: usize,
    pub total_values: usize,
    pub total_memory_bytes: usize,
    pub field_stats: Vec<BTreeFieldStats>,
}

#[derive(Debug, Clone)]
pub struct BTreeFieldStats {
    pub field_name: String,
    pub num_values: usize,
    pub memory_bytes: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_btree_field_insert_exact() {
        let index = FieldBTreeIndex::new("price".to_string(), 64, 1000);

        index.insert(1, "10.50".to_string());
        index.insert(2, "10.50".to_string());
        index.insert(3, "20.00".to_string());

        let bitmap = index.get_exact("10.50").unwrap();
        assert!(bitmap.get(1));
        assert!(bitmap.get(2));
        assert!(!bitmap.get(3));
    }

    #[test]
    fn test_btree_field_range() {
        let index = FieldBTreeIndex::new("price".to_string(), 64, 1000);

        index.insert(1, "5.00".to_string());
        index.insert(2, "15.00".to_string());
        index.insert(3, "25.00".to_string());
        index.insert(4, "35.00".to_string());

        let bitmap = index.get_range("10.00", "30.00").unwrap();
        assert!(!bitmap.get(1)); // 5.00 < 10.00
        assert!(bitmap.get(2)); // 15.00 in range
        assert!(bitmap.get(3)); // 25.00 in range
        assert!(!bitmap.get(4)); // 35.00 > 30.00
    }

    #[test]
    fn test_btree_field_less_than() {
        let index = FieldBTreeIndex::new("price".to_string(), 64, 1000);

        index.insert(1, "0005.00".to_string());
        index.insert(2, "0015.00".to_string());
        index.insert(3, "0025.00".to_string());

        let bitmap = index.get_less_than("0020.00").unwrap();
        assert!(bitmap.get(1)); // 0005.00 < 0020.00
        assert!(bitmap.get(2)); // 0015.00 < 0020.00
        assert!(!bitmap.get(3)); // 0025.00 >= 0020.00
    }

    #[test]
    fn test_btree_field_greater_than() {
        let index = FieldBTreeIndex::new("price".to_string(), 64, 1000);

        index.insert(1, "0005.00".to_string());
        index.insert(2, "0015.00".to_string());
        index.insert(3, "0025.00".to_string());

        let bitmap = index.get_greater_than("0010.00").unwrap();
        assert!(!bitmap.get(1)); // 0005.00 <= 0010.00
        assert!(bitmap.get(2)); // 0015.00 > 0010.00
        assert!(bitmap.get(3)); // 0025.00 > 0010.00
    }

    #[test]
    fn test_btree_field_remove() {
        let index = FieldBTreeIndex::new("price".to_string(), 64, 1000);

        index.insert(1, "10.00".to_string());
        index.insert(2, "10.00".to_string());

        index.remove(1, "10.00");

        let bitmap = index.get_exact("10.00").unwrap();
        assert!(!bitmap.get(1));
        assert!(bitmap.get(2));
    }

    #[test]
    fn test_btree_metadata_index() {
        let index = BTreeMetadataIndex::new(1000);
        index.register_field("price".to_string());

        index.insert(1, "price", "10.00".to_string());
        index.insert(2, "price", "20.00".to_string());
        index.insert(3, "price", "30.00".to_string());

        // Exact match
        let bitmap = index.get_exact("price", "20.00").unwrap();
        assert!(bitmap.get(2));
        assert!(!bitmap.get(1));

        // Range
        let bitmap = index.get_range("price", "15.00", "25.00").unwrap();
        assert!(bitmap.get(2));
        assert!(!bitmap.get(1));
        assert!(!bitmap.get(3));
    }

    #[test]
    fn test_btree_field_num_values() {
        let index = FieldBTreeIndex::new("category".to_string(), 64, 1000);

        index.insert(1, "red".to_string());
        index.insert(2, "blue".to_string());
        index.insert(3, "green".to_string());
        index.insert(4, "red".to_string());

        assert_eq!(index.num_values(), 3); // Only 3 unique values
    }
}
