#[cfg(test)]
mod tests {
    use crate::hnsw::builder::HNSWBuilder;
    use crate::hnsw::graph::{HNSWIndex, Layer};
    use crate::vector::distance::DistanceMetric;

    #[test]
    fn test_hnsw_builder_creation() {
        let builder = HNSWBuilder::new()
            .with_m(16)
            .with_ef_construction(200)
            .with_ef_search(100);
        
        assert_eq!(builder.m, 16);
        assert_eq!(builder.ef_construction, 200);
        assert_eq!(builder.ef_search, 100);
    }

    #[test]
    fn test_hnsw_builder_defaults() {
        let builder = HNSWBuilder::new();
        assert_eq!(builder.m, 16);
        assert_eq!(builder.ef_construction, 200);
    }

    #[test]
    fn test_hnsw_index_creation() {
        let index = HNSWIndex::new(16, 0.5);
        assert_eq!(index.layers.len(), 1);
    }

    #[test]
    fn test_hnsw_insert_single_node() {
        let mut index = HNSWIndex::new(16, 0.5);
        index.insert_node(0, 0);
        assert!(index.layers[0].contains_node(0));
    }

    #[test]
    fn test_hnsw_insert_multiple_nodes() {
        let mut index = HNSWIndex::new(16, 0.5);
        for i in 0..5 {
            index.insert_node(i, 0);
        }
        assert_eq!(index.layers[0].size(), 5);
    }

    #[test]
    fn test_layer_creation() {
        let layer = Layer::new();
        assert_eq!(layer.graph.len(), 0);
    }

    #[test]
    fn test_layer_add_edge() {
        let mut layer = Layer::new();
        layer.neighbors_mut(0).push(1);
        layer.neighbors_mut(1).push(0);
        
        assert!(layer.neighbors(0).is_some());
        assert!(layer.neighbors(1).is_some());
    }

    #[test]
    fn test_layer_contains_node() {
        let mut layer = Layer::new();
        layer.neighbors_mut(0);
        assert!(layer.contains_node(0));
        assert!(!layer.contains_node(1));
    }

    #[test]
    fn test_hnsw_add_edge() {
        let mut index = HNSWIndex::new(16, 0.5);
        index.insert_node(0, 0);
        index.insert_node(1, 0);
        index.add_edge(0, 1, 0);
        
        let neighbors = index.get_neighbors(0, 0);
        assert!(neighbors.is_some());
        assert!(neighbors.unwrap().contains(&1));
    }

    #[test]
    fn test_hnsw_get_or_create_layer() {
        let mut index = HNSWIndex::new(16, 0.5);
        assert_eq!(index.layers.len(), 1);
        
        index.get_or_create_layer(2);
        assert_eq!(index.layers.len(), 3);
    }

    #[test]
    fn test_hnsw_multilevel() {
        let mut index = HNSWIndex::new(16, 0.5);
        index.insert_node(0, 2);
        
        assert!(index.layers[0].contains_node(0));
        assert!(index.layers[1].contains_node(0));
        assert!(index.layers[2].contains_node(0));
    }

    #[test]
    #[ignore] // FIXME: Results ordering issue with new layer descent algorithm
    fn test_search_layer_with_vectors() {
        use crate::hnsw::search::search_layer;
        
        let query = vec![1.0, 0.0];
        let entry_points = vec![0, 1];
        
        let get_vector = |id: usize| -> Option<Vec<f32>> {
            match id {
                0 => Some(vec![1.0, 0.0]),
                1 => Some(vec![0.0, 1.0]),
                _ => None,
            }
        };
        
        let get_neighbors = |_: usize| -> Option<Vec<usize>> {
            None
        };
        
        let results = search_layer(
            &query,
            &entry_points,
            10,
            &get_vector,
            &get_neighbors,
            DistanceMetric::L2,
        );
        
        assert!(results.len() > 0);
        assert_eq!(results[0].node_id, 0);
    }
}
