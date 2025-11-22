use std::collections::HashMap;

/// HNSW graph layer representation.
#[derive(Debug, Clone)]
pub struct Layer {
    /// Adjacency list: node_id -> set of neighbor node_ids
    pub graph: HashMap<usize, Vec<usize>>,
}

impl Layer {
    pub fn new() -> Self {
        Layer {
            graph: HashMap::new(),
        }
    }

    /// Add an edge from node `from` to node `to`.
    pub fn add_edge(&mut self, from: usize, to: usize) {
        self.graph
            .entry(from)
            .or_insert_with(Vec::new)
            .push(to);
    }

    /// Get neighbors of a node.
    pub fn neighbors(&self, node: usize) -> Option<&Vec<usize>> {
        self.graph.get(&node)
    }

    /// Get mutable neighbors of a node.
    pub fn neighbors_mut(&mut self, node: usize) -> &mut Vec<usize> {
        self.graph.entry(node).or_insert_with(Vec::new)
    }

    /// Check if a node exists in the graph.
    pub fn contains_node(&self, node: usize) -> bool {
        self.graph.contains_key(&node)
    }

    /// Get the size of this layer.
    pub fn size(&self) -> usize {
        self.graph.len()
    }
}

/// HNSW index structure.
#[derive(Debug, Clone)]
pub struct HNSWIndex {
    /// Layers: layer_id -> Layer
    pub layers: Vec<Layer>,
    /// Entry point for search
    pub entry_point: Option<usize>,
    /// M: max neighbors per node
    pub m: usize,
    /// M_l: level multiplier
    pub m_l: f32,
}

impl HNSWIndex {
    /// Create a new HNSW index.
    pub fn new(m: usize, m_l: f32) -> Self {
        HNSWIndex {
            layers: vec![Layer::new()],
            entry_point: None,
            m,
            m_l,
        }
    }

    /// Get or create a layer at the given level.
    pub fn get_or_create_layer(&mut self, level: usize) {
        while self.layers.len() <= level {
            self.layers.push(Layer::new());
        }
    }

    /// Insert a node with a given level.
    pub fn insert_node(&mut self, node_id: usize, level: usize) {
        self.get_or_create_layer(level);
        for i in 0..=level {
            self.layers[i].graph.entry(node_id).or_insert_with(Vec::new);
        }
        if self.entry_point.is_none() {
            self.entry_point = Some(node_id);
        }
    }

    /// Add an edge in the graph.
    pub fn add_edge(&mut self, from: usize, to: usize, level: usize) {
        if level < self.layers.len() {
            self.layers[level].add_edge(from, to);
        }
    }

    /// Get neighbors at a specific layer.
    pub fn get_neighbors(&self, node: usize, level: usize) -> Option<&Vec<usize>> {
        if level < self.layers.len() {
            self.layers[level].neighbors(node)
        } else {
            None
        }
    }

    /// Get maximum layer with a node.
    pub fn max_layer(&self) -> usize {
        self.layers.len().saturating_sub(1)
    }
}
