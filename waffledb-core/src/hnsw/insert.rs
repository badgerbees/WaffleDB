use crate::hnsw::graph::HNSWIndex;
use crate::hnsw::builder::HNSWBuilder;
use std::collections::HashSet;

/// HNSW insertion logic.
pub struct HNSWInserter {
    builder: HNSWBuilder,
}

impl HNSWInserter {
    pub fn new(builder: HNSWBuilder) -> Self {
        HNSWInserter { builder }
    }

    /// Insert a node into the HNSW graph.
    /// Returns the new level of the inserted node.
    pub fn insert(
        &self,
        index: &mut HNSWIndex,
        node_id: usize,
        vector: &[f32],
        get_vector: &dyn Fn(usize) -> Option<Vec<f32>>,
        distance_fn: &dyn Fn(&[f32], &[f32]) -> f32,
    ) -> usize {
        let node_level = self.builder.assign_level();

        // If index is empty, add first node
        if index.entry_point.is_none() {
            index.insert_node(node_id, node_level);
            return node_level;
        }

        // Find nearest neighbors at all levels
        index.insert_node(node_id, node_level);

        let max_level = index.max_layer();
        let mut nearest = index.entry_point.unwrap();

        // Search for nearest at higher levels
        for lc in (node_level + 1..=max_level).rev() {
            if let Some(neighbors) = index.get_neighbors(nearest, lc) {
                let mut best = nearest;
                let mut best_dist = f32::MAX;

                if let Some(best_vec) = get_vector(best) {
                    best_dist = distance_fn(vector, &best_vec);
                }

                for &neighbor in neighbors {
                    if let Some(neighbor_vec) = get_vector(neighbor) {
                        let dist = distance_fn(vector, &neighbor_vec);
                        if dist < best_dist {
                            best_dist = dist;
                            best = neighbor;
                        }
                    }
                }
                nearest = best;
            }
        }

        // Insert at all levels from node_level down to 0
        for lc in (0..=node_level).rev() {
            let candidates = self.search_and_collect(
                index, lc, nearest, vector, get_vector, distance_fn, self.builder.ef_construction,
            );

            // Select M neighbors
            let m = if lc == 0 { self.builder.m * 2 } else { self.builder.m };
            let neighbors = self.select_neighbors(&candidates, m);

            // Add bidirectional connections
            for neighbor_id in neighbors {
                index.add_edge(node_id, neighbor_id, lc);
                index.add_edge(neighbor_id, node_id, lc);

                // Prune neighbors of neighbor if needed
                if let Some(neighbor_neighbors) = index.layers[lc].neighbors(neighbor_id) {
                    let m_max = if lc == 0 { self.builder.m * 2 } else { self.builder.m };
                    if neighbor_neighbors.len() > m_max {
                        // Simple pruning: keep closest neighbors
                    }
                }
            }

            nearest = if !candidates.is_empty() {
                candidates[0].0
            } else {
                nearest
            };
        }

        node_level
    }

    fn search_and_collect(
        &self,
        index: &HNSWIndex,
        level: usize,
        entry_point: usize,
        query: &[f32],
        get_vector: &dyn Fn(usize) -> Option<Vec<f32>>,
        distance_fn: &dyn Fn(&[f32], &[f32]) -> f32,
        ef: usize,
    ) -> Vec<(usize, f32)> {
        let mut visited = HashSet::new();
        let mut candidates: Vec<(usize, f32)> = vec![];
        let mut nearest: Vec<(usize, f32)> = vec![];

        if let Some(ep_vec) = get_vector(entry_point) {
            let dist = distance_fn(query, &ep_vec);
            visited.insert(entry_point);
            candidates.push((entry_point, dist));
            nearest.push((entry_point, dist));
        }

        while !candidates.is_empty() {
            // Find minimum without full sort - O(n) instead of O(n log n)
            let min_idx = candidates
                .iter()
                .enumerate()
                .min_by(|(_, a), (_, b)| a.1.partial_cmp(&b.1).unwrap())
                .map(|(idx, _)| idx)
                .unwrap_or(0);
            
            let (current, current_dist) = candidates.swap_remove(min_idx);

            // Check stopping condition
            if let Some(max_dist) = nearest.iter().map(|(_, d)| *d).max_by(|a, b| a.partial_cmp(b).unwrap()) {
                if current_dist > max_dist {
                    break;
                }
            }

            if let Some(neighbors) = index.get_neighbors(current, level) {
                for &neighbor in neighbors {
                    if !visited.contains(&neighbor) {
                        visited.insert(neighbor);
                        if let Some(neighbor_vec) = get_vector(neighbor) {
                            let dist = distance_fn(query, &neighbor_vec);
                            let max_nearest = nearest.iter().map(|(_, d)| *d).max_by(|a, b| a.partial_cmp(b).unwrap()).unwrap_or(f32::INFINITY);
                            
                            if dist < max_nearest || nearest.len() < ef {
                                candidates.push((neighbor, dist));
                                nearest.push((neighbor, dist));
                                
                                // Keep only top ef results efficiently
                                if nearest.len() > ef {
                                    // Find and remove max
                                    let max_idx = nearest
                                        .iter()
                                        .enumerate()
                                        .max_by(|(_, a), (_, b)| a.1.partial_cmp(&b.1).unwrap())
                                        .map(|(idx, _)| idx)
                                        .unwrap();
                                    nearest.swap_remove(max_idx);
                                }
                            }
                        }
                    }
                }
            }
        }

        nearest
    }

    fn select_neighbors(&self, candidates: &[(usize, f32)], m: usize) -> Vec<usize> {
        candidates.iter().take(m).map(|(id, _)| *id).collect()
    }
}
