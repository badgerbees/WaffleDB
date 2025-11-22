/// Product Quantization training using k-means.
pub struct PQTrainer {
    pub n_subvectors: usize,
    pub n_centroids: usize,
}

impl PQTrainer {
    /// Create a new PQ trainer.
    pub fn new(n_subvectors: usize, n_centroids: usize) -> Self {
        PQTrainer {
            n_subvectors,
            n_centroids,
        }
    }

    /// Train centroids for each subvector using k-means.
    pub fn train(&self, vectors: &[Vec<f32>]) -> Vec<Vec<Vec<f32>>> {
        if vectors.is_empty() {
            return vec![];
        }

        let dim = vectors[0].len();
        let subvector_dim = (dim + self.n_subvectors - 1) / self.n_subvectors;

        let mut codebooks = vec![];

        for subvector_idx in 0..self.n_subvectors {
            let start = subvector_idx * subvector_dim;
            let end = (start + subvector_dim).min(dim);

            // Extract subvectors
            let mut subvectors = vec![];
            for v in vectors {
                if start < v.len() {
                    subvectors.push(v[start..end.min(v.len())].to_vec());
                }
            }

            // K-means clustering
            let centroids = self.kmeans(&subvectors, self.n_centroids);
            codebooks.push(centroids);
        }

        codebooks
    }

    fn kmeans(&self, vectors: &[Vec<f32>], k: usize) -> Vec<Vec<f32>> {
        if vectors.is_empty() {
            return vec![];
        }

        let dim = vectors[0].len();
        let mut centroids: Vec<Vec<f32>> = vectors.iter().take(k).cloned().collect();

        for _ in 0..10 {
            // Simple k-means: 10 iterations
            let mut assignments = vec![vec![]; k];

            // Assign each vector to nearest centroid
            for v in vectors {
                let mut best_idx = 0;
                let mut best_dist = f32::MAX;

                for (idx, centroid) in centroids.iter().enumerate() {
                    let dist: f32 = v
                        .iter()
                        .zip(centroid.iter())
                        .map(|(x, y)| (x - y).powi(2))
                        .sum();
                    if dist < best_dist {
                        best_dist = dist;
                        best_idx = idx;
                    }
                }

                assignments[best_idx].push(v.clone());
            }

            // Update centroids
            for (idx, cluster) in assignments.iter().enumerate() {
                if !cluster.is_empty() {
                    let mut new_centroid = vec![0.0; dim];
                    for v in cluster {
                        for (i, &val) in v.iter().enumerate() {
                            new_centroid[i] += val;
                        }
                    }
                    for c in &mut new_centroid {
                        *c /= cluster.len() as f32;
                    }
                    centroids[idx] = new_centroid;
                }
            }
        }

        centroids
    }
}
