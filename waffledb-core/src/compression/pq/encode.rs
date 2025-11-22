/// Product Quantization encoder.
pub struct PQEncoder {
    pub codebooks: Vec<Vec<Vec<f32>>>,
    pub subvector_dim: usize,
}

impl PQEncoder {
    /// Create a new PQ encoder with trained codebooks.
    pub fn new(codebooks: Vec<Vec<Vec<f32>>>) -> Self {
        let subvector_dim = if codebooks.is_empty() {
            0
        } else {
            codebooks[0][0].len()
        };

        PQEncoder {
            codebooks,
            subvector_dim,
        }
    }

    /// Encode a vector to PQ codes.
    pub fn encode(&self, vector: &[f32]) -> Vec<u8> {
        let mut codes = vec![];

        for (subvector_idx, codebook) in self.codebooks.iter().enumerate() {
            let start = subvector_idx * self.subvector_dim;
            let end = (start + self.subvector_dim).min(vector.len());

            if start >= vector.len() {
                break;
            }

            let subvector = &vector[start..end];

            // Find nearest centroid
            let mut best_idx = 0;
            let mut best_dist = f32::MAX;

            for (idx, centroid) in codebook.iter().enumerate() {
                let dist: f32 = subvector
                    .iter()
                    .zip(centroid.iter())
                    .map(|(x, y)| (x - y).powi(2))
                    .sum();
                if dist < best_dist {
                    best_dist = dist;
                    best_idx = idx;
                }
            }

            codes.push(best_idx as u8);
        }

        codes
    }
}
