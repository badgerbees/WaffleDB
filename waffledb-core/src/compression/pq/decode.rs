/// Product Quantization decoder.
pub struct PQDecoder {
    pub codebooks: Vec<Vec<Vec<f32>>>,
    pub subvector_dim: usize,
}

impl PQDecoder {
    /// Create a new PQ decoder with trained codebooks.
    pub fn new(codebooks: Vec<Vec<Vec<f32>>>) -> Self {
        let subvector_dim = if codebooks.is_empty() {
            0
        } else {
            codebooks[0][0].len()
        };

        PQDecoder {
            codebooks,
            subvector_dim,
        }
    }

    /// Decode PQ codes back to a vector.
    pub fn decode(&self, codes: &[u8]) -> Vec<f32> {
        let mut result = vec![];

        for (subvector_idx, code) in codes.iter().enumerate() {
            if subvector_idx >= self.codebooks.len() {
                break;
            }

            let codebook = &self.codebooks[subvector_idx];
            if (*code as usize) < codebook.len() {
                result.extend_from_slice(&codebook[*code as usize]);
            }
        }

        result
    }
}
