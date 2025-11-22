/// Binary quantization: compress each dimension to 1 bit.
pub struct BinaryQuantizer {
    thresholds: Vec<f32>, // Per-dimension thresholds (typically mean)
}

impl BinaryQuantizer {
    /// Create a new binary quantizer.
    pub fn new(thresholds: Vec<f32>) -> Self {
        BinaryQuantizer { thresholds }
    }

    /// Train thresholds from vectors (simple: use mean).
    pub fn train(vectors: &[Vec<f32>]) -> Self {
        if vectors.is_empty() {
            return BinaryQuantizer {
                thresholds: vec![],
            };
        }

        let dim = vectors[0].len();
        let mut mean = vec![0.0; dim];

        for v in vectors {
            for (i, &val) in v.iter().enumerate() {
                mean[i] += val;
            }
        }

        for m in &mut mean {
            *m /= vectors.len() as f32;
        }

        BinaryQuantizer {
            thresholds: mean,
        }
    }

    /// Encode a vector to binary (packed bits).
    pub fn encode(&self, vector: &[f32]) -> Vec<u8> {
        let mut bytes = vec![0u8; (vector.len() + 7) / 8];

        for (i, &val) in vector.iter().enumerate() {
            let bit = if val >= self.thresholds[i] { 1 } else { 0 };
            let byte_idx = i / 8;
            let bit_idx = i % 8;
            bytes[byte_idx] |= (bit << bit_idx) as u8;
        }

        bytes
    }

    /// Decode a binary vector.
    pub fn decode(&self, data: &[u8]) -> Vec<f32> {
        let mut result = vec![0.0; self.thresholds.len()];

        for (i, threshold) in self.thresholds.iter().enumerate() {
            let byte_idx = i / 8;
            let bit_idx = i % 8;
            let bit = (data[byte_idx] >> bit_idx) & 1;
            result[i] = if bit == 1 { *threshold + 0.5 } else { *threshold - 0.5 };
        }

        result
    }

    /// Hamming distance between two binary vectors.
    pub fn hamming_distance(a: &[u8], b: &[u8]) -> usize {
        a.iter()
            .zip(b.iter())
            .map(|(x, y)| (x ^ y).count_ones() as usize)
            .sum()
    }
}
