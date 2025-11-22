/// Scalar quantization: compress each dimension to 1 byte (u8).
pub struct ScalarQuantizer {
    mins: Vec<f32>,
    maxs: Vec<f32>,
}

impl ScalarQuantizer {
    /// Create a new scalar quantizer with given bounds.
    pub fn new(mins: Vec<f32>, maxs: Vec<f32>) -> Self {
        assert_eq!(mins.len(), maxs.len());
        ScalarQuantizer { mins, maxs }
    }

    /// Train quantizer from vectors (compute min/max per dimension).
    pub fn train(vectors: &[Vec<f32>]) -> Self {
        if vectors.is_empty() {
            return ScalarQuantizer {
                mins: vec![],
                maxs: vec![],
            };
        }

        let dim = vectors[0].len();
        let mut mins = vec![f32::MAX; dim];
        let mut maxs = vec![f32::MIN; dim];

        for v in vectors {
            for (i, &val) in v.iter().enumerate() {
                mins[i] = mins[i].min(val);
                maxs[i] = maxs[i].max(val);
            }
        }

        ScalarQuantizer { mins, maxs }
    }

    /// Encode a vector to u8 per dimension.
    pub fn encode(&self, vector: &[f32]) -> Vec<u8> {
        vector
            .iter()
            .zip(self.mins.iter().zip(self.maxs.iter()))
            .map(|(val, (min, max))| {
                let range = max - min;
                if range == 0.0 {
                    0
                } else {
                    let normalized = ((val - min) / range).clamp(0.0, 1.0);
                    (normalized * 255.0) as u8
                }
            })
            .collect()
    }

    /// Decode a u8 vector back to f32.
    pub fn decode(&self, data: &[u8]) -> Vec<f32> {
        data.iter()
            .zip(self.mins.iter().zip(self.maxs.iter()))
            .map(|(byte, (min, max))| {
                let range = max - min;
                let normalized = *byte as f32 / 255.0;
                min + normalized * range
            })
            .collect()
    }

    /// L2 distance between quantized vectors.
    pub fn distance(a: &[u8], b: &[u8]) -> f32 {
        a.iter()
            .zip(b.iter())
            .map(|(x, y)| {
                let diff = (*x as i32) - (*y as i32);
                (diff * diff) as f32
            })
            .sum::<f32>()
            .sqrt()
    }
}
