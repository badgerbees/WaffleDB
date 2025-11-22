/// Represents a dense float vector.
#[derive(Debug, Clone)]
pub struct Vector {
    pub data: Vec<f32>,
}

impl Vector {
    /// Create a new vector from raw f32 data.
    pub fn new(data: Vec<f32>) -> Self {
        Vector { data }
    }

    /// Get the dimension of the vector.
    pub fn dim(&self) -> usize {
        self.data.len()
    }

    /// Compute the L2 norm of the vector.
    pub fn l2_norm(&self) -> f32 {
        self.data.iter().map(|x| x * x).sum::<f32>().sqrt()
    }

    /// Normalize the vector to unit length.
    pub fn normalize(&self) -> Vector {
        let norm = self.l2_norm();
        if norm == 0.0 {
            Vector {
                data: vec![0.0; self.data.len()],
            }
        } else {
            Vector {
                data: self.data.iter().map(|x| x / norm).collect(),
            }
        }
    }
}

/// Quantized vector using 1 bit per dimension (binary).
#[derive(Debug, Clone)]
pub struct BinaryVector {
    pub data: Vec<u8>, // Packed bits
    pub dim: usize,    // Original dimension
}

/// Quantized vector using 1 byte per dimension.
#[derive(Debug, Clone)]
pub struct ScalarVector {
    pub data: Vec<u8>,
    pub dim: usize,
}

/// Product-quantized vector (compressed subvectors).
#[derive(Debug, Clone)]
pub struct PQVector {
    pub codes: Vec<u8>,     // Centroid indices per subvector
    pub n_subvectors: usize,
    pub dim: usize,
}
