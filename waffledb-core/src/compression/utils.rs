/// Compression utility functions.

/// Byte size of compressed vector with Binary Quantization.
pub fn binary_compressed_size(dim: usize) -> usize {
    (dim + 7) / 8
}

/// Byte size of compressed vector with Scalar Quantization.
pub fn scalar_compressed_size(dim: usize) -> usize {
    dim
}

/// Byte size of compressed vector with Product Quantization.
pub fn pq_compressed_size(n_subvectors: usize, _n_centroids: usize) -> usize {
    n_subvectors
}

/// Compute compression ratio.
pub fn compression_ratio(original_size: usize, compressed_size: usize) -> f32 {
    if original_size == 0 {
        1.0
    } else {
        original_size as f32 / compressed_size as f32
    }
}
