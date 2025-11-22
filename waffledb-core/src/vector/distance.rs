/// Compute L2 (Euclidean) distance between two vectors.
pub fn l2_distance(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(
        a.len(),
        b.len(),
        "Vectors must have the same dimension"
    );
    a.iter()
        .zip(b.iter())
        .map(|(x, y)| (x - y).powi(2))
        .sum::<f32>()
        .sqrt()
}

/// Compute cosine distance (1 - cosine_similarity) between two vectors.
/// Assumes vectors are normalized.
pub fn cosine_distance(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(
        a.len(),
        b.len(),
        "Vectors must have the same dimension"
    );
    let dot_product: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
    1.0 - dot_product
}

/// Compute inner product (negative cosine distance for max search).
pub fn inner_product(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(
        a.len(),
        b.len(),
        "Vectors must have the same dimension"
    );
    a.iter().zip(b.iter()).map(|(x, y)| x * y).sum()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DistanceMetric {
    L2,
    Cosine,
    InnerProduct,
}

impl DistanceMetric {
    /// Compute distance using the specified metric.
    pub fn distance(&self, a: &[f32], b: &[f32]) -> f32 {
        match self {
            DistanceMetric::L2 => l2_distance(a, b),
            DistanceMetric::Cosine => cosine_distance(a, b),
            DistanceMetric::InnerProduct => -inner_product(a, b), // Negate for consistency
        }
    }
}
