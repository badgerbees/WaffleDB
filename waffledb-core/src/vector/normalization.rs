/// Normalize a raw vector to unit length.
pub fn normalize_vector(data: &[f32]) -> Vec<f32> {
    let norm = data.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm == 0.0 {
        vec![0.0; data.len()]
    } else {
        data.iter().map(|x| x / norm).collect()
    }
}

/// Batch normalize multiple vectors.
pub fn batch_normalize(vectors: &[Vec<f32>]) -> Vec<Vec<f32>> {
    vectors.iter().map(|v| normalize_vector(v)).collect()
}

/// Center vectors by subtracting the mean.
pub fn center_vectors(vectors: &[Vec<f32>]) -> (Vec<Vec<f32>>, Vec<f32>) {
    if vectors.is_empty() {
        return (vec![], vec![]);
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

    let centered = vectors
        .iter()
        .map(|v| v.iter().zip(mean.iter()).map(|(x, m)| x - m).collect())
        .collect();

    (centered, mean)
}
