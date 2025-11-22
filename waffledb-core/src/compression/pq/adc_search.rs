/// Asymmetric Distance Computation (ADC) for PQ.
/// Computes distance between full query vector and PQ-compressed database vector.

pub struct ADCSearcher {
    pub codebooks: Vec<Vec<Vec<f32>>>,
    pub subvector_dim: usize,
}

impl ADCSearcher {
    /// Create a new ADC searcher.
    pub fn new(codebooks: Vec<Vec<Vec<f32>>>) -> Self {
        let subvector_dim = if codebooks.is_empty() {
            0
        } else {
            codebooks[0][0].len()
        };

        ADCSearcher {
            codebooks,
            subvector_dim,
        }
    }

    /// Compute lookup table for ADC: distance from each query subvector to all centroids.
    pub fn compute_lookup_table(&self, query: &[f32]) -> Vec<Vec<f32>> {
        let mut lookup = vec![];

        for (subvector_idx, codebook) in self.codebooks.iter().enumerate() {
            let start = subvector_idx * self.subvector_dim;
            let end = (start + self.subvector_dim).min(query.len());

            if start >= query.len() {
                break;
            }

            let query_subvector = &query[start..end];
            let mut distances = vec![];

            for centroid in codebook {
                let dist: f32 = query_subvector
                    .iter()
                    .zip(centroid.iter())
                    .map(|(x, y)| (x - y).powi(2))
                    .sum();
                distances.push(dist);
            }

            lookup.push(distances);
        }

        lookup
    }

    /// Compute distance using precomputed lookup table.
    pub fn adc_distance(&self, codes: &[u8], lookup: &[Vec<f32>]) -> f32 {
        let mut distance = 0.0;

        for (subvector_idx, code) in codes.iter().enumerate() {
            if subvector_idx < lookup.len() {
                let code_idx = *code as usize;
                if code_idx < lookup[subvector_idx].len() {
                    distance += lookup[subvector_idx][code_idx];
                }
            }
        }

        distance.sqrt()
    }
}

/// Quick ADC distance computation without precomputed lookup.
pub fn adc_distance(
    query: &[f32],
    codes: &[u8],
    codebooks: &[Vec<Vec<f32>>],
    subvector_dim: usize,
) -> f32 {
    let mut distance = 0.0;

    for (subvector_idx, code) in codes.iter().enumerate() {
        if subvector_idx >= codebooks.len() {
            break;
        }

        let start = subvector_idx * subvector_dim;
        let end = (start + subvector_dim).min(query.len());

        if start >= query.len() {
            break;
        }

        let query_subvector = &query[start..end];
        let code_idx = *code as usize;

        if code_idx < codebooks[subvector_idx].len() {
            let centroid = &codebooks[subvector_idx][code_idx];

            let dist: f32 = query_subvector
                .iter()
                .zip(centroid.iter())
                .map(|(x, y)| (x - y).powi(2))
                .sum();
            distance += dist;
        }
    }

    distance.sqrt()
}
