#[cfg(test)]
mod tests {
    use crate::vector::types::Vector;
    use crate::vector::distance::{l2_distance, cosine_distance, inner_product, DistanceMetric};
    use crate::vector::normalization::normalize_vector;

    #[test]
    fn test_vector_creation_and_norm() {
        let vec = Vector::new(vec![3.0, 4.0]);
        assert_eq!(vec.data.len(), 2);
        assert!((vec.l2_norm() - 5.0).abs() < 1e-6); // sqrt(9 + 16)
    }

    #[test]
    fn test_l2_distance() {
        let v1 = vec![0.0, 0.0];
        let v2 = vec![3.0, 4.0];
        let dist = l2_distance(&v1, &v2);
        assert!((dist - 5.0).abs() < 1e-6);
    }

    #[test]
    fn test_l2_distance_identical_vectors() {
        let v = vec![1.0, 2.0, 3.0];
        let dist = l2_distance(&v, &v);
        assert!(dist < 1e-6);
    }

    #[test]
    fn test_cosine_distance() {
        let v1 = vec![1.0, 0.0];
        let v2 = vec![1.0, 0.0];
        let dist = cosine_distance(&v1, &v2);
        assert!(dist < 1e-6); // identical vectors
    }

    #[test]
    fn test_cosine_distance_orthogonal() {
        let v1 = vec![1.0, 0.0];
        let v2 = vec![0.0, 1.0];
        let dist = cosine_distance(&v1, &v2);
        assert!((dist - 1.0).abs() < 1e-6); // orthogonal -> distance = 1
    }

    #[test]
    fn test_inner_product() {
        let v1 = vec![1.0, 2.0, 3.0];
        let v2 = vec![4.0, 5.0, 6.0];
        let prod = inner_product(&v1, &v2);
        assert_eq!(prod, 32.0); // 1*4 + 2*5 + 3*6
    }

    #[test]
    fn test_normalize_vector() {
        let v = vec![3.0, 4.0];
        let normalized = normalize_vector(&v);
        assert_eq!(normalized.len(), 2);
        assert!((normalized[0] - 0.6).abs() < 1e-6);
        assert!((normalized[1] - 0.8).abs() < 1e-6);
    }

    #[test]
    fn test_distance_metric_enum() {
        let v1 = vec![1.0, 0.0];
        let v2 = vec![0.0, 1.0];
        
        let l2 = DistanceMetric::L2.distance(&v1, &v2);
        assert!((l2 - std::f32::consts::SQRT_2).abs() < 1e-6);
        
        let cosine = DistanceMetric::Cosine.distance(&v1, &v2);
        assert!((cosine - 1.0).abs() < 1e-6);
        
        let ip = DistanceMetric::InnerProduct.distance(&v1, &v2);
        assert_eq!(ip, 0.0);
    }

    #[test]
    fn test_binary_vector() {
        use crate::vector::types::BinaryVector;
        let bits = vec![0xAA, 0x55];
        let bv = BinaryVector { data: bits.clone(), dim: 16 };
        assert_eq!(bv.data, bits);
        assert_eq!(bv.data.len(), 2);
    }

    #[test]
    fn test_scalar_vector() {
        use crate::vector::types::ScalarVector;
        let data = vec![100u8, 200u8, 50u8];
        let sv = ScalarVector { data: data.clone(), dim: 3 };
        assert_eq!(sv.data, data);
        assert_eq!(sv.data.len(), 3);
    }

    #[test]
    fn test_pq_vector() {
        use crate::vector::types::PQVector;
        let codes = vec![0u8, 1u8, 2u8, 3u8];
        let pq = PQVector { codes: codes.clone(), n_subvectors: 4, dim: 16 };
        assert_eq!(pq.codes, codes);
        assert_eq!(pq.n_subvectors, 4);
    }
}
