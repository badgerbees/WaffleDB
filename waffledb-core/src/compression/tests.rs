#[cfg(test)]
mod tests {
    use crate::compression::binary_quant::BinaryQuantizer;
    use crate::compression::scalar_quant::ScalarQuantizer;
    use crate::compression::pq::training::PQTrainer;

    #[test]
    fn test_binary_quantizer_train() {
        let data = vec![
            vec![0.5, 1.5, -0.5, 2.0],
            vec![1.0, 2.0, 0.0, 1.5],
        ];
        
        let quantizer = BinaryQuantizer::train(&data);
        let encoded = quantizer.encode(&data[0]);
        let decoded = quantizer.decode(&encoded);
        assert_eq!(decoded.len(), 4);
    }

    #[test]
    fn test_binary_quantizer_encode() {
        let data = vec![vec![0.5, 1.5, -0.5, 2.0]];
        let quantizer = BinaryQuantizer::train(&data);
        let encoded = quantizer.encode(&data[0]);
        assert!(!encoded.is_empty());
    }

    #[test]
    fn test_binary_quantizer_decode() {
        let data = vec![vec![0.1, 0.5, 0.9, 0.2]];
        let quantizer = BinaryQuantizer::train(&data);
        let encoded = quantizer.encode(&data[0]);
        let decoded = quantizer.decode(&encoded);
        assert_eq!(decoded.len(), 4);
    }

    #[test]
    fn test_scalar_quantizer_train() {
        let data = vec![
            vec![0.0, 50.0, 100.0],
            vec![10.0, 60.0, 90.0],
        ];
        
        let quantizer = ScalarQuantizer::train(&data);
        let encoded = quantizer.encode(&data[0]);
        assert_eq!(encoded.len(), 3);
    }

    #[test]
    fn test_scalar_quantizer_encode_decode() {
        let data = vec![
            vec![0.0, 50.0, 100.0],
            vec![10.0, 60.0, 90.0],
        ];
        
        let quantizer = ScalarQuantizer::train(&data);
        let encoded = quantizer.encode(&data[0]);
        let decoded = quantizer.decode(&encoded);
        assert_eq!(decoded.len(), 3);
    }

    #[test]
    fn test_scalar_quantizer_bounds() {
        let data = vec![
            vec![-100.0, 0.0, 100.0],
            vec![-50.0, 50.0, 50.0],
        ];
        
        let quantizer = ScalarQuantizer::train(&data);
        let encoded = quantizer.encode(&data[0]);
        assert_eq!(encoded.len(), 3);
    }

    #[test]
    fn test_pq_trainer_creation() {
        let trainer = PQTrainer::new(4, 256);
        assert_eq!(trainer.n_subvectors, 4);
        assert_eq!(trainer.n_centroids, 256);
    }

    #[test]
    fn test_pq_trainer_small() {
        let trainer = PQTrainer::new(2, 4);
        let training_data = vec![
            vec![1.0, 2.0, 3.0, 4.0],
            vec![1.5, 2.5, 3.5, 4.5],
            vec![2.0, 3.0, 4.0, 5.0],
            vec![2.5, 3.5, 4.5, 5.5],
        ];
        
        let codebooks = trainer.train(&training_data);
        assert_eq!(codebooks.len(), 2);
        assert_eq!(codebooks[0].len(), 4);
    }

    #[test]
    fn test_pq_trainer_large() {
        let trainer = PQTrainer::new(4, 256);
        let data: Vec<Vec<f32>> = (0..300)  // Provide 300 data points instead of 100
            .map(|i| vec![i as f32; 16])
            .collect();
        
        let codebooks = trainer.train(&data);
        assert_eq!(codebooks.len(), 4);
        
        for codebook in codebooks {
            assert_eq!(codebook.len(), 256);
            assert_eq!(codebook[0].len(), 4);
        }
    }

    #[test]
    fn test_binary_hamming_distance_different() {
        let a = vec![0xFF, 0x00];
        let b = vec![0x00, 0xFF];
        let dist = BinaryQuantizer::hamming_distance(&a, &b);
        assert!(dist > 0);
    }

    #[test]
    fn test_binary_hamming_distance_identical() {
        let a = vec![0xAA, 0x55];
        let b = vec![0xAA, 0x55];
        let dist = BinaryQuantizer::hamming_distance(&a, &b);
        assert_eq!(dist, 0);
    }
}
