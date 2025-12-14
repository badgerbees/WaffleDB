/// SIMD-accelerated kernels for distance metrics using AVX2 and AVX512.
///
/// This module provides high-performance distance computations with automatic
/// fallback to scalar operations on unsupported platforms.

#[cfg(all(target_arch = "x86_64", any(target_feature = "avx2", target_feature = "avx512f")))]
use std::arch::x86_64::*;

/// SIMD L2 distance computation with automatic fallback.
pub fn l2_distance_simd(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len());
    
    #[cfg(all(target_arch = "x86_64", target_feature = "avx512f"))]
    unsafe {
        l2_distance_avx512(a, b)
    }
    
    #[cfg(all(target_arch = "x86_64", target_feature = "avx2", not(target_feature = "avx512f")))]
    unsafe {
        l2_distance_avx2(a, b)
    }
    
    #[cfg(not(all(target_arch = "x86_64", any(target_feature = "avx2", target_feature = "avx512f"))))]
    {
        l2_distance_scalar(a, b)
    }
}

/// SIMD cosine distance computation with automatic fallback.
pub fn cosine_distance_simd(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len());
    
    #[cfg(all(target_arch = "x86_64", target_feature = "avx512f"))]
    unsafe {
        cosine_distance_avx512(a, b)
    }
    
    #[cfg(all(target_arch = "x86_64", target_feature = "avx2", not(target_feature = "avx512f")))]
    unsafe {
        cosine_distance_avx2(a, b)
    }
    
    #[cfg(not(all(target_arch = "x86_64", any(target_feature = "avx2", target_feature = "avx512f"))))]
    {
        cosine_distance_scalar(a, b)
    }
}

/// SIMD inner product computation with automatic fallback.
pub fn inner_product_simd(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len());
    
    #[cfg(all(target_arch = "x86_64", target_feature = "avx512f"))]
    unsafe {
        inner_product_avx512(a, b)
    }
    
    #[cfg(all(target_arch = "x86_64", target_feature = "avx2", not(target_feature = "avx512f")))]
    unsafe {
        inner_product_avx2(a, b)
    }
    
    #[cfg(not(all(target_arch = "x86_64", any(target_feature = "avx2", target_feature = "avx512f"))))]
    {
        inner_product_scalar(a, b)
    }
}

// ============================================================================
// Scalar Fallbacks (always available)
// ============================================================================

#[inline]
fn l2_distance_scalar(a: &[f32], b: &[f32]) -> f32 {
    a.iter()
        .zip(b.iter())
        .map(|(x, y)| (x - y).powi(2))
        .sum::<f32>()
        .sqrt()
}

#[inline]
fn cosine_distance_scalar(a: &[f32], b: &[f32]) -> f32 {
    let dot: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
    1.0 - dot
}

#[inline]
fn inner_product_scalar(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b.iter()).map(|(x, y)| x * y).sum()
}

// ============================================================================
// AVX2 Implementations (8 floats per iteration, 256-bit vectors)
// ============================================================================

#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
#[inline]
unsafe fn l2_distance_avx2(a: &[f32], b: &[f32]) -> f32 {
    let mut sum_vec = _mm256_setzero_ps();
    let mut i = 0;
    
    // Process 8 floats at a time
    while i + 8 <= a.len() {
        let a_vec = _mm256_loadu_ps(&a[i]);
        let b_vec = _mm256_loadu_ps(&b[i]);
        
        // Compute (a - b)^2
        let diff = _mm256_sub_ps(a_vec, b_vec);
        let sq = _mm256_mul_ps(diff, diff);
        
        sum_vec = _mm256_add_ps(sum_vec, sq);
        i += 8;
    }
    
    // Horizontal sum
    let mut sum = horizontal_sum_avx2(sum_vec);
    
    // Scalar remainder
    while i < a.len() {
        let diff = a[i] - b[i];
        sum += diff * diff;
        i += 1;
    }
    
    sum.sqrt()
}

#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
#[inline]
unsafe fn cosine_distance_avx2(a: &[f32], b: &[f32]) -> f32 {
    1.0 - inner_product_avx2(a, b)
}

#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
#[inline]
unsafe fn inner_product_avx2(a: &[f32], b: &[f32]) -> f32 {
    let mut dot_vec = _mm256_setzero_ps();
    let mut i = 0;
    
    // Process 8 floats at a time
    while i + 8 <= a.len() {
        let a_vec = _mm256_loadu_ps(&a[i]);
        let b_vec = _mm256_loadu_ps(&b[i]);
        
        // Compute a * b
        let prod = _mm256_mul_ps(a_vec, b_vec);
        
        dot_vec = _mm256_add_ps(dot_vec, prod);
        i += 8;
    }
    
    // Horizontal sum
    let mut dot = horizontal_sum_avx2(dot_vec);
    
    // Scalar remainder
    while i < a.len() {
        dot += a[i] * b[i];
        i += 1;
    }
    
    dot
}

#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
#[inline]
unsafe fn horizontal_sum_avx2(v: __m256) -> f32 {
    // Sum all 8 floats: [a, b, c, d, e, f, g, h]
    let v = _mm256_hadd_ps(v, v); // [a+b, c+d, a+b, c+d, e+f, g+h, e+f, g+h]
    let v = _mm256_hadd_ps(v, v); // [a+b+c+d, a+b+c+d, a+b+c+d, a+b+c+d, ...]
    
    // Extract from low lane and add high lane
    let sum_low = _mm_cvtss_f32(_mm256_castps256_ps128(v));
    let sum_high = _mm_cvtss_f32(_mm256_extractf128_ps(v, 1));
    
    sum_low + sum_high
}

// ============================================================================
// AVX512 Implementations (16 floats per iteration, 512-bit vectors)
// ============================================================================

#[cfg(all(target_arch = "x86_64", target_feature = "avx512f"))]
#[inline]
unsafe fn l2_distance_avx512(a: &[f32], b: &[f32]) -> f32 {
    let mut sum = 0.0f32;
    let mut i = 0;
    
    // Process 16 floats at a time
    while i + 16 <= a.len() {
        let a_vec = _mm512_loadu_ps(&a[i]);
        let b_vec = _mm512_loadu_ps(&b[i]);
        
        // Compute (a - b)^2
        let diff = _mm512_sub_ps(a_vec, b_vec);
        let sq = _mm512_mul_ps(diff, diff);
        
        // Accumulate
        sum += _mm512_reduce_add_ps(sq);
        i += 16;
    }
    
    // Process remaining with AVX2 if available
    #[cfg(target_feature = "avx2")]
    {
        while i + 8 <= a.len() {
            let a_vec = _mm256_loadu_ps(&a[i]);
            let b_vec = _mm256_loadu_ps(&b[i]);
            
            let diff = _mm256_sub_ps(a_vec, b_vec);
            let sq = _mm256_mul_ps(diff, diff);
            
            sum += horizontal_sum_avx2(sq);
            i += 8;
        }
    }
    
    // Scalar remainder
    while i < a.len() {
        let diff = a[i] - b[i];
        sum += diff * diff;
        i += 1;
    }
    
    sum.sqrt()
}

#[cfg(all(target_arch = "x86_64", target_feature = "avx512f"))]
#[inline]
unsafe fn cosine_distance_avx512(a: &[f32], b: &[f32]) -> f32 {
    1.0 - inner_product_avx512(a, b)
}

#[cfg(all(target_arch = "x86_64", target_feature = "avx512f"))]
#[inline]
unsafe fn inner_product_avx512(a: &[f32], b: &[f32]) -> f32 {
    let mut sum = 0.0f32;
    let mut i = 0;
    
    // Process 16 floats at a time
    while i + 16 <= a.len() {
        let a_vec = _mm512_loadu_ps(&a[i]);
        let b_vec = _mm512_loadu_ps(&b[i]);
        
        // Compute a * b
        let prod = _mm512_mul_ps(a_vec, b_vec);
        
        sum += _mm512_reduce_add_ps(prod);
        i += 16;
    }
    
    // Process remaining with AVX2 if available
    #[cfg(target_feature = "avx2")]
    {
        while i + 8 <= a.len() {
            let a_vec = _mm256_loadu_ps(&a[i]);
            let b_vec = _mm256_loadu_ps(&b[i]);
            
            let prod = _mm256_mul_ps(a_vec, b_vec);
            
            sum += horizontal_sum_avx2(prod);
            i += 8;
        }
    }
    
    // Scalar remainder
    while i < a.len() {
        sum += a[i] * b[i];
        i += 1;
    }
    
    sum
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_l2_distance_simd() {
        let a = vec![0.0; 256];
        let b: Vec<f32> = (0..256).map(|i| i as f32).collect();
        
        let dist = l2_distance_simd(&a, &b);
        
        // Expected: sqrt(0^2 + 1^2 + 2^2 + ... + 255^2)
        let expected_sum: f32 = (0..256).map(|i: i32| (i as f32).powi(2)).sum();
        let expected = expected_sum.sqrt();
        
        assert!((dist - expected).abs() < 1e-4);
    }

    #[test]
    fn test_cosine_distance_simd() {
        let norm = 128.0_f32.sqrt();
        let a: Vec<f32> = vec![1.0 / norm; 128];
        let b: Vec<f32> = vec![1.0 / norm; 128];
        
        let dist = cosine_distance_simd(&a, &b);
        
        // Identical normalized vectors
        assert!(dist.abs() < 1e-5);
    }

    #[test]
    fn test_inner_product_simd() {
        let a = vec![1.0, 2.0, 3.0, 4.0];
        let b = vec![1.0, 2.0, 3.0, 4.0];
        
        let dot = inner_product_simd(&a, &b);
        
        // Expected: 1*1 + 2*2 + 3*3 + 4*4 = 30
        assert!((dot - 30.0).abs() < 1e-6);
    }

    #[test]
    fn test_large_vector_l2() {
        let a: Vec<f32> = (0..10000).map(|i| (i as f32) * 0.1).collect();
        let b: Vec<f32> = (0..10000).map(|i| (i as f32) * 0.1 + 1.0).collect();
        
        let dist = l2_distance_simd(&a, &b);
        
        // All differences are 1.0, so distance = sqrt(10000)
        assert!((dist - 100.0).abs() < 1e-4);
    }

    #[test]
    fn test_orthogonal_vectors() {
        let a: Vec<f32> = (0..100).map(|i| if i < 50 { 1.0 } else { 0.0 }).collect();
        let b: Vec<f32> = (0..100).map(|i| if i >= 50 { 1.0 } else { 0.0 }).collect();
        
        let dot = inner_product_simd(&a, &b);
        
        // Orthogonal vectors
        assert!(dot.abs() < 1e-6);
    }

    #[test]
    fn test_simd_consistency_small() {
        let a = vec![0.5, 1.5, 2.5, 3.5];
        let b = vec![1.0, 2.0, 3.0, 4.0];
        
        let dist_simd = l2_distance_simd(&a, &b);
        let dist_scalar = l2_distance_scalar(&a, &b);
        
        assert!((dist_simd - dist_scalar).abs() < 1e-6);
    }

    #[test]
    fn test_simd_consistency_large() {
        let a: Vec<f32> = (0..512).map(|i| (i as f32) * 0.01).collect();
        let b: Vec<f32> = (0..512).map(|i| (i as f32) * 0.01 + 0.5).collect();
        
        let dist_simd = l2_distance_simd(&a, &b);
        let dist_scalar = l2_distance_scalar(&a, &b);
        
        assert!((dist_simd - dist_scalar).abs() < 1e-4);
    }

    #[test]
    fn test_cosine_simd_consistency() {
        let a: Vec<f32> = (0..256).map(|i| ((i as f32) * 0.1).cos()).collect();
        let b: Vec<f32> = (0..256).map(|i| ((i as f32) * 0.1).sin()).collect();
        
        let dist_simd = cosine_distance_simd(&a, &b);
        let dist_scalar = cosine_distance_scalar(&a, &b);
        
        assert!((dist_simd - dist_scalar).abs() < 1e-4);
    }

    #[test]
    fn test_inner_product_simd_consistency() {
        let a: Vec<f32> = (0..384).map(|i| (i as f32).sqrt()).collect();
        let b: Vec<f32> = (0..384).map(|i| (i as f32).sin()).collect();
        
        let dot_simd = inner_product_simd(&a, &b);
        let dot_scalar = inner_product_scalar(&a, &b);
        
        assert!((dot_simd - dot_scalar).abs() < 1e-4);
    }
}
