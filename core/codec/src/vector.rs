// vector.rs — SIMD-accelerated vector distance functions.
//
// Supports three distance metrics commonly used in vector search:
//   - L2 (Euclidean): sqrt(sum((a[i] - b[i])^2))
//   - Cosine: 1 - dot(a, b) / (|a| * |b|)
//   - Dot product: sum(a[i] * b[i])
//
// SIMD strategy:
//   - x86_64: AVX2+FMA (8 f32/instruction) with runtime detection
//   - aarch64: NEON (4 f32/instruction) — Apple Silicon, AWS Graviton
//   - Fallback: scalar loop (auto-vectorized by LLVM)
//
// The runtime check uses is_x86_feature_detected!, so a single binary
// works across x86_64 generations. On aarch64, NEON is always available
// (it's part of the base ISA), so no runtime check is needed.

#![allow(dead_code)]

/// Compute the L2 (Euclidean) distance between two f32 vectors.
pub fn l2_distance(a: &[f32], b: &[f32]) -> f64 {
    if a.len() != b.len() { return f64::INFINITY; }
    if a.is_empty() { return 0.0; }

    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx2") && is_x86_feature_detected!("fma") {
            return unsafe { l2_distance_avx2(a, b) };
        }
    }

    #[cfg(target_arch = "aarch64")]
    {
        return unsafe { l2_distance_neon(a, b) };
    }

    l2_distance_scalar(a, b)
}

/// Compute the cosine distance between two f32 vectors.
/// Returns 1.0 for zero vectors (treated as orthogonal).
pub fn cosine_distance(a: &[f32], b: &[f32]) -> f64 {
    if a.len() != b.len() || a.is_empty() { return 1.0; }
    let dot = dot_product(a, b);
    let na = dot_product(a, a).sqrt();
    let nb = dot_product(b, b).sqrt();
    if na == 0.0 || nb == 0.0 { return 1.0; }
    1.0 - dot / (na * nb)
}

/// Compute the dot product of two f32 vectors.
pub fn dot_product(a: &[f32], b: &[f32]) -> f64 {
    if a.len() != b.len() { return 0.0; }
    if a.is_empty() { return 0.0; }

    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx2") && is_x86_feature_detected!("fma") {
            return unsafe { dot_product_avx2(a, b) };
        }
    }

    #[cfg(target_arch = "aarch64")]
    {
        return unsafe { dot_product_neon(a, b) };
    }

    dot_product_scalar(a, b)
}

/// Search stored vectors for the k closest to the query.
/// Returns (index, distance) pairs sorted by distance ascending.
pub fn search_vectors(query: &[f32], stored: &[Vec<f32>], metric: &str, limit: usize) -> Vec<(usize, f64)> {
    if stored.is_empty() || limit == 0 { return Vec::new(); }
    let compute = |v: &Vec<f32>| -> f64 {
        match metric {
            "l2" | "euclidean" => l2_distance(query, v),
            "cosine" => cosine_distance(query, v),
            "dot" => -dot_product(query, v),
            _ => l2_distance(query, v),
        }
    };
    let mut results: Vec<(usize, f64)> = stored.iter().enumerate().map(|(i, v)| (i, compute(v))).collect();
    let k = limit.min(results.len());
    results.select_nth_unstable_by(k - 1, |a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
    results.truncate(k);
    results.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
    results
}

// ===========================================================================
// Scalar implementations (fallback + reference for testing)
// ===========================================================================

fn l2_distance_scalar(a: &[f32], b: &[f32]) -> f64 {
    let mut sum: f64 = 0.0;
    for i in 0..a.len() {
        let diff = a[i] as f64 - b[i] as f64;
        sum += diff * diff;
    }
    sum.sqrt()
}

fn dot_product_scalar(a: &[f32], b: &[f32]) -> f64 {
    let mut sum: f64 = 0.0;
    for i in 0..a.len() {
        sum += a[i] as f64 * b[i] as f64;
    }
    sum
}

// ===========================================================================
// AVX2 + FMA implementations (x86_64)
// ===========================================================================

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
#[target_feature(enable = "fma")]
unsafe fn l2_distance_avx2(a: &[f32], b: &[f32]) -> f64 {
    use std::arch::x86_64::*;
    let n = a.len();
    let mut sum = _mm256_setzero_ps();
    let mut i = 0;

    // Process 8 f32 at a time
    while i + 8 <= n {
        let va = _mm256_loadu_ps(a.as_ptr().add(i));
        let vb = _mm256_loadu_ps(b.as_ptr().add(i));
        let diff = _mm256_sub_ps(va, vb);
        sum = _mm256_fmadd_ps(diff, diff, sum);
        i += 8;
    }

    // Horizontal sum
    let mut result = horizontal_sum_m256(sum);

    // Process remaining elements
    while i < n {
        let diff = a[i] as f64 - b[i] as f64;
        result += diff * diff;
        i += 1;
    }

    result.sqrt()
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
#[target_feature(enable = "fma")]
unsafe fn dot_product_avx2(a: &[f32], b: &[f32]) -> f64 {
    use std::arch::x86_64::*;
    let n = a.len();
    let mut sum = _mm256_setzero_ps();
    let mut i = 0;

    while i + 8 <= n {
        let va = _mm256_loadu_ps(a.as_ptr().add(i));
        let vb = _mm256_loadu_ps(b.as_ptr().add(i));
        sum = _mm256_fmadd_ps(va, vb, sum);
        i += 8;
    }

    let mut result = horizontal_sum_m256(sum);

    while i < n {
        result += a[i] as f64 * b[i] as f64;
        i += 1;
    }

    result
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn horizontal_sum_m256(v: std::arch::x86_64::__m256) -> f64 {
    use std::arch::x86_64::*;
    let mut buf = [0f32; 8];
    _mm256_storeu_ps(buf.as_mut_ptr(), v);
    let mut sum: f64 = 0.0;
    for x in &buf {
        sum += *x as f64;
    }
    sum
}

// ===========================================================================
// NEON implementations (aarch64 — Apple Silicon, AWS Graviton, Raspberry Pi)
// ===========================================================================

#[cfg(target_arch = "aarch64")]
unsafe fn l2_distance_neon(a: &[f32], b: &[f32]) -> f64 {
    use std::arch::aarch64::*;
    let n = a.len();
    let mut sum = vdupq_n_f32(0.0);
    let mut i = 0;

    // Process 4 f32 at a time
    while i + 4 <= n {
        let va = vld1q_f32(a.as_ptr().add(i));
        let vb = vld1q_f32(b.as_ptr().add(i));
        let diff = vsubq_f32(va, vb);
        // FMA: sum = sum + diff * diff
        sum = vfmaq_f32(sum, diff, diff);
        i += 4;
    }

    // Horizontal sum
    let mut result: f64 = vaddvq_f32(sum) as f64;

    // Process remaining elements
    while i < n {
        let diff = a[i] as f64 - b[i] as f64;
        result += diff * diff;
        i += 1;
    }

    result.sqrt()
}

#[cfg(target_arch = "aarch64")]
unsafe fn dot_product_neon(a: &[f32], b: &[f32]) -> f64 {
    use std::arch::aarch64::*;
    let n = a.len();
    let mut sum = vdupq_n_f32(0.0);
    let mut i = 0;

    while i + 4 <= n {
        let va = vld1q_f32(a.as_ptr().add(i));
        let vb = vld1q_f32(b.as_ptr().add(i));
        sum = vfmaq_f32(sum, va, vb);
        i += 4;
    }

    let mut result: f64 = vaddvq_f32(sum) as f64;

    while i < n {
        result += a[i] as f64 * b[i] as f64;
        i += 1;
    }

    result
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_l2_distance_identical() {
        let a = vec![1.0, 2.0, 3.0];
        assert!(l2_distance(&a, &a) < 1e-6);
    }

    #[test]
    fn test_l2_distance_known() {
        assert!((l2_distance(&[0.0, 0.0], &[3.0, 4.0]) - 5.0).abs() < 1e-4);
    }

    #[test]
    fn test_dot_product_known() {
        assert!((dot_product(&[1.0, 2.0, 3.0], &[4.0, 5.0, 6.0]) - 32.0).abs() < 1e-4);
    }

    #[test]
    fn test_cosine_distance_identical() {
        let a = vec![1.0, 2.0, 3.0];
        assert!(cosine_distance(&a, &a).abs() < 1e-5);
    }

    #[test]
    fn test_cosine_distance_orthogonal() {
        assert!((cosine_distance(&[1.0, 0.0], &[0.0, 1.0]) - 1.0).abs() < 1e-5);
    }

    #[test]
    fn test_search_vectors_l2() {
        let query = vec![1.0, 0.0];
        let stored = vec![vec![1.0, 0.0], vec![0.0, 1.0], vec![2.0, 0.0]];
        let results = search_vectors(&query, &stored, "l2", 2);
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].0, 0);
    }

    #[test]
    fn test_mismatched_dimensions() {
        assert_eq!(l2_distance(&[1.0, 2.0, 3.0], &[1.0, 2.0]), f64::INFINITY);
    }

    #[test]
    fn test_large_dim_512() {
        let a: Vec<f32> = (0..512).map(|i| i as f32 * 0.01).collect();
        let b: Vec<f32> = (0..512).map(|i| i as f32 * 0.01 + 0.1).collect();
        let d_simd = l2_distance(&a, &b);
        let d_scalar = l2_distance_scalar(&a, &b);
        assert!((d_simd - d_scalar).abs() < 1e-2, "SIMD vs scalar: {} vs {}", d_simd, d_scalar);
    }

    #[test]
    fn test_large_dim_1536() {
        // 1536-dim = OpenAI text-embedding-ada-002
        let a: Vec<f32> = (0..1536).map(|i| (i as f32) * 0.001).collect();
        let b: Vec<f32> = (0..1536).map(|i| (i as f32) * 0.001 + 0.01).collect();
        let d_simd = cosine_distance(&a, &b);
        assert!((0.0..0.1).contains(&d_simd), "1536-dim cosine: {}", d_simd);
    }

    #[test]
    fn test_dot_product_orthogonal() {
        let a = vec![1.0, 0.0];
        let b = vec![0.0, 1.0];
        let d = dot_product(&a, &b);
        assert!(d.abs() < 1e-6, "orthogonal: expected 0, got {}", d);
    }

    #[test]
    fn test_cosine_distance_opposite() {
        let a = vec![1.0, 2.0];
        let b = vec![-1.0, -2.0];
        let d = cosine_distance(&a, &b);
        assert!((d - 2.0).abs() < 1e-5, "opposite: expected 2.0, got {}", d);
    }

    #[test]
    fn test_zero_vector_cosine() {
        let a = vec![0.0, 0.0];
        let b = vec![1.0, 2.0];
        let d = cosine_distance(&a, &b);
        assert_eq!(d, 1.0, "zero vector should be treated as orthogonal");
    }

    #[test]
    fn test_simd_vs_scalar_consistency() {
        // Verify SIMD path produces same results as scalar
        // Note: SIMD accumulates in f32 (not f64), so there's precision drift
        // for large sums. Use relative tolerance for large values.
        for n in [1, 4, 8, 16, 100, 256, 1000] {
            let a: Vec<f32> = (0..n).map(|i| (i as f32) * 0.1).collect();
            let b: Vec<f32> = (0..n).map(|i| (i as f32) * 0.1 + 0.5).collect();

            let l2_simd = l2_distance(&a, &b);
            let l2_scalar = l2_distance_scalar(&a, &b);
            let l2_tol = (l2_scalar * 0.01).max(1e-2); // 1% relative tolerance
            assert!((l2_simd - l2_scalar).abs() < l2_tol, "L2 mismatch at n={}: {} vs {}", n, l2_simd, l2_scalar);

            let dot_simd = dot_product(&a, &b);
            let dot_scalar = dot_product_scalar(&a, &b);
            let dot_tol = (dot_scalar.abs() * 0.01).max(1e-2); // 1% relative tolerance
            assert!((dot_simd - dot_scalar).abs() < dot_tol, "Dot mismatch at n={}: {} vs {}", n, dot_simd, dot_scalar);
        }
    }

    #[test]
    fn test_search_vectors_cosine() {
        let query = vec![1.0, 0.0];
        let stored = vec![
            vec![2.0, 0.0],    // same direction → cosine 0
            vec![0.0, 1.0],    // orthogonal → cosine 1
            vec![-1.0, 0.0],   // opposite → cosine 2
        ];
        let results = search_vectors(&query, &stored, "cosine", 3);
        assert_eq!(results[0].0, 0); // same direction is closest
        assert_eq!(results[2].0, 2); // opposite is farthest
    }

    #[test]
    fn test_search_vectors_empty() {
        let query = vec![1.0, 2.0];
        let stored: Vec<Vec<f32>> = vec![];
        assert!(search_vectors(&query, &stored, "l2", 10).is_empty());
    }

    #[test]
    fn test_search_vectors_dot() {
        let query = vec![1.0, 1.0];
        let stored = vec![
            vec![3.0, 3.0],   // dot = 6 (highest)
            vec![1.0, 0.0],   // dot = 1
            vec![0.0, 0.0],   // dot = 0 (lowest)
        ];
        let results = search_vectors(&query, &stored, "dot", 2);
        assert_eq!(results[0].0, 0); // highest dot = closest
    }
}
