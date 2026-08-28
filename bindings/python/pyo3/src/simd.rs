// SIMD-accelerated INT64 predicate evaluation.
//
// Uses std::arch::x86_64 AVX2 intrinsics for parallel comparison of 4 i64
// values at once. Falls back to scalar on non-x86_64 or pre-AVX2 CPUs.
//
// This is the hottest loop in read_rows: filtering INT64 columns by
// predicates like `col > value` or `col = value`.

#[cfg(target_arch = "x86_64")]
use std::arch::x86_64::*;

/// Filter an INT64 array by equality (col = value).
///
/// Returns a boolean mask: true for each element that equals `value`.
/// Uses AVX2 when available (4x i64 comparison per instruction).
pub fn filter_eq_i64(data: &[i64], value: i64) -> Vec<bool> {
    let mut result = vec![false; data.len()];

    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx2") {
            unsafe { filter_eq_i64_avx2(data, value, &mut result); }
            return result;
        }
    }

    // Scalar fallback (LLVM may auto-vectorize this)
    for (i, &v) in data.iter().enumerate() {
        result[i] = v == value;
    }
    result
}

/// Filter an INT64 array by comparison (col op value).
///
/// op: "=", "!=", ">", ">=", "<", "<="
pub fn filter_cmp_i64(data: &[i64], op: &str, value: i64) -> Vec<bool> {
    let mut result = vec![false; data.len()];

    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx2") {
            unsafe { filter_cmp_i64_avx2(data, op, value, &mut result); }
            return result;
        }
    }

    // Scalar fallback
    for (i, &v) in data.iter().enumerate() {
        result[i] = match op {
            "=" | "==" => v == value,
            "!=" | "<>" => v != value,
            ">" => v > value,
            ">=" => v >= value,
            "<" => v < value,
            "<=" => v <= value,
            _ => false,
        };
    }
    result
}

/// Filter INT64 array by range: value_min <= col <= value_max.
#[allow(dead_code)]
pub fn filter_range_i64(data: &[i64], value_min: i64, value_max: i64) -> Vec<bool> {
    let mut result = vec![false; data.len()];

    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx2") {
            unsafe { filter_range_i64_avx2(data, value_min, value_max, &mut result); }
            return result;
        }
    }

    // Scalar fallback
    for (i, &v) in data.iter().enumerate() {
        result[i] = v >= value_min && v <= value_max;
    }
    result
}

// ---------------------------------------------------------------------------
// AVX2 implementations (4x i64 per instruction)
// ---------------------------------------------------------------------------

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn filter_eq_i64_avx2(data: &[i64], value: i64, result: &mut [bool]) {
    let broadcast = _mm256_set1_epi64x(value);
    let mut i = 0;
    while i + 4 <= data.len() {
        let chunk = _mm256_loadu_si256(data.as_ptr().add(i) as *const __m256i);
        let cmp = _mm256_cmpeq_epi64(chunk, broadcast);
        let mask = _mm256_movemask_epi8(cmp) as u32;
        for j in 0..4 {
            result[i + j] = (mask >> (j * 8)) & 1 != 0;
        }
        i += 4;
    }
    // Handle remaining elements (scalar)
    while i < data.len() {
        result[i] = data[i] == value;
        i += 1;
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn filter_cmp_i64_avx2(data: &[i64], op: &str, value: i64, result: &mut [bool]) {
    let broadcast = _mm256_set1_epi64x(value);
    let mut i = 0;
    while i + 4 <= data.len() {
        let chunk = _mm256_loadu_si256(data.as_ptr().add(i) as *const __m256i);
        let mask_bits = match op {
            "=" | "==" => _mm256_movemask_epi8(_mm256_cmpeq_epi64(chunk, broadcast)) as u32,
            ">" => _mm256_movemask_epi8(_mm256_cmpgt_epi64(chunk, broadcast)) as u32,
            "<" => _mm256_movemask_epi8(_mm256_cmpgt_epi64(broadcast, chunk)) as u32,
            ">=" => {
                let gt = _mm256_cmpgt_epi64(chunk, broadcast);
                let eq = _mm256_cmpeq_epi64(chunk, broadcast);
                _mm256_movemask_epi8(_mm256_or_si256(gt, eq)) as u32
            }
            "<=" => {
                let gt = _mm256_cmpgt_epi64(broadcast, chunk);
                let eq = _mm256_cmpeq_epi64(chunk, broadcast);
                _mm256_movemask_epi8(_mm256_or_si256(gt, eq)) as u32
            }
            "!=" | "<>" => {
                let eq = _mm256_cmpeq_epi64(chunk, broadcast);
                // NOT eq — use XOR with all-ones
                _mm256_movemask_epi8(_mm256_xor_si256(eq, _mm256_set1_epi32(-1))) as u32
            }
            _ => 0,
        };
        for j in 0..4 {
            result[i + j] = (mask_bits >> (j * 8)) & 1 != 0;
        }
        i += 4;
    }
    // Handle remaining elements (scalar)
    while i < data.len() {
        result[i] = match op {
            "=" | "==" => data[i] == value,
            "!=" | "<>" => data[i] != value,
            ">" => data[i] > value,
            ">=" => data[i] >= value,
            "<" => data[i] < value,
            "<=" => data[i] <= value,
            _ => false,
        };
        i += 1;
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
#[allow(dead_code)]
unsafe fn filter_range_i64_avx2(data: &[i64], value_min: i64, value_max: i64, result: &mut [bool]) {
    let min_broadcast = _mm256_set1_epi64x(value_min);
    let max_broadcast = _mm256_set1_epi64x(value_max);
    let mut i = 0;
    while i + 4 <= data.len() {
        let chunk = _mm256_loadu_si256(data.as_ptr().add(i) as *const __m256i);
        // col >= min: cmpgt_epi64(chunk, min) OR cmpeq(chunk, min)
        let ge_min = _mm256_or_si256(
            _mm256_cmpgt_epi64(chunk, min_broadcast),
            _mm256_cmpeq_epi64(chunk, min_broadcast),
        );
        // col <= max: cmpgt_epi64(max, chunk) OR cmpeq(chunk, max)
        let le_max = _mm256_or_si256(
            _mm256_cmpgt_epi64(max_broadcast, chunk),
            _mm256_cmpeq_epi64(chunk, max_broadcast),
        );
        // AND: both conditions must be true
        let in_range = _mm256_and_si256(ge_min, le_max);
        let mask = _mm256_movemask_epi8(in_range) as u32;
        for j in 0..4 {
            result[i + j] = (mask >> (j * 8)) & 1 != 0;
        }
        i += 4;
    }
    // Handle remaining elements (scalar)
    while i < data.len() {
        result[i] = data[i] >= value_min && data[i] <= value_max;
        i += 1;
    }
}

// ---------------------------------------------------------------------------
// FLOAT64 SIMD filters (4x f64 per instruction via AVX2 __m256d)
// ---------------------------------------------------------------------------

/// Filter a FLOAT64 array by comparison (col op value).
///
/// op: "=", "!=", ">", ">=", "<", "<="
pub fn filter_cmp_f64(data: &[f64], op: &str, value: f64) -> Vec<bool> {
    let mut result = vec![false; data.len()];

    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx2") {
            unsafe { filter_cmp_f64_avx2(data, op, value, &mut result); }
            return result;
        }
    }

    // Scalar fallback
    for (i, &v) in data.iter().enumerate() {
        result[i] = match op {
            "=" | "==" => v == value,
            "!=" | "<>" => v != value,
            ">" => v > value,
            ">=" => v >= value,
            "<" => v < value,
            "<=" => v <= value,
            _ => false,
        };
    }
    result
}

/// Filter FLOAT64 array by range: value_min <= col <= value_max.
#[allow(dead_code)]
pub fn filter_range_f64(data: &[f64], value_min: f64, value_max: f64) -> Vec<bool> {
    let mut result = vec![false; data.len()];

    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx2") {
            unsafe { filter_range_f64_avx2(data, value_min, value_max, &mut result); }
            return result;
        }
    }

    for (i, &v) in data.iter().enumerate() {
        result[i] = v >= value_min && v <= value_max;
    }
    result
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn filter_cmp_f64_avx2(data: &[f64], op: &str, value: f64, result: &mut [bool]) {
    let broadcast = _mm256_set1_pd(value);
    let mut i = 0;
    while i + 4 <= data.len() {
        let chunk = _mm256_loadu_pd(data.as_ptr().add(i));
        let mask_bits = match op {
            "=" | "==" => _mm256_movemask_pd(_mm256_cmp_pd(chunk, broadcast, _CMP_EQ_OQ)) as u32,
            "!=" | "<>" => _mm256_movemask_pd(_mm256_cmp_pd(chunk, broadcast, _CMP_NEQ_OQ)) as u32,
            ">" => _mm256_movemask_pd(_mm256_cmp_pd(chunk, broadcast, _CMP_GT_OQ)) as u32,
            ">=" => _mm256_movemask_pd(_mm256_cmp_pd(chunk, broadcast, _CMP_GE_OQ)) as u32,
            "<" => _mm256_movemask_pd(_mm256_cmp_pd(chunk, broadcast, _CMP_LT_OQ)) as u32,
            "<=" => _mm256_movemask_pd(_mm256_cmp_pd(chunk, broadcast, _CMP_LE_OQ)) as u32,
            _ => 0,
        };
        for j in 0..4 {
            result[i + j] = (mask_bits >> j) & 1 != 0;
        }
        i += 4;
    }
    while i < data.len() {
        result[i] = match op {
            "=" | "==" => data[i] == value,
            "!=" | "<>" => data[i] != value,
            ">" => data[i] > value,
            ">=" => data[i] >= value,
            "<" => data[i] < value,
            "<=" => data[i] <= value,
            _ => false,
        };
        i += 1;
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
#[allow(dead_code)]
unsafe fn filter_range_f64_avx2(data: &[f64], value_min: f64, value_max: f64, result: &mut [bool]) {
    let min_b = _mm256_set1_pd(value_min);
    let max_b = _mm256_set1_pd(value_max);
    let mut i = 0;
    while i + 4 <= data.len() {
        let chunk = _mm256_loadu_pd(data.as_ptr().add(i));
        let ge_min = _mm256_cmp_pd(chunk, min_b, _CMP_GE_OQ);
        let le_max = _mm256_cmp_pd(chunk, max_b, _CMP_LE_OQ);
        let in_range = _mm256_and_pd(ge_min, le_max);
        let mask = _mm256_movemask_pd(in_range) as u32;
        for j in 0..4 {
            result[i + j] = (mask >> j) & 1 != 0;
        }
        i += 4;
    }
    while i < data.len() {
        result[i] = data[i] >= value_min && data[i] <= value_max;
        i += 1;
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_filter_eq() {
        let data = vec![1, 2, 3, 4, 5, 6, 7, 8];
        let mask = filter_eq_i64(&data, 3);
        assert_eq!(mask, vec![false, false, true, false, false, false, false, false]);
    }

    #[test]
    fn test_filter_gt() {
        let data = vec![1, 2, 3, 4, 5, 6, 7, 8];
        let mask = filter_cmp_i64(&data, ">", 5);
        assert_eq!(mask, vec![false, false, false, false, false, true, true, true]);
    }

    #[test]
    fn test_filter_range() {
        let data = vec![1, 5, 10, 15, 20, 25, 30, 35];
        let mask = filter_range_i64(&data, 10, 25);
        assert_eq!(mask, vec![false, false, true, true, true, true, false, false]);
    }

    #[test]
    fn test_filter_neq() {
        let data = vec![1, 2, 3, 4];
        let mask = filter_cmp_i64(&data, "!=", 2);
        assert_eq!(mask, vec![true, false, true, true]);
    }

    #[test]
    fn test_filter_odd_length() {
        // Test with non-multiple-of-4 length
        let data = vec![1, 2, 3, 4, 5];
        let mask = filter_eq_i64(&data, 3);
        assert_eq!(mask, vec![false, false, true, false, false]);
    }
}
