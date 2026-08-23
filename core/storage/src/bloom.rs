// Bloom filter — compact, dependency-free, no-std-compatible.
//
// Used by PSLB slab footers for per-column value membership testing.
// A bloom filter can definitively prove a value is ABSENT (no false negatives),
// but may yield false positives (report present when it isn't).
//
// Design choices for Pond2:
//   - 10 bits per element, 1% target FP rate (matches Parquet/ICEBERG defaults)
//   - Two hash functions derived from SipHash (deterministic, no external deps)
//   - Serialized as raw bits (ceil(m/8) bytes) — no framing overhead
//   - Per-column bloom filters embedded in slab footer

use std::f64::consts::LN_2;

/// Number of hash functions for a given bits-per-element ratio.
/// k = (m/n) * ln(2), rounded. We cap at 8 to keep CPU cost bounded.
fn optimal_k(bits_per_element: f64) -> u32 {
    let k = (bits_per_element * LN_2).round() as u32;
    k.clamp(1, 8)
}

/// Next power of two ≥ n.
fn next_power_of_two(n: usize) -> usize {
    let mut p = 1usize;
    while p < n { p <<= 1; }
    p
}

/// A simple Bloom filter backed by a Vec<u64> bit vector.
///
/// Uses two independent SipHash-derived hash functions h1, h2 and generates
/// k additional hashes as `h1 + i * h2` (Kirsch-Mitzenmacker optimization).
#[derive(Debug, Clone)]
pub struct BloomFilter {
    /// Bit vector stored as u64 words for fast access.
    bits: Vec<u64>,
    /// Number of bits in the filter (always ≤ bits.len() * 64).
    num_bits: u64,
    /// Number of hash functions.
    k: u32,
    /// Number of elements inserted (tracking, not used for filtering).
    #[allow(dead_code)]
    count: u64,
}

impl BloomFilter {
    /// Create a new Bloom filter sized for `expected_elements` at ~1% FP rate.
    ///
    /// Uses 10 bits per element (m/n = 10 → FP ≈ 0.82% for optimal k).
    /// For 1024 i64 values: 1024 * 10 = 10,240 bits → 16,384 (rounded to pow2) = 2,048 bytes.
    pub fn new(expected_elements: usize) -> Self {
        let bits_per_element = 10.0_f64;
        let num_bits = ((expected_elements as f64 * bits_per_element).ceil()) as u64;
        // Round up to next power of two for fast modular arithmetic via masking.
        let num_bits = next_power_of_two(num_bits as usize) as u64;
        let num_words = num_bits.div_ceil(64) as usize;
        let k = optimal_k(bits_per_element);
        BloomFilter {
            bits: vec![0u64; num_words],
            num_bits,
            k,
            count: 0,
        }
    }

    /// Create a Bloom filter from serialized bit bytes.
    /// `num_bits` is the logical bit count (may not be 64-aligned).
    /// `k` is the number of hash functions used during insertion.
    pub fn from_bytes(bit_bytes: &[u8], num_bits: u64, k: u32) -> Self {
        let num_words = num_bits.div_ceil(64) as usize;
        let mut bits = vec![0u64; num_words];
        // Each byte holds 8 bits. Byte i maps to word (i/8), bits [(i%8)*8 .. (i%8)*8+8).
        for (byte_idx, &byte) in bit_bytes.iter().enumerate() {
            if byte_idx * 8 >= num_bits as usize {
                break;
            }
            let word_idx = byte_idx / 8;
            let bit_offset = (byte_idx % 8) * 8;
            if word_idx < num_words {
                bits[word_idx] |= (byte as u64) << bit_offset;
            }
        }
        BloomFilter { bits, num_bits, k, count: 0 }
    }

    /// Serialize the bit vector to raw bytes (little-endian).
    /// Returns `ceil(num_bits / 8)` bytes.
    pub fn to_bytes(&self) -> Vec<u8> {
        let byte_len = self.num_bits.div_ceil(8) as usize;
        let mut bytes = vec![0u8; byte_len];
        for (i, byte) in bytes.iter_mut().enumerate() {
            let word_idx = i / 8;
            let bit_offset = (i % 8) * 8;
            if word_idx < self.bits.len() {
                *byte = ((self.bits[word_idx] >> bit_offset) & 0xFF) as u8;
            }
        }
        bytes
    }

    /// Number of bits in the filter.
    pub fn num_bits(&self) -> u64 {
        self.num_bits
    }

    /// Number of hash functions.
    pub fn k(&self) -> u32 {
        self.k
    }

    /// Insert a value (hashed via SipHash-1-3 with fixed keys).
    pub fn insert(&mut self, value: &[u8]) {
        let (h1, h2) = sip_hash_pair(value);
        for i in 0..self.k {
            let bit = (h1.wrapping_add((i as u64).wrapping_mul(h2))) & (self.num_bits - 1);
            let word_idx = (bit / 64) as usize;
            let bit_idx = (bit % 64) as u32;
            if word_idx < self.bits.len() {
                self.bits[word_idx] |= 1u64 << bit_idx;
            }
        }
        self.count += 1;
    }

    /// Check if a value MIGHT be in the set.
    /// Returns `false` only if the value is definitively NOT in the set.
    /// Returns `true` if the value might be in the set (could be false positive).
    pub fn might_contain(&self, value: &[u8]) -> bool {
        let (h1, h2) = sip_hash_pair(value);
        for i in 0..self.k {
            let bit = (h1.wrapping_add((i as u64).wrapping_mul(h2))) & (self.num_bits - 1);
            let word_idx = (bit / 64) as usize;
            let bit_idx = (bit % 64) as u32;
            if word_idx >= self.bits.len() {
                return false;
            }
            if (self.bits[word_idx] >> bit_idx) & 1 == 0 {
                return false;
            }
        }
        true
    }

    /// Insert a column name prefix + raw value (used for per-column bloom filters).
    /// The column name prefix prevents cross-column false positives.
    pub fn insert_col_value(&mut self, col_name: &str, value: &[u8]) {
        let mut buf = Vec::with_capacity(col_name.len() + 1 + value.len());
        buf.extend_from_slice(col_name.as_bytes());
        buf.push(0); // NUL separator
        buf.extend_from_slice(value);
        self.insert(&buf);
    }

    /// Check if a column-value pair MIGHT be in the set.
    pub fn might_contain_col_value(&self, col_name: &str, value: &[u8]) -> bool {
        let mut buf = Vec::with_capacity(col_name.len() + 1 + value.len());
        buf.extend_from_slice(col_name.as_bytes());
        buf.push(0);
        buf.extend_from_slice(value);
        self.might_contain(&buf)
    }
}

// ---------------------------------------------------------------------------
// SipHash-1-3 implementation (SipHash-128 for two independent 64-bit hashes)
// ---------------------------------------------------------------------------

/// Fixed SipHash-1-3 keys (deterministic across all Pond2 instances).
/// Using fixed keys means bloom filters are reproducible — same data always
/// produces the same bits. This is critical for content-addressed storage.
const SIP_KEY: [u64; 2] = [0x506f6e645f626c6f, 0x6f6d5f66696c7465]; // "Pond_blo" "om_filte"

/// SipHash round function.
#[inline(always)]
fn sipround(v0: &mut u64, v1: &mut u64, v2: &mut u64, v3: &mut u64) {
    *v0 = v0.wrapping_add(*v1);
    *v1 = v1.rotate_left(13);
    *v1 ^= *v0;
    *v0 = v0.rotate_left(32);
    *v2 = v2.wrapping_add(*v3);
    *v3 = v3.rotate_left(16);
    *v3 ^= *v2;
    *v0 = v0.wrapping_add(*v3);
    *v3 = v3.rotate_left(21);
    *v3 ^= *v0;
    *v2 = v2.wrapping_add(*v1);
    *v1 = v1.rotate_left(17);
    *v1 ^= *v2;
    *v2 = v2.rotate_left(32);
}

/// Compute two independent 64-bit hashes via SipHash-1-3-128.
/// Returns `(h1, h2)` — derived from the SipHash state after finalization.
fn sip_hash_pair(input: &[u8]) -> (u64, u64) {
    let k0 = SIP_KEY[0];
    let k1 = SIP_KEY[1];
    let mut v0 = k0 ^ 0x736f6d6570736575;
    let mut v1 = k1 ^ 0x646f72616e646f6d;
    let mut v2 = k0 ^ 0x6c7967656e657261;
    let mut v3 = k1 ^ 0x7465646279746573;

    let len = input.len();
    let blocks = len / 8;

    // Process full 8-byte blocks (1 round compression = SipHash-1).
    for block_idx in 0..blocks {
        let base = block_idx * 8;
        let mut m = 0u64;
        for j in 0..8u32 {
            m |= (input[base + j as usize] as u64) << (j * 8);
        }
        v3 ^= m;
        sipround(&mut v0, &mut v1, &mut v2, &mut v3);
        v0 ^= m;
    }

    // Process remaining bytes.
    let rem_start = blocks * 8;
    let mut last = (len as u64).wrapping_mul(0xff);
    for (offset, byte) in input[rem_start..].iter().enumerate() {
        last |= (*byte as u64) << (offset * 8);
    }
    v3 ^= last;
    sipround(&mut v0, &mut v1, &mut v2, &mut v3);
    v0 ^= last;

    // Finalization (3 rounds = SipHash-1-3).
    v2 ^= 0xff;
    sipround(&mut v0, &mut v1, &mut v2, &mut v3);
    sipround(&mut v0, &mut v1, &mut v2, &mut v3);
    sipround(&mut v0, &mut v1, &mut v2, &mut v3);

    let h1 = v0 ^ v1 ^ v2 ^ v3;
    let h2 = v1.wrapping_add(v3);

    (h1, h2)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bloom_basic_insert_and_query() {
        let mut bf = BloomFilter::new(100);
        bf.insert(b"hello");
        bf.insert(b"world");

        assert!(bf.might_contain(b"hello"));
        assert!(bf.might_contain(b"world"));
        // "missing" was never inserted — should definitely not be found.
        assert!(!bf.might_contain(b"missing"));
    }

    #[test]
    fn test_bloom_false_positive_rate() {
        // Insert 1000 elements, check 10000 non-members.
        // With 10 bits/element and 1% target FP, we expect ~100 false positives.
        // Allow up to 3% to account for random variance.
        let mut bf = BloomFilter::new(1000);
        for i in 0u32..1000 {
            bf.insert(&i.to_le_bytes());
        }

        let mut false_positives = 0;
        for i in 1000u32..11000 {
            if bf.might_contain(&i.to_le_bytes()) {
                false_positives += 1;
            }
        }

        let fp_rate = false_positives as f64 / 10000.0;
        assert!(fp_rate < 0.03,
            "FP rate {} exceeds 3% ({} false positives out of 10000)",
            fp_rate, false_positives);
    }

    #[test]
    fn test_bloom_col_value_separation() {
        let mut bf = BloomFilter::new(100);
        // Insert value "42" for column "age"
        bf.insert_col_value("age", &42i64.to_le_bytes());
        // Same value "42" for different column "id" should NOT match
        assert!(!bf.might_contain_col_value("id", &42i64.to_le_bytes()));
        // But it SHOULD match for "age"
        assert!(bf.might_contain_col_value("age", &42i64.to_le_bytes()));
    }

    #[test]
    fn test_bloom_serialize_roundtrip() {
        let mut bf = BloomFilter::new(100);
        bf.insert(b"test");
        bf.insert(b"data");

        let bytes = bf.to_bytes();
        let num_bits = bf.num_bits();
        let k = bf.k();

        let bf2 = BloomFilter::from_bytes(&bytes, num_bits, k);
        assert!(bf2.might_contain(b"test"));
        assert!(bf2.might_contain(b"data"));
        assert!(!bf2.might_contain(b"other"));
    }

    #[test]
    fn test_bloom_empty_filter_always_negative() {
        let bf = BloomFilter::new(100);
        assert!(!bf.might_contain(b"anything"));
    }

    #[test]
    fn test_bloom_size_reasonable() {
        // 1024 elements at 10 bits/element = 10,240 bits → 16,384 (pow2) = 2,048 bytes.
        let bf = BloomFilter::new(1024);
        let byte_size = bf.num_bits().div_ceil(8) as usize;
        // Should be ≤ 4 KB per 1024 elements per column.
        assert!(byte_size <= 4096,
            "Bloom filter for 1024 elements is {} bytes (expected ≤ 4096)", byte_size);
    }

    #[test]
    fn test_bloom_deterministic() {
        // Same input must produce the same filter (required for content-addressing).
        let mut bf1 = BloomFilter::new(10);
        let mut bf2 = BloomFilter::new(10);
        bf1.insert(b"abc");
        bf2.insert(b"abc");
        assert_eq!(bf1.to_bytes(), bf2.to_bytes());
    }
}
