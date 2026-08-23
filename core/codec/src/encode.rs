// encode.rs — Pure-Rust PND2 encoders.
//
// Two APIs:
//   - Single-column convenience: `pnd2_encode_i64`, `pnd2_encode_f64`,
//     `pnd2_encode_str` (each emits a 1-column blob with the appropriate
//     stats).
//   - Multi-column low-level: `pnd2_encode_multi` takes a slice of
//     `EncodeMultiColumn` specs (name + vtype + raw payload bytes + optional
//     stats) and assembles the outer PND2 container. This is the foundation
//     for cross-language SDK ports that need to build multi-column PND2
//     blobs without reimplementing the format assembly.

use crate::constants::*;

/// Type alias for an encoded column: (encoding, payload, optional min/max stats).
type EncodedCol = (u8, Vec<u8>, Option<(i64, i64)>);

/// Encode an array of f64 values into an uncompressed PND2 blob.
///
/// Schema: 1 column named "v", type FLOAT64, encoding RAW, with stats.
pub fn pnd2_encode_f64(values: &[f64]) -> Vec<u8> {
    let n_values = values.len();

    let mut inner = Vec::new();

    inner.extend_from_slice(&[1]);
    inner.extend_from_slice(b"v");
    inner.extend_from_slice(&[VT_FLOAT64, ENC_RAW]);

    let min_val = values.iter().cloned().fold(f64::INFINITY, f64::min);
    let max_val = values.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
    inner.push(1);
    inner.extend_from_slice(&min_val.to_le_bytes());
    inner.extend_from_slice(&max_val.to_le_bytes());
    inner.extend_from_slice(&0u32.to_le_bytes());

    let mut payload = Vec::with_capacity(1 + n_values * 8);
    payload.push(VT_FLOAT64);
    for v in values {
        payload.extend_from_slice(&v.to_le_bytes());
    }
    inner.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    inner.extend_from_slice(&payload);

    let mut blob = Vec::with_capacity(13 + inner.len());
    blob.extend_from_slice(PND2_MAGIC);
    blob.push(PND2_VERSION);
    blob.push(FLAG_HAS_STATS);
    blob.extend_from_slice(&(n_values as u32).to_le_bytes());
    blob.extend_from_slice(&1u16.to_le_bytes());
    blob.push(COMPRESSION_NONE);
    blob.extend_from_slice(&inner);
    blob
}

/// Encode a slice of strings into an uncompressed PND2 blob.
///
/// Schema: 1 column named "v", type STRING, encoding RAW, no stats (strings
/// don't have a meaningful min/max in the PND2 stat layout).
pub fn pnd2_encode_str(values: &[&str]) -> Vec<u8> {
    let n_values = values.len();

    let mut inner = Vec::new();

    inner.extend_from_slice(&[1]);
    inner.extend_from_slice(b"v");
    inner.extend_from_slice(&[VT_STRING, ENC_RAW]);

    // No stats for strings (has_min = 0)
    inner.push(0);
    inner.extend_from_slice(&0u32.to_le_bytes()); // null_count

    // Payload: value_type(1B) + [len(4B) + bytes]*N
    let mut payload = Vec::with_capacity(1 + n_values * 12);
    payload.push(VT_STRING);
    for v in values {
        let vb = v.as_bytes();
        payload.extend_from_slice(&(vb.len() as u32).to_le_bytes());
        payload.extend_from_slice(vb);
    }
    inner.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    inner.extend_from_slice(&payload);

    let mut blob = Vec::with_capacity(13 + inner.len());
    blob.extend_from_slice(PND2_MAGIC);
    blob.push(PND2_VERSION);
    blob.push(FLAG_HAS_STATS);
    blob.extend_from_slice(&(n_values as u32).to_le_bytes());
    blob.extend_from_slice(&1u16.to_le_bytes());
    blob.push(COMPRESSION_NONE);
    blob.extend_from_slice(&inner);
    blob
}

/// Encode an array of i64 values into a PND2 blob (single column, RAW
/// encoding, no compression).
///
/// Schema: 1 column named "v", type INT64, encoding RAW, with stats.
pub fn pnd2_encode_i64(values: &[i64]) -> Vec<u8> {
    let n_values = values.len();

    let mut inner = Vec::new();

    // Schema: 1 column "v" of type INT64, encoding RAW
    inner.extend_from_slice(&[1]);           // name_len = 1
    inner.extend_from_slice(b"v");           // name
    inner.extend_from_slice(&[VT_INT64, ENC_RAW]);

    // Stats: min/max for INT64
    let min = values.iter().min().copied().unwrap_or(0);
    let max = values.iter().max().copied().unwrap_or(0);
    inner.push(1);                            // has_min
    inner.extend_from_slice(&min.to_le_bytes());
    inner.extend_from_slice(&max.to_le_bytes());
    inner.extend_from_slice(&0u32.to_le_bytes()); // null_count

    // Payload: value_type(1B) + values (8B each)
    let mut payload = Vec::with_capacity(1 + n_values * 8);
    payload.push(VT_INT64);
    for v in values {
        payload.extend_from_slice(&v.to_le_bytes());
    }
    inner.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    inner.extend_from_slice(&payload);

    // Final PND2 blob
    let mut blob = Vec::with_capacity(13 + inner.len());
    blob.extend_from_slice(PND2_MAGIC);
    blob.push(PND2_VERSION);
    blob.push(FLAG_HAS_STATS);
    blob.extend_from_slice(&(n_values as u32).to_le_bytes());
    blob.extend_from_slice(&1u16.to_le_bytes());  // 1 column
    blob.push(COMPRESSION_NONE);
    blob.extend_from_slice(&inner);
    blob
}

// ---------------------------------------------------------------------------
// Pure-Rust encode — multi-column encoder (RAW only, all numeric/string vtypes)
// ---------------------------------------------------------------------------

/// A column spec for `pnd2_encode_multi`. Each column carries its name,
/// value type, and a slice of raw bytes for its payload (which the caller
/// is responsible for laying out in PND2 RAW format).
///
/// This is a low-level API — callers must understand the RAW payload
/// layout for their chosen vtype. For single-column convenience, use
/// `pnd2_encode_i64` / `pnd2_encode_f64` / `pnd2_encode_str`.
pub struct EncodeMultiColumn<'a> {
    pub name: &'a str,
    pub vtype: u8,
    /// RAW payload bytes (NOT including the outer `payload_len` u32 — that's
    /// added by `pnd2_encode_multi`). For VT_INT64/FLOAT64 the payload is
    /// `value_type(1B) + values(N*8B)`. For VT_STRING it's
    /// `value_type(1B) + [len(4B) + bytes]*N`. For VT_BINARY it's
    /// `n_values(4B) + [len(4B) + bytes]*N`.
    pub payload: &'a [u8],
    /// Optional stats: (min_bytes, max_bytes, null_count).
    /// - INT64: 8 bytes each for min/max
    /// - FLOAT64: 8 bytes each
    /// - STRING/BINARY: None (no stats written)
    pub stats: Option<(&'a [u8], &'a [u8], u32)>,
}

/// Encode multiple columns into a single PND2 blob (RAW encoding only,
/// no compression). Each column's payload is provided directly by the
/// caller — this function just assembles the outer PND2 container.
///
/// Returns the assembled blob.
///
/// This is the foundation for cross-language SDK ports that need to build
/// multi-column PND2 blobs without reimplementing the format assembly.
pub fn pnd2_encode_multi(columns: &[EncodeMultiColumn], n_rows: usize) -> Vec<u8> {
    let mut inner = Vec::new();

    // Schema section
    for col in columns {
        let name_bytes = col.name.as_bytes();
        let name_len = name_bytes.len().min(255) as u8;
        inner.push(name_len);
        inner.extend_from_slice(&name_bytes[..name_len as usize]);
        inner.push(col.vtype);
        inner.push(ENC_RAW);
    }

    // Stats section
    for col in columns {
        match &col.stats {
            None => {
                inner.push(0); // has_min = 0
                inner.extend_from_slice(&0u32.to_le_bytes()); // null_count
            }
            Some((min_bytes, max_bytes, null_count)) => {
                inner.push(1); // has_min = 1
                inner.extend_from_slice(min_bytes);
                inner.extend_from_slice(max_bytes);
                inner.extend_from_slice(&null_count.to_le_bytes());
            }
        }
    }

    // Per-column payloads
    for col in columns {
        inner.extend_from_slice(&(col.payload.len() as u32).to_le_bytes());
        inner.extend_from_slice(col.payload);
    }

    // Final PND2 blob
    let mut blob = Vec::with_capacity(13 + inner.len());
    blob.extend_from_slice(PND2_MAGIC);
    blob.push(PND2_VERSION);
    blob.push(FLAG_HAS_STATS);
    blob.extend_from_slice(&(n_rows as u32).to_le_bytes());
    blob.extend_from_slice(&(columns.len() as u16).to_le_bytes());
    blob.push(COMPRESSION_NONE);
    blob.extend_from_slice(&inner);
    blob
}

// ===========================================================================
// RLE, DICT, BITPACK encoders — match the Rust decoder's expected format
// ===========================================================================

/// Encode INT64 values using RLE (Run-Length Encoding).
///
/// Payload format: `value_type(1B) + n_runs(4B) + [value(8B) + run_len(4B)] * n_runs`
///
/// Best for: sorted data, data with many consecutive repeats.
/// Compression: n_runs * 12 bytes vs n_values * 8 bytes.
pub fn encode_rle_i64(values: &[i64]) -> Vec<u8> {
    let mut payload = Vec::new();
    payload.push(VT_INT64); // value_type

    if values.is_empty() {
        payload.extend_from_slice(&0u32.to_le_bytes()); // n_runs = 0
        return payload;
    }

    // Build runs
    let mut runs: Vec<(i64, u32)> = Vec::new();
    let mut current = values[0];
    let mut count = 1u32;
    for &v in &values[1..] {
        if v == current {
            count += 1;
        } else {
            runs.push((current, count));
            current = v;
            count = 1;
        }
    }
    runs.push((current, count));

    payload.extend_from_slice(&(runs.len() as u32).to_le_bytes());
    for (val, run_len) in &runs {
        payload.extend_from_slice(&val.to_le_bytes());
        payload.extend_from_slice(&run_len.to_le_bytes());
    }
    payload
}

/// Encode INT64 values using BITPACK.
///
/// Payload format: `bitwidth(1B) + offset(8B) + min(8B) + max(8B) + packed_bytes`
///
/// Best for: small-range integers (e.g., ages 0-120, status codes 0-5).
/// Compression: ceil(n_rows * bitwidth / 8) bytes vs n_rows * 8 bytes.
pub fn encode_bitpack_i64(values: &[i64]) -> Vec<u8> {
    if values.is_empty() {
        let mut payload = Vec::new();
        payload.push(0u8); // bitwidth = 0
        payload.extend_from_slice(&0i64.to_le_bytes()); // offset
        payload.extend_from_slice(&0i64.to_le_bytes()); // min
        payload.extend_from_slice(&0i64.to_le_bytes()); // max
        return payload;
    }

    let vmin = *values.iter().min().unwrap();
    let vmax = *values.iter().max().unwrap();
    let offset = vmin;
    let range_val = vmax - vmin;

    let bitwidth = if range_val == 0 {
        1u8
    } else {
        let bw = 64 - range_val.leading_zeros() as u8;
        bw.min(64)
    };

    // Pack values
    let n_rows = values.len();
    let total_bits = n_rows * bitwidth as usize;
    let n_bytes = total_bits.div_ceil(8);
    let mut packed = vec![0u8; n_bytes];

    let mut bit_pos = 0usize;
    for &v in values {
        let offset_val = (v - offset) as u64;
        for b in 0..bitwidth as usize {
            if offset_val & (1u64 << b) != 0 {
                let bp = bit_pos + b;
                let byte_idx = bp / 8;
                let bit_idx = bp % 8;
                if byte_idx < packed.len() {
                    packed[byte_idx] |= 1u8 << bit_idx;
                }
            }
        }
        bit_pos += bitwidth as usize;
    }

    let mut payload = Vec::with_capacity(25 + n_bytes);
    payload.push(bitwidth);
    payload.extend_from_slice(&offset.to_le_bytes());
    payload.extend_from_slice(&vmin.to_le_bytes());
    payload.extend_from_slice(&vmax.to_le_bytes());
    payload.extend_from_slice(&packed);
    payload
}

/// Encode INT64 values using DICT (Dictionary Encoding).
///
/// Payload format: `n_unique(4B) + dict_vtype(1B) + [value(8B)]*n_unique + code_bitwidth(1B) + packed_codes`
///
/// Best for: low-cardinality data (e.g., categories, enums).
/// Compression: n_unique * 8 + ceil(n_rows * code_bitwidth / 8) bytes.
pub fn encode_dict_i64(values: &[i64]) -> Vec<u8> {
    let mut payload = Vec::new();

    if values.is_empty() {
        payload.extend_from_slice(&0u32.to_le_bytes()); // n_unique = 0
        payload.push(VT_INT64); // dict_vtype
        payload.push(0u8); // code_bitwidth = 0
        return payload;
    }

    // Build dictionary
    use std::collections::HashMap;
    let mut unique: Vec<i64> = Vec::new();
    let mut code_map: HashMap<i64, u32> = HashMap::new();
    let mut codes: Vec<u32> = Vec::with_capacity(values.len());

    for &v in values {
        if let Some(&code) = code_map.get(&v) {
            codes.push(code);
        } else {
            let code = unique.len() as u32;
            code_map.insert(v, code);
            unique.push(v);
            codes.push(code);
        }
    }

    // Write n_unique + dict_vtype
    payload.extend_from_slice(&(unique.len() as u32).to_le_bytes());
    payload.push(VT_INT64);

    // Write dictionary values
    for val in &unique {
        payload.extend_from_slice(&val.to_le_bytes());
    }

    // Pack codes using bitpacking
    let code_max = *codes.iter().max().unwrap_or(&0);
    let code_bitwidth = if code_max == 0 {
        1u8
    } else {
        let bw = 32 - code_max.leading_zeros() as u8;
        bw.max(1)
    };

    payload.push(code_bitwidth);

    let total_bits = codes.len() * code_bitwidth as usize;
    let n_code_bytes = total_bits.div_ceil(8);
    let mut packed_codes = vec![0u8; n_code_bytes];

    let mut bit_pos = 0usize;
    for &c in &codes {
        for b in 0..code_bitwidth as usize {
            if b < 32 && c & (1u32 << b) != 0 {
                let bp = bit_pos + b;
                let byte_idx = bp / 8;
                let bit_idx = bp % 8;
                if byte_idx < packed_codes.len() {
                    packed_codes[byte_idx] |= 1u8 << bit_idx;
                }
            }
        }
        bit_pos += code_bitwidth as usize;
    }
    payload.extend_from_slice(&packed_codes);
    payload
}

/// Encode FLOAT64 values using RLE.
pub fn encode_rle_f64(values: &[f64]) -> Vec<u8> {
    let mut payload = Vec::new();
    payload.push(VT_FLOAT64);

    if values.is_empty() {
        payload.extend_from_slice(&0u32.to_le_bytes());
        return payload;
    }

    let mut runs: Vec<(f64, u32)> = Vec::new();
    let mut current = values[0];
    let mut count = 1u32;
    for &v in &values[1..] {
        if v == current {
            count += 1;
        } else {
            runs.push((current, count));
            current = v;
            count = 1;
        }
    }
    runs.push((current, count));

    payload.extend_from_slice(&(runs.len() as u32).to_le_bytes());
    for (val, run_len) in &runs {
        payload.extend_from_slice(&val.to_le_bytes());
        payload.extend_from_slice(&run_len.to_le_bytes());
    }
    payload
}

/// Auto-select the best encoding for INT64 values and encode.
///
/// Heuristics (matching Python ColumnEncoding.choose):
///   - Low cardinality (< 10% unique, < 1000 unique) → DICT
///   - Run-heavy (many consecutive repeats) → RLE
///   - Small range (< 2^16) → BITPACK
///   - Default → RAW
pub fn encode_i64_auto(values: &[i64]) -> (u8, Vec<u8>) {
    if values.is_empty() {
        return (ENC_RAW, encode_raw_i64_payload(values));
    }

    let n = values.len();
    use std::collections::HashSet;
    let unique: HashSet<i64> = values.iter().cloned().collect();
    let cardinality = unique.len();

    // Low cardinality → DICT
    let card_ratio = cardinality as f64 / n as f64;
    if card_ratio < 0.1 && cardinality < 1000 {
        return (ENC_DICT, encode_dict_i64(values));
    }

    // Run-heavy → RLE
    if is_run_heavy(values) {
        return (ENC_RLE, encode_rle_i64(values));
    }

    // Small range → BITPACK
    let vmin = *values.iter().min().unwrap();
    let vmax = *values.iter().max().unwrap();
    let range = vmax - vmin;
    if (0..(1i64 << 16)).contains(&range) {
        return (ENC_BITPACK, encode_bitpack_i64(values));
    }

    // Default → RAW
    (ENC_RAW, encode_raw_i64_payload(values))
}

/// Check if values have many consecutive repeats (RLE-friendly).
fn is_run_heavy(values: &[i64]) -> bool {
    if values.len() < 10 {
        return false;
    }
    let sample: &[i64] = if values.len() > 1000 {
        &values[..1000]
    } else {
        values
    };
    let mut runs = 1;
    for i in 1..sample.len() {
        if sample[i] != sample[i - 1] {
            runs += 1;
        }
    }
    // RLE is worth it if runs < 50% of sample size
    runs < sample.len() / 2
}

/// Encode INT64 values as RAW payload (value_type + raw bytes).
fn encode_raw_i64_payload(values: &[i64]) -> Vec<u8> {
    let mut payload = Vec::with_capacity(1 + values.len() * 8);
    payload.push(VT_INT64);
    for v in values {
        payload.extend_from_slice(&v.to_le_bytes());
    }
    payload
}

/// Encode FLOAT64 values as RAW payload (value_type + raw bytes).
fn encode_raw_f64_payload(values: &[f64]) -> Vec<u8> {
    let mut payload = Vec::with_capacity(1 + values.len() * 8);
    payload.push(VT_FLOAT64);
    for v in values {
        payload.extend_from_slice(&v.to_le_bytes());
    }
    payload
}

/// Encode STRING values as RAW payload (value_type + [len + bytes]*N).
fn encode_raw_str_payload(values: &[&str]) -> Vec<u8> {
    let mut payload = Vec::new();
    payload.push(VT_STRING);
    for v in values {
        let vb = v.as_bytes();
        payload.extend_from_slice(&(vb.len() as u32).to_le_bytes());
        payload.extend_from_slice(vb);
    }
    payload
}

// ===========================================================================
// TypedColumn — typed column data for multi-type PND2 encoding
// ===========================================================================

/// A typed column for multi-type PND2 encoding.
///
/// Supports INT64, FLOAT64, STRING, BINARY, and VARIANT value types.
/// VARIANT is for mixed-type columns where each value can be a different type
/// (int, float, string, bool, null, nested JSON) — stored as JSON-encoded strings.
#[derive(Clone, Debug)]
pub enum TypedColumn {
    Int64(Vec<i64>),
    Float64(Vec<f64>),
    String(Vec<String>),
    Binary(Vec<Vec<u8>>),
    Variant(Vec<String>),
    Boolean(Vec<bool>),
    Date(Vec<i64>),
    Timestamp(Vec<i64>),
    Vector(Vec<Vec<f32>>),
}

impl TypedColumn {
    pub fn vtype(&self) -> u8 {
        match self {
            TypedColumn::Int64(_) => VT_INT64,
            TypedColumn::Float64(_) => VT_FLOAT64,
            TypedColumn::String(_) => VT_STRING,
            TypedColumn::Binary(_) => VT_BINARY,
            TypedColumn::Variant(_) => VT_VARIANT,
            TypedColumn::Boolean(_) => VT_BOOLEAN,
            TypedColumn::Date(_) => VT_DATE,
            TypedColumn::Timestamp(_) => VT_TIMESTAMP,
            TypedColumn::Vector(_) => VT_VECTOR,
        }
    }

    pub fn len(&self) -> usize {
        match self {
            TypedColumn::Int64(v) => v.len(),
            TypedColumn::Float64(v) => v.len(),
            TypedColumn::String(v) => v.len(),
            TypedColumn::Binary(v) => v.len(),
            TypedColumn::Variant(v) => v.len(),
            TypedColumn::Boolean(v) => v.len(),
            TypedColumn::Date(v) => v.len(),
            TypedColumn::Timestamp(v) => v.len(),
            TypedColumn::Vector(v) => v.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn encode_payload(&self) -> Vec<u8> {
        match self {
            TypedColumn::Int64(v) => {
                let (_enc, payload) = encode_i64_auto(v);
                payload
            }
            TypedColumn::Float64(v) => encode_raw_f64_payload(v),
            TypedColumn::String(v) => {
                let refs: Vec<&str> = v.iter().map(|s| s.as_str()).collect();
                encode_raw_str_payload(&refs)
            }
            TypedColumn::Binary(v) => {
                let mut p = Vec::new();
                p.extend_from_slice(&(v.len() as u32).to_le_bytes());
                for d in v { p.extend_from_slice(&(d.len() as u32).to_le_bytes()); p.extend_from_slice(d); }
                p
            }
            TypedColumn::Variant(v) => {
                let mut p = Vec::new();
                p.push(VT_VARIANT);
                for s in v { let b = s.as_bytes(); p.extend_from_slice(&(b.len() as u32).to_le_bytes()); p.extend_from_slice(b); }
                p
            }
            TypedColumn::Boolean(v) => {
                let i64s: Vec<i64> = v.iter().map(|&b| if b { 1 } else { 0 }).collect();
                let (_, p) = encode_i64_auto(&i64s); p
            }
            TypedColumn::Date(v) => { let (_, p) = encode_i64_auto(v); p }
            TypedColumn::Timestamp(v) => { let (_, p) = encode_i64_auto(v); p }
            TypedColumn::Vector(v) => {
                let mut p = Vec::new();
                p.push(VT_VECTOR);
                p.extend_from_slice(&(v.len() as u32).to_le_bytes());
                for vec in v {
                    p.extend_from_slice(&(vec.len() as u32).to_le_bytes());
                    for &f in vec { p.extend_from_slice(&f.to_le_bytes()); }
                }
                p
            }
        }
    }

    pub fn encode_encoding(&self) -> u8 {
        match self {
            TypedColumn::Int64(v) => encode_i64_auto(v).0,
            TypedColumn::Float64(_) => ENC_RAW,
            TypedColumn::String(_) => ENC_RAW,
            TypedColumn::Binary(_) => ENC_RAW,
            TypedColumn::Variant(_) => ENC_RAW,
            TypedColumn::Boolean(v) => { let i64s: Vec<i64> = v.iter().map(|&b| if b {1} else {0}).collect(); encode_i64_auto(&i64s).0 }
            TypedColumn::Date(v) => encode_i64_auto(v).0,
            TypedColumn::Timestamp(v) => encode_i64_auto(v).0,
            TypedColumn::Vector(_) => ENC_RAW,
        }
    }

    pub fn min_max_bytes(&self) -> Option<(Vec<u8>, Vec<u8>)> {
        match self {
            TypedColumn::Int64(v) if !v.is_empty() => {
                let min = *v.iter().min().unwrap();
                let max = *v.iter().max().unwrap();
                Some((min.to_le_bytes().to_vec(), max.to_le_bytes().to_vec()))
            }
            TypedColumn::Float64(v) if !v.is_empty() => {
                let min = v.iter().cloned().fold(f64::INFINITY, f64::min);
                let max = v.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
                Some((min.to_le_bytes().to_vec(), max.to_le_bytes().to_vec()))
            }
            TypedColumn::Boolean(v) if !v.is_empty() => {
                let has_true = v.iter().any(|&b| b);
                Some((0i64.to_le_bytes().to_vec(), (if has_true { 1i64 } else { 0i64 }).to_le_bytes().to_vec()))
            }
            TypedColumn::Date(v) if !v.is_empty() => {
                Some(((*v.iter().min().unwrap()).to_le_bytes().to_vec(), (*v.iter().max().unwrap()).to_le_bytes().to_vec()))
            }
            TypedColumn::Timestamp(v) if !v.is_empty() => {
                Some(((*v.iter().min().unwrap()).to_le_bytes().to_vec(), (*v.iter().max().unwrap()).to_le_bytes().to_vec()))
            }
            _ => None,
        }
    }
}

/// Encode a multi-type PND2 blob from typed columns.
///
/// This is the high-level API: pass column names + typed values, get a PND2 blob
/// with per-column encoding selection and stats.
///
/// Example:
/// ```ignore
/// use pond_core::TypedColumn;
/// let blob = pnd2_encode_multi_typed(&[
///     ("id", TypedColumn::Int64(vec![1, 2, 3])),
///     ("score", TypedColumn::Float64(vec![1.5, 2.5, 3.5])),
///     ("name", TypedColumn::String(vec!["a".into(), "b".into(), "c".into()])),
/// ]);
/// ```
pub fn pnd2_encode_multi_typed(columns: &[(&str, TypedColumn)]) -> Vec<u8> {
    let n_rows = columns.first().map(|(_, c)| c.len()).unwrap_or(0);
    let mut inner = Vec::new();

    // Phase 1: Write ALL schemas
    for (name, col) in columns {
        let name_bytes = name.as_bytes();
        inner.push(name_bytes.len() as u8);
        inner.extend_from_slice(name_bytes);
        inner.push(col.vtype());
        inner.push(col.encode_encoding());
    }

    // Phase 2: Write ALL stats
    for (_, col) in columns {
        if let Some((min, max)) = col.min_max_bytes() {
            inner.push(1); // has_min
            inner.extend_from_slice(&min);
            inner.extend_from_slice(&max);
        } else {
            inner.push(0); // no stats
        }
        inner.extend_from_slice(&0u32.to_le_bytes()); // null_count
    }

    // Phase 3: Write ALL payloads
    for (_, col) in columns {
        let payload = col.encode_payload();
        inner.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        inner.extend_from_slice(&payload);
    }

    // Final PND2 blob
    let mut blob = Vec::with_capacity(13 + inner.len());
    blob.extend_from_slice(PND2_MAGIC);
    blob.push(PND2_VERSION);
    blob.push(FLAG_HAS_STATS);
    blob.extend_from_slice(&(n_rows as u32).to_le_bytes());
    blob.extend_from_slice(&(columns.len() as u16).to_le_bytes());
    blob.push(COMPRESSION_NONE);
    blob.extend_from_slice(&inner);
    blob
}

/// Encode a PND2 blob with automatic encoding selection for INT64 columns.
///
/// This is the high-level API: pass column names + i64 values, get a PND2 blob
/// with the best encoding per column.
pub fn pnd2_encode_i64_auto(columns: &[(&str, &[i64])]) -> Vec<u8> {
    let n_rows = columns.first().map(|(_, v)| v.len()).unwrap_or(0);
    let mut inner = Vec::new();

    // Phase 1: Write ALL schemas (name_len + name + vtype + enc per column)
    let mut encoded_cols: Vec<EncodedCol> = Vec::new();
    for (name, values) in columns {
        let name_bytes = name.as_bytes();
        let (enc, payload) = encode_i64_auto(values);
        let stats = if !values.is_empty() {
            let min = *values.iter().min().unwrap();
            let max = *values.iter().max().unwrap();
            Some((min, max))
        } else {
            None
        };
        encoded_cols.push((enc, payload, stats));

        inner.push(name_bytes.len() as u8);
        inner.extend_from_slice(name_bytes);
        inner.push(VT_INT64);
        inner.push(enc);
    }

    // Phase 2: Write ALL stats (has_min + min + max + null_count per column)
    for (_, _, stats) in &encoded_cols {
        if let Some((min, max)) = stats {
            inner.push(1); // has_min
            inner.extend_from_slice(&min.to_le_bytes());
            inner.extend_from_slice(&max.to_le_bytes());
        } else {
            inner.push(0);
        }
        inner.extend_from_slice(&0u32.to_le_bytes()); // null_count
    }

    // Phase 3: Write ALL payload lengths + payloads
    for (_, payload, _) in &encoded_cols {
        inner.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        inner.extend_from_slice(payload);
    }

    // Final PND2 blob
    let mut blob = Vec::with_capacity(13 + inner.len());
    blob.extend_from_slice(PND2_MAGIC);
    blob.push(PND2_VERSION);
    blob.push(FLAG_HAS_STATS);
    blob.extend_from_slice(&(n_rows as u32).to_le_bytes());
    blob.extend_from_slice(&(columns.len() as u16).to_le_bytes());
    blob.push(COMPRESSION_NONE);
    blob.extend_from_slice(&inner);
    blob
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::decode::pnd2_decode;

    #[test]
    fn test_rle_i64_roundtrip() {
        let values = vec![1i64, 1, 1, 2, 2, 3, 3, 3, 3];
        let payload = encode_rle_i64(&values);
        // Wrap in a PND2 blob and decode
        let blob = pnd2_encode_single_col("v", VT_INT64, ENC_RLE, &payload, &values, values.len());
        let cols = pnd2_decode(&blob).unwrap();
        assert_eq!(cols[0].i64_data, values);
    }

    #[test]
    fn test_bitpack_i64_roundtrip() {
        let values: Vec<i64> = (0..100).map(|i| i % 10).collect();
        let payload = encode_bitpack_i64(&values);
        let blob = pnd2_encode_single_col("v", VT_INT64, ENC_BITPACK, &payload, &values, values.len());
        let cols = pnd2_decode(&blob).unwrap();
        assert_eq!(cols[0].i64_data, values);
    }

    #[test]
    fn test_dict_i64_roundtrip() {
        let values = vec![5i64, 3, 5, 1, 3, 5, 1, 1, 3, 5];
        let payload = encode_dict_i64(&values);
        let blob = pnd2_encode_single_col("v", VT_INT64, ENC_DICT, &payload, &values, values.len());
        let cols = pnd2_decode(&blob).unwrap();
        assert_eq!(cols[0].i64_data, values);
    }

    #[test]
    fn test_auto_encode_low_cardinality() {
        // Low cardinality → DICT
        let values: Vec<i64> = (0..1000).map(|i| i % 5).collect();
        let (enc, _) = encode_i64_auto(&values);
        assert_eq!(enc, ENC_DICT);
    }

    #[test]
    fn test_auto_encode_run_heavy() {
        // Run-heavy with HIGH cardinality → RLE (not DICT, since cardinality > 10%)
        // 50 unique values, each repeated 10 times = 500 values, ratio = 50/500 = 0.1
        // Use higher cardinality to avoid DICT
        let values: Vec<i64> = (0..2000).map(|i| i / 5).collect(); // 400 unique, ratio = 400/2000 = 0.2
        let (enc, _) = encode_i64_auto(&values);
        assert_eq!(enc, ENC_RLE);
    }

    #[test]
    fn test_auto_encode_small_range() {
        // Small range → BITPACK
        let values: Vec<i64> = (0..100).map(|i| i % 50).collect();
        let (enc, _) = encode_i64_auto(&values);
        assert_eq!(enc, ENC_BITPACK);
    }

    #[test]
    fn test_auto_encode_large_range() {
        // Large range → RAW
        let values: Vec<i64> = (0..1000).map(|i| i * 1_000_000_000).collect();
        let (enc, _) = encode_i64_auto(&values);
        assert_eq!(enc, ENC_RAW);
    }

    #[test]
    fn test_pnd2_encode_i64_auto_multi() {
        let ids: Vec<i64> = (0..100).collect();
        let cats: Vec<i64> = (0..100).map(|i| i % 3).collect(); // low cardinality
        let blob = pnd2_encode_i64_auto(&[("id", &ids), ("cat", &cats)]);
        let cols = pnd2_decode(&blob).unwrap();
        assert_eq!(cols.len(), 2);
        assert_eq!(cols[0].i64_data, ids);
        assert_eq!(cols[1].i64_data, cats);
    }

    /// Helper: wrap a single column payload in a PND2 blob.
    fn pnd2_encode_single_col(
        name: &str,
        vtype: u8,
        enc: u8,
        payload: &[u8],
        values: &[i64],
        n_rows: usize,
    ) -> Vec<u8> {
        let mut inner = Vec::new();
        let name_bytes = name.as_bytes();

        // Phase 1: Schema
        inner.push(name_bytes.len() as u8);
        inner.extend_from_slice(name_bytes);
        inner.push(vtype);
        inner.push(enc);

        // Phase 2: Stats
        if !values.is_empty() {
            let min = *values.iter().min().unwrap();
            let max = *values.iter().max().unwrap();
            inner.push(1);
            inner.extend_from_slice(&min.to_le_bytes());
            inner.extend_from_slice(&max.to_le_bytes());
        } else {
            inner.push(0);
        }
        inner.extend_from_slice(&0u32.to_le_bytes());

        // Phase 3: Payload
        inner.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        inner.extend_from_slice(payload);

        // PND2 header
        let mut blob = Vec::with_capacity(13 + inner.len());
        blob.extend_from_slice(PND2_MAGIC);
        blob.push(PND2_VERSION);
        blob.push(FLAG_HAS_STATS);
        blob.extend_from_slice(&(n_rows as u32).to_le_bytes());
        blob.extend_from_slice(&1u16.to_le_bytes());
        blob.push(COMPRESSION_NONE);
        blob.extend_from_slice(&inner);
        blob
    }
}
