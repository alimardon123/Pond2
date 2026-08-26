// Pond Core — pure-Rust PND2 codec + C ABI
//
// This crate is the language-agnostic core of Pond's binary storage layer.
//   - Python binds to it via the `pond-python` crate (PyO3 wrapper).
//   - Go, Java, Node, C, C++, Zig, etc. bind to it directly via the C ABI
//     (`extern "C"` functions declared in `pond_core.h`).
//
// DESIGN PRINCIPLES
//   1. Zero external dependencies — so static linking from other languages
//      doesn't pull in transitive Rust crates.
//   2. Pure Rust only — no PyO3, no async runtime, no I/O.
//   3. The C ABI is the universal interop layer.
//   4. All heap allocations across the FFI boundary are explicitly owned by
//      the caller; every `*_free` function documents its contract.
//
// PND2 FORMAT
//   Header (13 bytes):
//     Magic: "PND2" (4B)
//     Version: 2 (1B)
//     Flags: has_stats=0x01, compressed=0x02 (1B)
//     n_rows: u32 LE (4B)
//     n_columns: u16 LE (2B)
//     Compression tag: u8 (0=none, 2=zstd)
//   Inner data (schema + stats + payloads):
//     Schema:  per col: name_len(1B) + name + vtype(1B) + enc(1B)
//     Stats:   per col: has_min(1B) + [min + max] + null_count(4B)
//     Payload: per col: payload_len(4B) + payload_bytes
//
// C ABI SUMMARY (see pond_core.h for full docs)
//   pond_pnd2_decode(blob, len)            -> *mut PondResult
//   pond_result_num_columns(result)        -> usize
//   pond_result_column_name(result, i)     -> *const c_char
//   pond_result_column_vtype(result, i)    -> u8
//   pond_result_column_len(result, i)      -> usize
//   pond_result_column_i64(result, i)      -> *const i64
//   pond_result_column_f64(result, i)      -> *const f64
//   pond_result_column_str(result, ci, ri) -> *const c_char
//   pond_result_free(result)
//   pond_pnd2_encode_i64(vals, n, &blob, &len) -> i32
//   pond_blob_free(blob, len)
//
// MODULE LAYOUT
//   - `constants` — public PND2 format constants (magic, version, flags,
//     value types, encodings).
//   - `parser`    — `PND2Parser`, a zero-copy cursor over PND2 inner data.
//   - `types`     — `PondColumn` + CString helpers used by the decoder.
//   - `decode`    — pure-Rust decoder (`pnd2_decode` + per-encoding helpers).
//   - `encode`    — pure-Rust encoders (single-column + `pnd2_encode_multi`).
//   - `c_abi`     — `extern "C"` wrappers for cross-language FFI.

#![allow(dead_code)]

pub mod c_abi;
pub mod constants;
pub mod decode;
pub mod encode;
pub mod parser;
pub mod types;
pub mod vector;
pub mod search;

// Re-export the public API at the crate root so downstream callers (and the
// existing test module) can keep using `pond_core::pnd2_decode` etc. without
// reaching into submodules.
pub use c_abi::*;
pub use constants::*;
pub use decode::*;
pub use encode::*;
pub use parser::*;
pub use types::*;

// ---------------------------------------------------------------------------
// Tests — pure Rust unit tests for the encode/decode logic
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_decode_i64_roundtrip() {
        let input: Vec<i64> = vec![1, 2, 3, 100, -50, 999999, 0, -1];
        let blob = pnd2_encode_i64(&input);
        assert_eq!(&blob[0..4], PND2_MAGIC);
        assert_eq!(blob[4], PND2_VERSION);

        let cols = pnd2_decode(&blob).expect("decode should succeed");
        assert_eq!(cols.len(), 1);
        assert_eq!(cols[0].name.to_str().unwrap(), "v");
        assert_eq!(cols[0].vtype, VT_INT64);
        assert_eq!(cols[0].n_values, input.len());
        assert_eq!(cols[0].i64_data, input);
    }

    #[test]
    fn test_encode_empty_returns_blob() {
        // Empty input is rejected by the C ABI wrapper, but the pure-Rust
        // function should still produce a valid (empty) blob.
        let blob = pnd2_encode_i64(&[]);
        assert_eq!(&blob[0..4], PND2_MAGIC);
        let cols = pnd2_decode(&blob).expect("decode should succeed");
        assert_eq!(cols.len(), 1);
        assert_eq!(cols[0].n_values, 0);
    }

    #[test]
    fn test_decode_rejects_bad_magic() {
        let garbage = [1u8, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13];
        assert!(pnd2_decode(&garbage).is_err());
    }

    #[test]
    fn test_decode_rejects_zstd() {
        // Construct a minimal blob with compression_tag = ZSTD
        let mut blob = vec![b'P', b'N', b'D', b'2', 2, 0, 0, 0, 0, 0, 0, 0];
        blob.push(COMPRESSION_ZSTD);
        assert!(pnd2_decode(&blob).is_err());
    }

    #[test]
    fn test_encode_decode_f64_roundtrip() {
        let input: Vec<f64> = vec![1.5, 2.5, 3.5, -0.5, 99.99, 0.0, -1.0, 1e10];
        let blob = pnd2_encode_f64(&input);
        assert_eq!(&blob[0..4], PND2_MAGIC);

        let cols = pnd2_decode(&blob).expect("decode should succeed");
        assert_eq!(cols.len(), 1);
        assert_eq!(cols[0].vtype, VT_FLOAT64);
        assert_eq!(cols[0].n_values, input.len());
        assert_eq!(cols[0].f64_data, input);
    }

    #[test]
    fn test_encode_decode_str_roundtrip() {
        let input: Vec<&str> = vec!["alice", "bob", "carol", "dave", ""];
        let blob = pnd2_encode_str(&input);
        assert_eq!(&blob[0..4], PND2_MAGIC);

        let cols = pnd2_decode(&blob).expect("decode should succeed");
        assert_eq!(cols.len(), 1);
        assert_eq!(cols[0].vtype, VT_STRING);
        assert_eq!(cols[0].n_values, input.len());
        for (i, expected) in input.iter().enumerate() {
            assert_eq!(cols[0].str_data[i].to_str().unwrap(), *expected, "string at index {} mismatch", i);
        }
    }

    #[test]
    fn test_decode_raw_int64_with_stats() {
        // Encode a column with stats, verify stats are skipped correctly
        let input: Vec<i64> = vec![10, 20, 30];
        let blob = pnd2_encode_i64(&input);
        let cols = pnd2_decode(&blob).expect("decode should succeed");
        assert_eq!(cols[0].i64_data, input);
    }

    #[test]
    fn test_decode_handles_empty_string_payload() {
        // Build a PND2 blob with one empty STRING column (zero-length payload).
        // This is the structure the Python encoder may produce for an empty
        // string column.
        let mut inner = Vec::new();
        // Schema: 1 col "v" STRING RAW
        inner.extend_from_slice(&[1]);
        inner.extend_from_slice(b"v");
        inner.extend_from_slice(&[VT_STRING, ENC_RAW]);
        // Stats: no min/max, null_count=0
        inner.push(0);
        inner.extend_from_slice(&0u32.to_le_bytes());
        // Payload: length 0
        inner.extend_from_slice(&0u32.to_le_bytes());

        let mut blob = Vec::with_capacity(13 + inner.len());
        blob.extend_from_slice(PND2_MAGIC);
        blob.push(PND2_VERSION);
        blob.push(FLAG_HAS_STATS);
        blob.extend_from_slice(&0u32.to_le_bytes()); // n_rows = 0
        blob.extend_from_slice(&1u16.to_le_bytes());
        blob.push(COMPRESSION_NONE);
        blob.extend_from_slice(&inner);

        let cols = pnd2_decode(&blob).expect("decode should succeed");
        assert_eq!(cols.len(), 1);
        assert_eq!(cols[0].vtype, VT_STRING);
        assert_eq!(cols[0].n_values, 0);
    }

    #[test]
    fn test_encode_multi_roundtrip() {
        // Build a 3-column blob (INT64 + FLOAT64 + STRING) using pnd2_encode_multi
        let n_rows = 4usize;

        // Column 1: INT64 "id"
        let id_vals: Vec<i64> = vec![10, 20, 30, 40];
        let mut id_payload = Vec::with_capacity(1 + n_rows * 8);
        id_payload.push(VT_INT64);
        for v in &id_vals { id_payload.extend_from_slice(&v.to_le_bytes()); }
        let id_min = *id_vals.iter().min().unwrap();
        let id_max = *id_vals.iter().max().unwrap();
        let id_min_bytes = id_min.to_le_bytes();
        let id_max_bytes = id_max.to_le_bytes();

        // Column 2: FLOAT64 "score"
        let score_vals: Vec<f64> = vec![1.5, 2.5, 3.5, 4.5];
        let mut score_payload = Vec::with_capacity(1 + n_rows * 8);
        score_payload.push(VT_FLOAT64);
        for v in &score_vals { score_payload.extend_from_slice(&v.to_le_bytes()); }
        let score_min = score_vals.iter().cloned().fold(f64::INFINITY, f64::min);
        let score_max = score_vals.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
        let score_min_bytes = score_min.to_le_bytes();
        let score_max_bytes = score_max.to_le_bytes();

        // Column 3: STRING "name" (no stats)
        let name_vals: Vec<&str> = vec!["alice", "bob", "carol", "dave"];
        let mut name_payload = Vec::new();
        name_payload.push(VT_STRING);
        for v in &name_vals {
            let vb = v.as_bytes();
            name_payload.extend_from_slice(&(vb.len() as u32).to_le_bytes());
            name_payload.extend_from_slice(vb);
        }

        let cols = vec![
            EncodeMultiColumn {
                name: "id",
                vtype: VT_INT64,
                payload: &id_payload,
                stats: Some((id_min_bytes.as_slice(),
                            id_max_bytes.as_slice(), 0)),
            },
            EncodeMultiColumn {
                name: "score",
                vtype: VT_FLOAT64,
                payload: &score_payload,
                stats: Some((score_min_bytes.as_slice(),
                            score_max_bytes.as_slice(), 0)),
            },
            EncodeMultiColumn {
                name: "name",
                vtype: VT_STRING,
                payload: &name_payload,
                stats: None,
            },
        ];

        let blob = pnd2_encode_multi(&cols, n_rows);
        assert_eq!(&blob[0..4], PND2_MAGIC);

        // Decode and verify
        let decoded = pnd2_decode(&blob).expect("decode should succeed");
        assert_eq!(decoded.len(), 3);

        assert_eq!(decoded[0].name.to_str().unwrap(), "id");
        assert_eq!(decoded[0].vtype, VT_INT64);
        assert_eq!(decoded[0].n_values, n_rows);
        assert_eq!(decoded[0].i64_data, id_vals);

        assert_eq!(decoded[1].name.to_str().unwrap(), "score");
        assert_eq!(decoded[1].vtype, VT_FLOAT64);
        assert_eq!(decoded[1].n_values, n_rows);
        assert_eq!(decoded[1].f64_data, score_vals);

        assert_eq!(decoded[2].name.to_str().unwrap(), "name");
        assert_eq!(decoded[2].vtype, VT_STRING);
        assert_eq!(decoded[2].n_values, n_rows);
        for (i, expected) in name_vals.iter().enumerate() {
            assert_eq!(decoded[2].str_data[i].to_str().unwrap(), *expected);
        }
    }

    #[test]
    fn test_decode_projection_pushdown() {
        use std::collections::HashSet;

        // Build a 3-column blob (id, score, name)
        let n_rows = 4usize;
        let id_vals: Vec<i64> = vec![10, 20, 30, 40];
        let mut id_payload = Vec::with_capacity(1 + n_rows * 8);
        id_payload.push(VT_INT64);
        for v in &id_vals { id_payload.extend_from_slice(&v.to_le_bytes()); }

        let score_vals: Vec<f64> = vec![1.5, 2.5, 3.5, 4.5];
        let mut score_payload = Vec::with_capacity(1 + n_rows * 8);
        score_payload.push(VT_FLOAT64);
        for v in &score_vals { score_payload.extend_from_slice(&v.to_le_bytes()); }

        let name_vals: Vec<&str> = vec!["alice", "bob", "carol", "dave"];
        let mut name_payload = Vec::new();
        name_payload.push(VT_STRING);
        for v in &name_vals {
            let vb = v.as_bytes();
            name_payload.extend_from_slice(&(vb.len() as u32).to_le_bytes());
            name_payload.extend_from_slice(vb);
        }

        let cols = vec![
            EncodeMultiColumn { name: "id", vtype: VT_INT64, payload: &id_payload, stats: None },
            EncodeMultiColumn { name: "score", vtype: VT_FLOAT64, payload: &score_payload, stats: None },
            EncodeMultiColumn { name: "name", vtype: VT_STRING, payload: &name_payload, stats: None },
        ];
        let blob = pnd2_encode_multi(&cols, n_rows);

        // Project only "id" — should skip score + name decode entirely
        let mut proj: HashSet<&str> = HashSet::new();
        proj.insert("id");
        let decoded = pnd2_decode_projected(&blob, Some(&proj)).unwrap();
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].name.to_str().unwrap(), "id");
        assert_eq!(decoded[0].i64_data, id_vals);

        // Project "name" + "score" — should skip id decode
        let mut proj2: HashSet<&str> = HashSet::new();
        proj2.insert("name");
        proj2.insert("score");
        let decoded2 = pnd2_decode_projected(&blob, Some(&proj2)).unwrap();
        assert_eq!(decoded2.len(), 2);
        assert_eq!(decoded2[0].name.to_str().unwrap(), "score");
        assert_eq!(decoded2[1].name.to_str().unwrap(), "name");

        // None projection = decode all (same as pnd2_decode)
        let decoded_all = pnd2_decode_projected(&blob, None).unwrap();
        assert_eq!(decoded_all.len(), 3);

        // Nonexistent column = empty result
        let mut proj3: HashSet<&str> = HashSet::new();
        proj3.insert("nonexistent");
        let decoded3 = pnd2_decode_projected(&blob, Some(&proj3)).unwrap();
        assert_eq!(decoded3.len(), 0);
    }
}
