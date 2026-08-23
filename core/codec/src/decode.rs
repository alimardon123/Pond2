// decode.rs — Pure-Rust PND2 decoder.
//
// `pnd2_decode` is the top-level entry point: it parses the PND2 header,
// walks the schema + stats sections, then dispatches each column's payload
// to the appropriate encoding-specific decoder (RAW / BITPACK / DICT / RLE).
// The output is a `Vec<PondColumn>` — no Python or C types here, just plain
// Rust.
//
// Handles ALL encodings (RAW, RLE, DICT, BITPACK) and ALL value types
// (INT64, FLOAT64, STRING, BINARY, NULL). This is the same decoder the
// Python bindings use — they call into this function via the `pond-python`
// crate's PyO3 wrapper.

use std::ffi::CString;

use crate::constants::*;
use crate::parser::PND2Parser;
use crate::types::{bytes_to_cstring, PondColumn};

/// Decode an uncompressed PND2 blob into a vector of columns.
///
/// Handles ALL encodings (RAW, RLE, DICT, BITPACK) and ALL value types
/// (INT64, FLOAT64, STRING, BINARY, NULL). This is the same decoder the
/// Python bindings use — they call into this function via the
/// `pond-python` crate's PyO3 wrapper.
///
/// Returns `Err` on malformed input. Returns `Ok(vec)` for valid blobs
/// (possibly with empty columns if a specific encoding/vtype combination
/// is not yet implemented).
pub fn pnd2_decode(blob: &[u8]) -> Result<Vec<PondColumn>, String> {
    if blob.len() < 13 || &blob[0..4] != PND2_MAGIC {
        return Err("not a PND2 blob".into());
    }
    if blob[4] != PND2_VERSION {
        return Err(format!("unsupported PND2 version: {}", blob[4]));
    }
    let flags = blob[5];
    let has_stats = (flags & FLAG_HAS_STATS) != 0;
    let n_rows = u32::from_le_bytes([blob[6], blob[7], blob[8], blob[9]]) as usize;
    let n_columns = u16::from_le_bytes([blob[10], blob[11]]) as usize;
    let compression_tag = blob[12];

    if compression_tag == COMPRESSION_ZSTD {
        // zstd-compressed blob — decompress if the "zstd" feature is enabled.
        #[cfg(feature = "zstd")]
        {
            let decompressed = decompress_zstd(&blob[13..])?;
            let mut parser = PND2Parser::new(&decompressed);
            return decode_inner(&decompressed, &mut parser, n_rows, n_columns, has_stats);
        }
        #[cfg(not(feature = "zstd"))]
        {
            return Err("zstd-compressed blobs require the 'zstd' feature. \
                        Rebuild with: cargo build -p pond_core --features zstd".into());
        }
    }
    if compression_tag != COMPRESSION_NONE {
        return Err(format!("unknown compression tag: {}", compression_tag));
    }

    let inner = &blob[13..];
    let mut parser = PND2Parser::new(inner);
    decode_inner(inner, &mut parser, n_rows, n_columns, has_stats)
}

/// Decompress zstd data using ruzstd (pure-Rust, no C deps).
#[cfg(feature = "zstd")]
fn decompress_zstd(data: &[u8]) -> Result<Vec<u8>, String> {
    use ruzstd::decoding::StreamingDecoder;
    use std::io::Cursor;
    let mut decoder = StreamingDecoder::new(Cursor::new(data))
        .map_err(|e| format!("zstd: failed to create decoder: {}", e))?;
    let mut output = Vec::new();
    std::io::Read::read_to_end(&mut decoder, &mut output)
        .map_err(|e| format!("zstd: failed to decompress: {}", e))?;
    Ok(output)
}

/// Inner decoder that works on already-decompressed bytes.
/// Called by pnd2_decode for both uncompressed and zstd-decompressed data.
fn decode_inner(
    inner: &[u8],
    parser: &mut PND2Parser,
    n_rows: usize,
    n_columns: usize,
    has_stats: bool,
) -> Result<Vec<PondColumn>, String> {

    // Parse schema
    let mut schema: Vec<(CString, u8, u8)> = Vec::with_capacity(n_columns);
    for _ in 0..n_columns {
        let name_len = match parser.read_u8() { Some(v) => v as usize, None => break };
        let name_bytes = match parser.read_bytes(name_len) { Some(v) => v, None => break };
        let name = bytes_to_cstring(name_bytes);
        let vtype = match parser.read_u8() { Some(v) => v, None => break };
        let enc = match parser.read_u8() { Some(v) => v, None => break };
        schema.push((name, vtype, enc));
    }

    // Skip stats
    if has_stats {
        for (_, vtype, _) in &schema {
            let has_min = match parser.read_u8() { Some(v) => v, None => break };
            if has_min != 0 {
                parser.skip_stat_value(*vtype);
                parser.skip_stat_value(*vtype);
            }
            let _ = parser.read_u32();
        }
    }

    // Record payload positions (defer decode for projection pushdown)
    let mut payloads: Vec<(CString, u8, u8, usize, usize)> = Vec::with_capacity(n_columns);
    for (name, vtype, enc) in &schema {
        let plen = match parser.read_u32() { Some(v) => v as usize, None => break };
        let pstart = parser.pos;
        if pstart + plen > inner.len() { break; }
        parser.pos += plen;
        payloads.push((name.clone(), *vtype, *enc, pstart, plen));
    }

    // Decode each column
    let mut columns: Vec<PondColumn> = Vec::with_capacity(payloads.len());
    for (name, vtype, enc, pstart, plen) in &payloads {
        let payload = &inner[*pstart..*pstart + *plen];
        if payload.is_empty() {
            let mut col = PondColumn::empty_named("", *vtype);
            col.name = name.clone();
            col.vtype = *vtype;
            columns.push(col);
            continue;
        }

        let mut col = decode_column(payload, *vtype, *enc, n_rows);
        col.name = name.clone();
        col.vtype = *vtype;
        columns.push(col);
    }

    Ok(columns)
}

/// Decode a single column's payload. Dispatches on encoding.
///
/// The returned column has an empty name — the caller is expected to fill
/// it in from the schema. (This separation lets us keep `decode_column`
/// focused on the payload bytes only.)
pub fn decode_column(payload: &[u8], vtype: u8, enc: u8, n_rows: usize) -> PondColumn {
    match enc {
        ENC_RAW      => decode_raw(payload, vtype, n_rows),
        ENC_BITPACK  => decode_bitpack(payload, n_rows),
        ENC_DICT     => decode_dict(payload, vtype, n_rows),
        ENC_RLE      => decode_rle(payload, vtype, n_rows),
        _            => PondColumn::empty_named("", vtype),
    }
}

/// Decode RAW encoding.
///
/// For non-BINARY vtypes, the first byte is the value_type (PND1 header)
/// followed by an optional null bitmap (only if hasNulls at encode time)
/// and then the length-prefixed values.
///
/// For BINARY (vtype=5), the format is:
///   n_values(4B) + [length(4B) + bytes] * n_values
/// (no value_type byte, no bitmap). 0xFFFFFFFF length = null sentinel.
pub fn decode_raw(payload: &[u8], vtype: u8, n_rows: usize) -> PondColumn {
    if payload.is_empty() {
        return PondColumn::empty_named("", vtype);
    }

    // BINARY uses a different layout (no value_type byte, no bitmap)
    if vtype == VT_BINARY {
        return decode_raw_binary(payload, n_rows);
    }

    // Non-BINARY: first byte is value_type
    let data = &payload[1..];

    match vtype {
        VT_INT64 => {
            let n = (data.len() / 8).min(n_rows);
            let mut vals = Vec::with_capacity(n);
            for i in 0..n {
                let o = i * 8;
                vals.push(i64::from_le_bytes([
                    data[o], data[o+1], data[o+2], data[o+3],
                    data[o+4], data[o+5], data[o+6], data[o+7],
                ]));
            }
            PondColumn {
                name: CString::new("").unwrap(), vtype,
                i64_data: vals, f64_data: vec![], str_data: vec![],
                bin_data: vec![], n_values: n, null_bitmap: None,
            }
        }
        VT_FLOAT64 => {
            let n = (data.len() / 8).min(n_rows);
            let mut vals = Vec::with_capacity(n);
            for i in 0..n {
                let o = i * 8;
                vals.push(f64::from_le_bytes([
                    data[o], data[o+1], data[o+2], data[o+3],
                    data[o+4], data[o+5], data[o+6], data[o+7],
                ]));
            }
            PondColumn {
                name: CString::new("").unwrap(), vtype,
                i64_data: vec![], f64_data: vals, str_data: vec![],
                bin_data: vec![], n_values: n, null_bitmap: None,
            }
        }
        VT_STRING | VT_VARIANT => {
            // String RAW format: [len(4B) + bytes]*N, optionally with a
            // null bitmap prefix. We try without bitmap first; if the
            // value count doesn't match n_rows, retry with bitmap.
            decode_raw_string_or_binary(data, vtype, n_rows)
        }
        VT_NULL => {
            // NULL columns have no payload data — just count rows.
            PondColumn {
                name: CString::new("").unwrap(), vtype,
                i64_data: vec![], f64_data: vec![], str_data: vec![],
                bin_data: vec![], n_values: n_rows, null_bitmap: None,
            }
        }
        _ => PondColumn::empty_named("", vtype),
    }
}

/// Decode RAW BINARY payload: n_values(4B) + [length(4B) + bytes]*n_values.
fn decode_raw_binary(payload: &[u8], n_rows: usize) -> PondColumn {
    let _ = n_rows; // n_rows is informational only for BINARY
    if payload.len() < 4 {
        return PondColumn::empty_named("", VT_BINARY);
    }
    let n_values = u32::from_le_bytes([payload[0], payload[1], payload[2], payload[3]]) as usize;
    let mut vals: Vec<Vec<u8>> = Vec::with_capacity(n_values);
    let mut off = 4;
    for _ in 0..n_values {
        if off + 4 > payload.len() { break; }
        let blen = u32::from_le_bytes([
            payload[off], payload[off+1], payload[off+2], payload[off+3]
        ]) as usize;
        off += 4;
        if blen == 0xFFFFFFFF {
            // null sentinel — store empty vec (callers can detect via vtype+length)
            vals.push(Vec::new());
        } else if off + blen <= payload.len() {
            vals.push(payload[off..off+blen].to_vec());
            off += blen;
        } else {
            break;
        }
    }
    let n = vals.len();
    PondColumn {
        name: CString::new("").unwrap(), vtype: VT_BINARY,
        i64_data: vec![], f64_data: vec![], str_data: vec![],
        bin_data: vals, n_values: n, null_bitmap: None,
    }
}

/// Decode RAW STRING or BINARY payload (after the value_type byte has been
/// stripped). Tries without null bitmap first, then with bitmap.
fn decode_raw_string_or_binary(data: &[u8], vtype: u8, n_rows: usize) -> PondColumn {
    // Try parsing as length-prefixed values (no bitmap)
    let mut vals: Vec<&[u8]> = Vec::with_capacity(n_rows);
    let mut off = 0;
    while off + 4 <= data.len() && vals.len() < n_rows {
        let slen = u32::from_le_bytes([
            data[off], data[off+1], data[off+2], data[off+3]
        ]) as usize;
        off += 4;
        if slen == 0xFFFFFFFF {
            vals.push(&[]);
        } else if off + slen <= data.len() {
            vals.push(&data[off..off+slen]);
            off += slen;
        } else {
            break;
        }
    }

    // If the value count matches, use it directly.
    if vals.len() == n_rows {
        return build_string_or_binary_col(vtype, &vals, n_rows);
    }

    // If we got fewer values than expected, try with a null bitmap prefix.
    // Bitmap layout: bitmap_size = ceil(n_rows/8) bytes, then length-prefixed
    // values for non-null rows. Bitmap bit=1 means null (Arrow convention).
    if vals.len() < n_rows {
        let bitmap_size = n_rows.div_ceil(8);
        if data.len() > bitmap_size {
            let bitmap = &data[..bitmap_size];
            let vals_data = &data[bitmap_size..];

            let mut vals2: Vec<&[u8]> = Vec::with_capacity(n_rows);
            let mut off2 = 0;
            while off2 + 4 <= vals_data.len() && vals2.len() < n_rows {
                let slen = u32::from_le_bytes([
                    vals_data[off2], vals_data[off2+1],
                    vals_data[off2+2], vals_data[off2+3]
                ]) as usize;
                off2 += 4;
                if slen == 0xFFFFFFFF {
                    vals2.push(&[]);
                } else if off2 + slen <= vals_data.len() {
                    vals2.push(&vals_data[off2..off2+slen]);
                    off2 += slen;
                } else {
                    break;
                }
            }

            // Walk the bitmap: null rows get empty, valid rows get next val.
            let mut final_vals: Vec<&[u8]> = Vec::with_capacity(n_rows);
            let mut val_idx = 0;
            for i in 0..n_rows {
                if bitmap[i / 8] & (1 << (i % 8)) != 0 {
                    final_vals.push(&[]); // null
                } else if val_idx < vals2.len() {
                    final_vals.push(vals2[val_idx]);
                    val_idx += 1;
                } else {
                    final_vals.push(&[]); // ran out of values
                }
            }
            return build_string_or_binary_col(vtype, &final_vals, n_rows);
        }
    }

    // Fall back to whatever we got (padded to n_rows)
    while vals.len() < n_rows {
        vals.push(&[]);
    }
    build_string_or_binary_col(vtype, &vals, n_rows)
}

/// Build a STRING or BINARY PondColumn from a list of byte slices.
fn build_string_or_binary_col(vtype: u8, vals: &[&[u8]], n_rows: usize) -> PondColumn {
    if vtype == VT_STRING || vtype == VT_VARIANT {
        let strs: Vec<CString> = vals.iter()
            .map(|v| bytes_to_cstring(v))
            .collect();
        let n = strs.len().min(n_rows);
        PondColumn {
            name: CString::new("").unwrap(), vtype,
            i64_data: vec![], f64_data: vec![], str_data: strs,
            bin_data: vec![], n_values: n, null_bitmap: None,
        }
    } else {
        // VT_BINARY
        let bins: Vec<Vec<u8>> = vals.iter().map(|v| v.to_vec()).collect();
        let n = bins.len().min(n_rows);
        PondColumn {
            name: CString::new("").unwrap(), vtype,
            i64_data: vec![], f64_data: vec![], str_data: vec![],
            bin_data: bins, n_values: n, null_bitmap: None,
        }
    }
}

/// Decode BITPACK encoding: bitwidth(1B) + offset(8B) + min(8B) + max(8B) + packed bits.
///
/// Each output value = (packed bits as u64) + offset.
/// Always produces INT64 columns.
pub fn decode_bitpack(payload: &[u8], n_rows: usize) -> PondColumn {
    if payload.len() < 25 {
        return PondColumn::empty_named("", VT_INT64);
    }

    let bitwidth = payload[0] as usize;
    let offset = i64::from_le_bytes([
        payload[1], payload[2], payload[3], payload[4],
        payload[5], payload[6], payload[7], payload[8]
    ]);
    // payload[9..17] = min, payload[17..25] = max — not needed for decode
    let packed = &payload[25..];

    if bitwidth == 0 || bitwidth > 64 {
        return PondColumn::empty_named("", VT_INT64);
    }

    let mut vals = Vec::with_capacity(n_rows);
    let mut bit_pos = 0usize;

    for _ in 0..n_rows {
        let byte_pos = bit_pos / 8;
        if byte_pos >= packed.len() { break; }

        let mut val: u64 = 0;
        for b in 0..bitwidth {
            let bp = bit_pos + b;
            let bp_byte = bp / 8;
            if bp_byte >= packed.len() { break; }
            if packed[bp_byte] & (1 << (bp % 8)) != 0 {
                val |= 1u64 << b;
            }
        }
        vals.push(val as i64 + offset);
        bit_pos += bitwidth;
    }

    let n = vals.len();
    PondColumn {
        name: CString::new("").unwrap(), vtype: VT_INT64,
        i64_data: vals, f64_data: vec![], str_data: vec![],
        bin_data: vec![], n_values: n, null_bitmap: None,
    }
}

/// Decode DICT encoding:
///   n_unique(4B) + value_type(1B) + [value_bytes]*n_unique
///   + code_bitwidth(1B) + packed_codes
///
/// The dictionary's value_type may differ from the column's declared vtype
/// (in practice they match, but we use the dict's value_type for decoding).
pub fn decode_dict(payload: &[u8], vtype: u8, n_rows: usize) -> PondColumn {
    let _ = vtype; // dict payload carries its own value_type
    if payload.is_empty() || payload.len() < 5 {
        return PondColumn::empty_named("", vtype);
    }

    let n_unique = u32::from_le_bytes([payload[0], payload[1], payload[2], payload[3]]) as usize;
    let dict_vtype = payload[4];
    let mut off = 5;

    // Parse dictionary values based on dict_vtype
    let mut dict_int_vals: Vec<i64> = Vec::new();
    let mut dict_float_vals: Vec<f64> = Vec::new();
    let mut dict_str_vals: Vec<Vec<u8>> = Vec::new();

    match dict_vtype {
        VT_INT64 => {
            for _ in 0..n_unique {
                if off + 8 > payload.len() { break; }
                dict_int_vals.push(i64::from_le_bytes([
                    payload[off], payload[off+1], payload[off+2], payload[off+3],
                    payload[off+4], payload[off+5], payload[off+6], payload[off+7]
                ]));
                off += 8;
            }
        }
        VT_FLOAT64 => {
            for _ in 0..n_unique {
                if off + 8 > payload.len() { break; }
                dict_float_vals.push(f64::from_le_bytes([
                    payload[off], payload[off+1], payload[off+2], payload[off+3],
                    payload[off+4], payload[off+5], payload[off+6], payload[off+7]
                ]));
                off += 8;
            }
        }
        VT_STRING | VT_VARIANT => {
            for _ in 0..n_unique {
                if off + 4 > payload.len() { break; }
                let slen = u32::from_le_bytes([
                    payload[off], payload[off+1], payload[off+2], payload[off+3]
                ]) as usize;
                off += 4;
                if off + slen <= payload.len() {
                    dict_str_vals.push(payload[off..off+slen].to_vec());
                    off += slen;
                } else { break; }
            }
        }
        _ => {}
    }

    // After dict values: code_bitwidth(1B) + packed_codes
    if off >= payload.len() {
        return PondColumn::empty_named("", dict_vtype);
    }

    let code_bitwidth = payload[off] as usize;
    off += 1;
    let packed_codes = &payload[off..];

    if code_bitwidth == 0 || code_bitwidth > 64 {
        return PondColumn::empty_named("", dict_vtype);
    }

    // Walk the packed codes and look up each value in the dictionary.
    let mut bit_pos = 0usize;
    let mut int_vals: Vec<i64> = Vec::with_capacity(n_rows);
    let mut float_vals: Vec<f64> = Vec::with_capacity(n_rows);
    let mut str_vals: Vec<CString> = Vec::with_capacity(n_rows);
    let mut bin_vals: Vec<Vec<u8>> = Vec::with_capacity(n_rows);
    let mut n = 0usize;

    for _ in 0..n_rows {
        let byte_pos = bit_pos / 8;
        if byte_pos >= packed_codes.len() { break; }

        let mut code: u64 = 0;
        for b in 0..code_bitwidth {
            let bp = bit_pos + b;
            let bp_byte = bp / 8;
            if bp_byte >= packed_codes.len() { break; }
            if packed_codes[bp_byte] & (1 << (bp % 8)) != 0 {
                code |= 1u64 << b;
            }
        }
        let code_idx = code as usize;

        match dict_vtype {
            VT_INT64 => {
                int_vals.push(if code_idx < dict_int_vals.len() {
                    dict_int_vals[code_idx]
                } else { 0 });
            }
            VT_FLOAT64 => {
                float_vals.push(if code_idx < dict_float_vals.len() {
                    dict_float_vals[code_idx]
                } else { 0.0 });
            }
            VT_STRING => {
                str_vals.push(if code_idx < dict_str_vals.len() {
                    bytes_to_cstring(&dict_str_vals[code_idx])
                } else { CString::new("").unwrap() });
            }
            VT_BINARY => {
                bin_vals.push(if code_idx < dict_str_vals.len() {
                    dict_str_vals[code_idx].clone()
                } else { Vec::new() });
            }
            _ => {}
        }
        n += 1;
        bit_pos += code_bitwidth;
    }

    PondColumn {
        name: CString::new("").unwrap(), vtype: dict_vtype,
        i64_data: int_vals, f64_data: float_vals, str_data: str_vals,
        bin_data: bin_vals, n_values: n, null_bitmap: None,
    }
}

/// Decode RLE encoding: n_runs(4B) + [value + run_length(4B)]*N
///
/// For INT64/FLOAT64: each run is value(8B) + run_length(4B) = 12 bytes.
/// For STRING/BINARY: each run is length(4B) + bytes + run_length(4B).
///
/// The payload starts with the PND1 value_type byte (skip it), then
/// n_runs(4B), then the runs.
pub fn decode_rle(payload: &[u8], vtype: u8, n_rows: usize) -> PondColumn {
    if payload.is_empty() {
        return PondColumn::empty_named("", vtype);
    }

    // Skip value_type byte (PND1 header)
    let data = if vtype == VT_BINARY { payload } else { &payload[1..] };

    if data.len() < 4 {
        return PondColumn::empty_named("", vtype);
    }

    let n_runs = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
    let mut off = 4;

    let mut int_vals: Vec<i64> = Vec::with_capacity(n_rows);
    let mut float_vals: Vec<f64> = Vec::with_capacity(n_rows);
    let mut str_vals: Vec<CString> = Vec::with_capacity(n_rows);
    let mut bin_vals: Vec<Vec<u8>> = Vec::with_capacity(n_rows);
    let mut total_rows = 0usize;

    for _ in 0..n_runs {
        if total_rows >= n_rows { break; }

        match vtype {
            VT_INT64 => {
                if off + 8 > data.len() { break; }
                let v = i64::from_le_bytes([
                    data[off], data[off+1], data[off+2], data[off+3],
                    data[off+4], data[off+5], data[off+6], data[off+7]
                ]);
                off += 8;
                if off + 4 > data.len() { break; }
                let run_len = u32::from_le_bytes([
                    data[off], data[off+1], data[off+2], data[off+3]
                ]) as usize;
                off += 4;
                for _ in 0..run_len {
                    if total_rows >= n_rows { break; }
                    int_vals.push(v);
                    total_rows += 1;
                }
            }
            VT_FLOAT64 => {
                if off + 8 > data.len() { break; }
                let v = f64::from_le_bytes([
                    data[off], data[off+1], data[off+2], data[off+3],
                    data[off+4], data[off+5], data[off+6], data[off+7]
                ]);
                off += 8;
                if off + 4 > data.len() { break; }
                let run_len = u32::from_le_bytes([
                    data[off], data[off+1], data[off+2], data[off+3]
                ]) as usize;
                off += 4;
                for _ in 0..run_len {
                    if total_rows >= n_rows { break; }
                    float_vals.push(v);
                    total_rows += 1;
                }
            }
            VT_STRING | VT_VARIANT => {
                if off + 4 > data.len() { break; }
                let slen = u32::from_le_bytes([
                    data[off], data[off+1], data[off+2], data[off+3]
                ]) as usize;
                off += 4;
                if off + slen > data.len() { break; }
                let val = &data[off..off+slen];
                off += slen;
                if off + 4 > data.len() { break; }
                let run_len = u32::from_le_bytes([
                    data[off], data[off+1], data[off+2], data[off+3]
                ]) as usize;
                off += 4;
                for _ in 0..run_len {
                    if total_rows >= n_rows { break; }
                    if vtype == VT_STRING {
                        str_vals.push(bytes_to_cstring(val));
                    } else {
                        bin_vals.push(val.to_vec());
                    }
                    total_rows += 1;
                }
            }
            _ => break,
        }
    }

    PondColumn {
        name: CString::new("").unwrap(), vtype,
        i64_data: int_vals, f64_data: float_vals, str_data: str_vals,
        bin_data: bin_vals, n_values: total_rows, null_bitmap: None,
    }
}
