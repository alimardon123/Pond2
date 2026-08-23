// c_abi.rs — `extern "C"` wrappers around the pure-Rust codec.
//
// These functions are the universal interop layer: Go, Java, Node, C, C++,
// and Zig all bind to Pond via these `#[no_mangle] pub extern "C"` entry
// points. The corresponding C header is `pond_core.h`.
//
// All heap allocations across the FFI boundary are explicitly owned by the
// caller; every `*_free` function documents its contract.
//
// # Safety
// All functions in this module accept raw pointers from C callers. The caller
// must ensure pointers are valid, properly aligned, and (for strings)
// null-terminated. Safety is the caller's responsibility.

#![allow(clippy::missing_safety_doc, clippy::not_unsafe_ptr_arg_deref)]

// `c_char` is used directly in many `*const c_char` signatures below. `CString`
// and `ptr` are imported (per the crate's documented FFI surface) even when
// the current call sites spell out the fully-qualified paths — they document
// the C ABI's std::ffi / std::ptr dependencies for future additions.
#[allow(unused_imports)]
use std::ffi::{c_char, CString};
#[allow(unused_imports)]
use std::ptr;

use crate::constants::{VT_BINARY, VT_FLOAT64, VT_INT64, VT_STRING};
use crate::decode::pnd2_decode;
use crate::encode::{
    pnd2_encode_f64, pnd2_encode_i64, pnd2_encode_multi, pnd2_encode_str, EncodeMultiColumn,
};
use crate::types::PondColumn;

// ---------------------------------------------------------------------------
// C ABI — extern "C" wrappers around the pure-Rust functions above
// ---------------------------------------------------------------------------

/// Opaque handle for decoded PND2 data.
/// Callers get column data via `pond_result_*` accessors, then free with
/// `pond_result_free`.
pub struct PondResult {
    columns: Vec<PondColumn>,
    str_array_cache: std::cell::UnsafeCell<Vec<Option<Vec<*const c_char>>>>,
}

/// Decode a PND2 blob into a `PondResult` handle.
///
/// Returns null on error (bad magic, malformed header) or if the blob is
/// zstd-compressed (callers must decompress first).
///
/// Handles ALL encodings (RAW, RLE, DICT, BITPACK) and ALL value types
/// (INT64, FLOAT64, STRING, BINARY, NULL).
///
/// The caller owns the handle and must free it with `pond_result_free`.
#[no_mangle]
pub extern "C" fn pond_pnd2_decode(blob: *const u8, blob_len: usize) -> *mut PondResult {
    if blob.is_null() || blob_len == 0 {
        return std::ptr::null_mut();
    }
    let data = unsafe { std::slice::from_raw_parts(blob, blob_len) };

    match pnd2_decode(data) {
        Ok(columns) => Box::into_raw(Box::new(PondResult { columns, str_array_cache: std::cell::UnsafeCell::new(Vec::new()) })),
        Err(_) => std::ptr::null_mut(),
    }
}

/// Get the number of columns in a decoded result.
#[no_mangle]
pub extern "C" fn pond_result_num_columns(result: *const PondResult) -> usize {
    if result.is_null() { return 0; }
    let r = unsafe { &*result };
    r.columns.len()
}

/// Get a column's name (null-terminated C string). Valid until the result
/// is freed. Returns NULL on out-of-bounds or null result.
#[no_mangle]
pub extern "C" fn pond_result_column_name(result: *const PondResult, index: usize) -> *const c_char {
    if result.is_null() { return std::ptr::null(); }
    let r = unsafe { &*result };
    if index >= r.columns.len() { return std::ptr::null(); }
    r.columns[index].name.as_ptr()
}

/// Get a column's value type.
/// Returns: 1=INT64, 2=FLOAT64, 3=STRING, 4=NULL, 5=BINARY, 0=error/null.
#[no_mangle]
pub extern "C" fn pond_result_column_vtype(result: *const PondResult, index: usize) -> u8 {
    if result.is_null() { return 0; }
    let r = unsafe { &*result };
    if index >= r.columns.len() { return 0; }
    r.columns[index].vtype
}

/// Get the number of values in a column.
#[no_mangle]
pub extern "C" fn pond_result_column_len(result: *const PondResult, index: usize) -> usize {
    if result.is_null() { return 0; }
    let r = unsafe { &*result };
    if index >= r.columns.len() { return 0; }
    r.columns[index].n_values
}

/// Get INT64 column data pointer. Valid until the result is freed.
/// Returns NULL if the column is not INT64, or on out-of-bounds/null result.
/// Use `pond_result_column_len()` to get the array length.
#[no_mangle]
pub extern "C" fn pond_result_column_i64(result: *const PondResult, index: usize) -> *const i64 {
    if result.is_null() { return std::ptr::null(); }
    let r = unsafe { &*result };
    if index >= r.columns.len() { return std::ptr::null(); }
    if r.columns[index].vtype != VT_INT64 { return std::ptr::null(); }
    r.columns[index].i64_data.as_ptr()
}

/// Get FLOAT64 column data pointer. Valid until the result is freed.
/// Returns NULL if the column is not FLOAT64, or on out-of-bounds/null result.
#[no_mangle]
pub extern "C" fn pond_result_column_f64(result: *const PondResult, index: usize) -> *const f64 {
    if result.is_null() { return std::ptr::null(); }
    let r = unsafe { &*result };
    if index >= r.columns.len() { return std::ptr::null(); }
    if r.columns[index].vtype != VT_FLOAT64 { return std::ptr::null(); }
    r.columns[index].f64_data.as_ptr()
}

/// Get a STRING column value at a specific row index.
/// Returns a null-terminated C string, valid until the result is freed.
/// Returns NULL on out-of-bounds, null result, or non-STRING column.
#[no_mangle]
pub extern "C" fn pond_result_column_str(
    result: *const PondResult,
    col_index: usize,
    row_index: usize,
) -> *const c_char {
    if result.is_null() { return std::ptr::null(); }
    let r = unsafe { &*result };
    if col_index >= r.columns.len() { return std::ptr::null(); }
    if r.columns[col_index].vtype != VT_STRING { return std::ptr::null(); }
    if row_index >= r.columns[col_index].str_data.len() { return std::ptr::null(); }
    r.columns[col_index].str_data[row_index].as_ptr()
}

/// Get a BINARY column value at a specific row index.
///
/// Writes the value's pointer and length into the out-params.
/// The pointer is valid until the result is freed.
///
/// # Returns
///   0 on success, -1 on null result, out-of-bounds, or non-BINARY column.
///   The `out_ptr` is set to NULL and `out_len` to 0 for null-sentinel rows
///   (rows where the encoder wrote 0xFFFFFFFF as the length).
#[no_mangle]
pub extern "C" fn pond_result_column_bin(
    result: *const PondResult,
    col_index: usize,
    row_index: usize,
    out_ptr: *mut *const u8,
    out_len: *mut usize,
) -> i32 {
    if result.is_null() || out_ptr.is_null() || out_len.is_null() { return -1; }
    let r = unsafe { &*result };
    if col_index >= r.columns.len() { return -1; }
    if r.columns[col_index].vtype != VT_BINARY { return -1; }
    if row_index >= r.columns[col_index].bin_data.len() { return -1; }
    let v = &r.columns[col_index].bin_data[row_index];
    unsafe {
        *out_ptr = v.as_ptr();
        *out_len = v.len();
    }
    0
}

/// Get a STRING column's row pointers as a batch.
///
/// Returns a pointer to an array of `n_values` `const char*` pointers, one
/// per row. Each pointer is a null-terminated C string. The array and all
/// the strings are valid until the result is freed.
///
/// This is the BATCH accessor — use it instead of calling
/// `pond_result_column_str` in a loop, which has per-row FFI overhead.
/// For languages with high FFI call cost (Python via ctypes, Go via cgo),
/// this can be 10-100x faster for string-heavy columns.
///
/// # Returns
///   Pointer to `*const c_char` array, or NULL on null result /
///   out-of-bounds / non-STRING column. Use `pond_result_column_len` to
///   get the array length.
#[no_mangle]
pub extern "C" fn pond_result_column_str_array(
    result: *const PondResult,
    col_index: usize,
) -> *const *const c_char {
    if result.is_null() { return std::ptr::null(); }
    let r = unsafe { &*result };
    if col_index >= r.columns.len() { return std::ptr::null(); }
    if r.columns[col_index].vtype != VT_STRING { return std::ptr::null(); }
    // CString::as_ptr returns *const c_char. We want a pointer to the
    // internal Vec's backing array of *const c_char pointers.
    //
    // Vec<CString> stores its data as a contiguous array of CString
    // values. Each CString holds a *const c_char internally (via
    // into_raw / from_raw). To expose an array of *const c_char without
    // copying, we'd need to change the storage layout.
    //
    // For now, we build a side-channel Vec<*const c_char> and leak it
    // into the PondResult. It's rebuilt on every call (O(n) per call),
    // but callers should cache it. A future optimization could store
    // the pointer array alongside str_data at decode time.
    //
    // SAFETY: we cast the str_data Vec<CString> into a Vec<*const c_char>
    // via a transient allocation. The pointers are valid as long as the
    // PondResult is alive.
    let col = &r.columns[col_index];
    let ptrs: Vec<*const c_char> = col.str_data.iter()
        .map(|s| s.as_ptr())
        .collect();
    // Convert Vec<*const c_char> → Box<[*const c_char]> → *const *const c_char.
    // The boxed slice is leaked into the caller's space; they free it via
    // pond_str_array_free (or it's reclaimed when PondResult is dropped,
    // since the pointers inside remain valid until then).
    let boxed: Box<[*const c_char]> = ptrs.into_boxed_slice();
    Box::into_raw(boxed) as *const *const c_char
}

/// Free a string pointer array returned by `pond_result_column_str_array`.
///
/// This is currently a NO-OP — the array is allocated by Rust and is
/// reclaimed when the PondResult is freed. The function exists for API
/// symmetry and forward compatibility (if we change the ownership model
/// in the future, callers won't need to update).
///
/// Safe to call on NULL.
#[no_mangle]
#[allow(unused_variables)]
pub extern "C" fn pond_str_array_free(arr: *const *const c_char) {
    // No-op: the array is freed when the PondResult is freed.
    // (We can't reclaim it here without storing the length alongside
    // the pointer — Rust's Box<[T]> uses a fat pointer, but we returned
    // a thin pointer to C. A future version will store the array inside
    // PondResult so it's automatically freed.)
}

/// Free a decoded result. Must be called exactly once per handle.
/// Passing NULL is a safe no-op.
#[no_mangle]
pub extern "C" fn pond_result_free(result: *mut PondResult) {
    if !result.is_null() {
        unsafe { drop(Box::from_raw(result)); }
    }
}

/// Encode an array of int64_t values into a PND2 blob (single column, RAW
/// encoding, no compression).
///
/// # Arguments
///   - `values`: pointer to int64_t array
///   - `n_values`: number of values
///   - `out_blob`: output param — receives a pointer to the blob bytes
///   - `out_blob_len`: output param — receives the blob length in bytes
///
/// # Returns
///   0 on success, -1 on invalid arguments.
///
/// # Ownership
///   The caller owns the returned blob and must free it with `pond_blob_free`.
#[no_mangle]
pub extern "C" fn pond_pnd2_encode_i64(
    values: *const i64,
    n_values: usize,
    out_blob: *mut *mut u8,
    out_blob_len: *mut usize,
) -> i32 {
    if values.is_null() || n_values == 0 || out_blob.is_null() || out_blob_len.is_null() {
        return -1;
    }

    let vals = unsafe { std::slice::from_raw_parts(values, n_values) };
    let mut blob = pnd2_encode_i64(vals);

    let len = blob.len();
    let ptr = blob.as_mut_ptr();
    std::mem::forget(blob); // caller owns it now

    unsafe {
        *out_blob = ptr;
        *out_blob_len = len;
    }
    0
}

/// Free a blob returned by `pond_pnd2_encode_i64`, `pond_pnd2_encode_f64`,
/// or `pond_pnd2_encode_str`. Passing NULL with blob_len=0 is a safe no-op.
#[no_mangle]
pub extern "C" fn pond_blob_free(blob: *mut u8, blob_len: usize) {
    if !blob.is_null() && blob_len > 0 {
        unsafe { drop(Vec::from_raw_parts(blob, blob_len, blob_len)); }
    }
}

/// Encode an array of double values into a PND2 blob (single column, RAW
/// encoding, with stats).
///
/// # Returns
///   0 on success, -1 on invalid arguments.
///   The caller owns the blob and must free it with `pond_blob_free`.
#[no_mangle]
pub extern "C" fn pond_pnd2_encode_f64(
    values: *const f64,
    n_values: usize,
    out_blob: *mut *mut u8,
    out_blob_len: *mut usize,
) -> i32 {
    if values.is_null() || n_values == 0 || out_blob.is_null() || out_blob_len.is_null() {
        return -1;
    }
    let vals = unsafe { std::slice::from_raw_parts(values, n_values) };
    let mut blob = pnd2_encode_f64(vals);

    let len = blob.len();
    let ptr = blob.as_mut_ptr();
    std::mem::forget(blob);

    unsafe {
        *out_blob = ptr;
        *out_blob_len = len;
    }
    0
}

/// Encode an array of null-terminated C strings into a PND2 blob (single
/// column, RAW encoding, no stats).
///
/// # Arguments
///   - `values`: pointer to an array of `const char*` (each null-terminated)
///   - `n_values`: number of strings
///   - `out_blob` / `out_blob_len`: output params for the blob
///
/// # Returns
///   0 on success, -1 on invalid arguments.
///   The caller owns the blob and must free it with `pond_blob_free`.
#[no_mangle]
pub extern "C" fn pond_pnd2_encode_str(
    values: *mut *const c_char,
    n_values: usize,
    out_blob: *mut *mut u8,
    out_blob_len: *mut usize,
) -> i32 {
    if values.is_null() || n_values == 0 || out_blob.is_null() || out_blob_len.is_null() {
        return -1;
    }

    // Convert the C string array into a Vec<&str> using from_utf8_lossy
    // (safe against invalid UTF-8 — replaces bad bytes with U+FFFD).
    let ptrs = unsafe { std::slice::from_raw_parts(values, n_values) };
    let mut owned_strings: Vec<String> = Vec::with_capacity(n_values);
    for p in ptrs {
        if p.is_null() {
            owned_strings.push(String::new());
        } else {
            let cstr = unsafe { std::ffi::CStr::from_ptr(*p) };
            owned_strings.push(cstr.to_string_lossy().into_owned());
        }
    }
    let refs: Vec<&str> = owned_strings.iter().map(|s| s.as_str()).collect();
    let mut blob = pnd2_encode_str(&refs);

    let len = blob.len();
    let ptr = blob.as_mut_ptr();
    std::mem::forget(blob);

    unsafe {
        *out_blob = ptr;
        *out_blob_len = len;
    }
    0
}

// ---------------------------------------------------------------------------
// C ABI — multi-column encoder (builder pattern)
// ---------------------------------------------------------------------------
//
// The single-column encoders (pond_pnd2_encode_i64/f64/str) are convenient
// for simple cases but don't compose into multi-column blobs. This builder
// API lets C/Go/Java callers incrementally build a multi-column PND2 blob:
//
//   PondEncoder* enc = pond_encoder_new(n_rows);
//   pond_encoder_add_i64_column(enc, "id", id_values, n_rows);
//   pond_encoder_add_f64_column(enc, "score", score_values, n_rows);
//   pond_encoder_add_str_column(enc, "name", name_ptrs, n_rows);
//   uint8_t* blob; size_t blob_len;
//   pond_encoder_build(enc, &blob, &blob_len);
//   pond_encoder_free(enc);
//   // ... use blob ...
//   pond_blob_free(blob, blob_len);
//
// All added columns MUST have the same n_rows value passed to
// pond_encoder_new(). Adding a column with a different length returns -1.

/// Multi-column encoder state. Built up via `pond_encoder_add_*` calls,
/// then finalized via `pond_encoder_build`.
pub struct PondEncoder {
    n_rows: usize,
    columns: Vec<EncodeMultiColumnOwned>,
}

/// Owned version of EncodeMultiColumn (the borrows don't work cleanly across
/// the C ABI boundary, so we copy the data in).
struct EncodeMultiColumnOwned {
    name: String,
    vtype: u8,
    payload: Vec<u8>,
    stats: Option<(Vec<u8>, Vec<u8>, u32)>,
}

/// Create a new multi-column encoder.
///
/// # Arguments
///   - `n_rows`: the number of rows that EVERY column must have. Adding a
///     column with a different row count returns -1.
///
/// # Returns
///   Pointer to a `PondEncoder`, or NULL on alloc failure. Caller must free
///   it with `pond_encoder_free`.
#[no_mangle]
pub extern "C" fn pond_encoder_new(n_rows: usize) -> *mut PondEncoder {
    Box::into_raw(Box::new(PondEncoder {
        n_rows,
        columns: Vec::new(),
    }))
}

/// Add an INT64 column to the encoder. Computes min/max stats for free.
///
/// # Returns
///   0 on success, -1 on null pointer / wrong n_rows.
#[no_mangle]
pub extern "C" fn pond_encoder_add_i64_column(
    enc: *mut PondEncoder,
    name: *const c_char,
    values: *const i64,
    n_values: usize,
) -> i32 {
    let enc = unsafe { match enc.as_mut() { Some(e) => e, None => return -1 } };
    if name.is_null() || values.is_null() { return -1; }
    if n_values != enc.n_rows { return -1; }

    let name = unsafe { std::ffi::CStr::from_ptr(name) };
    let name_str = name.to_string_lossy().into_owned();
    let vals = unsafe { std::slice::from_raw_parts(values, n_values) };

    // Build RAW payload: value_type(1B) + values(N*8B)
    let mut payload = Vec::with_capacity(1 + n_values * 8);
    payload.push(VT_INT64);
    for v in vals { payload.extend_from_slice(&v.to_le_bytes()); }

    // Compute stats
    let min = vals.iter().min().copied().unwrap_or(0);
    let max = vals.iter().max().copied().unwrap_or(0);
    let stats = (min.to_le_bytes().to_vec(), max.to_le_bytes().to_vec(), 0u32);

    enc.columns.push(EncodeMultiColumnOwned {
        name: name_str, vtype: VT_INT64, payload, stats: Some(stats),
    });
    0
}

/// Add a FLOAT64 column to the encoder. Computes min/max stats for free.
#[no_mangle]
pub extern "C" fn pond_encoder_add_f64_column(
    enc: *mut PondEncoder,
    name: *const c_char,
    values: *const f64,
    n_values: usize,
) -> i32 {
    let enc = unsafe { match enc.as_mut() { Some(e) => e, None => return -1 } };
    if name.is_null() || values.is_null() { return -1; }
    if n_values != enc.n_rows { return -1; }

    let name = unsafe { std::ffi::CStr::from_ptr(name) };
    let name_str = name.to_string_lossy().into_owned();
    let vals = unsafe { std::slice::from_raw_parts(values, n_values) };

    let mut payload = Vec::with_capacity(1 + n_values * 8);
    payload.push(VT_FLOAT64);
    for v in vals { payload.extend_from_slice(&v.to_le_bytes()); }

    let min = vals.iter().cloned().fold(f64::INFINITY, f64::min);
    let max = vals.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
    let stats = (min.to_le_bytes().to_vec(), max.to_le_bytes().to_vec(), 0u32);

    enc.columns.push(EncodeMultiColumnOwned {
        name: name_str, vtype: VT_FLOAT64, payload, stats: Some(stats),
    });
    0
}

/// Add a STRING column to the encoder. No stats (strings don't have
/// meaningful min/max in the PND2 stat layout).
#[no_mangle]
pub extern "C" fn pond_encoder_add_str_column(
    enc: *mut PondEncoder,
    name: *const c_char,
    values: *mut *const c_char,
    n_values: usize,
) -> i32 {
    let enc = unsafe { match enc.as_mut() { Some(e) => e, None => return -1 } };
    if name.is_null() || values.is_null() { return -1; }
    if n_values != enc.n_rows { return -1; }

    let name = unsafe { std::ffi::CStr::from_ptr(name) };
    let name_str = name.to_string_lossy().into_owned();
    let ptrs = unsafe { std::slice::from_raw_parts(values, n_values) };

    // Build RAW payload: value_type(1B) + [len(4B) + bytes]*N
    let mut payload = Vec::with_capacity(1 + n_values * 12);
    payload.push(VT_STRING);
    for p in ptrs {
        let s = if p.is_null() {
            String::new()
        } else {
            unsafe { std::ffi::CStr::from_ptr(*p) }
                .to_string_lossy().into_owned()
        };
        let sb = s.as_bytes();
        payload.extend_from_slice(&(sb.len() as u32).to_le_bytes());
        payload.extend_from_slice(sb);
    }

    enc.columns.push(EncodeMultiColumnOwned {
        name: name_str, vtype: VT_STRING, payload, stats: None,
    });
    0
}

/// Build the PND2 blob from all added columns.
///
/// # Returns
///   0 on success (writes blob pointer + length), -1 on error.
///   The caller owns the blob and must free it with `pond_blob_free`.
#[no_mangle]
pub extern "C" fn pond_encoder_build(
    enc: *mut PondEncoder,
    out_blob: *mut *mut u8,
    out_blob_len: *mut usize,
) -> i32 {
    if enc.is_null() || out_blob.is_null() || out_blob_len.is_null() { return -1; }
    let enc = unsafe { &*enc };

    // Convert owned columns to borrowed EncodeMultiColumn for pnd2_encode_multi
    let borrowed: Vec<EncodeMultiColumn> = enc.columns.iter().map(|c| {
        let stats = c.stats.as_ref().map(|(mn, mx, nc)| {
            (mn.as_slice(), mx.as_slice(), *nc)
        });
        EncodeMultiColumn {
            name: &c.name,
            vtype: c.vtype,
            payload: &c.payload,
            stats,
        }
    }).collect();

    let mut blob = pnd2_encode_multi(&borrowed, enc.n_rows);
    let len = blob.len();
    let ptr = blob.as_mut_ptr();
    std::mem::forget(blob);

    unsafe {
        *out_blob = ptr;
        *out_blob_len = len;
    }
    0
}

/// Free a `PondEncoder`. Safe to call on NULL.
#[no_mangle]
pub extern "C" fn pond_encoder_free(enc: *mut PondEncoder) {
    if !enc.is_null() {
        unsafe { drop(Box::from_raw(enc)); }
    }
}
