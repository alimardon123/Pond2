// C ABI — extern "C" wrappers for cross-language SDKs (Go, Java, Node, C)
//
// These functions expose the kernel's 3 primitives (Write, Read, Ref) through
// a C ABI. Any language that can call C functions gets full kernel access.
//
// Memory management:
//   - Strings returned are heap-allocated. Caller MUST free with pond_string_free().
//   - Data returned via out-params is heap-allocated. Caller MUST free with pond_data_free().
//   - Handles must be freed with pond_kernel_free().
//
// # Safety
// All functions in this module accept raw pointers from C callers. The caller
// must ensure that:
//   - Handle pointers are valid (returned by pond_kernel_new, not yet freed).
//   - String/data pointers are valid and null-terminated (for strings) or
//     valid for the specified length (for data pointers).
//   - Out-param pointers are valid and writable.
//   - No data races exist (each handle must not be used concurrently from
//     multiple threads without external synchronization).
//
// We allow clippy::not_unsafe_ptr_arg_deref because these are C FFI functions
// where safety is the caller's responsibility (documented above).

#![allow(clippy::missing_safety_doc, clippy::not_unsafe_ptr_arg_deref)]

use std::ffi::{c_char, CStr, CString};
use std::ptr;

use crate::PondKernel;

// ---------------------------------------------------------------------------
// C ABI — extern "C" wrappers
// ---------------------------------------------------------------------------

pub struct PondKernelHandle {
    kernel: PondKernel,
}

#[no_mangle]
pub extern "C" fn pond_kernel_new(base_dir: *const c_char) -> *mut PondKernelHandle {
    if base_dir.is_null() { return ptr::null_mut(); }
    let dir = match unsafe { CStr::from_ptr(base_dir) }.to_str() {
        Ok(s) => s,
        Err(_) => return ptr::null_mut(),
    };
    match PondKernel::new_local(dir) {
        Ok(kernel) => Box::into_raw(Box::new(PondKernelHandle { kernel })),
        Err(_) => ptr::null_mut(),
    }
}

#[no_mangle]
pub extern "C" fn pond_kernel_free(handle: *mut PondKernelHandle) {
    if !handle.is_null() {
        unsafe { drop(Box::from_raw(handle)); }
    }
}

#[no_mangle]
pub extern "C" fn pond_kernel_write(
    handle: *mut PondKernelHandle, data: *const u8, data_len: usize,
) -> *mut c_char {
    let handle = unsafe { match handle.as_ref() { Some(h) => h, None => return ptr::null_mut() }};
    if data.is_null() { return ptr::null_mut(); }
    let slice = unsafe { std::slice::from_raw_parts(data, data_len) };
    match handle.kernel.write(slice) {
        Ok(hash) => CString::new(hash).map(|cs| cs.into_raw()).unwrap_or(ptr::null_mut()),
        Err(_) => ptr::null_mut(),
    }
}

#[no_mangle]
pub extern "C" fn pond_kernel_read(
    handle: *mut PondKernelHandle, hash_or_name: *const c_char,
    out_data: *mut *const u8, out_len: *mut usize,
) -> i32 {
    let handle = unsafe { match handle.as_ref() { Some(h) => h, None => return -1 }};
    if hash_or_name.is_null() || out_data.is_null() || out_len.is_null() { return -1; }
    let key = match unsafe { CStr::from_ptr(hash_or_name) }.to_str() {
        Ok(s) => s,
        Err(_) => return -1,
    };
    match handle.kernel.read(key) {
        Ok(data) => {
            let boxed = data.into_boxed_slice();
            let ptr = boxed.as_ptr();
            let len = boxed.len();
            std::mem::forget(boxed);
            unsafe { *out_data = ptr; *out_len = len; }
            0
        }
        Err(_) => -1,
    }
}

#[no_mangle]
pub extern "C" fn pond_data_free(data: *mut u8, len: usize) {
    if !data.is_null() && len > 0 {
        unsafe { drop(Vec::from_raw_parts(data, len, len)); }
    }
}

#[no_mangle]
pub extern "C" fn pond_string_free(s: *mut c_char) {
    if !s.is_null() { unsafe { drop(CString::from_raw(s)); } }
}

#[no_mangle]
pub extern "C" fn pond_kernel_reference(
    handle: *mut PondKernelHandle, name: *const c_char, hash: *const c_char,
) -> i32 {
    let handle = unsafe { match handle.as_ref() { Some(h) => h, None => return -1 }};
    if name.is_null() || hash.is_null() { return -1; }
    let name = match unsafe { CStr::from_ptr(name) }.to_str() { Ok(s) => s, Err(_) => return -1 };
    let hash = match unsafe { CStr::from_ptr(hash) }.to_str() { Ok(s) => s, Err(_) => return -1 };
    match handle.kernel.reference(name, hash) { Ok(()) => 0, Err(_) => -1 }
}

#[no_mangle]
pub extern "C" fn pond_kernel_resolve(
    handle: *mut PondKernelHandle, name: *const c_char,
) -> *mut c_char {
    let handle = unsafe { match handle.as_ref() { Some(h) => h, None => return ptr::null_mut() }};
    if name.is_null() { return ptr::null_mut(); }
    let name = match unsafe { CStr::from_ptr(name) }.to_str() { Ok(s) => s, Err(_) => return ptr::null_mut() };
    // C ABI LIMITATION (C17): `resolve` is fallible at the Rust level
    // (io::Result<Option<String>> — a transient backend failure is NOT an
    // absent ref), but this C entry point has no error channel, so Err(_)
    // maps to NULL exactly like None: C callers cannot distinguish a
    // transient failure from an unbound ref. Recorded in CRITIQUE.md C17.
    match handle.kernel.resolve(name) {
        Ok(Some(hash)) => CString::new(hash).map(|cs| cs.into_raw()).unwrap_or(ptr::null_mut()),
        Ok(None) | Err(_) => ptr::null_mut(),
    }
}

// ---------------------------------------------------------------------------
