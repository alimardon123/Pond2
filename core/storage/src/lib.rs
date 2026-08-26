// Pond UnifiedStorage — Rust port of the Python unified_storage.py
//
// BROKEN INTO MODULES (following the user's request for smaller files):
//   lib.rs          — UnifiedStorage struct + public API + ref namespace helpers
//   manifest.rs     — CollectionManifest (RowGroupEntry, ColumnStats, encode/decode)
//   commit.rs       — Commit struct + write/read commit blobs + history walking
//   branch.rs       — Branch management (branch, checkout, merge)
//   shard.rs        — CRDT shard management (append, list, clear)
//   read.rs         — Read path (read, read_with_shards, read_at_snapshot)
//   write.rs        — Write path (write, append)
//   transaction.rs  — Atomic publication (begin_tx, commit_tx, abort_tx)
//
// This is a FAITHFUL PORT of the Python implementation — same commit format,
// same ref conventions, same merge logic. The Python code is the reference;
// this Rust code is the production implementation.
//
// DESIGN PRINCIPLES:
//   - Simple: each module has one responsibility
//   - Powerful: composes the kernel's 3 primitives into a full storage layer
//   - Performant: Rust native speed, no Python GIL, no dict intermediate
//   - Scalable: O(conflicting) merge, content-addressed dedup, parallel I/O
//   - Beautiful: clear module boundaries, downward dependencies only

// C ABI functions below (§ C ABI) accept raw pointers from C callers.
// Safety contract is documented in that section and applies to all FFI functions.
#![allow(clippy::missing_safety_doc, clippy::not_unsafe_ptr_arg_deref)]

pub mod manifest;
pub mod commit;
pub mod branch;
pub mod shard;
pub mod read;
pub mod write;
pub mod transaction;
pub mod maintenance;
pub mod pond_pack;
pub mod slab;
pub mod bloom;
pub mod bptx;
pub mod write_buffer;

use pond_kernel::PondKernel;
use std::sync::Mutex;

// ---------------------------------------------------------------------------
// Ref namespace helpers — match Python UnifiedStorage conventions exactly
// ---------------------------------------------------------------------------

/// Branch commit ref: collections/{name}/_branches/{branch}/commit
pub fn branch_ref(collection: &str, branch: &str) -> String {
    format!("collections/{}/_branches/{}/commit", collection, branch)
}

/// Manifest ref: collections/{name}/_branches/{branch}/manifest
pub fn manifest_ref(collection: &str, branch: &str) -> String {
    format!("collections/{}/_branches/{}/manifest", collection, branch)
}

/// Shard prefix: collections/{name}/_branches/{branch}/shards/
pub fn shards_prefix(collection: &str, branch: &str) -> String {
    format!("collections/{}/_branches/{}/shards/", collection, branch)
}

/// Transaction ref: transactions/{tx_id}
pub fn tx_ref(tx_id: &str) -> String {
    format!("transactions/{}", tx_id)
}

/// Collection definition ref: collections/{name}/definition
pub fn definition_ref(collection: &str) -> String {
    format!("collections/{}/definition", collection)
}

// ---------------------------------------------------------------------------
// UnifiedStorage — the main struct
// ---------------------------------------------------------------------------

/// The unified storage layer. Owns a PondKernel and provides:
///   - Collection management (create, read, list)
///   - Commit history (write commits, walk parent chain, undo, revert)
///   - Branching (branch, checkout, merge)
///   - CRDT shards (append_shard, read_with_shards, compact_shards)
///   - Atomic publication (begin_tx, commit_tx, abort_tx)
///
/// This is the Rust equivalent of Python's UnifiedStorage class.
/// It composes the kernel's 3 primitives (Write, Read, Ref) into a
/// full versioned storage layer with git-like branching.
pub struct UnifiedStorage {
    kernel: PondKernel,
    /// Active branch per collection (in-memory, like Python's _active_branches)
    active_branches: Mutex<std::collections::HashMap<String, String>>,
}

impl UnifiedStorage {
    /// Create a new UnifiedStorage with a local FS kernel.
    pub fn new_local(base_dir: impl AsRef<std::path::Path>) -> std::io::Result<Self> {
        Ok(Self {
            kernel: PondKernel::new_local(base_dir)?,
            active_branches: Mutex::new(std::collections::HashMap::new()),
        })
    }

    /// Create a UnifiedStorage wrapping an existing kernel.
    pub fn new(kernel: PondKernel) -> Self {
        Self {
            kernel,
            active_branches: Mutex::new(std::collections::HashMap::new()),
        }
    }

    /// Get a reference to the kernel.
    pub fn kernel(&self) -> &PondKernel {
        &self.kernel
    }

    /// Get the active branch for a collection (default: "main").
    /// Matches Python's _get_active_branch.
    pub fn get_active_branch(&self, collection: &str) -> String {
        self.active_branches.lock().unwrap()
            .get(collection)
            .cloned()
            .unwrap_or_else(|| "main".to_string())
    }

    /// Set the active branch for a collection (in-memory only, like Python).
    pub fn set_active_branch(&self, collection: &str, branch: &str) {
        self.active_branches.lock().unwrap()
            .insert(collection.to_string(), branch.to_string());
    }

    /// Get the active commit ref for a collection.
    pub fn active_commit_ref(&self, collection: &str) -> String {
        let branch = self.get_active_branch(collection);
        branch_ref(collection, &branch)
    }

    /// Get the active manifest ref for a collection.
    pub fn active_manifest_ref(&self, collection: &str) -> String {
        let branch = self.get_active_branch(collection);
        manifest_ref(collection, &branch)
    }

    // Delegate to submodules
    // The actual implementations are in the module files and take
    // &UnifiedStorage (or &PondKernel) as the first argument.
}

// ---------------------------------------------------------------------------

// ===========================================================================
// C ABI — extern "C" wrappers for cross-language SDKs (Go, Java, Node, C)
// ===========================================================================
//
// These functions expose the full UnifiedStorage API through a C ABI.
// Any language that can call C functions (Go via cgo, Java via JNI, Node
// via N-API, C/C++ directly) gets full Pond storage access.
//
// Memory management:
//   - Strings returned by pond_storage_* functions are heap-allocated.
//     Caller MUST free them with pond_string_free().
//   - Data returned via out-params is heap-allocated.
//     Caller MUST free with pond_data_free().
//   - Handles (PondStorageHandle*) must be freed with pond_storage_free().
//
// Error handling:
//   - Functions that return strings return NULL on error.
//   - Functions that return int return 0 on success, -1 on error.
//   - Functions that return handles return NULL on error.

use std::ffi::{c_char, CStr, CString};
use std::ptr;

/// Opaque handle for UnifiedStorage.
pub struct PondStorageHandle {
    storage: UnifiedStorage,
}

impl PondStorageHandle {
    /// Create a handle from a UnifiedStorage. Used by C ABI constructors
    /// in other crates (e.g., pond_s3's `pond_storage_new_s3`).
    pub fn new(storage: UnifiedStorage) -> Self {
        Self { storage }
    }
}

/// Create a new UnifiedStorage with a local FS backend.
/// Returns NULL on error.
#[no_mangle]
pub extern "C" fn pond_storage_new(base_dir: *const c_char) -> *mut PondStorageHandle {
    if base_dir.is_null() { return ptr::null_mut(); }
    let dir = match unsafe { CStr::from_ptr(base_dir) }.to_str() {
        Ok(s) => s,
        Err(_) => return ptr::null_mut(),
    };
    match UnifiedStorage::new_local(dir) {
        Ok(storage) => Box::into_raw(Box::new(PondStorageHandle { storage })),
        Err(_) => ptr::null_mut(),
    }
}

/// Free a PondStorageHandle. Safe on NULL.
#[no_mangle]
pub extern "C" fn pond_storage_free(handle: *mut PondStorageHandle) {
    if !handle.is_null() {
        unsafe { drop(Box::from_raw(handle)); }
    }
}

/// Get the active branch for a collection.
/// Returns a heap-allocated string (caller must free with pond_string_free).
/// Returns NULL if the collection has no active branch (defaults to "main").
#[no_mangle]
pub extern "C" fn pond_storage_get_active_branch(
    handle: *const PondStorageHandle,
    collection: *const c_char,
) -> *mut c_char {
    let handle = unsafe { match handle.as_ref() { Some(h) => h, None => return ptr::null_mut() }};
    if collection.is_null() { return ptr::null_mut(); }
    let coll = match unsafe { CStr::from_ptr(collection) }.to_str() {
        Ok(s) => s, Err(_) => return ptr::null_mut(),
    };
    let branch = handle.storage.get_active_branch(coll);
    CString::new(branch).map(|cs| cs.into_raw()).unwrap_or(ptr::null_mut())
}

/// Set the active branch for a collection (in-memory only).
#[no_mangle]
pub extern "C" fn pond_storage_set_active_branch(
    handle: *mut PondStorageHandle,
    collection: *const c_char,
    branch: *const c_char,
) {
    let handle = unsafe { match handle.as_mut() { Some(h) => h, None => return }};
    if collection.is_null() || branch.is_null() { return; }
    let coll = match unsafe { CStr::from_ptr(collection) }.to_str() { Ok(s) => s, Err(_) => return };
    let br = match unsafe { CStr::from_ptr(branch) }.to_str() { Ok(s) => s, Err(_) => return };
    handle.storage.set_active_branch(coll, br);
}

/// Write data to a collection on the active branch.
/// Returns the commit hash (heap-allocated, caller must free), or NULL on error.
#[no_mangle]
pub extern "C" fn pond_storage_write(
    handle: *mut PondStorageHandle,
    collection: *const c_char,
    data: *const u8,
    data_len: usize,
    message: *const c_char,
) -> *mut c_char {
    let handle = unsafe { match handle.as_ref() { Some(h) => h, None => return ptr::null_mut() }};
    if collection.is_null() || data.is_null() { return ptr::null_mut(); }
    let coll = match unsafe { CStr::from_ptr(collection) }.to_str() { Ok(s) => s, Err(_) => return ptr::null_mut() };
    let msg = if message.is_null() { "" } else {
        unsafe { CStr::from_ptr(message) }.to_str().unwrap_or_default()
    };
    let data_slice = unsafe { std::slice::from_raw_parts(data, data_len) };
    let active = handle.storage.get_active_branch(coll);
    match write::write(handle.storage.kernel(), coll, &active, data_slice, msg) {
        Ok(hash) => CString::new(hash).map(|cs| cs.into_raw()).unwrap_or(ptr::null_mut()),
        Err(_) => ptr::null_mut(),
    }
}

/// Read data from a collection's active branch.
/// Writes the data pointer + length into out-params.
/// Returns 0 on success, -1 on error.
#[no_mangle]
pub extern "C" fn pond_storage_read(
    handle: *const PondStorageHandle,
    collection: *const c_char,
    out_data: *mut *const u8,
    out_len: *mut usize,
) -> i32 {
    let handle = unsafe { match handle.as_ref() { Some(h) => h, None => return -1 }};
    if collection.is_null() || out_data.is_null() || out_len.is_null() { return -1; }
    let coll = match unsafe { CStr::from_ptr(collection) }.to_str() { Ok(s) => s, Err(_) => return -1 };
    let active = handle.storage.get_active_branch(coll);
    match read::read(handle.storage.kernel(), coll, &active) {
        Ok(data) => {
            let boxed = data.into_boxed_slice();
            let p = boxed.as_ptr();
            let len = boxed.len();
            std::mem::forget(boxed);
            unsafe { *out_data = p; *out_len = len; }
            0
        }
        Err(_) => -1,
    }
}

/// Create a branch from the active branch.
/// Returns the commit hash (caller must free), or NULL on error.
#[no_mangle]
pub extern "C" fn pond_storage_branch(
    handle: *mut PondStorageHandle,
    collection: *const c_char,
    branch_name: *const c_char,
) -> *mut c_char {
    let handle = unsafe { match handle.as_ref() { Some(h) => h, None => return ptr::null_mut() }};
    if collection.is_null() || branch_name.is_null() { return ptr::null_mut(); }
    let coll = match unsafe { CStr::from_ptr(collection) }.to_str() { Ok(s) => s, Err(_) => return ptr::null_mut() };
    let br = match unsafe { CStr::from_ptr(branch_name) }.to_str() { Ok(s) => s, Err(_) => return ptr::null_mut() };
    let active = handle.storage.get_active_branch(coll);
    match branch::branch(handle.storage.kernel(), coll, br, &active) {
        Ok(hash) => CString::new(hash).map(|cs| cs.into_raw()).unwrap_or(ptr::null_mut()),
        Err(_) => ptr::null_mut(),
    }
}

/// Checkout a branch (verify it exists + set active).
/// Returns 0 on success, -1 on error.
#[no_mangle]
pub extern "C" fn pond_storage_checkout(
    handle: *mut PondStorageHandle,
    collection: *const c_char,
    branch_name: *const c_char,
) -> i32 {
    let handle = unsafe { match handle.as_mut() { Some(h) => h, None => return -1 }};
    if collection.is_null() || branch_name.is_null() { return -1; }
    let coll = match unsafe { CStr::from_ptr(collection) }.to_str() { Ok(s) => s, Err(_) => return -1 };
    let br = match unsafe { CStr::from_ptr(branch_name) }.to_str() { Ok(s) => s, Err(_) => return -1 };
    match branch::checkout(handle.storage.kernel(), coll, br) {
        Ok(()) => {
            handle.storage.set_active_branch(coll, br);
            0
        }
        Err(_) => -1,
    }
}

/// Merge a source branch into a target branch.
/// If target is NULL, uses the active branch.
/// Returns the merge commit hash (caller must free), or NULL on error.
#[no_mangle]
pub extern "C" fn pond_storage_merge(
    handle: *mut PondStorageHandle,
    collection: *const c_char,
    source_branch: *const c_char,
    target_branch: *const c_char,
    message: *const c_char,
) -> *mut c_char {
    let handle = unsafe { match handle.as_ref() { Some(h) => h, None => return ptr::null_mut() }};
    if collection.is_null() || source_branch.is_null() { return ptr::null_mut(); }
    let coll = match unsafe { CStr::from_ptr(collection) }.to_str() { Ok(s) => s, Err(_) => return ptr::null_mut() };
    let src = match unsafe { CStr::from_ptr(source_branch) }.to_str() { Ok(s) => s, Err(_) => return ptr::null_mut() };
    let tgt = if target_branch.is_null() {
        handle.storage.get_active_branch(coll)
    } else {
        match unsafe { CStr::from_ptr(target_branch) }.to_str() { Ok(s) => s.to_string(), Err(_) => return ptr::null_mut() }
    };
    let msg = if message.is_null() { "" } else {
        unsafe { CStr::from_ptr(message) }.to_str().unwrap_or_default()
    };
    match branch::merge(handle.storage.kernel(), coll, src, &tgt, msg) {
        Ok(hash) => CString::new(hash).map(|cs| cs.into_raw()).unwrap_or(ptr::null_mut()),
        Err(_) => ptr::null_mut(),
    }
}

/// Undo the last N commits on the active branch.
/// Returns the new HEAD hash (caller must free), or NULL on error.
#[no_mangle]
pub extern "C" fn pond_storage_undo(
    handle: *mut PondStorageHandle,
    collection: *const c_char,
    steps: usize,
) -> *mut c_char {
    let handle = unsafe { match handle.as_ref() { Some(h) => h, None => return ptr::null_mut() }};
    if collection.is_null() { return ptr::null_mut(); }
    let coll = match unsafe { CStr::from_ptr(collection) }.to_str() { Ok(s) => s, Err(_) => return ptr::null_mut() };
    let active = handle.storage.get_active_branch(coll);
    match branch::undo(handle.storage.kernel(), coll, &active, steps) {
        Ok(hash) => CString::new(hash).map(|cs| cs.into_raw()).unwrap_or(ptr::null_mut()),
        Err(_) => ptr::null_mut(),
    }
}

/// Revert the active branch to a specific commit.
/// Returns 0 on success, -1 on error.
#[no_mangle]
pub extern "C" fn pond_storage_revert(
    handle: *mut PondStorageHandle,
    collection: *const c_char,
    commit_hash: *const c_char,
) -> i32 {
    let handle = unsafe { match handle.as_ref() { Some(h) => h, None => return -1 }};
    if collection.is_null() || commit_hash.is_null() { return -1; }
    let coll = match unsafe { CStr::from_ptr(collection) }.to_str() { Ok(s) => s, Err(_) => return -1 };
    let hash = match unsafe { CStr::from_ptr(commit_hash) }.to_str() { Ok(s) => s, Err(_) => return -1 };
    let active = handle.storage.get_active_branch(coll);
    match branch::revert(handle.storage.kernel(), coll, &active, hash) {
        Ok(()) => 0,
        Err(_) => -1,
    }
}

/// List branches for a collection.
/// Returns a newline-separated string of branch names (caller must free).
/// Returns NULL on error.
#[no_mangle]
pub extern "C" fn pond_storage_list_branches(
    handle: *const PondStorageHandle,
    collection: *const c_char,
) -> *mut c_char {
    let handle = unsafe { match handle.as_ref() { Some(h) => h, None => return ptr::null_mut() }};
    if collection.is_null() { return ptr::null_mut(); }
    let coll = match unsafe { CStr::from_ptr(collection) }.to_str() { Ok(s) => s, Err(_) => return ptr::null_mut() };
    let branches = branch::list_branches(handle.storage.kernel(), coll);
    let joined = branches.join("\n");
    CString::new(joined).map(|cs| cs.into_raw()).unwrap_or(ptr::null_mut())
}

/// Free a string returned by pond_storage_* functions.
#[no_mangle]
pub extern "C" fn pond_storage_string_free(s: *mut c_char) {
    if !s.is_null() {
        unsafe { drop(CString::from_raw(s)); }
    }
}

/// Free data returned by pond_storage_read.
#[no_mangle]
pub extern "C" fn pond_storage_data_free(data: *mut u8, len: usize) {
    if !data.is_null() && len > 0 {
        unsafe { drop(Vec::from_raw_parts(data, len, len)); }
    }
}

// Layer 2b: Structured row operations (write_rows, read_rows)
// =============================================================

// (Legacy doc comment preserved for history — the actual write_rows function
//  is documented below.)
//
// Write structured INT64 columns as a PND2 blob with column stats.
//
// Args:
//   handle: Storage handle
//   collection: Collection name
//   message: Commit message
//   num_cols: Number of columns
//   col_names: Array of column names (num_cols pointers)
//   col_data: Array of pointers to column data arrays
//   col_lens: Array of column lengths (must all be equal)
//   col_types: Array of column type codes (0=i64, 1=f64, 2=str)
//   str_data: For string columns, array of pointers to string arrays
//             (each string column has col_lens[i] pointers to null-terminated strings)
//
// Returns: commit hash (caller must free with pond_storage_string_free), or NULL on error.

// ---------------------------------------------------------------------------
// Structured row operations (write_rows / read_rows)
// ---------------------------------------------------------------------------

/// Write structured rows as a PND2 blob with CRDT metadata.
///
/// Args:
///   - handle: storage handle
///   - collection: collection name
///   - message: commit message
///   - n_columns: number of columns
///   - col_names: array of column name strings (null-terminated)
///   - col_types: array of column type codes (1=INT64, 2=FLOAT64, 3=STRING)
///   - col_data: array of pointers to column data (i64*, f64*, or char**)
///   - col_lens: array of column lengths (number of values per column)
///
/// Returns: commit hash string (caller must free with pond_storage_string_free),
///          or NULL on error.
#[no_mangle]
pub extern "C" fn pond_storage_write_rows(
    handle: *mut PondStorageHandle,
    collection: *const c_char,
    message: *const c_char,
    n_columns: usize,
    col_names: *const *const c_char,
    col_types: *const u8,
    col_data: *const *const std::ffi::c_void,
    col_lens: *const usize,
) -> *mut c_char {
    let handle = unsafe { match handle.as_mut() { Some(h) => h, None => return ptr::null_mut() }};
    if collection.is_null() || col_names.is_null() || col_types.is_null() || col_data.is_null() || col_lens.is_null() {
        return ptr::null_mut();
    }

    let coll = match unsafe { CStr::from_ptr(collection) }.to_str() { Ok(s) => s, Err(_) => return ptr::null_mut() };
    let msg = if message.is_null() { "" } else {
        unsafe { CStr::from_ptr(message) }.to_str().unwrap_or_default()
    };

    use pond_core::TypedColumn;

    // First pass: collect owned column names (no borrows from col_name_buf
    // can exist while we push to it). Second pass: build columns.
    let mut col_name_buf: Vec<String> = Vec::with_capacity(n_columns);
    // Intermediate storage for column data (name index, vtype, data)
    struct ColInput {
        name_idx: usize,
        vtype: u8,
        data_ptr: *const std::ffi::c_void,
        len: usize,
    }
    let mut inputs: Vec<ColInput> = Vec::with_capacity(n_columns);

    for i in 0..n_columns {
        let name_ptr = unsafe { *col_names.add(i) };
        let vtype = unsafe { *col_types.add(i) };
        let data_ptr = unsafe { *col_data.add(i) };
        let len = unsafe { *col_lens.add(i) };

        if name_ptr.is_null() || data_ptr.is_null() || len == 0 {
            continue;
        }

        let name = match unsafe { CStr::from_ptr(name_ptr) }.to_str() {
            Ok(s) => s,
            Err(_) => continue,
        };

        col_name_buf.push(name.to_string());
        inputs.push(ColInput { name_idx: col_name_buf.len() - 1, vtype, data_ptr, len });
    }

    let mut columns: Vec<(&str, TypedColumn)> = Vec::with_capacity(inputs.len());
    for inp in &inputs {
        let name_borrowed: &str = &col_name_buf[inp.name_idx];
        match inp.vtype {
            1 => { // VT_INT64
                let ptr = inp.data_ptr as *const i64;
                let slice = unsafe { std::slice::from_raw_parts(ptr, inp.len) };
                columns.push((name_borrowed, TypedColumn::Int64(slice.to_vec())));
            }
            2 => { // VT_FLOAT64
                let ptr = inp.data_ptr as *const f64;
                let slice = unsafe { std::slice::from_raw_parts(ptr, inp.len) };
                columns.push((name_borrowed, TypedColumn::Float64(slice.to_vec())));
            }
            3 => { // VT_STRING
                let ptr = inp.data_ptr as *const *const c_char;
                let mut vals = Vec::with_capacity(inp.len);
                for j in 0..inp.len {
                    let s_ptr = unsafe { *ptr.add(j) };
                    if s_ptr.is_null() {
                        vals.push(String::new());
                    } else {
                        let s = unsafe { CStr::from_ptr(s_ptr) }.to_str().unwrap_or("").to_string();
                        vals.push(s);
                    }
                }
                columns.push((name_borrowed, TypedColumn::String(vals)));
            }
            _ => continue,
        }
    }

    if columns.is_empty() {
        return ptr::null_mut();
    }

    let active = handle.storage.get_active_branch(coll);
    match write::write_rows(handle.storage.kernel(), coll, &active, &columns, msg) {
        Ok(hash) => CString::new(hash).map(|cs| cs.into_raw()).unwrap_or(ptr::null_mut()),
        Err(_) => ptr::null_mut(),
    }
}

/// Read structured rows from a collection's HEAD.
///
/// Returns a PND2 blob handle (caller must free with pond_storage_data_free),
/// or NULL on error. The blob contains all columns from the HEAD manifest's
/// first row group.
#[no_mangle]
pub extern "C" fn pond_storage_read_rows(
    handle: *const PondStorageHandle,
    collection: *const c_char,
    out_data: *mut *mut u8,
    out_len: *mut usize,
) -> i32 {
    let handle = unsafe { match handle.as_ref() { Some(h) => h, None => return -1 }};
    if collection.is_null() || out_data.is_null() || out_len.is_null() { return -1; }

    let coll = match unsafe { CStr::from_ptr(collection) }.to_str() { Ok(s) => s, Err(_) => return -1 };
    let active = handle.storage.get_active_branch(coll);

    match read::read(handle.storage.kernel(), coll, &active) {
        Ok(data) => {
            let len = data.len();
            let boxed = data.into_boxed_slice();
            let ptr = boxed.as_ptr() as *mut u8;
            std::mem::forget(boxed);
            unsafe {
                *out_data = ptr;
                *out_len = len;
            }
            0
        }
        Err(_) => -1,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ref_namespace() {
        assert_eq!(
            branch_ref("users", "main"),
            "collections/users/_branches/main/commit"
        );
        assert_eq!(
            manifest_ref("users", "main"),
            "collections/users/_branches/main/manifest"
        );
        assert_eq!(
            shards_prefix("users", "main"),
            "collections/users/_branches/main/shards/"
        );
        assert_eq!(tx_ref("abc123"), "transactions/abc123");
        assert_eq!(definition_ref("users"), "collections/users/definition");
    }

    #[test]
    fn test_active_branch_default() {
        let dir = tempfile::tempdir().unwrap();
        let storage = UnifiedStorage::new_local(dir.path()).unwrap();
        assert_eq!(storage.get_active_branch("users"), "main");
    }

    #[test]
    fn test_set_active_branch() {
        let dir = tempfile::tempdir().unwrap();
        let storage = UnifiedStorage::new_local(dir.path()).unwrap();
        storage.set_active_branch("users", "experiment");
        assert_eq!(storage.get_active_branch("users"), "experiment");
        assert_eq!(storage.active_commit_ref("users"),
                   "collections/users/_branches/experiment/commit");
    }
}
