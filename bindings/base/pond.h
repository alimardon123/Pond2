/*
 * Pond — Unified C ABI Header
 *
 * This is the SINGLE header that all language SDKs include.
 * It declares the C ABI for all three layers:
 *   1. Kernel (Write, Read, Ref — the 3 primitives)
 *   2. Storage (UnifiedStorage — write, read, branch, merge, history, undo)
 *   3. Codec (PND2 — encode, decode, all encodings/vtypes)
 *
 * Usage:
 *   1. Link against libpond_storage.a (static) or libpond_storage.so (dynamic)
 *      (it pulls in libpond_kernel.a and libpond_core.a automatically)
 *   2. #include "pond.h"
 *   3. Call pond_storage_new() to get a storage handle
 *   4. Call pond_storage_write/read/branch/merge/etc.
 *   5. Free handles with pond_storage_free()
 *   6. Free strings with pond_storage_string_free()
 *   7. Free data with pond_storage_data_free()
 *
 * Language SDKs:
 *   - Go: import "pond" (cgo wrapper around this header)
 *   - Java: org.pond.Pond (JNI wrapper around this header)
 *   - Node: require('pond') (N-API wrapper around this header)
 *   - C/C++: #include "pond.h" (direct)
 *   - Python: import pond (PyO3 — does NOT use this header, calls Rust directly)
 */

#ifndef POND_H
#define POND_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ============================================================= *
 * Layer 1: Kernel (3 primitives — Write, Read, Ref)
 * ============================================================= */

typedef struct PondKernel PondKernel;
typedef struct PondResult PondResult;

PondKernel* pond_kernel_new(const char* base_dir);
void        pond_kernel_free(PondKernel* k);
const char* pond_kernel_write(PondKernel* k, const uint8_t* data, size_t len);
int         pond_kernel_read(PondKernel* k, const char* hash_or_name,
                              const uint8_t** out, size_t* out_len);
int         pond_kernel_reference(PondKernel* k, const char* name, const char* hash);
const char* pond_kernel_resolve(PondKernel* k, const char* name);
void        pond_kernel_string_free(char* s);
void        pond_kernel_data_free(uint8_t* data, size_t len);

/* ============================================================= *
 * Layer 2: Storage (UnifiedStorage — versioning, branching, merge)
 * ============================================================= */

typedef struct PondStorageHandle PondStorageHandle;

PondStorageHandle* pond_storage_new(const char* base_dir);
void               pond_storage_free(PondStorageHandle* s);

/* Create a UnifiedStorage backed by S3-compatible storage.
 * URL format: s3://bucket/prefix?region=us-east-1&endpoint=https://...
 * Credentials are read from the environment (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY).
 * Returns NULL on error.
 * Link against libpond_s3.a (in addition to libpond_storage.a) to use this. */
PondStorageHandle* pond_storage_new_s3(const char* s3_url);

const char* pond_storage_get_active_branch(PondStorageHandle* s, const char* collection);
void        pond_storage_set_active_branch(PondStorageHandle* s, const char* collection, const char* branch);

const char* pond_storage_write(PondStorageHandle* s, const char* collection,
                                const uint8_t* data, size_t len, const char* message);
int         pond_storage_read(PondStorageHandle* s, const char* collection,
                               const uint8_t** out, size_t* out_len);

const char* pond_storage_branch(PondStorageHandle* s, const char* collection, const char* branch_name);
int         pond_storage_checkout(PondStorageHandle* s, const char* collection, const char* branch_name);
const char* pond_storage_merge(PondStorageHandle* s, const char* collection,
                                const char* source, const char* target, const char* message);
const char* pond_storage_undo(PondStorageHandle* s, const char* collection, size_t steps);
int         pond_storage_revert(PondStorageHandle* s, const char* collection, const char* commit_hash);
const char* pond_storage_list_branches(PondStorageHandle* s, const char* collection);

void        pond_storage_string_free(char* s);
void        pond_storage_data_free(uint8_t* data, size_t len);

/* Structured row operations (write_rows, read_rows) */

/* Write structured columns as PND2.
 * col_types: 0=i64, 1=f64, 2=string
 * For string columns, col_data[i] points to an array of const char* pointers.
 * Returns: commit hash (caller must free), or NULL on error. */
const char* pond_storage_write_rows(PondStorageHandle* s, const char* collection,
                                     const char* message,
                                     size_t num_cols,
                                     const char* const* col_names,
                                     const void* const* col_data,
                                     const size_t* col_lens,
                                     const uint8_t* col_types,
                                     size_t n_rows);

/* Read structured columns from a collection's HEAD.
 * Returns: PondResult* (free with pond_result_free), or NULL on error. */
PondResult* pond_storage_read_rows(PondStorageHandle* s, const char* collection);

/* ============================================================= *
 * Layer 3: Codec (PND2 — encode, decode, all encodings/vtypes)
 * ============================================================= */

typedef struct PondEncoder PondEncoder;

PondResult* pond_pnd2_decode(const uint8_t* blob, size_t blob_len);
size_t      pond_result_num_columns(const PondResult* result);
const char* pond_result_column_name(const PondResult* result, size_t index);
uint8_t     pond_result_column_vtype(const PondResult* result, size_t index);
size_t      pond_result_column_len(const PondResult* result, size_t index);
const int64_t* pond_result_column_i64(const PondResult* result, size_t index);
const double*  pond_result_column_f64(const PondResult* result, size_t index);
const char* pond_result_column_str(const PondResult* result, size_t col, size_t row);
const char* const* pond_result_column_str_array(const PondResult* result, size_t col_index);
void pond_str_array_free(const char* const* arr);
int32_t     pond_result_column_bin(const PondResult* result, size_t col, size_t row,
                                    const uint8_t** out, size_t* out_len);
void        pond_result_free(PondResult* result);

int32_t     pond_pnd2_encode_i64(const int64_t* values, size_t n,
                                  uint8_t** out_blob, size_t* out_len);
int32_t     pond_pnd2_encode_f64(const double* values, size_t n,
                                  uint8_t** out_blob, size_t* out_len);
int32_t     pond_pnd2_encode_str(const char** values, size_t n,
                                  uint8_t** out_blob, size_t* out_len);
void        pond_blob_free(uint8_t* blob, size_t len);

PondEncoder* pond_encoder_new(size_t n_rows);
int32_t      pond_encoder_add_i64_column(PondEncoder* enc, const char* name,
                                          const int64_t* values, size_t n);
int32_t      pond_encoder_add_f64_column(PondEncoder* enc, const char* name,
                                          const double* values, size_t n);
int32_t      pond_encoder_add_str_column(PondEncoder* enc, const char* name,
                                          const char** values, size_t n);
int32_t      pond_encoder_build(PondEncoder* enc, uint8_t** out_blob, size_t* out_len);
void         pond_encoder_free(PondEncoder* enc);

#ifdef __cplusplus
}
#endif

#endif /* POND_H */
