# Pond SDK Specification

> **Status:** This document describes the CURRENT state of the Pond SDK
> (August 2026). It replaces the previous version which described APIs
> that no longer exist.
>
> For migration status, see [`docs/STATUS.md`](docs/STATUS.md).

---

## 1. Architecture

```
Lenses (KV, Vector, Streaming, Lakehouse, OLTP)
  ↓ compose
UnifiedStorage (Rust core or Python reference)
  ↓
Kernel (3 ops: Write, Read, Ref)
  ↓
ObjectStore trait
  ├── LocalFSObjectStore (Rust)
  ├── S3ObjectStore (Rust, SigV4 from scratch)
  └── InMemoryObjectStore (Python, testing)
```

### Two implementations

| Implementation | Path | Status |
|---|---|---|
| **Rust core** (canonical) | `core/` | Production for storage, CLI, C ABI |
| **Python reference** | `bindings/python/` | Production for lenses, maintained for bugs |

The Rust core handles: kernel, storage (versioning, branching, shards),
PND2 codec, S3, CLI, C ABI. The Python SDK handles: lenses (KeyValue,
Lakehouse, Streaming, Vector, OLTP), extensions (indexing, semantic).

Python calls Rust via PyO3 (`pond.Storage`, `pond.decode`, `pond.encode`).
Other languages call Rust via the C ABI (`pond.h`).

---

## 2. Kernel (Layer 0)

### 2.1 Three primitives

```rust
// Rust (core/kernel/)
pub trait ObjectStore: Send + Sync {
    fn put_blob(&self, data: &[u8]) -> io::Result<String>;
    fn get_blob(&self, hash: &str) -> io::Result<Vec<u8>>;
    fn put_path(&self, path: &str, hash: &str) -> io::Result<()>;
    fn get_path(&self, path: &str) -> Option<String>;
    fn delete_path(&self, path: &str) -> io::Result<bool>;
    fn list_paths(&self, prefix: &str) -> io::Result<Vec<String>>;
    fn blob_exists(&self, hash: &str) -> bool;
    fn delete_blob(&self, hash: &str) -> io::Result<bool>;
}

pub struct PondKernel { /* ... */ }
impl PondKernel {
    pub fn write(&self, data: &[u8]) -> io::Result<String>;  // → hash
    pub fn read(&self, name_or_hash: &str) -> io::Result<Vec<u8>>;
    pub fn reference(&self, name: &str, hash: &str) -> io::Result<()>;
    pub fn resolve(&self, name: &str) -> Option<String>;
    pub fn list_names_prefix(&self, prefix: &str) -> Vec<String>;
}
```

### 2.2 Path layout (same on ALL backends)

```
blobs/{hash[:2]}/{hash}                          — content-addressed blobs
collections/{name}/_branches/{branch}/commit      — branch commit refs
collections/{name}/_branches/{branch}/manifest    — branch manifest refs
collections/{name}/_branches/{branch}/shards/{id} — CRDT shards
collections/{name}/_active_branch                 — active branch name
transactions/{tx_id}                              — transaction markers
```

### 2.3 Storage backends

| Backend | Rust | Python | Notes |
|---|---|---|---|
| Local FS | ✅ | ✅ | `LocalFSObjectStore` |
| S3-compatible | ✅ | ✅ (boto3) | AWS S3, R2, MinIO, LocalStack, Wasabi |
| In-memory | ❌ | ✅ | Testing only |

S3 credentials from environment: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`,
`AWS_SESSION_TOKEN` (optional).

---

## 3. UnifiedStorage (Layer 1)

### 3.1 Write paths

```rust
// Raw bytes (JSON or any format) — simple, used by CLI
pub fn write(kernel, collection, branch, data: &[u8], message) -> Result<String, String>;

// Structured INT64 columns as PND2 — production with stats + pruning
pub fn write_rows_i64(kernel, collection, branch, columns: &[(&str, &[i64])], message) -> Result<String, String>;
```

`write_rows_i64` encodes columns as a PND2 blob with:
- Auto-encoding per column (RLE/DICT/BITPACK/RAW based on heuristics)
- Per-column stats in manifest (min/max/null_count for pruning)
- Schema in manifest (column names + types)

### 3.2 Read paths

```rust
// Read HEAD (raw bytes)
pub fn read(kernel, collection, branch) -> Result<Vec<u8>, String>;

// Read at specific commit (time-travel)
pub fn read_at_snapshot(kernel, commit_hash) -> Result<Vec<u8>, String>;

// Read HEAD + all shards (CRDT merge)
pub fn read_full(kernel, collection, branch) -> Vec<Vec<u8>>;
```

### 3.3 Versioning (git-like)

```rust
pub fn branch(kernel, collection, source_branch, new_branch) -> Result<String, String>;
pub fn checkout(storage, collection, branch_name);
pub fn merge(kernel, collection, source, target, message) -> Result<String, String>;
pub fn undo(kernel, collection, branch, steps) -> Result<String, String>;
pub fn revert(kernel, collection, branch, commit_hash) -> Result<(), String>;
pub fn history(kernel, commit_hash, limit) -> Vec<(String, Commit)>;
```

### 3.4 CRDT shards (concurrent multi-writer)

```rust
pub fn append_shard(kernel, collection, branch, shard_id, data) -> Result<String, String>;
pub fn upsert_shard(kernel, collection, branch, shard_id, rows, key_col, hlc) -> Result<String, String>;
pub fn delete_shard(kernel, collection, branch, shard_id, rowids, hlc) -> Result<String, String>;
pub fn read_with_shards(kernel, collection, branch) -> (Option<Vec<u8>>, Vec<Vec<u8>>);
pub fn compact_shards(kernel, collection, branch) -> Result<usize, String>;
```

CRDT merge uses HLC (Hybrid Logical Clock) for clock-skew-safe LWW.
Each row gets `_rowid` (UUIDv7) and `_version` (HLC) columns.

### 3.5 Transactions (atomic visibility)

```rust
pub fn begin_tx() -> String;  // → tx_id
pub fn commit_tx(kernel, tx_id, message) -> Result<String, String>;
pub fn abort_tx(kernel, tx_id);
pub fn is_tx_committed(kernel, tx_id) -> bool;
```

**Note:** This is atomic VISIBILITY (not full ACID — no isolation, no rollback).
Once the commit marker exists, all tentative shards become visible together.

### 3.6 PondPack (PNPK format)

```rust
// core/storage/src/pond_pack.rs
pub fn encode_pack(commit: &Value, manifest_bytes: &[u8], inline_data: Option<&[Vec<u8>]>) -> Vec<u8>;
pub fn decode_pack(blob: &[u8]) -> Option<(Value, Vec<u8>, Option<Vec<Vec<u8>>>)>;
```

PondPack combines commit JSON + manifest bytes into ONE blob, saving
1-2 GETs per cold read and 1 PUT per write. Backward compatible with
old separate commit (JSON) + manifest (PMAN) format.

---

## 4. PND2 Codec (Layer 2)

### 4.1 Format

```
Header (13 bytes):
  magic "PND2" (4B) + version 1 (1B) + flags (1B)
  + n_rows (4B) + n_columns (2B) + compression_tag (1B)

Inner (after header):
  Phase 1: ALL schemas — per column: name_len(1B) + name + vtype(1B) + enc(1B)
  Phase 2: ALL stats — per column: has_min(1B) + min(8B) + max(8B) + null_count(4B)
  Phase 3: ALL payloads — per column: payload_len(4B) + payload

Compression: 0=none, 2=zstd (feature flag in Rust)
```

### 4.2 Encodings

| Encoding | Code | Best for | Rust decode | Rust encode |
|---|---|---|---|---|
| RAW | 0 | General purpose | ✅ | ✅ |
| RLE | 1 | Consecutive repeats | ✅ | ✅ |
| DICT | 2 | Low cardinality | ✅ | ✅ |
| BITPACK | 3 | Small-range integers | ✅ | ✅ |

Auto-selection (`encode_i64_auto`):
1. Low cardinality (<10% unique, <1000 unique) → DICT
2. Run-heavy (<50% runs in sample) → RLE
3. Small range (<2^16) → BITPACK
4. Default → RAW

### 4.3 Value types

| Type | Code | Rust |
|---|---|---|
| INT64 | 1 | ✅ |
| FLOAT64 | 2 | ✅ |
| STRING | 3 | ✅ |
| NULL | 4 | ✅ |
| BINARY | 5 | ✅ |

### 4.4 zstd decompression

Enabled via `zstd` feature flag (uses `ruzstd`, pure-Rust, no C deps):
```toml
pond_core = { path = "core/codec", features = ["zstd"] }
```

---

## 5. Lenses (Layer 3)

### 5.1 Lens design

Each lens owns a `UnifiedStorage` and adds workload-specific semantics.
All 5 Python lenses extend `PondLens` (base_lens.py) for shared
branch/list/history support. Rust lenses own UnifiedStorage directly.

```rust
pub struct KeyValueLens {
    storage: UnifiedStorage,
    staged: Mutex<HashMap<String, HashMap<String, Option<Value>>>>,
}
```

### 5.2 Available lenses

| Lens | Rust | Python | Notes |
|---|---|---|---|
| KeyValueLens | ✅ core API | ✅ full | KV with staging + commit |
| StreamingLens | ✅ core API | ✅ full | Chunked storage, range read |
| OLTPLens | ✅ core API | ✅ full | Memtable + batch flush |
| LakehouseLens | ✅ core API | ✅ | Tabular + DuckDB SQL |
| VectorLens | ✅ core API | ✅ | IVF + HNSW ANN |

### 5.3 Lens structure

```
lenses/{name}/
├── python/     # Python implementation
├── rust/       # Rust implementation (if ported)
└── README.md
```

---

## 6. Extensions (Layer 2.5)

### 6.1 Physical structures

| Extension | Python | Rust | Notes |
|---|---|---|---|
| UnifiedStorage (PND2) | ✅ 5,767 LOC | ✅ | Python has full PND2 + caching + pruning; Rust has write/read/branch/merge/GC |
| CollectionManifest | ✅ | ✅ | PMAN format |
| StatsTree | ✅ | ❌ | PB-scale hierarchical stats (defer) |
| Encoding (RLE/DICT/BITPACK) | ✅ | ✅ | Auto-selection |
| Compression (zstd) | ✅ | ✅ | Feature flag |
| PondPack (PNPK) | ✅ | ✅ | Commit+manifest in one blob |

### 6.2 Indexing

| Extension | Python | Rust | Notes |
|---|---|---|---|
| CollectionIndexer | ✅ | ❌ | Secondary indexes (JSON blob format) |
| IVFIndex | ✅ | ✅ | Vector ANN (Bug 10 fixed in Rust; Python still reads ALL vectors) |
| HNSWIndex | ✅ | ✅ | Graph ANN (pure-Python, 10-100x slower than Rust) |

**Known gap (Python only):** IVF search reads ALL vectors then filters —
`n_probe` has no effect on I/O in Python. Fixed in Rust via per-cluster
blob references.

### 6.3 Maintenance

| Extension | Python | Rust | Notes |
|---|---|---|---|
| GarbageCollector | ✅ 476 LOC | ❌ | GC + vacuum with preserve_days |
| Tombstone helpers | ✅ | ✅ | drop_name, is_dropped, resolve_active |

### 6.4 Semantic

| Extension | Python | Rust | Notes |
|---|---|---|---|
| SemanticMixin | ✅ | ❌ | Defer (Ossie is a placeholder name) |

---

## 7. C ABI (`pond.h`)

One header for all languages:

```c
// Kernel
PondKernel* pond_kernel_new(const char* base_dir);
const char* pond_kernel_write(PondKernel* k, const uint8_t* data, size_t len);
int         pond_kernel_read(PondKernel* k, const char* hash_or_name, ...);

// Storage
PondStorageHandle* pond_storage_new(const char* base_dir);
PondStorageHandle* pond_storage_new_s3(const char* s3_url);
const char* pond_storage_write(PondStorageHandle* s, const char* collection, ...);
int         pond_storage_read(PondStorageHandle* s, const char* collection, ...);
const char* pond_storage_branch(PondStorageHandle* s, ...);
const char* pond_storage_merge(PondStorageHandle* s, ...);

// Codec
PondResult* pond_pnd2_decode(const uint8_t* blob, size_t blob_len);
int32_t     pond_pnd2_encode_i64(const int64_t* values, size_t n, ...);
```

### 7.1 Language bindings

| Language | Status | How |
|---|---|---|
| Python | ✅ Full | PyO3 (`import pond`) |
| Rust | ✅ Full | Direct (it's the core) |
| Go | ✅ Full | cgo over C ABI |
| C/C++ | ✅ Full | Direct `#include "pond.h"` |

---

## 8. CLI (`pond` command)

### 8.1 Storage discovery

Priority order:
1. `--root <url>` (explicit override)
2. `POND_ROOT` env var
3. `.pond/` marker (walk up from CWD — local FS only)
4. `.` (current directory)

### 8.2 Commands

```
pond init [location]           # Initialize/connect (local path or s3:// URL)
pond write <collection> --json '...' -m "msg"
pond read <collection>
pond branch <collection> <name>
pond checkout <collection> <name> [-b]
pond merge <collection> <source> [-i <target>] -m "msg"
pond branches <collection>
pond history <collection> [-l <limit>]
pond undo <collection> [steps]
pond revert <collection> <commit_hash>
pond ls
pond cat <hash>
pond version
```

### 8.3 S3 support

S3 is a cargo feature (`default = ["s3"]`):
```bash
pond init "s3://bucket/prefix?region=us-east-1&endpoint=https://..."
pond write users --json '[{"id":1}]' -m "first"  # uses POND_ROOT or --root
```

---

## 9. Design Principles

1. **Simple** — ONE format (PND2), ONE commit format (PNPK), ONE concurrency model (CRDT)
2. **Powerful** — branch/merge + CRDT + IVF + streaming + GC + optimize
3. **Performant** — O(1) point lookup, O(1) warm writes, PND2 columnar encoding
4. **Scalable** — linear PUTs, flat GETs, PB-scale via StatsTree
5. **Efficient** — immutable blobs (deduped), O(live) GC, parallel fetch
6. **Beautiful** — shards ARE branches, CRDT = G-Set union, no CAS
7. **Functional** — lakehouse, KV, vector, streaming, notebook, git
8. **Storage-Independent** — no CAS, works on local FS / S3 / R2 / MinIO
   (GCS via S3-interop, not a native backend)

---

## 10. Non-Goals (what Pond deliberately doesn't do)

- **Distributed consensus** — single-writer per collection (CRDT handles multi-writer)
- **Online schema evolution** — collections are schema-less at the kernel level
- **Materialized views** — lenses compute on read (no pre-computation)
- **Cross-collection joins** — each lens operates on one collection
- **Full-text search** — not a search engine (use a lens on top)
- **Streaming ingestion pipelines** — StreamingLens provides storage, not pipelines

These are intentionally out of scope. Pond is a storage substrate, not an
application framework.
