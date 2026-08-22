# Pond — Current Status (August 2026)

> **This document tracks what's done, what's in progress, and what's next.**
> It replaces the archived `MIGRATION_STRATEGY.md` and `NEXT_STEPS_DEEP_REVIEW.md`.

---

## Migration: Python → Rust

Pond is migrating from Python to Rust as the core implementation language.
The Rust core is now the canonical implementation; Python is maintained
for bug fixes only.

### Design Decision: PND2 Storage (Rust + Python)

Both the Rust and Python storage layers use **PND2 binary format** (columnar,
compressed, with stats for pruning). The Rust PND2 codec implements all encodings
(RLE, DICT, BITPACK, RAW) with auto-selection, zstd compression, and all value
types (INT64, FLOAT64, STRING, BINARY, NULL, VARIANT, BOOLEAN, DATE, TIMESTAMP,
VECTOR). PND2 is the universal format — a Rust-written collection can be read
by Python and vice versa.

**Current state (August 2026):**
- Rust PND2 codec: encode (all encodings, all vtypes) + decode (all encodings)
- Rust storage: `write_rows_i64()` with PND2 auto-encoding, `read_rows_i64()` with predicate pruning + column projection
- Rust PondPack (`write_rows_i64_packed()`) — commit+manifest in ONE blob
- Python UnifiedStorage stores PND2 blobs — same format, same codec
- IVF Bug 10 FIXED (Rust) — per-cluster blob references for true I/O reduction
  Note: Python IVF still has Bug 10 open (reads all vectors)

### Done (Rust core)

| Component | Path | Status |
|---|---|---|
| Kernel (3 primitives + ObjectStore trait) | `core/kernel/` | ✅ Done |
| CRDT (UUIDv7, HLC, upsert/delete/merge) | `core/kernel/src/crdt.rs` | ✅ Done |
| LocalFSObjectStore | `core/kernel/src/object_store.rs` | ✅ Done |
| S3ObjectStore (SigV4 from scratch) | `core/s3/` | ✅ Done |
| UnifiedStorage (versioning, branching, shards) | `core/storage/` | ✅ Done |
| PND2 codec — decode (all encodings, all vtypes) | `core/codec/` | ✅ Done |
| PND2 codec — zstd decompression | `core/codec/` | ✅ Done (feature flag) |
| PND2 codec — encode (RLE, DICT, BITPACK, RAW + auto-select) | `core/codec/` | ✅ Done |
| PND2 → Arrow bridge | `core/arrow/` | ✅ Done |
| PND2 storage write path (write_rows_i64) | `core/storage/src/write.rs` | ✅ Done |
| PND2 storage read path (read_rows_i64) | `core/storage/src/read.rs` | ✅ Done (pruning + projection) |
| PondPack (PNPK) format | `core/storage/src/pond_pack.rs` | ✅ Done |
| GarbageCollector / vacuum | `core/storage/src/maintenance.rs` | ✅ Done |
| IVF Index (Bug 10 fixed) | `lenses/vector/rust/` | ✅ Done (per-cluster blob refs) |
| CLI (`pond` command, local + S3 + auto-discovery) | `cli/` | ✅ Done |
| C ABI (pond.h — kernel + storage + codec + S3) | `bindings/base/pond.h` | ✅ Done |
| Go SDK (full storage access via cgo) | `bindings/go/` | ✅ Done |
| Python PyO3 wrapper (codec + storage) | `bindings/python/pyo3/` | ✅ Done |
| Parallel S3 batch operations | `core/s3/` | ✅ Done (32 concurrent threads) |
| KeyValueLens (Rust port) | `lenses/keyvalue/rust/` | ✅ Done (core API) |
| StreamingLens (Rust port) | `lenses/streaming/rust/` | ✅ Done (core API) |
| OLTPLens (Rust port) | `lenses/oltp/rust/` | ✅ Done (core API) |

### In Progress (Python still in use)

| Component | Path | Status |
|---|---|---|
| Python reference kernel | `bindings/python/core/` | Maintained (bug fixes only) |
| Python SDK (PondStorage, lenses) | `bindings/python/sdk/` | Maintained (bug fixes only) |
| Python UnifiedStorage (PND2, 5767 LOC) | `bindings/python/sdk/extensions/physical_structures/` | Production (PND2 format) |
| LakehouseLens, VectorLens (Python) | `lenses/{name}/python/` | Production (Python) | Rust core API exists (see Done table) |
| base_lens.py (PondLens) | `bindings/python/sdk/base_lens.py` | Production (5 Python lenses extend it) |

### Not Started (Future — prioritized by impact)

| Component | Path | Priority | Notes |
|---|---|---|---|
| LakehouseLens (Rust production polish) | `lenses/lakehouse/rust/` | HIGH | Core API done; needs DuckDB SQL pushdown |
| VectorLens (Rust production polish) | `lenses/vector/rust/` | HIGH | Core API done; IVF Bug 10 fixed |
| eval_predicate_encoded | `core/codec/` | MEDIUM | Vortex-style pruning without decode |
| StatsTree | `core/storage/` | LOW | PB-scale hierarchical stats (defer) |
| Lens C ABI protocol | `lenses/base/pond_lens.h` | LOW | Placeholder only |
| Python IVF Bug 10 fix | `bindings/python/sdk/extensions/indexing/ivf_index.py` | MEDIUM | Python IVF still reads ALL vectors |

---

## Test Coverage

| Suite | Count | Status |
|---|---|---|
| Rust unit tests (cargo test --workspace) | ~391 | ✅ All pass |
| CLI integration tests | 17 | ✅ All pass |
| S3 unit tests (SigV4, HMAC, URL encoding) | 6 | ✅ All pass |
| S3 mock server tests (moto) | 12 | ✅ All pass |
| S3 real R2 tests (Cloudflare R2) | 7 | ✅ All pass |
| Go SDK tests | 10 | ✅ All pass |
| Python pytest suite | 25 | ✅ All pass (2 skipped: R2/S3 env) |
| KNOWLEDGE_GRAPH coverage | 236/236 | ✅ 100% |

---

## Storage Backend Support

| Backend | Rust | Python | Notes |
|---|---|---|---|
| Local filesystem | ✅ | ✅ | `core/kernel/LocalFSObjectStore` |
| AWS S3 | ✅ | ✅ (boto3) | `core/s3/S3ObjectStore` (SigV4 from scratch) |
| Cloudflare R2 | ✅ | ✅ (boto3) | Verified against real R2 |
| MinIO | ✅ | ✅ (boto3) | S3-compatible |
| LocalStack | ✅ | ✅ (boto3) | S3-compatible |
| Wasabi | ✅ | ✅ (boto3) | S3-compatible |
| DigitalOcean Spaces | ✅ | ✅ (boto3) | S3-compatible |
| In-memory | ❌ | ✅ | Python only (for testing) |
| GCS | ❌ | ❌ | Future (S3-compatible API works via interop) |

---

## Cross-Language Support

| Language | Status | How |
|---|---|---|
| Python | ✅ Full | PyO3 (codec + storage) + Python SDK (lenses) |
| Rust | ✅ Full | Direct (it's the core) |
| Go | ✅ Full | cgo over C ABI (kernel + storage + codec) |
| C/C++ | ✅ Full | Direct C ABI (`#include "pond.h"`) |

---

## Architecture (Current)

```
Lenses (KV, Vector, Streaming, Lakehouse, OLTP)
  ↓
UnifiedStorage (Rust core) — core/storage/
  ↓
Kernel (3 ops: Write, Read, Ref) — core/kernel/
  ↓
ObjectStore trait
  ├── LocalFSObjectStore (Rust, core/kernel/)
  ├── S3ObjectStore (Rust, core/s3/ — SigV4 from scratch)
  └── InMemoryObjectStore (Python, testing only)
```

**Storage format:**
- Rust storage: PND2 binary (columnar, compressed, with stats) — same as Python
- Python UnifiedStorage: PND2 binary (columnar, compressed, with stats)
- Both use the same PND2 codec — fully compatible, zero-copy decode via Rust

---

## Key Architectural Decisions

1. **Rust core, Python first-class SDK** — Rust is canonical; Python gets PyO3 wrappers
2. **Unified C ABI** — one `pond.h` for all languages (kernel + storage + codec + S3)
3. **SigV4 from scratch** — no AWS SDK dependency, keeps binary small
4. **Sync HTTP by default, async opt-in** — `ureq` for sync; `reqwest`+`tokio` behind `feature = "async"`
5. **S3 as a separate crate** — `core/s3/` has HTTP deps; `core/kernel/` stays minimal
6. **Lens rust/python subdirs** — each lens has both, Python is production today
7. **Cargo features for S3 and zstd** — `default = ["s3", "zstd"]`, can disable
8. **PND2 storage in Rust** — `write_rows_i64()` uses PND2 auto-encoding; Rust lenses use JSON via `storage.write()` (simpler path for lens-level data)
9. **PondLens base class** — Python lenses extend `PondLens` (base_lens.py) for shared branch/list/history; Rust lenses own UnifiedStorage directly
10. **.pond/config lives at storage root** — not in local CWD for remote storage

---

## Known Gaps (from deep audit)

1. **Python IVF Bug 10 unfixed** — Python IVF still reads ALL vectors (`n_probe` has no effect on I/O). Rust IVF is fixed (per-cluster blob refs).
2. **Rust merge lacks row-level CRDT** — row-group LWW only (acceptable for JSON storage)
3. **SDK_SPEC.md needs refresh** — some sections could be updated to reflect the Rust storage layer
4. **Python unified_storage.py is 5,767 LOC monolith** — needs splitting into modules (Rust port is already modular)
5. **No catalog service** — needed for lakehouse ecosystem adoption
6. **No partitioning / Z-Order** — needed for large-table scan performance
7. **No snapshot isolation for long-running queries** — `read_with_shards` sees mid-flight writes
8. **`compact_after_commit=True` default** — KV lens compacts after every commit (perf bug)
