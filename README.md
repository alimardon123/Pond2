# Pond

> *One copy of data on object storage, serving all workloads without
> duplication, with built-in versioning, CRDT concurrency, and
> competitive performance vs specialized systems.*

Pond is a **unified content-addressed storage system** — not another
lakehouse, not another table format, not another Spark.

The core hypothesis: a tiny storage kernel (3 operations, ~200 LOC) is
sufficient for radically different workloads — SQL, vectors, streaming,
KV, Git, notebooks, ML — to be implemented as independent **Lenses** over
a shared immutable substrate, with built-in versioning (branch/merge),
CRDT concurrency (no CAS), and PB-scale performance.

---

## Universal data support — structured, semi-structured, unstructured

Pond treats **all data types as first-class citizens**:

| Data type | How to store | How to query | Example |
|---|---|---|---|
| **Structured** (tabular) | `write_rows()` — INT64, FLOAT64, STRING, BINARY columns | `read_rows()`, `.sql()`, predicates, SIMD filter | User tables, metrics, logs |
| **Semi-structured** (JSON) | `write_rows()` with JSON in STRING columns | `.sql()` on metadata + `json.loads()` on payload | Event streams, API logs, documents |
| **Unstructured** (binary) | `write_rows()` with BINARY columns (video, images, PDFs) | `read_rows()` returns `bytes`, `.sql()` returns `bytes` | Video, photos, audio, model weights |
| **Raw bytes** | `write()` — any format, no structure | `read()` — get bytes by name | Configs, blobs without metadata |

```python
# Structured — typed columns with SIMD-accelerated queries
s.write_rows('users', [('id', [1, 2]), ('name', ['alice', 'bob'])], 'init')
s.read_rows('users', predicates=[('id', '>', 1)])  # AVX2 SIMD filter

# Semi-structured — JSON in STRING columns
s.write_rows('events', [
    ('id', [1, 2]),
    ('event', ['click', 'purchase']),
    ('payload', [json.dumps({'btn': 'buy'}), json.dumps({'item': 'widget', 'price': 9.99})]),
], 'init')

# Unstructured — BINARY column holds raw bytes (video, images, PDFs)
# All operations work uniformly: write_rows, read_rows, .sql(), CRDT, merge_rows
s.write_rows('assets', [
    ('id', [1, 2]),
    ('name', ['video.mp4', 'photo.jpg']),
    ('mime_type', ['video/mp4', 'image/jpeg']),
    ('duration', [120.5, 0.0]),                    # FLOAT64
    ('file_data', [video_bytes, photo_bytes]),      # BINARY — raw bytes inline!
], 'init')
cols = s.read_rows('assets')
cols['file_data'][0]  # → bytes (the actual video)
s.sql("SELECT name FROM assets WHERE duration > 60")  # SQL on metadata

# Raw bytes — no structure needed
s.write('configs/app.json', config_bytes, 'init')
data = s.read('configs/app.json')  # → raw bytes
```

All three types get the same benefits: **versioning** (branch/merge),
**CRDT** (concurrent writes), **content-addressed dedup**, and
**storage-independence** (local FS / S3 / R2 / MinIO).

---

## Architecture

```
Lenses (KV, Vector, Streaming, Lakehouse, OLTP)
  ↓ compose
UnifiedStorage (ONE storage engine — Rust core)
  - write / append / read / point_lookup / iter_rows
  - append_shard / upsert_shard / delete_shard (CRDT, no CAS)
  - read_with_shards (two-level merge: row groups + rows)
  - branch / checkout / merge / revert / history / diff
  - gc / vacuum / optimize (Delta/Iceberg parity)
  ↓
Kernel (3 ops: Write, Read, Ref)
  - ObjectStore trait (local FS, S3, in-memory)
  - PND2 format (ONE binary format for ALL workloads)
  - CollectionManifest (ONE index — flat → StatsTree at PB scale)
  - JSON commit blobs (ONE commit format)
  - Shards (CRDT G-Set) + row-level version vectors
  ↓
Storage Backends
  - LocalFSObjectStore  (Rust, zero deps)
  - S3ObjectStore       (Rust, SigV4 — works with AWS S3, R2, MinIO, etc.)
  - InMemoryObjectStore (Python, for testing)
```

---

## Repository Structure

```
pond_repo/
├── core/                    # Language-AGNOSTIC Rust crates
│   ├── kernel/              # 3 primitives + ObjectStore trait + CRDT
│   ├── storage/             # UnifiedStorage (versioning, branching, shards)
│   ├── codec/               # PND2 encode/decode (all encodings, all vtypes)
│   ├── arrow/               # PND2 → Arrow direct conversion
│   └── s3/                  # S3-compatible object store (SigV4, zero AWS SDK deps)
├── cli/                     # `pond` CLI binary (DuckDB philosophy)
├── bindings/                # Language-specific bindings
│   ├── base/                # Shared C ABI: pond.h, C tests, test blobs
│   ├── python/
│   │   ├── pyo3/            # PyO3 Rust crate (produces pond.so)
│   │   ├── sdk/             # Python SDK (PondStorage, lenses, extensions)
│   │   └── core/            # Python reference kernel (being migrated to Rust)
│   └── go/                  # Go SDK (cgo wrapper around C ABI)
├── lenses/                  # Workload-specific lenses
│   ├── base/                # Lens protocol (C ABI placeholder)
│   ├── keyvalue/
│   │   ├── python/          # KeyValueLens (production)
│   │   └── rust/            # Placeholder for future Rust port
│   ├── lakehouse/{python,rust}/
│   ├── oltp/{python,rust}/
│   ├── streaming/{python,rust}/
│   └── vector/{python,rust}/
├── services/                # Cross-cutting services (transport, schema, replication)
├── pond-labs/               # Experiments and demos
├── tests/                   # All tests (architecture, integration, lens algebra)
├── scripts/                 # Verification scripts (property tests, benchmarks)
├── docs/                    # Documentation
├── tla/                     # TLA+ formal specification
└── archive/                 # Historical code (not active)
```

---

## v1 Features

### Core Engine
- **PND2 columnar format** with 10 types: INT64, FLOAT64, STRING, NULL, BINARY, VARIANT, BOOLEAN, DATE, TIMESTAMP, VECTOR
- **Null bitmap** for INT64/FLOAT64 (Arrow-style, bit=1 = null)
- **Row-level CRDT branch merge** (latest _version wins, tombstone retention for associativity)
- **LRU block cache** (256 entries, Arc sharing, cache invalidation on delete)
- **CSPRNG** for UUIDv7 (/dev/urandom + BCryptGenRandom)
- **Bloom filter** for point lookups (SHA-256 double hashing, 1.1% FPR)
- **PondPack** atomic publication (commit+manifest fused in one blob)
- **Real abort_tx** with is_tx_aborted + tx_status
- **vacuum()** respects preserve_days (time-travel safety) + tracks freed_bytes

### SQL Engine (pure-Rust, in `core/sql/`)
- **SELECT, INSERT, UPDATE, DELETE, MERGE**
- **6 JOIN types**: INNER, LEFT, RIGHT, FULL OUTER, CROSS + subqueries
- **Aggregates**: SUM, COUNT, AVG, MIN, MAX with GROUP BY, HAVING
- **ORDER BY, LIMIT/OFFSET**
- **Parquet read** via arrow-rs (all scalar types)
- **RFC 4180 CSV parser** (quoted fields, embedded newlines, CRLF)
- **File reading**: CSV, TSV, JSON, NDJSON, Parquet

### Performance
- **AVX2+FMA SIMD** (x86_64, 8 f32/instruction) for vector distance
- **NEON SIMD** (aarch64, 4 f32/instruction) — Apple Silicon, AWS Graviton
- **SIMD range filters** wired into columnar_filter
- **Parallel PND2 decode** (rayon, >4 row groups threshold)
- **S3 multipart upload** (100MB threshold, 16MB parts, 4-way parallel)
- **S3 connection pooling** (ureq::Agent with split-phase timeouts)
- **Real XML parser** for ListObjectsV2 (replaces string search)
- **Async I/O** (tokio + reqwest, feature-gated behind `--features async`)

### AI/Agent Features
- **Native VECTOR type** with SIMD distance (L2, cosine, dot)
- **search_vectors** — brute-force k-NN with optional WHERE filter
- **Hybrid search** — BM25 + vector + filter with weighted RRF fusion
- **MCP server** (9 tools: write_rows, read_rows, sql, list_collections, branch, merge, vacuum, get_schema, search_vectors)
- **Streaming reads** (read_rows_stream iterator)
- **UDF pushdown** in SQL WHERE (register_udf)
- **Row-Level Security** (set_rls_policy + auto _tenant column)

### Multi-Language Support
- **Python** (PyO3) — full API with .pyi type stubs (50+ methods)
- **Go SDK** — WriteRows, ReadRows, ReadRowsWithProjection, ReadRowsWithPredicates
- **C ABI** — 36+ functions (shards, tx, maintenance, history, layers)
- **CLI** — write-rows, read-rows, sql, shell (REPL with \l \d \b \history)
- **MCP** — 9 tools for any AI agent (Claude, GPT, etc.)

### Trust & Verification
- **TLA+ specification** (6 invariants pass TLC model check)
- **Property-based CRDT tests** (commutativity, associativity, idempotence, 200 iterations)
- **Chaos tests** (concurrent branch merges under HLC skew)
- **Benchmark suite** (5 criterion groups: write, read, vector search, CRDT merge, upsert)
- **340+ tests** passing, 0 failures

---

## Quick Start

### Using the Rust CLI (recommended)

```bash
# Build the CLI (with S3 support enabled by default)
cargo build -p pond_cli

# Local filesystem (git-style auto-discovery)
cd /var/lib/pond
pond init                          # creates .pond/ marker
pond write users --json '[{"id":1,"name":"alice"}]' -m "first"
pond read users
pond branch users dev
pond checkout -b users dev
pond merge users dev -m "merge"
pond history users
pond ls

# Works from subdirectories too (like git)
cd /var/lib/pond/subdir
pond read users                    # auto-discovers .pond/

# S3-compatible storage (AWS S3, R2, MinIO, etc.)
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
pond init "s3://my-bucket/prod?region=us-east-1"
pond write users --json '[{"id":1}]' -m "first"   # no --root needed!
pond read users

# Cloudflare R2
pond init "s3://bucket/prefix?region=auto&endpoint=https://<account>.r2.cloudflarestorage.com"

# MinIO
pond init "s3://bucket/prefix?region=us-east-1&endpoint=http://localhost:9000"
```

**How it works:** `pond init` creates a `.pond/` marker directory with a
`config` file. Subsequent commands auto-discover it by walking up from CWD
(just like `git` finds `.git/`). No need for `--root` on every command.

`--root` and `POND_ROOT` env var still work as overrides (for scripts/CI).

### Using the Rust Python SDK (recommended)

```python
from pond import Storage

# ONE storage connection — local or S3 (auto-detected)
s = Storage('/var/lib/pond')
# OR: s = Storage('s3://bucket/prefix?region=us-east-1&endpoint=...')

# Raw bytes (JSON or any format)
s.write('users', b'[{"id":1,"name":"alice"}]', 'init')
data = s.read('users')

# Structured PND2 columns (auto-encoding: RLE/DICT/BITPACK/RAW + pruning)
# Auto-adds _rowid (UUIDv7) + _version (HLC) by default — CRDT-compatible
s.write_rows('metrics', [('id', [1, 2, 3]), ('val', [10, 20, 30])], 'init')
cols = s.read_rows('metrics')           # → {'id': [1,2,3], 'val': [10,20,30]}
cols = s.read_rows('metrics', columns=['val'])  # projection
cols = s.read_rows('metrics', predicates=[('id', '>', 1)])  # SIMD-accelerated pruning

# SQL-like row operations (SQL WHERE strings, CRDT shards by default)
s.update_rows('metrics', {'val': 999}, where="id = 2")        # UPDATE ... WHERE
s.delete_rows('metrics', where="id = 3")                       # DELETE FROM ... WHERE
s.merge_rows('metrics', [{'id': 4, 'val': 40}], on='t.id = s.id')  # MERGE / upsert

# Full SQL MERGE with multi-action + column mapping + t./s. prefixes
s.merge_rows('inventory', rows, on='t.id = s.id',
    on_match="UPDATE WHERE t.status = 'low' SET t.qty = s.new_qty; DELETE WHERE s.remove = true",
    on_miss="INSERT WHERE s.qty > 0",
    on_miss_target="DELETE WHERE t.status = 'discontinued'")  # WHEN NOT MATCHED BY SOURCE

# .sql() — full SQL interface (SELECT/UPDATE/DELETE/INSERT/MERGE + JOIN + files)
result = s.sql("SELECT * FROM users WHERE age >= 18 AND city = 'NYC'")
s.sql("UPDATE users SET status = 'active' WHERE age >= 18")
s.sql("SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id")
s.sql("SELECT * FROM 'data.csv' WHERE age > 25")  # read CSV files

# Opt out of CRDT: s.write_rows(..., crdt=False) for raw bulk loads

# Version control (git-like)
s.branch('users', 'dev')
s.checkout_new('users', 'dev')
s.write('users', b'[{"id":2,"name":"bob"}]', 'add bob')
s.checkout('users', 'main')
s.merge('users', 'dev', 'main', 'merge dev')

# Unified indexing — simple / ivf / hnsw via one method (reads from collection)
s.build_index('users', 'by_name', 'simple', config={'key_field': 'name'})
rowid = s.lookup_index('users', 'by_name', 'alice')  # → 'user:1'
# read_rows with equality predicates auto-uses indexes for O(1) lookup

s.build_index('vectors', 'ann', 'ivf',
              config={'n_clusters': 10, 'metric': 'euclidean'})
results = s.search_index('vectors', 'ivf', [0.1, 0.2], k=10, n_probe=5)

s.build_index('vectors', 'ann', 'hnsw',
              config={'m': 16, 'metric': 'l2'})
results = s.search_index('vectors', 'hnsw', [0.1, 0.2], k=10, ef=50)

# CRDT shards — concurrent multi-writer without CAS (updates + deletes)
s.upsert_shard('users', 'w1_001', rows=[{'id': 1, 'name': 'alice'}], key_col='id')
s.delete_shard('users', 'w1_del', rowids=['user:2'], key_col='id')
s.read_rows('users')  # auto-merges HEAD + all shards (latest _version wins)

# Atomic publication (transactions) — NOT full ACID, atomic visibility only
tx = s.begin_tx()
s.append_shard('users', f'{tx}_u', b'{"id":3,"name":"carol"}')
s.append_shard('orders', f'{tx}_o', b'{"id":3,"amount":50.0}')
s.commit_tx(tx, 'add carol + her order')  # both visible atomically

# Semantic Layer — multi-adapter, batch ops, auto-exposure
m = s.layer('sales', adapters=['ossie'], enable_reflection=True)
m.add_datasets(['orders', 'users'])
m.add_metrics({'revenue': 'SUM(orders.amount)', 'count': 'COUNT(orders.id)'})
m.add_dimensions({'country': ('users', 'country', 'string')})
m.add_relationships({'user_orders': ('users', 'orders', 'users.id = orders.user_id')})
m.add_adapter('cube')         # multi-adapter — independent of the spec
m.info()                      # → full spec dict
m.export('ossie')             # optional one-shot export

# Maintenance
s.gc_stats()                    # → {'live': N, 'dead': M, ...}
s.vacuum(preserve_days=7, dry_run=True)
s.optimize()                    # compact shards + flatten manifests
```

**Full API workflow**: see [`docs/API_WORKFLOW.md`](docs/API_WORKFLOW.md) for
the complete end-to-end guide with every method documented.

### Using the Python reference SDK (legacy)

```python
import sys, os
sys.path.insert(0, "bindings/python/core")
sys.path.insert(0, "bindings/python/sdk")

from make_kernel import make_kernel
from pond_storage import PondStorage

kernel = make_kernel("file:///var/lib/pond")
storage = PondStorage(kernel)

storage.write("users", [{"id": 1, "name": "alice"}], key_col="id")
rows = storage.read("users")
storage.branch("users", "dev")
storage.merge("users", "dev")
```

### Using the Go SDK

```go
import "github.com/pond/pond-go/pond"

// Open storage (local FS or S3)
store, _ := pond.OpenStorage("/var/lib/pond")
defer store.Free()

// Write
hash, _ := store.Write("users", []byte(`[{"id":1,"name":"alice"}]`), "initial commit")

// Read
data, _ := store.Read("users")
fmt.Println(string(data))

// Branch + merge
store.Branch("users", "dev")
store.Checkout("users", "dev")
store.Merge("users", "dev", "main", "merge dev")
```

---

## S3-Compatible Storage

Pond's Rust core includes a **from-scratch S3 client** with SigV4 signing.
No AWS SDK dependency — just `sha2` + `ureq` (sync HTTP) + `hex`. This
keeps the binary small and the build fast.

**Supported S3-compatible providers:**
- AWS S3
- Cloudflare R2
- MinIO
- LocalStack
- Wasabi
- DigitalOcean Spaces
- Any S3-compatible API

**URL format:**
```
s3://<bucket>/<prefix>?region=<region>&endpoint=<url>
```

**Credentials** (read from environment):
- `AWS_ACCESS_KEY_ID` (or `AWS_ACCESS_KEY`)
- `AWS_SECRET_ACCESS_KEY` (or `AWS_SECRET_KEY`)
- `AWS_SESSION_TOKEN` (optional, for STS temporary credentials)

**Migration** between local FS and S3 is a straight copy:
```bash
aws s3 sync /var/lib/pond/ s3://my-pond/prod/
aws s3 sync s3://my-pond/prod/ /var/lib/pond/
```
No format conversion needed — blobs and paths use the same layout.

---

## Cross-Language Support

All bindings share the same Rust core (`core/kernel`, `core/storage`, `core/codec`).
The Python PyO3 binding is the most complete — it's the primary development surface.

| Feature | Python (PyO3) | Go (cgo) | C ABI | Rust CLI |
|---|---|---|---|---|
| write / read (raw bytes) | ✅ | ✅ | ✅ | ✅ |
| write_rows / read_rows (PND2) | ✅ | ❌ | ❌ | ❌ |
| branch / checkout / merge | ✅ | ✅ | ✅ | ✅ |
| history / undo / revert | ✅ | ✅ | ✅ | ✅ |
| update_rows / delete_rows | ✅ | ❌ | ❌ | ❌ |
| merge_rows (SQL MERGE) | ✅ | ❌ | ❌ | ❌ |
| .sql() (SELECT/JOIN/INSERT/UPDATE/DELETE/MERGE) | ✅ | ❌ | ❌ | ❌ |
| CRDT shards (upsert/delete/read_with_shards) | ✅ | ❌ | ❌ | ❌ |
| Transactions (begin/commit/abort_tx) | ✅ | ❌ | ❌ | ❌ |
| build_index / search_index / lookup_index | ✅ | ❌ | ❌ | ❌ |
| Semantic Layer (layer/add_metrics/etc.) | ✅ | ❌ | ❌ | ❌ |
| gc_stats / vacuum / optimize | ✅ | ❌ | ✅ (gc/vacuum) | ✅ (gc/vacuum) |
| SIMD-accelerated INT64 filter | ✅ | ❌ | ❌ | ❌ |
| Parallel row group decode | ✅ | ❌ | ❌ | ❌ |

**Go SDK** (`bindings/go/`): wraps the C ABI via cgo. Currently exposes basic
operations (write, read, branch, merge, undo, revert). To add high-level APIs,
extend `bindings/go/pond/pond.go` with cgo calls to the C ABI.

**C ABI** (`bindings/base/pond.h`): the lowest-level interface. Exposes kernel
primitives, storage versioning, and PND2 codec. Other languages (Java, Node)
would wrap this.

**Rust CLI** (`cli/`): a standalone binary for git-like operations. Currently
supports init, write, read, branch, checkout, merge, history, undo, revert,
gc, vacuum. Does not expose structured row operations or .sql().

---

## Design Principles

1. **Simple** — ONE storage format, ONE commit format, ONE concurrency model
2. **Powerful** — branch/merge + CRDT + IVF + HNSW + SQL MERGE + .sql() + semantic layers
3. **Performant** — AVX2 SIMD filters, parallel row group decode, parallel S3 GETs
4. **Scalable** — linear PUTs, flat GETs, PB-scale via StatsTree
5. **Efficient** — immutable blobs (deduped), O(live) GC, parallel fetch
6. **Beautiful** — shards ARE branches, CRDT = G-Set union, no CAS, SQL WHERE strings
7. **Functional** — lakehouse, KV, vector, streaming, semantic, OLTP
8. **Storage-Independent** — no CAS, works on local FS / S3 / R2 / MinIO (GCS interface-ready, not implemented)

---

## Migration Strategy: Python → Rust

Pond is migrating from Python to Rust as the core implementation language:

- **Rust core** (done): kernel, storage, codec, arrow, S3, CLI
- **Python SDK** (current): PyO3 wrapper for codec; Python kernel + lenses still in use
- **Future**: port lenses to Rust, expose via C ABI, Python becomes thin wrapper

New development happens in Rust. Python is maintained for bug fixes only.

---

## Documentation

- [`docs/API_WORKFLOW.md`](docs/API_WORKFLOW.md) — **Full end-to-end API workflow with examples for every method** (start here)
- [`DESIGN_GOALS.md`](DESIGN_GOALS.md) — The 8 design principles, in detail
- [`REPO_ORGANIZATION.md`](REPO_ORGANIZATION.md) — Folder structure rules
- [`PACKAGES.md`](PACKAGES.md) — Package dependency graph
- [`KNOWLEDGE_GRAPH.md`](KNOWLEDGE_GRAPH.md) — Every file, its purpose, its exports
- [`SDK_SPEC.md`](SDK_SPEC.md) — SDK API specification
- [`docs/`](docs/) — Design documents, whitepaper, formal algebras, TLA+ spec
