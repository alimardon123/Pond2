# Pond Package Structure

Pond is organized into a clean layer hierarchy. Every package is
removable without breaking any lower layer (Design Goal 3.4 Scalable).

## Structure (current)

```
pond_repo/
│
├── README.md                    # 5-minute intro (start here)
├── DESIGN_GOALS.md              # 7 design principles + roadmap
├── REPO_ORGANIZATION.md         # Folder rules, naming, promotion process
├── PACKAGES.md                  # This file
├── SDK_SPEC.md                  # Authoritative SDK contract
├── KNOWLEDGE_GRAPH.md           # Navigational map of the repo
├── worklog.md                   # Append-only research log
│
├── core/                        # Layer 0: Rust core (canonical implementation)
│   ├── kernel/                  # PondKernel — 3 primitives (Write/Read/Ref) + CRDT + HLC
│   ├── storage/                 # UnifiedStorage — versioning, branching, shards, GC, PondPack
│   ├── codec/                   # PND2 codec — encode/decode (RLE/DICT/BITPACK/RAW) + C ABI
│   ├── arrow/                   # PND2 → Arrow bridge
│   ├── s3/                      # S3ObjectStore — SigV4 from scratch, no AWS SDK dep
│   ├── sql/                     # SQL engine — parser, WHERE clauses, executor
│   └── build.sh                 # Unified build script
│
├── lenses/                      # Layer 1: Lens implementations (Python + Rust)
│   ├── base/                    # Shared lens infrastructure
│   │   └── pond_lens.h          # Lens C ABI protocol (placeholder)
│   ├── keyvalue/                # KeyValueLens (KV storage with staging + commit)
│   │   ├── python/              # Python — extends PondLens
│   │   └── rust/                # Rust — owns UnifiedStorage directly
│   ├── lakehouse/               # LakehouseLens (tabular: INSERT, time travel, SQL pushdown)
│   │   ├── python/              # Python — extends PondLens
│   │   └── rust/                # Rust — core API done
│   ├── vector/                  # VectorLens (packed binary vectors + k-NN search)
│   │   ├── python/              # Python — extends PondLens
│   │   └── rust/                # Rust — IVF Bug 10 fixed (per-cluster blob refs)
│   ├── streaming/               # StreamingLens (chunked segments, range reads)
│   │   ├── python/              # Python — extends PondLens
│   │   └── rust/                # Rust — owns UnifiedStorage directly
│   └── oltp/                    # OLTPLens (in-memory memtable + batch flush)
│       ├── python/              # Python — extends PondLens
│       └── rust/                # Rust — owns UnifiedStorage directly
│
├── extensions/                  # Layer 1.5: Optional extensions
│   ├── indexes/                 # Vector and scalar indexes
│   │   ├── ivf/rust/            # IVF ANN index (Bug 10 fixed — per-cluster blob refs)
│   │   ├── hnsw/rust/           # HNSW graph ANN index
│   │   └── simple/rust/         # Simple B-tree index
│   └── semantic/                # Semantic model adapters
│       ├── base/rust/           # SemanticModelAdapter trait
│       └── ossie/rust/          # Ossie adapter (placeholder)
│
├── cli/                         # `pond` CLI binary (Rust)
│   └── src/main.rs              # init, read, write, branch, merge, history, ls, cat
│
├── mcp-server/                  # MCP server (Rust)
│
├── bindings/                    # Cross-language bindings
│   ├── base/                    # C ABI header + test blobs
│   │   ├── pond.h               # Unified C ABI (kernel + storage + codec + S3)
│   │   ├── test_c_abi.c         # 131-check codec C ABI test
│   │   ├── test_storage_c_abi.c # Storage C ABI test
│   │   └── test_blobs/          # 7 binary blobs (all encodings x vtypes)
│   ├── python/
│   │   ├── core/                # Python reference kernel (~274 LOC) + storage backends
│   │   │   ├── kernel.py        # PondMinimal — 3 ops + batch I/O
│   │   │   ├── local_fs_object_store.py
│   │   │   ├── s3_object_store.py
│   │   │   └── make_kernel.py   # make_kernel(url) factory
│   │   ├── sdk/                 # Python SDK + extensions
│   │   │   ├── base_lens.py     # PondLens — shared base (all 5 Python lenses extend it)
│   │   │   ├── pond_storage.py  # PondStorage — unified SDK class
│   │   │   ├── pond_config.py   # PondConfig — persistent settings
│   │   │   ├── row_query.py     # LensQuery — lazy query builder
│   │   │   ├── uuid7.py         # UUIDv7 time-ordered UUID
│   │   │   ├── hlc.py           # Hybrid Logical Clock
│   │   │   ├── maintenance.py   # Tombstone helpers (RFC-0008)
│   │   │   └── extensions/
│   │   │       ├── indexing/    # IVF (Python, Bug 10 open), HNSW, CollectionIndexer
│   │   │       ├── maintenance/ # GarbageCollector (vacuum)
│   │   │       ├── semantic/    # SemanticMixin + OssieAdapter
│   │   │       └── physical_structures/ # UnifiedStorage + PND2 (5,767 LOC)
│   │   └── pyo3/                # PyO3 wrapper (produces pond.so)
│   └── go/                      # Go SDK (cgo over C ABI)
│       ├── pond/                # Public Go API
│       └── internal/cabi/       # Private cgo layer
│
├── services/                    # Cross-cutting Python services
│   ├── transport/               # Transport Layer (compression + encryption)
│   ├── schema/                  # Schema Registry (versioned schemas)
│   └── replication/             # Replication Coordinator
│
├── pond-labs/                   # Development & experimental code (NOT production)
│   ├── lenses/                  # Lab lens prototypes
│   ├── tracks/                  # Lab tracks (compat, benchmarks, case studies)
│   ├── demos/                   # Demonstration scripts
│   └── benchmarks/              # Performance benchmarks
│
├── tests/                       # All tests, organized by purpose
│   ├── test_all.py              # Single pytest entry point (25 tests)
│   ├── architecture/            # 18 architecture laws (executable spec)
│   ├── lens_algebra/            # RFC-0007 6-law property tests
│   └── integration/             # Integration tests (pruning, projection, etc.)
│
├── scripts/                     # Verification scripts
├── docs/                        # Documentation (whitepaper, formal algebras, RFCs)
├── tla/                         # TLA+ formal specification
└── archive/                     # Historical code (preserved, NOT active)
```

## Dependency rules

```
core/kernel (3 primitives, CRDT, HLC, ObjectStore trait — zero external deps)
    ↓
core/storage (UnifiedStorage — versioning, branching, shards, PondPack, GC)
core/codec (PND2 encode/decode — zero external deps, statically linkable)
core/s3 (S3ObjectStore — SigV4 from scratch, HTTP deps)
core/arrow (PND2 → Arrow bridge)
core/sql (SQL parser, WHERE, executor)
    ↓                              ↓
bindings/python/pyo3           bindings/go/ (cgo over C ABI)
(PyO3 wrapper → pond.so)
    ↓
bindings/python/sdk (PondStorage, PondLens, extensions)
    ↓
lenses/ (all 5: keyvalue, lakehouse, vector, streaming, oltp)
    ↓
pond-labs/ (experimental code, depends on everything)
```

**Rules:**
- No lens depends on another lens.
- All 5 Python lenses extend `PondLens` (base_lens.py).
- Rust lenses own `UnifiedStorage` directly (no shared base struct).
- `core/codec` has ZERO external dependencies (statically linkable from Go/Java/Node).
- `core/s3` has HTTP deps; `core/kernel` stays minimal.
- Extensions are data-side (collection-level), not lens-side.
- Services depend only on `bindings/python/core` (not on SDK or lenses).
- pond-labs depends on everything (it's experimental).

## The weekly question

> If I deleted everything except `core/` and `bindings/`, would the
> architecture still make sense? (DESIGN_GOALS.md §4)

Yes. The Rust core (kernel + storage + codec + s3) and the C ABI are
self-contained. Every lens, extension, service, and lab code can be
removed without affecting lower layers.
