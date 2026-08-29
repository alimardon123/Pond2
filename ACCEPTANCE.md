# ACCEPTANCE.md — Pond2 Definition of Done

> Crucible state file. Written in Phase 0; amended only deliberately, with the
> change logged in CHANGELOG.md.

## Mission (restated)

Build Pond2 as **the fastest storage system at PB scale on object storage** —
faster than **staledb**, DuckDB-on-S3, Databricks LTAP, and the Databricks RT
engine — by keeping object-storage round-trips to the bare minimum and serving
hot data from a local-disk smart cache in single-digit milliseconds. One
substrate, five lenses (KV / Lakehouse / Vector / Streaming / OLTP), any data
structure, multi-user.

**Main language is RUST.** Python (pyo3) and other language bindings stay
thin. The CLI is a first-class product: a single lightweight-but-powerful
binary — the DuckDB methodology (one static-ish binary that is both an
embedded library and a full engine).

## Reference class (named, never adjectives)

| Reference | What they set the bar for |
|---|---|
| **staledb** | Local-disk smart cache over object storage; sub-10ms warm reads (NOTE: the project is "staledb", not "StalixDB" — earlier docs said StalixDB; correct the name wherever it appears) |
| **DuckDB-on-S3** | Single-binary analytical power; columnar scans + predicate/projection pushdown over remote data |
| **Databricks LTAP + RT engine** | Lakehouse-scale latency expectations; what "fast at PB scale" means commercially |

## Definition of done — project level

The project is done only when every statement below is demonstrably true:

1. Every advertised capability works end-to-end right now through the **pyo3
   binding** and the **CLI** — no stubs, no demo-data-only paths.
2. The flagship read API (`read_rows`) and SQL `WHERE` execute through the
   **pruned slab-aware columnar pipeline** (leaf pruning → zone-map pruning →
   bloom pre-check → slab range reads + coalescing → projection pushdown),
   identical in spirit to `read_rows_i64` — never through a full-scan
   fallback.
3. Multi-writer correctness WITHOUT CAS as the primary mechanism: concurrency
   is solved by CRDT/architectural design (immutable unique-path commits,
   deterministic merge), not compare-and-swap retry loops. CAS is at most a
   transitional, backend-specific fallback — the target architecture does not
   depend on it (it has no localfs equivalent and creates retry storms).
4. Warm-path reads do zero uncacheable LISTs per query (shard visibility must
   come from cached/probeable metadata, not per-read prefix listings).
5. CI (`.github/workflows/view-laws.yml`) is green on `origin/main`.

## Numeric budgets (hard constraints, measured — never estimated)

- Warm cached read (blob in local disk cache): **< 10 ms** end-to-end from pyo3
  (target: single-digit ms, staledb-class).
- Cold read round-trips for a selective point query on a packed collection:
  **≤ 2 object-store GETs** before data bytes (HEAD/manifest resolve + one
  ranged slab GET).
- Full-scan of N row groups inside one slab: **1 range GET** after coalescing
  (already implemented — must not regress).
- `read_rows` with predicates on a pruned workload must read **≤ 10% of the
  bytes** read by the old full-scan path (measured via a counting
  ObjectStore in tests).
- R2 live-testing budget: **≤ 8 GB total** (free tier is 10 GB); all live I/O
  **streaming** — never buffer whole multi-GB blobs in memory or on disk.

## Non-goals (scope fence)

- No distributed transaction coordinator, no 2PC, no consensus service
  (Raft/ZooKeeper) — atomic-publish + CRDT semantics only.
- **No CAS-centric concurrency architecture** (see DoD #3). Do not add new
  CAS dependency; design it away instead.
- No reimplementation of SQL planning beyond what the pond_sql crate already
  does (pushdown into the reader, not a new optimizer).
- Not this cycle: string zone maps, BPTX index wiring for journal entries,
  cross-process HLC persistence, C16 row-level merge compaction, C15
  identity dedup, and **C5-python PHASE 2** (format unification: the Python
  lens world's manifest/encoding layer still differs from PND2/PMAN — this
  cycle lands the SUBSTRATE delegation only; see ARCHITECTURE.md D8).

## Assumptions (recorded, autonomous mode)

- Sandbox has limited disk/RAM: build with default dev profile where
  possible, `cargo clean` when pressure builds, stream all live-R2 I/O.
- The GitHub PAT and R2 credentials live ONLY in `~/.git-credentials` and
  `.env` (both git-ignored, mode 600). NEVER commit secrets.
- Push only to `origin` (alimardon123/Pond2). No other remotes get pushes.
- CI runners are 2-vCPU: keep the bitpack benchmark calibrated (f85a351) —
  do not add uncalibrated long benchmarks.

## This-cycle acceptance (crucible iteration N+5 — the Python-substrate delegation cycle)

Mission for this cycle: **open the C5-python fence — phase 1, the I/O
substrate**. The pure-Python kernel stack (ObjectStoreNativeKernel +
UnifiedStorage + SDK + the Python lens world) stops needing its own object
store implementations: a new `pond.ObjectStore` pyo3 class exposes the Rust
core's ObjectStore trait (LocalFS + S3/R2 with the 3-tier cache), a Python
`RustObjectStore` adapter implements the exact LocalFSObjectStore/
S3ObjectStore duck interface on top of it (byte-identical layouts — the
stores were verified to converge: `blobs/{h[:2]}/{h}` + JSON `{"hash":...}`
refs at `{path}`), and `make_kernel()` prefers the Rust backend whenever
the compiled module is importable (graceful pure-Python fallback, env
override `POND_PY_BACKEND=python|rust|auto`). The Python world keeps its
formats/semantics this cycle; what it delegates is the I/O layer — so it
inherits the Rust S3 client (SigV4, connection reuse, disk cache) instead
of boto3 per-call, and Python-written stores become readable by the Rust
kernel (same bytes, same keys). Plus two same-cycle repairs: **C8** (SQL
executor must PROPAGATE HEAD-read errors like pyo3 does — no more silent
partial SQL results on transient S3 failures) and **C13** (document the
raw-path `pond read` journal staleness in the CLI help + docs; routing
deferred with an honest CRITIQUE note).

1. **`pond.ObjectStore` pyo3 surface** (bindings/python/pyo3/src/lib.rs +
   pond.pyi): constructors `ObjectStore(base_dir)` (LocalFS) and
   `ObjectStore.from_s3(url, cache_dir=None)` (S3/R2 — cache wired via
   the same `s3_kernel_cached` path Storage uses); semantic methods
   mapping the trait — `put_blob/get_blob/get_blob_range/get_blob_suffix/
   put_blob_batch/get_blob_batch/blob_exists/delete_blob/put_path/get_path/
   delete_path/list_paths/list_dirs/store_id`; plus a raw-key escape
   hatch `get_raw/put_raw/delete_raw/list_raw` (new default-Unsupported
   trait methods, implemented by LocalFS + S3 only — the Python adapter
   uses them for OLD-layout fallback reads `b/…` + `paths/…` and
   `list_all_blob_hashes`). I/O-bound methods release the GIL
   (`py.allow_threads`) so the Python kernel's ThreadPoolExecutor batches
   actually parallelize.
2. **`RustObjectStore` adapter** (bindings/python/core/rust_object_store.py):
   duck-compatible with LocalFSObjectStore/S3ObjectStore (put_blob,
   get_blob, put/get_blob_batch, has_blob, delete_blob,
   list_all_blob_hashes, put_path, get_path, delete_path, list_paths,
   stats, reset_stats, print_stats). NEW-layout ops delegate to the Rust
   semantic methods; OLD-layout blob reads (`b/{h[:2]}/{h}`) and old refs
   (`paths/{path}`) fall back via raw ops; hash computation comes from the
   Rust `put_blob` return (sha256, identical to Python's `hash_bytes`).
3. **`make_kernel` wiring** (bindings/python/core/make_kernel.py): new
   `backend="auto"` kwarg — "auto" uses RustObjectStore when `import pond`
   succeeds (file:// → `pond.ObjectStore(base_dir)`; s3:// →
   `pond.ObjectStore.from_s3` with region/endpoint/creds translated to URL
   query params + env, mirroring pond.Storage's constructor), falls back to
   the pure-Python stores (with a one-time stderr note) when the module is
   absent or construction fails; `POND_PY_BACKEND=python` forces the
   fallback; `memory://` unchanged (InMemoryObjectStore).
4. **Byte-interop pinned by tests**: (a) a store written via pure-Python
   LocalFSObjectStore reads identically through RustObjectStore and
   vice versa (same dir, same files — blobs + JSON refs byte-identical);
   (b) `hash_bytes` equality across the boundary for identical data;
   (c) old-layout fallback: a `b/…` blob + `paths/…` ref written by hand
   (pre-layout-change shape) resolves through RustObjectStore.
5. **The Python kernel stack works end-to-end on the Rust store**:
   ObjectStoreNativeKernel(RustObjectStore) + UnifiedStorage write/read/
   point-lookup round trip; PondStorage (SDK) write/read round trip;
   make_kernel("file://…", backend="auto") picks Rust when built and
   pure-Python when not (both asserted in tests via forced backends).
6. **S3 via the Rust client, pinned by moto**: ObjectStoreNativeKernel on
   RustObjectStore.from_s3(moto endpoint) — put/get/list/delete round
   trips, ref resolution, kernel read-your-write; proves the Python world
   no longer needs boto3 for its object I/O.
7. **C8 repaired**: `read_collection_as_json_rows` (core/sql executor)
   propagates HEAD-read errors (Err no longer swallowed); a test pins a
   failing store → SQL query returns an error, not silently-partial rows.
8. **C13 documented**: `pond read --help` + docs/API_WORKFLOW.md note that
   the raw path resolves the branch ref (a CACHE of the last fold) and can
   be journal-stale for structured data; use `pond read-rows`/SQL for
   journal-aware reads. CRITIQUE keeps C13 open for the routing fix.
9. Zero-warning build: `cargo clippy --workspace --all-targets -- -D
   warnings` clean (pond_python included in clippy; excluded from cargo
   test per CI policy — its surface is covered by the pytest job);
   `cargo test --workspace --exclude pond_python --exclude pond_sql` green
   (CI command) + pond_sql suite green; pytest green (existing suites +
   the new tests in tests/test_rust_object_store.py); lens laws
   untouched-green; moto S3 (Rust suite) green.
10. CI green on the pushed HEAD (the pytest job builds pond_python and
    runs the new tests through `import pond`).
11. Honest gap report: C5-python phase 2 (format unification — Python
    manifests/encoding vs PND2/PMAN; the SDK's semantics still live in
    Python), C12/C14/C15/C16 dispositions unchanged, C13 routing deferred.
