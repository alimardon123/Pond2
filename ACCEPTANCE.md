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
  cross-process HLC persistence, C8 executor error parity, C5-python
  (the pure-Python SDK/lens stack's shard surface — separate storage world,
  see ARCHITECTURE.md D7 boundary note; endgame is SDK delegation to the
  Rust core, not a Python journal port).

## Assumptions (recorded, autonomous mode)

- Sandbox has limited disk/RAM: build with default dev profile where
  possible, `cargo clean` when pressure builds, stream all live-R2 I/O.
- The GitHub PAT and R2 credentials live ONLY in `~/.git-credentials` and
  `.env` (both git-ignored, mode 600). NEVER commit secrets.
- Push only to `origin` (alimardon123/Pond2). No other remotes get pushes.
- CI runners are 2-vCPU: keep the bitpack benchmark calibrated (f85a351) —
  do not add uncalibrated long benchmarks.

## This-cycle acceptance (crucible iteration N+4 — the C5 cycle)

Mission for this cycle: **kill the JSON-shard write surface in the Rust
core** (C5-a): `upsert_shard`/`delete_shard` — and every high-level
operation built on them (pyo3 update_rows/delete_rows/merge_rows/upload) —
become journal writers (PND2 columnar packs at probeable per-writer paths,
zero shared objects, warm reads without LIST). Plus C5-b: buffered
multi-batch flushes land as ONE PSLB slab. Scoping discovery recorded
this cycle: the Python lens stack (keyvalue/streaming/oltp + pure-Python
UnifiedStorage on PondMinimal/ObjectStoreNativeKernel) is a SEPARATE
storage world from the Rust core — it shares path CONVENTIONS but not ref
mechanisms, and does not interop with CLI/pyo3/Go today. Its shard surface
is the C5-python residual (D1 says delegate to Rust, never port the
journal semantics to Python).

1. **C5-a — journal the CRDT row surface**: `shard::upsert_shard` and
   `shard::delete_shard` stamp rows exactly as today (_rowid UUIDv7,
   _version HLC, _deleted tombstones) but append ONE journal pack per call
   (stamped rows → PND2 RG → manifest → `journal::append_pack`) instead of
   a JSON blob at a `shards/` ref. No JSON shard blob is written anywhere
   in the Rust core afterwards.
2. **Semantics preserved, pinned by tests**: upsert → `read_rows` returns
   the live rows (CRDT merge across journal packs — the existing
   read.rs:1079 merge); tombstones suppress; resurrection (later live
   version) works; two concurrent writers' upserts union; round-trip
   through a FRESH process (empty caches) sees the same rows (the C9
   law applied to the upsert surface). Existing shard tests' SEMANTICS
   re-pinned against the journal-era surface (shard_count 0, rows
   visible via read_rows).
3. **Legacy compat**: `read_with_shards`/`list_shards`/`shard_count`
   keep reading pre-migration shards (old repos stay readable);
   `compact` still folds them; a test pins MIXED state (pre-existing
   JSON shard + new journal upsert) reading correctly through both the
   shard-compat reader and read_rows.
4. **C5-b — SlabWriter default for buffered flush**:
   `WriteBuffer::flush_internal` packs its staged RGs into ONE PSLB slab
   blob (footer: offsets + bloom) before the journal append — N buffered
   writes flush as ≤ 2 new blob objects (slab + pack), read back
   identically through the slab-aware reader (byte-count + row-equality
   tests). Descope path: if SlabWriter integration reveals a real format
   blocker, record it in CRITIQUE with evidence and deliver C5-a alone —
   the tribunal judges the honesty of the descope.
5. **pyo3 surface honesty**: `shard_count`/`read_with_shards`/
   `compact_shards` docstrings marked legacy-compat; upsert/delete
   visibility surfaces through the journal (`read_rows`,
   `journal::status` live_entries). Every pyo3/pytest assertion that
   pinned shard-era behavior updated to journal-era expectations.
6. Zero-warning build: `cargo clippy --workspace --all-targets -- -D
   warnings` clean; `cargo test --workspace` green; pytest green (incl.
   the pyo3 suites that exercise upsert/update/delete/merge/upload);
   moto S3 green; live R2 green (streaming, small footprint); lens laws
   untouched-green.
7. CI green on the pushed HEAD.
8. Honest gap report: C5-python residual (separate world, SDK delegation
   endgame), finding #1 disposition note (journal-era upserts/deletes
   ALWAYS carry _rowid — the identity-less input class shrinks to
   pre-migration data + explicit `write_rows_no_crdt`).
