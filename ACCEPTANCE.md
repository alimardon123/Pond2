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
- Not this cycle: removing the existing S3 CAS commit loop (it is correct and
  tested; it gets superseded by the journal design in a write-path cycle, not
  ripped out mid-read-cycle).
- Not this cycle: Rust proptest suite (next-cycle P1; tracked in CRITIQUE.md).

## Assumptions (recorded, autonomous mode)

- Sandbox has limited disk/RAM: build with default dev profile where
  possible, `cargo clean` when pressure builds, stream all live-R2 I/O.
- The GitHub PAT and R2 credentials live ONLY in `~/.git-credentials` and
  `.env` (both git-ignored, mode 600). NEVER commit secrets.
- Push only to `origin` (alimardon123/Pond2). No other remotes get pushes.
- CI runners are 2-vCPU: keep the bitpack benchmark calibrated (f85a351) —
  do not add uncalibrated long benchmarks.

## This-cycle acceptance (crucible iteration N+1)

1. `ACCEPTANCE.md` + `ARCHITECTURE.md` exist in repo root and capture the
   no-CAS concurrency direction, the Rust-first/CLI-first product shape, and
   the ownership map (this file).
2. pyo3 `read_rows` (+ SQL `WHERE` via the same reader) routes through the
   pruned slab-aware pipeline with projection pushdown, for **all column
   types** (i64, f64, string/bytes, mixed), with a byte-counting test proving
   ≤ 10%-of-full-scan on a pruned workload, and correctness tests vs the old
   path's outputs on mixed data.
3. Zero-warning build: `cargo clippy --workspace --all-targets -- -D warnings`
   clean; `cargo test --workspace` all green; moto S3 suite green; live R2
   suite green (streaming, small footprint).
4. CI green on the pushed HEAD.
5. Honest gap report in the cycle's worklog entry (what remains short of the
   staledb/DuckDB bar).
