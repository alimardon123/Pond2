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

## This-cycle acceptance (crucible iteration N+6 — the error-channel + codec-laws cycle)

Mission for this cycle: **close the C17 error-channel hole end-to-end,
land the C12 codec proptest laws, and validate against REAL object
storage (R2)**. `ObjectStore::get_path` gains an error channel
(`io::Result<Option<String>>`) so a transient ref-read failure (S3
500/429/timeout) is an ERROR everywhere it occurs — journal snapshot
resolution, epoch probes, branch reads, CAS pre-reads, the Python
surface — never a silent `None` masquerading as "absent" (the empirically
proven blindness: failing ref reads returned EMPTY query results). With
C17 landed, `read::read` (the raw path) routes through the journal
resolver (C13) so raw reads stop being fold-stale for structured data.
In parallel, the two uncovered binary codecs (PNPK packs, PSLB slabs —
C12's natural home, the C3 laws family) get property-based laws. And the
whole storage stack runs once against REAL R2 object storage via an
env-gated live harness (credentials NEVER in the repo, never in CI) —
the journal's delimiter-LIST writer discovery and range reads have only
ever run against moto emulation.

1. **C17 trait refactor**: `ObjectStore::get_path` returns
   `io::Result<Option<String>>` across the trait + every backend (LocalFS
   discriminates NotFound; S3 discriminates 404 via the existing
   `is_s3_not_found`; CachingObjectStore passes errors through WITHOUT
   poisoning the ref cache — errors are not absence; S3's
   `get_path_with_etag` and `get_path_async` follow suit) +
   `PondKernel::resolve` returns `io::Result<Option<String>>` + EVERY
   caller updated: production read paths propagate with context
   ("Failed to read branch ref for collection 'x': …"), `journal::
   probe_writer` becomes fallible (a failed epoch probe is a TRUNCATED
   journal view — an error, not an empty suffix), existence probes
   become `?.is_some()`, tests unwrap. The pyo3 `pond.ObjectStore.
   get_path` surface raises `IOError` on failure (PyResult), keeping
   `None` for absent; the Python adapter propagates.
2. **C17 pinned by tests**: a ref-outage store (get_path fails, blobs
   healthy) makes (a) `read_rows_json_pruned` return an error, (b) the
   SQL executor return an error (not empty rows), (c) `journal::
   resolve_view` return an error (not an empty view) — the exact
   blindness the C8 test's first version exposed. REVERT-VERIFIED: at
   least one new test fails against the pre-refactor code.
3. **C13 raw-reader routing**: `read::read` (and the CLI `pond read`)
   resolves the journal view via `journal::resolve_packs` — snapshot
   pack + live journal entries at RG granularity — and concatenates the
   live RG bytes (byte-exact for raw-write collections; journal-union
   for mixed usage). A raw read after `write_rows` (no fold yet) returns
   the journal data, not "has no commits" / stale bytes. Docs updated:
   API_WORKFLOW §2.1 caveat replaced with the new contract; `pond read
   --help` updated; CRITIQUE C13 closed.
4. **C12 codec laws**: proptest suites for PNPK (`pond_pack::
   encode_pack`/`decode_pack`/`is_pack` — JSON ref + manifest bytes +
   optional bloom round-trip through arbitrary-byte and degenerate
   inputs, magic discrimination) and PSLB (`slab::encode_slab`/
   `decode_slab`/`decode_slab_tail`/`decode_slab_footer`, the
   COMPRESSED variant, `plan_ranges` — multi-RG round-trips, footer
   offset/len invariants, tail-magic discrimination, bloom-flag
   consistency, malformed-truncation rejection). Follow the
   laws_pman.rs patterns (deterministic seeds, boxed strategies,
   regressions file honored).
5. **R2 live harness**: an env-gated integration script/test
   (POND_R2_ENDPOINT + POND_R2_BUCKET + POND_R2_ACCESS_KEY_ID +
   POND_R2_SECRET_ACCESS_KEY absent → SKIP silently) exercising the REAL
   S3 client end-to-end: blob put/get/range, refs, list_paths,
   list_dirs delimiter semantics, the journal write→probe→read cycle,
   cache warm-read timing. Run locally against the owner-provided R2
   bucket; ZERO credential material in the repo, .gitignore-safe, CI
   unaffected. Results (timings, round-trip counts) recorded in the
   worklog.
6. **Zero-warning build + full validation**: `cargo clippy --workspace
   --all-targets -- -D warnings` clean; `cargo test --workspace
   --exclude pond_python --exclude pond_sql` green (CI command) +
   pond_sql green; pytest green (test_all + test_rust_object_store +
   any new surface); lens laws green; moto green; the R2 harness green
   when credentials present.
7. **CI green on the pushed HEAD** (credentials available this cycle:
   PAT stored at /home/z/.secrets/pond-credentials.env, never in the
   repo).
8. **Honest gap report**: C5-python phase 2 unchanged (conditional),
   C14/C15/C16 unchanged, C12's lenient-skip DECISION may be revisited
   in the laws' light (documented either way), new findings recorded
   with locations + root-cause hypotheses.
