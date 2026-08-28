# CRITIQUE.md — Open findings (append-only until resolved)

> Crucible state file. Each finding: location + root-cause hypothesis.
> Resolved items move to CHANGELOG.md.

## Open

- **C7** — Mirrored-semantics duplication: `determine_rowid` now exists in 3
  copies (read.rs, pyo3 lib.rs, sql executor) and `base64_encode` in 2.
  Root cause: pond_storage couldn't host shared helpers for the pyo3 crate
  historically. Fix: export from pond_storage, delegate everywhere.
- **C8** — SQL executor still swallows HEAD-read errors (`Err(_) => {}`,
  executor.rs read_collection_as_json_rows): a transient S3 500 yields
  silently partial SQL results, while pyo3 propagates (2239e65 fixed shards
  only). Root cause: predates this cycle; preserved deliberately to avoid
  behavior churn mid-read-cycle. Fix: propagate like pyo3 + test.
- **C1 (RESOLVED this cycle)** — moved to Resolved below.
- **C2** — `core/storage/src/shard.rs:49` `list_shards` runs a prefix LIST
  per read (uncacheable): warm-path sub-10ms impossible with live shards.
  Root cause: shard visibility lives in the ref namespace, not in
  probeable/cachable metadata. Fix direction: journal/snapshot metadata
  (ARCHITECTURE D3) or shard-list-in-manifest.
- **C3** — Rust proptest suite is zero for PMAN/PNPK/PSLB codecs and CRDT
  merge laws. Root cause: harness debt. Fix: dedicated proptest cycle.
- **C4** — Two redundant derived refs per commit (manifest_ref + bare
  collection) — extra PUTs per write. Fix after reader verification.
- **C5** — SlabWriter is not the default write path (packed slabs happen
  only via explicit paths); shards are JSON. Fix: write-path cycle that
  also lands D3 journal.
- **C6** — Naming: docs sometimes say "StalixDB"; the reference project is
  **staledb**. Sweep docs when touched; do not mass-edit untouched files.

## Resolved (moved to CHANGELOG.md entries)

- (2026-08-28) **C1** — flagship read path: pyo3 `read_rows`, SQL executor
  (WHERE conjunction pushdown), and CLI `read-rows` all route through
  `read_rows_json_pruned` (leaf → zone maps → blooms → coalesced slab
  ranges → projection → columnar pre-filter); old full-scan readers
  deleted (they also mis-decoded PMAN v3 roots); +type-strict pre-filter
  (tribunal r1 finding 1: `name != 5` vs `name=""` regression) and
  degenerate-intersection full-decode guard (finding 3). 59352dd + repair.
- (2026-08-28) pyo3 shard error propagation, shard-id collisions, unified
  slab header reads — 2239e65.
- (2026-08-28) multi-writer lost updates on S3 (transitional CAS loop) —
  172a3da.
- (2026-08-28) block-cache invalidation no-op — 7b621b9.
- (2026-08-28) lens-laws CI segfault (SQLite close race) — d6e43c1.
- (2026-08-28) CI bitpack benchmark 291s flake — f85a351.
