# CRITIQUE.md — Open findings (append-only until resolved)

> Crucible state file. Each finding: location + root-cause hypothesis.
> Resolved items move to CHANGELOG.md.

## Open

- **C1 (flagship, this cycle)** — `bindings/python/pyo3/src/lib.rs:5292`
  `read_collection_as_json_rows_filtered`: the flagship `read_rows` API
  reads FULL blobs per row group (`kernel.read_blob`), decodes ALL columns
  (no projection pushdown), and never uses leaf pruning / zone maps /
  blooms / slab range reads / coalescing that `core/storage/src/read.rs`
  already implements for i64. Root cause: the pruned pipeline was built as
  a typed i64 special case rather than the general reader. Fix: generalize
  the pipeline to all column types and route pyo3 (and SQL) through it.
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

- (2026-08-28) pyo3 shard error propagation, shard-id collisions, unified
  slab header reads — 2239e65.
- (2026-08-28) multi-writer lost updates on S3 (transitional CAS loop) —
  172a3da.
- (2026-08-28) block-cache invalidation no-op — 7b621b9.
- (2026-08-28) lens-laws CI segfault (SQLite close race) — d6e43c1.
- (2026-08-28) CI bitpack benchmark 291s flake — f85a351.
