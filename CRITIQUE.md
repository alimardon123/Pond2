# CRITIQUE.md — Open findings (append-only until resolved)

> Crucible state file. Each finding: location + root-cause hypothesis.
> Resolved items move to CHANGELOG.md.

## Open

- **C11 (tribunal r1 residual, F2-partial)** — Racing compactors with
  PARTIAL overlap (the loser folded entries the winner missed): the loser's
  fold pack is kept whole by resolve_view's coverage skip, so the RGs it
  shares with the winner's snapshot still duplicate for the CONCATENATING
  readers (read_rows_i64, read_all_row_groups, lakehouse/vector lenses).
  CRDT readers are unaffected (rows collapse by _rowid). Root cause: the
  coverage check is pack-granular; RG-level plan filtering (per-entry
  `only_rgs` sets in the resolved view) is the fix. Next cycle, with the
  C3 proptest harness.
- **C12 (tribunal r1, F7 — accepted trade-off)** — The lenient non-PND2
  skip in read.rs (~1197): a corrupted/truncated PND2 RG blob is silently
  skipped as raw data where the reader previously errored loudly. Raw
  `write()` blobs must be skipped (journal folds pull them into read
  paths), and content-addressing makes silent corruption unlikely — but a
  checksum-verify option (or erroring when the RG's declared n_rows > 0
  and the blob is non-PND2 AND the manifest schema is non-empty) would be
  stricter. Revisit with the C3 codec proptest cycle.
- **C13 (tribunal r1, F10)** — `pond read` / `read::read` (raw path)
  resolves branch_ref only — journal-stale for structured data. Pre-
  existing raw-path semantics; now a UX trap that the ref is officially
  "a cache". Fix: route through the journal resolver or document in the
  CLI help.
- **C14 (bootstrap-fold race, bounded)** — A reader whose branch_ref GET
  predates the FIRST-ever fold (ref=None) probes from seq 1, dies at the
  fold's deleted entry, and observes an EMPTY state — a valid CRDT prefix
  but a surprising one. Window: only around a collection's first
  bootstrap fold. Root cause: probes cannot discover an entry past a gap
  without the watermark. Acceptable (next read is correct); revisit if
  read-your-writes on fresh collections matters.
- **C7** — Mirrored-semantics duplication: `determine_rowid` in 3 copies
  (read.rs, pyo3 lib.rs, sql executor), `base64_encode` in 2, AND now the
  "snapshot + entries → pack list" loop in 5 copies (read.rs ×3,
  lakehouse, vector lenses). Root cause: pond_storage couldn't host shared
  helpers for the pyo3 crate historically. Fix: export
  `journal::resolve_packs()` + shared helpers from pond_storage, delegate
  everywhere. Next cycle.
- **C8** — SQL executor still swallows HEAD-read errors (`Err(_) => {}`,
  executor.rs read_collection_as_json_rows): a transient S3 500 yields
  silently partial SQL results, while pyo3 propagates. Fix: propagate
  like pyo3 + test.
- **C3** — Rust proptest suite is zero for PMAN/PNPK/PSLB codecs (the
  journal itself now has property-style tests: permutation determinism,
  interleaved prefixes, fabricated multi-writer logs). Root cause: harness
  debt. Fix: dedicated proptest cycle — also the right home for the PMAN
  v2 schema/stats-count format check that the journal cycle surfaced (see
  manifest::normalize_rgs_to_schema).
- **C5 (partial)** — SlabWriter is not the default write path. Journal
  entries are columnar packs (PND2) now — the JSON-shard write surface is
  the remaining gap (python lenses still write JSON shards). Fix: journal
  the shards / SlabWriter default in a later cycle.
- **C6** — Naming: docs sometimes say "StalixDB"; the reference project is
  **staledb**. Sweep docs when touched; do not mass-edit untouched files.

## Resolved (moved to CHANGELOG.md entries)

- (2026-08-28) **C9** — write history lost (every commit after the first
  hid its parent's rows; SQL INSERT data loss): the D3 per-writer journal
  — every write appends an immutable pack at a unique path, readers union
  snapshot + live entries, CRDT merge. Also fixed en route: the CAS loop
  was semantically vacuous (losers rebuilt packs that still excluded the
  winner's data).
- (2026-08-28) **C10** — order-dependent CRDT merge tiebreak: total order
  (version, rowid, payload) in shard.rs merge_rows_by_rowid AND the pyo3
  chunk merge; permutation tests at 3 levels.
- (2026-08-28) **C2** — per-read uncacheable shard LIST killed the warm
  path: journal-era reads perform ZERO LISTs on a fresh discovery cache
  (TTL-bounded, POND_JOURNAL_TTL_MS=0 for exact freshness); parallel
  per-writer epoch probes at computable paths. Legacy shards keep their
  LIST (compat) until the python lenses migrate.
- (2026-08-28) **C4** — two redundant derived refs per commit: journal-era
  writes touch ZERO shared objects (no branch ref, no manifest ref, no
  bare collection ref). The branch ref is written only by `compact` (and
  the legacy `write()`/`merge()` base-snapshot paths, which now carry the
  journal watermark).
- (2026-08-28) pyo3 compact_shards data-loss footgun — it deleted shards
  WITHOUT folding them into HEAD; now delegates to journal::compact (real
  fold). Same fix class: merge/branch now fold live state first, and the
  merge keeps tombstones (deletion-as-data) instead of resurrecting the
  source's rows when the CRDT merge empties a conflicting pair.
- (2026-08-28) PMAN v2 latent format bug — manifests whose RG stats count
  ≠ schema count corrupt on encode/decode roundtrip (decoder reads stats
  bytes as slab offsets). First writers to hit it: journal compact's union
  fold and branch merge. Fixed by manifest::normalize_rgs_to_schema (all
  multi-origin manifests normalize per-RG stats to the union schema by
  name).
- (2026-08-28) VT_BOOLEAN tombstone round-trip — branch.rs's merge
  re-encoder encoded `_deleted: true` as i64 1 (no bool arm) and the
  decoder dropped VT_BOOLEAN columns entirely: tombstones read as live
  rows and deleted rows resurrected after every merge. Both arms fixed.
- (2026-08-28) tribunal r1 F1 — raw `write()` between journal writes
  permanently blinded fresh readers (probes died at the fold-deleted gap;
  unrecoverable orphan). Fixed: write()/merge() carry the previous ref's
  `journal.upto` watermark; read_snapshot_upto parses plain commits.
  Child-process regression test (the exact tribunal probe).
- (2026-08-28) tribunal r1 F2 (common case) — racing compactors
  duplicated rows for concatenating readers (loser's fold pack stayed
  live): resolve_view drops a live COMPACT entry whose RG set is fully
  covered by snapshot ∪ data entries. Partial-overlap residual = C11.
- (2026-08-28) tribunal r1 F3/F6 — compaction delete loop was O(total
  seq) per run (quadratic under auto-compact) and writer dirs + upto maps
  grew unboundedly: delta deletes (prev_upto..=upto) + upto pruning +
  LocalFS empty-parent-dir cleanup.
- (2026-08-28) lens-laws CI segfault (SQLite close race) — d6e43c1.
- (2026-08-28) CI bitpack benchmark 291s flake — f85a351.
