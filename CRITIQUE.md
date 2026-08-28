# CRITIQUE.md — Open findings (append-only until resolved)

> Crucible state file. Each finding: location + root-cause hypothesis.
> Resolved items move to CHANGELOG.md.

## Open

- **C15 (tribunal r3, NIT — duplicate-identical-RG data entries)** — Two
  DATA entries with byte-identical content share an RgIdentity
  `(blob_hash, slab_byte_offset)` but are never plan-filtered (data
  entries ARE the coverage source), so concatenating readers see the
  identical RG twice until the next fold's identity dedup
  (journal.rs compact) heals it. Content addressing makes genuinely
  identical RGs rare (same bytes = same data); CRDT readers collapse by
  _rowid regardless. Fix if it ever matters: dedup identical data-entry
  RG identities in resolve_packs (same mechanism as compact's extension
  dedup). Recorded, not scheduled.
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
- **C8** — SQL executor still swallows HEAD-read errors (`Err(_) => {}`,
  executor.rs read_collection_as_json_rows): a transient S3 500 yields
  silently partial SQL results, while pyo3 propagates. Fix: propagate
  like pyo3 + test.
- **C5 (partial)** — SlabWriter is not the default write path. Journal
  entries are columnar packs (PND2) now — the JSON-shard write surface is
  the remaining gap (python lenses still write JSON shards). Fix: journal
  the shards / SlabWriter default in a later cycle. NOTE (laws-cycle
  finding #1): full-state CRDT merge byte-permutation-invariance needs an
  owner decision on identity-less (legacy) row ordering — legacy rows pass
  through in input order (the ignored `merge_is_permutation_invariant`
  law in tests/laws_crdt.rs documents the counterexample); the CRDT
  substate IS invariant (proven by the crdt_only law). Natural home: this
  item's shard-surface removal.
- **C6** — Naming: docs sometimes say "StalixDB"; the reference project is
  **staledb**. Sweep docs when touched; do not mass-edit untouched files.

## Resolved (moved to CHANGELOG.md entries)

- (2026-08-28) **C7** — the "snapshot + entries → pack list" loop that had
  grown to 5 copies: `journal::resolve_packs()` is THE reader entry point
  (D6, commit 73fde0c); read.rs ×3, lakehouse, vector lenses all delegate.
  (The determine_rowid ×3 / base64 ×2 duplication remains inside their
  crates — reduced, not eliminated.)
- (2026-08-28) **C11** — racing compactors with PARTIAL overlap duplicated
  shared RGs for concatenating readers: the coverage check moved from pack
  granularity to RG granularity (per-plan `only_rgs` sets, D6 commit
  73fde0c); a partially-covered compact contributes ONLY its novel RGs,
  fully-covered compacts drop from the plan, stale loser entries stay raw
  so the next fold's upto deletes their zombie paths. Chain case (multiple
  partially-overlapping compacts) pinned by
  test_c11_chain_of_partial_overlaps_each_novel_once.
- (2026-08-28) **C3** — the proptest suite: laws_crdt (6 laws × 256 cases:
  permutation invariance [finding #1 — legacy-row caveat documented,
  verbatim-ignored], idempotence live+full, both tombstone laws,
  determinism, crdt_only permutation), laws_pman (normalize invariant,
  byte-stable roundtrips, RootManifest laws × 128 cases), laws_journal
  (C9 union law, compact-preserves-state, multi-writer interleavings × 24
  cases on real kernels). Seeds pinned in code — byte-reproducible in CI.
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
