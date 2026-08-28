# CHANGELOG.md — One entry per iteration

> Crucible state file. The deep per-cycle log lives in `worklog.md`
> (append-only); this file tracks iterations and why.

## 2026-08-28 — Crucible iteration N+4: the C5 cycle (journal the CRDT row surface)

- **D7 landed (C5-a)**: `upsert_shard`/`delete_shard` are journal writers —
  stamped rows (`_rowid`/`_version`/`_deleted`, semantics unchanged) encode
  as ONE PND2 row group (the shared `journal::build_rg_from_json_rows`
  encoder) inside ONE journal pack per call (unique per-writer path, plain
  PUT, zero shared objects). The JSON-shard write surface is GONE from the
  Rust core; every pyo3/CLI CRDT row write (incl. the high-level
  update_rows/delete_rows/merge_rows/upload) is probeable, and
  upsert/delete workloads need no LIST on warm reads (the last C2-compat
  surface closes for Rust-written data). `shard_count == 0` is the steady
  state; upsert visibility surfaces through `read_rows` +
  `journal::status`. Legacy `shards/` namespaces stay READ-compat
  (`read_with_shards`/`list_shards`) and compaction-foldable — old repos
  migrate by compacting once. `append_shard` (raw bytes) stays the escape
  hatch.
- **C5-b landed**: `WriteBuffer::flush_internal` packs its staged RGs into
  ONE PSLB slab (per-RG zstd + footer with offsets + incremental INT64
  bloom built at stage time) — N buffered writes flush as ≤2 new blob
  objects (slab + pack), down from N+1; RG entries carry
  (slab_hash, offset, len) and read back through the slab-aware range
  reader identically.
- **FINDING (D7-exposed, fixed same cycle)**: moving CRDT updates into the
  journal made them value-prunable — the pruned reader's pre-filter could
  drop the UPDATE copy (moves OUT of the predicate range) while keeping
  the stale base copy, so the CRDT merge RESURRECTED outdated state
  (SQL `test_where_pushdown_shard_updated_row_disappears`: 3 rows for 2).
  Pre-D7 the hole was unreachable (updates lived in the unfiltered shard
  channel) but LATENT for folded shard RGs. Fix — the D7 reader rule:
  `RowGroupEntry::is_crdt_update_rg()` (a `_deleted` stat WITH REAL
  min/max — the requirement is load-bearing: tribunal r4 finding 1
  proved a name-only check misfires on normalize PLACEHOLDER stats and
  blinded the non-merging readers post-fold, 0 rows for 3); MERGING
  readers exempt CRDT RGs from zone-map/bloom/row-filter; NON-MERGING
  readers (`read_rows_i64`, `read_all_row_groups`, lakehouse/vector
  lenses) skip them (pre-D7 shard-invisible equivalence). The branch
  merge re-encoder now writes REAL stats (was all-None — genuine merged
  CRDT RGs would have been misclassified as base). Pinned by 6
  regressions: live update-OUT-of-range, post-compact update-OUT/INTO
  range, compact-blindness (the tribunal probe), i64-skip,
  resurrection-post-compact, two-writer same-rowid.
- **FINDING (pre-existing flake, fixed)**: `write_rows` generated rowids
  with plain `uuidv7()` (RANDOM within the same millisecond); the CRDT
  merge sorts by rowid, so a fresh batch's read-back order was random per
  run (test_write_rows_auto_crdt failed ~5/6 of runs). Fix:
  `uuidv7_monotonic()` (existing kernel fn) in write_rows_inner,
  shard::upsert_shard's generated rowids, AND the SQL MERGE insert path
  (executor.rs — tribunal r4 finding 3) — batch order is now
  deterministic insertion order.
- **Scoping discovery (recorded)**: the Python lens stack
  (keyvalue/streaming/oltp + pure-Python UnifiedStorage on
  PondMinimal/ObjectStoreNativeKernel) is a SEPARATE storage world from
  the Rust core — shared path conventions, different ref mechanisms, no
  interop with CLI/pyo3/Go today (verified: no test mixes the worlds).
  C5-python residual: SDK delegation to the Rust core via pyo3 per D1 —
  never a Python journal port.
- Validation: pond_storage 220 tests green (163 unit + 26 journal + 12
  upsert_journal + 5 chaos + laws ×13); full workspace green; SQL
  integration 30/30 (incl. the fixed regression); clippy `-D warnings`
  clean; pytest pyo3 suites 28/28 + test_all 25 pass / 2 env-skips; moto
  S3 32/32; live Cloudflare R2 35/35; lens laws (pure Python world,
  untouched) all 6 laws compliant.

## 2026-08-28 — Crucible iteration N+3: the LAWS cycle (cron-2026-08-28-1100)

- **D6 landed (C7 + C11)**: `journal::resolve_packs()` is THE reader entry
  point — the "snapshot + entries → pack list" loop that had grown to 5
  copies (read.rs ×3, lakehouse, vector lenses) now lives exactly once.
  Coverage moved from PACK granularity to RG granularity: a partially-
  covered COMPACT entry contributes ONLY its novel row groups (per-plan
  `only_rgs` sets applied before zone-map/bloom pruning and I/O), fully-
  covered compacts drop from the plan, and stale loser entries stay live in
  the RAW view so the next compact's `upto` deletes their zombie paths.
  `compact` builds its union manifest from the SAME frozen view's plans
  (build_pack_plans — no second resolve) with identity dedup at extension
  time. Concatenating readers now see each RG exactly once under racing
  compactors with partial overlap (pre-D6: 15 rows for 10 logical; chain
  case: 20 for 15). (commit 73fde0c)
- **C3 landed — the proptest suites**: 11 property laws, ~1700 pinned-seed
  cases. laws_crdt (6 laws × 256 cases): merge permutation invariance,
  idempotence (live AND full state), both tombstone laws, determinism.
  laws_pman (3 laws × 128 cases + boundary): normalize-aligns-stats-to-
  schema (the invariant whose violation corrupted PMAN v2 in the wild),
  byte-stable encode→decode roundtrips, RootManifest laws. laws_journal
  (3 laws × 24 cases on REAL kernels): the C9 union law (read-after-N-
  appends, exact-once), compact-preserves-state-for-every-reader (same
  kernel + fresh reader + nothing-left-live), multi-writer interleavings
  (2-3 writers, shuffled physical PUT order → exact union, fold-stable).
  Seeds pinned IN CODE (`RngSeed::Fixed`, distinct per file) — CI runs are
  byte-reproducible.
- **FINDING #1 (the review doing its job)**: the full-state CRDT merge
  permutation law FAILS on legacy rows — rows without `_rowid` pass
  through in INPUT order, so the C10 doc claim ("byte-identical state
  under any permutation") was overstated. Shrunk counterexample (9×`{}` +
  1 versioned legacy row) persisted in laws_crdt.proptest-regressions;
  law kept VERBATIM as `#[ignore]`; the CRDT substate (rows carrying
  `_rowid` — the actual C10 subject) proven invariant by the crdt_only
  sub-law; shard.rs doc comment now carries the caveat. Production impact
  today: none (resolve_packs feeds a deterministic plan order). Full fix
  needs an owner decision on identity-less row ordering — tracked with C5.
- **Tribunal r3 (fresh-context critic): PASS-WITH-REPAIRS** — no HIGH
  findings; principles 9/8/9/8/8/8/8/8; done-statements 9/7/9/10/9/9.
  Empirically verified: all test gates, the finding's counterexample, seed
  determinism (identical results across runs), strategy attack rates (66%
  rowid collisions, 23% version ties, 40% legacy-row cases — the laws
  genuinely attack), zero secrets. Repairs landed same-cycle: the
  multi-writer interleaving law (closes the item-2 contract gap), the C11
  CHAIN test (multiple partially-overlapping compacts — tribunal finding
  4), the shard.rs legacy-row doc caveat (finding 3), state-file sync
  (finding 1). C15 opened (duplicate-identical-RG data entries, NIT).
- Validation: 567 workspace tests listed (542→567; pond_storage alone
  203 pass / 0 fail / 1 ignored), clippy -D warnings clean, moto S3 32/32,
  live Cloudflare R2 green, pytest 25 pass / 2 env-only skips.

## 2026-08-28 — Crucible iteration N+2: THE no-CAS journal cycle (cron-2026-08-28-0353)

- **The P0 that framed everything**: discovered + verified empirically that
  `write_rows`/SQL INSERT LOST HISTORY — every commit's manifest held only
  its own row group while reads resolved only HEAD (2 writes × 10 rows →
  10 readable). The CAS loop was semantically vacuous. Recorded as C9.
- **D3 landed (the owner's core architectural directive)**: per-writer
  immutable journal at
  `collections/<c>/_branches/<b>/journal/<writer_id>/<seq:012>` — every
  structured write path (write_rows, write_rows_i64, _packed, _slab,
  SlabWriter) appends a PNPK pack at a unique path via plain PUT: always
  succeeds, zero retries, identical on localfs/S3/R2, ZERO shared-object
  writes. The CAS loop is DELETED; `put_path_if` has no production callers.
- Readers = snapshot ∪ live entries: `read_rows_json_pruned` (and the i64 /
  raw-RG / lakehouse / vector paths) resolve the journal view — branch-ref
  snapshot + parallel per-writer epoch probes from the `upto` watermark —
  and CRDT-merge (LWW by _version, total tiebreak (_version, _rowid,
  payload) — C10 fixed in shard.rs + pyo3). Warm path: ZERO LISTs on a
  fresh discovery cache (TTL default 1s, `POND_JOURNAL_TTL_MS=0` for exact
  freshness); CountingStore proves the exact GET/LIST counts.
- Compaction: manifest-level union fold (O(metadata), never reads data
  blobs), fold pack appended to the compactor's log FIRST then branch ref
  LWW-advanced (benign race — every ref value is a valid folded state),
  delta-only entry deletes, upto-map pruning, LocalFS empty-writer-dir
  cleanup, auto-compact at `POND_JOURNAL_AUTO_COMPACT` (default 32) with a
  bootstrap fold on first write. Tombstones are deletion-as-data (kept in
  folds — no resurrection). CLI: `pond journal-status` + `pond compact` +
  journal-aware `history` (folds list preserves folded write messages).
- **Three latent bugs surfaced by the journal and fixed**: (1) PMAN v2
  corrupts when RG stats count ≠ schema count (manifest::
  normalize_rgs_to_schema now guards every multi-origin manifest);
  (2) branch-merge's re-encoder had no bool arm — `_deleted: true` encoded
  as i64 1 and VT_BOOLEAN columns vanished on decode → deleted rows
  RESURRECTED after every merge; (3) pyo3 compact_shards deleted shards
  without folding them (data-loss footgun) — now a real fold.
- **Tribunal r2 verdict: FAIL (repairable) → repaired same-cycle**:
  F1 raw-`write()` journal blindness (probes died at fold-deleted gaps;
  fixed by watermark carry into write()/merge() commits + plain-commit
  upto parsing + child-process regression test), F2 compactor-race row
  duplication for concatenating readers (resolve_view drops fully-covered
  compact entries; partial-overlap residual = C11), F3 quadratic delete
  loop, F6 unbounded writer growth, F8 test-honesty (child-process and
  fabricated-multi-writer tests added). Harness: 511 → 542 tests, clippy
  clean, moto 32/32, live R2 35/35.
- Behavior changes (honest): `write-rows` now APPENDS (journal semantics)
  where it previously REPLACED the branch HEAD — the old behavior was the
  C9 data-loss bug, and the moto/R2 tests asserted it (updated with the
  rationale); `pond history` may show `journal compaction` entries between
  user writes; raw `write()` still replaces the folded base but carries
  the journal watermark.

## 2026-08-28 — Crucible iteration N+1 (cycle cron-2026-08-28-0120)

- Phase 0/1: wrote `ACCEPTANCE.md` + `ARCHITECTURE.md` + `SCORECARD.md` +
  `CRITIQUE.md`. Settled decisions recorded: D1 Rust-first, D2 CLI as
  first-class product (DuckDB methodology), D3 no-CAS concurrency
  (immutable journal + CRDT merge + epoch probe; existing S3 CAS loop is
  transitional), D4 one pruned read pipeline, D5 atomic publish not ACID.
  Reference class corrected to **staledb** (not StalixDB).
- Phase 2/3 (C1 resolved): `read_rows_json_pruned` in core/storage — the
  ONE pruned pipeline generalized to all column types (leaf pruning →
  zone maps → parallel blooms → coalesced slab range reads → projection
  pushdown → columnar pre-filter). pyo3 `read_rows`, SQL executor (WHERE
  conjunction pushdown), and CLI `read-rows` all route through it. The
  old pyo3/CLI full-scan paths (which also mis-decoded PMAN v3 roots)
  are deleted. +VT_BOOLEAN decode/filter. Byte-savings tests (CountingStore):
  pruned ≤ 10% of full-scan bytes on standalone + slab layouts.
- CI deselect removal (P0): pond_sql re-included in the Rust test job
  (exclusion was collateral from f1932ce; its tests are pure Rust, fast,
  green). pond_python stays excluded (no Python dev headers on runners —
  documented in the workflow header).
- Cargo.lock: committed the zstd dependency entry left out of 66ecca3.
- Behavior note (tribunal r1 finding 2): CLI `read-rows` now renders
  VT_BINARY as `__bin_b64__:…` and VT_VARIANT as parsed JSON (was: Null
  for both) — consistent with pyo3 `read_rows` and arguably a fix for
  silently-nulled binary data; recorded here because the commit message
  only claimed VT_BOOLEAN parity.
- Tribunal round 1 verdict: PASS-WITH-REPAIRS (all principles ≥8, all
  done-statements ≥9). Repairs landed same-cycle: type-strict string
  pre-filter (+ regression test incl. name="" vs `!= 5`), degenerate
  projection-intersection full-decode guard, stale CI comment, docs;
  C7 (helper dedup) + C8 (executor HEAD-error swallowing) opened in
  CRITIQUE.md.
- (prior cycles, summary): cache wiring (04ac316), close-race fix
  (d6e43c1), cache invalidation (7b621b9), CAS write loop (172a3da),
  shard robustness (2239e65), native zstd (66ecca3), tmp-name uniqueness
  (f49a12d), CI benchmark calibration (f85a351). Details: `worklog.md`.
