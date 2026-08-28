# CHANGELOG.md — One entry per iteration

> Crucible state file. The deep per-cycle log lives in `worklog.md`
> (append-only); this file tracks iterations and why.

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
