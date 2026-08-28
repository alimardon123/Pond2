# CHANGELOG.md — One entry per iteration

> Crucible state file. The deep per-cycle log lives in `worklog.md`
> (append-only); this file tracks iterations and why.

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
