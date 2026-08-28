# Builder spec — N+4 C5 cycle: journal the CRDT row surface + slab-packed buffered flush

> Crucible builder contract. Task ID: **1** (builder). You MUST read
> `/home/z/my-project/worklog.md` (all prior cycle entries) BEFORE working,
> and MUST append your own entry (Task ID: 1) when done. Do NOT commit,
> push, or touch git state — the orchestrator commits after verification.

## Context (what already exists — read these first)

- `ARCHITECTURE.md` D3 (journal), D6 (read plan), **D7 (NEW — this cycle's
  contract: the CRDT row surface journals)**, and the Python-stack boundary
  note.
- `ACCEPTANCE.md` "This-cycle acceptance (crucible iteration N+4)" — the
  8 items. Items 1–5 are yours; 6–8 are the gates.
- `core/storage/src/journal.rs` — `append_pack` (the append entry point,
  auto-compaction + bootstrap fold included), `build_rg_from_json_rows`
  (PRIVATE, ~line 1185: JSON rows → typed columns → PND2 RG + stats —
  your reuse target), `compact` (folds legacy shards ALREADY — keep),
  `status`, `history`.
- `core/storage/src/shard.rs` — the migration target. `upsert_shard` /
  `delete_shard` currently stamp rows then JSON-serialize + `append_shard`
  (blob + `shards/` ref). `merge_rows_by_rowid` / `filter_live_rows` /
  `crdt_row_greater` are the merge law — DO NOT change their semantics.
  `append_shard` (raw bytes) stays as an escape hatch. `list_shards` /
  `read_with_shards` / `shard_count` / `clear_shards` stay as
  legacy-compat readers.
- `core/storage/src/read.rs` `read_rows_json_pruned` (~line 1040) — the
  flagship reader: resolves the D6 plan, runs the pruned pipeline per pack,
  CRDT-merges at ~line 1079. Journal-pack upserts are ALREADY visible
  here. Update the stale doc comment at ~line 1023 ("Shards remain the
  caller's CRDT responsibility — the python lenses still write them") to
  the D7 reality.
- `core/storage/src/write.rs` — `write_rows_inner` (~line 240 commit_obj
  construction: parent/index/timestamp — mirror it), `SlabWriter`
  (~line 695: slab packing + PSLB footer + journal-append at flush).
- `core/storage/src/write_buffer.rs` — `WriteBuffer` (C5-b target):
  `write_rows_buffered` stages ONE PND2 blob per call; `flush_internal`
  lands them as ONE journal pack but N separate blob objects.
- `bindings/python/pyo3/src/lib.rs` — shard methods ~3101–3260 (docstrings
  → legacy-compat wording), high-level ops (update_rows/delete_rows/
  merge_rows/upload) call `shard::upsert_shard`/`delete_shard` directly —
  they migrate automatically; verify each.
- Tests: `core/storage/tests/journal_test.rs` (child-process pattern +
  CountingStore pattern exist here), `core/storage/tests/laws_*.rs`
  (pinned seeds — do not touch), pyo3 pytest suites
  (`tests/integration/test_merge_advanced.py`, `test_beautiful_api.py`,
  `test_api_demo.py`, `test_sql_where.py`, `tests/test_all.py`) — grep
  them for `shard_count|read_with_shards|upsert_shard|delete_shard` and
  update shard-era assertions to journal-era expectations.

## Deliverable 1 — C5-a: `upsert_shard`/`delete_shard` write journal packs

1. `shard::upsert_shard(kernel, collection, branch, shard_name, rows,
   key_col, hlc)`:
   - Stamp rows EXACTLY as today (`_rowid` UUIDv7 if absent, `_version`
     HLC tick, `_deleted: false`).
   - Encode the stamped rows as ONE PND2 row group (reuse
     `journal::build_rg_from_json_rows` — make it `pub(crate)` or move it
     to a shared module; minimal churn wins).
   - Build a `CollectionManifest` (schema from the RG's column stats,
     key_col) and append via `journal::append_pack` with a commit_obj
     mirroring `write_rows_inner` (parent = branch_ref resolve, index+1,
     timestamp, message `upsert_shard` — include shard_name in the message
     so `journal::history` keeps per-write visibility).
   - Pass `key_fields` = key_col (auto-compaction merge key).
   - Empty-rows edge: return early WITHOUT appending (document + test) —
     an empty journal pack buys nothing. Pick the honest signature
     behavior (empty String return = nothing written, matching
     Python-side `append_shard`'s `""` convention) and pin it.
2. `shard::delete_shard`: tombstones (unchanged stamping: `_rowid`,
   `_deleted: true`, `_version`, key_col backfill) → same journal-pack
   path. Message `delete_shard`.
3. NO JSON shard blob, NO `shards/` ref write from these two functions.
   `append_shard` (raw bytes) unchanged (escape hatch).
4. pyo3: keep all existing method signatures (Python API stability).
   Update docstrings of `append_shard`/`upsert_shard`/`delete_shard`/
   `read_with_shards`/`shard_count`/`compact_shards` to D7 wording.
   `read_with_shards` stays legacy-compat — its callers must not be
   blinded by NEW upserts: check every pyo3 caller of `read_with_shards`
   (Rust lenses, CLI, tests); if a production caller needs journal-era
   upsert visibility, route it through `read_rows_json_pruned` instead
   (report in worklog what you found).

## Deliverable 2 — C5-b: `WriteBuffer::flush_internal` packs ONE PSLB slab

- Pack the staged RGs into ONE slab blob (mirror `SlabWriter`'s slab
  format: sequential RGs + PSLB footer with offsets — reuse its internals
  rather than re-encoding the format) so N buffered writes flush as
  ≤ 2 new blob objects (slab + pack). RG entries in the flushed manifest
  get `slab_byte_offset`/`slab_byte_len` pointing into the slab.
- Readback invariant: `read_rows` over the flushed collection returns the
  identical rows as before the change (slab-aware reader already handles
  slab RGs — verify, don't re-implement).
- Tests: (a) byte/object-count test with a counting store — 3 buffered
  writes + flush ⇒ ≤ 2 new blobs, was 4; (b) row-equality test vs the
  pre-change behavior.
- DESCOPE PATH (honest, not silent): if the slab format genuinely cannot
  take the staged RGs (e.g. compression mismatch, footer assumptions),
  STOP, write the evidence into CRITIQUE.md (new finding) + your worklog
  entry, and deliver C5-a alone. The tribunal judges the honesty.

## Deliverable 3 — tests (semantics re-pinned, journal era)

In `core/storage/tests/` (extend `journal_test.rs` and/or a new
`upsert_journal_test.rs`; unit tests near the code where idiomatic):

1. upsert → `read_rows_json_pruned` returns the live rows (values,
   `_rowid` stability across upserts of the same rowid).
2. delete tombstone suppresses; resurrection (later live `_version`)
   works.
3. Two writers (two `UnifiedStorage`/kernel instances or the
   child-process pattern) upsert concurrently → union visible to a fresh
   reader (empty caches — C9 law on the upsert surface).
4. MIXED legacy state: hand-write a JSON shard via `shard::append_shard`
   (escape hatch still writes shards/) + a journal upsert →
   `read_with_shards` sees the legacy shard AND `read_rows` sees both
   (shard via compact fold or the reader's shard union — check what
   `read_rows` does with legacy shards today and pin the ACTUAL correct
   behavior; report any gap you find rather than papering over it).
5. `journal::status` shows the upsert entries as live entries;
   `compact` folds them; post-compact fresh read identical.
6. shard.rs unit tests: re-pin `upsert_shard`/`delete_shard` tests to the
   journal surface (was: read the shard blob + JSON parse; now: read_rows
   roundtrip + `shard_count == 0`).
7. Existing `test_merge_advanced.py` (pyo3 merge over upserts/deletes)
   must pass UNCHANGED if the semantics hold — if an assertion encodes
   shard-era internals (shard_count, raw blob reads), update it to
   journal-era expectations and say so in the worklog.

## Environment facts (from prior cycles — verified)

- cargo lives at `~/.cargo/bin` (prepend to PATH). Build RELEASE-only
  (`--release`); target/debug was deleted for disk pressure — do not
  recreate it. If disk fills, delete `target/release` incremental dirs.
- pytest runs under `/home/z/.venv` python (`python3 -m pip` for installs;
  pytest spawns `sys.executable`). pyo3 `pond` module + CLI binary must be
  rebuilt (`maturin develop --release` equivalent for this repo is
  `cargo build --release -p pond_python` + copy per existing scripts —
  check how tests import `pond` and rebuild the same way prior cycles did;
  the worklog records the artifact-restore dance).
- `.env` (repo root) holds R2 creds; moto[server] + flask + duckdb +
  boto3 are installed. Do NOT run live-R2 tests yourself (orchestrator
  runs them post-merge) — but keep them passing locally if cheap.
- Seeds: NEVER touch `tests/laws_*.rs` pinned seeds.

## Validation gates (run ALL, record results verbatim)

1. `cargo test --release -p pond_storage` — all green (incl. new tests).
2. `cargo test --release -p pond_python` bindings tests (if any) + CLI
   tests green.
3. `cargo clippy --release -p pond_storage -p pond_python --all-targets
   -- -D warnings` clean.
4. pytest pyo3 suites green: `test_merge_advanced.py`,
   `test_beautiful_api.py`, `test_api_demo.py`, `test_sql_where.py`,
   `tests/test_all.py` (the pure-Python world must stay green UNTOUCHED —
   if a pure-Python test breaks, you have crossed the world boundary:
   STOP and re-read D7's boundary note).
5. Full workspace: `cargo test --release --workspace` green.

## STOP path

If you find a REAL semantic break (e.g. journal upserts break the
merge/branch flows, or read_rows double-counts shard+journal rows): STOP,
preserve the failing construction as a test (ignored if necessary, with
the counterexample documented verbatim — the laws-cycle finding-#1
protocol), write the finding to CRITIQUE.md + your worklog entry, and
report back. Findings are successful reviews; silently weakened laws/tests
are failures.

## Worklog protocol

Read `/home/z/my-project/worklog.md` first. When done, APPEND (never
overwrite) an entry:

```
---
Task ID: 1
Agent: shard-journal-builder
Task: <one line>

Work Log:
- <steps>

Stage Summary:
- <artifacts, key decisions, findings, validation results verbatim>
```
