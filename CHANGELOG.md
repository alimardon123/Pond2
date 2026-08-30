# CHANGELOG.md — One entry per iteration

> Crucible state file. The deep per-cycle log lives in `worklog.md`
> (append-only); this file tracks iterations and why.

## 2026-08-30 — Crucible iteration N+7: C20 short-hash cat (sandbox-recovery rebuild)

- **Context**: the N+7 builder cycle completed its C20 work but left it
  UNCOMMITTED; the sandbox was reset before the commit/push. This cycle
  re-cloned, re-established credentials/toolchain, verified origin/main
  CI green on aadf04d (N+6), and re-delivered C20 from the worklog
  record + fresh implementation. No user-visible spec drift: resolution
  semantics, error shapes, and test coverage match the lost build.
- **C20 landed (`pond cat <short-hash>` prefix resolution)**: 6–63-char
  lowercase-hex prefixes resolve against the blobs/ tree on EVERY
  backend — including the default S3+cache configuration that produced
  the live R2 finding (prefixed-S3 `list_paths` filters blob keys and
  `CachingObjectStore` deliberately refuses the raw hatch, so the
  kernel's own listing surfaces cannot see `blobs/` at all). The CLI
  builds a throwaway raw-listing handle from the storage root URL
  (LocalFS / pure-URL-parsed S3 — no connections; the kernel and its
  3-tier cache stay the READ path), PARSES the hash out of each key
  (only well-formed 64-char hex components — never whole-key
  string-match), and reports: one match → the blob's bytes; zero → the
  historical "no blob with prefix" error verbatim; many → ambiguity
  error with up to 5 sorted candidates + "... and K more". A listing
  failure exits loudly (C17 law: Err ≠ absence). Bonus crash fix found
  during root-cause: 0/1-char args previously PANICKED in the store
  layer (`blob_path` slices `hash[..2]`, exit 101) — now a clean
  exit-1 error. `pond cat --help` documents the full contract. Pinned
  by 6 new CLI integration tests incl. a deterministic in-memory
  birthday grind for the ambiguity case (first 6-char collision at
  grind i=5652, cross-checked with an independent python sha256 probe)
  and the below-minimum gate (a 4-char prefix that uniquely matches
  must NOT resolve).
- **Live R2 verification**: `scripts/test_rust_s3_r2.py` now 36/36 —
  step 10's `cat <12-char-prefix>` RESOLVES the commit blob on live
  prefixed R2 (was SKIP pre-C20). The step's assertion was also
  corrected: the 12-char prefix from `pond write` is the COMMIT hash,
  so a resolved cat returns the commit JSON (asserted structurally:
  manifest + timestamp fields), not the raw payload — the old
  `stdout == payload` check could never have passed for a commit hash.
- **C21 opened (adjacent gap)**: the same S3 `list_paths` blob-filter
  blinds every `list_names_prefix("blobs/…")` caller on prefixed
  stores — maintenance GC/vacuum enumerate 0 blobs there (silent
  under-collection). Recorded in CRITIQUE with a fix shape.

## 2026-08-30 — Crucible iteration N+6: the error-channel + codec-laws + live-R2 cycle (C17 + C13 + C12)

- **C17 landed (D9 — the ref-surface error channel)**:
  `ObjectStore::get_path` returns `io::Result<Option<String>>` across
  the trait and every backend — LocalFS discriminates NotFound, S3
  discriminates 404 via the shared `is_s3_not_found` (a corrupt ref
  body is `InvalidData`, not absence), CachingObjectStore propagates
  errors WITHOUT poisoning the ref cache, `get_path_with_etag` +
  `get_path_async` follow suit. `PondKernel::resolve` delegates bare
  and the full 112-caller sweep landed: journal snapshot resolution,
  branch/transaction/bptx/maintenance/read/write paths, lenses,
  extensions, mcp-server, CLI, pyo3 (IOError on failure — Python
  parity), test stores. The critical semantic: **a FAILED journal
  epoch probe is a TRUNCATED-view error, never an empty suffix** —
  `probe_writer` is fallible; a transient outage mid-log can no longer
  silently shorten the journal view. `read_full`, `has_live_state`,
  `read_with_shards`, `list_shards` all became fallible with the same
  law (an outage is not "nothing there"). Pinned by
  test_c17_ref_outage_errors_not_empty / _recovery (SQL errors naming
  the ref, not empty rows) and
  test_c17_probe_outage_is_error_not_truncation.
- **C13 closed (the raw path is journal-aware)**: `read::read`
  (`pond read`, and thereby the OLTP/streaming lenses' full-replace
  reads) resolves the D6 read plan — snapshot + live journal entries,
  RG-granular stale filtering — and concatenates the live RG bytes,
  instead of resolving the branch ref alone (a cache of the last fold).
  A second `write_rows` is now visible to raw reads immediately; the
  entries-only bootstrap window reads entries instead of erroring "has
  no commits"; post-fold reads stay correct. CRDT row merge remains
  the row readers' job (documented). Pinned by
  test_c13_raw_read_is_journal_aware +
  test_c13_raw_read_no_fold_yet_returns_entry_bytes.
- **C12 codec laws landed (the C3 zero-coverage residual closed)**:
  `core/storage/tests/laws_pnps.rs` — 11 proptest laws + 10+
  adversarial companions attacking PNPK (pack framing) and PSLB (slab
  framing): lossless round-trips, magic discrimination, truncation
  rejection, fuzz-no-panic, determinism, the PSLB tail invariant,
  range-fetch reconstruction (the get_blob_range contract),
  compressed round-trips, and conservative plan_ranges pruning. The
  laws found TWO real bugs, both fixed same-cycle:
  **(1) serde_json 1-ULP float loss** — the pack framing is exact but
  serde_json's default float parser re-parses ~30% of arbitrary finite
  f64s 1 ULP off; fixed by enabling serde_json's `float_roundtrip`
  feature in pond_storage (exact parser; the strict law is now
  un-ignored and green). **(2) decode_slab_footer ABORT** — an
  unvalidated footer entry count ran `Vec::with_capacity` on up to
  u32::MAX → ~192 GiB allocation → process abort (no unwind) on a
  malformed blob; fixed by validating `n_entries ≤ footer_len / 21`
  before allocating (un-ignored + green).
- **CI repaired (stale pond.so hard link)**: run 33259199081 failed
  the pytest job because the restored cargo target cache carried a
  pre-D8 `pond.so` HARD link — cargo rebuilt `libpond.so` (new inode)
  but the `[ ! -f pond.so ]` guard skipped re-linking, so Python
  imported the stale module (`pond.ObjectStore` missing, 17 substrate
  tests failed). Fixed with an unconditional relative symlink
  (`ln -sf libpond.so pond.so`); KG entries for the cycle's new docs
  fixed the coverage job same-commit.
- **LIVE R2 validation (first real-object-storage cycle)**: with
  owner-provided credentials (stored at /home/z/.secrets + gitignored
  .env; ZERO secret material in the repo), both live harnesses ran
  green against real Cloudflare R2: the CLI harness
  (scripts/test_rust_s3_r2.py — 35/35: init, write-rows, read-rows
  with pushdown + projection, ls, history, branch/checkout/merge with
  journal-append semantics verified on real storage, raw blobs,
  cleanup) and the NEW Rust store-primitive harness
  (core/s3/tests/r2_live.rs + scripts/test_r2_live_rust.sh — SigV4
  writes, sha256 content addressing, REAL HTTP Range semantics,
  C17's 404→Ok(None) discrimination on live R2, list_paths, the D3
  delimiter-LIST writer-discovery primitive, deletes, and warm-read
  timing). **Timing evidence: cold 4 KiB R2 GET 207–253 ms; warm
  local-disk cache read 9.8 µs → 1.4 µs — a ~21,000× speedup,
  single-digit-MICROseconds against the <10 ms warm budget.** Two live
  findings recorded: C19 (R2's idempotent DELETE blurs delete_path's
  "existed" — documented trade) and C20 (`pond cat` short-hash prefix
  resolution, UX NIT).
- Validation: 543 workspace tests + 65 pond_sql + 44 pytest (+
  substrate suite) + lens laws + moto green; clippy `-D warnings`
  clean; CI green on the pushed HEAD.

## 2026-08-29 — Crucible iteration N+5: the Python-substrate delegation cycle (C5-python phase 1 + C8 + C13)

- **D8 landed (C5-python phase 1)**: the pure-Python kernel stack's
  OBJECT-STORE layer now runs on the Rust core. `pond.ObjectStore` (pyo3,
  `Arc<dyn ObjectStore>` — NOT a kernel) exposes the Rust trait surface
  (put/get_blob + range/suffix + batches + blob_exists/delete_blob +
  put/get/delete_path + list_paths/list_dirs + store_id), constructors
  mirroring Storage (local auto-detect + `from_s3` with the 3-tier cache
  via the shared `s3_store_cached` core), ALL methods GIL-releasing
  (`py.allow_threads` — the Python kernel's ThreadPoolExecutor batches
  now parallelize into Rust's native pools). `RustObjectStore`
  (bindings/python/core/rust_object_store.py) implements the exact
  LocalFSObjectStore/S3ObjectStore duck interface over it (KeyError
  parity, stats, list_paths shape parity incl. the legacy `paths/` tree,
  `base_dir`/`_bucket` duck-compat). `make_kernel(backend="auto")`
  prefers Rust whenever `import pond` works, with byte-identical
  pure-Python fallback (one-time stderr note; `POND_PY_BACKEND`
  override; `memory://` untouched; `backend="rust"` hard-fails).
- **The layouts CONVERGE — verified, not assumed**: blobs at
  `blobs/{h[:2]}/{h}` (same sha256 both sides — Rust put_blob returns the
  hash Python's `hash_bytes` computes), refs at `{path}` as JSON. The Rust
  ref parser (`extract_hash_from_json`, LocalFS + S3 copies) now reads
  BOTH the canonical `{"hash":"x"}` and Python `json.dump`'s
  `{"hash": "x"}` spellings (a space after the colon — without this the
  Rust core read Python-written refs as absent). Byte-interop pinned BOTH
  directions: a store written by LocalFSObjectStore reads identically
  through RustObjectStore and vice versa — same files, same tree.
- **ObjectStore trait raw-key escape hatch** (`get_raw/put_raw/
  delete_raw/list_raw`, default Unsupported): store-relative keys without
  content addressing — the adapter's OLD-layout fallbacks (`b/{h[:2]}/{h}`
  blobs, `paths/{p}` refs — pre-layout-change stores stay readable
  through the Rust backend) and `list_all_blob_hashes` enumeration.
  LocalFS validates keys against root-escape (`..`, absolute, drive
  prefixes → InvalidInput); S3 maps 404 → None. CachingObjectStore
  deliberately does NOT implement raw ops (they'd bypass cache layers);
  the adapter capability-probes once and degrades to new-layout-only on
  cache-wrapped S3 stores (`cache_dir='off'` restores legacy reads).
  S3's duplicated list-pagination loop was extracted (`list_all_keys`)
  and shared with `list_paths` (+ raw parity tests on both backends).
- **S3 via the Rust client, moto-pinned**: ObjectStoreNativeKernel +
  UnifiedStorage round trips on `RustObjectStore.from_s3(moto endpoint)`
  — the Python world's object I/O no longer needs boto3 (the Rust SigV4
  client + 3-tier disk cache serve it); 20-test suite in
  tests/test_rust_object_store.py (skips gracefully when `import pond`
  fails — CI's pytest job runs it, `pond_python` built there).
- **C8 resolved (tribunal r1 finding 7)**: the SQL executor's
  `read_collection_as_json_rows` PROPAGATES HEAD-read errors — a
  transient S3 500 during `SELECT` used to yield silently
  empty/partial results while pyo3 propagated. The one legitimate
  empty-state error (`has no commits`) still reads as zero rows (INSERT
  INTO a fresh collection depends on it). Pinned by a BlobOutage-store
  test (outage → SQL error naming the collection; recovery → rows
  intact) + the fresh-collection-empty test.
- **C17 OPENED (found by the C8 fix's own test)**: `get_path`'s Option
  API has NO error channel — a transient REF-read failure is
  indistinguishable from an absent ref for EVERY caller (journal
  snapshot resolution, writer probes, branch resolution). The C8 fix
  closes the data-blob half; the ref half needs a trait signature change
  (`io::Result<Option<String>>`) — recorded in CRITIQUE, out of this
  cycle's scope.
- **C13 documented**: `pond read --help` + docs/API_WORKFLOW.md §2.1 now
  state the raw path's journal-staleness contract (branch ref = cache of
  the last fold; journal-aware reads go through read-rows/SQL). Routing
  the raw reader through the journal stays open (natural owner: the C17
  cycle).
- Validation: 581 Rust tests green (579 + 2 C8 tests; CI command
  excludes pond_python/pond_sql, pond_sql 32 green incl. the new ones),
  clippy `-D warnings` clean (workspace + pond_python), pytest 23
  passed/4 by-design skips (test_all) + 20/20 new suite, lens laws
  compliant, moto S3 via Rust client green, knowledge-graph coverage
  updated (210 files).

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
