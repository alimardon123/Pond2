# CRITIQUE.md — Open findings (append-only until resolved)

> Crucible state file. Each finding: location + root-cause hypothesis.
> Resolved items move to CHANGELOG.md.

## Open

- **C18 (N+6, recorded — C ABI cannot carry the C17 error channel).**
  `pond_kernel_resolve` (core/kernel/src/c_abi.rs) maps BOTH `Ok(None)`
  and `Err` to NULL: C callers cannot distinguish a transient backend
  failure from an unbound ref. Fixing needs an ABI break or a new
  entry point (`pond_kernel_resolve_err` returning an error code out-param);
  no C caller reads the distinction today (the Go SDK/C tests go through
  higher surfaces). Accepted trade, revisited when a C consumer needs
  honest resolve errors.
- **C19 (N+6, found LIVE on R2 — idempotent DELETE blurs "existed").**
  The trait contract says `delete_path` "returns true if it existed",
  but S3/R2's DELETE is idempotent (204 even for absent keys), so the
  second `delete_path` on R2 returns `Ok(true)` while LocalFS honestly
  returns `Ok(false)`. Exact "existed" semantics on R2 would cost a HEAD
  before every DELETE — an extra round trip per delete for a return
  value no production caller reads. Documented trade (pinned live in
  tests/r2_live.rs); do NOT "fix" without a caller that needs it.
- **C20 (N+6, NIT — `pond cat <short-hash>` prefix resolution).** The
  live R2 CLI harness found `cat` requires the FULL hash — short-hash
  prefix resolution errored ("no blob with prefix 'b6d5d430b154'")
  instead of resolving. UX gap only (CLI convenience); `read` by full
  hash works. Fix shape: blob_exists/prefix scan via list_raw on the
  blobs/ tree, or accept full-hash only in help text.
- **C16 (N+4, residual — CRDT-RG read cost)** — The D7 reader rule (see
  ARCHITECTURE.md) exempts CRDT-update RGs from value pruning in merging
  readers and skips them in non-merging readers. Cost model = the pre-D7
  shard channel (updates were never pruned), but at PB scale a hot
  upsert workload accumulates un-prunable RGs until the next fold.
  Mitigation path when it matters: row-level merge compaction (the
  deferred "future cycle" rewrite) would fold base + update RGs into one
  prunable RG. Recorded, not scheduled.
- **C5-python PHASE 2 (format unification — recorded, conditional)** —
  Phase 1 (substrate delegation) LANDED N+5: the Python kernel stack's
  object-store layer runs on the Rust core (ARCHITECTURE.md D8 —
  `pond.ObjectStore` pyo3 surface + `RustObjectStore` adapter +
  `make_kernel(backend=…)`; byte-identical layouts, moto-proven S3 via
  the Rust client, old-layout fallbacks via the raw-key hatch). The
  residual: the Python world's FORMATS/semantics (UnifiedStorage's own
  manifest/encoding/stats layers, the SDK's CRDT surface) still live in
  Python and differ from PND2/PMAN — interop today is at the BLOB/ref
  layer (same bytes, same keys), not the manifest layer. Phase 2
  (Python formats → Rust codecs) is CONDITIONAL: only if the lens
  world's own formats stop earning their keep (the lens algebra is the
  Python world's raison d'être — D1 forbids porting semantics to
  Python, it does not demand deleting useful Python formats). The
  identity-less row-ordering decision (finding #1 from the laws cycle)
  rides along with any phase-2 work.
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
- **C12 (tribunal r1, F7 — REVISITED N+6 with the codec laws; trade
  stands, now INFORMED)** — The lenient non-PND2 skip in read.rs: a
  corrupted/truncated PND2 RG blob is silently skipped as raw data
  where the reader previously errored loudly. Raw `write()` blobs must
  be skipped (journal folds pull them into read paths), and
  content-addressing makes silent corruption unlikely. The N+6 codec
  laws (laws_pnps.rs) attacked PNPK/PSLB with adversarial bytes and
  found the decoders themselves robust (post-fix); the residual skip
  remains an accepted trade — the stricter checksum-verify option
  stays on the shelf until a real corruption incident demands it.
- **C13 (tribunal r1, F10) — RESOLVED N+6** — `read::read` (raw path,
  incl. `pond read`) now resolves the JOURNAL view via
  `journal::resolve_packs` (snapshot + live entries, RG-granular stale
  filtering) and concatenates live RG bytes — see ARCHITECTURE.md D9.
  Pinned by test_c13_raw_read_is_journal_aware (second write visible
  pre-fold; post-fold read stays correct) +
  test_c13_raw_read_no_fold_yet_returns_entry_bytes (entries-only
  bootstrap window reads entries, not "has no commits"). Docs updated
  (API_WORKFLOW §2.1 + CLI help: the raw path is journal-aware; CRDT
  row merge stays with read_rows/SQL).
- **C14 (bootstrap-fold race, bounded)** — A reader whose branch_ref GET
  predates the FIRST-ever fold (ref=None) probes from seq 1, dies at the
  fold's deleted entry, and observes an EMPTY state — a valid CRDT prefix
  but a surprising one. Window: only around a collection's first
  bootstrap fold. Root cause: probes cannot discover an entry past a gap
  without the watermark. Acceptable (next read is correct); revisit if
  read-your-writes on fresh collections matters.
- **C8** — RESOLVED N+5: `read_collection_as_json_rows` (SQL executor)
  PROPAGATES HEAD-read errors (`Failed to read collection '{c}': …`);
  the ONE legitimate empty-state error (`has no commits`) still reads
  as zero rows (INSERT INTO a fresh collection depends on it — pinned
  by test_c8_no_commits_is_empty_not_error). Pinned by
  test_c8_transient_read_failure_propagates (BlobOutage store: outage →
  SQL error naming the collection; recovery → rows intact). The
  REF-read half of the hole moved to C17 (get_path has no error
  channel) — CLOSED by C17 below.
- **C17** — RESOLVED N+6 (ARCHITECTURE.md D9):
  `ObjectStore::get_path` → `io::Result<Option<String>>` across the
  trait + LocalFS/S3/Caching backends (+`get_path_with_etag`,
  `get_path_async`), `PondKernel::resolve` + every caller (the
  112-site sweep), fallible `probe_writer` (a failed epoch probe is a
  TRUNCATED-view error, never an empty suffix), pyo3 raises IOError.
  Pinned by test_c17_ref_outage_errors_not_empty +
  test_c17_ref_outage_recovery (RefOutageStore: healthy blobs, failing
  refs → SQL errors naming the ref, NOT empty rows; recovery → rows
  intact) + test_c17_probe_outage_is_error_not_truncation (entry-path
  outage → resolve_packs Err, not a silently-shorter view). C ABI
  limitation recorded as C18.
- **C5** — see **C5-python PHASE 2** above for the residual (format
  unification, conditional). Phase 1 (substrate delegation) RESOLVED
  N+5 — ARCHITECTURE.md D8. C5-a/C5-b (Rust core) RESOLVED N+4:
  `upsert_shard`/`delete_shard` journal their packs — the JSON-shard
  write surface is GONE from the Rust core, `shard_count == 0` is the
  steady state, C5-b (WriteBuffer slab-packed flush) landed with it.
  NOTE (laws-cycle finding #1): full-state CRDT merge
  byte-permutation-invariance needs an owner decision on identity-less
  (legacy) row ordering — legacy rows pass through in input order (the
  ignored `merge_is_permutation_invariant` law in tests/laws_crdt.rs
  documents the counterexample); the CRDT substate IS invariant (proven by
  the crdt_only law). After N+4, the identity-less input class shrinks to
  pre-migration data + explicit `write_rows_no_crdt` (journal-era
  upserts/deletes always carry _rowid).
- **C6** — Naming: docs sometimes say "StalixDB"; the reference project is
  **staledb**. Sweep docs when touched; do not mass-edit untouched files.

## Resolved (moved to CHANGELOG.md entries)

- (2026-08-28, N+4) **D7-exposed value-pruning hole (SQL
  update-OUT-of-range resurrection)** — moving CRDT updates into the
  journal made them value-prunable: the pruned reader's pre-filter
  dropped the UPDATE copy (age 22 fails `age >= 30`) while keeping the
  stale base copy (age 30 passes), so the CRDT merge resurrected the
  stale row (`test_where_pushdown_shard_updated_row_disappears` failed:
  3 rows instead of 2). Pre-D7 the hole was unreachable (updates lived
  in the unfiltered shard channel) but LATENT for folded shard RGs
  (compact's shard fold kept tombstones → folded update RGs were
  value-prunable identically). Fix: `RowGroupEntry::is_crdt_update_rg()`
  — merging readers exempt CRDT RGs from zone-map/bloom/row-filter;
  non-merging readers (i64, all_row_groups, lakehouse/vector lenses)
  skip them (pre-D7 shard-invisible equivalence). **Amended same cycle
  (tribunal r4 finding 1, HIGH, empirically proven)**: the first version
  of the check was NAME-ONLY and misfired on `normalize_rgs_to_schema`
  PLACEHOLDER stats — after any mixed fold every base RG carries a
  `_deleted` placeholder and the non-merging readers went BLIND
  post-fold (0 rows for 3). Fix v2: the check requires REAL min/max
  stats on `_deleted` (placeholders never carry them; every genuine
  CRDT RG writer does — and branch.rs's merge re-encoder now writes real
  stats instead of all-None, which would have misclassified genuine
  merged CRDT RGs). Pinned by 6 regressions in upsert_journal_test
  (live + post-compact update-OUT/INTO, compact-blindness probe,
  i64-skip, resurrection-post-compact, two-writer same-rowid).
- (2026-08-28, N+4) **write_rows batch order flake (pre-existing)** —
  `write_rows` generated rowids with plain `uuidv7()`, which is RANDOM
  within the same millisecond; the CRDT merge sorts by rowid, so a
  fresh batch's read-back order was random per run
  (test_write_rows_auto_crdt failed 5/6 of the time; order varied
  across runs). Fix: `uuidv7_monotonic()` (existing kernel function,
  counter in the bytes after the timestamp) in write_rows_inner AND
  shard::upsert_shard's generated rowids — batch rowids now follow
  insertion order, so the merge preserves the batch's write order
  deterministically.
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
