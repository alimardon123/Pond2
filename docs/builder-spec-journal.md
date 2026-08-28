# Builder Spec — D3 No-CAS Journal Core (Task cron-2026-08-28-0353-a)

You are the WRITE-PATH BUILDER for Pond2 at /home/z/Pond-review.
Your contract: ACCEPTANCE.md §"This-cycle acceptance (crucible iteration N+2)".
Your design (settled, follow EXACTLY): ARCHITECTURE.md §D3.
Findings you fix: CRITIQUE.md C9 (P0 history loss), C10 (tiebreak), C2, C4.

## The P0 bug (verified experimentally)

Two successive `write_rows_i64` calls → second commit's manifest holds ONLY its
own row group; HEAD points to it; reads resolve only HEAD → first write's rows
VANISH (10+10 written, 10 readable). SQL INSERT routes through the same path.
The CAS loop in write_rows_inner is semantically vacuous (losers rebuild packs
that still exclude the winner's data).

## Design

**Per-writer immutable journal, benign snapshot cache. NO CAS anywhere new.**

1. **Writes append, never overwrite.** After building the PNPK pack (commit
   JSON + manifest bytes — as today), append ONE pointer at a unique path
   `collections/<c>/_branches/<b>/journal/<writer_id>/<seq:012>` (12-digit
   zero-padded seq) via plain `kernel.reference()`. Unique path ⇒ always
   succeeds, zero retries. `writer_id` = fresh uuidv7 per writer instance;
   `seq` = writer-local counter from 1.

2. **Journal metadata in the pack's commit JSON**: `"journal":
   {"writer": id, "seq": n}` for data entries; compaction snapshots add
   `"upto": {writer → max_seq_folded}`. Path files stay `{"hash":"..."}`.

3. **Reads = snapshot ∪ live entries.** `read_rows_json_pruned(kernel,
   collection, branch, ...)` becomes journal-aware:
   - branch_ref → snapshot pack hash (None ⇒ empty base).
   - snapshot pack's commit JSON → its `journal.upto` ({} if legacy/absent).
   - Discover writers: ONE-LEVEL listing of the journal prefix (new
     `list_dirs` ObjectStore method), TTL-cached process-locally (env
     `POND_JOURNAL_TTL_MS`, default 1000; 0 = always fresh).
   - Per writer w: probe `journal/<w>/<seq:012>` from
     `max(snapshot_upto[w], locally_seen[w]) + 1` until first miss. Probes
     across writers PARALLEL (std::thread::scope, like read.rs bloom checks).
   - Run the EXISTING pruned pipeline per pack via
     `read_rows_json_pruned_with_head(kernel, pack_hash, ...)` for snapshot +
     every live entry. SAFETY (keep as comment): predicates are a CONSERVATIVE
     pre-filter per pack; a row updated by a later entry to match the
     predicate still surfaces from that entry — same argument as the shard
     layer.
   - CRDT-merge rows from all packs: `shard::merge_rows_by_rowid(rows,
     key_fields.first())` then `filter_live_rows`.
   - `read_rows_json_pruned_with_head` stays PURE (no journal resolution).
   - Guard: a pack whose manifest has zero row groups contributes Ok(vec![]),
     not an error.

4. **Fix C10 — total tiebreak.** shard.rs `merge_rows_by_rowid`: replace when
   `(version, rowid) > (existing_version, existing_rowid)` (tuple string
   compare) instead of strict `version >`. Same fix in the pyo3 chunk merge
   (bindings/python/pyo3/src/lib.rs, `final_latest` map, ~line 5127:
   `version > *existing_ver` → tuple compare with rowid). Regression test:
   same _rowid, same _version, different payloads, both merge orders →
   identical output; plus tombstone-vs-live at equal versions.

5. **All storage write paths route through the journal.** write.rs has direct
   branch_ref writes at ~lines 116 (write), 213 (write_rows_i64), 313
   (write_rows_i64_packed), 642 (write_rows_i64_slab), 959 (SlabWriter), plus
   the CAS loop 457–488 (write_rows_inner):
   - write_rows_inner: DELETE the CAS loop. Build pack once (parent = snapshot
     hash for display). Append to journal. Do NOT touch branch_ref/derived
     refs (C4: zero shared writes). REMOVE the manifest_ref + bare-collection
     reference() calls.
   - write_rows_i64, write_rows_i64_packed, write_rows_i64_slab, SlabWriter:
     same — journal append instead of branch_ref write. Writer state: process-
     local registry `journal::writer_for(kernel, collection, branch) ->
     Arc<Mutex<JournalWriter>>` (static OnceLock map keyed by
     (store_id, collection, branch)); create on first use (fresh uuidv7, seq
     from 1); appends serialized by the Mutex.
   - write() raw-bytes path (~77–123): KEEP AS-IS (legacy base-snapshot path;
     already CAS-free plain reference(); journal entries above it union in).
     Add a doc comment: it sets the snapshot base.

6. **Compaction.** `journal::compact(kernel, collection, branch, key_fields)
   -> Result<CompactStats, String>`:
   - Fresh (bypass TTL) journal view: snapshot + live entries + shards.
   - Union manifest: snapshot pack's manifest RGs + every live entry pack's
     RGs (pond_pack::decode_pack for manifest bytes; concatenate into ONE
     CollectionManifest; schema = snapshot's else first entry's). Manifest-
     level fold: reads pack headers only, NEVER data blobs. No row dedup
     needed (read path CRDT-merges anyway).
   - Shards: if shards exist, decode their rows and write them as ONE extra
     PND2 pack (reuse write_rows encode machinery), include its RGs in the
     union; then shard::clear_shards.
   - New pack commit JSON: `journal: {writer: <compactor id>, seq: <next>,
     upto: {w → max(snapshot_upto[w], max_live_seq[w])}}`, message
     "journal compaction". Append it to the compactor's journal log FIRST,
     then LWW-update branch_ref to the new pack (plain reference() — benign:
     every ref value is a valid folded state; a losing racer's pack stays a
     live journal entry).
   - Delete folded journal entry paths seq 1..=upto[w] per writer (EXCEPT the
     compactor's just-written entry). Return CompactStats {entries_folded,
     shards_folded, new_snapshot}.
   - Auto-compact: after append in write paths, if writer.seq -
     last_fold_seq >= threshold (env POND_JOURNAL_AUTO_COMPACT, default 32,
     0 disables) → compact synchronously. Track last_fold_seq in
     JournalWriter; compact() updates the registry entry when present.

7. **ObjectStore::list_dirs(prefix) -> io::Result<Vec<String>>** — immediate
   child directory names, one level:
   - LocalFS: fs::read_dir(base_dir/prefix), dir names.
   - S3 (core/s3/src/lib.rs): ListObjectsV2 + `delimiter=/`, parse
     `<CommonPrefixes><Prefix>` values (string-search XML style like the
     existing list_paths), strip list prefix + trailing '/'.
   - CachingObjectStore: delegate to inner.
   - Default trait impl: return io::Error Unsupported. Implement everywhere
     `impl ObjectStore` exists (search the repo — incl. CountingStore in
     read.rs tests: delegate + COUNT the calls).
   - Also add `fn store_id(&self) -> String` with default
     `format!("{:p}", self as *const dyn ObjectStore as *const ())`;
     override LocalFS (canonical base_dir), S3 (endpoint+bucket+prefix),
     CachingObjectStore (delegate to inner).

8. **Discovery cache (journal module).** Process-local
   `static DISCOVERY: OnceLock<RwLock<HashMap<(store_id, journal_prefix),
   Discovered>>>`; `Discovered { writers: BTreeSet<String>, seen:
   BTreeMap<String, u64>, refreshed_at: Instant }`. TTL from
   POND_JOURNAL_TTL_MS (default 1000, parse once). Own appends: registry
   append() sets seen[own] = seq + inserts own writer into cached set
   immediately (own writes visible to own reads instantly). Fresh refresh:
   LIST prefix; keep seen for still-existing writers (immutable entries —
   watermarks never stale); drop vanished writers.

9. **Env knobs**: POND_JOURNAL_TTL_MS (1000), POND_JOURNAL_AUTO_COMPACT (32).

## Tests (harness — write FIRST; the history test MUST fail before, pass after)

In journal.rs #[cfg(test)] and/or core/storage/tests/journal_test.rs:
1. `test_history_preserved_across_writes` — 2 × write_rows_i64 × 10 rows →
   20 rows (the exact P0 probe).
2. `test_n_sequential_writes_all_visible` — 5 × 20 rows → 100.
3. `test_concurrent_writers_no_lost_rows` — 8 threads × 8 writes × 5 rows →
   320 rows, zero write errors (proves no-CAS correctness).
4. `test_merge_deterministic_under_permutation` — equal _version rows with
   different payloads + tombstone-vs-live at equal versions; shuffled merge
   orders → identical output. Also resolver-level: shuffled entry lists.
5. `test_warm_read_zero_lists` — CountingStore (localfs): 3 writers × 3
   entries; second read within TTL: list_dirs == 0 AND list_paths == 0;
   probes == 1 get_path miss per writer (+ branch_ref get_path). Exact counts.
6. `test_legacy_repo_reads_correctly` — old layout manually (branch_ref →
   pack; 2 shards via append_shard) → new reader returns both.
7. `test_compact_folds_journal_and_shards` — 3 entries + 2 shards; compact;
   branch_ref → pack with ALL RGs; folded journal paths probe → None; shards
   cleared; read returns all rows; post-compact write+read works.
8. `test_compact_race_benign` — two sequential compactions; final read
   correct (LWW-updates-are-benign invariant).
9. `test_upto_watermark_skips_folded_entries` — after compact, read probes
   only above watermark (CountingStore: get_path to folded paths == 0).
10. `test_journal_status` — status() reports writers/entries/upto.

Update EXISTING tests that assert write_rows updates branch_ref/manifest_ref
(journal semantics: plain writes leave branch_ref alone; compact updates it).
reference_if kernel tests stay green (primitive remains, no new callers).
Prior CountingStore byte-budget tests: single-write collections see one
journal entry pack — verify, adjust only if genuinely needed.

## Constraints

- Toolchain: `. $HOME/.cargo/env` first in EVERY bash call.
- NO new external crates (no proptest — seeded std shuffles in loops).
- `cargo clippy --workspace --all-targets -- -D warnings` clean;
  `cargo test --workspace --exclude pond_python` green; `cargo check -p
  pond_python` compiles.
- Do NOT create git commits. Do NOT touch: cli/src/main.rs, scripts/
  (moto/R2 python), CHANGELOG/SCORECARD/ACCEPTANCE/ARCHITECTURE/CRITIQUE.
- DO update KNOWLEDGE_GRAPH.md §2 file map for new files (journal.rs,
  journal_test.rs) — CI runs scripts/verify_knowledge_graph.py.
- Keep safety-argument comments (why no-CAS is correct, why pre-filter+merge
  is safe, why LWW-on-ref is benign). Match the codebase comment culture.
- pyo3 change: ONLY the C10 tiebreak fix. Nothing else in that crate.
- Disk-limited (~9.9GB): iterate with `cargo test -p pond_storage`; full
  workspace at the end; selective `cargo clean -p` if pressure builds.

## Validation (ALL before reporting done)

1. `. $HOME/.cargo/env && cd /home/z/Pond-review && cargo test -p pond_storage`
2. `cargo test --workspace --exclude pond_python`
3. `cargo clippy --workspace --all-targets -- -D warnings`
4. `cargo check -p pond_python`
5. `python scripts/verify_knowledge_graph.py` (check .github/workflows/
   view-laws.yml knowledge-graph job for exact invocation)

## Worklog (MANDATORY)

APPEND (never overwrite) to /home/z/my-project/worklog.md:
`---` then `Task ID: cron-2026-08-28-0353-a`, `Agent: write-path-builder`,
`Task: D3 no-CAS journal core — journal.rs, write/read routing, C10 tiebreak,
compaction, tests`, Work Log steps, Stage Summary (test counts, API surface,
files+line counts, deviations+reasons, integration notes for orchestrator).
