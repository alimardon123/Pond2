# Builder Spec — Task 2-b: C3 property-test suites (the LAWS cycle)

You are a builder subagent for the Pond2 repo at `/home/z/Pond-review`.
READ `/home/z/my-project/worklog.md` FIRST (previous agents' records — the
last entry is cron-2026-08-28-0353; the D6 read-plan work it foreshadowed
has ALREADY LANDED as commit 73fde0c, verified green — do not redo it).
Then read `ACCEPTANCE.md` items 1-3 (this cycle's contract — YOUR task is
exactly those three items), `CRITIQUE.md` C3, and `ARCHITECTURE.md` §D3.

## Goal

Three proptest suites proving the CRDT/journal/PMAN laws (ACCEPTANCE
items 1-3). The owner's framing: the CRDT shards layer is the owner's own
foundational work — review it by ATTACKING it with random inputs. A law
that FAILS is a successful review finding: report it, do not weaken the
law to pass. (If you find a genuine counterexample you cannot fix with a
clearly-correct small change, STOP and report it in your worklog entry.)

## Ground rules

- Add `proptest = "1.11"` to `[dev-dependencies]` of
  `core/storage/Cargo.toml` (per-crate dependency — same pattern as the
  existing `tempfile` dev-dep). Run `cargo add` or edit + build; the
  registry cache already has proptest-1.11.0 fetched.
- DETERMINISM (acceptance item 6): every suite pins its seed IN CODE:

  ```rust
  use proptest::prelude::*;
  use proptest::test_runner::{Config as ProptestConfig, RngSeed};

  const LAWS_SEED: u64 = 0x504F4E44_00000001; // "POND" — distinct per file
  fn law_config(cases: u32) -> ProptestConfig {
      let mut c = ProptestConfig::with_cases(cases);
      c.rng_seed = RngSeed::Fixed(LAWS_SEED);
      c
  }
  ```
  (Verified against proptest-1.11.0 source: `Config.rng_seed` is a public
  field; `RngSeed::Fixed(u64)` exists and parses from u64.)
  Use a DIFFERENT trailing constant per file (…0001/…0002/…0003) so the
  three suites explore different case spaces. File-header comment:
  "bump the seed to explore a new random space".
- Use the `proptest!` macro form with `#![proptest_config(law_config(N))]`
  at the top of each `proptest!` block.
- NO new runtime dependencies. NO changes to src/ code unless a law fails
  (then: smallest clearly-correct fix + a regression note in your worklog).
- Time budget on 2-vCPU CI: the whole new suite must stay < 30s.

## File 1: `core/storage/tests/laws_crdt.rs` (256 cases per law)

API under test (all `pub` in `pond_storage::shard`):
`merge_rows_by_rowid(rows: &[Value], key_col: Option<&str>) -> Vec<Value>`,
`filter_live_rows(rows: &[Value]) -> Vec<Value>`.

Row strategy (one `Vec<Value>` = one case's row set):
- rowid: 20% absent (legacy row), else `"r{0..4}"` (5 ids → guaranteed
  collisions across the row set).
- _version: 80% `"v{:04}"` of `0..=6` (small range → frequent ties), 20%
  absent (legacy row without version).
- _deleted: 30% true when _rowid present, else false/absent.
- payload: `"name"` from `["alice","bob","carol",""]` (include empty
  string — it caught a real regression once), `"value"` i64 `-4..=4`,
  sometimes only one of the two fields, sometimes extra fields
  (`"extra"` from a small string pool) — JSON objects stay small.
- key_col strategy: 25% `None`, else one of `Some("name")`, `Some("value")`,
  `Some("no_such_col")`.

Laws (each its own `#[test]` inside one `proptest!` block):
1. `merge_is_permutation_invariant` — prop: (rows, key_col, perm) where
   perm is a `Vec<usize>` shuffle of indices. Assert:
   `merge_rows_by_rowid(&permuted, kc) == merge_rows_by_rowid(&rows, kc)`
   — the FULL merged output (tombstones included), compared as
   `serde_json::to_string` (byte-identical state, the C10 guarantee).
2. `merge_is_idempotent` — `live(merge(merge(S))) == live(merge(S))` AND
   (verify empirically first; assert if it holds — if it does NOT hold,
   that is a FINDING, report it) `merge(merge(S)) == merge(S)` full-state.
3. `tombstone_suppresses_when_strictly_latest` — prop: (base_rows for one
   rowid r with versions from `0..=k`, a live max version `k`, tombstone
   version `k+1..k+4`). Assert r absent from `filter_live_rows(merge(...))`.
   Dual law `live_survives_when_strictly_latest`: live version
   `k+1..k+4`, tombstone at version `k` → r present.
4. `merge_is_deterministic` — two calls on the same input produce
   byte-identical output (`serde_json::to_string` equality).

## File 2: `core/storage/tests/laws_pman.rs` (128 cases per law)

API under test (all `pub` in `pond_storage::manifest`):
`normalize_rgs_to_schema`, `CollectionManifest::{new, add_row_group,
encode, decode}`, `RootManifest::{new, encode, decode, prune_leaves,
total_row_groups}`, `LeafEntry`, `RowGroupEntry`, `ColumnStatsEntry`,
`MAX_LEAF_RGS`. VT tags: `pond_core::{VT_INT64, VT_FLOAT64, VT_STRING,
VT_BOOLEAN}` (constants module re-exported — check what journal_test.rs
imports; `pond_core` is already a dependency and integration tests may
use it).

Laws:
1. `normalize_aligns_stats_to_schema` — prop: (schema: 1..=6 columns with
   unique names from a pool + random VT tags, per-RG stats lists that are
   random SUBSETS/SUPERSETS/PERMUTATIONS of the schema — generate each
   RG's stats as: random subset of schema names + 0..2 foreign names,
   random order, random min/max byte blobs (0..=8 bytes) and null_counts).
   After `normalize_rgs_to_schema`: every RG has exactly `schema.len()`
   stats, names equal the schema names IN ORDER, value_types equal the
   schema's tags.
2. `manifest_encode_decode_roundtrips_byte_stably` — build a manifest
   whose RGs are already normalized (normalize first — PMAN v2 encodes
   stats per the manifest schema count; that alignment is the format's
   precondition), `e1 = m.encode()`, `m2 = CollectionManifest::decode(&e1)`
   (assert Some), `e2 = m2.encode()`, assert `e1 == e2` (byte-stable) and
   `m2` equals `m` field-by-field (row_groups in order, columns, key_col).
3. `root_manifest_roundtrips_and_resolves_all_leaves` — prop: (1..=5
   leaves, each 1..=4 RGs with `n_rows` 0..=1000 and arbitrary
   `slab_byte_offset: Option<u64>` / `slab_byte_len: Option<u32>`).
   Build `RootManifest` with `LeafEntry`s (leaf_hash = 64-hex-char string
   from the strategy — arbitrary is fine, this law never fetches blobs),
   `e = root.encode()`, `RootManifest::decode(&e)` (assert Some), assert:
   leaves preserved in order (hash, n_row_groups, key_min/key_max bytes),
   `total_row_groups()` == sum of `n_row_groups`, and
   `prune_leaves(&[])` == all indices (no predicates → no pruning).
   `prune_leaves` takes `&[(String, String, Vec<u8>)]` — the empty-slice
   call covers the resolve-all law.

## File 3: `core/storage/tests/laws_journal.rs` (24 cases per law)

Real kernel on a fresh `tempfile::tempdir()` per case (pattern:
`UnifiedStorage::new_local(dir.path())`, `storage.kernel()` — copy the
imports from `journal_test.rs`). Writes:
`write::write_rows_i64(kernel, coll, "main", &[("id", &ids), ("val", &vals)], msg)`.
Reads: `read::read_rows_i64(kernel, coll, "main", None, None)`.
Compact: `journal::compact(kernel, coll, "main", &[])`.

Laws:
1. `read_after_n_appends_is_the_union` (the C9 history law) — prop:
   (k in 1..=5, batch_sizes 1..=8, a random partition of
   `total_ids = 0..sum(batch_sizes)` into k batches, optionally SHUFFLE
   which ids land in which batch). Write the k batches sequentially.
   Assert: `read_rows_i64` returns each id EXACTLY once (concatenating
   readers have no CRDT dedup — exact-once is the law), and the id SET
   equals the generated union.
2. `compact_preserves_state_for_every_reader` — after the k appends from
   law 1, snapshot `before = read_rows_i64(...)`. Run `journal::compact`.
   Assert `read_rows_i64` (same kernel) == `before`, AND a SECOND
   `UnifiedStorage::new_local(same dir)` + its kernel reads == `before`
   (fresh-reader-equivalent: new instance, entries ≤ upto dropped by
   resolve_view, probes resume above the watermark). Also assert
   `resolve_view(kernel2, coll, "main", true)` has no live entries (the
   fold consumed everything).

Case budget note: each case is ~10 local-FS ops — 24 cases × 2 laws is
comfortably < 10s. If a case count must drop for time, drop to 16 and
say so in your worklog.

## Non-negotiables

- `cargo test --release -p pond_storage` ALL green (existing 160+5+25+3
  + your new suites). Run `cargo test -p pond_storage` (debug) too —
  both profiles must pass; debug artifacts were deleted for disk space,
  so note debug build time and `df -h /home/z` in your log.
- `cargo clippy --release -p pond_storage --all-targets -- -D warnings`
  clean (integration tests are in --all-targets).
- Zero changes to production src/ unless a law found a bug — then the
  smallest fix + a dedicated regression example in the worklog.
- ZERO secrets in any diff. Commit NOTHING — leave the working tree
  dirty; the orchestrator reviews, commits, pushes.
- When done, APPEND your work record to `/home/z/my-project/worklog.md`
  (Task ID `cron-2026-08-28-1100-b`) using the template at the top of
  that file. Include: files touched, exact test counts (pass/fail,
  cases per law), any law that failed + what you did about it,
  deviations from this spec.

## Validation commands

```bash
cd /home/z/Pond-review
export PATH="$HOME/.cargo/bin:$PATH"
cargo test --release -p pond_storage 2>&1 | tail -8
cargo test -p pond_storage --test laws_crdt --test laws_pman --test laws_journal --release 2>&1 | tail -6
cargo clippy --release -p pond_storage --all-targets -- -D warnings 2>&1 | tail -3
df -h /home/z | tail -1
```

Watch disk: if `target/` balloons past ~6.5G, `cargo clean -p pond_python`.
