# Builder Spec — Task 2-a: D6 read plan (C7 resolve_packs + C11 RG-level filtering)

You are a builder subagent for the Pond2 repo at `/home/z/Pond-review`.
READ `/home/z/my-project/worklog.md` FIRST (previous agents' records), then
`ARCHITECTURE.md` §D6 (the settled design you are implementing — it is the
authoritative spec), `ACCEPTANCE.md` (this-cycle contract items 4 and 5),
and `CRITIQUE.md` (C7, C11). This document adds implementation-level detail;
where it conflicts with D6, D6 wins.

## Goal

1. **C7**: export `journal::resolve_packs()` and delegate the FIVE duplicated
   "snapshot + entries → pack list" loops to it: read.rs:67
   (read_all_row_groups), read.rs:767 (read_rows_i64), read.rs:1028
   (read_rows_json_pruned), lenses/lakehouse/rust/src/lib.rs:156,
   lenses/vector/rust/src/lib.rs:217.
2. **C11**: RG-level plan filtering — a partially-covered COMPACT journal
   entry contributes only its NOVEL row groups, so concatenating readers see
   each RG exactly once even under racing compactors with partial overlap.

## Exact design (follow precisely)

### 1. New types + function in `core/storage/src/journal.rs`

```rust
/// Identity of one row group: (blob_hash, slab_byte_offset). Stable across
/// folds because compaction copies RG entries verbatim (only `key` is
/// re-sequenced). Two RG entries with the same identity are the same data.
pub type RgIdentity = (String, Option<u64>);

/// One pack in the read plan.
#[derive(Debug, Clone)]
pub struct PackPlan {
    pub pack_hash: String,
    /// None = read all RGs of this pack (snapshot packs, data entries,
    /// fully-novel compact entries). Some(set) = read ONLY the RGs whose
    /// identity is in the set (the novel RGs of a partially-covered
    /// compaction pack).
    pub only_rgs: Option<std::collections::BTreeSet<RgIdentity>>,
}

pub fn resolve_packs(
    kernel: &PondKernel,
    collection: &str,
    branch: &str,
    force_refresh: bool,
) -> Result<Vec<PackPlan>, String>
```

Implementation:
- `let view = resolve_view(kernel, collection, branch, force_refresh)?;`
- If `view.entries` is empty → plans = `[snapshot?]` (only_rgs: None each).
  If no snapshot either → empty vec.
- Classify live entry packs with the existing `classify_packs` (reuse it —
  do NOT duplicate it). If NO entry is a compact pack → return
  `[snapshot?] + entries.map(PackPlan { pack_hash, only_rgs: None })`.
  This is the steady-state fast path: zero extra blob reads.
- With compact entries present:
  1. `covered: BTreeSet<RgIdentity>` = identities of the snapshot's RGs
     (if snapshot exists) ∪ identities of every DATA entry's RGs.
     Write a small helper `collect_rg_identities(kernel, pack_hash, &mut set)`
     next to the existing `collect_rg_hashes` (which you may then DELETE if
     nothing else uses it — check first: it is used by the old step 4.5 you
     are removing).
  2. For each live entry in (writer, seq) order (the order they already sit
     in `view.entries`):
     - DATA entry → plan `PackPlan { pack_hash, only_rgs: None }`, and its
       RG identities join `covered`.
     - COMPACT entry → `novel = its RG identities − covered`.
       - `novel.is_empty()` → DROP the entry from the plan entirely.
       - `novel.len() == all its RGs` → keep whole (`only_rgs: None`).
       - else → `PackPlan { pack_hash, only_rgs: Some(novel) }`.
       - In all kept cases, `covered ∪= novel` (for subsequent compacts).
     - Classification failures (unreadable pack → classify_packs says
       `false` = data) fail safe: kept whole. Same as today.
  3. Plan order: snapshot FIRST, then entries in (writer, seq) order —
     identical to today's pack order, so output row ordering is unchanged.

### 2. `resolve_view` loses step 4.5 (the pack-granular F2 drop)

Delete the whole "4.5 TRIBUNAL F2" block from `resolve_view` (journal.rs
lines ~527–590): the classification, coverage, and filtering all move into
`resolve_packs` at RG granularity. `resolve_view` becomes the RAW view:
snapshot + every live entry above `upto`, persisted to the discovery cache
via `note_live_entries` as today. Keep everything else in resolve_view
(probing, watermark logic, remembered-entry filtering) byte-identical.

Update the doc comment of `resolve_view` to say the raw view may contain
stale compaction packs and that READERS must go through `resolve_packs`
(compact/status/history keep using the raw view directly).

### 3. `compact` uses BOTH the raw view and the plan

In `compact` (journal.rs ~816):
- KEEP `let view = resolve_view(kernel, collection, branch, true)?;` — the
  raw view drives `upto` (live_max now correctly includes stale compact
  entries → the delete loop finally removes zombie entries) and
  `entries_folded`.
- BUILD the union from the plan: `let plans = resolve_packs(kernel,
  collection, branch, true)?;` and iterate `plans` instead of the raw pack
  list for `union_rgs`/`union_schema`. Apply each plan's `only_rgs` filter
  when extending: `for plan in &plans { ...manifest resolve...; if let
  Some(only) = &plan.only_rgs { manifest.row_groups.retain(|rg|
  only.contains(&(rg.blob_hash.clone(), rg.slab_byte_offset))) } ...
  union_rgs.extend(manifest.row_groups) }`.
- ADDITIONALLY dedup `union_rgs` by identity while building (BTreeSet of
  identities seen; skip an RG whose identity was already added). This
  self-heals pre-D6 snapshots that already carry duplicated RGs. Note:
  apply the dedup at extension time (after the retain), and keep FIRST
  occurrence (order stability).
- CAREFUL: resolve_view(force=true) and resolve_packs(force=true) each
  refresh discovery — that is fine (idempotent), but call them ONCE each;
  do not resolve twice.
- `has_live_state` keeps using the raw `resolve_view` — unchanged.

### 4. The five readers delegate

Each site replaces its inline loop:
```rust
let plans = crate::journal::resolve_packs(kernel, collection, branch, false)?;
// (lenses: pond_storage::journal::resolve_packs)
for plan in &plans {
    let manifest_bytes = commit::resolve_manifest_bytes(kernel, &plan.pack_hash)...;
    let mut manifest = resolve_manifest(kernel, &manifest_bytes, None)?;
    if let Some(only) = &plan.only_rgs {
        manifest.row_groups.retain(|rg|
            only.contains(&(rg.blob_hash.clone(), rg.slab_byte_offset)));
    }
    ... existing per-manifest logic unchanged ...
}
```
- read.rs:67 `read_all_row_groups` — also KEEP the "no commits" error when
  plans is empty (packs.is_empty() check preserved).
- read.rs:767 `read_rows_i64` — same.
- read.rs:1028 `read_rows_json_pruned` — same; find where it builds its
  pack list and delegate. NOTE: this function's callers expect the same
  error string for empty collections — preserve it.
- lenses/lakehouse/rust/src/lib.rs:156 and lenses/vector/rust/src/lib.rs:217
  — same pattern with `pond_storage::journal::resolve_packs`.
- read.rs:2300 is a TEST using resolve_view directly — leave it, or migrate
  if trivial.

### 5. Tests (all in `core/storage/tests/journal_test.rs` unless noted)

1. **C11 regression (the acceptance-required construction)** — the
   partial-overlap aftermath is: the ref-LWW WINNER folded LESS than the
   loser, and the loser's delete loop already removed the data entries the
   winner missed, so those rows are ONLY reachable through the loser's
   live compact pack. Fabricate directly with the F2 test's technique
   (`fabricate_entry`, hand-built packs, `entry_path`, `branch_ref`):
   - w1 appends data entry D1 (rows A: ids 1–5) at W1 seq 1; w2 appends
     D2 (rows B: ids 6–10) at W2 seq 1.
   - Winner S_A: hand-built compact pack whose manifest contains ONLY
     D1's RG, commit `journal: {writer: "W_A", seq: 1, upto: {W1: 1}}`;
     reference `branch_ref` → S_A. (It resolved before D2 existed.)
   - DELETE both data entry paths (`W1/000000000001` and `W2/000000000001`)
     — the two racing delete loops removed them in the real aftermath.
     THIS IS THE CRITICAL DETAIL: with D2's path still present, D2 would
     be a live DATA entry, S_B would be fully covered, and the test would
     pass even without D6 (that is the F2 full-overlap case).
   - Loser S_B: compact pack, manifest = D1's RG + D2's RG, commit
     `journal: {writer: "W_B", seq: 1, upto: {W1: 1, W2: 1, W_B: 1}}`,
     referenced at `entry_path("c", "main", "W_B", 1)`. Branch_ref STAYS
     at S_A (B lost the LWW race).
   - Assert: `read_rows_i64` returns exactly 10 rows, ids 1–10 once each
     (pre-D6: D1's rows appear TWICE — 15 total). Assert
     `read_rows_json_pruned` sees 10 rows. Assert `resolve_packs` yields
     a plan for S_B with `only_rgs == Some({D2's RG identity})` and that
     D1's identity is NOT in it.
2. **F2 regression still green** (already exists — verify it passes
   unchanged; the loser fully-covered pack must drop at plan level now).
3. **Fast path**: with only data entries live, `resolve_packs` performs NO
   blob reads beyond resolve_view's own (use the existing CountingStore
   technique from read.rs tests if cheap; otherwise assert plans ==
   [snapshot, entries] with all `only_rgs: None`).
4. **Zombie cleanup**: after the C11 construction, run one real
   `journal::compact` — the stale S_B entry path must be DELETED (probe or
   resolve returns None for its entry path) and the folded snapshot's
   manifest has NO duplicate RG identities.
5. **Order stability**: plans order == [snapshot] + entries order; a
   read before/after the refactor gives identical row order (covered by
   existing tests, just don't break them).

### 6. Non-negotiables

- `cargo test --workspace` green (542+ tests today; expect +4 or so).
- `cargo clippy --workspace --all-targets -- -D warnings` clean.
- NO behavior change for CRDT-merging readers beyond dedup of RGs they
  were double-reading (rows collapsed by _rowid anyway).
- `put_path_if`/`reference_if` gain NO new callers (D3).
- Zero secrets in the diff. Commit nothing — leave the working tree dirty;
  the orchestrator reviews, commits, and pushes.
- When done, APPEND your work record to `/home/z/my-project/worklog.md`
  (Task ID `cron-2026-08-28-1100-a`) using the template at the top of that
  file. Include: files touched, test results (exact numbers), deviations
  from this spec, and anything you left unfinished.

## Validation commands

```bash
cd /home/z/Pond-review
cargo test -p pond_storage 2>&1 | tail -5
cargo test -p pond-lakehouse -p pond-vector 2>&1 | tail -5   # actual crate names: check lenses/*/rust/Cargo.toml
cargo clippy -p pond_storage -p pond-lakehouse -p pond-vector --all-targets -- -D warnings 2>&1 | tail -5
```
(Also run the full `cargo test --workspace` once at the end if time
permits; it is the orchestrator's job to do the final full validation.)
Watch disk space: `df -h /home/z` — if the target dir balloons past ~6GB,
`cargo clean -p pond_python` first.
