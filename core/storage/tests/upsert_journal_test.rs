// Upsert-journal integration tests — the D7 contract (ARCHITECTURE.md D7,
// ACCEPTANCE.md iteration N+4 items 1–3; builder spec
// docs/builder-spec-journal-shards.md Deliverable 3).
//
// C5-a made `shard::upsert_shard`/`delete_shard` journal writers: stamped
// rows become ONE PND2 pack per call at a unique per-writer path. These
// tests pin the journal-era semantics on the upsert surface:
//   1. upsert → read_rows_json_pruned returns the live rows (values,
//      _rowid stability across upserts of the same rowid);
//   2. delete tombstones suppress; resurrection (later live version) works;
//   3. two writer PROCESSES upsert concurrently → a FRESH reader (empty
//      caches — the C9 law applied to the upsert surface) sees the union;
//   4. MIXED legacy state: a hand-written JSON shard (raw append_shard
//      escape hatch) + a journal upsert — read_with_shards sees the legacy
//      shard, the core journal reader sees the upsert, compact folds BOTH,
//      and a fresh post-compact reader sees both rows;
//   5. journal::status shows the upsert entries as live; compact folds
//      them; the post-compact fresh read is identical; journal::history
//      keeps the per-write messages (upsert_shard:<name>) across folds.
//
// The shard.rs unit tests re-pin the same surface at the unit level
// (roundtrip + tombstone + the empty-rows nothing-written edge).

use pond_kernel::crdt::HLC;
use pond_storage::journal;
use pond_storage::read;
use pond_storage::shard;
use pond_storage::UnifiedStorage;
use serde_json::{json, Value};

fn key_fields() -> Vec<String> {
    vec!["_rowid".to_string()]
}

/// Sorted (rowid, value) pairs out of a pruned read — order-independent
/// comparison (the CRDT merge sorts by rowid, but new code paths may
/// batch differently).
fn sorted_rows(rows: &[(String, Value)], val_col: &str) -> Vec<(String, String)> {
    let mut out: Vec<(String, String)> = rows
        .iter()
        .map(|(rid, row)| {
            (
                rid.clone(),
                row.get(val_col)
                    .map(|v| v.to_string())
                    .unwrap_or_default(),
            )
        })
        .collect();
    out.sort();
    out
}

// ---------------------------------------------------------------------------
// 1. Upsert roundtrip through the journal-aware pruned reader
// ---------------------------------------------------------------------------

#[test]
fn test_upsert_roundtrip_and_rowid_stability() {
    let dir = tempfile::tempdir().unwrap();
    let storage = UnifiedStorage::new_local(dir.path()).unwrap();
    let kernel = storage.kernel();
    let mut hlc = HLC::new();

    // Two rows with explicit _rowids, one without (upsert generates it).
    let rows = vec![
        json!({"_rowid": "r-alpha", "id": 1, "val": "one"}),
        json!({"_rowid": "r-beta", "id": 2, "val": "two"}),
        json!({"id": 3, "val": "three"}),
    ];
    let pack = shard::upsert_shard(kernel, "users", "main", "s1", &rows, Some("id"), &mut hlc).unwrap();
    assert!(!pack.is_empty());

    let live = read::read_rows_json_pruned(kernel, "users", "main", &key_fields(), None, &[])
        .unwrap();
    assert_eq!(live.len(), 3, "all three upserted rows must be live");
    let val_of = |rows: &[(String, Value)], rid: &str| -> String {
        rows.iter()
            .find(|(r, _)| r == rid)
            .map(|(_, row)| row["val"].to_string())
            .unwrap_or_else(|| panic!("rowid {} missing from {:?}", rid, rows))
    };
    assert_eq!(val_of(&live, "r-alpha"), "\"one\"");
    assert_eq!(val_of(&live, "r-beta"), "\"two\"");
    // The third row's generated rowid is a UUIDv7 string (non-empty, and
    // the only row left after the two explicit ones).
    let generated: Vec<&String> = live.iter()
        .map(|(r, _)| r)
        .filter(|r| **r != "r-alpha" && **r != "r-beta")
        .collect();
    assert_eq!(generated.len(), 1, "exactly one generated rowid");
    assert!(!generated[0].is_empty(), "generated _rowid must be non-empty");
    assert_eq!(val_of(&live, generated[0]), "\"three\"");

    // Upsert the SAME _rowid with a new value (same HLC ⇒ strictly later
    // _version): the rowid must stay stable, the value must update, and
    // the row count must NOT grow (LWW dedup, not append).
    let upd = vec![json!({"_rowid": "r-alpha", "id": 1, "val": "ONE-updated"})];
    shard::upsert_shard(kernel, "users", "main", "s2", &upd, Some("id"), &mut hlc).unwrap();

    let live = read::read_rows_json_pruned(kernel, "users", "main", &key_fields(), None, &[])
        .unwrap();
    assert_eq!(live.len(), 3, "re-upsert of an existing rowid dedups (LWW)");
    assert_eq!(val_of(&live, "r-alpha"), "\"ONE-updated\"",
        "latest _version must win");
    assert_eq!(val_of(&live, "r-beta"), "\"two\"",
        "unrelated rows must be untouched");

    // No JSON shard blob was written anywhere (D7).
    assert_eq!(shard::shard_count(kernel, "users", "main"), 0);
}

// ---------------------------------------------------------------------------
// 2. Tombstones suppress; resurrection works
// ---------------------------------------------------------------------------

#[test]
fn test_delete_tombstone_suppresses_and_resurrect() {
    let dir = tempfile::tempdir().unwrap();
    let storage = UnifiedStorage::new_local(dir.path()).unwrap();
    let kernel = storage.kernel();
    let mut hlc = HLC::new();
    let kc = key_fields();

    let rows = vec![
        json!({"_rowid": "keep", "id": 1, "val": "keep"}),
        json!({"_rowid": "kill", "id": 2, "val": "kill"}),
    ];
    shard::upsert_shard(kernel, "t", "main", "u1", &rows, Some("id"), &mut hlc).unwrap();
    let live = read::read_rows_json_pruned(kernel, "t", "main", &kc, None, &[]).unwrap();
    assert_eq!(live.len(), 2);

    // Tombstone "kill" (same HLC ⇒ strictly later version than the upsert).
    let del = shard::delete_shard(kernel, "t", "main", "d1", &["kill".to_string()], Some("id"), &mut hlc).unwrap();
    assert!(!del.is_empty());

    let live = read::read_rows_json_pruned(kernel, "t", "main", &kc, None, &[]).unwrap();
    assert_eq!(live.len(), 1, "the tombstoned row must be suppressed");
    assert_eq!(live[0].0, "keep");

    // Resurrection: a LATER live version (same HLC keeps ticking) revives
    // the row.
    let res = vec![json!({"_rowid": "kill", "id": 2, "val": "revived"})];
    shard::upsert_shard(kernel, "t", "main", "u2", &res, Some("id"), &mut hlc).unwrap();
    let live = read::read_rows_json_pruned(kernel, "t", "main", &kc, None, &[]).unwrap();
    assert_eq!(live.len(), 2, "a later live version must resurrect the row");
    let revived = live.iter().find(|(rid, _)| rid == "kill").unwrap();
    assert_eq!(revived.1["val"], json!("revived"));

    // No JSON shards anywhere (D7).
    assert_eq!(shard::shard_count(kernel, "t", "main"), 0);
}

// ---------------------------------------------------------------------------
// 3. Two writer PROCESSES → union visible to a FRESH reader (C9 law on the
//    upsert surface). Parent writes 2 rows; a spawned child (fresh
//    process, TTL=0, empty caches) upserts 2 more and reads the union;
//    a second spawned child reads-only with completely empty caches.
// ---------------------------------------------------------------------------

#[test]
fn test_upsert_two_writers_fresh_process_union() {
    // Child role 1: WRITER — fresh process over the parent's dir.
    if std::env::var("POND_UPSERT_WRITER_CHILD").is_ok() {
        let dir = std::env::var("POND_TEST_DIR").unwrap();
        let storage = UnifiedStorage::new_local(std::path::Path::new(&dir)).unwrap();
        let kernel = storage.kernel();
        let mut hlc = HLC::new();

        let rows = vec![
            json!({"_rowid": "c-1", "id": 11, "val": "child-one"}),
            json!({"_rowid": "c-2", "id": 12, "val": "child-two"}),
        ];
        shard::upsert_shard(kernel, "two", "main", "child_u1", &rows, Some("id"), &mut hlc).unwrap();

        // The child sees its OWN writes (instant) + the parent's (snapshot
        // + probes) — the union.
        let live = read::read_rows_json_pruned(kernel, "two", "main", &key_fields(), None, &[])
            .unwrap();
        let got = sorted_rows(&live, "val");
        assert_eq!(got.len(), 4, "writer child must see the union: {:?}", got);
        assert!(got.iter().any(|(r, _)| r == "p-1"));
        assert!(got.iter().any(|(r, _)| r == "p-2"));
        assert!(got.iter().any(|(r, _)| r == "c-1"));
        assert!(got.iter().any(|(r, _)| r == "c-2"));
        return;
    }

    // Child role 2: READER — fresh process, empty caches, reads only.
    if std::env::var("POND_UPSERT_READER_CHILD").is_ok() {
        let dir = std::env::var("POND_TEST_DIR").unwrap();
        let storage = UnifiedStorage::new_local(std::path::Path::new(&dir)).unwrap();
        let kernel = storage.kernel();

        let live = read::read_rows_json_pruned(kernel, "two", "main", &key_fields(), None, &[])
            .unwrap();
        let got = sorted_rows(&live, "val");
        assert_eq!(got.len(), 4, "fresh reader must see both writers' upserts: {:?}", got);
        assert!(got.iter().any(|(r, _)| r == "p-1"));
        assert!(got.iter().any(|(r, _)| r == "c-1"));
        return;
    }

    // Parent: write 2 rows, then spawn both children (TTL=0 — exact
    // freshness; the discovery cache in THIS process must not matter to
    // the children, whose caches start empty).
    let dir = tempfile::tempdir().unwrap();
    let storage = UnifiedStorage::new_local(dir.path()).unwrap();
    let kernel = storage.kernel();
    let mut hlc = HLC::new();

    let rows = vec![
        json!({"_rowid": "p-1", "id": 1, "val": "parent-one"}),
        json!({"_rowid": "p-2", "id": 2, "val": "parent-two"}),
    ];
    shard::upsert_shard(kernel, "two", "main", "parent_u1", &rows, Some("id"), &mut hlc).unwrap();

    for (marker, name) in [
        ("POND_UPSERT_WRITER_CHILD", "writer"),
        ("POND_UPSERT_READER_CHILD", "reader"),
    ] {
        // The writer child must run BEFORE the reader child (the reader
        // asserts the union of BOTH writers).
        let out = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("test_upsert_two_writers_fresh_process_union")
            .env(marker, "1")
            .env("POND_TEST_DIR", dir.path())
            .env("POND_JOURNAL_TTL_MS", "0")
            .output()
            .unwrap_or_else(|e| panic!("{} child must run: {}", name, e));
        assert!(out.status.success(),
            "{} child (TTL=0) failed:\nstdout: {}\nstderr: {}",
            name,
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr));
    }
}

// ---------------------------------------------------------------------------
// 4. MIXED state: legacy JSON shard (raw escape hatch) + journal upsert
// ---------------------------------------------------------------------------

#[test]
fn test_mixed_legacy_shard_plus_journal_upsert() {
    let dir = tempfile::tempdir().unwrap();
    let storage = UnifiedStorage::new_local(dir.path()).unwrap();
    let kernel = storage.kernel();
    let kc = key_fields();

    // Journal upsert FIRST (its first append bootstraps the snapshot —
    // and the bootstrap fold would absorb any pre-existing shard, so the
    // legacy shard must be written AFTER it to stay live).
    let mut hlc = HLC::new();
    let rows = vec![json!({"_rowid": "journal-1", "id": 2, "src": "journal"})];
    shard::upsert_shard(kernel, "mixed", "main", "j1", &rows, Some("id"), &mut hlc).unwrap();

    // Legacy JSON shard via the raw escape hatch (pre-D7 write shape).
    let legacy_rows = vec![json!({
        "_rowid": "legacy-1", "_version": "00000000000000000000000000000001",
        "_deleted": false, "id": 1, "src": "legacy",
    })];
    let legacy_bytes = serde_json::to_vec(&legacy_rows).unwrap();
    shard::append_shard(kernel, "mixed", "main", "legacy_shard", &legacy_bytes).unwrap();

    // read_with_shards (legacy-compat) sees the LEGACY shard...
    let (_, shards) = shard::read_with_shards(kernel, "mixed", "main");
    assert_eq!(shards.len(), 1, "the hand-written legacy shard must be visible");
    assert_eq!(shards[0].0, "legacy_shard");

    // ...while the core journal reader sees the journal upsert (the legacy
    // shard is NOT folded yet). PINNING THE ACTUAL SPLIT: the pyo3/SQL
    // read paths union read_with_shards on top of this reader, so the
    // flagship API sees both; the core reader alone sees legacy shard rows
    // only after the fold below.
    let live = read::read_rows_json_pruned(kernel, "mixed", "main", &kc, None, &[]).unwrap();
    assert_eq!(live.len(), 1, "core reader sees the journal upsert only (pre-fold)");
    assert_eq!(live[0].0, "journal-1");

    // Compact folds BOTH worlds: the journal entry (already in the
    // snapshot after the bootstrap) and the live legacy shard.
    let stats = journal::compact(kernel, "mixed", "main", &["id".to_string()]).unwrap();
    assert_eq!(stats.shards_folded, 1, "the legacy shard must be folded");
    assert!(!stats.new_snapshot.is_empty());

    // Post-compact: BOTH rows visible through the journal reader, the
    // legacy shard is cleared, and a FRESH reader sees the same state.
    let live = read::read_rows_json_pruned(kernel, "mixed", "main", &kc, None, &[]).unwrap();
    assert_eq!(sorted_rows(&live, "src").len(), 2,
        "post-compact read must see the legacy row (fold RG) + the journal row");
    assert!(live.iter().any(|(r, _)| r == "legacy-1"));
    assert!(live.iter().any(|(r, _)| r == "journal-1"));
    assert_eq!(shard::shard_count(kernel, "mixed", "main"), 0);

    let fresh = UnifiedStorage::new_local(dir.path()).unwrap();
    let live2 = read::read_rows_json_pruned(fresh.kernel(), "mixed", "main", &kc, None, &[])
        .unwrap();
    assert_eq!(sorted_rows(&live2, "src"), sorted_rows(&live, "src"),
        "a fresh reader (empty caches) must see the identical post-compact state");
}

// ---------------------------------------------------------------------------
// 5. journal::status / history / compact on the upsert surface
// ---------------------------------------------------------------------------

#[test]
fn test_status_shows_upsert_entries_and_compact_folds() {
    let dir = tempfile::tempdir().unwrap();
    let storage = UnifiedStorage::new_local(dir.path()).unwrap();
    let kernel = storage.kernel();
    let kc = key_fields();
    let mut hlc = HLC::new();

    // Three upserts, distinct shard names. The FIRST append bootstraps
    // the snapshot (its entry folds immediately); the remaining appends
    // stay live until a fold.
    for i in 0..3 {
        let rows = vec![json!({"_rowid": format!("r-{}", i), "id": i, "val": i})];
        shard::upsert_shard(kernel, "st", "main", &format!("up_{}", i), &rows, Some("id"), &mut hlc).unwrap();
    }

    let status = journal::status(kernel, "st", "main").unwrap();
    assert!(status.snapshot.is_some(), "bootstrap fold created a snapshot");
    assert!(status.live_entries >= 1,
        "the post-bootstrap upserts must be live journal entries (got {})",
        status.live_entries);
    assert!(status.writers.iter().all(|w| w.entries <= status.live_entries));

    // History keeps per-write visibility: each upsert's message carries
    // its shard name (upsert_shard:<name>).
    let hist = journal::history(kernel, "st", "main", 64).unwrap();
    let messages: Vec<&str> = hist.iter().map(|(_, c)| c.message.as_str()).collect();
    for i in 0..3 {
        let want = format!("upsert_shard:up_{}", i);
        assert!(messages.iter().any(|m| *m == want),
            "history must keep the per-write message {:?} (got {:?})", want, messages);
    }

    // Compact folds the live entries; post-compact fresh read identical.
    let before = read::read_rows_json_pruned(kernel, "st", "main", &kc, None, &[]).unwrap();
    let stats = journal::compact(kernel, "st", "main", &["id".to_string()]).unwrap();
    assert_eq!(stats.entries_folded, status.live_entries,
        "compact must fold exactly the live entries");

    let after = read::read_rows_json_pruned(kernel, "st", "main", &kc, None, &[]).unwrap();
    assert_eq!(sorted_rows(&after, "val"), sorted_rows(&before, "val"),
        "compaction must not change the visible state");

    let status2 = journal::status(kernel, "st", "main").unwrap();
    assert_eq!(status2.live_entries, 0, "everything is folded after compact");

    // Fresh reader: identical state (C9 law — the fold is the complete state).
    let fresh = UnifiedStorage::new_local(dir.path()).unwrap();
    let fresh_rows = read::read_rows_json_pruned(fresh.kernel(), "st", "main", &kc, None, &[])
        .unwrap();
    assert_eq!(sorted_rows(&fresh_rows, "val"), sorted_rows(&before, "val"));

    // The folded upsert messages stay visible through the fold's `folds`
    // list (compaction must not erase history granularity).
    let hist2 = journal::history(kernel, "st", "main", 64).unwrap();
    let messages2: Vec<&str> = hist2.iter().map(|(_, c)| c.message.as_str()).collect();
    for i in 0..3 {
        let want = format!("upsert_shard:up_{}", i);
        assert!(messages2.iter().any(|m| *m == want),
            "post-compact history must keep the folded write's message {:?} (got {:?})",
            want, messages2);
    }
}

// ---------------------------------------------------------------------------
// 6. D7 pruning-exemption regressions (the SQL update-OUT-of-range fix)
// ---------------------------------------------------------------------------
//
// Root cause (found by test_where_pushdown_shard_updated_row_disappears,
// sql_integration): a value-based pre-filter can drop the authoritative
// NEWER copy of an updated row while the stale copy survives, so the CRDT
// merge resurrects outdated state. Fix: CRDT-update RGs (stats carry
// `_deleted`) are exempt from value pruning in the MERGING reader and
// skipped by NON-MERGING readers (pre-D7 shard-invisible equivalence).

use pond_core::TypedColumn;

fn seed_users_base(kernel: &pond_kernel::PondKernel) {
    let columns: Vec<(&str, TypedColumn)> = vec![
        ("name", TypedColumn::String(vec![
            "alice".to_string(), "carol".to_string(), "dave".to_string(),
        ])),
        ("age", TypedColumn::Int64(vec![30, 35, 40])),
    ];
    pond_storage::write::write_rows(kernel, "users", "main", &columns, "seed").unwrap();
}

/// Update moves a row OUT of the predicate range: the pruned read's output
/// must carry the UPDATED value (age 22) for the row, not the stale base
/// copy (age 30) — the pre-fix bug resurrected the stale copy. The caller's
/// post-merge filter (SQL executor / pyo3) then correctly drops the row.
#[test]
fn test_upsert_update_out_of_range_not_resurrected() {
    let dir = tempfile::tempdir().unwrap();
    let storage = UnifiedStorage::new_local(dir.path()).unwrap();
    let kernel = storage.kernel();
    let kc = key_fields();
    seed_users_base(kernel);

    // Find alice's base _rowid (write_rows CRDT-stamped it), observe the
    // versions, then upsert her to age 22 — OUT of the `age >= 30` range.
    let base = read::read_rows_json_pruned(kernel, "users", "main", &kc, None, &[]).unwrap();
    let alice_rowid = base.iter()
        .find(|(_, row)| row.get("name").and_then(|v| v.as_str()) == Some("alice"))
        .map(|(rid, _)| rid.clone())
        .expect("alice must exist in the base write");
    let mut hlc = HLC::new();
    for (_, row) in &base {
        if let Some(v) = row.get("_version").and_then(|v| v.as_str()) {
            hlc.observe(v);
        }
    }
    let upd = vec![json!({
        "_rowid": alice_rowid, "name": "alice", "age": 22,
    })];
    shard::upsert_shard(kernel, "users", "main", "upd_alice", &upd, Some("name"), &mut hlc).unwrap();

    // Pruned read with the predicate. The pre-filter exemption must keep
    // the update pack's alice row (age 22 — does NOT match) so the CRDT
    // merge overrides her base copy. Output contract: the merged alice row
    // carries age 22 (updated), and the matching rows carol/dave survive.
    let preds = vec![("age".to_string(), ">=".to_string(), json!(30))];
    let out = read::read_rows_json_pruned(kernel, "users", "main", &kc, None, &preds).unwrap();
    let ages: Vec<(String, i64)> = out.iter()
        .filter_map(|(_, row)| {
            let name = row.get("name")?.as_str()?.to_string();
            let age = row.get("age")?.as_i64()?;
            Some((name, age))
        })
        .collect();
    assert!(ages.contains(&("alice".to_string(), 22)),
        "the UPDATED alice copy (age 22) must reach the merge output — got {:?}",
        ages);
    assert!(ages.contains(&("carol".to_string(), 35)));
    assert!(ages.contains(&("dave".to_string(), 40)));
    assert!(!ages.contains(&("alice".to_string(), 30)),
        "the STALE base copy (age 30) must be overridden by the update — got {:?}",
        ages);
    assert_eq!(out.len(), 3, "exactly one row per surviving rowid");
}

/// The folded snapshot keeps the exemption: after compact, the CRDT RG's
/// stats still carry `_deleted`, so the update-OUT-of-range query still
/// yields the updated copy (this also pins the LATENT pre-D7 hole for
/// folded shard RGs — compact's shard fold kept tombstones, so a folded
/// update RG was value-prunable the same way before D7).
#[test]
fn test_compact_preserves_update_out_of_range_correctness() {
    let dir = tempfile::tempdir().unwrap();
    let storage = UnifiedStorage::new_local(dir.path()).unwrap();
    let kernel = storage.kernel();
    let kc = key_fields();
    seed_users_base(kernel);

    let base = read::read_rows_json_pruned(kernel, "users", "main", &kc, None, &[]).unwrap();
    let alice_rowid = base.iter()
        .find(|(_, row)| row.get("name").and_then(|v| v.as_str()) == Some("alice"))
        .map(|(rid, _)| rid.clone())
        .expect("alice must exist in the base write");
    let mut hlc = HLC::new();
    for (_, row) in &base {
        if let Some(v) = row.get("_version").and_then(|v| v.as_str()) {
            hlc.observe(v);
        }
    }
    let upd = vec![json!({"_rowid": alice_rowid, "name": "alice", "age": 22})];
    shard::upsert_shard(kernel, "users", "main", "upd_alice", &upd, Some("name"), &mut hlc).unwrap();

    // Fold EVERYTHING (bootstrap snapshot + base + update entries).
    journal::compact(kernel, "users", "main", &["name".to_string()]).unwrap();
    let status = journal::status(kernel, "users", "main").unwrap();
    assert_eq!(status.live_entries, 0, "everything folded");

    // Post-fold: the `age >= 30` pruned read must still deliver the
    // UPDATED alice (22) — the folded CRDT RG is exempt from value pruning.
    let preds = vec![("age".to_string(), ">=".to_string(), json!(30))];
    let out = read::read_rows_json_pruned(kernel, "users", "main", &kc, None, &preds).unwrap();
    let ages: Vec<(String, i64)> = out.iter()
        .filter_map(|(_, row)| {
            let name = row.get("name")?.as_str()?.to_string();
            let age = row.get("age")?.as_i64()?;
            Some((name, age))
        })
        .collect();
    assert!(ages.contains(&("alice".to_string(), 22)),
        "post-fold: the updated alice (22) must still override the stale copy — got {:?}",
        ages);
    assert!(!ages.contains(&("alice".to_string(), 30)),
        "post-fold: the stale copy must not resurrect — got {:?}", ages);

    // And a FRESH reader (empty caches) sees the same state.
    let fresh = UnifiedStorage::new_local(dir.path()).unwrap();
    let out2 = read::read_rows_json_pruned(fresh.kernel(), "users", "main", &kc, None, &preds).unwrap();
    assert_eq!(out.len(), out2.len());
}

/// NON-MERGING readers skip CRDT-update RGs (pre-D7 shard-invisible
/// equivalence): read_rows_i64 over an i64 base write + a journal upsert
/// returns EXACTLY the base rows — no duplicates from the update copy. The
/// merging reader still SEES the upsert row (mixed legacy+CRDT state: the
/// rowid-less i64 base rows pass through; the upsert row is live).
#[test]
fn test_i64_reader_skips_crdt_update_rgs() {
    let dir = tempfile::tempdir().unwrap();
    let storage = UnifiedStorage::new_local(dir.path()).unwrap();
    let kernel = storage.kernel();

    // Base i64 data (write_rows_i64 — the pre-D7 KV shape: no CRDT stamps,
    // prunable RGs the i64 reader reads normally).
    let base_cols: Vec<(&str, &[i64])> = vec![("k", &[1, 2, 3]), ("v", &[10, 20, 30])];
    pond_storage::write::write_rows_i64(kernel, "kv", "main", &base_cols, "seed").unwrap();

    // A journal upsert on the same collection (D7).
    let mut hlc = HLC::new();
    let upd = vec![json!({"_rowid": "upd-1", "k": 2, "v": 99})];
    shard::upsert_shard(kernel, "kv", "main", "upd", &upd, Some("k"), &mut hlc).unwrap();

    // The i64 columnar reader (COLUMN-MAJOR output: (name, values) per
    // column): base columns ONLY — the update RG is skipped, no duplicate
    // k=2 row concatenated into the column vectors.
    let want_cols = vec!["k".to_string(), "v".to_string()];
    let cols = read::read_rows_i64(kernel, "kv", "main", Some(&want_cols), None).unwrap();
    let col = |name: &str| cols.iter().find(|(n, _)| n == name).map(|(_, v)| v.clone());
    assert_eq!(col("k"), Some(vec![1, 2, 3]),
        "the k column carries exactly the base rows (update RG skipped): {:?}", cols);
    assert_eq!(col("v"), Some(vec![10, 20, 30]),
        "the v column carries the BASE values (pre-D7 i64 semantics): {:?}", cols);

    // The merging reader sees the upsert row too (mixed legacy+CRDT: the
    // rowid-less base rows pass through; the upsert row is live).
    let merged = read::read_rows_json_pruned(kernel, "kv", "main", &key_fields(), None, &[])
        .unwrap();
    assert_eq!(merged.len(), 4,
        "3 legacy base rows + 1 live upsert row (no shared rowid to collapse): {:?}",
        merged.len());
    let upsert_v = merged.iter()
        .find(|(rid, _)| rid == "upd-1")
        .and_then(|(_, r)| r.get("v").and_then(|v| v.as_i64()));
    assert_eq!(upsert_v, Some(99), "the upsert row is visible through the merging reader");
}

// ---------------------------------------------------------------------------
// 7. Tribunal r4 finding-1 regressions + the missing constructions
// ---------------------------------------------------------------------------
//
// The tribunal empirically proved: after a compact whose union schema
// absorbs `_deleted`, normalize_rgs_to_schema pads EVERY base RG with a
// `_deleted` PLACEHOLDER — the name-only is_crdt_update_rg misfired on all
// of them and the non-merging readers went blind post-fold (0 rows for 3).
// Fix: the check requires REAL min/max stats on `_deleted`. These tests
// pin the exact constructions that were missing.

/// THE tribunal probe as a regression: base write + ONE upsert + compact
/// ⇒ the non-merging readers still see the BASE rows post-fold (the
/// placeholder-padded base RGs are NOT CRDT RGs), and read_all_row_groups
/// still returns the base RGs.
#[test]
fn test_compact_mixed_base_plus_upsert_nonmerging_readers_see_base() {
    let dir = tempfile::tempdir().unwrap();
    let storage = UnifiedStorage::new_local(dir.path()).unwrap();
    let kernel = storage.kernel();

    // Base i64 data + one journal upsert, then fold EVERYTHING.
    let base_cols: Vec<(&str, &[i64])> = vec![("k", &[1, 2, 3]), ("v", &[10, 20, 30])];
    pond_storage::write::write_rows_i64(kernel, "kv", "main", &base_cols, "seed").unwrap();
    let mut hlc = HLC::new();
    let upd = vec![json!({"_rowid": "upd-1", "k": 2, "v": 99})];
    shard::upsert_shard(kernel, "kv", "main", "upd", &upd, Some("k"), &mut hlc).unwrap();
    journal::compact(kernel, "kv", "main", &["k".to_string()]).unwrap();

    // The union schema now includes _deleted (from the upsert RG) — every
    // base RG in the folded manifest carries a _deleted PLACEHOLDER. The
    // real-stats discriminator must classify them as BASE:
    let want_cols = vec!["k".to_string(), "v".to_string()];
    let cols = read::read_rows_i64(kernel, "kv", "main", Some(&want_cols), None).unwrap();
    let col = |name: &str| cols.iter().find(|(n, _)| n == name).map(|(_, v)| v.clone());
    assert_eq!(col("k"), Some(vec![1, 2, 3]),
        "post-compact: the i64 reader must still see the BASE rows (tribunal r4 f1)");
    assert_eq!(col("v"), Some(vec![10, 20, 30]),
        "post-compact: base values intact");

    let rgs = read::read_all_row_groups(kernel, "kv", "main").unwrap();
    assert!(!rgs.is_empty(),
        "post-compact: read_all_row_groups must still return the base RGs (tribunal r4 f1)");

    // The merging reader still applies the upsert over the folded state.
    let merged = read::read_rows_json_pruned(kernel, "kv", "main", &key_fields(), None, &[])
        .unwrap();
    let v2 = merged.iter()
        .find(|(rid, _)| rid == "upd-1")
        .and_then(|(_, r)| r.get("v").and_then(|v| v.as_i64()));
    assert_eq!(v2, Some(99), "the folded upsert row stays visible to the merging reader");
}

/// Resurrection post-compact: base row → tombstoned → folded (suppressed)
/// → a LATER live upsert of the same rowid resurrects it.
#[test]
fn test_resurrection_after_compact() {
    let dir = tempfile::tempdir().unwrap();
    let storage = UnifiedStorage::new_local(dir.path()).unwrap();
    let kernel = storage.kernel();
    let kc = key_fields();
    let mut hlc = HLC::new();

    // Base row (write_rows CRDT-stamps its rowid).
    let columns: Vec<(&str, TypedColumn)> = vec![
        ("name", TypedColumn::String(vec!["alice".to_string()])),
        ("age", TypedColumn::Int64(vec![30])),
    ];
    pond_storage::write::write_rows(kernel, "users", "main", &columns, "seed").unwrap();

    let base = read::read_rows_json_pruned(kernel, "users", "main", &kc, None, &[]).unwrap();
    let rowid = base[0].0.clone();
    for (_, row) in &base {
        if let Some(v) = row.get("_version").and_then(|v| v.as_str()) {
            hlc.observe(v);
        }
    }

    // Tombstone, then fold — the row is suppressed post-fold.
    shard::delete_shard(kernel, "users", "main", "del", std::slice::from_ref(&rowid), Some("name"), &mut hlc).unwrap();
    journal::compact(kernel, "users", "main", &["name".to_string()]).unwrap();
    let live = read::read_rows_json_pruned(kernel, "users", "main", &kc, None, &[]).unwrap();
    assert!(live.is_empty(), "post-fold tombstone suppresses the base row");

    // Resurrect: a LATER live upsert of the same rowid (new _version).
    let res = vec![json!({"_rowid": rowid, "name": "alice", "age": 31})];
    shard::upsert_shard(kernel, "users", "main", "resurrect", &res, Some("name"), &mut hlc).unwrap();
    let live = read::read_rows_json_pruned(kernel, "users", "main", &kc, None, &[]).unwrap();
    assert_eq!(live.len(), 1, "the later live version resurrects the row");
    assert_eq!(live[0].1.get("age").and_then(|v| v.as_i64()), Some(31));
}

/// Update-INTO-range post-compact (the mirror of the update-OUT-of-range
/// regression): a base row outside the predicate range, updated INTO the
/// range, folded — the pruned read must deliver the UPDATED row (the
/// folded CRDT RG is exempt from value pruning).
#[test]
fn test_update_into_range_after_compact() {
    let dir = tempfile::tempdir().unwrap();
    let storage = UnifiedStorage::new_local(dir.path()).unwrap();
    let kernel = storage.kernel();
    let kc = key_fields();
    let mut hlc = HLC::new();

    // Base: alice@22 (OUTSIDE `age >= 30`), carol@35, dave@40.
    let columns: Vec<(&str, TypedColumn)> = vec![
        ("name", TypedColumn::String(vec![
            "alice".to_string(), "carol".to_string(), "dave".to_string(),
        ])),
        ("age", TypedColumn::Int64(vec![22, 35, 40])),
    ];
    pond_storage::write::write_rows(kernel, "users", "main", &columns, "seed").unwrap();

    let base = read::read_rows_json_pruned(kernel, "users", "main", &kc, None, &[]).unwrap();
    let alice_rowid = base.iter()
        .find(|(_, r)| r.get("name").and_then(|v| v.as_str()) == Some("alice"))
        .map(|(rid, _)| rid.clone()).unwrap();
    for (_, row) in &base {
        if let Some(v) = row.get("_version").and_then(|v| v.as_str()) {
            hlc.observe(v);
        }
    }

    // Update alice INTO the range, then fold.
    let upd = vec![json!({"_rowid": alice_rowid, "name": "alice", "age": 33})];
    shard::upsert_shard(kernel, "users", "main", "upd_alice", &upd, Some("name"), &mut hlc).unwrap();
    journal::compact(kernel, "users", "main", &["name".to_string()]).unwrap();

    // Post-fold `age >= 30`: the folded CRDT RG (real _deleted stats) is
    // exempt from value pruning, so the updated alice@33 reaches the merge
    // even though BOTH her copies (22, 33) sit in prunable-looking value
    // ranges on the base side.
    let preds = vec![("age".to_string(), ">=".to_string(), json!(30))];
    let out = read::read_rows_json_pruned(kernel, "users", "main", &kc, None, &preds).unwrap();
    let ages: Vec<(String, i64)> = out.iter()
        .filter_map(|(_, row)| {
            let name = row.get("name")?.as_str()?.to_string();
            let age = row.get("age")?.as_i64()?;
            Some((name, age))
        })
        .collect();
    assert!(ages.contains(&("alice".to_string(), 33)),
        "post-compact: the updated-INTO-range row must be delivered — got {:?}", ages);
    assert!(ages.contains(&("carol".to_string(), 35)));
    assert!(ages.contains(&("dave".to_string(), 40)));
    assert_eq!(out.len(), 3, "exactly one row per rowid");
}

/// Two writers upserting the SAME rowid concurrently: the CRDT merge
/// yields exactly ONE row — the strictly-later version wins (LWW), and a
/// FRESH reader (empty caches) agrees.
#[test]
fn test_two_writers_same_rowid_lww() {
    let dir = tempfile::tempdir().unwrap();
    let storage = UnifiedStorage::new_local(dir.path()).unwrap();
    let kernel = storage.kernel();
    let kc = key_fields();

    // Writer A: version V1, value "a".
    let mut hlc_a = HLC::new();
    let rows_a = vec![json!({"_rowid": "shared-1", "k": 1, "who": "a"})];
    shard::upsert_shard(kernel, "shared", "main", "w_a", &rows_a, Some("k"), &mut hlc_a).unwrap();

    // Writer B observes A's version (reads it back, like the production
    // update paths do), so its tick is strictly later. Value "b".
    let mut hlc_b = HLC::new();
    let seen = read::read_rows_json_pruned(kernel, "shared", "main", &kc, None, &[]).unwrap();
    for (_, row) in &seen {
        if let Some(v) = row.get("_version").and_then(|v| v.as_str()) {
            hlc_b.observe(v);
        }
    }
    let rows_b = vec![json!({"_rowid": "shared-1", "k": 1, "who": "b"})];
    shard::upsert_shard(kernel, "shared", "main", "w_b", &rows_b, Some("k"), &mut hlc_b).unwrap();

    // Exactly ONE row; B's copy wins (strictly later version).
    let live = read::read_rows_json_pruned(kernel, "shared", "main", &kc, None, &[]).unwrap();
    assert_eq!(live.len(), 1, "two upserts of the same rowid must collapse to one row");
    assert_eq!(live[0].1.get("who").and_then(|v| v.as_str()), Some("b"),
        "the strictly-later version wins (LWW)");

    // A fresh reader agrees.
    let fresh = UnifiedStorage::new_local(dir.path()).unwrap();
    let live2 = read::read_rows_json_pruned(fresh.kernel(), "shared", "main", &kc, None, &[])
        .unwrap();
    assert_eq!(live2.len(), 1);
    assert_eq!(live2[0].1.get("who").and_then(|v| v.as_str()), Some("b"));
}
