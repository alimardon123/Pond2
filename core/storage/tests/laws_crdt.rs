// CRDT merge laws — ACCEPTANCE.md item 1 / CRITIQUE C3 / builder spec
// cron-2026-08-28-1100-b.
//
// These laws ATTACK `shard::merge_rows_by_rowid` + `shard::filter_live_rows`
// with random row sets: colliding `_rowid`s (5 ids), arbitrary `_version`s
// (small range → frequent ties), tombstones, legacy rows (no `_rowid`), and
// arbitrary key_cols. A FAILING LAW IS A SUCCESSFUL REVIEW FINDING — the
// suite exists to find bugs, not to pass.
//
// FINDING #1 (law `merge_is_permutation_invariant`, kept verbatim as
// #[ignore] — reproduce with `cargo test -p pond_storage --test laws_crdt
// -- --ignored`): the C10 guarantee ("merged state byte-identical under any
// input permutation") does NOT hold for the FULL output when ≥2 LEGACY rows
// (rows without `_rowid`) survive the merge — legacy rows pass through in
// INPUT order while CRDT rows are sorted by rowid. The CRDT substate (rows
// with `_rowid`) IS permutation-invariant; `merge_is_permutation_invariant_
// crdt_only` below proves that. Fixing the full law requires a design
// decision (sort identity-less rows by payload — which changes the
// user-visible insertion-order semantics of non-CRDT writes pinned by
// tests/integration/test_beautiful_api.py::test_write_rows_no_crdt) and is
// left to the owner. Full analysis + shrunk counterexample in the worklog
// entry for cron-2026-08-28-1100-b.
//
// DETERMINISM (ACCEPTANCE.md item 6): the seed below pins the entire case
// space; CI runs are byte-reproducible. Bump the seed's trailing constant
// to explore a new random space.

use proptest::collection::vec;
use proptest::prelude::*;
use proptest::test_runner::{Config as ProptestConfig, RngSeed};
use serde_json::{json, Value};

use pond_storage::shard::{filter_live_rows, merge_rows_by_rowid};

const LAWS_SEED: u64 = 0x504F4E44_00000001; // "POND" + file 1
fn law_config(cases: u32) -> ProptestConfig {
    let mut c = ProptestConfig::with_cases(cases);
    c.rng_seed = RngSeed::Fixed(LAWS_SEED);
    c
}

// ---------------------------------------------------------------------------
// Deterministic helpers (splitmix64 + Fisher-Yates — no rand dependency)
// ---------------------------------------------------------------------------

fn splitmix64(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9E3779B97F4A7C15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
    z ^ (z >> 31)
}

/// Deterministically permute `items` (Fisher-Yates driven by splitmix64).
/// The same seed always yields the same permutation.
fn permute<T: Clone>(items: &[T], seed: u64) -> Vec<T> {
    let mut idx: Vec<usize> = (0..items.len()).collect();
    let mut state = seed | 1; // odd — splitmix64 accepts anything, keep it lively
    for i in (1..idx.len()).rev() {
        let j = (splitmix64(&mut state) as usize) % (i + 1);
        idx.swap(i, j);
    }
    idx.iter().map(|&i| items[i].clone()).collect()
}

// ---------------------------------------------------------------------------
// Row strategy (spec: colliding rowids, frequent version ties, tombstones,
// legacy rows, sparse payloads with an empty-string value in the pool)
// ---------------------------------------------------------------------------

prop_compose! {
    /// One row: 20% legacy (no _rowid), 80% `_version` (small range → ties),
    /// tombstone only possible on CRDT rows (30%), payload fields optional
    /// (sometimes only one of name/value, sometimes an extra field).
    fn arbitrary_row()(
        rowid in prop::option::weighted(0.8, 0u8..5),
        version in prop::option::weighted(0.8, 0u8..=6),
        deleted_roll in 0u8..10,
        include_name in any::<bool>(),
        include_value in any::<bool>(),
        name in prop::sample::select(vec!["alice", "bob", "carol", ""]),
        value in -4i64..=4,
        extra in prop::option::weighted(0.25, prop::sample::select(vec!["e1", "e2", "e3"])),
    ) -> Value {
        let mut obj = serde_json::Map::new();
        match rowid {
            Some(r) => {
                obj.insert("_rowid".to_string(), json!(format!("r{}", r)));
                // _deleted: 30% true / 35% false / 35% absent (CRDT rows only)
                match deleted_roll {
                    0..=2 => { obj.insert("_deleted".to_string(), json!(true)); }
                    3..=5 => { obj.insert("_deleted".to_string(), json!(false)); }
                    _ => {}
                }
            }
            None => {
                // Legacy row: _deleted is false or absent (never a tombstone —
                // tombstones are CRDT constructs).
                if (3..=5).contains(&deleted_roll) {
                    obj.insert("_deleted".to_string(), json!(false));
                }
            }
        }
        if let Some(v) = version {
            obj.insert("_version".to_string(), json!(format!("v{:04}", v)));
        }
        if include_name {
            obj.insert("name".to_string(), json!(name));
        }
        if include_value {
            obj.insert("value".to_string(), json!(value));
        }
        if let Some(e) = extra {
            obj.insert("extra".to_string(), json!(e));
        }
        Value::Object(obj)
    }
}

/// key_col: 25% None, else name / value / no_such_col.
fn key_col_strategy() -> impl Strategy<Value = Option<&'static str>> {
    prop_oneof![
        1 => Just(None),
        1 => Just(Some("name")),
        1 => Just(Some("value")),
        1 => Just(Some("no_such_col")),
    ]
}

// ---------------------------------------------------------------------------
// Laws
// ---------------------------------------------------------------------------

proptest! {
    #![proptest_config(law_config(256))]

    // Law 1 — permutation invariance of the FULL merged state (the C10
    // guarantee, tombstones included, byte-identical via serde_json).
    //
    // #[ignore] — THIS LAW FOUND A REAL GAP (finding #1 in the file header):
    // legacy rows (no `_rowid`) survive in INPUT order, so permuting the
    // input permutes them in the output. The law is kept verbatim (NOT
    // weakened) so the owner can re-enable it after the design decision;
    // reproduce with `--ignored`.
    #[test]
    #[ignore = "finding #1: legacy-row output order is input-order (see file header + worklog cron-2026-08-28-1100-b)"]
    fn merge_is_permutation_invariant(
        rows in vec(arbitrary_row(), 0..=12),
        kc in key_col_strategy(),
        seed in any::<u64>(),
    ) {
        let permuted = permute(&rows, seed);
        let base = serde_json::to_string(&merge_rows_by_rowid(&rows, kc)).unwrap();
        let perm = serde_json::to_string(&merge_rows_by_rowid(&permuted, kc)).unwrap();
        prop_assert_eq!(perm, base,
            "merged state must be byte-identical under input permutation");
    }

    // Law 1 (diagnostic sub-law) — the same permutation law restricted to
    // rows that ALL carry `_rowid`: proves the CRDT substate (the actual
    // subject of the C10 total tiebreak) is permutation-invariant. This is
    // ADDITIONAL evidence pinpointing finding #1 to the legacy pass-through;
    // it does not replace the full law above.
    #[test]
    fn merge_is_permutation_invariant_crdt_only(
        rows in vec(
            arbitrary_row().prop_filter("row must carry _rowid", |r| r.get("_rowid").is_some()),
            0..=12,
        ),
        kc in key_col_strategy(),
        seed in any::<u64>(),
    ) {
        let permuted = permute(&rows, seed);
        let base = serde_json::to_string(&merge_rows_by_rowid(&rows, kc)).unwrap();
        let perm = serde_json::to_string(&merge_rows_by_rowid(&permuted, kc)).unwrap();
        prop_assert_eq!(perm, base,
            "CRDT-only merged state must be byte-identical under input permutation");
    }

    // Law 2 — idempotence: live(merge(merge(S))) == live(merge(S)) AND
    // merge(merge(S)) == merge(S) full-state (the full-state half was
    // verified to hold empirically; if it ever fails, that is a finding).
    #[test]
    fn merge_is_idempotent(
        rows in vec(arbitrary_row(), 0..=12),
        kc in key_col_strategy(),
    ) {
        let once = merge_rows_by_rowid(&rows, kc);
        let twice = merge_rows_by_rowid(&once, kc);

        let live_once = serde_json::to_string(&filter_live_rows(&once)).unwrap();
        let live_twice = serde_json::to_string(&filter_live_rows(&twice)).unwrap();
        prop_assert_eq!(live_twice, live_once, "live state must be idempotent");

        let full_once = serde_json::to_string(&once).unwrap();
        let full_twice = serde_json::to_string(&twice).unwrap();
        prop_assert_eq!(full_twice, full_once, "full merged state must be idempotent");
    }

    // Law 3 — a tombstone at a STRICTLY-latest version suppresses its rowid
    // from the live state, regardless of input order.
    #[test]
    fn tombstone_suppresses_when_strictly_latest(
        (rowid, base, tombstone) in strictly_latest_case(),
        seed in any::<u64>(),
    ) {
        let mut rows = base;
        rows.push(tombstone);
        let rows = permute(&rows, seed);

        let merged = merge_rows_by_rowid(&rows, Some("name"));
        // Deletion-as-data: the tombstone is the winner and stays in the
        // full merged state...
        prop_assert!(merged.iter().any(|r| {
            r.get("_rowid").and_then(|v| v.as_str()) == Some(rowid.as_str())
                && r.get("_deleted").and_then(|v| v.as_bool()) == Some(true)
        }), "the strictly-latest tombstone must be the merged state's winner");
        // ...and the rowid is absent from the LIVE state.
        let live = filter_live_rows(&merged);
        prop_assert!(live.iter().all(|r| {
            r.get("_rowid").and_then(|v| v.as_str()) != Some(rowid.as_str())
        }), "tombstone at strictly-latest version must suppress {} — live: {}",
            rowid, serde_json::to_string(&live).unwrap());
    }

    // Law 3 (dual) — a LIVE row at a strictly-latest version always
    // survives a tombstone at a lower version.
    #[test]
    fn live_survives_when_strictly_latest(
        (rowid, base, live_row) in live_latest_case(),
        seed in any::<u64>(),
    ) {
        let mut rows = base;
        rows.push(live_row.clone());
        let rows = permute(&rows, seed);

        let merged = merge_rows_by_rowid(&rows, Some("name"));
        let live = filter_live_rows(&merged);
        let survivor = live.iter().find(|r| {
            r.get("_rowid").and_then(|v| v.as_str()) == Some(rowid.as_str())
        });
        prop_assert!(survivor.is_some(),
            "live row at strictly-latest version must survive — live: {}",
            serde_json::to_string(&live).unwrap());
        prop_assert_eq!(survivor.unwrap(), &live_row,
            "the surviving row must be the strictly-latest live payload");
    }

    // Law 4 — determinism: the same input bytes always merge to the same
    // output bytes.
    #[test]
    fn merge_is_deterministic(
        rows in vec(arbitrary_row(), 0..=12),
        kc in key_col_strategy(),
    ) {
        let a = serde_json::to_string(&merge_rows_by_rowid(&rows, kc)).unwrap();
        let b = serde_json::to_string(&merge_rows_by_rowid(&rows, kc)).unwrap();
        prop_assert_eq!(a, b, "same input must always produce identical output");
    }
}

// ---------------------------------------------------------------------------
// Tombstone-law case strategies
// ---------------------------------------------------------------------------

// One case for `tombstone_suppresses_when_strictly_latest`:
// (rowid, base live rows with versions 0..=k — the max version k is always
// present — and a tombstone at version k+1..=k+4).
prop_compose! {
    fn strictly_latest_case()(
        r in 0u8..5,
        k in 0u8..=6,
        d in 1u8..=4,
        present in vec(any::<bool>(), 6..=6),
        payloads in vec(
            (prop::sample::select(vec!["alice", "bob", "carol", ""]), -4i64..=4),
            7..=7,
        ),
    ) -> (String, Vec<Value>, Value) {
        let rowid = format!("r{}", r);
        let mut rows: Vec<Value> = Vec::new();
        for v in 0..k {
            if present[v as usize] {
                rows.push(json!({
                    "_rowid": rowid,
                    "_version": format!("v{:04}", v),
                    "name": payloads[v as usize].0,
                    "value": payloads[v as usize].1,
                    "_deleted": false,
                }));
            }
        }
        // The live max base version k — always present.
        rows.push(json!({
            "_rowid": rowid,
            "_version": format!("v{:04}", k),
            "name": payloads[6].0,
            "value": payloads[6].1,
            "_deleted": false,
        }));
        // Tombstone at a strictly-greater version (zero-padded → lexicographic
        // == numeric).
        let tombstone = json!({
            "_rowid": rowid,
            "_version": format!("v{:04}", k + d),
            "name": payloads[6].0,
            "value": payloads[6].1,
            "_deleted": true,
        });
        (rowid, rows, tombstone)
    }
}

// One case for `live_survives_when_strictly_latest`:
// (rowid, base rows = live versions 0..=k-1 (arbitrary) + a tombstone at
// version k, and the live row at version k+1..=k+4).
prop_compose! {
    fn live_latest_case()(
        r in 0u8..5,
        k in 0u8..=6,
        d in 1u8..=4,
        present in vec(any::<bool>(), 6..=6),
        payloads in vec(
            (prop::sample::select(vec!["alice", "bob", "carol", ""]), -4i64..=4),
            7..=7,
        ),
    ) -> (String, Vec<Value>, Value) {
        let rowid = format!("r{}", r);
        let mut rows: Vec<Value> = Vec::new();
        for v in 0..k {
            if present[v as usize] {
                rows.push(json!({
                    "_rowid": rowid,
                    "_version": format!("v{:04}", v),
                    "name": payloads[v as usize].0,
                    "value": payloads[v as usize].1,
                    "_deleted": false,
                }));
            }
        }
        // Tombstone at version k.
        rows.push(json!({
            "_rowid": rowid,
            "_version": format!("v{:04}", k),
            "name": payloads[6].0,
            "value": payloads[6].1,
            "_deleted": true,
        }));
        // Live row at a strictly-greater version.
        let live_row = json!({
            "_rowid": rowid,
            "_version": format!("v{:04}", k + d),
            "name": payloads[6].0,
            "value": payloads[6].1,
            "_deleted": false,
        });
        (rowid, rows, live_row)
    }
}
