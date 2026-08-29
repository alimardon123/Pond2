// Chaos tests — adversarial concurrency + CRDT correctness under pressure.
//
// These tests exercise the storage layer's concurrency guarantees:
//   - Concurrent branch writes + merge order independence
//   - Concurrent writes to the SAME branch (CRDT shard append-only)
//   - Delete-then-merge: tombstones must not be resurrected
//   - Cascading merge: result must be independent of merge order
//   - HLC clock-skew tolerance
//
// All randomness is DETERMINISTIC — uses a local xorshift64 PRNG seeded
// with a fixed constant. No external `rand` crate dependency.
//
// Run with: cargo test -p pond_storage --test chaos_test -- --nocapture

use pond_kernel::crdt::HLC;
use pond_kernel::PondKernel;
use pond_storage::branch;
use pond_storage::shard;
use pond_storage::write;
use pond_storage::UnifiedStorage;
use serde_json::{json, Value};

// ---------------------------------------------------------------------------
// Deterministic PRNG — xorshift64. No external dependencies.
// ---------------------------------------------------------------------------

struct Rng {
    state: u64,
}

impl Rng {
    /// Create a new PRNG. Seed must be non-zero.
    fn new(seed: u64) -> Self {
        Self {
            state: if seed == 0 { 0xDEADBEEFCAFEBABE } else { seed },
        }
    }

    /// Next pseudo-random u64.
    fn next_u64(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.state = x;
        x
    }

    /// Next pseudo-random usize in [0, max).
    fn next_usize(&mut self, max: usize) -> usize {
        if max == 0 {
            return 0;
        }
        (self.next_u64() as usize) % max
    }

    /// Fisher-Yates shuffle (deterministic).
    fn shuffle<T>(&mut self, slice: &mut [T]) {
        for i in (1..slice.len()).rev() {
            let j = self.next_usize(i + 1);
            slice.swap(i, j);
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Read all live rows from a branch (CRDT-merged, tombstones filtered).
///
/// JOURNAL ERA (D3): the production read path — the journal-aware pruned
/// reader (snapshot ∪ live entry packs) — UNIONED with the legacy shard
/// layer, mirroring the pyo3 read path exactly. The old helper read raw
/// HEAD blobs as JSON arrays directly; it cannot see journal entries or
/// columnar packs (which is where merged/folded rows now live), so every
/// merge-order test would see 0 rows.
fn read_live_rows(kernel: &PondKernel, collection: &str, branch: &str) -> Vec<Value> {
    // 1. Journal-aware pruned read (snapshot + live entries, merged).
    let mut all_rows: Vec<Value> =
        pond_storage::read::read_rows_json_pruned(kernel, collection, branch, &[], None, &[])
            .unwrap_or_default()
            .into_iter()
            .map(|(_, row)| row)
            .collect();

    // 2. Legacy shard union (the CRDT layer the python lenses still write).
    let (_, shards) = shard::read_with_shards(kernel, collection, branch).unwrap();
    for (_, shard_hash) in shards {
        if let Ok(data) = kernel.read_blob(&shard_hash) {
            if let Ok(arr) = serde_json::from_slice::<Vec<Value>>(&data) {
                all_rows.extend(arr);
            }
        }
    }

    let merged = shard::merge_rows_by_rowid(&all_rows, Some("id"));
    shard::filter_live_rows(&merged)
}

/// Write a raw CRDT shard with manually-constructed rows (full control over
/// _rowid, _version, _deleted). Used to set up precise conflict scenarios.
fn write_raw_shard(
    kernel: &PondKernel,
    collection: &str,
    branch: &str,
    shard_name: &str,
    rows: Vec<Value>,
) -> String {
    let data = serde_json::to_vec(&rows).expect("serialize shard rows");
    shard::append_shard(kernel, collection, branch, shard_name, &data)
        .expect("write raw shard")
}

/// Build a 32-char HLC hex string from (physical_ms, logical).
fn hlc_value(physical: u64, logical: u64) -> String {
    format!("{:016x}{:016x}", physical, logical)
}

/// Initialize a collection with a placeholder HEAD commit so branches can be created.
fn init_collection(storage: &UnifiedStorage, collection: &str) {
    let kernel = storage.kernel();
    write::write(kernel, collection, "main", b"init", "initial commit")
        .expect("initial write");
}

// ---------------------------------------------------------------------------
// Test 1: Concurrent branch + merge consistency
//
// 5 threads each write unique rows to their own branch using HLCs advanced
// to different starting points (simulating clock skew). After all threads
// finish, branches are merged into `main` in a RANDOM order (deterministic
// via seeded PRNG). The final state must contain ALL rows from ALL branches
// — no data loss, regardless of merge order.
// ---------------------------------------------------------------------------

#[test]
fn test_concurrent_branch_merge_consistency() {
    let dir = tempfile::tempdir().unwrap();
    let storage = UnifiedStorage::new_local(dir.path()).unwrap();
    let kernel = storage.kernel();

    let collection = "chaos_branch_merge";
    init_collection(&storage, collection);

    // Create 5 branches from main
    let branch_names: Vec<String> = (0..5).map(|i| format!("b{}", i)).collect();
    for bn in &branch_names {
        branch::branch(kernel, collection, bn, "main").unwrap();
    }

    // Each thread gets a "skewed" HLC — advanced to a different physical time
    // via observe(). This simulates clock skew between distributed nodes.
    let rows_per_branch = 200;
    let total_expected = branch_names.len() * rows_per_branch;

    std::thread::scope(|s| {
        for (i, bn) in branch_names.iter().enumerate() {
            let kernel = &*kernel;
            let bn = bn.clone();
            s.spawn(move || {
                // Skew: each thread's HLC starts at a different "physical time"
                // (i * 10_000_000 ms apart — well beyond real-time differences)
                let skew_base = 1_700_000_000_000u64 + (i as u64) * 10_000_000;
                let mut hlc = HLC::new();
                hlc.observe(&hlc_value(skew_base, 0));

                let bn_ref = bn.clone();
                for j in 0..rows_per_branch {
                    let rowid = format!("branch-{}-row-{}", i, j);
                    let rows = vec![json!({
                        "_rowid": rowid,
                        "id": rowid,
                        "branch": i,
                        "seq": j,
                        "value": format!("data-{}-{}", i, j),
                    })];
                    let shard_name = format!("s_{}_{}", i, j);
                    shard::upsert_shard(
                        kernel,
                        collection,
                        &bn_ref,
                        &shard_name,
                        &rows,
                        Some("id"),
                        &mut hlc,
                    ).unwrap();
                }
            });
        }
    });

    // Merge branches into main in RANDOM order (deterministic seed)
    let mut merge_order: Vec<usize> = (0..branch_names.len()).collect();
    let mut rng = Rng::new(42);
    rng.shuffle(&mut merge_order);

    for &idx in &merge_order {
        let bn = &branch_names[idx];
        branch::merge(kernel, collection, bn, "main", &format!("merge {}", bn))
            .unwrap();
    }

    // Verify: all rows from all branches survive the merge
    let live_rows = read_live_rows(kernel, collection, "main");
    assert_eq!(
        live_rows.len(),
        total_expected,
        "expected {} rows after merging all branches, got {}",
        total_expected,
        live_rows.len()
    );

    // Verify: no duplicate _rowids
    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
    for row in &live_rows {
        let rowid = row.get("_rowid").and_then(|v| v.as_str()).unwrap_or("");
        assert!(seen.insert(rowid.to_string()), "duplicate _rowid after merge: {}", rowid);
    }
}

// ---------------------------------------------------------------------------
// Test 2: Concurrent writes to the SAME branch — no corruption
//
// 10 threads each write 1000 unique rows (unique _rowids) to the SAME branch
// via upsert_shard. Since shards are append-only (no HEAD rewrite), this
// should be safe. After all threads finish, all 10000 rows must survive.
// ---------------------------------------------------------------------------

#[test]
fn test_concurrent_writes_no_corruption() {
    let dir = tempfile::tempdir().unwrap();
    let storage = UnifiedStorage::new_local(dir.path()).unwrap();
    let kernel = storage.kernel();

    let collection = "chaos_concurrent_writes";
    init_collection(&storage, collection);

    let n_threads = 10;
    let rows_per_thread = 1000;
    let total_expected = n_threads * rows_per_thread;

    std::thread::scope(|s| {
        for t in 0..n_threads {
            let kernel = &*kernel;
            s.spawn(move || {
                let mut hlc = HLC::new();
                for j in 0..rows_per_thread {
                    let rowid = format!("t{}-r{}", t, j);
                    let rows = vec![json!({
                        "_rowid": rowid,
                        "id": rowid,
                        "thread": t,
                        "seq": j,
                    })];
                    let shard_name = format!("t{}_s{}", t, j);
                    shard::upsert_shard(
                        kernel,
                        collection,
                        "main",
                        &shard_name,
                        &rows,
                        Some("id"),
                        &mut hlc,
                    ).unwrap();
                }
            });
        }
    });

    // Verify all 10000 rows survive
    let live_rows = read_live_rows(kernel, collection, "main");
    assert_eq!(
        live_rows.len(),
        total_expected,
        "expected {} rows after concurrent writes, got {} — DATA LOSS!",
        total_expected,
        live_rows.len()
    );

    // Verify no duplicates
    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
    for row in &live_rows {
        let rowid = row.get("_rowid").and_then(|v| v.as_str()).unwrap_or("");
        assert!(seen.insert(rowid.to_string()), "duplicate _rowid: {}", rowid);
    }
    assert_eq!(seen.len(), total_expected);
}

// ---------------------------------------------------------------------------
// Test 3: Delete-then-merge — no resurrection
//
// A tombstone (v5) on one branch and a live row (v3) on another, for the
// SAME _rowid. Merge in BOTH orders:
//   - live → tombstone branch
//   - tombstone → live branch
// In both cases, the tombstone (v5 > v3) must win — the row must NOT be
// resurrected.
// ---------------------------------------------------------------------------

#[test]
fn test_delete_then_merge_no_resurrection() {
    let rowid = "test-rowid-delete-resurrect-001";

    // ---- Order 1: merge live → tombstone branch ----
    {
        let dir = tempfile::tempdir().unwrap();
        let storage = UnifiedStorage::new_local(dir.path()).unwrap();
        let kernel = storage.kernel();

        let collection = "chaos_delete_1";
        init_collection(&storage, collection);
        branch::branch(kernel, collection, "live_branch", "main").unwrap();
        branch::branch(kernel, collection, "tomb_branch", "main").unwrap();

        // Live row at v3 (physical=3, logical=1)
        let live_row = json!({
            "_rowid": rowid,
            "_version": hlc_value(3, 1),
            "_deleted": false,
            "id": rowid,
            "name": "alive",
        });
        write_raw_shard(kernel, collection, "live_branch", "s_live", vec![live_row]);

        // Tombstone at v5 (physical=5, logical=1) — LATER than the live row
        let tombstone = json!({
            "_rowid": rowid,
            "_version": hlc_value(5, 1),
            "_deleted": true,
            "id": rowid,
        });
        write_raw_shard(kernel, collection, "tomb_branch", "s_tomb", vec![tombstone]);

        // Merge live_branch → tomb_branch
        branch::merge(kernel, collection, "live_branch", "tomb_branch", "merge live into tomb")
            .unwrap();

        let live_rows = read_live_rows(kernel, collection, "tomb_branch");
        assert!(
            live_rows.is_empty(),
            "tombstone (v5) must win over live (v3) after merge live→tomb: got {} rows",
            live_rows.len()
        );
    }

    // ---- Order 2: merge tombstone → live branch ----
    {
        let dir = tempfile::tempdir().unwrap();
        let storage = UnifiedStorage::new_local(dir.path()).unwrap();
        let kernel = storage.kernel();

        let collection = "chaos_delete_2";
        init_collection(&storage, collection);
        branch::branch(kernel, collection, "live_branch", "main").unwrap();
        branch::branch(kernel, collection, "tomb_branch", "main").unwrap();

        let live_row = json!({
            "_rowid": rowid,
            "_version": hlc_value(3, 1),
            "_deleted": false,
            "id": rowid,
            "name": "alive",
        });
        write_raw_shard(kernel, collection, "live_branch", "s_live", vec![live_row]);

        let tombstone = json!({
            "_rowid": rowid,
            "_version": hlc_value(5, 1),
            "_deleted": true,
            "id": rowid,
        });
        write_raw_shard(kernel, collection, "tomb_branch", "s_tomb", vec![tombstone]);

        // Merge tomb_branch → live_branch
        branch::merge(kernel, collection, "tomb_branch", "live_branch", "merge tomb into live")
            .unwrap();

        let live_rows = read_live_rows(kernel, collection, "live_branch");
        assert!(
            live_rows.is_empty(),
            "tombstone (v5) must win over live (v3) after merge tomb→live: got {} rows — RESURRECTION!",
            live_rows.len()
        );
    }
}

// ---------------------------------------------------------------------------
// Test 4: Cascading merge order independence
//
// 4 branches, each with the SAME _rowid but different _version (v1, v2, v3, v4)
// and different `value` fields. Merge in ascending order (v1→v2→v3→v4→main)
// vs descending order (v4→v3→v2→v1→main). The final state must be IDENTICAL
// — the row with v4 (highest version) wins, regardless of merge order.
// ---------------------------------------------------------------------------

#[test]
fn test_cascading_merge_order_independence() {
    let rowid = "test-rowid-cascade-001";
    let versions: Vec<(u64, u64, &str)> = vec![
        (1, 1, "v1_data"),
        (2, 1, "v2_data"),
        (3, 1, "v3_data"),
        (4, 1, "v4_data"),
    ];

    // Run both merge orders and compare results
    let run_with_order = |order: &[usize]| -> (String, Value) {
        let dir = tempfile::tempdir().unwrap();
        let storage = UnifiedStorage::new_local(dir.path()).unwrap();
        let kernel = storage.kernel();

        let collection = "chaos_cascade";
        init_collection(&storage, collection);

        // Create 4 branches with versioned rows.
        // Each branch uses a UNIQUE shard name so that merge doesn't overwrite
        // (merge copies shards by name — same name would clobber).
        for (i, (phys, logical, value)) in versions.iter().enumerate() {
            let bn = format!("b{}", i);
            branch::branch(kernel, collection, &bn, "main").unwrap();
            let row = json!({
                "_rowid": rowid,
                "_version": hlc_value(*phys, *logical),
                "_deleted": false,
                "id": rowid,
                "value": value,
                "version_tag": i,
            });
            write_raw_shard(kernel, collection, &bn, &format!("s_{}", i), vec![row]);
        }

        // Merge in the given order
        for &idx in order {
            let bn = format!("b{}", idx);
            branch::merge(kernel, collection, &bn, "main", &format!("merge {}", bn))
                .unwrap();
        }

        let live_rows = read_live_rows(kernel, collection, "main");
        assert_eq!(live_rows.len(), 1, "exactly 1 row should survive (latest version wins)");
        let row = &live_rows[0];
        let value = row.get("value").and_then(|v| v.as_str()).unwrap_or("").to_string();
        let version_tag = row.get("version_tag").and_then(|v| v.as_u64()).unwrap_or(u64::MAX);
        (value.clone(), json!({"value": value, "version_tag": version_tag}))
    };

    // Ascending: b0 → b1 → b2 → b3
    let ascending = run_with_order(&[0, 1, 2, 3]);

    // Descending: b3 → b2 → b1 → b0
    let descending = run_with_order(&[3, 2, 1, 0]);

    // Random order 1: b2 → b0 → b3 → b1
    let random1 = run_with_order(&[2, 0, 3, 1]);

    // Random order 2: b1 → b3 → b0 → b2
    let random2 = run_with_order(&[1, 3, 0, 2]);

    // All orders must produce the SAME result: v4 wins (highest version)
    assert_eq!(ascending.0, "v4_data", "ascending: v4 should win, got {}", ascending.0);
    assert_eq!(descending.0, "v4_data", "descending: v4 should win, got {}", descending.0);
    assert_eq!(random1.0, "v4_data", "random1: v4 should win, got {}", random1.0);
    assert_eq!(random2.0, "v4_data", "random2: v4 should win, got {}", random2.0);

    // Full result equality (value + version_tag)
    assert_eq!(ascending.1, descending.1, "ascending != descending");
    assert_eq!(ascending.1, random1.1, "ascending != random1");
    assert_eq!(ascending.1, random2.1, "ascending != random2");
}

// ---------------------------------------------------------------------------
// Test 5: HLC clock-skew tolerance
//
// Simulate a scenario where node 2's physical clock is behind node 1's.
// Verify that HLC::observe() handles this correctly — the local clock
// advances past the observed value, maintaining monotonicity.
// ---------------------------------------------------------------------------

#[test]
fn test_hlc_clock_skew_tolerance() {
    use std::time::{SystemTime, UNIX_EPOCH};

    // Get real-world time so we can construct values that are
    // "behind" and "ahead" of the current clock.
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(1_700_000_000_000);

    // --- Scenario A: node 2 is BEHIND node 1 ---
    // Node 1 ticks and produces a value at (now+5000, logical=0).
    // Node 2's physical clock is at (now-5000) — 10 seconds behind.
    let mut node1 = HLC::new();
    let mut node2 = HLC::new();

    // Advance node1 to (now+5000, 0)
    node1.observe(&hlc_value(now_ms + 5_000, 0));
    let v1 = node1.tick();

    // Node 2 is behind — its clock is at (now-5000, 0)
    node2.observe(&hlc_value(now_ms.saturating_sub(5_000), 0));
    let v2 = node2.tick();

    // v1 should be > v2 (node1 is ahead)
    assert!(v1 > v2, "node1 (ahead) should produce a higher HLC: {} > {}", v1, v2);

    // Node 2 receives v1 from node 1 (observe the higher clock)
    node2.observe(&v1);
    let v3 = node2.tick();

    // After observing v1, node2's next tick must be > v1
    assert!(v3 > v1, "node2 must advance past v1 after observing it: {} > {}", v3, v1);

    // --- Scenario B: node 2 is AHEAD of node 1 ---
    let mut node_a = HLC::new();
    let mut node_b = HLC::new();

    // node_a is at (now-3000, 0) — behind
    node_a.observe(&hlc_value(now_ms.saturating_sub(3_000), 0));
    let _va = node_a.tick();

    // node_b is at (now+3000, 0) — ahead
    node_b.observe(&hlc_value(now_ms + 3_000, 0));
    let vb = node_b.tick();

    // node_a observes vb (the higher clock)
    node_a.observe(&vb);
    let vc = node_a.tick();
    assert!(vc > vb, "node_a must advance past vb after observing it: {} > {}", vc, vb);

    // --- Scenario C: rapid same-physical-time observations ---
    // Two nodes observe each other within the same millisecond.
    // The logical counter must increment to break ties.
    let mut fast_node1 = HLC::new();
    let mut fast_node2 = HLC::new();

    // Both start at the same physical time
    fast_node1.observe(&hlc_value(now_ms, 0));
    fast_node2.observe(&hlc_value(now_ms, 0));

    let f1 = fast_node1.tick();
    let f2 = fast_node2.tick();

    // f1 and f2 should both be valid and at the same physical time
    // (or higher if real time advanced). The logical counters should differ.
    assert!(HLC::is_valid(&f1), "f1 should be valid HLC");
    assert!(HLC::is_valid(&f2), "f2 should be valid HLC");

    // Node 1 observes node 2's value, then ticks — must be strictly greater
    fast_node1.observe(&f2);
    let f3 = fast_node1.tick();
    assert!(f3 > f2, "after observing f2, node1's tick must be > f2: {} > {}", f3, f2);
    assert!(f3 > f1, "after observing f2, node1's tick must be > f1: {} > {}", f3, f1);

    // --- Scenario D: observe a malformed value (should be a no-op) ---
    let mut robust = HLC::new();
    let _ = robust.tick();
    robust.observe("not-a-valid-hlc");  // too short
    robust.observe("");                   // empty
    robust.observe("zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz");  // non-hex
    // Should not panic, and the clock should still work
    let v_after_garbage = robust.tick();
    assert!(HLC::is_valid(&v_after_garbage), "clock should still produce valid values after observing garbage");
}
