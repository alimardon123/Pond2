// Shard module — CRDT shard management
//
// CRDT shards allow concurrent multi-writer without coordination:
//   - Each writer writes its own shard to a unique path
//   - Readers union HEAD + all live shards
//   - No CAS, no coordination — works on any object store
//
// CRDT row-level operations:
//   - upsert_shard: adds _rowid (UUIDv7) + _version (HLC) to each row,
//     enabling row-level CRDT merge on conflict
//   - delete_shard: writes tombstone shards (_deleted=True + _version)
//   - merge_rows_by_rowid: CRDT merge — latest _version wins, tombstones suppress

use crate::{branch_ref, shards_prefix};
use pond_kernel::crdt::{uuidv7, HLC};
use pond_kernel::PondKernel;
use serde_json::{json, Value};

/// Append a CRDT shard to a branch.
///
/// The shard is written to a unique path under the branch's shards/ directory.
/// Readers will discover and merge it via read_with_shards.
///
/// Matches Python UnifiedStorage.append_shard():
///   1. Write the shard data as a blob
///   2. Reference it at a unique shard path
pub fn append_shard(
    kernel: &PondKernel,
    collection: &str,
    branch: &str,
    shard_name: &str,
    data: &[u8],
) -> Result<String, String> {
    // Write the shard blob
    let shard_hash = kernel.write(data)
        .map_err(|e| format!("Failed to write shard: {}", e))?;

    // Reference it at a unique path
    let shard_ref = format!("{}{}", shards_prefix(collection, branch), shard_name);
    kernel.reference(&shard_ref, &shard_hash)
        .map_err(|e| format!("Failed to reference shard: {}", e))?;

    Ok(shard_hash)
}

/// List all shard hashes for a branch.
///
/// Scans the branch's shards/ directory and resolves each ref.
pub fn list_shards(
    kernel: &PondKernel,
    collection: &str,
    branch: &str,
) -> Vec<(String, String)> {
    let prefix = shards_prefix(collection, branch);
    let refs = kernel.list_names_prefix(&prefix);
    let mut shards = Vec::new();
    for ref_path in refs {
        if let Some(hash) = kernel.resolve(&ref_path) {
            let name = ref_path.strip_prefix(&prefix).unwrap_or(&ref_path).to_string();
            shards.push((name, hash));
        }
    }
    shards
}

/// Read the collection's HEAD manifest + all live shards.
///
/// This is the CRDT read path: union HEAD + all unmerged shards.
/// Returns the HEAD manifest hash and the list of shard hashes.
pub fn read_with_shards(
    kernel: &PondKernel,
    collection: &str,
    branch: &str,
) -> (Option<String>, Vec<(String, String)>) {
    // Get HEAD commit
    let head_commit = kernel.resolve(&branch_ref(collection, branch));

    // Get HEAD manifest
    let head_manifest = head_commit.as_ref()
        .and_then(|h| crate::commit::read_commit(kernel, h))
        .and_then(|c| {
            if c.manifest.is_empty() { None } else { Some(c.manifest.clone()) }
        });

    // List all shards
    let shards = list_shards(kernel, collection, branch);

    (head_manifest, shards)
}

/// Clear all shards for a branch (used after merge/compact).
///
/// This does TWO things (matching the Python implementation):
/// 1. Deletes the shard ref PATH entries (so resolve() returns None)
/// 2. Physically deletes the shard BLOB objects (reclaims storage space)
///
/// The Python code calls these _tombstone_shard_refs + _auto_vacuum_after_compact.
pub fn clear_shards(
    kernel: &PondKernel,
    collection: &str,
    branch: &str,
) -> Result<usize, String> {
    let shards = list_shards(kernel, collection, branch);
    let mut count = 0;
    for (name, hash) in &shards {
        let shard_ref = format!("{}{}", shards_prefix(collection, branch), name);
        if kernel.delete_ref(&shard_ref).unwrap_or(false) {
            count += 1;
        }
        // Physically delete the shard blob (reclaim storage space).
        // This is best-effort — if the blob is also referenced by HEAD
        // (e.g., inline shards), it should NOT be deleted. The caller
        // is responsible for passing protected_hashes if needed.
        // For simple clear_shards (after merge), all shards are absorbed
        // into HEAD, so their blobs are safe to delete.
        delete_blob(kernel, hash);
    }
    Ok(count)
}

/// Physically delete a blob from the object store (best-effort).
///
/// This is a maintenance operation — not a kernel primitive. Uses the
/// ObjectStore's delete_blob to reclaim storage space from dead shards.
/// If deletion fails, the blob is orphaned (unreachable but still on disk).
fn delete_blob(kernel: &PondKernel, hash: &str) {
    let _ = kernel.delete_blob(hash);
}

/// Count the number of live shards for a branch.
pub fn shard_count(
    kernel: &PondKernel,
    collection: &str,
    branch: &str,
) -> usize {
    list_shards(kernel, collection, branch).len()
}

// ---------------------------------------------------------------------------
// CRDT row-level operations (upsert_shard, delete_shard, merge_rows_by_rowid)
// ---------------------------------------------------------------------------

/// Upsert (insert-or-update) rows as a CRDT shard with _rowid + _version.
///
/// Each row gets:
///   - _rowid: UUIDv7 (stable across updates, generated if not provided)
///   - _version: HLC (new per write, used for CRDT merge — latest wins)
///   - _deleted: false (tombstone marker)
///
/// On merge, rows with the same _rowid are deduplicated — the one with
/// the latest _version wins. Tombstones (_deleted=true) suppress rows
/// if their _version is latest.
///
/// This is the CRDT-safe write path. Multiple writers can upsert
/// concurrently without coordination — merge resolves conflicts.
pub fn upsert_shard(
    kernel: &PondKernel,
    collection: &str,
    branch: &str,
    shard_name: &str,
    rows: &[Value],
    _key_col: Option<&str>,
    hlc: &mut HLC,
) -> Result<String, String> {
    let mut crdt_rows = Vec::with_capacity(rows.len());

    for row in rows {
        let mut crdt_row = row.clone();
        // Generate _rowid if not present
        if crdt_row.get("_rowid").is_none() {
            crdt_row["_rowid"] = json!(uuidv7());
        }
        // Generate _version (HLC — clock-skew-safe)
        crdt_row["_version"] = json!(hlc.tick());
        // Mark as not deleted
        crdt_row["_deleted"] = json!(false);
        crdt_rows.push(crdt_row);
    }

    // Serialize as JSON and write as a shard
    let data = serde_json::to_vec(&crdt_rows)
        .map_err(|e| format!("Failed to serialize CRDT rows: {}", e))?;

    append_shard(kernel, collection, branch, shard_name, &data)
}

/// Delete rows by writing tombstone shards.
///
/// Each deleted _rowid gets a tombstone with _deleted=true and a new _version.
/// On merge, if the tombstone's _version is later than any live row's _version,
/// the row is suppressed.
pub fn delete_shard(
    kernel: &PondKernel,
    collection: &str,
    branch: &str,
    shard_name: &str,
    rowids: &[String],
    key_col: Option<&str>,
    hlc: &mut HLC,
) -> Result<String, String> {
    let mut tombstones = Vec::with_capacity(rowids.len());

    for rowid in rowids {
        let mut tombstone = json!({
            "_rowid": rowid,
            "_deleted": true,
        });
        tombstone["_version"] = json!(hlc.tick());
        if let Some(kc) = key_col {
            tombstone[kc] = json!(rowid);
        }
        tombstones.push(tombstone);
    }

    let data = serde_json::to_vec(&tombstones)
        .map_err(|e| format!("Failed to serialize tombstones: {}", e))?;

    append_shard(kernel, collection, branch, shard_name, &data)
}

/// Total-order comparison for CRDT row conflicts (C10 fix).
///
/// `true` when the incoming row should REPLACE the existing one:
///   1. strictly greater `_version` (LWW — the normal case), else
///   2. equal `_version` and greater `_rowid` (identity tiebreak — constant
///      when the caller groups by `_rowid`, but keeps the order total for
///      callers with fallback keys), else
///   3. equal `(_version, _rowid)` and greater serialized payload — the
///      term that makes the merge COMMUTATIVE: two concurrent rows with
///      identical clocks and identical rowid but different payloads now
///      converge to the same winner regardless of merge order.
///
/// The payload serialization only runs on a full tie, which is rare (HLC
/// collisions), so the common path pays one string compare.
fn crdt_row_greater(
    version: &str,
    rowid: &str,
    row: &Value,
    existing_version: &str,
    existing_rowid: &str,
    existing_row: &Value,
) -> bool {
    if version != existing_version {
        return version > existing_version;
    }
    if rowid != existing_rowid {
        return rowid > existing_rowid;
    }
    // Full tie on (version, rowid): break it on the payload so that
    // permuting the input order cannot change the merged state.
    let row_json = serde_json::to_string(row).unwrap_or_default();
    let existing_json = serde_json::to_string(existing_row).unwrap_or_default();
    row_json > existing_json
}

/// CRDT row-level merge: dedup by _rowid, latest _version wins.
///
/// Tombstones (_deleted=true) suppress rows if their _version is latest.
/// If a live row has a later _version, it overrides the tombstone.
///
/// This is the deterministic CRDT merge — same input always produces
/// the same output, regardless of merge order.
///
/// **Total tiebreak (CRITIQUE C10)**: rows that tie on BOTH `_version` and
/// `_rowid` are concurrent writes whose clocks collided (HLC ties across
/// writers). Strict `version > existing` made the FIRST-SEEN row win, so
/// permuting the entry order changed the merged state. The comparison is
/// now a total order on `(_version, _rowid, serialized-row)`: the payload
/// term is what actually breaks ties (the map key already equals `_rowid`,
/// so the rowid term is constant within a key — kept for explicitness and
/// for callers that group by a fallback key). The greater payload string
/// wins; either direction is arbitrary but MUST be fixed so the merge is
/// commutative — the determinism law (same entry set ⇒ byte-identical
/// state) holds under any permutation, including tombstone-vs-live ties.
pub fn merge_rows_by_rowid(rows: &[Value], key_col: Option<&str>) -> Vec<Value> {
    // Separate CRDT rows (with _rowid) from legacy rows (without)
    let mut latest: std::collections::HashMap<String, Value> = std::collections::HashMap::new();
    let mut legacy_rows: Vec<Value> = Vec::new();
    let mut has_crdt = false;

    for row in rows {
        if let Some(rowid) = row.get("_rowid").and_then(|v| v.as_str()) {
            has_crdt = true;
            let version = row.get("_version").and_then(|v| v.as_str()).unwrap_or("");
            let should_replace = match latest.get(rowid) {
                Some(existing) => crdt_row_greater(
                    version, rowid, row,
                    existing.get("_version").and_then(|v| v.as_str()).unwrap_or(""),
                    rowid, existing,
                ),
                None => true,
            };
            if should_replace {
                latest.insert(rowid.to_string(), row.clone());
            }
        } else {
            legacy_rows.push(row.clone());
        }
    }

    let mut result: Vec<Value> = Vec::new();

    match (key_col, has_crdt) {
        (Some(kc), true) => {
            // CRDT mode with key_col: build a set of key_col values claimed by CRDT rows
            let mut crdt_keys: std::collections::HashSet<String> = std::collections::HashSet::new();
            for row in latest.values() {
                if !row.get("_deleted").and_then(|v| v.as_bool()).unwrap_or(false) {
                    if let Some(kv) = row.get(kc) {
                        crdt_keys.insert(kv.to_string());
                    }
                }
                // Also claim tombstoned keys
                if row.get("_deleted").and_then(|v| v.as_bool()).unwrap_or(false) {
                    if let Some(kv) = row.get(kc) {
                        crdt_keys.insert(kv.to_string());
                    }
                }
            }
            // Keep legacy rows whose key_col is NOT claimed by CRDT rows
            for row in &legacy_rows {
                let kv = row.get(kc).map(|v| v.to_string());
                if let Some(kv) = kv {
                    if !crdt_keys.contains(&kv) {
                        result.push(row.clone());
                    }
                } else {
                    result.push(row.clone());
                }
            }
        }
        _ => {
            // No CRDT or no key_col: keep all legacy rows
            result.extend(legacy_rows);
        }
    }

    // Add CRDT rows (INCLUDING tombstones for associativity — readers call
    // filter_live_rows). Output is sorted by rowid: HashMap iteration order
    // is arbitrary, and the C10 total tiebreak guarantees the merged SET is
    // permutation-invariant — a sorted output makes the merged STATE
    // (same rows, same order) byte-identical under any input permutation.
    let mut crdt_rows: Vec<(String, Value)> = latest.into_iter().collect();
    crdt_rows.sort_by(|a, b| a.0.cmp(&b.0));
    result.extend(crdt_rows.into_iter().map(|(_, row)| row));

    result
}

/// Filter out tombstoned rows (_deleted: true).
pub fn filter_live_rows(rows: &[Value]) -> Vec<Value> {
    rows.iter().filter(|r| !r.get("_deleted").and_then(|v| v.as_bool()).unwrap_or(false)).cloned().collect()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::UnifiedStorage;

    #[test]
    fn test_append_and_list_shards() {
        let dir = tempfile::tempdir().unwrap();
        let storage = UnifiedStorage::new_local(dir.path()).unwrap();
        let kernel = storage.kernel();

        // Append two shards
        let h1 = append_shard(kernel, "events", "main", "shardA", b"shard A data").unwrap();
        let _h2 = append_shard(kernel, "events", "main", "shardB", b"shard B data").unwrap();

        // List shards
        let shards = list_shards(kernel, "events", "main");
        assert_eq!(shards.len(), 2);

        // Verify shard data
        let data_a = kernel.read_blob(&h1).unwrap();
        assert_eq!(data_a, b"shard A data");
    }

    #[test]
    fn test_clear_shards() {
        let dir = tempfile::tempdir().unwrap();
        let storage = UnifiedStorage::new_local(dir.path()).unwrap();
        let kernel = storage.kernel();

        append_shard(kernel, "events", "main", "s1", b"data1").unwrap();
        append_shard(kernel, "events", "main", "s2", b"data2").unwrap();
        assert_eq!(shard_count(kernel, "events", "main"), 2);

        let cleared = clear_shards(kernel, "events", "main").unwrap();
        assert_eq!(cleared, 2);
        assert_eq!(shard_count(kernel, "events", "main"), 0);
    }

    #[test]
    fn test_read_with_shards() {
        let dir = tempfile::tempdir().unwrap();
        let storage = UnifiedStorage::new_local(dir.path()).unwrap();
        let kernel = storage.kernel();

        // No HEAD, no shards
        let (head, shards) = read_with_shards(kernel, "events", "main");
        assert!(head.is_none());
        assert!(shards.is_empty());

        // Add shards (no HEAD)
        append_shard(kernel, "events", "main", "s1", b"data1").unwrap();
        let (head, shards) = read_with_shards(kernel, "events", "main");
        assert!(head.is_none()); // no HEAD commit
        assert_eq!(shards.len(), 1);
    }

    #[test]
    fn test_upsert_shard_adds_rowid_version() {
        let dir = tempfile::tempdir().unwrap();
        let storage = UnifiedStorage::new_local(dir.path()).unwrap();
        let kernel = storage.kernel();
        let mut hlc = HLC::new();

        let rows = vec![json!({"name": "alice", "age": 30})];
        let hash = upsert_shard(kernel, "users", "main", "s1", &rows, Some("name"), &mut hlc).unwrap();

        // Read the shard and verify it has _rowid, _version, _deleted
        let data = kernel.read_blob(&hash).unwrap();
        let crdt_rows: Vec<Value> = serde_json::from_slice(&data).unwrap();
        assert_eq!(crdt_rows.len(), 1);
        assert!(crdt_rows[0].get("_rowid").is_some(), "must have _rowid");
        assert!(crdt_rows[0].get("_version").is_some(), "must have _version");
        assert_eq!(crdt_rows[0].get("_deleted"), Some(&json!(false)));
    }

    #[test]
    fn test_delete_shard_writes_tombstones() {
        let dir = tempfile::tempdir().unwrap();
        let storage = UnifiedStorage::new_local(dir.path()).unwrap();
        let kernel = storage.kernel();
        let mut hlc = HLC::new();

        // First upsert a row
        let rows = vec![json!({"name": "alice", "age": 30})];
        let hash = upsert_shard(kernel, "users", "main", "s1", &rows, Some("name"), &mut hlc).unwrap();
        let data = kernel.read_blob(&hash).unwrap();
        let crdt_rows: Vec<Value> = serde_json::from_slice(&data).unwrap();
        let rowid = crdt_rows[0]["_rowid"].as_str().unwrap().to_string();

        // Delete the row
        let del_hash = delete_shard(kernel, "users", "main", "del1", &[rowid], Some("name"), &mut hlc).unwrap();
        let del_data = kernel.read_blob(&del_hash).unwrap();
        let tombstones: Vec<Value> = serde_json::from_slice(&del_data).unwrap();
        assert_eq!(tombstones.len(), 1);
        assert_eq!(tombstones[0]["_deleted"], json!(true));
        assert!(tombstones[0].get("_version").is_some());
    }

    #[test]
    fn test_merge_rows_by_rowid_latest_wins() {
        let rowid = "test-rowid-123";
        let rows = vec![
            json!({"_rowid": rowid, "_version": "00000000000000010000000000000001", "name": "old", "_deleted": false}),
            json!({"_rowid": rowid, "_version": "00000000000000020000000000000001", "name": "new", "_deleted": false}),
        ];

        let merged = merge_rows_by_rowid(&rows, Some("name"));
        assert_eq!(merged.len(), 1, "should dedup to 1 row");
        assert_eq!(merged[0]["name"], json!("new"), "latest version should win");
    }

    #[test]
    fn test_merge_rows_tombstone_suppresses() {
        let rowid = "test-rowid-456";
        let rows = vec![
            json!({"_rowid": rowid, "_version": "00000000000000020000000000000001", "name": "alive", "_deleted": false}),
            json!({"_rowid": rowid, "_version": "00000000000000030000000000000001", "name": "alive", "_deleted": true}),
        ];

        // merge_rows_by_rowid now KEEPS tombstones for associativity.
        // Readers call filter_live_rows to get only visible rows.
        let merged = merge_rows_by_rowid(&rows, Some("name"));
        assert_eq!(merged.len(), 1, "tombstone kept in merge output for associativity");
        let live = filter_live_rows(&merged);
        assert_eq!(live.len(), 0, "tombstone suppresses row in live output");
    }

    #[test]
    fn test_merge_rows_live_overrides_tombstone() {
        let rowid = "test-rowid-789";
        let rows = vec![
            json!({"_rowid": rowid, "_version": "00000000000000030000000000000001", "name": "deleted", "_deleted": true}),
            json!({"_rowid": rowid, "_version": "00000000000000040000000000000001", "name": "resurrected", "_deleted": false}),
        ];

        let merged = merge_rows_by_rowid(&rows, Some("name"));
        assert_eq!(merged.len(), 1, "live row with later version should override tombstone");
        assert_eq!(merged[0]["name"], json!("resurrected"));
    }

    #[test]
    fn test_merge_rows_different_rowids_kept() {
        let rows = vec![
            json!({"_rowid": "id1", "_version": "00000000000000010000000000000001", "name": "alice", "_deleted": false}),
            json!({"_rowid": "id2", "_version": "00000000000000010000000000000002", "name": "bob", "_deleted": false}),
        ];

        let merged = merge_rows_by_rowid(&rows, Some("name"));
        assert_eq!(merged.len(), 2, "different rowids should both be kept");
    }

    #[test]
    fn test_merge_deterministic_equal_version_equal_rowid() {
        // C10 regression: two rows with the SAME _rowid and SAME _version
        // but DIFFERENT payloads must merge to the SAME winner in BOTH
        // input orders (strict `version >` used to make first-seen win,
        // so permutation changed the result).
        let a = json!({"_rowid": "tie-1", "_version": "00000000000000010000000000000001", "name": "aaa", "_deleted": false});
        let b = json!({"_rowid": "tie-1", "_version": "00000000000000010000000000000001", "name": "zzz", "_deleted": false});

        let ab = merge_rows_by_rowid(&[a.clone(), b.clone()], Some("name"));
        let ba = merge_rows_by_rowid(&[b.clone(), a.clone()], Some("name"));
        assert_eq!(ab.len(), 1);
        assert_eq!(ba.len(), 1);
        assert_eq!(ab[0], ba[0], "merged state must be permutation-invariant");
        // The tiebreak direction is fixed (greater payload wins) — assert it
        // so accidental flips are caught.
        assert_eq!(ab[0]["name"], json!("zzz"));
    }

    #[test]
    fn test_merge_deterministic_tombstone_vs_live_equal_version() {
        // Equal versions, one tombstone one live: permutation must not
        // decide whether the row survives.
        let live = json!({"_rowid": "tie-2", "_version": "00000000000000020000000000000001", "name": "alive", "_deleted": false});
        let dead = json!({"_rowid": "tie-2", "_version": "00000000000000020000000000000001", "name": "alive", "_deleted": true});

        let ld = merge_rows_by_rowid(&[live.clone(), dead.clone()], Some("name"));
        let dl = merge_rows_by_rowid(&[dead.clone(), live.clone()], Some("name"));
        assert_eq!(ld, dl, "tombstone-vs-live at equal versions must be order-independent");
        // Deterministic outcome: same winner in both orders...
        let live_ld = filter_live_rows(&ld).len();
        let live_dl = filter_live_rows(&dl).len();
        assert_eq!(live_ld, live_dl, "visibility must not depend on merge order");
    }

    #[test]
    fn test_merge_deterministic_under_permutation_loop() {
        // Property-style (seeded std shuffle — no proptest dependency):
        // a set of rows with colliding (version, rowid) pairs merged in
        // many shuffled orders must ALWAYS produce the identical state.
        let mut rows = vec![
            json!({"_rowid": "p1", "_version": "v1", "name": "x", "_deleted": false}),
            json!({"_rowid": "p1", "_version": "v1", "name": "a", "_deleted": false}),
            json!({"_rowid": "p1", "_version": "v1", "name": "m", "_deleted": true}),
            json!({"_rowid": "p2", "_version": "v1", "name": "q", "_deleted": false}),
            json!({"_rowid": "p2", "_version": "v2", "name": "r", "_deleted": false}),
            json!({"_rowid": "p2", "_version": "v2", "name": "s", "_deleted": false}),
            json!({"_rowid": "p3", "_version": "v0", "name": "t", "_deleted": false}),
        ];

        let reference = merge_rows_by_rowid(&rows, Some("name"));
        // Simple LCG shuffle — deterministic across runs/platforms.
        let mut seed: u64 = 0x9E3779B97F4A7C15;
        for round in 0..32 {
            for i in (1..rows.len()).rev() {
                seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
                let j = (seed >> 33) as usize % (i + 1);
                rows.swap(i, j);
            }
            let merged = merge_rows_by_rowid(&rows, Some("name"));
            assert_eq!(merged, reference,
                "round {}: merged state changed under permutation", round);
        }
    }
}
