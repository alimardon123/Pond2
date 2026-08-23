// Branch module — branch, checkout, merge
//
// FAITHFUL PORT of Python UnifiedStorage's branch/checkout/merge methods.
//
// Branch: O(1) ref copy — copies commit ref AND manifest ref from active branch
// Checkout: sets active branch in-memory (no storage mutation)
// Merge: O(conflicting) — identifies conflicting row groups, applies row-level
//        CRDT merge only on those, writes a merge commit with two parents

use crate::commit;
use crate::manifest::CollectionManifest;
use crate::{branch_ref, manifest_ref};
use pond_kernel::PondKernel;
use std::collections::HashMap;

/// Create a branch — O(1) ref copy.
/// Copies BOTH the commit ref AND the manifest ref from the active branch.
pub fn branch(
    kernel: &PondKernel,
    collection: &str,
    branch_name: &str,
    active_branch: &str,
) -> Result<String, String> {
    let source_commit = kernel.resolve(&branch_ref(collection, active_branch))
        .ok_or_else(|| format!("Collection '{}' has no commits to branch from", collection))?;

    // Copy commit ref
    kernel.reference(&branch_ref(collection, branch_name), &source_commit)
        .map_err(|e| format!("Failed to create branch ref: {}", e))?;

    // Copy manifest ref (matches Python branch())
    if let Some(source_manifest) = kernel.resolve(&manifest_ref(collection, active_branch)) {
        let _ = kernel.reference(&manifest_ref(collection, branch_name), &source_manifest);
    }

    Ok(source_commit)
}

/// Checkout a branch — verify it exists (no storage mutation, active branch
/// is tracked in-memory by UnifiedStorage).
pub fn checkout(
    kernel: &PondKernel,
    collection: &str,
    branch_name: &str,
) -> Result<(), String> {
    if kernel.resolve(&branch_ref(collection, branch_name)).is_none() {
        return Err(format!("Branch '{}' does not exist in '{}'", branch_name, collection));
    }
    Ok(())
}

/// Create a branch AND checkout (like `git checkout -b`).
pub fn checkout_new(
    kernel: &PondKernel,
    collection: &str,
    branch_name: &str,
    active_branch: &str,
) -> Result<String, String> {
    let head = branch(kernel, collection, branch_name, active_branch)?;
    checkout(kernel, collection, branch_name)?;
    Ok(head)
}

/// List all branches for a collection.
pub fn list_branches(kernel: &PondKernel, collection: &str) -> Vec<String> {
    let prefix = format!("collections/{}/_branches/", collection);
    let refs = kernel.list_names_prefix(&prefix);
    let mut branches: Vec<String> = Vec::new();
    for ref_path in refs {
        // Extract branch name from "collections/{name}/_branches/{branch}/commit"
        if let Some(rest) = ref_path.strip_prefix(&prefix) {
            if let Some(branch) = rest.split('/').next() {
                if !branches.contains(&branch.to_string()) {
                    branches.push(branch.to_string());
                }
            }
        }
    }
    branches.sort();
    branches
}

/// Merge a source branch into a target branch.
///
/// O(conflicting) merge strategy (matches the Python fix from Round 62):
///   1. Build per-source maps of rg_key → RowGroupEntry
///   2. Identify CONFLICTING rg_keys (in BOTH target and source)
///   3. Non-conflicting: keep as-is (zero decode)
///   4. Conflicting: decode only these, apply row-level CRDT, re-encode
///
/// Writes a merge commit with TWO parents (parent = target, second_parent = source).
pub fn merge(
    kernel: &PondKernel,
    collection: &str,
    source_branch: &str,
    target_branch: &str,
    message: &str,
) -> Result<String, String> {
    // Resolve both branch HEADs
    let target_head = kernel.resolve(&branch_ref(collection, target_branch))
        .ok_or_else(|| format!("Target branch '{}' not found", target_branch))?;
    let source_head = kernel.resolve(&branch_ref(collection, source_branch))
        .ok_or_else(|| format!("Source branch '{}' not found", source_branch))?;

    // Read both commits to get manifest hashes
    let target_commit = commit::read_commit(kernel, &target_head)
        .ok_or_else(|| "Failed to read target commit".to_string())?;
    let source_commit = commit::read_commit(kernel, &source_head)
        .ok_or_else(|| "Failed to read source commit".to_string())?;

    // Load both manifests
    let target_manifest = if !target_commit.manifest.is_empty() {
        load_manifest(kernel, &target_commit.manifest)
    } else {
        None
    };
    let source_manifest = if !source_commit.manifest.is_empty() {
        load_manifest(kernel, &source_commit.manifest)
    } else {
        None
    };

    // Build per-source maps of rg_key → RowGroupEntry
    let target_rgs: HashMap<String, &_> = target_manifest.as_ref()
        .map(|m| m.row_groups.iter().map(|rg| (rg.key.clone(), rg)).collect())
        .unwrap_or_default();
    let source_rgs: HashMap<String, &_> = source_manifest.as_ref()
        .map(|m| m.row_groups.iter().map(|rg| (rg.key.clone(), rg)).collect())
        .unwrap_or_default();

    // Identify conflicting keys (in BOTH target and source)
    let conflicting_keys: Vec<String> = target_rgs.keys()
        .filter(|k| source_rgs.contains_key(*k))
        .cloned()
        .collect();

    // Build merged entries
    let mut merged_entries: Vec<crate::manifest::RowGroupEntry> = Vec::new();

    // For non-conflicting keys: keep as-is (zero decode cost)
    let all_keys: std::collections::BTreeSet<String> = target_rgs.keys()
        .chain(source_rgs.keys())
        .cloned()
        .collect();

    for key in &all_keys {
        if conflicting_keys.contains(key) {
            continue; // handled below (or by CRDT merge if applicable)
        }
        // Prefer source (branch), then target
        if let Some(rg) = source_rgs.get(key) {
            merged_entries.push((*rg).clone());
        } else if let Some(rg) = target_rgs.get(key) {
            merged_entries.push((*rg).clone());
        }
    }

    // Determine key_col for CRDT merge
    let key_col = source_manifest.as_ref()
        .or(target_manifest.as_ref())
        .map(|m| m.key_col.clone())
        .unwrap_or_default();

    // For conflicting keys: attempt row-level CRDT merge.
    for key in &conflicting_keys {
        if let (Some(trg), Some(srg)) = (target_rgs.get(key), source_rgs.get(key)) {
            match try_crdt_merge_row_groups(kernel, trg, srg, &key_col) {
                Some(merged_rg) => merged_entries.push(merged_rg),
                None => merged_entries.push((*srg).clone()), // LWW fallback
            }
        }
    }

    // Build the merged manifest
    let schema = source_manifest.as_ref()
        .or(target_manifest.as_ref())
        .map(|m| m.columns.clone())
        .unwrap_or_default();

    let mut new_manifest = CollectionManifest::new(schema, key_col.clone());
    for entry in merged_entries {
        new_manifest.add_row_group(entry);
    }
    let manifest_bytes = new_manifest.encode();
    let manifest_hash = kernel.write(&manifest_bytes)
        .map_err(|e| format!("Failed to write merged manifest: {}", e))?;

    // Write the merge commit with TWO parents
    let commit_index = target_commit.index + 1;
    let merge_message = if message.is_empty() {
        format!("Merge '{}' into '{}'", source_branch, target_branch)
    } else {
        message.to_string()
    };

    let merge_hash = commit::write_commit(
        kernel,
        collection,
        &manifest_hash,
        Some(&target_head),       // parent = target
        Some(&source_head),       // second_parent = source
        &merge_message,
        commit_index,
    ).map_err(|e| format!("Failed to write merge commit: {}", e))?;

    // Point target branch at the merge commit
    kernel.reference(&branch_ref(collection, target_branch), &merge_hash)
        .map_err(|e| format!("Failed to update branch ref: {}", e))?;
    kernel.reference(&manifest_ref(collection, target_branch), &manifest_hash)
        .map_err(|e| format!("Failed to update manifest ref: {}", e))?;

    // === Copy shards from source branch to target branch ===
    // CRDT shards (upsert_shard, delete_shard) live alongside HEAD. When merging
    // branches, these shards must be copied so that row-level CRDT updates/deletes
    // from the source branch are visible in the target branch after merge.
    let source_shards = crate::shard::list_shards(kernel, collection, source_branch);
    let target_shard_prefix = crate::shards_prefix(collection, target_branch);
    for (shard_name, shard_hash) in &source_shards {
        let target_ref = format!("{}{}", target_shard_prefix, shard_name);
        let _ = kernel.reference(&target_ref, shard_hash);
    }

    Ok(merge_hash)
}

/// Undo the last N commits — walk parent pointers.
pub fn undo(
    kernel: &PondKernel,
    collection: &str,
    active_branch: &str,
    steps: usize,
) -> Result<String, String> {
    let mut current = kernel.resolve(&branch_ref(collection, active_branch))
        .ok_or_else(|| "No commits to undo".to_string())?;

    for _ in 0..steps {
        let commit = commit::read_commit(kernel, &current)
            .ok_or_else(|| "Failed to read commit during undo".to_string())?;
        current = commit.parent
            .ok_or_else(|| "Cannot undo: no parent commit".to_string())?;
    }

    // Point active branch at the target commit
    kernel.reference(&branch_ref(collection, active_branch), &current)
        .map_err(|e| format!("Failed to update branch ref: {}", e))?;

    // Sync manifest ref
    if let Some(commit) = commit::read_commit(kernel, &current) {
        if !commit.manifest.is_empty() {
            let _ = kernel.reference(&manifest_ref(collection, active_branch), &commit.manifest);
        }
    }

    Ok(current)
}

/// Revert the active branch to a specific commit hash.
pub fn revert(
    kernel: &PondKernel,
    collection: &str,
    active_branch: &str,
    commit_hash: &str,
) -> Result<(), String> {
    // Verify the commit exists
    if kernel.read_blob(commit_hash).is_err() {
        return Err(format!("Commit '{}' not found", commit_hash));
    }

    kernel.reference(&branch_ref(collection, active_branch), commit_hash)
        .map_err(|e| format!("Failed to update branch ref: {}", e))?;

    // Sync manifest ref
    if let Some(commit) = commit::read_commit(kernel, commit_hash) {
        if !commit.manifest.is_empty() {
            let _ = kernel.reference(&manifest_ref(collection, active_branch), &commit.manifest);
        }
    }

    Ok(())
}

/// Load a manifest from a hash.
fn load_manifest(kernel: &PondKernel, manifest_hash: &str) -> Option<CollectionManifest> {
    let data = kernel.read_blob(manifest_hash).ok()?;
    CollectionManifest::decode(&data)
}

// ---------------------------------------------------------------------------
// Row-level CRDT merge for branch merge
// ---------------------------------------------------------------------------

/// Attempt to row-level CRDT merge two conflicting row groups.
/// Returns None if CRDT merge is not possible (non-CRDT data, decode failure).
fn try_crdt_merge_row_groups(
    kernel: &PondKernel,
    target_rg: &crate::manifest::RowGroupEntry,
    source_rg: &crate::manifest::RowGroupEntry,
    _key_col: &str,
) -> Option<crate::manifest::RowGroupEntry> {
    // Read both blobs
    let target_data = kernel.read_blob(&target_rg.blob_hash).ok()?;
    let source_data = kernel.read_blob(&source_rg.blob_hash).ok()?;

    // Decode both to JSON rows (handles PND2 and JSON formats)
    let target_rows = decode_blob_to_json_rows(&target_data)?;
    let source_rows = decode_blob_to_json_rows(&source_data)?;

    // Verify both have CRDT columns
    if !has_crdt_columns(&target_rows) || !has_crdt_columns(&source_rows) {
        return None;
    }

    // Concatenate and merge
    let mut all_rows: Vec<serde_json::Value> = Vec::with_capacity(target_rows.len() + source_rows.len());
    all_rows.extend(target_rows);
    all_rows.extend(source_rows);

    let merged_rows = crate::shard::merge_rows_by_rowid(&all_rows, None);
    let live_rows = crate::shard::filter_live_rows(&merged_rows);

    // Re-encode as PND2 using simple type inference
    let (blob, col_stats) = encode_json_rows_to_pnd2(&live_rows, &target_rg.columns)?;
    let new_hash = kernel.write(&blob).ok()?;

    Some(crate::manifest::RowGroupEntry {
        key: target_rg.key.clone(),
        blob_hash: new_hash,
        n_rows: live_rows.len() as u32,
        columns: col_stats,
    })
}

/// Check if rows have CRDT columns (_rowid and _version).
fn has_crdt_columns(rows: &[serde_json::Value]) -> bool {
    if rows.is_empty() { return false; }
    let first = &rows[0];
    first.get("_rowid").is_some() && first.get("_version").is_some()
}

/// Decode a blob into JSON rows (PND2 or JSON array).
fn decode_blob_to_json_rows(data: &[u8]) -> Option<Vec<serde_json::Value>> {
    if data.len() >= 4 && &data[0..4] == b"PND2" {
        let cols = pond_core::pnd2_decode(data).ok()?;
        Some(pnd2_columns_to_json_rows(&cols))
    } else if data.first() == Some(&b'[') {
        serde_json::from_slice::<Vec<serde_json::Value>>(data).ok()
    } else {
        None
    }
}

/// Transpose PND2 columns into JSON rows.
fn pnd2_columns_to_json_rows(cols: &[pond_core::PondColumn]) -> Vec<serde_json::Value> {
    let n_rows = cols.first().map(|c| c.n_values).unwrap_or(0);
    let mut rows = Vec::with_capacity(n_rows);
    for i in 0..n_rows {
        let mut row = serde_json::Map::new();
        for col in cols {
            let name = col.name.to_str().unwrap_or("").to_string();
            if name.is_empty() { continue; }
            let val = match col.vtype {
                1 => col.i64_data.get(i).map(|x| serde_json::json!(x)), // VT_INT64
                2 => col.f64_data.get(i).map(|x| serde_json::json!(x)), // VT_FLOAT64
                3 | 6 => col.str_data.get(i).map(|s| serde_json::json!(s.to_str().unwrap_or(""))), // VT_STRING/VT_VARIANT
                _ => None,
            };
            if let Some(v) = val { row.insert(name, v); }
        }
        rows.push(serde_json::Value::Object(row));
    }
    rows
}

/// Encode JSON rows into a PND2 blob with type inference.
fn encode_json_rows_to_pnd2(
    rows: &[serde_json::Value],
    _ref_stats: &[crate::manifest::ColumnStatsEntry],
) -> Option<(Vec<u8>, Vec<crate::manifest::ColumnStatsEntry>)> {
    if rows.is_empty() { return None; }

    // Collect column names
    let mut col_names: Vec<String> = Vec::new();
    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
    for row in rows {
        if let Some(obj) = row.as_object() {
            for k in obj.keys() {
                if seen.insert(k.clone()) {
                    col_names.push(k.clone());
                }
            }
        }
    }

    // Build typed columns with type inference
    use pond_core::TypedColumn;
    let mut typed_cols: Vec<(&str, TypedColumn)> = Vec::new();
    let mut col_stats: Vec<crate::manifest::ColumnStatsEntry> = Vec::new();

    for name in &col_names {
        // Infer type: check if all values are i64, f64, or string
        let mut has_i64 = false;
        let mut has_f64 = false;
        let mut has_string = false;
        for row in rows {
            match row.get(name) {
                Some(serde_json::Value::Number(n)) if n.is_i64() => has_i64 = true,
                Some(serde_json::Value::Number(n)) if n.is_f64() => has_f64 = true,
                Some(serde_json::Value::Number(_)) => has_f64 = true,
                Some(serde_json::Value::String(_)) => has_string = true,
                _ => {}
            }
        }

        let (typed_col, vtype) = if has_string {
            let vals: Vec<String> = rows.iter().map(|r| {
                r.get(name).and_then(|v| v.as_str()).map(|s| s.to_string()).unwrap_or_default()
            }).collect();
            (TypedColumn::String(vals), 3u8)
        } else if has_f64 {
            let vals: Vec<f64> = rows.iter().map(|r| {
                r.get(name).and_then(|v| v.as_f64()).unwrap_or(0.0)
            }).collect();
            (TypedColumn::Float64(vals), 2u8)
        } else if has_i64 {
            let vals: Vec<i64> = rows.iter().map(|r| {
                r.get(name).and_then(|v| v.as_i64()).unwrap_or(0)
            }).collect();
            (TypedColumn::Int64(vals), 1u8)
        } else {
            let vals: Vec<String> = rows.iter().map(|_| String::new()).collect();
            (TypedColumn::String(vals), 3u8)
        };

        typed_cols.push((name.as_str(), typed_col));
        col_stats.push(crate::manifest::ColumnStatsEntry {
            name: name.clone(),
            value_type: vtype,
            min: None,
            max: None,
            null_count: 0,
        });
    }

    let blob = pond_core::pnd2_encode_multi_typed(&typed_cols);
    Some((blob, col_stats))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::UnifiedStorage;
    use crate::commit;

    fn setup() -> (tempfile::TempDir, UnifiedStorage) {
        let dir = tempfile::tempdir().unwrap();
        let storage = UnifiedStorage::new_local(dir.path()).unwrap();
        (dir, storage)
    }

    #[test]
    fn test_branch_creates_refs() {
        let (_dir, storage) = setup();
        let kernel = storage.kernel();

        // Write initial data + commit
        let data_hash = kernel.write(b"test data").unwrap();
        let commit_hash = commit::write_commit(
            kernel, "users", &data_hash, None, None, "initial", 0,
        ).unwrap();
        kernel.reference(&branch_ref("users", "main"), &commit_hash).unwrap();
        kernel.reference(&manifest_ref("users", "main"), &data_hash).unwrap();

        // Create branch
        let result = branch(kernel, "users", "experiment", "main").unwrap();
        assert_eq!(result, commit_hash);

        // Both branches should point at the same commit
        assert_eq!(
            kernel.resolve(&branch_ref("users", "main")),
            kernel.resolve(&branch_ref("users", "experiment"))
        );
        // Both manifest refs should match
        assert_eq!(
            kernel.resolve(&manifest_ref("users", "main")),
            kernel.resolve(&manifest_ref("users", "experiment"))
        );
    }

    #[test]
    fn test_list_branches() {
        let (_dir, storage) = setup();
        let kernel = storage.kernel();

        // Setup main branch
        let data_hash = kernel.write(b"data").unwrap();
        let commit_hash = commit::write_commit(
            kernel, "users", &data_hash, None, None, "init", 0,
        ).unwrap();
        kernel.reference(&branch_ref("users", "main"), &commit_hash).unwrap();

        // Create two more branches
        branch(kernel, "users", "experiment", "main").unwrap();
        branch(kernel, "users", "feature", "main").unwrap();

        let branches = list_branches(kernel, "users");
        assert!(branches.contains(&"main".to_string()));
        assert!(branches.contains(&"experiment".to_string()));
        assert!(branches.contains(&"feature".to_string()));
    }

    #[test]
    fn test_merge_writes_two_parents() {
        let (_dir, storage) = setup();
        let kernel = storage.kernel();

        // Setup main branch with commit
        let data1 = kernel.write(b"data1").unwrap();
        let commit1 = commit::write_commit(
            kernel, "users", &data1, None, None, "c1", 0,
        ).unwrap();
        kernel.reference(&branch_ref("users", "main"), &commit1).unwrap();
        kernel.reference(&manifest_ref("users", "main"), &data1).unwrap();

        // Create feature branch
        branch(kernel, "users", "feature", "main").unwrap();

        // Write different data on main (so parents differ)
        let data2 = kernel.write(b"data2").unwrap();
        let commit2 = commit::write_commit(
            kernel, "users", &data2, Some(&commit1), None, "c2", 1,
        ).unwrap();
        kernel.reference(&branch_ref("users", "main"), &commit2).unwrap();
        kernel.reference(&manifest_ref("users", "main"), &data2).unwrap();

        // Merge feature into main
        let merge_hash = merge(kernel, "users", "feature", "main", "test merge").unwrap();

        // Verify the merge commit has two parents
        let merge_commit = commit::read_commit(kernel, &merge_hash).unwrap();
        assert_eq!(merge_commit.parent, Some(commit2.clone()));       // target (main)
        assert_eq!(merge_commit.second_parent, Some(commit1.clone())); // source (feature)
        assert!(merge_commit.is_merge());
    }

    #[test]
    fn test_undo_walks_parent_chain() {
        let (_dir, storage) = setup();
        let kernel = storage.kernel();

        // Write 3 commits: c1 → c2 → c3
        let data = kernel.write(b"data").unwrap();
        let c1 = commit::write_commit(kernel, "users", &data, None, None, "c1", 0).unwrap();
        let c2 = commit::write_commit(kernel, "users", &data, Some(&c1), None, "c2", 1).unwrap();
        let c3 = commit::write_commit(kernel, "users", &data, Some(&c2), None, "c3", 2).unwrap();
        kernel.reference(&branch_ref("users", "main"), &c3).unwrap();
        kernel.reference(&manifest_ref("users", "main"), &data).unwrap();

        // Undo 1 step → should be at c2
        let result = undo(kernel, "users", "main", 1).unwrap();
        assert_eq!(result, c2);

        // Undo 1 more → should be at c1
        let result = undo(kernel, "users", "main", 1).unwrap();
        assert_eq!(result, c1);
    }

    #[test]
    fn test_revert() {
        let (_dir, storage) = setup();
        let kernel = storage.kernel();

        let data = kernel.write(b"data").unwrap();
        let c1 = commit::write_commit(kernel, "users", &data, None, None, "c1", 0).unwrap();
        let c2 = commit::write_commit(kernel, "users", &data, Some(&c1), None, "c2", 1).unwrap();
        kernel.reference(&branch_ref("users", "main"), &c2).unwrap();
        kernel.reference(&manifest_ref("users", "main"), &data).unwrap();

        // Revert to c1
        revert(kernel, "users", "main", &c1).unwrap();

        // main should now point at c1
        assert_eq!(
            kernel.resolve(&branch_ref("users", "main")),
            Some(c1)
        );
    }
}
