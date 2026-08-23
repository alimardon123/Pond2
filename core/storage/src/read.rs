// Read module — read data from collections
//
// FAITHFUL PORT of Python UnifiedStorage's read / read_at_snapshot methods.

use crate::branch_ref;
use crate::commit;
use crate::manifest::CollectionManifest;
use crate::shard;
use pond_kernel::PondKernel;

/// Read the current data for a collection (from the active branch's HEAD).
///
/// Returns the raw data blob for the HEAD commit's manifest.
/// For a full row-level read (with shard merging), use read_with_shards.
pub fn read(
    kernel: &PondKernel,
    collection: &str,
    branch: &str,
) -> Result<Vec<u8>, String> {
    // Resolve HEAD commit
    let head = kernel.resolve(&branch_ref(collection, branch))
        .ok_or_else(|| format!("Collection '{}' has no commits", collection))?;

    // Read the commit to get the manifest hash
    let commit = commit::read_commit(kernel, &head)
        .ok_or_else(|| "Failed to read HEAD commit".to_string())?;

    if commit.manifest.is_empty() {
        return Err("HEAD commit has no manifest".to_string());
    }

    // Load the manifest
    let manifest_bytes = kernel.read_blob(&commit.manifest)
        .map_err(|e| format!("Failed to read manifest: {}", e))?;
    let manifest = CollectionManifest::decode(&manifest_bytes)
        .ok_or_else(|| "Failed to decode manifest".to_string())?;

    // Read ALL row groups' blobs (parallel via read_blob_batch) and
    // concatenate the bytes. The previous implementation read only
    // `row_groups.first()` — a silent data-loss bug for multi-RG
    // collections (fixed Task 0-p, see worklog).
    //
    // For raw-byte writes (write() path, 1 RG), behavior is unchanged.
    // For structured PND2 writes (write_rows_i64() with >1 RG), callers
    // that need to decode individual PND2 blobs should use
    // `read_all_row_groups()` which returns `Vec<Vec<u8>>` (one per RG).
    if manifest.row_groups.is_empty() {
        return Err("Manifest has no row groups".to_string());
    }
    let hashes: Vec<String> = manifest.row_groups.iter()
        .map(|rg| rg.blob_hash.clone())
        .collect();
    let blobs = kernel.read_blob_batch(&hashes)
        .map_err(|e| format!("Failed to read data blobs: {}", e))?;
    let total: usize = blobs.iter().map(|b| b.len()).sum();
    let mut out = Vec::with_capacity(total);
    for b in &blobs {
        out.extend_from_slice(b);
    }
    Ok(out)
}

/// Read all row groups of a collection's HEAD as separate byte vectors.
///
/// Unlike [`read`] (which concatenates RG bytes), this preserves the
/// per-RG boundary so callers can decode each PND2 blob independently.
///
/// **Slab-aware**: if a RowGroupEntry has `slab_byte_offset` set, this
/// function uses `kernel.read_blob_range()` to fetch ONLY the needed
/// bytes from the slab (not the whole slab). For S3, this is a Range GET
/// — typically 1000x smaller than a full GET for selective queries.
pub fn read_all_row_groups(
    kernel: &PondKernel,
    collection: &str,
    branch: &str,
) -> Result<Vec<Vec<u8>>, String> {
    let head = kernel.resolve(&branch_ref(collection, branch))
        .ok_or_else(|| format!("Collection '{}' has no commits", collection))?;
    let commit = commit::read_commit(kernel, &head)
        .ok_or_else(|| "Failed to read HEAD commit".to_string())?;
    if commit.manifest.is_empty() {
        return Err("HEAD commit has no manifest".to_string());
    }
    let manifest_bytes = kernel.read_blob(&commit.manifest)
        .map_err(|e| format!("Failed to read manifest: {}", e))?;
    let manifest = CollectionManifest::decode(&manifest_bytes)
        .ok_or_else(|| "Failed to decode manifest".to_string())?;
    if manifest.row_groups.is_empty() {
        return Err("Manifest has no row groups".to_string());
    }

    // Separate slab-backed RGs from standalone RGs
    let mut standalone_hashes: Vec<String> = Vec::new();
    let mut slab_ranges: Vec<(String, u64, u64)> = Vec::new(); // (slab_hash, start, end_exclusive)
    let mut rg_order: Vec<RgSource> = Vec::new();

    for rg in &manifest.row_groups {
        if let (Some(offset), Some(len)) = (rg.slab_byte_offset, rg.slab_byte_len) {
            slab_ranges.push((rg.blob_hash.clone(), offset, offset + len as u64));
            rg_order.push(RgSource::Slab(slab_ranges.len() - 1));
        } else {
            standalone_hashes.push(rg.blob_hash.clone());
            rg_order.push(RgSource::Standalone(standalone_hashes.len() - 1));
        }
    }

    // Batch-read all standalone blobs (parallel on S3)
    let standalone_results = if !standalone_hashes.is_empty() {
        kernel.read_blob_batch(&standalone_hashes)
            .map_err(|e| format!("Failed to read data blobs: {}", e))?
    } else {
        Vec::new()
    };

    // Range-read all slab RGs (parallel via std::thread::scope)
    let slab_results = if !slab_ranges.is_empty() {
        read_slab_ranges_parallel(kernel, &slab_ranges)?
    } else {
        Vec::new()
    };

    // Reassemble in manifest order
    let mut result = Vec::with_capacity(rg_order.len());
    for src in &rg_order {
        match src {
            RgSource::Standalone(idx) => result.push(standalone_results[*idx].clone()),
            RgSource::Slab(idx) => result.push(slab_results[*idx].clone()),
        }
    }
    Ok(result)
}

/// Internal enum tracking whether each RG comes from a standalone blob or a slab.
enum RgSource {
    Standalone(usize),
    Slab(usize),
}

/// Parallel range-read multiple byte ranges from (possibly different) slabs.
///
/// Uses `std::thread::scope` for zero-overhead parallelism.
/// On S3, each range-read is an HTTP Range GET (206 Partial Content).
/// On LocalFS, each is a seek+read_exact (avoids loading the whole file).
fn read_slab_ranges_parallel(
    kernel: &PondKernel,
    ranges: &[(String, u64, u64)],
) -> Result<Vec<Vec<u8>>, String> {
    use std::sync::{Arc, Mutex, atomic::{AtomicBool, Ordering}};
    use std::thread;
    let n = ranges.len();
    let results = Arc::new(Mutex::new(vec![Vec::new(); n]));
    let failed = Arc::new(AtomicBool::new(false));
    let error_msg = Arc::new(Mutex::new(None::<String>));

    thread::scope(|s| {
        for (i, (hash, start, end)) in ranges.iter().enumerate() {
            let hash = hash.clone();
            let start = *start;
            let end = *end;
            let results = Arc::clone(&results);
            let failed = Arc::clone(&failed);
            let error_msg = Arc::clone(&error_msg);
            s.spawn(move || {
                if failed.load(Ordering::Relaxed) { return; }
                match kernel.read_blob_range(&hash, start, end) {
                    Ok(data) => {
                        if let Ok(mut r) = results.lock() {
                            r[i] = data;
                        }
                    }
                    Err(e) => {
                        failed.store(true, Ordering::Relaxed);
                        if let Ok(mut msg) = error_msg.lock() {
                            *msg = Some(format!("Range read failed for slab {}: {:?}", hash, e));
                        }
                    }
                }
            });
        }
    });

    let err = error_msg.lock().unwrap();
    match &*err {
        Some(e) => Err(e.clone()),
        None => {
            let guard = results.lock().unwrap();
            Ok(guard.clone())
        }
    }
}

/// Read data at a specific commit — SNAPSHOT ISOLATION.
///
/// Reads ONLY the manifest at the given commit, ignoring any shards
/// written after that commit. Provides a consistent snapshot for
/// long-running analytical queries.
pub fn read_at_snapshot(
    kernel: &PondKernel,
    commit_hash: &str,
) -> Result<Vec<u8>, String> {
    let commit = commit::read_commit(kernel, commit_hash)
        .ok_or_else(|| "Commit not found".to_string())?;

    if commit.manifest.is_empty() {
        return Err("Commit has no manifest".to_string());
    }

    let manifest_bytes = kernel.read_blob(&commit.manifest)
        .map_err(|e| format!("Failed to read manifest: {}", e))?;
    let manifest = CollectionManifest::decode(&manifest_bytes)
        .ok_or_else(|| "Failed to decode manifest".to_string())?;

    // Read ALL row groups (parallel via read_blob_batch) and concatenate.
    // Previous impl only read the first RG — silent data loss for >1 RG.
    if manifest.row_groups.is_empty() {
        return Err("Manifest has no row groups".to_string());
    }
    let hashes: Vec<String> = manifest.row_groups.iter()
        .map(|rg| rg.blob_hash.clone())
        .collect();
    let blobs = kernel.read_blob_batch(&hashes)
        .map_err(|e| format!("Failed to read data blobs: {}", e))?;
    let total: usize = blobs.iter().map(|b| b.len()).sum();
    let mut out = Vec::with_capacity(total);
    for b in &blobs {
        out.extend_from_slice(b);
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// Async read path — behind `feature = "async"`.
//
// `read_rows_async` is the async equivalent of [`read`]. It uses
// `PondKernel::read_blob_async` (which is itself `spawn_blocking` on the
// sync `ObjectStore`) so callers get a non-blocking API without the storage
// crate having to re-implement the manifest / commit / row-group logic.
//
// Ref lookups (`resolve`) stay sync — they're fast (one stat or one HEAD
// request) and don't benefit from async. Only blob reads (which can be
// hundreds of KB) are async.
// ---------------------------------------------------------------------------

/// Async variant of [`read`].
///
/// Reads the HEAD data for a collection on the given branch, using the
/// kernel's async blob API. Returns the same bytes that [`read`] would.
///
/// The function is named `read_rows_async` for parity with the async API
/// spec, but it returns raw bytes (not structured rows) — same as [`read`].
/// For structured row reads with projection + pruning, see [`read_rows_i64`]
/// (a sync-only API for now).
#[cfg(feature = "async")]
pub async fn read_rows_async(
    kernel: &PondKernel,
    collection: &str,
    branch: &str,
) -> Result<Vec<u8>, String> {
    // 1. Resolve HEAD commit ref — sync, fast (one stat / one HEAD request).
    let head = kernel.resolve(&branch_ref(collection, branch))
        .ok_or_else(|| format!("Collection '{}' has no commits", collection))?;

    // 2. Read the commit blob async.
    let commit_bytes = kernel.read_blob_async(&head).await
        .map_err(|e| format!("Failed to read HEAD commit: {}", e))?;
    let commit = Commit::from_json_bytes(&commit_bytes)
        .ok_or_else(|| "Failed to decode HEAD commit".to_string())?;

    if commit.manifest.is_empty() {
        return Err("HEAD commit has no manifest".to_string());
    }

    // 3. Read the manifest blob async.
    let manifest_bytes = kernel.read_blob_async(&commit.manifest).await
        .map_err(|e| format!("Failed to read manifest: {}", e))?;
    let manifest = CollectionManifest::decode(&manifest_bytes)
        .ok_or_else(|| "Failed to decode manifest".to_string())?;

    // 4. Read ALL row group blobs async (sequential — parallel async is a
    // future optimization). Concatenate the bytes (matches sync `read`).
    if manifest.row_groups.is_empty() {
        return Err("Manifest has no row groups".to_string());
    }
    let mut out = Vec::new();
    for rg in &manifest.row_groups {
        let blob = kernel.read_blob_async(&rg.blob_hash).await
            .map_err(|e| format!("Failed to read data blob: {}", e))?;
        out.extend_from_slice(&blob);
    }
    Ok(out)
}

/// Async variant of [`read_at_snapshot`]. Reads the data at a specific
/// commit hash, ignoring shards written after that commit.
#[cfg(feature = "async")]
pub async fn read_at_snapshot_async(
    kernel: &PondKernel,
    commit_hash: &str,
) -> Result<Vec<u8>, String> {
    let commit_bytes = kernel.read_blob_async(commit_hash).await
        .map_err(|e| format!("Failed to read commit: {}", e))?;
    let commit = Commit::from_json_bytes(&commit_bytes)
        .ok_or_else(|| "Commit not found or corrupt".to_string())?;

    if commit.manifest.is_empty() {
        return Err("Commit has no manifest".to_string());
    }

    let manifest_bytes = kernel.read_blob_async(&commit.manifest).await
        .map_err(|e| format!("Failed to read manifest: {}", e))?;
    let manifest = CollectionManifest::decode(&manifest_bytes)
        .ok_or_else(|| "Failed to decode manifest".to_string())?;

    // Read ALL row groups async (sequential — parallel async is a future
    // optimization). Concatenate the bytes (matches sync `read_at_snapshot`).
    if manifest.row_groups.is_empty() {
        return Err("Manifest has no row groups".to_string());
    }
    let mut out = Vec::new();
    for rg in &manifest.row_groups {
        let blob = kernel.read_blob_async(&rg.blob_hash).await
            .map_err(|e| format!("Failed to read data blob: {}", e))?;
        out.extend_from_slice(&blob);
    }
    Ok(out)
}

/// Read the full collection data including shards (CRDT read path).
///
/// Returns the HEAD data plus all unmerged shard data.
/// For snapshot isolation, use read_at_snapshot instead.
pub fn read_full(
    kernel: &PondKernel,
    collection: &str,
    branch: &str,
) -> Vec<Vec<u8>> {
    let mut results = Vec::new();

    // Read HEAD data
    if let Ok(data) = read(kernel, collection, branch) {
        results.push(data);
    }

    // Read all shard data
    let (_, shards) = shard::read_with_shards(kernel, collection, branch);
    for (_, shard_hash) in shards {
        if let Ok(data) = kernel.read_blob(&shard_hash) {
            results.push(data);
        }
    }

    results
}

/// Read structured INT64 columns from a collection with optional pruning.
///
/// This is the PRODUCTION read path — decodes PND2 blobs and applies:
///   - Predicate pruning: skip row groups whose stats don't match predicates
///   - Column projection: only decode requested columns
///
/// Args:
///   - kernel: The PondKernel handle
///   - collection: Collection name
///   - branch: Branch to read from
///   - columns: Optional list of column names to project (None = all columns)
///   - predicates: Optional list of (column, op, value) for row-group pruning
///
/// Returns: Vec<(column_name, Vec<i64>)> — decoded column data
pub fn read_rows_i64(
    kernel: &PondKernel,
    collection: &str,
    branch: &str,
    columns: Option<&[String]>,
    predicates: Option<&[(&str, &str, i64)]>,
) -> Result<Vec<(String, Vec<i64>)>, String> {
    // Resolve HEAD commit
    let head = kernel.resolve(&branch_ref(collection, branch))
        .ok_or_else(|| format!("Collection '{}' has no commits", collection))?;

    // Check if HEAD is a PondPack blob
    let head_data = kernel.read_blob(&head)
        .map_err(|e| format!("Failed to read HEAD: {}", e))?;

    let manifest_bytes = if crate::pond_pack::is_pack(&head_data) {
        // PondPack — extract manifest from the pack
        let (commit, manifest_bytes, _inline) = crate::pond_pack::decode_pack(&head_data)
            .ok_or_else(|| "Failed to decode PondPack".to_string())?;
        let _ = commit; // commit metadata not needed for read
        manifest_bytes
    } else {
        // Old format — read commit, then manifest separately
        let commit = commit::read_commit(kernel, &head)
            .ok_or_else(|| "Failed to read HEAD commit".to_string())?;

        if commit.manifest.is_empty() {
            return Err("HEAD commit has no manifest".to_string());
        }

        kernel.read_blob(&commit.manifest)
            .map_err(|e| format!("Failed to read manifest: {}", e))?
    };

    // Decode manifest
    let manifest = CollectionManifest::decode(&manifest_bytes)
        .ok_or_else(|| "Failed to decode manifest".to_string())?;

    // Build projection set (which columns to decode)
    let projection: Option<std::collections::HashSet<&str>> = columns.map(|cols| {
        cols.iter().map(|s| s.as_str()).collect()
    });

    // Collect results: column_name → Vec<i64>
    use std::collections::HashMap;
    let mut result_cols: HashMap<String, Vec<i64>> = HashMap::new();

    // Read each row group, applying predicate pruning
    for rg in &manifest.row_groups {
        // Predicate pruning: check if this row group can be skipped
        if let Some(preds) = predicates {
            let mut skip = false;
            for (col_name, op, value) in preds {
                // Find the column stats
                let col_stats = rg.columns.iter().find(|c| c.name == *col_name);
                if let Some(stats) = col_stats {
                    // Check if the row group can be pruned
                    if can_prune_row_group(stats, op, *value) {
                        skip = true;
                        break;
                    }
                }
            }
            if skip {
                continue; // Skip this row group entirely
            }
        }

        // Read and decode the PND2 blob
        let blob_data = kernel.read_blob(&rg.blob_hash)
            .map_err(|e| format!("Failed to read data blob: {}", e))?;

        let cols = pond_core::pnd2_decode(&blob_data)
            .map_err(|e| format!("Failed to decode PND2 blob: {}", e))?;

        // Append decoded values to result columns (with projection)
        for col in &cols {
            let name = col.name.to_string_lossy().to_string();

            // Skip if projection requested and this column is not in it
            if let Some(ref proj) = projection {
                if !proj.contains(name.as_str()) {
                    continue;
                }
            }

            // Only collect INT64 columns
            if col.vtype == pond_core::VT_INT64 {
                let entry = result_cols.entry(name.clone()).or_default();
                entry.extend_from_slice(&col.i64_data);
            }
        }
    }

    // Convert to ordered Vec
    let mut result: Vec<(String, Vec<i64>)> = result_cols.into_iter().collect();
    result.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(result)
}

/// Check if a row group can be pruned based on column stats + predicate.
///
/// Returns true if the row group CANNOT match the predicate (should be skipped).
fn can_prune_row_group(
    stats: &crate::manifest::ColumnStatsEntry,
    op: &str,
    value: i64,
) -> bool {
    let (min, max) = match (&stats.min, &stats.max) {
        (Some(m), Some(x)) if m.len() >= 8 && x.len() >= 8 => {
            let min_val = i64::from_le_bytes([
                m[0], m[1], m[2], m[3], m[4], m[5], m[6], m[7]
            ]);
            let max_val = i64::from_le_bytes([
                x[0], x[1], x[2], x[3], x[4], x[5], x[6], x[7]
            ]);
            (min_val, max_val)
        }
        _ => return false, // No stats — can't prune
    };

    match op {
        "=" | "==" => value < min || value > max,
        "<" => min >= value,
        "<=" => min > value,
        ">" => max <= value,
        ">=" => max < value,
        "!=" | "<>" => false, // Can't prune != (row group might have other values)
        _ => false,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::UnifiedStorage;
    use crate::write;

    #[test]
    fn test_read_returns_head_data() {
        let dir = tempfile::tempdir().unwrap();
        let storage = UnifiedStorage::new_local(dir.path()).unwrap();
        let kernel = storage.kernel();

        write::write(kernel, "users", "main", b"hello world", "initial").unwrap();
        let data = read(kernel, "users", "main").unwrap();
        assert_eq!(data, b"hello world");
    }

    #[test]
    fn test_read_at_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        let storage = UnifiedStorage::new_local(dir.path()).unwrap();
        let kernel = storage.kernel();

        let c1 = write::write(kernel, "users", "main", b"v1", "first").unwrap();
        write::write(kernel, "users", "main", b"v2", "second").unwrap();

        // Read at c1 (should return v1, not v2)
        let data = read_at_snapshot(kernel, &c1).unwrap();
        assert_eq!(data, b"v1");
    }

    #[test]
    fn test_read_full_includes_shards() {
        let dir = tempfile::tempdir().unwrap();
        let storage = UnifiedStorage::new_local(dir.path()).unwrap();
        let kernel = storage.kernel();

        write::write(kernel, "events", "main", b"head data", "init").unwrap();
        crate::shard::append_shard(kernel, "events", "main", "s1", b"shard1").unwrap();

        let data = read_full(kernel, "events", "main");
        assert_eq!(data.len(), 2); // HEAD + 1 shard
        assert!(data.iter().any(|d| d == b"head data"));
        assert!(data.iter().any(|d| d == b"shard1"));
    }

    #[test]
    fn test_read_rows_i64_decodes_pnd2() {
        let dir = tempfile::tempdir().unwrap();
        let storage = UnifiedStorage::new_local(dir.path()).unwrap();
        let kernel = storage.kernel();

        let ids = vec![1i64, 2, 3, 4, 5];
        let ages = vec![30i64, 25, 35, 40, 28];

        // Write using write_rows_i64 (PND2 encoding)
        crate::write::write_rows_i64(
            kernel, "users", "main",
            &[("id", &ids), ("age", &ages)],
            "insert 5 users",
        ).unwrap();

        // Read back using read_rows_i64
        let cols = read_rows_i64(kernel, "users", "main", None, None).unwrap();

        assert_eq!(cols.len(), 2); // id + age

        // Find the columns by name
        let id_col = cols.iter().find(|(n, _)| n == "id").expect("id column");
        let age_col = cols.iter().find(|(n, _)| n == "age").expect("age column");

        assert_eq!(id_col.1, ids);
        assert_eq!(age_col.1, ages);
    }

    #[test]
    fn test_read_rows_i64_with_projection() {
        let dir = tempfile::tempdir().unwrap();
        let storage = UnifiedStorage::new_local(dir.path()).unwrap();
        let kernel = storage.kernel();

        let ids = vec![1i64, 2, 3];
        let ages = vec![30i64, 25, 35];
        let scores = vec![100i64, 200, 300];

        crate::write::write_rows_i64(
            kernel, "test", "main",
            &[("id", &ids), ("age", &ages), ("score", &scores)],
            "3 cols",
        ).unwrap();

        // Project only "id" and "score"
        let proj = vec!["id".to_string(), "score".to_string()];
        let cols = read_rows_i64(kernel, "test", "main", Some(&proj), None).unwrap();

        assert_eq!(cols.len(), 2); // only id + score (age projected out)
        assert!(cols.iter().any(|(n, _)| n == "id"));
        assert!(cols.iter().any(|(n, _)| n == "score"));
        assert!(!cols.iter().any(|(n, _)| n == "age"));
    }

    #[test]
    fn test_read_rows_i64_with_predicate_pruning() {
        let dir = tempfile::tempdir().unwrap();
        let storage = UnifiedStorage::new_local(dir.path()).unwrap();
        let kernel = storage.kernel();

        // Write data with id range [1, 100]
        let ids: Vec<i64> = (1..=100).collect();
        let vals: Vec<i64> = (1..=100).map(|i| i * 10).collect();

        crate::write::write_rows_i64(
            kernel, "data", "main",
            &[("id", &ids), ("val", &vals)],
            "100 rows",
        ).unwrap();

        // Read with predicate: id > 50
        // This won't prune the single row group (stats show min=1, max=100, so
        // the predicate might match), but it tests the predicate path
        let preds: Vec<(&str, &str, i64)> = vec![("id", ">", 50)];
        let cols = read_rows_i64(kernel, "data", "main", None, Some(&preds)).unwrap();

        // Should still return all 100 rows (single row group can't be pruned)
        let id_col = cols.iter().find(|(n, _)| n == "id").unwrap();
        assert_eq!(id_col.1.len(), 100);
    }

    #[test]
    fn test_read_rows_i64_from_packed_write() {
        let dir = tempfile::tempdir().unwrap();
        let storage = UnifiedStorage::new_local(dir.path()).unwrap();
        let kernel = storage.kernel();

        let ids = vec![10i64, 20, 30];
        let scores = vec![100i64, 200, 300];

        // Write using write_rows_i64_packed (PondPack format)
        crate::write::write_rows_i64_packed(
            kernel, "packed", "main",
            &[("id", &ids), ("score", &scores)],
            "packed write",
        ).unwrap();

        // Read back — should detect PondPack and extract manifest
        let cols = read_rows_i64(kernel, "packed", "main", None, None).unwrap();

        assert_eq!(cols.len(), 2);
        let id_col = cols.iter().find(|(n, _)| n == "id").expect("id column");
        let score_col = cols.iter().find(|(n, _)| n == "score").expect("score column");
        assert_eq!(id_col.1, ids);
        assert_eq!(score_col.1, scores);
    }
}

// ---------------------------------------------------------------------------
// Async tests — only compiled when `feature = "async"` is on.
// ---------------------------------------------------------------------------

#[cfg(all(test, feature = "async"))]
mod async_tests {
    use super::*;
    use crate::UnifiedStorage;
    use crate::write;

    /// `read_rows_async` returns the same bytes as the sync `read`.
    #[tokio::test]
    async fn test_read_rows_async_matches_sync() {
        let dir = tempfile::tempdir().unwrap();
        let storage = UnifiedStorage::new_local(dir.path()).unwrap();
        let kernel = storage.kernel();

        write::write(kernel, "users", "main", b"hello async", "initial").unwrap();

        let sync_data = read(kernel, "users", "main").unwrap();
        let async_data = read_rows_async(kernel, "users", "main").await.unwrap();
        assert_eq!(sync_data, async_data, "sync and async read must return identical bytes");
        assert_eq!(async_data, b"hello async");
    }

    /// `read_at_snapshot_async` returns the snapshot data (not the latest HEAD).
    #[tokio::test]
    async fn test_read_at_snapshot_async_isolates_from_later_writes() {
        let dir = tempfile::tempdir().unwrap();
        let storage = UnifiedStorage::new_local(dir.path()).unwrap();
        let kernel = storage.kernel();

        let c1 = write::write(kernel, "users", "main", b"v1", "first").unwrap();
        // Write a second commit — the snapshot read should still see v1.
        write::write(kernel, "users", "main", b"v2", "second").unwrap();

        let snap = read_at_snapshot_async(kernel, &c1).await.unwrap();
        assert_eq!(snap, b"v1", "snapshot read must isolate from later writes");
    }

    /// `read_rows_async` on a non-existent collection returns Err.
    #[tokio::test]
    async fn test_read_rows_async_missing_collection_errors() {
        let dir = tempfile::tempdir().unwrap();
        let storage = UnifiedStorage::new_local(dir.path()).unwrap();
        let kernel = storage.kernel();

        let err = read_rows_async(kernel, "ghosts", "main").await.unwrap_err();
        assert!(err.contains("no commits") || err.contains("ghosts"),
            "error message should mention the missing collection: {}", err);
    }

    /// Concurrent `read_rows_async` calls on different collections don't
    /// deadlock and each returns the right bytes.
    #[tokio::test]
    async fn test_read_rows_async_concurrent() {
        let dir = tempfile::tempdir().unwrap();
        let storage = UnifiedStorage::new_local(dir.path()).unwrap();
        let kernel = storage.kernel();

        // Write 4 collections with distinct payloads.
        let names = ["a", "b", "c", "d"];
        let payloads = [b"alpha".to_vec(), b"beta".to_vec(), b"gamma".to_vec(), b"delta".to_vec()];
        for (n, p) in names.iter().zip(payloads.iter()) {
            write::write(kernel, n, "main", p, "init").unwrap();
        }

        // Read them concurrently — borrow kernel via &PondKernel through Arc.
        let kernel_arc = std::sync::Arc::new(storage);
        let mut handles = Vec::new();
        for n in &names {
            let k = kernel_arc.clone();
            let n = n.to_string();
            handles.push(tokio::spawn(async move {
                read_rows_async(k.kernel(), &n, "main").await.unwrap()
            }));
        }
        let mut results = Vec::new();
        for h in handles {
            results.push(h.await.unwrap());
        }
        // Each result must match its corresponding payload.
        for (r, p) in results.iter().zip(payloads.iter()) {
            assert_eq!(r, p);
        }
    }
}
