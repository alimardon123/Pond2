// Read module — read data from collections
//
// FAITHFUL PORT of Python UnifiedStorage's read / read_at_snapshot methods.

use crate::branch_ref;
use crate::commit;
use crate::manifest::{CollectionManifest, RootManifest, pman_version};
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

    // Load the manifest (handles both v2 flat and v3 tree)
    let manifest_bytes = kernel.read_blob(&commit.manifest)
        .map_err(|e| format!("Failed to read manifest: {}", e))?;
    let manifest = resolve_manifest(kernel, &manifest_bytes, None)?;

    // Read ALL row groups (slab-aware) and concatenate.
    // Previous impl only read the first RG — silent data loss for multi-RG.
    // For raw-byte writes (write() path, 1 RG), behavior is unchanged.
    // For structured PND2 writes (write_rows_i64() with >1 RG), callers
    // that need to decode individual PND2 blobs should use
    // `read_all_row_groups()` which returns `Vec<Vec<u8>>` (one per RG).
    if manifest.row_groups.is_empty() {
        return Err("Manifest has no row groups".to_string());
    }
    let blobs = read_all_row_groups_from_manifest(kernel, &manifest)?;
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
    let manifest = resolve_manifest(kernel, &manifest_bytes, None)?;
    read_all_row_groups_from_manifest(kernel, &manifest)
}

/// Slab-aware row group reader from an already-loaded manifest.
///
/// Separates slab-backed RGs (uses range reads) from standalone RGs
/// (uses batch blob reads), then reassembles in manifest order.
/// Handles PSLB v2 compressed slabs (checks compression flag per slab).
fn read_all_row_groups_from_manifest(
    kernel: &PondKernel,
    manifest: &CollectionManifest,
) -> Result<Vec<Vec<u8>>, String> {
    if manifest.row_groups.is_empty() {
        return Err("Manifest has no row groups".to_string());
    }
    let refs: Vec<&crate::manifest::RowGroupEntry> = manifest.row_groups.iter().collect();
    read_rgs_slab_aware_with_decompress(kernel, &refs)
}

/// Shared slab-aware reader: separates slab/standalone RGs, does range reads,
/// and decompresses if the slab uses PSLB v2 zstd compression.
fn read_rgs_slab_aware_with_decompress(
    kernel: &PondKernel,
    row_groups: &[&crate::manifest::RowGroupEntry],
) -> Result<Vec<Vec<u8>>, String> {
    use std::collections::HashMap;

    let mut standalone_hashes: Vec<String> = Vec::new();
    let mut slab_ranges: Vec<(String, u64, u64)> = Vec::new();
    let mut rg_order: Vec<RgSource> = Vec::new();

    for rg in row_groups {
        if let (Some(offset), Some(len)) = (rg.slab_byte_offset, rg.slab_byte_len) {
            slab_ranges.push((rg.blob_hash.clone(), offset, offset + len as u64));
            rg_order.push(RgSource::Slab(slab_ranges.len() - 1));
        } else {
            standalone_hashes.push(rg.blob_hash.clone());
            rg_order.push(RgSource::Standalone(standalone_hashes.len() - 1));
        }
    }

    let standalone_results = if !standalone_hashes.is_empty() {
        kernel.read_blob_batch(&standalone_hashes)
            .map_err(|e| format!("Failed to read data blobs: {}", e))?
    } else {
        Vec::new()
    };

    // Check compression flag for each unique slab (read first 6 bytes).
    let mut slab_compressed: HashMap<String, bool> = HashMap::new();
    for (hash, _, _) in &slab_ranges {
        if !slab_compressed.contains_key(hash) {
            let header = kernel.read_blob_range(hash, 0, 6)
                .map_err(|e| format!("Failed to read slab header for {}: {}", hash, e))?;
            slab_compressed.insert(hash.clone(),
                header.len() >= 6 && &header[0..4] == b"PSLB" && (header[5] & 0x02) != 0);
        }
    }

    let slab_results = if !slab_ranges.is_empty() {
        read_slab_ranges_parallel(kernel, &slab_ranges)?
    } else {
        Vec::new()
    };

    // Decompress compressed slab RGs and reassemble in manifest order.
    let mut result = Vec::with_capacity(rg_order.len());
    for src in &rg_order {
        match src {
            RgSource::Standalone(idx) => result.push(standalone_results[*idx].clone()),
            RgSource::Slab(idx) => {
                let mut data = slab_results[*idx].clone();
                if slab_compressed.get(&slab_ranges[*idx].0).copied().unwrap_or(false) {
                    data = crate::slab::decompress_rg(&data)?;
                }
                result.push(data);
            }
        }
    }
    Ok(result)
}

/// Internal enum tracking whether each RG comes from a standalone blob or a slab.
enum RgSource {
    Standalone(usize),
    Slab(usize),
}

// ---------------------------------------------------------------------------
// PMAN v3 — Two-level manifest tree read support
// ---------------------------------------------------------------------------

/// Resolve manifest bytes to a flat `CollectionManifest`, handling both v2 and v3.
///
/// For v2 manifests, decodes directly. For v3 root manifests, fetches only
/// SURVIVING leaves (after key-range pruning if predicates are provided)
/// in parallel via `read_blob_batch`, then merges their row groups
/// into a single flat `CollectionManifest`.
///
/// CRITICAL PERFORMANCE FIX (architecture review finding):
/// Without predicates, all leaves are fetched. With predicates, we prune
/// leaves by key_min/key_max BEFORE issuing I/O. At PB scale (8K leaves),
/// a 1% selective query prunes from 8K → ~80 leaf fetches (100x reduction).
fn resolve_manifest(
    kernel: &PondKernel,
    manifest_bytes: &[u8],
    predicates: Option<&[(String, String, Vec<u8>)]>,
) -> Result<CollectionManifest, String> {
    match pman_version(manifest_bytes) {
        Some(3) => {
            let root = RootManifest::decode(manifest_bytes)
                .ok_or_else(|| "Failed to decode PMAN v3 root manifest".to_string())?;

            if root.leaves.is_empty() {
                return Err("Root manifest has no leaves".to_string());
            }

            // CRITICAL: prune leaves by key range BEFORE fetching (not after).
            // This reduces leaf manifest GETs by 100x for selective queries.
            let surviving_indices = if let Some(preds) = predicates {
                root.prune_leaves(preds)
            } else {
                (0..root.leaves.len()).collect::<Vec<usize>>()
            };

            // Batch-fetch only surviving leaf manifests in parallel
            let leaf_hashes: Vec<String> = surviving_indices.iter()
                .map(|&i| root.leaves[i].leaf_hash.clone())
                .collect();
            let leaf_blobs = kernel.read_blob_batch(&leaf_hashes)
                .map_err(|e| format!("Failed to read leaf manifests: {}", e))?;

            // Decode each leaf and merge row groups
            let mut all_rgs = Vec::new();
            let columns = root.columns.clone();
            let key_col = root.key_col.clone();

            for (i, leaf_bytes) in leaf_blobs.iter().enumerate() {
                let leaf = CollectionManifest::decode(leaf_bytes)
                    .ok_or_else(|| {
                        format!("Failed to decode leaf manifest {} ({})",
                                i, root.leaves[i].leaf_hash)
                    })?;
                all_rgs.extend(leaf.row_groups);
            }

            let mut flat = CollectionManifest::new(columns, key_col);
            for rg in all_rgs {
                flat.add_row_group(rg);
            }
            Ok(flat)
        }
        Some(1) | Some(2) => {
            // PMAN v1 or v2 — decode directly
            CollectionManifest::decode(manifest_bytes)
                .ok_or_else(|| "Failed to decode PMAN manifest".to_string())
        }
        _ => {
            Err("Unknown manifest format (not PMAN)".to_string())
        }
    }
}

// ---------------------------------------------------------------------------
// G5: Range Coalescing
// ---------------------------------------------------------------------------

/// Maximum gap (bytes) to tolerate when coalescing ranges in the same slab
/// into a single Range GET.
///
/// PSLB slabs pack RGs sequentially with a 4-byte `rg_len` prefix between
/// them. Consecutive RGs in the manifest have a 4-byte gap between the end
/// of one RG's data and the start of the next. Setting this ≥ 4 merges
/// consecutive RGs into one Range GET.
///
/// **Full-scan impact**: 1024 RGs in one slab → 1 Range GET (vs 1024).
/// **Selective impact**: surviving RGs separated by small pruned regions are
/// also merged, trading bandwidth for fewer round-trips. 1 MB extra data on
/// 10 Gbps costs ~0.8 ms vs 20-50 ms S3 RTT — a 25-60x win per merge.
const COALESCE_GAP_TOLERANCE: u64 = 8;

/// Result of coalescing multiple ranges from the same slab into one range.
struct CoalescedRange {
    /// The slab's content hash.
    slab_hash: String,
    /// Merged byte range: `[start, end)` (half-open, exclusive end).
    start: u64,
    end: u64,
    /// Per-original-range split instructions: `(original_index, offset_within_coalesced, len)`.
    /// After fetching the coalesced bytes, slice `data[offset..offset+len]` to
    /// recover each original RG's bytes.
    splits: Vec<(usize, usize, usize)>,
}

/// Coalesce byte ranges that share the same slab hash and are within
/// `gap_tolerance` bytes of each other.
///
/// **Algorithm**:
/// 1. Group ranges by slab hash.
/// 2. Sort each group by start offset.
/// 3. Merge consecutive ranges whose gap ≤ `gap_tolerance`.
/// 4. Record split instructions to re-extract each original range later.
///
/// Returns the coalesced ranges. The caller issues one Range GET per
/// coalesced range, then uses `CoalescedRange::splits` to recover per-RG
/// byte blobs from the (larger) response.
fn coalesce_ranges(
    ranges: &[(String, u64, u64)],
    gap_tolerance: u64,
) -> Vec<CoalescedRange> {
    if ranges.is_empty() {
        return Vec::new();
    }

    use std::collections::BTreeMap;

    // Group by slab hash, preserving original indices.
    let mut by_slab: BTreeMap<&str, Vec<(usize, u64, u64)>> = BTreeMap::new();
    for (i, (hash, start, end)) in ranges.iter().enumerate() {
        by_slab.entry(hash.as_str()).or_default().push((i, *start, *end));
    }

    let mut result = Vec::new();

    for (hash, entries) in &by_slab {
        // Sort by start offset within this slab.
        let mut sorted: Vec<(usize, u64, u64)> = entries.clone();
        sorted.sort_by_key(|e| e.1);

        // Merge pass: walk sorted ranges, extending the current coalesced
        // range as long as the next range starts within `gap_tolerance`.
        let mut merge_start = sorted[0].1;
        let mut merge_end = sorted[0].2;
        let mut splits: Vec<(usize, usize, usize)> = Vec::new();

        splits.push((
            sorted[0].0,                                          // original index
            0,                                                     // offset within coalesced range
            (sorted[0].2 - sorted[0].1) as usize,                 // length
        ));

        for &(orig_idx, r_start, r_end) in &sorted[1..] {
            if r_start <= merge_end + gap_tolerance {
                // Coalesce: extend the current merged range and record split.
                let offset_in_coalesced = (r_start - merge_start) as usize;
                splits.push((orig_idx, offset_in_coalesced, (r_end - r_start) as usize));
                merge_end = merge_end.max(r_end);
            } else {
                // Gap too large — emit current coalesced range, start new one.
                result.push(CoalescedRange {
                    slab_hash: hash.to_string(),
                    start: merge_start,
                    end: merge_end,
                    splits: std::mem::take(&mut splits),
                });
                merge_start = r_start;
                merge_end = r_end;
                splits.push((orig_idx, 0, (r_end - r_start) as usize));
            }
        }

        // Emit the last coalesced range for this slab.
        result.push(CoalescedRange {
            slab_hash: hash.to_string(),
            start: merge_start,
            end: merge_end,
            splits,
        });
    }

    result
}

/// Max parallel range reads. 32 saturates a 10 Gbps link to S3 without
/// hitting rate limits or exhausting connection pools. Prevents thread exhaustion
/// at PB scale where thousands of slab ranges could otherwise spawn thousands
/// of threads simultaneously.
const MAX_PARALLEL_RANGE_READS: usize = 32;

/// Parallel range-read multiple byte ranges from (possibly different) slabs.
///
/// **G5 range coalescing**: before issuing any I/O, adjacent ranges in the
/// same slab are merged into single Range GETs. For a full-scan of 1024 RGs
/// in one slab, this turns 1024 Range GETs into 1. For selective queries,
/// surviving RGs separated by small gaps (≤ `COALESCE_GAP_TOLERANCE`) are
/// also merged, trading minimal bandwidth for fewer RTTs.
///
/// Uses `std::thread::scope` for zero-overhead parallelism.
/// On S3, each range-read is an HTTP Range GET (206 Partial Content).
/// On LocalFS, each is a seek+read_exact (avoids loading the whole file).
fn read_slab_ranges_parallel(
    kernel: &PondKernel,
    ranges: &[(String, u64, u64)],
) -> Result<Vec<Vec<u8>>, String> {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};
    use std::thread;

    let n = ranges.len();
    if n == 0 { return Ok(Vec::new()); }

    // --- G5: coalesce adjacent ranges in the same slab ---
    let coalesced = coalesce_ranges(ranges, COALESCE_GAP_TOLERANCE);
    let n_coalesced = coalesced.len();

    let coalesced_results = Arc::new(Mutex::new(vec![Vec::new(); n_coalesced]));
    let failed = Arc::new(AtomicBool::new(false));
    let error_msg = Arc::new(Mutex::new(None::<String>));

    // Bounded parallelism via sync_channel as a semaphore.
    // Pre-fill the channel with MAX_PARALLEL_RANGE_READS permits.
    // Each thread acquires a permit (recv, blocks if 32 running),
    // does the range read, then sends the permit back (unblocks next).
    let (tx, rx) = std::sync::mpsc::sync_channel::<()>(MAX_PARALLEL_RANGE_READS);
    for _ in 0..MAX_PARALLEL_RANGE_READS {
        tx.send(()).unwrap();
    }
    let tx = Arc::new(tx);

    thread::scope(|s| {
        for (i, cr) in coalesced.iter().enumerate() {
            if failed.load(Ordering::Relaxed) { break; }
            // Acquire a permit (blocks if 32 threads already running).
            rx.recv().expect("permit channel closed unexpectedly");
            let hash = cr.slab_hash.clone();
            let start = cr.start;
            let end = cr.end;
            let results = Arc::clone(&coalesced_results);
            let failed = Arc::clone(&failed);
            let error_msg = Arc::clone(&error_msg);
            let tx = Arc::clone(&tx);
            s.spawn(move || {
                if failed.load(Ordering::Relaxed) {
                    let _ = tx.send(()); // release permit
                    return;
                }
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
                let _ = tx.send(()); // release permit for next thread
            });
        }
    });

    let err = error_msg.lock().unwrap();
    match &*err {
        Some(e) => Err(e.clone()),
        None => {
            let guard = coalesced_results.lock().unwrap();
            // --- G5: split coalesced results back into per-RG blobs ---
            let mut results = vec![Vec::new(); n];
            for (cr, cr_data) in coalesced.iter().zip(guard.iter()) {
                for &(orig_idx, offset, len) in &cr.splits {
                    results[orig_idx] = cr_data[offset..offset + len].to_vec();
                }
            }
            Ok(results)
        }
    }
}

/// Slab-aware reader for a pre-selected set of surviving row groups.
///
/// Used by `read_rows_i64()` after predicate pruning. Delegates to
/// `read_rgs_slab_aware_with_decompress` which handles both compressed
/// and uncompressed slabs.
fn read_surviving_rgs_slab_aware(
    kernel: &PondKernel,
    surviving_rgs: &[&crate::manifest::RowGroupEntry],
) -> Result<Vec<Vec<u8>>, String> {
    read_rgs_slab_aware_with_decompress(kernel, surviving_rgs)
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
    let manifest = resolve_manifest(kernel, &manifest_bytes, None)?;

    // Read ALL row groups (slab-aware) and concatenate.
    // Previous impl only read the first RG — silent data loss for >1 RG.
    if manifest.row_groups.is_empty() {
        return Err("Manifest has no row groups".to_string());
    }

    // Use read_all_row_groups which handles slab-backed RGs via range reads
    let blobs = read_all_row_groups_from_manifest(kernel, &manifest)?;
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
    let kernel_clone1 = kernel.clone();
    let mb_clone = manifest_bytes.clone();
    let manifest = tokio::task::spawn_blocking(move || {
        resolve_manifest(&kernel_clone1, &mb_clone, None)
    }).await.map_err(|e| format!("join error: {}", e))?
      .map_err(|e| e.to_string())?;

    // 4. Read ALL row groups (slab-aware) — delegates to sync reader
    // via spawn_blocking. The sync reader uses range reads for slab-backed
    // RGs (1000x less data transfer on S3) and batch reads for standalone.
    if manifest.row_groups.is_empty() {
        return Err("Manifest has no row groups".to_string());
    }
    let kernel_clone = kernel.clone();
    let manifest_clone = manifest.clone();
    let blobs = tokio::task::spawn_blocking(move || {
        read_all_row_groups_from_manifest(&kernel_clone, &manifest_clone)
    }).await.map_err(|e| format!("join error: {}", e))?
      .map_err(|e| e.to_string())?;
    let mut out = Vec::new();
    for b in &blobs {
        out.extend_from_slice(b);
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
    let kernel_clone1 = kernel.clone();
    let mb_clone = manifest_bytes.clone();
    let manifest = tokio::task::spawn_blocking(move || {
        resolve_manifest(&kernel_clone1, &mb_clone, None)
    }).await.map_err(|e| format!("join error: {}", e))?
      .map_err(|e| e.to_string())?;

    // Read ALL row groups (slab-aware) — delegates to sync reader
    // via spawn_blocking for range-read support.
    if manifest.row_groups.is_empty() {
        return Err("Manifest has no row groups".to_string());
    }
    let kernel_clone = kernel.clone();
    let manifest_clone = manifest.clone();
    let blobs = tokio::task::spawn_blocking(move || {
        read_all_row_groups_from_manifest(&kernel_clone, &manifest_clone)
    }).await.map_err(|e| format!("join error: {}", e))?
      .map_err(|e| e.to_string())?;
    let mut out = Vec::new();
    for b in &blobs {
        out.extend_from_slice(b);
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

    // Check if HEAD is a PondPack blob.
    // Optimization (G6): first fetch only 4 bytes to check the magic,
    // avoiding a full GET for the common case (plain commit JSON,
    // typically ~200 bytes). PondPack blobs have PNPK magic at offset 0.
    let head_magic = kernel.read_blob_range(&head, 0, 4)
        .map_err(|e| format!("Failed to read HEAD magic: {}", e))?;
    let is_pack = head_magic.len() >= 4 && &head_magic[..4] == b"PNPK";

    let manifest_bytes = if is_pack {
        // PondPack — need the full HEAD blob to extract manifest
        let head_data = kernel.read_blob(&head)
            .map_err(|e| format!("Failed to read HEAD: {}", e))?;
        let (commit, manifest_bytes, _inline) = crate::pond_pack::decode_pack(&head_data)
            .ok_or_else(|| "Failed to decode PondPack".to_string())?;
        let _ = commit; // commit metadata not needed for read
        manifest_bytes
    } else {
        // Plain commit — read commit JSON (typically ~200 bytes), then manifest
        let commit = commit::read_commit(kernel, &head)
            .ok_or_else(|| "Failed to read HEAD commit".to_string())?;

        if commit.manifest.is_empty() {
            return Err("HEAD commit has no manifest".to_string());
        }

        kernel.read_blob(&commit.manifest)
            .map_err(|e| format!("Failed to read manifest: {}", e))?
    };

    // Decode manifest (handles v2 flat and v3 tree)
    // Pass predicates for v3 leaf pruning: skip leaves whose key range
    // doesn't intersect the predicate. At PB scale (8K leaves), this
    // reduces leaf manifest GETs by ~100x for selective queries.
    let pman_preds: Option<Vec<(String, String, Vec<u8>)>> = predicates.map(|preds| {
        preds.iter().map(|(col, op, val)| {
            (col.to_string(), op.to_string(), val.to_le_bytes().to_vec())
        }).collect()
    });
    let manifest = resolve_manifest(kernel, &manifest_bytes, pman_preds.as_deref())?;

    // Build projection set (which columns to decode)
    let projection: Option<std::collections::HashSet<&str>> = columns.map(|cols| {
        cols.iter().map(|s| s.as_str()).collect()
    });

    // Collect results: column_name → Vec<i64>
    use std::collections::HashMap;
    let mut result_cols: HashMap<String, Vec<i64>> = HashMap::new();

    // Read each row group, applying predicate pruning, with slab-aware
    // range reads for minimum S3 round-trips.
    //
    // Architecture review finding G1 (CRITICAL): the previous impl did a
    // full kernel.read_blob() per RG, ignoring slab_byte_offset/slab_byte_len.
    // For 82 surviving RGs in 8 slabs at PB scale, this transferred 1 GB
    // instead of 10.5 MB. Fixed: now uses get_blob_range() for slab-backed RGs.
    //
    // G5 (range coalescing): adjacent RGs in the same slab are merged into a
    // single Range GET before I/O, then split apart after. For a full-scan of
    // 1024 RGs in one slab this turns 1024 Range GETs into 1.

    // 1. Predicate pruning — collect surviving RGs
    let surviving_rgs: Vec<&crate::manifest::RowGroupEntry> = manifest.row_groups.iter()
        .filter(|rg| {
            if let Some(preds) = predicates {
                for (col_name, op, value) in preds {
                    if let Some(stats) = rg.columns.iter().find(|c| c.name == *col_name) {
                        if stats.can_prune(op, value.to_le_bytes().as_ref()) {
                            return false;
                        }
                    }
                }
            }
            true
        })
        .collect();

    if surviving_rgs.is_empty() {
        // All RGs pruned — return empty columns from schema
        let mut result: Vec<(String, Vec<i64>)> = Vec::new();
        for (name, vtype) in &manifest.columns {
            if *vtype == pond_core::VT_INT64 {
                if let Some(ref proj) = projection {
                    if proj.contains(name.as_str()) {
                        result.push((name.clone(), Vec::new()));
                    }
                } else {
                    result.push((name.clone(), Vec::new()));
                }
            }
        }
        result.sort_by(|a, b| a.0.cmp(&b.0));
        return Ok(result);
    }

    // 2. Read surviving RGs (slab-aware)
    let blob_data_list = read_surviving_rgs_slab_aware(kernel, &surviving_rgs)?;

    // 3. Decode each PND2 blob and project columns
    for blob_data in &blob_data_list {
        let cols = pond_core::pnd2_decode(blob_data)
            .map_err(|e| format!("Failed to decode PND2 blob: {}", e))?;

        for col in &cols {
            let name = col.name.to_string_lossy().to_string();
            if let Some(ref proj) = projection {
                if !proj.contains(name.as_str()) {
                    continue;
                }
            }
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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::UnifiedStorage;
    use crate::write;

    // ------------------------------------------------------------------
    // G5 coalesce_ranges unit tests
    // ------------------------------------------------------------------

    #[test]
    fn test_coalesce_empty() {
        let ranges: Vec<(String, u64, u64)> = Vec::new();
        let coalesced = coalesce_ranges(&ranges, 8);
        assert!(coalesced.is_empty());
    }

    #[test]
    fn test_coalesce_single_range() {
        let ranges = vec![
            ("h1".to_string(), 100, 200),
        ];
        let coalesced = coalesce_ranges(&ranges, 8);
        assert_eq!(coalesced.len(), 1);
        assert_eq!(coalesced[0].slab_hash, "h1");
        assert_eq!(coalesced[0].start, 100);
        assert_eq!(coalesced[0].end, 200);
        assert_eq!(coalesced[0].splits, vec![(0, 0, 100)]);
    }

    #[test]
    fn test_coalesce_adjacent_ranges_same_slab() {
        // Two consecutive RGs in a PSLB slab: RG0=[14,114), RG1=[118,218)
        // Gap = 4 bytes (the rg_len prefix). With tolerance=8, should merge.
        let ranges = vec![
            ("slab_a".to_string(), 14, 114),
            ("slab_a".to_string(), 118, 218),
        ];
        let coalesced = coalesce_ranges(&ranges, 8);
        assert_eq!(coalesced.len(), 1, "two adjacent RGs should coalesce into 1");
        assert_eq!(coalesced[0].start, 14);
        assert_eq!(coalesced[0].end, 218);
        // RG0: offset 0, len 100. RG1: offset 104 (118-14), len 100.
        assert_eq!(coalesced[0].splits, vec![(0, 0, 100), (1, 104, 100)]);
    }

    #[test]
    fn test_coalesce_many_adjacent_ranges() {
        // 1024 consecutive RGs packed in one slab with 4-byte gaps.
        // Each RG is 100 bytes. Offsets: 14, 118, 222, 326, ...
        let mut ranges = Vec::new();
        let rg_len: u64 = 100;
        let header_len: u64 = 10;
        for i in 0..1024u64 {
            let offset = header_len + 4 + i * (4 + rg_len); // first RG at 14
            ranges.push(("big_slab".to_string(), offset, offset + rg_len));
        }
        let coalesced = coalesce_ranges(&ranges, 8);
        assert_eq!(coalesced.len(), 1, "1024 adjacent RGs should coalesce into 1 Range GET");
        assert_eq!(coalesced[0].splits.len(), 1024);
        // Verify first and last splits.
        assert_eq!(coalesced[0].splits[0], (0, 0, 100));
        let last_offset = (1023 * (4 + rg_len)) as usize; // 1023 * 104 = 106392
        assert_eq!(coalesced[0].splits[1023], (1023, last_offset, 100));
    }

    #[test]
    fn test_coalesce_different_slabs_not_merged() {
        // Ranges from different slabs must NOT coalesce.
        let ranges = vec![
            ("slab_x".to_string(), 14, 114),
            ("slab_y".to_string(), 14, 114),
        ];
        let coalesced = coalesce_ranges(&ranges, 8);
        assert_eq!(coalesced.len(), 2, "different slabs must stay separate");
    }

    #[test]
    fn test_coalesce_large_gap_not_merged() {
        // Two RGs in the same slab separated by a large gap (e.g., a pruned RG).
        // Gap = 500 bytes. Tolerance = 8. Should NOT merge.
        let ranges = vec![
            ("slab_a".to_string(), 14, 114),
            ("slab_a".to_string(), 614, 714),  // gap = 500 bytes
        ];
        let coalesced = coalesce_ranges(&ranges, 8);
        assert_eq!(coalesced.len(), 2, "large gap should prevent coalescing");
    }

    #[test]
    fn test_coalesce_large_gap_with_high_tolerance() {
        // Same as above but with tolerance=1000 — should merge.
        let ranges = vec![
            ("slab_a".to_string(), 14, 114),
            ("slab_a".to_string(), 614, 714),  // gap = 500 bytes
        ];
        let coalesced = coalesce_ranges(&ranges, 1000);
        assert_eq!(coalesced.len(), 1, "tolerance=1000 should bridge the 500-byte gap");
        assert_eq!(coalesced[0].start, 14);
        assert_eq!(coalesced[0].end, 714);
        assert_eq!(coalesced[0].splits[0], (0, 0, 100));
        assert_eq!(coalesced[0].splits[1], (1, 600, 100)); // offset 614-14=600
    }

    #[test]
    fn test_coalesce_preserves_original_order_in_splits() {
        // Ranges arrive out of order (e.g., from scattered surviving RGs).
        // Coalescing must still map each split back to the correct original index.
        let ranges = vec![
            ("s".to_string(), 100, 200),  // orig idx 0
            ("s".to_string(), 300, 400),  // orig idx 1
            ("s".to_string(), 200, 300),  // orig idx 2 — between 0 and 1
        ];
        let coalesced = coalesce_ranges(&ranges, 8);
        // After sorting by offset: 100-200, 200-300, 300-400. All adjacent → 1 coalesced.
        assert_eq!(coalesced.len(), 1);
        assert_eq!(coalesced[0].splits.len(), 3);
        // Verify each split maps to the right original index.
        let split_origins: Vec<usize> = coalesced[0].splits.iter().map(|(i,_,_)| *i).collect();
        assert_eq!(split_origins, vec![0, 2, 1]); // sorted by offset
        // Verify offsets are correct.
        assert_eq!(coalesced[0].splits[0], (0, 0, 100));      // [100,200) → offset 0
        assert_eq!(coalesced[0].splits[1], (2, 100, 100));     // [200,300) → offset 100
        assert_eq!(coalesced[0].splits[2], (1, 200, 100));     // [300,400) → offset 200
    }

    // ------------------------------------------------------------------
    // Existing read-path tests
    // ------------------------------------------------------------------

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
