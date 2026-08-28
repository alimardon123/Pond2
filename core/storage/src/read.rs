// Read module — read data from collections
//
// FAITHFUL PORT of Python UnifiedStorage's read / read_at_snapshot methods.

use crate::branch_ref;
use crate::commit;
use crate::manifest::{CollectionManifest, RootManifest, pman_version};
use crate::shard;
use pond_kernel::PondKernel;
use serde_json::Value as JsonValue;

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

    // Load the manifest (handles both v2 flat and v3 tree)
    // Uses resolve_manifest_bytes which handles PNPK packs transparently
    let manifest_bytes = commit::resolve_manifest_bytes(kernel, &head)
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
    // JOURNAL-AWARE (ARCHITECTURE.md D3): snapshot + live entry packs, RG
    // byte vectors concatenated in (writer, seq) entry order. See
    // read_rows_json_pruned for the CRDT-merge variant; this raw-RG API
    // has no merge semantics — concatenation is the union.
    let view = crate::journal::resolve_view(kernel, collection, branch, false)?;
    let mut packs: Vec<String> = Vec::with_capacity(view.entries.len() + 1);
    if let Some(snapshot) = &view.snapshot {
        packs.push(snapshot.clone());
    }
    packs.extend(view.entries.iter().map(|e| e.pack_hash.clone()));
    if packs.is_empty() {
        return Err(format!("Collection '{}' has no commits", collection));
    }
    let mut out: Vec<Vec<u8>> = Vec::new();
    for pack_hash in &packs {
        let manifest_bytes = commit::resolve_manifest_bytes(kernel, pack_hash)
            .map_err(|e| format!("Failed to read manifest: {}", e))?;
        let manifest = resolve_manifest(kernel, &manifest_bytes, None)?;
        // An empty manifest (e.g. a legacy write() of empty data) simply
        // contributes no RGs — not an error in the union path.
        if manifest.row_groups.is_empty() {
            continue;
        }
        out.extend(read_all_row_groups_from_manifest(kernel, &manifest)?);
    }
    Ok(out)
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
    read_rgs_slab_aware_with_decompress_inner(kernel, row_groups)
}

// ---------------------------------------------------------------------------
// Bloom filter pre-check for slab-backed RGs
// ---------------------------------------------------------------------------

/// Check if a slab's bloom filter can definitively rule out a set of
/// equality predicates. Returns `true` if the slab should be SKIPPED
/// (bloom miss — value is definitely not in this slab).
///
/// This uses the suffix-read primitive to read only the slab header (10 bytes),
/// tail (12 bytes), and footer (~100-500 bytes) — ~3 small I/Os total,
/// regardless of slab size. For warm queries, the memory cache serves these.
///
/// # Cost
///   - Cold: 2-3 small S3 RTTs (10 + 12 + ~500 bytes)
///   - Warm: 0 RTTs (memory cache)
///
/// # Savings
///   - Skips ALL RG reads for the slab on bloom miss
///   - For a 128-RG slab, saves 128 range reads (~128 KB of data)
fn slab_bloom_should_skip(
    kernel: &PondKernel,
    slab_hash: &str,
    predicates: &[(String, String, Vec<u8>)],
) -> bool {
    // Only bloom helps for equality predicates (=, in)
    let has_eq = predicates.iter().any(|(_, op, _)| op == "=" || op == "in");
    if !has_eq {
        return false;
    }

    // Read slab header (10 bytes) to check magic + PSLB_FLAG_HAS_BLOOM.
    // Canonical header read: ALWAYS [0, PSLB_HEADER_LEN) so the block-cache
    // key matches the compression-flag read in
    // read_rgs_slab_aware_with_decompress_inner (one GET + one cache entry).
    let header = match kernel.read_blob_range(slab_hash, 0, crate::slab::PSLB_HEADER_LEN as u64) {
        Ok(h) if h.len() >= 6 => h,
        _ => return false, // Can't read — don't prune (safe)
    };

    // Verify PSLB magic, version, and bloom flag.
    if &header[0..4] != crate::slab::PSLB_MAGIC {
        return false; // Not a slab (e.g. a standalone PND2 with same hash)
    }
    if header[4] != crate::slab::PSLB_VERSION {
        return false;
    }
    if (header[5] & crate::slab::PSLB_FLAG_HAS_BLOOM) == 0 {
        return false; // Slab has no bloom filter
    }

    // Read tail (last 12 bytes) to get footer_offset.
    let tail = match kernel.read_blob_suffix(slab_hash, crate::slab::PSLB_TAIL_LEN as u64) {
        Ok(t) if t.len() == crate::slab::PSLB_TAIL_LEN => t,
        _ => return false,
    };
    let (footer_offset, valid_magic) = match crate::slab::decode_slab_tail(&tail) {
        Some(pair) => pair,
        None => return false,
    };
    if !valid_magic {
        return false;
    }

    // Read footer: from footer_offset to EOF.
    // read_blob_range clamps `end` to file length on all backends,
    // so footer_offset + 2MB safely reads footer + tail.
    // decode_slab_footer has strict bounds checking — extra bytes (tail)
    // at the end are ignored after parsing n_entries + optional bloom.
    // 2 MB covers ~1024 RG entries × 100 cols (~1 MB) + the 512 KB
    // bloom bitset ceiling (SLAB_BLOOM_CAPACITY in write.rs) with headroom.
    const MAX_FOOTER_READ: u64 = 2 * 1_048_576; // 2 MB — entries + bloom
    let footer_raw = match kernel.read_blob_range(slab_hash, footer_offset, footer_offset + MAX_FOOTER_READ) {
        Ok(f) if !f.is_empty() => f,
        _ => return false,
    };

    let footer = match crate::slab::decode_slab_footer(&footer_raw, true) {
        Some(f) => f,
        None => return false, // Truncated footer — don't prune (safe)
    };

    // Check bloom for each equality predicate.
    // A SINGLE bloom miss is definitive — skip the entire slab.
    if let Some(ref bloom) = footer.bloom {
        for (col_name, op, value) in predicates {
            if (op == "=" || op == "in") && !bloom.might_contain_col_value(col_name, value) {
                return true; // BLOOM MISS — definitely not in this slab
            }
        }
    }

    false // Bloom hit (inconclusive) or no bloom — don't skip
}

/// Shared slab-aware reader: separates slab/standalone RGs, does range reads,
/// and decompresses if the slab uses PSLB v2 zstd compression.
fn read_rgs_slab_aware_with_decompress_inner(
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
            // Same canonical range as slab_bloom_should_skip — shares the
            // block-cache entry instead of minting a second [0,6) GET.
            let header = kernel.read_blob_range(hash, 0, crate::slab::PSLB_HEADER_LEN as u64)
                .map_err(|e| format!("Failed to read slab header for {}: {}", hash, e))?;
            slab_compressed.insert(hash.clone(),
                header.len() >= 6 && &header[0..4] == crate::slab::PSLB_MAGIC
                    && (header[5] & crate::slab::PSLB_FLAG_COMPRESSED) != 0);
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
pub(crate) fn resolve_manifest(
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
    let manifest_bytes = commit::resolve_manifest_bytes(kernel, commit_hash)
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

    // 2. Read manifest bytes (handles PNPK packs transparently).
    let kernel_clone = kernel.clone();
    let head_clone = head.clone();
    let manifest_bytes = tokio::task::spawn_blocking(move || {
        commit::resolve_manifest_bytes(&kernel_clone, &head_clone)
    }).await.map_err(|e| format!("join error: {}", e))?
      .map_err(|e| format!("Failed to read manifest: {}", e))?;

    // 3. Decode manifest.
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
    let kernel_clone = kernel.clone();
    let chash = commit_hash.to_string();
    let manifest_bytes = tokio::task::spawn_blocking(move || {
        commit::resolve_manifest_bytes(&kernel_clone, &chash)
    }).await.map_err(|e| format!("join error: {}", e))?
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
    // JOURNAL-AWARE (ARCHITECTURE.md D3): the union of the snapshot pack
    // and every live journal entry, read through the same per-pack i64
    // pipeline and CONCATENATED. This layer has no CRDT merge by design —
    // write_rows_i64 data carries no _rowid/_version (dedup is the JSON
    // pipeline's job via read_rows_json_pruned), so concatenation IS the
    // correct union semantics here. It also keeps the C9 history-loss
    // fix visible at this API level: before the journal, every
    // write_rows_i64 after the first silently hid its predecessors.
    let view = crate::journal::resolve_view(kernel, collection, branch, false)?;
    let mut packs: Vec<String> = Vec::with_capacity(view.entries.len() + 1);
    if let Some(snapshot) = &view.snapshot {
        packs.push(snapshot.clone());
    }
    packs.extend(view.entries.iter().map(|e| e.pack_hash.clone()));
    if packs.is_empty() {
        return Err(format!("Collection '{}' has no commits", collection));
    }

    use std::collections::HashMap;
    let mut result_cols: HashMap<String, Vec<i64>> = HashMap::new();
    for pack_hash in &packs {
        let cols = read_rows_i64_from_head(kernel, pack_hash, columns, predicates)?;
        for (name, data) in cols {
            result_cols.entry(name).or_default().extend(data);
        }
    }
    let mut result: Vec<(String, Vec<i64>)> = result_cols.into_iter().collect();
    result.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(result)
}

/// The per-pack i64 pruned pipeline (see [`read_rows_i64`]).
fn read_rows_i64_from_head(
    kernel: &PondKernel,
    head: &str,
    columns: Option<&[String]>,
    predicates: Option<&[(&str, &str, i64)]>,
) -> Result<Vec<(String, Vec<i64>)>, String> {
    // Resolve manifest bytes from HEAD in a SINGLE code path.
    //
    // The previous "G6 magic optimization" fetched 4 bytes first to peek at
    // the PNPK magic, then read the full blob in BOTH branches — a pure
    // +1 S3 GET on every cold read. `commit::resolve_manifest_bytes`
    // reads the HEAD blob exactly once (commit JSON is ~200 B, PNPK packs
    // are small too) and fetches the manifest blob only for plain commits:
    //   plain commit: 2 GETs (HEAD + manifest) — was 3 with the magic peek
    //   PNPK pack:    1 GET (manifest is inline) — was 2 with the magic peek
    let manifest_bytes = crate::commit::resolve_manifest_bytes(kernel, head)
        .map_err(|e| format!("Failed to resolve manifest from HEAD: {}", e))?;

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
    //
    // Two-phase pruning for slab-backed RGs with equality predicates:
    //   Phase 1: Zone-map pruning (per-RG, FREE — uses manifest stats, no I/O)
    //   Phase 2: Bloom pre-check (slab-level, 2-3 small RTTs per unique slab)
    //            A bloom miss skips ALL RGs in the slab. Run ONLY on slabs
    //            that still have zone-map survivors — bloom-checking a slab
    //            whose RGs were already pruned is pure wasted I/O. Checks
    //            run in parallel (bounded, 32 concurrent) so wall-clock on
    //            multi-slab queries is ~max(RTT), not sum(RTT).
    //
    // For standalone RGs and non-equality predicates, only Phase 1 applies.
    // For warm queries, Phase 2 is free (memory cache serves header/tail/footer).

    // Phase 1: zone-map pruning (free — manifest stats only)
    let zone_map_survivors: Vec<&crate::manifest::RowGroupEntry> = manifest.row_groups.iter()
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

    // Phase 2: bloom-check only the slabs that still have zone-map survivors
    let mut bloom_skip_slabs: std::collections::HashSet<String> = std::collections::HashSet::new();
    if let Some(preds) = predicates {
        let has_eq = preds.iter().any(|(_, op, _)| *op == "=" || *op == "in");
        if has_eq {
            // Collect unique slab hashes among zone-map survivors
            let mut unique_slab_hashes: Vec<&str> = Vec::new();
            let mut seen: std::collections::HashSet<&str> = std::collections::HashSet::new();
            for rg in &zone_map_survivors {
                if rg.slab_byte_offset.is_some() && seen.insert(&rg.blob_hash) {
                    unique_slab_hashes.push(&rg.blob_hash);
                }
            }

            if !unique_slab_hashes.is_empty() {
                let bloom_preds: Vec<(String, String, Vec<u8>)> = preds.iter().map(|(col, op, val)| {
                    (col.to_string(), op.to_string(), val.to_le_bytes().to_vec())
                }).collect();

                // Parallel bloom checks (bounded by MAX_PARALLEL_RANGE_READS=32).
                // slab_bloom_should_skip is total & monotone: it only ever
                // returns true when the value is DEFINITELY absent, so
                // parallel execution preserves semantics exactly.
                let skip_flags = std::sync::Arc::new(std::sync::Mutex::new(
                    vec![false; unique_slab_hashes.len()]));
                std::thread::scope(|s| {
                    let (tx, rx) = std::sync::mpsc::sync_channel(MAX_PARALLEL_RANGE_READS);
                    for _ in 0..MAX_PARALLEL_RANGE_READS {
                        tx.send(()).unwrap();
                    }
                    let tx = std::sync::Arc::new(tx);
                    for (i, slab_hash) in unique_slab_hashes.iter().enumerate() {
                        rx.recv().unwrap(); // acquire a parallelism permit (blocks at 32)
                        let tx = std::sync::Arc::clone(&tx);
                        let flags = std::sync::Arc::clone(&skip_flags);
                        let hash = *slab_hash;
                        let preds_ref = &bloom_preds;
                        s.spawn(move || {
                            let skip = slab_bloom_should_skip(kernel, hash, preds_ref);
                            if let Ok(mut f) = flags.lock() {
                                f[i] = skip;
                            }
                            let _ = tx.send(()); // release the permit
                        });
                    }
                });
                let flags = skip_flags.lock().map_err(|_| "bloom flags mutex poisoned")?;
                for (slab_hash, &skip) in unique_slab_hashes.iter().zip(flags.iter()) {
                    if skip {
                        bloom_skip_slabs.insert(slab_hash.to_string());
                    }
                }
            }
        }
    }

    // Final survivors: zone-map pass minus bloom-negative slabs
    let surviving_rgs: Vec<&crate::manifest::RowGroupEntry> = zone_map_survivors.into_iter()
        .filter(|rg| {
            !(rg.slab_byte_offset.is_some() && bloom_skip_slabs.contains(&rg.blob_hash))
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
        let cols = pond_core::pnd2_decode_projected(blob_data, projection.as_ref())
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
// General pruned JSON row reader — ALL column types (CRITIQUE C1 fix)
//
// `read_rows_i64` pioneered the pruned pipeline but is i64-only; the pyo3
// `read_rows` and SQL executor paths decoded FULL blobs per row group with
// no pruning. This reader generalizes the pipeline to every PND2 value
// type and returns JSON rows:
//   PMAN v3 leaf pruning → zone-map pruning → parallel bloom pre-check →
//   slab-aware coalesced range reads → projection pushdown → columnar
//   row pre-filter.
// ---------------------------------------------------------------------------

/// Pruned, projection-pushed-down JSON row reader for a collection —
/// JOURNAL-AWARE (ARCHITECTURE.md D3: reads = snapshot ∪ live entries).
///
/// Resolves the branch's journal view (branch_ref snapshot pack + every
/// live journal entry discovered above its watermarks), runs the ONE
/// pruned pipeline per pack via [`read_rows_json_pruned_with_head`], then
/// CRDT-merges the rows (LWW by `_version`, total tiebreak
/// `(_version, _rowid, payload)` — CRITIQUE C10 — tombstones suppressed).
/// Shards remain the caller's CRDT responsibility (`shard::read_with_shards`
/// + merge — the python lenses still write them).
///
/// SAFETY ARGUMENT — pre-filter + merge (the invariant that makes the
/// journal union safe under predicate pushdown): predicates are a
/// CONSERVATIVE pre-filter applied PER PACK. If a snapshot copy of a row
/// is pre-filtered out but a journal entry UPDATED that row to match, the
/// entry's copy survives this merge and delivers the row — the same
/// argument the shard layer has always relied on. The AUTHORITATIVE row
/// filter still runs post-CRDT-merge in the caller (a row whose snapshot
/// version matches but whose journal update no longer matches is
/// correctly dropped by it). Callers that skip the post-merge re-check
/// see false negatives on updated rows.
///
/// A collection with no snapshot AND no entries has no rows — the caller
/// proceeds to shards (matches the old "no HEAD" behavior of treating
/// that case as empty, not as an error).
pub fn read_rows_json_pruned(
    kernel: &PondKernel,
    collection: &str,
    branch: &str,
    key_fields: &[String],
    projection: Option<&[String]>,
    predicates: &[(String, String, JsonValue)],
) -> Result<Vec<(String, JsonValue)>, String> {
    // 1. Resolve the journal view (snapshot + live entries). This is the
    //    C9 fix: HEAD-only resolution used to hide every commit after the
    //    first; the journal unions ALL committed data.
    let view = crate::journal::resolve_view(kernel, collection, branch, false)?;
    let mut packs: Vec<String> = Vec::with_capacity(view.entries.len() + 1);
    if let Some(snapshot) = &view.snapshot {
        packs.push(snapshot.clone());
    }
    packs.extend(view.entries.iter().map(|e| e.pack_hash.clone()));
    if packs.is_empty() {
        return Ok(Vec::new());
    }

    // 2. ONE pruned pipeline per pack. Each pack is exactly what
    //    read_rows_json_pruned_with_head already handles (PNPK packs and
    //    legacy JSON commits alike); a pack with zero surviving RGs simply
    //    contributes zero rows (that function returns Ok(vec![]) when
    //    every RG is pruned or the manifest is empty — it never errors on
    //    empty manifests).
    let mut all_rows: Vec<JsonValue> = Vec::new();
    for pack_hash in &packs {
        let rows = read_rows_json_pruned_with_head(
            kernel,
            pack_hash,
            key_fields,
            projection,
            predicates,
        )?;
        all_rows.extend(rows.into_iter().map(|(_, row)| row));
    }

    // 3. CRDT-merge across packs (deterministic under any pack order after
    //    the C10 total tiebreak), drop tombstones, recompute output rowids.
    let key_col = key_fields.first().map(|s| s.as_str());
    let merged = shard::merge_rows_by_rowid(&all_rows, key_col);
    let live = shard::filter_live_rows(&merged);
    Ok(live
        .into_iter()
        .map(|row| (determine_rowid_json(&row, key_fields), row))
        .collect())
}

/// Head-override variant of [`read_rows_json_pruned`]: run the SAME pruned
/// pipeline from an explicit HEAD hash (a commit hash or a PNPK pack hash;
/// `commit::resolve_manifest_bytes` handles both). Used by callers that
/// resolve HEAD through a different ref chain — e.g. the CLI's legacy
/// bare-collection-ref fallback for pre-branch data — so they cannot bypass
/// the ONE production read pipeline (ARCHITECTURE.md D4).
pub fn read_rows_json_pruned_with_head(
    kernel: &PondKernel,
    head_hash: &str,
    key_fields: &[String],
    projection: Option<&[String]>,
    predicates: &[(String, String, JsonValue)],
) -> Result<Vec<(String, JsonValue)>, String> {
    let manifest_bytes = commit::resolve_manifest_bytes(kernel, head_hash)
        .map_err(|e| format!("Failed to read manifest: {}", e))?;

    // 2. Peek the schema (v1/v2 flat or v3 root) so predicates can be
    //    classified by declared column type BEFORE the v3 leaf fetch —
    //    leaf pruning needs typed key-column predicates up front. This is
    //    an in-memory re-decode, not a second GET.
    let (schema, key_col) = peek_manifest_schema(&manifest_bytes)?;
    let typed = classify_predicates(&schema, &key_col, predicates);

    // 3. PMAN v3 leaf pruning: only typed key-column predicates are passed
    //    (see classify_predicates for why f64/string predicates never
    //    reach prune_leaves). v1/v2 manifests decode directly.
    let manifest = resolve_manifest(
        kernel,
        &manifest_bytes,
        if typed.leaf.is_empty() { None } else { Some(&typed.leaf) },
    )?;

    // 4. Phase 1 — zone-map pruning (free: manifest stats, no I/O).
    let zone_map_survivors: Vec<&crate::manifest::RowGroupEntry> = manifest.row_groups.iter()
        .filter(|rg| !rg.can_prune(&typed.zone))
        .collect();

    // 5. Phase 2 — parallel bloom pre-check (bounded, 32-way) on slabs that
    //    still have zone-map survivors, mirroring read_rows_i64.
    //    slab_bloom_should_skip is total & monotone: it only ever returns
    //    true when the value is DEFINITELY absent, so parallel execution
    //    preserves semantics exactly.
    let mut bloom_skip_slabs: std::collections::HashSet<String> = std::collections::HashSet::new();
    if !typed.bloom.is_empty() {
        let mut unique_slab_hashes: Vec<&str> = Vec::new();
        let mut seen: std::collections::HashSet<&str> = std::collections::HashSet::new();
        for rg in &zone_map_survivors {
            if rg.slab_byte_offset.is_some() && seen.insert(&rg.blob_hash) {
                unique_slab_hashes.push(&rg.blob_hash);
            }
        }

        if !unique_slab_hashes.is_empty() {
            let skip_flags = std::sync::Arc::new(std::sync::Mutex::new(
                vec![false; unique_slab_hashes.len()]));
            std::thread::scope(|s| {
                let (tx, rx) = std::sync::mpsc::sync_channel(MAX_PARALLEL_RANGE_READS);
                for _ in 0..MAX_PARALLEL_RANGE_READS {
                    tx.send(()).unwrap();
                }
                let tx = std::sync::Arc::new(tx);
                for (i, slab_hash) in unique_slab_hashes.iter().enumerate() {
                    rx.recv().unwrap(); // acquire a parallelism permit (blocks at 32)
                    let tx = std::sync::Arc::clone(&tx);
                    let flags = std::sync::Arc::clone(&skip_flags);
                    let hash = *slab_hash;
                    let preds_ref = &typed.bloom;
                    s.spawn(move || {
                        let skip = slab_bloom_should_skip(kernel, hash, preds_ref);
                        if let Ok(mut f) = flags.lock() {
                            f[i] = skip;
                        }
                        let _ = tx.send(()); // release the permit
                    });
                }
            });
            let flags = skip_flags.lock().map_err(|_| "bloom flags mutex poisoned".to_string())?;
            for (slab_hash, &skip) in unique_slab_hashes.iter().zip(flags.iter()) {
                if skip {
                    bloom_skip_slabs.insert(slab_hash.to_string());
                }
            }
        }
    }

    // Final survivors: zone-map pass minus bloom-negative slabs.
    let surviving_rgs: Vec<&crate::manifest::RowGroupEntry> = zone_map_survivors.into_iter()
        .filter(|rg| {
            !(rg.slab_byte_offset.is_some() && bloom_skip_slabs.contains(&rg.blob_hash))
        })
        .collect();

    if surviving_rgs.is_empty() {
        // Every RG pruned — nothing at HEAD can match.
        return Ok(Vec::new());
    }

    // 6. Slab-aware range reads + coalescing for survivors ONLY (handles
    //    standalone blobs, PSLB slab offsets and PSLB v2 zstd).
    let blob_data_list = read_rgs_slab_aware_with_decompress(kernel, &surviving_rgs)?;

    // 7. Projection pushdown. pnd2_decode_projected skips non-member
    //    columns entirely (no memcpy, no decode). The always-decoded set
    //    preserves the old full-decode row shape for everything downstream
    //    of the CRDT merge depends on.
    let decode_set: Option<std::collections::HashSet<&str>> = projection.map(|proj| {
        let mut set: std::collections::HashSet<&str> =
            proj.iter().map(|s| s.as_str()).collect();
        // CRDT metadata: merge key (_rowid), LWW clock (_version),
        // tombstones (_deleted), RLS tenant (_tenant).
        // Rowid fallbacks + key fields: determine_rowid probes these when
        // _rowid is absent (write_rows_no_crdt data); omitting them would
        // silently change CRDT grouping for such rows.
        for meta in ["_rowid", "_version", "_deleted", "_tenant", "_key", "id", "key"] {
            set.insert(meta);
        }
        for kf in key_fields {
            set.insert(kf.as_str());
        }
        // Predicate columns: the row-level pre-filter below needs them.
        for (col, _, _) in predicates {
            set.insert(col.as_str());
        }
        set
    });

    // 8. Decode + conservative columnar row pre-filter, then assemble JSON
    //    rows. Rows that fail the pre-filter are never converted to JSON.
    let mut rows: Vec<(String, JsonValue)> = Vec::new();
    for blob_data in &blob_data_list {
        // LENIENT SKIP for non-PND2 blobs (raw `write()` data — the legacy
        // base-snapshot path stores arbitrary bytes as an RG; every
        // pre-journal reader skipped them: the old full-scan JSON reader
        // ignored non-`[`-prefixed HEADs, `pond read` serves them raw).
        // The journal-era union fold pulls such RGs into fold manifests,
        // so the pipeline must tolerate them: they contribute ZERO rows,
        // not an error (an error would make every collection that ever
        // had a raw write unreadable through read-rows).
        if blob_data.len() < 4 || &blob_data[0..4] != b"PND2" {
            continue;
        }
        let cols = pond_core::pnd2_decode_projected(blob_data, decode_set.as_ref())
            .map_err(|e| format!("Failed to decode PND2 blob: {}", e))?;
        // Degenerate-intersection guard (tribunal r1 finding 3): if the
        // projection set matched ZERO columns of this blob, the projected
        // decode yields no columns and the RG's rows would silently
        // vanish. Fall back to a FULL decode — matching the old
        // full-decode readers' behavior for data the projection doesn't
        // know about (e.g. no-CRDT rows lacking id/_key/key).
        let cols = if cols.is_empty() {
            pond_core::pnd2_decode(blob_data)
                .map_err(|e| format!("Failed to decode PND2 blob: {}", e))?
        } else {
            cols
        };
        if predicates.is_empty() {
            rows.extend(json_rows_from_cols(&cols, key_fields, None));
        } else {
            let mask = columnar_filter_scalar(&cols, predicates);
            rows.extend(json_rows_from_cols(&cols, key_fields, Some(&mask)));
        }
    }
    Ok(rows)
}

/// Peek the schema (columns + key column) out of manifest bytes without
/// fetching leaves. Handles PMAN v1/v2 (flat) and v3 (root) formats.
fn peek_manifest_schema(
    manifest_bytes: &[u8],
) -> Result<(Vec<(String, u8)>, String), String> {
    match pman_version(manifest_bytes) {
        Some(3) => {
            let root = RootManifest::decode(manifest_bytes)
                .ok_or_else(|| "Failed to decode PMAN v3 root manifest".to_string())?;
            Ok((root.columns, root.key_col))
        }
        Some(1) | Some(2) => {
            let m = CollectionManifest::decode(manifest_bytes)
                .ok_or_else(|| "Failed to decode PMAN manifest".to_string())?;
            Ok((m.columns, m.key_col))
        }
        _ => Err("Unknown manifest format (not PMAN)".to_string()),
    }
}

/// Predicates classified against a manifest schema for stats-based pruning.
struct TypedPredicates {
    /// Zone-map-checkable comparisons: (col, op, typed LE bytes). Only
    /// INT64/FLOAT64 stats with ops `= < <= > >=` — exactly the combinations
    /// `ColumnStatsEntry::can_prune` answers definitively.
    zone: Vec<(String, String, Vec<u8>)>,
    /// INT64 equality subset — the ONLY kind a slab bloom can definitively
    /// rule out: slab blooms are built from i64 LE bytes exclusively
    /// (write_rows_i64_slab + SlabWriter insert i64 values only), so an
    /// f64/string equality that bloom-misses is a false negative, not a
    /// proof of absence.
    bloom: Vec<(String, String, Vec<u8>)>,
    /// INT64 comparisons on the manifest key column — the ONLY kind
    /// `RootManifest::prune_leaves` interprets correctly: it compares raw
    /// stats bytes as SIGNED i64, and f64 bit patterns read as signed i64
    /// invert ordering across negative values (−2.0 > −1.0 as doubles but
    /// −2.0's bits < −1.0's bits as i64), which would mis-prune leaves.
    leaf: Vec<(String, String, Vec<u8>)>,
}

/// Split caller predicates into stats-prunable classes. Predicates that
/// cannot be answered from manifest stats (unknown column, `!=`/`<>`, `in`,
/// like, is-null, mistyped values, string columns whose stats can_prune
/// ignores) are simply not pushed to the pruning layers — they still run in
/// the columnar row filter, which is type-tolerant.
fn classify_predicates(
    schema: &[(String, u8)],
    key_col: &str,
    predicates: &[(String, String, JsonValue)],
) -> TypedPredicates {
    let mut out = TypedPredicates { zone: Vec::new(), bloom: Vec::new(), leaf: Vec::new() };
    for (col, op, val) in predicates {
        let Some(vtype) = schema.iter()
            .find(|(name, _)| name == col)
            .map(|(_, t)| *t)
        else {
            continue; // not a schema column — no stats to prune with
        };
        // "==" is a synonym for "=" everywhere in this codebase; can_prune
        // matches the bare forms only.
        let op = if op == "==" { "=" } else { op.as_str() };
        if !matches!(op, "=" | "<" | "<=" | ">" | ">=") {
            continue; // min/max cannot answer !=, <>, in, like, is-null
        }
        match vtype {
            pond_core::VT_INT64 => {
                // A float-typed JSON value against an INT64 column has no
                // stats representation — skip (conservative).
                if let Some(i) = val.as_i64() {
                    let bytes = i.to_le_bytes().to_vec();
                    if col == key_col {
                        out.leaf.push((col.clone(), op.to_string(), bytes.clone()));
                    }
                    if op == "=" {
                        out.bloom.push((col.clone(), op.to_string(), bytes.clone()));
                    }
                    out.zone.push((col.clone(), op.to_string(), bytes));
                }
            }
            pond_core::VT_FLOAT64 => {
                // as_f64 accepts integral JSON numbers too, so `score > 3`
                // and `score > 3.5` both prune.
                if let Some(f) = val.as_f64() {
                    out.zone.push((col.clone(), op.to_string(), f.to_le_bytes().to_vec()));
                }
            }
            _ => {} // STRING/BINARY/VARIANT stats exist but can_prune ignores them
        }
    }
    out
}

/// Scalar comparison for the columnar pre-filter. Unknown ops keep the
/// row — the authoritative post-merge filter is type/op-tolerant, so the
/// pre-filter must never be narrower than it.
fn scalar_cmp_op<T: PartialOrd + PartialEq + ?Sized>(v: &T, target: &T, op: &str) -> bool {
    match op {
        "=" | "==" => *v == *target,
        "!=" | "<>" => *v != *target,
        ">" => *v > *target,
        ">=" => *v >= *target,
        "<" => *v < *target,
        "<=" => *v <= *target,
        _ => true, // unknown op — keep (conservative)
    }
}

/// Scalar mirror of the pyo3 `simd::columnar_filter` (bindings/python/pyo3
/// src/simd.rs) — identical comparison semantics, no SIMD. Kept in this
/// crate so pond_storage cannot depend on the pyo3 binding; it only has to
/// be conservative-correct, not fast, since it pre-filters rows that the
/// caller re-checks post-CRDT-merge.
///
/// Semantics per column vtype (mirrored exactly):
///   INT64  + i64 JSON value  → = == != <> > >= < <= ; unknown op keeps all
///   FLOAT64 + any number     → same op set; unknown op keeps all
///                              (deliberate fix: simd::columnar_filter's
///                              f64 arm dropped ALL rows on unknown ops,
///                              which the post-merge filter keeps)
///   STRING + str JSON value  → lexicographic = == != <> > >= < <= against
///                              `value.as_str().unwrap_or("")` — a non-string
///                              value compares against "" (pyo3 parity)
///   other vtypes / missing column / mistyped value → no filtering (keep)
fn columnar_filter_scalar(
    cols: &[pond_core::PondColumn],
    predicates: &[(String, String, JsonValue)],
) -> Vec<bool> {
    use pond_core::{VT_FLOAT64, VT_INT64, VT_STRING};

    let n_rows = cols.first().map(|c| c.n_values).unwrap_or(0);
    if n_rows == 0 || predicates.is_empty() {
        return vec![true; n_rows];
    }

    let mut keep_mask = vec![true; n_rows];
    for (col_name, op, value) in predicates {
        let Some(col) = cols.iter().find(|c| c.name.to_string_lossy() == col_name.as_str()) else {
            continue; // column not decoded/present — don't filter
        };

        match col.vtype {
            // Boolean: compare the 0/1 i64 payload against a JSON bool
            // (true → 1, false → 0). Zone maps never see VT_BOOLEAN
            // (classify_predicates skips it), so this is row-level only.
            pond_core::VT_BOOLEAN => {
                if let Some(target) = value.as_bool().map(|b| b as i64) {
                    for (i, v) in col.i64_data.iter().enumerate() {
                        if i >= n_rows {
                            break;
                        }
                        if keep_mask[i] {
                            keep_mask[i] = scalar_cmp_op(v, &target, op);
                        }
                    }
                }
            }
            VT_INT64 => {
                if let Some(target) = value.as_i64() {
                    for (i, v) in col.i64_data.iter().enumerate() {
                        if i >= n_rows {
                            break;
                        }
                        if keep_mask[i] {
                            keep_mask[i] = scalar_cmp_op(v, &target, op);
                        }
                    }
                }
            }
            VT_FLOAT64 => {
                if let Some(target) = value.as_f64() {
                    for (i, v) in col.f64_data.iter().enumerate() {
                        if i >= n_rows {
                            break;
                        }
                        if keep_mask[i] {
                            keep_mask[i] = scalar_cmp_op(v, &target, op);
                        }
                    }
                }
            }
            VT_STRING => {
                // Type-strict: only filter when the JSON value IS a string.
                // `unwrap_or("")` here would turn `name != 5` into
                // `cell != ""` and drop name="" rows that the authoritative
                // post-merge filter keeps — a pre-filter must never be
                // narrower than the authoritative filter (tribunal r1).
                if let Some(target) = value.as_str() {
                    for (i, s) in col.str_data.iter().enumerate() {
                        if i >= n_rows {
                            break;
                        }
                        if keep_mask[i] {
                            let cell = s.to_string_lossy();
                            keep_mask[i] = scalar_cmp_op(cell.as_ref(), target, op);
                        }
                    }
                }
            }
            _ => {} // VARIANT/BINARY/NULL — not comparable at column level
        }
    }
    keep_mask
}

/// Assemble `(rowid, JSON row)` pairs from decoded PND2 columns, skipping
/// rows the keep mask rejects. Faithful mirror of the pyo3
/// `decode_cols_to_rows_filtered` row-assembly semantics:
///   INT64 → Number, FLOAT64 → Number, STRING → String, BOOLEAN → Bool,
///   BINARY → `__bin_b64__:<base64>` (same alphabet/padding as pyo3),
///   VARIANT → the stored JSON text parsed back into a JSON value.
fn json_rows_from_cols(
    cols: &[pond_core::PondColumn],
    key_fields: &[String],
    keep_mask: Option<&[bool]>,
) -> Vec<(String, JsonValue)> {
    use pond_core::{VT_BINARY, VT_BOOLEAN, VT_FLOAT64, VT_INT64, VT_STRING};

    let mut rows = Vec::new();
    let n_rows = cols.first().map(|c| c.n_values).unwrap_or(0);

    for row_idx in 0..n_rows {
        // Pre-filtered rows are never converted to JSON — the whole point
        // of the columnar pre-filter.
        if let Some(mask) = keep_mask {
            if !mask[row_idx] {
                continue;
            }
        }

        let mut row_obj = serde_json::Map::new();
        for col in cols {
            let name = col.name.to_string_lossy().to_string();
            let val = match col.vtype {
                VT_INT64 => col.i64_data.get(row_idx)
                    .map(|v| JsonValue::Number(serde_json::Number::from(*v)))
                    .unwrap_or(JsonValue::Null),
                VT_FLOAT64 => col.f64_data.get(row_idx)
                    .and_then(|v| serde_json::Number::from_f64(*v))
                    .map(JsonValue::Number)
                    .unwrap_or(JsonValue::Null),
                VT_STRING => col.str_data.get(row_idx)
                    .map(|v| JsonValue::String(v.to_string_lossy().to_string()))
                    .unwrap_or(JsonValue::Null),
                VT_BINARY => col.bin_data.get(row_idx)
                    .map(|b| JsonValue::String(format!("__bin_b64__:{}", base64_encode(b))))
                    .unwrap_or(JsonValue::Null),
                // Boolean: PND2 stores bools as 0/1 in i64_data (the CLI's
                // legacy decode path mapped them the same way).
                VT_BOOLEAN => col.i64_data.get(row_idx)
                    .map(|v| JsonValue::Bool(*v != 0))
                    .unwrap_or(JsonValue::Null),
                // Variant: JSON-encoded string — parse back to a JSON value.
                _ => col.str_data.get(row_idx)
                    .and_then(|s| serde_json::from_str::<JsonValue>(&s.to_string_lossy()).ok())
                    .unwrap_or(JsonValue::Null),
            };
            row_obj.insert(name, val);
        }
        let row = JsonValue::Object(row_obj);
        let rowid = determine_rowid_json(&row, key_fields);
        rows.push((rowid, row));
    }
    rows
}

/// Determine the rowid for a row — mirror of the pyo3 `determine_rowid`
/// (bindings/python/pyo3 src/lib.rs): `_rowid` (str, then i64), first
/// key_field (str, then i64), `_key`/`id`/`key` (str, then i64), then a
/// DefaultHasher digest of the row JSON. The exact order matters: it
/// decides CRDT grouping for rows that lack `_rowid`.
///
/// KNOWN LIMITATION (tribunal r1 finding 4): the last-resort hash covers
/// the row AS DECODED — under projection pushdown that's the projected
/// row, so identity for _rowid-less, key-less, id-less rows differs
/// between projected and unprojected reads. Unreachable via pyo3/CLI
/// writes (both always add _rowid/_version); affects hand-written blobs
/// only. A projection-independent identity (e.g. hash of all columns,
/// ignoring the projection) would close it — tracked in CRITIQUE.md.
fn determine_rowid_json(row: &JsonValue, key_fields: &[String]) -> String {
    if let Some(r) = row.get("_rowid").and_then(|v| v.as_str()) {
        return r.to_string();
    }
    if let Some(n) = row.get("_rowid").and_then(|v| v.as_i64()) {
        return n.to_string();
    }
    if let Some(kf) = key_fields.first() {
        if let Some(s) = row.get(kf).and_then(|v| v.as_str()) {
            return s.to_string();
        }
        if let Some(n) = row.get(kf).and_then(|v| v.as_i64()) {
            return n.to_string();
        }
    }
    for fallback in ["_key", "id", "key"] {
        if let Some(s) = row.get(fallback).and_then(|v| v.as_str()) {
            return s.to_string();
        }
        if let Some(n) = row.get(fallback).and_then(|v| v.as_i64()) {
            return n.to_string();
        }
    }
    // Last resort: hash the row.
    let s = serde_json::to_string(row).unwrap_or_default();
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut hasher = DefaultHasher::new();
    s.hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

/// Standard base64 (RFC 4648, padded) — byte-identical to the pyo3
/// `base64_encode` and executor `simple_base64_encode` so `__bin_b64__`
/// round-trips produce the same strings on every read path.
fn base64_encode(data: &[u8]) -> String {
    const CHARS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut result = String::with_capacity(data.len().div_ceil(3) * 4);
    for chunk in data.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = if chunk.len() > 1 { chunk[1] as u32 } else { 0 };
        let b2 = if chunk.len() > 2 { chunk[2] as u32 } else { 0 };
        let triple = (b0 << 16) | (b1 << 8) | b2;
        result.push(CHARS[((triple >> 18) & 0x3F) as usize] as char);
        result.push(CHARS[((triple >> 12) & 0x3F) as usize] as char);
        if chunk.len() > 1 {
            result.push(CHARS[((triple >> 6) & 0x3F) as usize] as char);
        } else {
            result.push('=');
        }
        if chunk.len() > 2 {
            result.push(CHARS[(triple & 0x3F) as usize] as char);
        } else {
            result.push('=');
        }
    }
    result
}

/// Indexed point lookup: read a single row by key column using the BPTX index.
///
/// This is the **fast path** for primary-key lookups. Instead of scanning all
/// row groups, it uses the B+ tree index to locate the exact RG and row offset
/// in O(log N), then reads ONLY that one RG.
///
/// # S3 Round-Trips (cold, no cache)
///
/// | Step | Operation | RTTs |
/// |------|-----------|------|
/// | 1    | Resolve branch HEAD (ref cache hit) | 0 |
/// | 2    | Resolve manifest hash (from commit) | 1 |
/// | 3    | Read index metadata (cached) | 0 |
/// | 4    | Read BPTX header (48 bytes) | 1 |
/// | 5    | Read internal nodes (if multi-level) | 1 |
/// | 6    | Read target leaf | 1 |
/// | 7    | Read target RG data | 1 |
/// | **Total** | | **3-5** |
///
/// vs. `read_rows_i64` with a point predicate: ~7+ RTTs cold.
///
/// # Arguments
/// * `kernel` — Pond kernel
/// * `collection` — collection name
/// * `branch` — branch name (typically "main")
/// * `columns` — columns to project (None = all i64 columns)
/// * `key_column` — the indexed column name (e.g., "id")
/// * `key` — the i64 key value to look up
///
/// # Returns
/// Same as `read_rows_i64`: `Vec<(column_name, Vec<i64>)>`.
/// Returns `Err` if no index exists or the index is stale —
/// the caller should fall back to `read_rows_i64` in that case.
pub fn read_rows_i64_indexed(
    kernel: &PondKernel,
    collection: &str,
    branch: &str,
    columns: Option<&[String]>,
    key_column: &str,
    key: i64,
) -> Result<Vec<(String, Vec<i64>)>, String> {
    // Try BPTX index with staleness check
    let hit = match crate::bptx::index_lookup_checked(kernel, collection, branch, key_column, key) {
        Ok(Some(hit)) => hit,
        Ok(None) => {
            // Key not in index — return empty result (correct for point lookup)
            // But we need to return the right column schema, so fall through to
            // get the manifest columns at least.
            let commit_ref = branch_ref(collection, branch);
            let commit_hash = kernel.resolve(&commit_ref)
                .ok_or_else(|| format!("Collection '{}' has no commits", collection))?;
            let manifest_bytes = commit::resolve_manifest_bytes(kernel, &commit_hash)
                .map_err(|e| format!("Failed to read manifest: {}", e))?;
            let manifest = resolve_manifest(kernel, &manifest_bytes, None)?;
            let projection: Option<std::collections::HashSet<&str>> =
                columns.map(|cols| cols.iter().map(|s| s.as_str()).collect());
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
        Err(_) => {
            // No index or stale index — caller must fall back to full scan
            return Err("no_fresh_bptx_index".to_string());
        }
    };

    // We have an IndexHit — read ONLY the specific RG
    let commit_ref = branch_ref(collection, branch);
    let commit_hash = kernel.resolve(&commit_ref)
        .ok_or_else(|| format!("Collection '{}' has no commits", collection))?;
    let manifest_bytes = commit::resolve_manifest_bytes(kernel, &commit_hash)
        .map_err(|e| format!("Failed to read manifest: {}", e))?;
    let manifest = resolve_manifest(kernel, &manifest_bytes, None)?;

    // Bounds check
    let rg = manifest.row_groups.get(hit.rg_index as usize)
        .ok_or_else(|| format!("BPTX rg_index {} out of range ({} RGs)",
            hit.rg_index, manifest.row_groups.len()))?;

    // Read only this one RG
    let blob_data = if let (Some(off), Some(len)) = (rg.slab_byte_offset, rg.slab_byte_len) {
        kernel.read_blob_range(&rg.blob_hash, off, off + len as u64)
            .map_err(|e| format!("Failed to read slab range for indexed RG: {}", e))?
    } else {
        kernel.read_blob(&rg.blob_hash)
            .map_err(|e| format!("Failed to read blob for indexed RG: {}", e))?
    };

    // Decode and project
    let projection: Option<std::collections::HashSet<&str>> =
        columns.map(|cols| cols.iter().map(|s| s.as_str()).collect());
    let mut result_cols: std::collections::HashMap<String, Vec<i64>> = std::collections::HashMap::new();

    let cols = pond_core::pnd2_decode_projected(&blob_data, projection.as_ref())
        .map_err(|e| format!("Failed to decode PND2 for indexed RG: {}", e))?;

    for col in &cols {
        let name = col.name.to_string_lossy().to_string();
        if let Some(ref proj) = projection {
            if !proj.contains(name.as_str()) {
                continue;
            }
        }
        if col.vtype == pond_core::VT_INT64 {
            // Extract only the single row at hit.row_offset
            let val = col.i64_data.get(hit.row_offset as usize)
                .copied()
                .unwrap_or_default();
            result_cols.entry(name).or_default().push(val);
        }
    }

    // Add projected columns that exist in schema but not in this RG's decoded data
    for (name, vtype) in &manifest.columns {
        if *vtype == pond_core::VT_INT64 && !result_cols.contains_key(name) {
            if let Some(ref proj) = projection {
                if proj.contains(name.as_str()) {
                    result_cols.insert(name.clone(), Vec::new());
                }
            }
        }
    }

    let mut result: Vec<(String, Vec<i64>)> = result_cols.into_iter().collect();
    result.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(result)
}

/// Indexed range scan: read rows by key range [start_key, end_key] using BPTX.
///
/// This is the **fast path for range queries**. Instead of scanning all row
/// groups, it uses the B+ tree index to identify only the relevant row groups
/// and row offsets, then reads ONLY those RGs.
///
/// # S3 Round-Trips (cold, no cache)
///
/// | Step | Operation | RTTs |
/// |------|-----------|------|
/// | 1 | Resolve HEAD + check staleness | 2 |
/// | 2 | Read index metadata (cached) | 0 |
/// | 3 | Read full BPTX blob (single GET) | 1 |
/// | 4 | Read unique RGs (slab-aware) | 1-K |
///
/// vs. `read_rows_i64` with predicates: O(N) RG reads where N = total RGs.
///
/// # Arguments
/// * `kernel` — Pond kernel
/// * `collection` — collection name
/// * `branch` — branch name
/// * `columns` — columns to project (None = all i64 columns)
/// * `key_column` — the indexed column name
/// * `start_key` — inclusive lower bound
/// * `end_key` — inclusive upper bound
///
/// # Returns
/// `Err("no_fresh_bptx_index")` if no index or stale — caller falls back to full scan.
pub fn read_rows_i64_range_indexed(
    kernel: &PondKernel,
    collection: &str,
    branch: &str,
    columns: Option<&[String]>,
    key_column: &str,
    start_key: i64,
    end_key: i64,
) -> Result<Vec<(String, Vec<i64>)>, String> {
    // 1. Use BPTX range scan with staleness check
    let hits = crate::bptx::range_scan_checked(
        kernel, collection, branch, key_column, start_key, end_key,
    )?;

    if hits.is_empty() {
        // No matching keys — return empty columns with correct schema
        let commit_ref = branch_ref(collection, branch);
        let commit_hash = kernel.resolve(&commit_ref)
            .ok_or_else(|| format!("Collection '{}' has no commits", collection))?;
        let manifest_bytes = commit::resolve_manifest_bytes(kernel, &commit_hash)
            .map_err(|e| format!("Failed to read manifest: {}", e))?;
        let manifest = resolve_manifest(kernel, &manifest_bytes, None)?;
        let projection: Option<std::collections::HashSet<&str>> =
            columns.map(|cols| cols.iter().map(|s| s.as_str()).collect());
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

    // 2. Load manifest for RG metadata and schema
    let commit_ref = branch_ref(collection, branch);
    let commit_hash = kernel.resolve(&commit_ref)
        .ok_or_else(|| format!("Collection '{}' has no commits", collection))?;
    let manifest_bytes = commit::resolve_manifest_bytes(kernel, &commit_hash)
        .map_err(|e| format!("Failed to read manifest: {}", e))?;
    let manifest = resolve_manifest(kernel, &manifest_bytes, None)?;

    let projection: Option<std::collections::HashSet<&str>> =
        columns.map(|cols| cols.iter().map(|s| s.as_str()).collect());

    // 3. Deduplicate RG indices and read only unique RGs (slab-aware)
    let mut unique_rg_indices: std::collections::BTreeSet<u32> = std::collections::BTreeSet::new();
    for hit in &hits {
        unique_rg_indices.insert(hit.rg_index);
    }

    // Read unique RGs
    let mut rg_data_map: std::collections::HashMap<u32, Vec<u8>> = std::collections::HashMap::new();
    for &rg_idx in &unique_rg_indices {
        let rg = manifest.row_groups.get(rg_idx as usize)
            .ok_or_else(|| format!("BPTX rg_index {} out of range ({} RGs)",
                rg_idx, manifest.row_groups.len()))?;

        let blob_data = if let (Some(off), Some(len)) = (rg.slab_byte_offset, rg.slab_byte_len) {
            kernel.read_blob_range(&rg.blob_hash, off, off + len as u64)
                .map_err(|e| format!("Failed to read slab range for indexed RG: {}", e))?
        } else {
            kernel.read_blob(&rg.blob_hash)
                .map_err(|e| format!("Failed to read blob for indexed RG: {}", e))?
        };
        rg_data_map.insert(rg_idx, blob_data);
    }

    // 4. Extract values at the indicated row offsets, maintaining key order
    // Build a map: rg_index → decoded columns
    let mut decoded_map: std::collections::HashMap<u32, Vec<pond_core::PondColumn>> = std::collections::HashMap::new();
    for (&rg_idx, blob_data) in &rg_data_map {
        let cols = pond_core::pnd2_decode_projected(blob_data, projection.as_ref())
            .map_err(|e| format!("Failed to decode PND2 for indexed RG: {}", e))?;
        decoded_map.insert(rg_idx, cols);
    }

    // Accumulate results in hit order (which is key-ascending from range scan)
    let mut result_cols: std::collections::HashMap<String, Vec<i64>> = std::collections::HashMap::new();
    for hit in &hits {
        if let Some(cols) = decoded_map.get(&hit.rg_index) {
            for col in cols {
                let name = col.name.to_string_lossy().to_string();
                if let Some(ref proj) = projection {
                    if !proj.contains(name.as_str()) {
                        continue;
                    }
                }
                if col.vtype == pond_core::VT_INT64 {
                    let val = col.i64_data.get(hit.row_offset as usize)
                        .copied()
                        .unwrap_or_default();
                    result_cols.entry(name).or_default().push(val);
                }
            }
        }
    }

    // Add projected columns that exist in schema but had no data
    for (name, vtype) in &manifest.columns {
        if *vtype == pond_core::VT_INT64 && !result_cols.contains_key(name) {
            if let Some(ref proj) = projection {
                if proj.contains(name.as_str()) {
                    result_cols.insert(name.clone(), Vec::new());
                }
            }
        }
    }

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

    // ------------------------------------------------------------------
    // read_rows_json_pruned — the general pruned JSON read pipeline
    // ------------------------------------------------------------------

    /// Byte-counting ObjectStore wrapper — measures the bytes the read path
    /// actually transfers (the ACCEPTANCE.md ≤10%-of-full-scan budget is
    /// measured through this). Counts only blob payloads (get_blob /
    /// get_blob_range / get_blob_suffix / get_blob_batch); ref-path reads
    /// are identical on both sides of every comparison, so they cancel.
    /// `list_dirs_calls` counts journal writer-discovery LISTs (the C2
    /// warm-path budget primitive) — zero on a TTL-warm read.
    struct CountingStore {
        inner: pond_kernel::LocalFSObjectStore,
        bytes_read: std::sync::Arc<std::sync::atomic::AtomicU64>,
        list_dirs_calls: std::sync::Arc<std::sync::atomic::AtomicU64>,
    }

    impl CountingStore {
        fn new(dir: &std::path::Path) -> Self {
            Self {
                inner: pond_kernel::LocalFSObjectStore::new(dir).unwrap(),
                bytes_read: std::sync::Arc::new(
                    std::sync::atomic::AtomicU64::new(0)),
                list_dirs_calls: std::sync::Arc::new(
                    std::sync::atomic::AtomicU64::new(0)),
            }
        }
    }

    impl pond_kernel::ObjectStore for CountingStore {
        fn put_blob(&self, data: &[u8]) -> std::io::Result<String> {
            self.inner.put_blob(data)
        }
        fn get_blob(&self, hash: &str) -> std::io::Result<Vec<u8>> {
            let d = self.inner.get_blob(hash)?;
            self.bytes_read.fetch_add(d.len() as u64,
                std::sync::atomic::Ordering::SeqCst);
            Ok(d)
        }
        fn get_blob_batch(&self, hashes: &[String]) -> std::io::Result<Vec<Vec<u8>>> {
            let results = self.inner.get_blob_batch(hashes)?;
            let total: usize = results.iter().map(|r| r.len()).sum();
            self.bytes_read.fetch_add(total as u64,
                std::sync::atomic::Ordering::SeqCst);
            Ok(results)
        }
        fn put_path(&self, path: &str, hash: &str) -> std::io::Result<()> {
            self.inner.put_path(path, hash)
        }
        fn get_path(&self, path: &str) -> Option<String> {
            self.inner.get_path(path)
        }
        fn delete_path(&self, path: &str) -> std::io::Result<bool> {
            self.inner.delete_path(path)
        }
        fn list_paths(&self, prefix: &str) -> std::io::Result<Vec<String>> {
            self.inner.list_paths(prefix)
        }
        fn list_dirs(&self, prefix: &str) -> std::io::Result<Vec<String>> {
            // Delegate AND count — journal writer discovery goes through
            // this primitive (ARCHITECTURE.md D3); the count is the C2
            // warm-path budget metric (zero LISTs on a TTL-warm read).
            self.list_dirs_calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.inner.list_dirs(prefix)
        }
        fn store_id(&self) -> String {
            self.inner.store_id()
        }
        fn blob_exists(&self, hash: &str) -> bool {
            self.inner.blob_exists(hash)
        }
        fn delete_blob(&self, hash: &str) -> std::io::Result<bool> {
            self.inner.delete_blob(hash)
        }
        fn get_blob_range(&self, hash: &str, start: u64, end: u64) -> std::io::Result<Vec<u8>> {
            let d = self.inner.get_blob_range(hash, start, end)?;
            self.bytes_read.fetch_add(d.len() as u64,
                std::sync::atomic::Ordering::SeqCst);
            Ok(d)
        }
        fn get_blob_suffix(&self, hash: &str, n: u64) -> std::io::Result<Vec<u8>> {
            let d = self.inner.get_blob_suffix(hash, n)?;
            self.bytes_read.fetch_add(d.len() as u64,
                std::sync::atomic::Ordering::SeqCst);
            Ok(d)
        }
    }

    /// Write a collection whose manifest holds K standalone (non-slab) row
    /// groups of mixed typed columns — mirrors write_rows_inner's manifest/
    /// commit layout but with one RG per batch. Used to exercise the pruned
    /// reader against multi-RG collections of every PND2 value type.
    fn write_multi_rg_typed(
        kernel: &PondKernel,
        collection: &str,
        branch: &str,
        rgs: &[Vec<(&str, pond_core::TypedColumn)>],
        message: &str,
    ) -> Result<String, String> {
        use crate::manifest::{CollectionManifest, ColumnStatsEntry, RowGroupEntry};

        assert!(!rgs.is_empty());
        let schema: Vec<(String, u8)> = rgs[0].iter()
            .map(|(name, col)| (name.to_string(), col.vtype()))
            .collect();
        let key_col = rgs[0].first()
            .map(|(name, _)| name.to_string())
            .unwrap_or_default();
        let mut manifest = CollectionManifest::new(schema, key_col);

        for (i, rg_cols) in rgs.iter().enumerate() {
            let blob = pond_core::pnd2_encode_multi_typed(rg_cols);
            let blob_hash = kernel.write(&blob).map_err(|e| e.to_string())?;
            let n_rows = rg_cols.first().map(|(_, c)| c.len()).unwrap_or(0) as u32;
            let col_stats: Vec<ColumnStatsEntry> = rg_cols.iter().map(|(name, col)| {
                let (min, max) = col.min_max_bytes()
                    .map(|(mn, mx)| (Some(mn), Some(mx)))
                    .unwrap_or((None, None));
                ColumnStatsEntry {
                    name: name.to_string(),
                    value_type: col.vtype(),
                    min,
                    max,
                    null_count: 0,
                }
            }).collect();
            manifest.add_row_group(RowGroupEntry {
                key: format!("rg_{:010}", i),
                blob_hash,
                n_rows,
                columns: col_stats,
                slab_byte_offset: None,
                slab_byte_len: None,
            });
        }

        let manifest_bytes = manifest.encode();
        let manifest_hash = kernel.write(&manifest_bytes).map_err(|e| e.to_string())?;
        let parent = kernel.resolve(&branch_ref(collection, branch));
        let parent_index = parent.as_ref()
            .and_then(|p| crate::commit::read_commit(kernel, p))
            .map(|c| c.index + 1)
            .unwrap_or(0);
        let commit_hash = crate::commit::write_commit(
            kernel, collection, &manifest_hash, parent.as_deref(), None,
            message, parent_index,
        ).map_err(|e| e.to_string())?;
        kernel.reference(&branch_ref(collection, branch), &commit_hash)
            .map_err(|e| e.to_string())?;
        Ok(commit_hash)
    }

    /// Emulate the OLD pyo3 HEAD read path: resolve HEAD + manifest, then
    /// one FULL `read_blob` per row group. This is what
    /// read_collection_as_json_rows_filtered did before the pruned-reader
    /// routing (and for slab-backed RGs it fetched the ENTIRE slab once per
    /// RG — 16 RGs in one slab = 16 full-slab GETs).
    ///
    /// Journal-aware: the "HEAD" of the emulation is the journal view's
    /// pack set (snapshot + live entries) — for the single-pack layouts
    /// these tests write, that is exactly the one pack the old path read.
    fn old_full_scan_bytes(kernel: &PondKernel, collection: &str, branch: &str) -> usize {
        let view = crate::journal::resolve_view(kernel, collection, branch, true).unwrap();
        let mut packs: Vec<String> = Vec::new();
        if let Some(snapshot) = &view.snapshot {
            packs.push(snapshot.clone());
        }
        packs.extend(view.entries.iter().map(|e| e.pack_hash.clone()));
        assert!(!packs.is_empty(), "emulated old path needs at least one pack");
        let mut total = 0usize;
        for pack_hash in &packs {
            let manifest_bytes = crate::commit::resolve_manifest_bytes(kernel, pack_hash).unwrap();
            let manifest = crate::manifest::CollectionManifest::decode(&manifest_bytes).unwrap();
            for rg in &manifest.row_groups {
                total += kernel.read_blob(&rg.blob_hash).unwrap().len();
            }
        }
        total
    }

    /// Byte-savings budget (ACCEPTANCE.md): a pruned read must transfer
    /// ≤ 10% of the bytes the old full-scan path transfers, measured via a
    /// counting ObjectStore. Standalone multi-RG layout: 24 RGs, an
    /// equality predicate on `id` leaves exactly 1 RG alive.
    #[test]
    fn test_read_rows_json_pruned_byte_savings_standalone() {
        let dir = tempfile::tempdir().unwrap();
        let store = CountingStore::new(dir.path());
        let counter = store.bytes_read.clone();
        let kernel = &PondKernel::new_with_store(Box::new(store));

        const N_RGS: usize = 24;
        const ROWS_PER_RG: usize = 100;
        // 60-char payload keeps blob bytes dominant over ref/manifest
        // overhead on both sides of the comparison.
        let mut rgs: Vec<Vec<(&str, pond_core::TypedColumn)>> = Vec::new();
        for rg in 0..N_RGS {
            let ids: Vec<i64> = (0..ROWS_PER_RG).map(|i| (rg * 1000 + i) as i64).collect();
            let payloads: Vec<String> = (0..ROWS_PER_RG)
                .map(|i| format!("payload-{}-{}", rg, "x".repeat(60 - i.to_string().len())))
                .collect();
            rgs.push(vec![
                ("id", pond_core::TypedColumn::Int64(ids)),
                ("payload", pond_core::TypedColumn::String(payloads)),
            ]);
        }
        write_multi_rg_typed(kernel, "budget", "main", &rgs, "seed").unwrap();

        // Baseline: the OLD path's bytes (manifest resolve + N full GETs).
        counter.store(0, std::sync::atomic::Ordering::SeqCst);
        let old_bytes = old_full_scan_bytes(kernel, "budget", "main");
        assert!(old_bytes > 0);

        // Pruned read: id=15_042 lives in exactly one RG (zone-map prunes
        // the other 23); the columnar filter then reduces to one row.
        counter.store(0, std::sync::atomic::Ordering::SeqCst);
        let preds = vec![("id".to_string(), "=".to_string(), serde_json::json!(15_042))];
        let rows = read_rows_json_pruned(kernel, "budget", "main", &["_rowid".to_string()], None, &preds).unwrap();
        let pruned_bytes = counter.load(std::sync::atomic::Ordering::SeqCst);

        assert_eq!(rows.len(), 1, "exactly one row matches id=15042");
        assert_eq!(rows[0].1["id"], serde_json::json!(15_042));

        let ratio = pruned_bytes as f64 / old_bytes as f64;
        assert!(ratio <= 0.10,
            "pruned read transferred {:.1}% of the old full-scan bytes \
             ({} of {} bytes) — budget is 10%",
            ratio * 100.0, pruned_bytes, old_bytes);
    }

    /// Same ≤10% budget on a SLAB-backed layout (write_rows_i64_slab): the
    /// old path fetched the ENTIRE slab once per RG (N×slab bytes); the
    /// pruned path reads header + tail + footer (bloom check) + ONE RG's
    /// byte range.
    #[test]
    fn test_read_rows_json_pruned_byte_savings_slab() {
        let dir = tempfile::tempdir().unwrap();
        let store = CountingStore::new(dir.path());
        let counter = store.bytes_read.clone();
        let kernel = &PondKernel::new_with_store(Box::new(store));

        const N_RGS: usize = 16;
        const ROWS_PER_RG: usize = 1000;
        let mut id_data: Vec<Vec<i64>> = Vec::new();
        let mut val_data: Vec<Vec<i64>> = Vec::new();
        for rg in 0..N_RGS {
            let ids: Vec<i64> = (0..ROWS_PER_RG).map(|i| (rg * 1000 + i) as i64).collect();
            let vals: Vec<i64> = ids.iter().map(|i| i * 10).collect();
            id_data.push(ids);
            val_data.push(vals);
        }
        let row_groups: Vec<Vec<(&str, &[i64])>> = (0..N_RGS)
            .map(|rg| vec![("id", id_data[rg].as_slice()), ("val", val_data[rg].as_slice())])
            .collect();
        let rg_refs: Vec<&[(&str, &[i64])]> = row_groups.iter().map(|rg| rg.as_slice()).collect();
        crate::write::write_rows_i64_slab(kernel, "slab_budget", "main", &rg_refs, "slab seed").unwrap();

        // Baseline: the OLD path — one FULL slab GET per RG. (The old path
        // couldn't even decode those bytes — PSLB slabs aren't PND2 blobs —
        // but the transfer cost is what the budget measures.)
        counter.store(0, std::sync::atomic::Ordering::SeqCst);
        let old_bytes = old_full_scan_bytes(kernel, "slab_budget", "main");
        assert!(old_bytes > 0);

        // Pruned read: id=15_500 → zone maps keep RG 15; the slab bloom
        // confirms presence (hit → no skip); one RG range is fetched.
        counter.store(0, std::sync::atomic::Ordering::SeqCst);
        let preds = vec![("id".to_string(), "=".to_string(), serde_json::json!(15_500))];
        let rows = read_rows_json_pruned(kernel, "slab_budget", "main", &["_rowid".to_string()], None, &preds).unwrap();
        let pruned_bytes = counter.load(std::sync::atomic::Ordering::SeqCst);

        assert_eq!(rows.len(), 1, "exactly one row matches id=15500");
        assert_eq!(rows[0].1["id"], serde_json::json!(15_500));
        assert_eq!(rows[0].1["val"], serde_json::json!(155_000));

        let ratio = pruned_bytes as f64 / old_bytes as f64;
        assert!(ratio <= 0.10,
            "pruned slab read transferred {:.1}% of the old full-scan bytes \
             ({} of {} bytes) — budget is 10%",
            ratio * 100.0, pruned_bytes, old_bytes);
    }

    /// A predicate that matches NOTHING prunes every RG: no data bytes are
    /// read — only the commit + manifest resolve (metadata), which is a
    /// small fraction of even a single RG's payload here.
    #[test]
    fn test_read_rows_json_pruned_no_match_reads_no_data() {
        let dir = tempfile::tempdir().unwrap();
        let store = CountingStore::new(dir.path());
        let counter = store.bytes_read.clone();
        let kernel = &PondKernel::new_with_store(Box::new(store));

        // 8 RGs × 100 rows × 60-char payloads — payload bytes dominate the
        // commit/manifest metadata so the ratio assertion is meaningful.
        let mut rgs: Vec<Vec<(&str, pond_core::TypedColumn)>> = Vec::new();
        for rg in 0..8 {
            let ids: Vec<i64> = (0..100).map(|i| (rg * 1000 + i) as i64).collect();
            let payloads: Vec<String> = (0..100)
                .map(|i| format!("nopayload-{}-{}", rg, "x".repeat(50 - i.to_string().len())))
                .collect();
            rgs.push(vec![
                ("id", pond_core::TypedColumn::Int64(ids)),
                ("payload", pond_core::TypedColumn::String(payloads)),
            ]);
        }
        write_multi_rg_typed(kernel, "nomatch", "main", &rgs, "seed").unwrap();

        counter.store(0, std::sync::atomic::Ordering::SeqCst);
        let old_bytes = old_full_scan_bytes(kernel, "nomatch", "main");
        assert!(old_bytes > 0);

        counter.store(0, std::sync::atomic::Ordering::SeqCst);
        let preds = vec![("id".to_string(), "=".to_string(), serde_json::json!(-999_999))];
        let rows = read_rows_json_pruned(kernel, "nomatch", "main", &["_rowid".to_string()], None, &preds).unwrap();
        let pruned_bytes = counter.load(std::sync::atomic::Ordering::SeqCst);

        assert!(rows.is_empty(), "no row can match id=-999999");

        // Only the commit + manifest blobs were fetched — every data blob
        // was skipped outright.
        let ratio = pruned_bytes as f64 / old_bytes as f64;
        assert!(ratio <= 0.10,
            "all-RGs-pruned read transferred {:.1}% of the old full-scan bytes \
             ({} of {} bytes)",
            ratio * 100.0, pruned_bytes, old_bytes);
    }

    /// Correctness across ALL column types: the pruned+pre-filtered result
    /// must equal the ground truth computed from the same input data, for
    /// predicates on i64 / f64 / string columns and combinations. VT_VARIANT
    /// (nested JSON incl. bools and nulls) and VT_BINARY round-trip through
    /// the row assembly.
    #[test]
    fn test_read_rows_json_pruned_mixed_types_correctness() {
        let dir = tempfile::tempdir().unwrap();
        let storage = UnifiedStorage::new_local(dir.path()).unwrap();
        let kernel = storage.kernel();

        const N_RGS: usize = 4;
        const ROWS_PER_RG: usize = 25;

        // Build the input data + the ground-truth JSON rows side by side.
        let mut rgs: Vec<Vec<(&str, pond_core::TypedColumn)>> = Vec::new();
        let mut truth: Vec<JsonValue> = Vec::new();
        let mut id = 0i64;
        for _rg in 0..N_RGS {
            let mut ids = Vec::new();
            let mut scores = Vec::new();
            let mut names = Vec::new();
            let mut metas = Vec::new();
            let mut blobs = Vec::new();
            let mut rowids = Vec::new();
            let mut versions = Vec::new();
            for _r in 0..ROWS_PER_RG {
                let score = (id as f64) * 0.25;
                let name = format!("user_{}", id);
                // Variant: mixed JSON — bools, nulls, nested objects.
                let meta = if id % 3 == 0 {
                    serde_json::json!({"flag": true, "nested": {"k": id}, "gap": null})
                } else if id % 3 == 1 {
                    serde_json::json!([id, "tag", false])
                } else {
                    serde_json::json!(id)
                };
                let blob: Vec<u8> = vec![0xDE, 0xAD, (id % 256) as u8];
                let rowid = format!("rid_{:04}", id);
                let version = format!("v_{:04}", id);

                truth.push(serde_json::json!({
                    "id": id,
                    "score": score,
                    "name": name,
                    "meta": meta,
                    "blob": format!("__bin_b64__:{}", base64_encode(&blob)),
                    "_rowid": rowid,
                    "_version": version,
                }));

                ids.push(id);
                scores.push(score);
                names.push(name);
                metas.push(meta.to_string());
                blobs.push(blob);
                rowids.push(rowid);
                versions.push(version);
                id += 1;
            }
            rgs.push(vec![
                ("id", pond_core::TypedColumn::Int64(ids)),
                ("score", pond_core::TypedColumn::Float64(scores)),
                ("name", pond_core::TypedColumn::String(names)),
                ("meta", pond_core::TypedColumn::Variant(metas)),
                ("blob", pond_core::TypedColumn::Binary(blobs)),
                ("_rowid", pond_core::TypedColumn::String(rowids)),
                ("_version", pond_core::TypedColumn::String(versions)),
            ]);
        }
        write_multi_rg_typed(kernel, "mixed", "main", &rgs, "seed").unwrap();

        let kc = vec!["_rowid".to_string()];

        // Ground truth for a predicate = truth rows whose JSON cells satisfy
        // the same scalar semantics the columnar pre-filter uses.
        let expected = |col: &str, op: &str, target: JsonValue| -> Vec<JsonValue> {
            truth.iter().filter(|row| {
                let cell = &row[col];
                match (cell, &target) {
                    (JsonValue::Number(a), JsonValue::Number(b)) => {
                        let (a, b) = (a.as_f64().unwrap(), b.as_f64().unwrap());
                        match op {
                            "=" | "==" => a == b,
                            "!=" | "<>" => a != b,
                            ">" => a > b,
                            ">=" => a >= b,
                            "<" => a < b,
                            "<=" => a <= b,
                            _ => true,
                        }
                    }
                    (JsonValue::String(a), JsonValue::String(b)) => match op {
                        "=" | "==" => a == b,
                        "!=" | "<>" => a != b,
                        ">" => a > b,
                        ">=" => a >= b,
                        "<" => a < b,
                        "<=" => a <= b,
                        _ => true,
                    },
                    _ => true,
                }
            }).cloned().collect()
        };

        let sort_rows = |mut rows: Vec<JsonValue>| -> Vec<JsonValue> {
            rows.sort_by_key(|r| r["id"].as_i64().unwrap_or(-1));
            rows
        };

        // Full scan (no predicates) equals ground truth exactly.
        let full = read_rows_json_pruned(kernel, "mixed", "main", &kc, None, &[]).unwrap();
        let full_rows: Vec<JsonValue> = full.into_iter().map(|(_, r)| r).collect();
        assert_eq!(sort_rows(full_rows), sort_rows(truth.clone()),
            "unpruned read must round-trip every column type exactly");

        let cases: Vec<(&str, &str, JsonValue)> = vec![
            ("id", "=", serde_json::json!(42)),
            ("id", ">", serde_json::json!(90)),
            ("id", "<=", serde_json::json!(10)),
            ("id", ">=", serde_json::json!(30)),
            ("score", "=", serde_json::json!(10.5)),
            ("score", ">", serde_json::json!(80.0)),
            ("name", "=", serde_json::json!("user_42")),
            ("name", "<", serde_json::json!("user_10")),
        ];
        for (col, op, val) in cases {
            let preds = vec![(col.to_string(), op.to_string(), val.clone())];
            let got = read_rows_json_pruned(kernel, "mixed", "main", &kc, None, &preds).unwrap();
            let got_rows: Vec<JsonValue> = got.into_iter().map(|(_, r)| r).collect();
            assert_eq!(
                sort_rows(got_rows),
                sort_rows(expected(col, op, val.clone())),
                "pruned read with {} {} {} must equal ground truth", col, op, val
            );
        }

        // Conjunction of predicates (AND semantics across columns).
        let preds = vec![
            ("id".to_string(), ">=".to_string(), serde_json::json!(10)),
            ("id".to_string(), "<".to_string(), serde_json::json!(20)),
        ];
        let got = read_rows_json_pruned(kernel, "mixed", "main", &kc, None, &preds).unwrap();
        let got_rows: Vec<JsonValue> = got.into_iter().map(|(_, r)| r).collect();
        assert_eq!(got_rows.len(), 10, "10 <= id < 20");
        assert!(got_rows.iter().all(|r| {
            let i = r["id"].as_i64().unwrap();
            (10..20).contains(&i)
        }));

        // !=" is row-filterable but must NOT prune RGs (all 100 rows alive,
        // every row survives the RG-level pass; the filter drops the rest).
        let preds = vec![("id".to_string(), "!=".to_string(), serde_json::json!(42))];
        let got = read_rows_json_pruned(kernel, "mixed", "main", &kc, None, &preds).unwrap();
        assert_eq!(got.len(), 99, "!= keeps every row but id=42");
    }

    /// Type-mismatched predicates must not filter (tribunal r1 finding 1):
    /// `name != 5` (string column, non-string literal) must keep EVERY row —
    /// including rows whose name is "" — because the authoritative
    /// post-merge filter treats cross-type comparisons as non-matches for
    /// `=`/`!=` and never drops them. The old `unwrap_or("")` pre-filter
    /// turned this into `cell != ""` and silently lost name="" rows.
    #[test]
    fn test_read_rows_json_pruned_type_mismatch_keeps_rows() {
        let dir = tempfile::tempdir().unwrap();
        let storage = UnifiedStorage::new_local(dir.path()).unwrap();
        let kernel = storage.kernel();

        // HEAD: two rows, one with an EMPTY string name (the row the bug ate).
        crate::write::write_rows(
            kernel, "typemismatch", "main",
            &[
                ("id", pond_core::TypedColumn::Int64(vec![1, 2])),
                ("name", pond_core::TypedColumn::String(vec![
                    "".to_string(),
                    "alice".to_string(),
                ])),
            ],
            "seed",
        ).unwrap();

        let kc = vec!["_rowid".to_string()];

        // `name != 5` — non-string literal against a string column: the
        // pre-filter must skip entirely; both rows survive to the caller.
        let preds = vec![("name".to_string(), "!=".to_string(), serde_json::json!(5))];
        let got = read_rows_json_pruned(kernel, "typemismatch", "main", &kc, None, &preds).unwrap();
        assert_eq!(got.len(), 2,
            "type-mismatched != must keep every row (incl. name=\"\")");

        // Same for every op: cross-type comparisons are never answerable at
        // the column level — conservative means keep.
        for op in ["=", "==", "<", "<=", ">", ">=", "<>"] {
            let preds = vec![("name".to_string(), op.to_string(), serde_json::json!(5))];
            let got = read_rows_json_pruned(kernel, "typemismatch", "main", &kc, None, &preds).unwrap();
            assert_eq!(got.len(), 2,
                "type-mismatched {} must keep every row", op);
        }

        // Control: a MATCHED-type predicate still filters normally.
        let preds = vec![("name".to_string(), "=".to_string(), serde_json::json!("alice"))];
        let got = read_rows_json_pruned(kernel, "typemismatch", "main", &kc, None, &preds).unwrap();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].1["name"], serde_json::json!("alice"));
    }

    /// CRDT pre-filter safety: the RG/row-level pre-filter is an I/O
    /// optimization ONLY — a HEAD row it drops is still delivered via its
    /// shard copy when a shard updated it to match (and a HEAD row it kept
    /// is removed by the authoritative post-merge filter when a shard
    /// updated it to no longer match).
    #[test]
    fn test_read_rows_json_pruned_crdt_prefilter_safety() {
        let dir = tempfile::tempdir().unwrap();
        let storage = UnifiedStorage::new_local(dir.path()).unwrap();
        let kernel = storage.kernel();

        // HEAD: ids 5, 7, 9 (write_rows auto-adds _rowid + _version).
        crate::write::write_rows(
            kernel, "crdt", "main",
            &[("id", pond_core::TypedColumn::Int64(vec![5, 7, 9]))],
            "head",
        ).unwrap();

        let kc = vec!["_rowid".to_string()];
        let full = read_rows_json_pruned(kernel, "crdt", "main", &kc, None, &[]).unwrap();
        let rowid_of = |want: i64| -> String {
            full.iter().find(|(_, r)| r["id"] == serde_json::json!(want))
                .map(|(rid, _)| rid.clone()).unwrap()
        };
        let rid7 = rowid_of(7);
        let rid9 = rowid_of(9);

        // Observe HEAD versions so the shard writes strictly-newer versions.
        let mut hlc = pond_kernel::crdt::HLC::new();
        for (_, row) in &full {
            if let Some(v) = row.get("_version").and_then(|v| v.as_str()) {
                hlc.observe(v);
            }
        }

        // Case A: HEAD row id=7 does NOT match `id = 5`; a shard updates it
        // TO match. Case B: HEAD row id=9 does not match either, but its
        // shard update moves it further away (id=99).
        let shard_rows = vec![
            serde_json::json!({"_rowid": rid7, "id": 5, "_deleted": false}),
            serde_json::json!({"_rowid": rid9, "id": 99, "_deleted": false}),
        ];
        crate::shard::upsert_shard(kernel, "crdt", "main", "upd_1", &shard_rows, Some("_rowid"), &mut hlc).unwrap();

        // The caller-side pipeline: pruned HEAD read + shard read + CRDT
        // merge + authoritative filter (mirrors pyo3 read_rows).
        let preds = vec![("id".to_string(), "=".to_string(), serde_json::json!(5))];
        let mut all_rows = read_rows_json_pruned(kernel, "crdt", "main", &kc, None, &preds).unwrap();

        let (_, shards) = crate::shard::read_with_shards(kernel, "crdt", "main");
        for (_, shard_hash) in shards {
            let data = kernel.read_blob(&shard_hash).unwrap();
            let arr: Vec<JsonValue> = serde_json::from_slice(&data).unwrap();
            for row in arr {
                let rowid = determine_rowid_json(&row, &kc);
                all_rows.push((rowid, row));
            }
        }

        // Mini CRDT merge: dedup by _rowid, latest _version wins, drop tombstones.
        let mut latest: std::collections::HashMap<String, (String, JsonValue)> =
            std::collections::HashMap::new();
        for (rowid, row) in all_rows {
            let eff = row.get("_rowid").and_then(|v| v.as_str())
                .map(|s| s.to_string()).unwrap_or_else(|| rowid.clone());
            let ver = row.get("_version").and_then(|v| v.as_str())
                .map(|s| s.to_string()).unwrap_or_default();
            match latest.get(&eff) {
                Some((existing, _)) if *existing >= ver => {}
                _ => {
                    latest.insert(eff, (ver, row));
                }
            }
        }
        let merged: Vec<JsonValue> = latest.into_values()
            .filter(|(_, row)| {
                !row.get("_deleted").and_then(|v| v.as_bool()).unwrap_or(false)
            })
            .map(|(_, row)| row)
            .filter(|row| row["id"] == serde_json::json!(5)) // authoritative filter
            .collect();

        let ids: Vec<i64> = {
            let mut v: Vec<i64> = merged.iter().map(|r| r["id"].as_i64().unwrap_or(-1)).collect();
            v.sort_unstable();
            v
        };
        // id=5 (HEAD) AND the shard-updated former id=7 row both match; the
        // former id=9 row (updated to 99) is gone.
        assert_eq!(ids, vec![5, 5],
            "shard-updated row must survive the HEAD pre-filter, and the \
             shard-unmatched row must be dropped post-merge");
    }

    /// Projection pushdown: requesting a single column decodes ONLY that
    /// column (+ the CRDT/rowid metadata the post-merge pipeline needs) —
    /// unrelated payload columns never leave the blob.
    #[test]
    fn test_read_rows_json_pruned_projection_pushdown() {
        let dir = tempfile::tempdir().unwrap();
        let storage = UnifiedStorage::new_local(dir.path()).unwrap();
        let kernel = storage.kernel();

        let rgs: Vec<Vec<(&str, pond_core::TypedColumn)>> = vec![vec![
            ("id", pond_core::TypedColumn::Int64(vec![1, 2, 3])),
            ("score", pond_core::TypedColumn::Float64(vec![1.5, 2.5, 3.5])),
            ("name", pond_core::TypedColumn::String(vec![
                "a".to_string(), "b".to_string(), "c".to_string()])),
            ("payload", pond_core::TypedColumn::String(vec![
                "x".repeat(64), "y".repeat(64), "z".repeat(64)])),
            ("_rowid", pond_core::TypedColumn::String(vec![
                "r1".to_string(), "r2".to_string(), "r3".to_string()])),
            ("_version", pond_core::TypedColumn::String(vec![
                "v1".to_string(), "v1".to_string(), "v1".to_string()])),
        ]];
        write_multi_rg_typed(kernel, "proj", "main", &rgs, "seed").unwrap();

        let kc = vec!["_rowid".to_string()];
        let projection = vec!["score".to_string()];
        let rows = read_rows_json_pruned(
            kernel, "proj", "main", &kc, Some(&projection), &[],
        ).unwrap();

        assert_eq!(rows.len(), 3);
        for (rowid, row) in &rows {
            let obj = row.as_object().unwrap();
            // The requested column survives.
            assert!(obj.contains_key("score"), "projected column must decode");
            // Unrelated payload columns are NOT decoded (pushdown happened).
            assert!(!obj.contains_key("name"), "non-requested column must not decode");
            assert!(!obj.contains_key("payload"), "non-requested column must not decode");
            // CRDT metadata stays decoded — the post-merge pipeline
            // (determine_rowid/CRDT/RLS) depends on it exactly as the old
            // full-decode path provided.
            assert!(obj.contains_key("_rowid"), "CRDT _rowid must decode");
            assert!(obj.contains_key("_version"), "CRDT _version must decode");
            assert_eq!(row["_rowid"].as_str().unwrap(), rowid.as_str(),
                "rowid must come from the decoded _rowid");
        }
        assert_eq!(rows[0].1["score"], serde_json::json!(1.5));

        // And with the predicate column outside the projection, it is still
        // decoded so the pre-filter can evaluate it.
        let preds = vec![("id".to_string(), ">=".to_string(), serde_json::json!(2))];
        let rows = read_rows_json_pruned(
            kernel, "proj", "main", &kc, Some(&projection), &preds,
        ).unwrap();
        assert_eq!(rows.len(), 2, "predicate on non-projected column still filters");
        assert!(rows.iter().all(|(_, r)| r["id"].as_i64().unwrap() >= 2));
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

    // ------------------------------------------------------------------
    // BPTX indexed read integration tests
    // ------------------------------------------------------------------

    #[test]
    fn test_read_rows_i64_indexed_hit() {
        let dir = tempfile::tempdir().unwrap();
        let storage = UnifiedStorage::new_local(dir.path()).unwrap();
        let kernel = storage.kernel();

        // Write 3 rows with ids [10, 20, 30], ages [100, 200, 300]
        let ids = vec![10i64, 20, 30];
        let ages = vec![100i64, 200, 300];
        crate::write::write_rows_i64(
            kernel, "idx_test", "main",
            &[("id", ids.as_slice()), ("age", ages.as_slice())],
            "init",
        ).unwrap();

        // Build BPTX index on "id" column
        crate::bptx::build_index_for_collection(kernel, "idx_test", "id", "main").unwrap();

        // Indexed lookup for key=20 -> should find row with id=20, age=200
        let result = read_rows_i64_indexed(
            kernel, "idx_test", "main", None, "id", 20,
        ).unwrap();
        assert_eq!(result.len(), 2);
        let id_col = result.iter().find(|(n, _)| n == "id").unwrap();
        let age_col = result.iter().find(|(n, _)| n == "age").unwrap();
        assert_eq!(id_col.1, vec![20]);
        assert_eq!(age_col.1, vec![200]);
    }

    #[test]
    fn test_read_rows_i64_indexed_miss() {
        let dir = tempfile::tempdir().unwrap();
        let storage = UnifiedStorage::new_local(dir.path()).unwrap();
        let kernel = storage.kernel();

        let ids = vec![10i64, 20, 30];
        crate::write::write_rows_i64(
            kernel, "idx_miss", "main",
            &[("id", ids.as_slice())],
            "init",
        ).unwrap();

        crate::bptx::build_index_for_collection(kernel, "idx_miss", "id", "main").unwrap();

        // Lookup for non-existent key=999
        let result = read_rows_i64_indexed(
            kernel, "idx_miss", "main", None, "id", 999,
        ).unwrap();
        let id_col = result.iter().find(|(n, _)| n == "id").unwrap();
        assert_eq!(id_col.1, Vec::<i64>::new());
    }

    #[test]
    fn test_read_rows_i64_indexed_stale_falls_back() {
        let dir = tempfile::tempdir().unwrap();
        let storage = UnifiedStorage::new_local(dir.path()).unwrap();
        let kernel = storage.kernel();

        // Write initial data
        let ids = vec![10i64, 20, 30];
        crate::write::write_rows_i64(
            kernel, "stale_test", "main",
            &[("id", ids.as_slice())],
            "v1",
        ).unwrap();

        // Build index on v1
        crate::bptx::build_index_for_collection(kernel, "stale_test", "id", "main").unwrap();

        // Write more data — this invalidates the index
        let ids2 = vec![40i64, 50];
        crate::write::write_rows_i64(
            kernel, "stale_test", "main",
            &[("id", ids2.as_slice())],
            "v2",
        ).unwrap();

        // Indexed lookup should fail because index is stale
        let result = read_rows_i64_indexed(
            kernel, "stale_test", "main", None, "id", 10,
        );
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("stale"));
    }

    #[test]
    fn test_read_rows_i64_indexed_no_index_falls_back() {
        let dir = tempfile::tempdir().unwrap();
        let storage = UnifiedStorage::new_local(dir.path()).unwrap();
        let kernel = storage.kernel();

        let ids = vec![10i64, 20, 30];
        crate::write::write_rows_i64(
            kernel, "no_idx", "main",
            &[("id", ids.as_slice())],
            "init",
        ).unwrap();

        // No index built — should return Err
        let result = read_rows_i64_indexed(
            kernel, "no_idx", "main", None, "id", 10,
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_read_rows_i64_indexed_with_projection() {
        let dir = tempfile::tempdir().unwrap();
        let storage = UnifiedStorage::new_local(dir.path()).unwrap();
        let kernel = storage.kernel();

        let ids = vec![10i64, 20];
        let ages = vec![100i64, 200];
        let scores = vec![1000i64, 2000];
        crate::write::write_rows_i64(
            kernel, "proj_test", "main",
            &[("id", ids.as_slice()), ("age", ages.as_slice()), ("score", scores.as_slice())],
            "init",
        ).unwrap();

        crate::bptx::build_index_for_collection(kernel, "proj_test", "id", "main").unwrap();

        // Project only "age" column
        let proj = vec!["age".to_string()];
        let result = read_rows_i64_indexed(
            kernel, "proj_test", "main", Some(&proj), "id", 20,
        ).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].0, "age");
        assert_eq!(result[0].1, vec![200]);
    }
}
