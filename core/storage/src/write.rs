// Write module — write data to collections
//
// Two write paths:
//   1. write() — raw bytes (JSON or any format). Simple, used by CLI.
//   2. write_rows() — structured rows encoded as PND2. Production path
//      with column stats, auto-encoding (RLE/DICT/BITPACK/RAW), and
//      proper manifest entries for pruning/projection.
//
// Both paths create a commit and update branch refs identically.

use crate::commit;
use crate::pond_pack;
use crate::manifest::{CollectionManifest, ColumnStatsEntry, LeafEntry, MAX_LEAF_RGS, RootManifest, RowGroupEntry, compute_key_range};
use crate::slab;
use crate::{branch_ref, manifest_ref};
use pond_core::{pnd2_encode_i64_auto, pnd2_encode_multi_typed, TypedColumn, VT_INT64, PND2_MAGIC, COMPRESSION_NONE, COMPRESSION_ZSTD};
use pond_kernel::PondKernel;

/// Minimum PND2 blob size (inner data) to consider zstd compression.
/// Below this, compression overhead outweighs savings.
const PND2_COMPRESS_THRESHOLD: usize = 1024;

/// Zstd compression level for PND2 blobs. Level 3 is the sweet spot
/// for columnar data: ~3x compression at ~200 MB/s encode speed.
const PND2_ZSTD_LEVEL: i32 = 3;

/// Compress a PND2 blob with zstd if the inner data exceeds the threshold.
///
/// Takes a PND2 blob with COMPRESSION_NONE, returns either:
/// - The original blob unchanged (if too small or compression doesn't help)
/// - A new blob with COMPRESSION_ZSTD and compressed inner data
///
/// PND2 header layout (13 bytes):
///   [0..4]  Magic "PND2"
///   [4]     Version
///   [5]     Flags
///   [6..10] n_rows (u32 LE)
///   [10..12] n_columns (u16 LE)
///   [12]    compression_tag
///   [13..]  inner data (schema + stats + payloads)
pub fn maybe_compress_pnd2(blob: &[u8]) -> Vec<u8> {
    if blob.len() < PND2_COMPRESS_THRESHOLD + 13 {
        return blob.to_vec();
    }
    if blob.len() < 13 || &blob[0..4] != PND2_MAGIC {
        return blob.to_vec();
    }
    if blob[12] != COMPRESSION_NONE {
        return blob.to_vec(); // already compressed
    }

    let inner = &blob[13..];
    let compressed = match zstd::encode_all(inner, PND2_ZSTD_LEVEL) {
        Ok(c) => c,
        Err(_) => return blob.to_vec(),
    };

    // Only use compression if it saves > 10%
    if compressed.len() as f64 > inner.len() as f64 * 0.9 {
        return blob.to_vec();
    }

    let mut out = Vec::with_capacity(13 + compressed.len());
    out.extend_from_slice(&blob[0..12]); // header with original compression tag
    out.push(COMPRESSION_ZSTD);         // overwrite with ZSTD tag
    out.extend_from_slice(&compressed);
    out
}

/// Write raw bytes to a collection. Creates a new commit on the active branch.
///
/// This is the simplest write path — it REPLACES the collection's data
/// (not an append). For append semantics, use shard::append_shard.
///
/// The data is stored as-is (no PND2 encoding). Use write_rows() for
/// structured data that benefits from columnar encoding + pruning.
pub fn write(
    kernel: &PondKernel,
    collection: &str,
    active_branch: &str,
    data: &[u8],
    message: &str,
) -> Result<String, String> {
    // Write the data blob
    let data_hash = kernel.write(data)
        .map_err(|e| format!("Failed to write data: {}", e))?;

    // Get parent commit (current HEAD of active branch)
    let parent = kernel.resolve(&branch_ref(collection, active_branch));
    let parent_index = parent.as_ref()
        .and_then(|p| commit::read_commit(kernel, p))
        .map(|c| c.index + 1)
        .unwrap_or(0);

    // Build a simple manifest with one row group pointing at the data blob
    let mut manifest = CollectionManifest::new(vec![], String::new());
    manifest.add_row_group(RowGroupEntry {
        key: "rg_0000000000".to_string(),
        blob_hash: data_hash.clone(),
        n_rows: 1, // raw bytes — row count unknown
        columns: vec![],
        slab_byte_offset: None,
        slab_byte_len: None,
    });
    let manifest_bytes = manifest.encode();
    let manifest_hash = kernel.write(&manifest_bytes)
        .map_err(|e| format!("Failed to write manifest: {}", e))?;

    // Write the commit
    let commit_hash = commit::write_commit(
        kernel, collection, &manifest_hash, parent.as_deref(), None,
        if message.is_empty() { "write" } else { message }, parent_index,
    ).map_err(|e| format!("Failed to write commit: {}", e))?;

    // Update branch refs
    kernel.reference(&branch_ref(collection, active_branch), &commit_hash)
        .map_err(|e| format!("Failed to update branch ref: {}", e))?;
    kernel.reference(&manifest_ref(collection, active_branch), &manifest_hash)
        .map_err(|e| format!("Failed to update manifest ref: {}", e))?;
    let _ = kernel.reference(collection, &commit_hash);

    Ok(commit_hash)
}

/// Write structured rows as a PND2 blob with proper column stats.
///
/// This is the PRODUCTION write path — it encodes rows as a PND2 blob
/// with automatic encoding selection (RLE/DICT/BITPACK/RAW per column),
/// builds a manifest with per-column stats (min/max/null_count), and
/// enables predicate pruning + projection pushdown on reads.
///
/// Args:
///   - kernel: The PondKernel handle
///   - collection: Collection name
///   - active_branch: Branch to write to
///   - columns: Column specs (name, i64 values)
///   - message: Commit message
///
/// Returns: commit hash
pub fn write_rows_i64(
    kernel: &PondKernel,
    collection: &str,
    active_branch: &str,
    columns: &[(&str, &[i64])],
    message: &str,
) -> Result<String, String> {
    let n_rows = columns.first().map(|(_, v)| v.len()).unwrap_or(0);

    // Encode as PND2 with auto-encoding per column, then compress
    let blob = pnd2_encode_i64_auto(columns);
    let blob = maybe_compress_pnd2(&blob);
    let data_hash = kernel.write(&blob)
        .map_err(|e| format!("Failed to write PND2 blob: {}", e))?;

    // Get parent commit
    let parent = kernel.resolve(&branch_ref(collection, active_branch));
    let parent_index = parent.as_ref()
        .and_then(|p| commit::read_commit(kernel, p))
        .map(|c| c.index + 1)
        .unwrap_or(0);

    // Build manifest with schema + column stats
    let schema: Vec<(String, u8)> = columns.iter()
        .map(|(name, _)| (name.to_string(), VT_INT64))
        .collect();
    let key_col = columns.first().map(|(name, _)| name.to_string()).unwrap_or_default();
    let mut manifest = CollectionManifest::new(schema, key_col);

    // Build column stats entries
    let mut col_stats: Vec<ColumnStatsEntry> = Vec::new();
    for (name, values) in columns {
        if values.is_empty() {
            col_stats.push(ColumnStatsEntry {
                name: name.to_string(),
                value_type: VT_INT64,
                min: None,
                max: None,
                null_count: 0,
            });
        } else {
            let min = *values.iter().min().unwrap();
            let max = *values.iter().max().unwrap();
            col_stats.push(ColumnStatsEntry {
                name: name.to_string(),
                value_type: VT_INT64,
                min: Some(min.to_le_bytes().to_vec()),
                max: Some(max.to_le_bytes().to_vec()),
                null_count: 0,
            });
        }
    }

    manifest.add_row_group(RowGroupEntry {
        key: "rg_0000000000".to_string(),
        blob_hash: data_hash.clone(),
        n_rows: n_rows as u32,
        columns: col_stats,
        slab_byte_offset: None,
        slab_byte_len: None,
    });

    let manifest_bytes = manifest.encode();
    let manifest_hash = kernel.write(&manifest_bytes)
        .map_err(|e| format!("Failed to write manifest: {}", e))?;

    // Write the commit
    let commit_hash = commit::write_commit(
        kernel, collection, &manifest_hash, parent.as_deref(), None,
        if message.is_empty() { "write_rows" } else { message }, parent_index,
    ).map_err(|e| format!("Failed to write commit: {}", e))?;

    // Update branch refs
    kernel.reference(&branch_ref(collection, active_branch), &commit_hash)
        .map_err(|e| format!("Failed to update branch ref: {}", e))?;
    kernel.reference(&manifest_ref(collection, active_branch), &manifest_hash)
        .map_err(|e| format!("Failed to update manifest ref: {}", e))?;
    let _ = kernel.reference(collection, &commit_hash);

    Ok(commit_hash)
}

/// Write structured rows as PND2 + PondPack (commit+manifest in ONE blob).
///
/// This is the OPTIMIZED write path — uses PondPack to combine the commit
/// JSON and manifest bytes into a single blob, saving 1 PUT per write and
/// 1-2 GETs per cold read.
///
/// Args:
///   - kernel: The PondKernel handle
///   - collection: Collection name
///   - active_branch: Branch to write to
///   - columns: Column specs (name, i64 values)
///   - message: Commit message
///
/// Returns: pack hash (HEAD ref points to pack — contains both commit + manifest)
pub fn write_rows_i64_packed(
    kernel: &PondKernel,
    collection: &str,
    active_branch: &str,
    columns: &[(&str, &[i64])],
    message: &str,
) -> Result<String, String> {
    let n_rows = columns.first().map(|(_, v)| v.len()).unwrap_or(0);

    // 1. Encode data as PND2 blob, compress if worthwhile
    let blob = maybe_compress_pnd2(&pnd2_encode_i64_auto(columns));
    let data_hash = kernel.write(&blob)
        .map_err(|e| format!("Failed to write PND2 blob: {}", e))?;

    // 2. Get parent commit
    let parent = kernel.resolve(&branch_ref(collection, active_branch));
    let parent_index = parent.as_ref()
        .and_then(|p| commit::read_commit(kernel, p))
        .map(|c| c.index + 1)
        .unwrap_or(0);

    // 3. Build manifest with schema + column stats
    let schema: Vec<(String, u8)> = columns.iter()
        .map(|(name, _)| (name.to_string(), VT_INT64))
        .collect();
    let key_col = columns.first().map(|(name, _)| name.to_string()).unwrap_or_default();
    let mut manifest = CollectionManifest::new(schema, key_col);

    let mut col_stats: Vec<ColumnStatsEntry> = Vec::new();
    for (name, values) in columns {
        if values.is_empty() {
            col_stats.push(ColumnStatsEntry {
                name: name.to_string(),
                value_type: VT_INT64,
                min: None,
                max: None,
                null_count: 0,
            });
        } else {
            let min = *values.iter().min().unwrap();
            let max = *values.iter().max().unwrap();
            col_stats.push(ColumnStatsEntry {
                name: name.to_string(),
                value_type: VT_INT64,
                min: Some(min.to_le_bytes().to_vec()),
                max: Some(max.to_le_bytes().to_vec()),
                null_count: 0,
            });
        }
    }

    manifest.add_row_group(RowGroupEntry {
        key: "rg_0000000000".to_string(),
        blob_hash: data_hash.clone(),
        n_rows: n_rows as u32,
        columns: col_stats,
        slab_byte_offset: None,
        slab_byte_len: None,
    });

    let manifest_bytes = manifest.encode();

    // 4. Build commit object
    let commit_obj = serde_json::json!({
        "parent": parent,
        "manifest": "",
        "message": if message.is_empty() { "write_rows_packed" } else { message },
        "timestamp": 0,
        "index": parent_index,
    });

    // 5. Encode as PondPack (commit JSON + manifest bytes in ONE blob)
    let pack_bytes = crate::pond_pack::encode_pack(&commit_obj, &manifest_bytes, None);
    let pack_hash = kernel.write(&pack_bytes)
        .map_err(|e| format!("Failed to write pack blob: {}", e))?;

    // 6. Update branch refs — both point to the pack hash
    kernel.reference(&branch_ref(collection, active_branch), &pack_hash)
        .map_err(|e| format!("Failed to update branch ref: {}", e))?;
    kernel.reference(&manifest_ref(collection, active_branch), &pack_hash)
        .map_err(|e| format!("Failed to update manifest ref: {}", e))?;
    let _ = kernel.reference(collection, &pack_hash);

    Ok(pack_hash)
}

/// Write typed columns (INT64, FLOAT64, STRING) as a PND2 blob.
///
/// This is the GENERIC write path — supports mixed column types.
/// Each column's encoding is chosen automatically (INT64: RLE/DICT/BITPACK/RAW,
/// FLOAT64: RAW, STRING: RAW).
///
/// **CRDT by default**: auto-adds `_rowid` (UUIDv7) and `_version` (HLC)
/// columns if not already present in the input. This makes all data
/// written via write_rows compatible with upsert_shard / delete_shard
/// (which match by _rowid). To opt out, pass `add_crdt_metadata = false`.
///
/// Args:
///   - kernel: The PondKernel handle
///   - collection: Collection name
///   - active_branch: Branch to write to
///   - columns: Column specs (name, TypedColumn)
///   - message: Commit message
///
/// Returns: commit hash
pub fn write_rows(
    kernel: &PondKernel,
    collection: &str,
    active_branch: &str,
    columns: &[(&str, TypedColumn)],
    message: &str,
) -> Result<String, String> {
    write_rows_inner(kernel, collection, active_branch, columns, message, true)
}

/// Write typed columns WITHOUT adding _rowid / _version.
///
/// Use this for raw bulk loads where you don't need CRDT compatibility
/// (e.g., immutable historical data, or data that will never be updated).
pub fn write_rows_no_crdt(
    kernel: &PondKernel,
    collection: &str,
    active_branch: &str,
    columns: &[(&str, TypedColumn)],
    message: &str,
) -> Result<String, String> {
    write_rows_inner(kernel, collection, active_branch, columns, message, false)
}

fn write_rows_inner(
    kernel: &PondKernel,
    collection: &str,
    active_branch: &str,
    columns: &[(&str, TypedColumn)],
    message: &str,
    add_crdt_metadata: bool,
) -> Result<String, String> {
    let n_rows = columns.first().map(|(_, c)| c.len()).unwrap_or(0);

    // Auto-add _rowid (UUIDv7) + _version (HLC) if requested and not present
    let has_rowid = columns.iter().any(|(name, _)| *name == "_rowid");
    let has_version = columns.iter().any(|(name, _)| *name == "_version");

    let mut final_columns: Vec<(&str, TypedColumn)> = columns.to_vec();

    if add_crdt_metadata && !has_rowid && n_rows > 0 {
        let rowids: Vec<String> = (0..n_rows).map(|_| {
            pond_kernel::crdt::uuidv7()
        }).collect();
        final_columns.push(("_rowid", TypedColumn::String(rowids)));
    }

    if add_crdt_metadata && !has_version && n_rows > 0 {
        let mut hlc = pond_kernel::crdt::HLC::new();
        let versions: Vec<String> = (0..n_rows).map(|_| {
            hlc.tick()
        }).collect();
        final_columns.push(("_version", TypedColumn::String(versions)));
    }

    // Encode as PND2 with per-type encoding, compress if worthwhile
    let blob = maybe_compress_pnd2(&pnd2_encode_multi_typed(&final_columns));
    let data_hash = kernel.write(&blob)
        .map_err(|e| format!("Failed to write PND2 blob: {}", e))?;

    // Build manifest with schema + column stats (use final_columns to include
    // the auto-added _rowid / _version columns)
    let schema: Vec<(String, u8)> = final_columns.iter()
        .map(|(name, col)| (name.to_string(), col.vtype()))
        .collect();
    let key_col = final_columns.first().map(|(name, _)| name.to_string()).unwrap_or_default();
    let mut manifest = CollectionManifest::new(schema, key_col);

    // Build column stats entries
    let col_stats: Vec<ColumnStatsEntry> = final_columns.iter()
        .map(|(name, col)| {
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
        })
        .collect();

    manifest.add_row_group(RowGroupEntry {
        key: "rg_0000000000".to_string(),
        blob_hash: data_hash.clone(),
        n_rows: n_rows as u32,
        columns: col_stats,
        slab_byte_offset: None,
        slab_byte_len: None,
    });

    let manifest_bytes = manifest.encode();

    // CAS COMMIT LOOP — closes the multi-writer lost-update hole (review bug #1).
    //
    // The old unconditional `reference()` meant two concurrent write_rows that
    // both parented off H0 would both succeed: the second PUT silently
    // overwrote the first commit's parent link, ORPHANING one commit chain
    // (rows lost from HEAD's history). reference_if() is a true CAS:
    //   - S3/R2: conditional PUT with If-Match ETag (atomic at the store)
    //   - CachingObjectStore forwards to inner + refreshes/drops its ref cache
    //   - LocalFS: read-check-write (default impl; single-process safe)
    //
    // The branch ref is the linearization point and the read path's source of
    // truth. Losers of the race re-read HEAD and rebuild their pack (the data
    // blob is content-addressed and NOT rewritten; the superseded pack blobs
    // become unreferenced garbage that GC/vacuum can reclaim).
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0);
    let b_ref = branch_ref(collection, active_branch);
    const MAX_CAS_ATTEMPTS: usize = 5;
    let mut pack_hash: Option<String> = None;
    for _attempt in 0..MAX_CAS_ATTEMPTS {
        let parent = kernel.resolve(&b_ref);
        let parent_index = parent.as_ref()
            .and_then(|p| commit::read_commit(kernel, p))
            .map(|c| c.index + 1)
            .unwrap_or(0);

        let commit_obj = serde_json::json!({
            "parent": parent,
            "second_parent": null,
            "manifest": "packed",
            "message": if message.is_empty() { "write_rows" } else { message },
            "timestamp": timestamp,
            "index": parent_index,
        });

        // Encode as PondPack (commit + manifest combined) → saves 1 S3 PUT
        let pack_bytes = pond_pack::encode_pack(&commit_obj, &manifest_bytes, None);
        let candidate = kernel.write(&pack_bytes)
            .map_err(|e| format!("Failed to write PondPack: {}", e))?;

        let won = kernel.reference_if(&b_ref, parent.as_deref(), &candidate)
            .map_err(|e| format!("Failed to CAS branch ref: {}", e))?;
        if won {
            pack_hash = Some(candidate);
            break;
        }
        // Lost the race — HEAD moved under us. Retry against the new HEAD.
    }
    let pack_hash = pack_hash.ok_or_else(|| {
        format!("Lost {} concurrent-writer races on '{}' — retry the write", MAX_CAS_ATTEMPTS, b_ref)
    })?;

    // Derived refs (legacy/compat readers): the branch ref is authoritative
    // for the read path; these are refreshed only AFTER winning the CAS so
    // they can never claim a commit that lost the linearization point.
    // manifest_ref → pack_hash (load_manifest in branch.rs handles PNPK)
    kernel.reference(&manifest_ref(collection, active_branch), &pack_hash)
        .map_err(|e| format!("Failed to update manifest ref: {}", e))?;
    let _ = kernel.reference(collection, &pack_hash);

    Ok(pack_hash)
}

/// Write multiple row-group batches as ONE PSLB slab blob.
///
/// This is the SLAB-OPTIMIZED write path — packs K row groups into a
/// single PSLB slab object, reducing S3 PUTs from K to 1 and enabling
/// HTTP Range-Read on the read side (3 RTTs instead of N×RTTs).
///
/// Each element of `row_groups` is a slice of `(name, &[i64])` column
/// specs representing one row group. All row groups must have the same
/// column names (same schema).
///
/// The manifest's RowGroupEntry entries will have `slab_byte_offset` and
/// `slab_byte_len` set, pointing into the slab blob. The read path
/// detects these fields and uses `get_blob_range()` instead of `get_blob()`.
///
/// Args:
///   - kernel: The PondKernel handle
///   - collection: Collection name
///   - active_branch: Branch to write to
///   - row_groups: Vec of column batches, each batch = one row group
///   - message: Commit message
///
/// Returns: commit hash
///
/// # Example
/// ```ignore
/// let rg1: Vec<( &str, &[i64] )> = vec![ ("id", &[1, 2, 3]), ("val", &[10, 20, 30]) ];
/// let rg2: Vec<( &str, &[i64] )> = vec![ ("id", &[4, 5, 6]), ("val", &[40, 50, 60]) ];
/// write_rows_i64_slab(kernel, "t", "main", &[rg1, rg2], "slab write");
/// // Result: 1 slab blob (not 2 individual PND2 blobs)
/// ```
pub fn write_rows_i64_slab<'a>(
    kernel: &PondKernel,
    collection: &str,
    active_branch: &str,
    row_groups: &[&[(&'a str, &'a [i64])]],
    message: &str,
) -> Result<String, String> {
    if row_groups.is_empty() {
        return Err("row_groups must not be empty".to_string());
    }

    // 1. Encode each RG as PND2 + compute column stats
    let mut encoded_rgs: Vec<(Vec<u8>, Vec<ColumnStatsEntry>, u32)> = Vec::with_capacity(row_groups.len());
    for rg in row_groups {
        let blob = maybe_compress_pnd2(&pnd2_encode_i64_auto(rg));
        let n_rows = rg.first().map(|(_, v)| v.len()).unwrap_or(0) as u32;
        let col_stats: Vec<ColumnStatsEntry> = rg.iter().map(|(name, values)| {
            let (min, max) = if values.is_empty() {
                (None, None)
            } else {
                (Some(values.iter().copied().min().unwrap().to_le_bytes().to_vec()),
                 Some(values.iter().copied().max().unwrap().to_le_bytes().to_vec()))
            };
            ColumnStatsEntry { name: name.to_string(), value_type: VT_INT64, min, max, null_count: 0 }
        }).collect();
        encoded_rgs.push((blob, col_stats, n_rows));
    }

    // 2. Build slab entries for encode_slab (now includes n_rows)
    let slab_inputs: Vec<(Vec<u8>, Vec<ColumnStatsEntry>, u32)> = encoded_rgs.iter()
        .map(|(blob, stats, n_rows)| (blob.clone(), stats.clone(), *n_rows))
        .collect();

    // 3. Encode as PSLB v2 slab (zstd-compressed per-RG), with a bloom
    //    filter when it fits the size ceiling. Compression reduces S3
    //    storage by 3-5x and transfer time proportionally. The bloom
    //    (exact-sized from the raw column values still in scope) lets
    //    equality queries skip this entire slab in 1-3 small RTTs on the
    //    read side — previously blooms were only written by tests, so
    //    production slabs paid a header RTT per slab for nothing.
    //    If the element count exceeds SLAB_BLOOM_CAPACITY, the bitset
    //    would exceed the read path's footer window and saturate to
    //    ~100% FP — skip it and rely on zone-map pruning instead.
    let mut bloom_cols: std::collections::HashMap<&str, Vec<Vec<u8>>> = std::collections::HashMap::new();
    let mut total_elements: usize = 0;
    for rg in row_groups {
        for (name, values) in rg.iter() {
            bloom_cols.entry(name).or_default()
                .extend(values.iter().map(|v| v.to_le_bytes().to_vec()));
            total_elements += values.len();
        }
    }
    let (slab_bytes, has_bloom) = if total_elements <= SLAB_BLOOM_CAPACITY {
        let bloom_col_refs: Vec<(&str, &[Vec<u8>])> = bloom_cols.iter()
            .map(|(name, vals)| (*name, vals.as_slice()))
            .collect();
        let bloom = slab::build_bloom(&bloom_col_refs);
        (slab::encode_slab_compressed_with_bloom(&slab_inputs, &bloom), true)
    } else {
        (slab::encode_slab_compressed(&slab_inputs), false)
    };
    let slab_hash = kernel.write(&slab_bytes)
        .map_err(|e| format!("Failed to write slab blob: {}", e))?;

    // 4. Decode slab footer to get exact byte offsets for each RG
    //    First get the tail to find footer_offset, then extract footer bytes.
    //    has_bloom matches whether WE embedded a bloom above.
    let tail = slab::decode_slab_tail(&slab_bytes)
        .ok_or_else(|| "Failed to decode slab tail after encode".to_string())?;
    let footer_offset = tail.0 as usize;
    let footer_end = slab_bytes.len() - slab::PSLB_TAIL_LEN;
    let footer = slab::decode_slab_footer(&slab_bytes[footer_offset..footer_end], has_bloom)
        .ok_or_else(|| "Failed to decode slab footer after encode".to_string())?;

    // 5. Get parent commit
    let parent = kernel.resolve(&branch_ref(collection, active_branch));
    let parent_index = parent.as_ref()
        .and_then(|p| commit::read_commit(kernel, p))
        .map(|c| c.index + 1)
        .unwrap_or(0);

    // 6. Build manifest with one RG entry per input RG, all pointing to the slab
    let schema: Vec<(String, u8)> = row_groups[0].iter()
        .map(|(name, _)| (name.to_string(), VT_INT64))
        .collect();
    let key_col = row_groups[0].first().map(|(name, _)| name.to_string()).unwrap_or_default();
    let mut manifest = CollectionManifest::new(schema, key_col);

    for (i, (_blob, col_stats, n_rows)) in encoded_rgs.iter().enumerate() {
        let entry = &footer.entries[i];
        manifest.add_row_group(RowGroupEntry {
            key: format!("rg_{:010}", i),
            blob_hash: slab_hash.clone(),
            n_rows: *n_rows,
            columns: col_stats.clone(),
            slab_byte_offset: Some(entry.byte_offset),
            slab_byte_len: Some(entry.byte_len),
        });
    }

    let manifest_bytes = manifest.encode();
    let manifest_hash = kernel.write(&manifest_bytes)
        .map_err(|e| format!("Failed to write manifest: {}", e))?;

    // 7. Write the commit
    let commit_hash = commit::write_commit(
        kernel, collection, &manifest_hash, parent.as_deref(), None,
        if message.is_empty() { "write_rows_slab" } else { message }, parent_index,
    ).map_err(|e| format!("Failed to write commit: {}", e))?;

    // 8. Update branch refs
    kernel.reference(&branch_ref(collection, active_branch), &commit_hash)
        .map_err(|e| format!("Failed to update branch ref: {}", e))?;
    kernel.reference(&manifest_ref(collection, active_branch), &manifest_hash)
        .map_err(|e| format!("Failed to update manifest ref: {}", e))?;
    let _ = kernel.reference(collection, &commit_hash);

    Ok(commit_hash)
}



// ---------------------------------------------------------------------------
// SlabWriter — stateful buffer that accumulates row groups into PSLB slabs
// ---------------------------------------------------------------------------

/// Target number of row groups per slab. At ~128 KB per RG, 1024 RGs ≈ 128 MB
/// — the sweet spot for S3 multipart upload + Range-Read efficiency.
const SLAB_TARGET_RG_COUNT: usize = 1024;

/// Target byte size per slab (128 MB). Triggers auto-flush even if
/// SLAB_TARGET_RG_COUNT hasn't been reached yet.
const SLAB_TARGET_BYTES: usize = 128 * 1024 * 1024;

/// Upper bound for the incremental per-slab bloom filter (elements).
/// 419,430 elements × 10 bits ≈ 4M bits = 512 KB bitset — the largest
/// bloom that keeps a full footer (entries + bloom) comfortably inside
/// the read path's MAX_FOOTER_READ window. Past the cap the false-positive
/// rate grows (bloom filters never false-negative), so pruning stays
/// CORRECT — just less selective.
const SLAB_BLOOM_CAPACITY: usize = 419_430;

/// A stateful buffer that accumulates row groups and flushes them as PSLB slabs.
///
/// This is the production write path for high-throughput workloads. Instead of
/// creating N separate S3 objects (one per `write_rows_i64` call), SlabWriter
/// buffers K row groups and writes ONE slab blob, reducing S3 PUTs from K to 1
/// and enabling HTTP Range-Read on the read side (3 RTTs instead of K×RTTs).
///
/// # Example
/// ```ignore
/// let mut sw = SlabWriter::new(kernel, "events", "main");
/// for batch in data_stream {
///     sw.write_rows_i64(&[("id", &ids), ("val", &vals)])?;
/// }
/// let commit_hash = sw.flush("load complete")?;
/// // Result: data_stream.len() RGs packed into ceil(len/1024) slab blobs
/// ```
pub struct SlabWriter<'a> {
    kernel: &'a PondKernel,
    collection: &'a str,
    active_branch: &'a str,
    /// Buffered RGs for the current (in-progress) slab.
    /// Each entry: (pnd2_bytes, col_stats, n_rows)
    buffer: Vec<(Vec<u8>, Vec<ColumnStatsEntry>, u32)>,
    buffer_bytes: usize,
    /// Completed RG entries from previous auto-flushed slabs.
    completed_rgs: Vec<RowGroupEntry>,
    /// Schema (set on first write, validated on subsequent).
    schema: Option<Vec<(String, u8)>>,
    key_col: Option<String>,
    /// Incremental bloom over the values buffered for the CURRENT slab.
    /// Sized adaptively from the first batch (rows × target RG count,
    /// clamped to [1024, SLAB_BLOOM_CAPACITY]); reset at every flush_slab.
    /// Enables read-side whole-slab pruning for equality predicates.
    slab_bloom: Option<crate::bloom::BloomFilter>,
}

impl<'a> SlabWriter<'a> {
    /// Create a new SlabWriter. Does not write anything until `flush()`.
    pub fn new(kernel: &'a PondKernel, collection: &'a str, active_branch: &'a str) -> Self {
        Self {
            kernel,
            collection,
            active_branch,
            buffer: Vec::with_capacity(SLAB_TARGET_RG_COUNT),
            buffer_bytes: 0,
            completed_rgs: Vec::new(),
            schema: None,
            key_col: None,
            slab_bloom: None,
        }
    }

    /// Buffer one row group as PND2. Auto-flushes when K RGs or 128 MB reached.
    ///
    /// All calls must have the same schema (same column names, same order).
    /// Returns an error on schema mismatch.
    pub fn write_rows_i64(&mut self, columns: &[(&str, &[i64])]) -> Result<(), String> {
        let n_rows = columns.first().map(|(_, v)| v.len()).unwrap_or(0) as u32;
        let blob = maybe_compress_pnd2(&pnd2_encode_i64_auto(columns));

        // Set/validate schema on first write
        let schema: Vec<(String, u8)> = columns.iter()
            .map(|(name, _)| (name.to_string(), VT_INT64))
            .collect();
        match &self.schema {
            None => {
                self.schema = Some(schema.clone());
                self.key_col = columns.first().map(|(n, _)| n.to_string());
            }
            Some(expected) if expected != &schema => {
                return Err(format!(
                    "SlabWriter: schema mismatch. Expected {:?}, got {:?}",
                    expected, schema
                ));
            }
            _ => {}
        }

        // Compute column stats
        let col_stats: Vec<ColumnStatsEntry> = columns.iter().map(|(name, values)| {
            let (min, max) = if values.is_empty() {
                (None, None)
            } else {
                (Some(values.iter().copied().min().unwrap().to_le_bytes().to_vec()),
                 Some(values.iter().copied().max().unwrap().to_le_bytes().to_vec()))
            };
            ColumnStatsEntry { name: name.to_string(), value_type: VT_INT64,
                min, max, null_count: 0 }
        }).collect();

        // Insert values into the incremental slab bloom BEFORE any
        // auto-flush below — the bloom must cover exactly the RGs that
        // end up in the current slab.
        if self.slab_bloom.is_none() {
            // Adaptive sizing: first batch's (rows × columns) × target RGs
            // per slab — the bloom hashes EVERY (column, value) pair —
            // clamped to the 512 KB bitset ceiling. Keeps small workloads
            // tiny and bounds the footer size at PB scale.
            let est_elements = (n_rows as usize)
                .saturating_mul(columns.len().max(1))
                .saturating_mul(SLAB_TARGET_RG_COUNT);
            if est_elements <= SLAB_BLOOM_CAPACITY {
                self.slab_bloom = Some(crate::bloom::BloomFilter::new(
                    est_elements.max(1024)));
            }
            // else: estimated elements exceed the bitset ceiling — the
            // bloom would saturate to ~100% FP and bloat the footer with
            // zero pruning power. Skip it; zone-map pruning still applies.
        }
        if let Some(ref mut bloom) = self.slab_bloom {
            for (name, values) in columns {
                for v in values.iter() {
                    bloom.insert_col_value(name, &v.to_le_bytes());
                }
            }
        }

        self.buffer_bytes += blob.len();
        self.buffer.push((blob, col_stats, n_rows));

        // Auto-flush if threshold reached
        if self.buffer.len() >= SLAB_TARGET_RG_COUNT
            || self.buffer_bytes >= SLAB_TARGET_BYTES
        {
            self.flush_slab()?;
        }
        Ok(())
    }

    /// Encode buffered RGs as one PSLB slab, write it, record RG entries.
    fn flush_slab(&mut self) -> Result<(), String> {
        if self.buffer.is_empty() { return Ok(()); }

        // Take the incremental bloom covering exactly this slab's RGs.
        // If it is somehow absent (defensive — every buffered RG inserts
        // into it), encode WITHOUT a bloom rather than embedding an empty
        // one: an empty bloom would falsely prune the whole slab on reads.
        let (slab_bytes, has_bloom) = match self.slab_bloom.take() {
            Some(bloom) => (
                slab::encode_slab_compressed_with_bloom(&self.buffer, &bloom),
                true,
            ),
            None => (slab::encode_slab_compressed(&self.buffer), false),
        };
        let slab_hash = self.kernel.write(&slab_bytes)
            .map_err(|e| format!("SlabWriter: failed to write slab: {}", e))?;

        // Decode footer for exact byte offsets
        // (has_bloom matches whether WE embedded a bloom above.)
        let tail = slab::decode_slab_tail(&slab_bytes)
            .ok_or_else(|| "SlabWriter: slab tail decode failed".to_string())?;
        let footer_end = slab_bytes.len() - slab::PSLB_TAIL_LEN;
        let footer = slab::decode_slab_footer(&slab_bytes[tail.0 as usize..footer_end], has_bloom)
            .ok_or_else(|| "SlabWriter: slab footer decode failed".to_string())?;

        for (i, (_, col_stats, n_rows)) in self.buffer.iter().enumerate() {
            let entry = &footer.entries[i];
            self.completed_rgs.push(RowGroupEntry {
                key: format!("rg_{:010}", self.completed_rgs.len()),
                blob_hash: slab_hash.clone(),
                n_rows: *n_rows,
                columns: col_stats.clone(),
                slab_byte_offset: Some(entry.byte_offset),
                slab_byte_len: Some(entry.byte_len),
            });
        }

        self.buffer.clear();
        self.buffer_bytes = 0;
        Ok(())
    }

    /// Final flush: write remaining buffered RGs as partial slab, then commit.
    ///
    /// If the total RG count exceeds `MAX_LEAF_RGS` (1024), this produces a
    /// PMAN v3 **root manifest** pointing to PMAN v2 **leaf manifests**.
    /// Each leaf holds up to 1024 RGs. This prevents manifests from growing
    /// beyond ~400 KB per leaf, keeping S3 GET latency bounded at PB scale.
    ///
    /// If ≤ 1024 RGs, produces a single PMAN v2 manifest (backward compatible).
    ///
    /// Returns the commit hash. Consumes self (cannot write more after flush).
    pub fn flush(mut self, message: &str) -> Result<String, String> {
        self.flush_slab()?; // flush partial buffer

        if self.completed_rgs.is_empty() {
            return Err("SlabWriter: no data to flush".to_string());
        }

        let schema = self.schema.clone().unwrap();
        let key_col = self.key_col.clone().unwrap_or_default();

        // Determine manifest hash: v3 root if > MAX_LEAF_RGS, v2 leaf otherwise
        let manifest_hash = if self.completed_rgs.len() > MAX_LEAF_RGS {
            self.flush_as_tree(&schema, &key_col, message)?
        } else {
            self.flush_as_flat(&schema, &key_col, message)?
        };

        Ok(manifest_hash)
    }

    /// Flat flush: single PMAN v2 manifest (backward compatible, ≤1024 RGs).
    fn flush_as_flat(
        &self,
        schema: &[(String, u8)],
        key_col: &str,
        message: &str,
    ) -> Result<String, String> {
        let mut manifest = CollectionManifest::new(schema.to_vec(), key_col.to_string());
        for rg in &self.completed_rgs {
            manifest.add_row_group(rg.clone());
        }

        let manifest_bytes = manifest.encode();
        let manifest_hash = self.kernel.write(&manifest_bytes)
            .map_err(|e| format!("SlabWriter: failed to write manifest: {}", e))?;

        let commit_hash = self.commit(&manifest_hash, message)?;
        Ok(commit_hash)
    }

    /// Tree flush: PMAN v3 root + PMAN v2 leaves (>1024 RGs).
    ///
    /// Chunks completed_rgs into leaves of MAX_LEAF_RGS each, writes each
    /// leaf as a PMAN v2 manifest, then writes a PMAN v3 root pointing to
    /// all leaves. The root is tiny (~100 B/leaf) and stays under 1 MB even
    /// at 8K leaves (8.2M RGs, 1 TB of data).
    fn flush_as_tree(
        &self,
        schema: &[(String, u8)],
        key_col: &str,
        message: &str,
    ) -> Result<String, String> {
        let mut root = RootManifest::new(schema.to_vec(), key_col.to_string());

        // Chunk RGs into leaves
        for chunk in self.completed_rgs.chunks(MAX_LEAF_RGS) {
            let mut leaf_manifest = CollectionManifest::new(schema.to_vec(), key_col.to_string());
            for rg in chunk {
                leaf_manifest.add_row_group(rg.clone());
            }

            let leaf_bytes = leaf_manifest.encode();
            let leaf_hash = self.kernel.write(&leaf_bytes)
                .map_err(|e| format!("SlabWriter: failed to write leaf manifest: {}", e))?;

            // Compute key range for this leaf
            let (key_min, key_max) = compute_key_range(chunk, key_col);
            let total_data_bytes: u64 = chunk.iter()
                .map(|rg| rg.slab_byte_len.unwrap_or(0) as u64)
                .sum();

            root.leaves.push(LeafEntry {
                leaf_hash,
                n_row_groups: chunk.len() as u32,
                total_data_bytes,
                key_min,
                key_max,
            });
        }

        // Write root manifest
        let root_bytes = root.encode();
        let root_hash = self.kernel.write(&root_bytes)
            .map_err(|e| format!("SlabWriter: failed to write root manifest: {}", e))?;

        let commit_hash = self.commit(&root_hash, message)?;
        Ok(commit_hash)
    }

    /// Shared commit flow: write commit JSON + update branch refs.
    fn commit(&self, manifest_hash: &str, message: &str) -> Result<String, String> {
        let parent = self.kernel.resolve(&branch_ref(self.collection, self.active_branch));
        let parent_index = parent.as_ref()
            .and_then(|p| commit::read_commit(self.kernel, p))
            .map(|c| c.index + 1)
            .unwrap_or(0);

        let commit_hash = commit::write_commit(
            self.kernel, self.collection, manifest_hash,
            parent.as_deref(), None,
            if message.is_empty() { "slab_write" } else { message },
            parent_index,
        ).map_err(|e| format!("SlabWriter: commit failed: {}", e))?;

        self.kernel.reference(&branch_ref(self.collection, self.active_branch), &commit_hash)
            .map_err(|e| format!("SlabWriter: branch ref failed: {}", e))?;
        self.kernel.reference(&manifest_ref(self.collection, self.active_branch), manifest_hash)
            .map_err(|e| format!("SlabWriter: manifest ref failed: {}", e))?;
        let _ = self.kernel.reference(self.collection, &commit_hash);

        Ok(commit_hash)
    }

    /// Returns the number of row groups currently buffered (not yet flushed).
    pub fn buffered_count(&self) -> usize {
        self.buffer.len()
    }

    /// Returns the total number of completed (flushed) row groups.
    pub fn completed_count(&self) -> usize {
        self.completed_rgs.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::UnifiedStorage;
    use crate::commit;

    #[test]
    fn test_maybe_compress_pnd2_small_blob_unchanged() {
        // Small blobs should not be compressed
        let small = pond_core::pnd2_encode_i64(&[1, 2, 3]);
        let result = maybe_compress_pnd2(&small);
        assert_eq!(result[12], COMPRESSION_NONE);
        assert_eq!(result.len(), small.len());
    }

    #[test]
    fn test_maybe_compress_pnd2_large_blob_compressed() {
        // Large repetitive data should compress well
        let values: Vec<i64> = (0..10_000).map(|i| i % 100).collect();
        let blob = pond_core::pnd2_encode_i64(&values);
        assert!(blob.len() > PND2_COMPRESS_THRESHOLD + 13);

        let compressed = maybe_compress_pnd2(&blob);
        assert_eq!(compressed[12], COMPRESSION_ZSTD);
        assert!(compressed.len() < blob.len(),
            "compressed {} should be < uncompressed {}", compressed.len(), blob.len());

        // Verify round-trip: decode the compressed blob
        let decoded = pond_core::pnd2_decode(&compressed).unwrap();
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].i64_data.len(), 10_000);
        assert_eq!(decoded[0].i64_data[0], 0);
        assert_eq!(decoded[0].i64_data[9999], 99);
    }

    #[test]
    fn test_maybe_compress_pnd2_non_pnd2_unchanged() {
        let random_data = vec![0u8; 100];
        let result = maybe_compress_pnd2(&random_data);
        assert_eq!(result.len(), 100);
    }

    #[test]
    fn test_write_creates_commit() {
        let dir = tempfile::tempdir().unwrap();
        let storage = UnifiedStorage::new_local(dir.path()).unwrap();
        let kernel = storage.kernel();

        let hash = write(kernel, "users", "main", b"hello world", "initial").unwrap();

        // Verify the commit exists and has the right structure
        let commit = commit::read_commit(kernel, &hash).unwrap();
        assert_eq!(commit.message, "initial");
        assert!(commit.parent.is_none()); // first commit
        assert_eq!(commit.index, 0);

        // Verify the branch ref points at the commit
        assert_eq!(
            kernel.resolve(&branch_ref("users", "main")),
            Some(hash.clone())
        );
    }

    #[test]
    fn test_write_chains_commits() {
        let dir = tempfile::tempdir().unwrap();
        let storage = UnifiedStorage::new_local(dir.path()).unwrap();
        let kernel = storage.kernel();

        let c1 = write(kernel, "users", "main", b"v1", "first").unwrap();
        let c2 = write(kernel, "users", "main", b"v2", "second").unwrap();

        // c2's parent should be c1
        let commit2 = commit::read_commit(kernel, &c2).unwrap();
        assert_eq!(commit2.parent, Some(c1));
        assert_eq!(commit2.index, 1);
    }

    #[test]
    fn test_write_rows_i64_creates_pnd2_blob() {
        let dir = tempfile::tempdir().unwrap();
        let storage = UnifiedStorage::new_local(dir.path()).unwrap();
        let kernel = storage.kernel();

        let ids = vec![1i64, 2, 3, 4, 5];
        let ages = vec![30i64, 25, 35, 40, 28];

        let hash = write_rows_i64(
            kernel, "users", "main",
            &[("id", &ids), ("age", &ages)],
            "insert 5 users",
        ).unwrap();

        // Verify commit exists
        let commit = commit::read_commit(kernel, &hash).unwrap();
        assert_eq!(commit.message, "insert 5 users");
        assert_eq!(commit.index, 0);

        // Verify the PND2 blob can be decoded
        let manifest_hash = &commit.manifest;
        let manifest_data = kernel.read_blob(manifest_hash).unwrap();
        let manifest = CollectionManifest::decode(&manifest_data).expect("manifest should decode");
        assert_eq!(manifest.row_groups.len(), 1);
        assert_eq!(manifest.row_groups[0].n_rows, 5);
        assert_eq!(manifest.row_groups[0].columns.len(), 2); // id + age

        // Verify column stats
        let id_stats = &manifest.row_groups[0].columns[0];
        assert_eq!(id_stats.name, "id");
        assert_eq!(id_stats.value_type, VT_INT64);
        let id_min = i64::from_le_bytes(id_stats.min.as_ref().unwrap()[..8].try_into().unwrap());
        let id_max = i64::from_le_bytes(id_stats.max.as_ref().unwrap()[..8].try_into().unwrap());
        assert_eq!(id_min, 1);
        assert_eq!(id_max, 5);

        // Verify the PND2 blob is decodable
        let blob_hash = &manifest.row_groups[0].blob_hash;
        let blob_data = kernel.read_blob(blob_hash).unwrap();
        let cols = pond_core::pnd2_decode(&blob_data).unwrap();
        assert_eq!(cols.len(), 2);
        assert_eq!(cols[0].i64_data, ids);
        assert_eq!(cols[1].i64_data, ages);
    }

    #[test]
    fn test_write_rows_i64_packed_uses_pondpack() {
        let dir = tempfile::tempdir().unwrap();
        let storage = UnifiedStorage::new_local(dir.path()).unwrap();
        let kernel = storage.kernel();

        let ids = vec![1i64, 2, 3];
        let scores = vec![10i64, 20, 30];

        let hash = write_rows_i64_packed(
            kernel, "metrics", "main",
            &[("id", &ids), ("score", &scores)],
            "packed write test",
        ).unwrap();

        // The hash should be a pack (PNPK magic)
        let pack_data = kernel.read_blob(&hash).unwrap();
        assert!(crate::pond_pack::is_pack(&pack_data),
            "HEAD should point to a PondPack blob");

        // Decode the pack
        let (commit, manifest_bytes, _inline) = crate::pond_pack::decode_pack(&pack_data).unwrap();
        assert_eq!(commit["message"], "packed write test");
        assert_eq!(commit["index"], 0);

        // Verify manifest is decodable
        let manifest = CollectionManifest::decode(&manifest_bytes).expect("manifest should decode");
        assert_eq!(manifest.row_groups.len(), 1);
        assert_eq!(manifest.row_groups[0].n_rows, 3);

        // Verify the PND2 data blob is decodable
        let blob_hash = &manifest.row_groups[0].blob_hash;
        let blob_data = kernel.read_blob(blob_hash).unwrap();
        let cols = pond_core::pnd2_decode(&blob_data).unwrap();
        assert_eq!(cols[0].i64_data, ids);
        assert_eq!(cols[1].i64_data, scores);

        // Verify only 2 blobs were written (1 pack + 1 data) — NOT 3 (commit + manifest + data)
        // The pack replaces both commit and manifest with ONE blob
        let all_blobs = kernel.list_names_prefix("blobs/");
        assert_eq!(all_blobs.len(), 2, "packed write should create 2 blobs (pack + data), got {}", all_blobs.len());
    }

    #[test]
    fn test_write_rows_i64_slab_integration() {
        // End-to-end test: write 3 RGs as one slab, read them back via range reads.
        let dir = tempfile::tempdir().unwrap();
        let storage = UnifiedStorage::new_local(dir.path()).unwrap();
        let kernel = storage.kernel();

        // 3 row groups, each with 2 columns (id, val), 3 rows each
        let rg1: Vec<(&str, &[i64])> = vec![
            ("id", &[1i64, 2, 3]),
            ("val", &[10i64, 20, 30]),
        ];
        let rg2: Vec<(&str, &[i64])> = vec![
            ("id", &[4i64, 5, 6]),
            ("val", &[40i64, 50, 60]),
        ];
        let rg3: Vec<(&str, &[i64])> = vec![
            ("id", &[7i64, 8, 9]),
            ("val", &[70i64, 80, 90]),
        ];
        let rgs: Vec<&[(&str, &[i64])]> = vec![&rg1, &rg2, &rg3];

        let hash = write_rows_i64_slab(kernel, "slab_test", "main", &rgs, "3 RGs as slab").unwrap();

        // Verify commit exists
        let commit_obj = commit::read_commit(kernel, &hash).unwrap();
        assert_eq!(commit_obj.message, "3 RGs as slab");

        // Verify the manifest is decodable and has correct RG count
        let manifest_bytes = kernel.read_blob(&commit_obj.manifest).unwrap();
        let manifest = CollectionManifest::decode(&manifest_bytes).expect("slab manifest should decode");
        assert_eq!(manifest.row_groups.len(), 3);

        // All 3 RGs should point to the SAME slab blob
        let slab_hash = &manifest.row_groups[0].blob_hash;
        for rg in &manifest.row_groups[1..] {
            assert_eq!(&rg.blob_hash, slab_hash, "all RGs in a slab must share the same blob hash");
        }

        // Each RG should have slab_byte_offset and slab_byte_len set
        for (i, rg) in manifest.row_groups.iter().enumerate() {
            assert!(rg.slab_byte_offset.is_some(), "RG {} missing slab_byte_offset", i);
            assert!(rg.slab_byte_len.is_some(), "RG {} missing slab_byte_len", i);
        }

        // Verify each RG's data is decodable as PND2 via range reads
        let rg_data = crate::read::read_all_row_groups(kernel, "slab_test", "main").unwrap();
        assert_eq!(rg_data.len(), 3);
        for (i, data) in rg_data.iter().enumerate() {
            let cols = pond_core::pnd2_decode(data).unwrap_or_else(|_| panic!("RG {} PND2 decode failed", i));
            assert_eq!(cols.len(), 2, "RG {} should have 2 columns", i);
            assert_eq!(cols[0].i64_data.len(), 3, "RG {} should have 3 rows", i);
        }
        // Verify actual data values
        let cols0 = pond_core::pnd2_decode(&rg_data[0]).unwrap();
        assert_eq!(cols0[0].i64_data, vec![1i64, 2, 3]);
        assert_eq!(cols0[1].i64_data, vec![10i64, 20, 30]);
    }

    #[test]
    fn test_write_rows_i64_slab_writes_bloom_and_prunes_correctly() {
        // Production slabs must carry a bloom filter (header flag set) and
        // the read path must prune with NO false negatives: an equality
        // query for a PRESENT value still returns it, and a query for an
        // ABSENT value returns empty (pruning correctness).
        let dir = tempfile::tempdir().unwrap();
        let storage = UnifiedStorage::new_local(dir.path()).unwrap();
        let kernel = storage.kernel();

        let rg1: Vec<(&str, &[i64])> = vec![
            ("id", &[1i64, 2, 3]),
            ("val", &[10i64, 20, 30]),
        ];
        let rg2: Vec<(&str, &[i64])> = vec![
            ("id", &[4i64, 5, 6]),
            ("val", &[40i64, 50, 60]),
        ];
        let rgs: Vec<&[(&str, &[i64])]> = vec![&rg1, &rg2];
        let hash = write_rows_i64_slab(kernel, "slab_bloom", "main", &rgs, "bloom slab").unwrap();

        // 1. The slab blob must have the PSLB bloom flag set.
        let commit_obj = commit::read_commit(kernel, &hash).unwrap();
        let manifest_bytes = kernel.read_blob(&commit_obj.manifest).unwrap();
        let manifest = CollectionManifest::decode(&manifest_bytes).unwrap();
        let slab_hash = manifest.row_groups[0].blob_hash.clone();
        let header = kernel.read_blob_range(&slab_hash, 0, slab::PSLB_HEADER_LEN as u64).unwrap();
        assert_eq!(&header[0..4], slab::PSLB_MAGIC);
        assert_ne!(header[5] & slab::PSLB_FLAG_HAS_BLOOM, 0,
            "production slab writes must embed a bloom filter");

        // 2. Equality query for a PRESENT value → must return it
        //    (no false-negative pruning through the bloom).
        let present = crate::read::read_rows_i64(
            kernel, "slab_bloom", "main", None,
            Some(&[("id", "=", 4i64)]),
        ).unwrap();
        let id_col = present.iter().find(|(n, _)| n == "id")
            .expect("id column present");
        assert!(id_col.1.contains(&4), "bloom must not prune a PRESENT value; got {:?}",
            id_col.1);

        // 3. Equality query for an ABSENT value → empty result
        //    (the bloom legitimately prunes the whole slab).
        let absent = crate::read::read_rows_i64(
            kernel, "slab_bloom", "main", None,
            Some(&[("id", "=", 9999i64)]),
        ).unwrap();
        let id_absent = absent.iter().find(|(n, _)| n == "id")
            .expect("id column present (empty)");
        assert!(id_absent.1.is_empty(),
            "absent value must return no rows; got {:?}", id_absent.1);

        // 4. Full read (no predicates) → all 6 rows intact.
        let all = crate::read::read_rows_i64(kernel, "slab_bloom", "main", None, None).unwrap();
        let id_all = all.iter().find(|(n, _)| n == "id").unwrap();
        assert_eq!(id_all.1.len(), 6, "full read must return all rows");
    }
    #[test]
    fn test_slab_writer_single_batch() {
        let dir = tempfile::tempdir().unwrap();
        let storage = UnifiedStorage::new_local(dir.path()).unwrap();
        let kernel = storage.kernel();

        let ids = vec![1i64, 2, 3, 4, 5];
        let vals = vec![10i64, 20, 30, 40, 50];

        let mut sw = SlabWriter::new(kernel, "sw_test", "main");
        sw.write_rows_i64(&[("id", &ids), ("val", &vals)]).unwrap();
        let commit_hash = sw.flush("single batch").unwrap();

        // Verify commit exists
        let commit = commit::read_commit(kernel, &commit_hash).unwrap();
        assert_eq!(commit.message, "single batch");

        // Verify manifest has 1 RG with slab offsets
        let manifest_bytes = kernel.read_blob(&commit.manifest).unwrap();
        let manifest = CollectionManifest::decode(&manifest_bytes).unwrap();
        assert_eq!(manifest.row_groups.len(), 1);
        assert!(manifest.row_groups[0].slab_byte_offset.is_some());
        assert!(manifest.row_groups[0].slab_byte_len.is_some());

        // Verify data is readable via range reads
        let cols = crate::read::read_rows_i64(kernel, "sw_test", "main", None, None).unwrap();
        let id_col = cols.iter().find(|(n, _)| n == "id").unwrap();
        assert_eq!(id_col.1, ids);
    }

    #[test]
    fn test_slab_writer_multiple_batches_auto_flush() {
        let dir = tempfile::tempdir().unwrap();
        let storage = UnifiedStorage::new_local(dir.path()).unwrap();
        let kernel = storage.kernel();

        // Write 5 small batches — should all fit in one slab (well under 128 MB)
        let mut sw = SlabWriter::new(kernel, "multi_batch", "main");
        for i in 0..5 {
            let ids = vec![i * 10i64, i * 10 + 1, i * 10 + 2];
            let vals = vec![i * 100i64, i * 100 + 1, i * 100 + 2];
            sw.write_rows_i64(&[("id", &ids), ("val", &vals)]).unwrap();
        }
        assert_eq!(sw.buffered_count(), 5);
        assert_eq!(sw.completed_count(), 0);
        let commit_hash = sw.flush("5 batches").unwrap();

        // Verify manifest has 5 RGs all pointing to the same slab
        let commit = commit::read_commit(kernel, &commit_hash).unwrap();
        let manifest_bytes = kernel.read_blob(&commit.manifest).unwrap();
        let manifest = CollectionManifest::decode(&manifest_bytes).unwrap();
        assert_eq!(manifest.row_groups.len(), 5);

        let slab_hash = &manifest.row_groups[0].blob_hash;
        for rg in &manifest.row_groups[1..] {
            assert_eq!(&rg.blob_hash, slab_hash, "all RGs must share the same slab");
        }

        // All 15 rows should be readable
        let cols = crate::read::read_rows_i64(kernel, "multi_batch", "main", None, None).unwrap();
        let id_col = cols.iter().find(|(n, _)| n == "id").unwrap();
        assert_eq!(id_col.1.len(), 15);
    }

    #[test]
    fn test_slab_writer_schema_mismatch_rejects() {
        let dir = tempfile::tempdir().unwrap();
        let storage = UnifiedStorage::new_local(dir.path()).unwrap();
        let kernel = storage.kernel();

        let mut sw = SlabWriter::new(kernel, "schema_test", "main");
        sw.write_rows_i64(&[("id", &[1i64, 2])]).unwrap();

        // Different schema — should fail
        let result = sw.write_rows_i64(&[("name", &[1i64])]);
        assert!(result.is_err(), "schema mismatch should be rejected");
        assert!(result.unwrap_err().contains("schema mismatch"));
    }

    #[test]
    fn test_slab_writer_empty_flush_errors() {
        let dir = tempfile::tempdir().unwrap();
        let storage = UnifiedStorage::new_local(dir.path()).unwrap();
        let kernel = storage.kernel();

        let sw = SlabWriter::new(kernel, "empty_test", "main");
        let result = sw.flush("nothing");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("no data"));
    }

    #[test]
    fn test_slab_writer_v3_tree_e2e() {
        // End-to-end: write 3 RGs, manually create a v3 root with 2 leaves,
        // then read back via resolve_manifest and verify all data.
        let dir = tempfile::tempdir().unwrap();
        let storage = UnifiedStorage::new_local(dir.path()).unwrap();
        let kernel = storage.kernel();

        // Write 3 RGs via SlabWriter (creates a v2 manifest since ≤1024 RGs)
        let mut sw = SlabWriter::new(kernel, "tree_test", "main");
        for i in 0..3 {
            let ids = vec![(i * 3 + 1) as i64, (i * 3 + 2) as i64, (i * 3 + 3) as i64];
            let vals = vec![(i * 30 + 10) as i64, (i * 30 + 20) as i64, (i * 30 + 30) as i64];
            sw.write_rows_i64(&[("id", &ids), ("val", &vals)]).unwrap();
        }
        let _commit_hash = sw.flush("v2 baseline").unwrap();

        // Now manually create a v3 tree: split the 3 RGs into 2 leaves
        // Read the v2 manifest to get the RG entries
        let head = kernel.resolve(&branch_ref("tree_test", "main")).unwrap();
        let commit_obj = commit::read_commit(kernel, &head).unwrap();
        let manifest_bytes = kernel.read_blob(&commit_obj.manifest).unwrap();
        let v2_manifest = CollectionManifest::decode(&manifest_bytes).unwrap();
        assert_eq!(v2_manifest.row_groups.len(), 3);

        // Split: leaf 0 = RGs 0,1; leaf 1 = RG 2
        let leaf0_rgs = &v2_manifest.row_groups[0..2];
        let leaf1_rgs = &v2_manifest.row_groups[2..3];

        // Create leaf manifests
        let mut leaf0 = CollectionManifest::new(v2_manifest.columns.clone(), v2_manifest.key_col.clone());
        for rg in leaf0_rgs { leaf0.add_row_group(rg.clone()); }
        let leaf0_hash = kernel.write(&leaf0.encode()).unwrap();

        let mut leaf1 = CollectionManifest::new(v2_manifest.columns.clone(), v2_manifest.key_col.clone());
        for rg in leaf1_rgs { leaf1.add_row_group(rg.clone()); }
        let leaf1_hash = kernel.write(&leaf1.encode()).unwrap();

        // Create root manifest
        let (key_min_0, key_max_0) = compute_key_range(leaf0_rgs, &v2_manifest.key_col);
        let (key_min_1, key_max_1) = compute_key_range(leaf1_rgs, &v2_manifest.key_col);
        let mut root = RootManifest::new(v2_manifest.columns.clone(), v2_manifest.key_col.clone());
        root.leaves.push(LeafEntry {
            leaf_hash: leaf0_hash.clone(), n_row_groups: 2, total_data_bytes: 0,
            key_min: key_min_0, key_max: key_max_0,
        });
        root.leaves.push(LeafEntry {
            leaf_hash: leaf1_hash.clone(), n_row_groups: 1, total_data_bytes: 0,
            key_min: key_min_1, key_max: key_max_1,
        });

        // Write root and point commit at it
        let root_hash = kernel.write(&root.encode()).unwrap();
        let new_commit = commit::write_commit(
            kernel, "tree_test", &root_hash, Some(&head), None,
            "v3 tree commit", 1,
        ).unwrap();
        kernel.reference(&branch_ref("tree_test", "main"), &new_commit).unwrap();

        // Now read back via the standard read path — it should transparently
        // resolve the v3 root → fetch leaves → merge RGs → read data
        let cols = crate::read::read_rows_i64(kernel, "tree_test", "main", None, None).unwrap();
        let id_col = cols.iter().find(|(n, _)| n == "id").unwrap();
        let val_col = cols.iter().find(|(n, _)| n == "val").unwrap();
        assert_eq!(id_col.1.len(), 9, "should have 9 rows total");
        assert_eq!(val_col.1.len(), 9);
        // Verify first and last values
        assert_eq!(id_col.1[0], 1);
        assert_eq!(id_col.1[8], 9);
        assert_eq!(val_col.1[0], 10);
        assert_eq!(val_col.1[8], 90);
    }

    #[test]
    fn test_write_rows_packed_commit_chain() {
        // Verify write_rows (PondPack path) chains commits correctly.
        // Parent is a PNPK pack, child should be able to read parent's commit.
        let dir = tempfile::tempdir().unwrap();
        let storage = UnifiedStorage::new_local(dir.path()).unwrap();
        let kernel = storage.kernel();

        let ids1 = vec![1i64, 2, 3];
        let vals1 = vec![10.0f64, 20.0, 30.0];

        // First write via write_rows (PondPack path)
        let c1 = write_rows(
            kernel, "chain_test", "main",
            &[("id", TypedColumn::Int64(ids1.clone())), ("val", TypedColumn::Float64(vals1.clone()))],
            "first commit",
        ).unwrap();

        // Verify c1 is a PNPK pack
        let pack_data = kernel.read_blob(&c1).unwrap();
        assert!(pond_pack::is_pack(&pack_data), "write_rows should produce PNPK pack");

        // Verify commit chain: c2's parent = c1
        let ids2 = vec![4i64, 5];
        let vals2 = vec![40.0f64, 50.0];
        let c2 = write_rows(
            kernel, "chain_test", "main",
            &[("id", TypedColumn::Int64(ids2)), ("val", TypedColumn::Float64(vals2))],
            "second commit",
        ).unwrap();

        let commit2 = commit::read_commit(kernel, &c2).unwrap();
        assert_eq!(commit2.parent, Some(c1.clone()));
        assert_eq!(commit2.index, 1);
        assert_eq!(commit2.message, "second commit");

        // Verify parent (c1) is also readable as a commit via commit::read_commit
        let commit1 = commit::read_commit(kernel, &c1).unwrap();
        assert!(commit1.parent.is_none());
        assert_eq!(commit1.index, 0);
        assert_eq!(commit1.message, "first commit");

        // Verify data is readable through the standard read path
        // Note: read_rows_i64 reads HEAD manifest only (latest commit's data)
        let cols = crate::read::read_rows_i64(kernel, "chain_test", "main", None, None).unwrap();
        let id_col = cols.iter().find(|(n, _)| n == "id").unwrap();
        assert_eq!(id_col.1.len(), 2, "HEAD should have 2 rows from second commit");
    }

    #[test]
    fn test_reference_if_cas_semantics() {
        // Deterministic CAS semantics on LocalFS (the S3/R2 If-Match /
        // If-None-Match paths are exercised end-to-end by the moto + R2
        // suites through write_rows' CAS commit loop).
        let dir = tempfile::tempdir().unwrap();
        let storage = UnifiedStorage::new_local(dir.path()).unwrap();
        let kernel = storage.kernel();

        let c1 = write_rows(kernel, "cas_sem", "main",
            &[("id", TypedColumn::Int64(vec![1i64]))], "c1").unwrap();
        let c2 = write_rows(kernel, "cas_sem", "main",
            &[("id", TypedColumn::Int64(vec![2i64]))], "c2").unwrap();
        let b_ref = branch_ref("cas_sem", "main");
        assert_eq!(kernel.resolve(&b_ref), Some(c2.clone()));

        // Stale writer (holds the pre-c2 view): CAS must FAIL and leave
        // HEAD untouched — this is the guard against lost updates.
        let won = kernel.reference_if(&b_ref, Some(&c1), &c1).unwrap();
        assert!(!won, "stale CAS must lose");
        assert_eq!(kernel.resolve(&b_ref), Some(c2.clone()), "HEAD unchanged after lost CAS");

        // Fresh writer (current HEAD as expected): CAS wins.
        let won = kernel.reference_if(&b_ref, Some(&c2), &c1).unwrap();
        assert!(won, "fresh CAS must win");
        assert_eq!(kernel.resolve(&b_ref), Some(c1.clone()));

        // Expected=Some(h) but ref absent → fail (no false create).
        let won = kernel.reference_if(&branch_ref("cas_sem", "ghost"), Some(&c1), &c2).unwrap();
        assert!(!won, "CAS with expected-value on absent ref must fail");

        // Expected=None → create-if-absent only.
        let fresh = branch_ref("cas_fresh", "main");
        let won = kernel.reference_if(&fresh, None, &c1).unwrap();
        assert!(won, "create-if-absent on fresh ref must win");
        let won = kernel.reference_if(&fresh, None, &c2).unwrap();
        assert!(!won, "create-if-absent must LOSE when the ref already exists");
        assert_eq!(kernel.resolve(&fresh), Some(c1), "existing binding survives failed create");
    }

    #[test]
    fn test_write_rows_concurrent_threads_all_succeed() {
        // Behavioral smoke: N threads commit to the same collection through
        // the CAS loop. Every write must return Ok (retries converge), the
        // final HEAD must resolve, and the chain must be walkable. On
        // LocalFS put_path_if is read-check-write (non-atomic), so a truly
        // simultaneous check window can still orphan a commit — the atomic
        // guarantee lives in the S3/R2 If-Match path; this test proves the
        // loop itself never wedges, errors, or corrupts state under races.
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};

        let dir = tempfile::tempdir().unwrap();
        let storage = Arc::new(UnifiedStorage::new_local(dir.path()).unwrap());
        let kernel = storage.kernel();
        let ok_count = Arc::new(AtomicUsize::new(0));

        let mut handles = Vec::new();
        for t in 0..4i64 {
            let storage = Arc::clone(&storage);
            let ok_count = Arc::clone(&ok_count);
            handles.push(std::thread::spawn(move || {
                let k = storage.kernel();
                let h = write_rows(k, "cas_threads", "main",
                    &[("id", TypedColumn::Int64(vec![t])), ("t", TypedColumn::Int64(vec![t]))],
                    &format!("thread {}", t)).unwrap();
                ok_count.fetch_add(1, Ordering::SeqCst);
                h
            }));
        }
        let mut hashes = Vec::new();
        for h in handles {
            hashes.push(h.join().expect("writer thread must not panic"));
        }
        assert_eq!(ok_count.load(Ordering::SeqCst), 4, "all concurrent writes must succeed");

        // HEAD resolves and is one of the commits.
        let head = kernel.resolve(&branch_ref("cas_threads", "main")).expect("HEAD must resolve");
        assert!(hashes.contains(&head));

        // The chain from HEAD is walkable and terminates (no cycles).
        let mut cur = Some(head);
        let mut hops = 0;
        while let Some(h) = cur {
            let c = commit::read_commit(kernel, &h).expect("commit must be readable");
            cur = c.parent;
            hops += 1;
            assert!(hops < 100, "commit chain must terminate (possible cycle)");
        }
        assert!(hops >= 1);
    }

}
