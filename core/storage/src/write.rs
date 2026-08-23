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
use crate::manifest::{CollectionManifest, ColumnStatsEntry, RowGroupEntry};
use crate::slab;
use crate::{branch_ref, manifest_ref};
use pond_core::{pnd2_encode_i64_auto, pnd2_encode_multi_typed, TypedColumn, VT_INT64};
use pond_kernel::PondKernel;

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

    // Encode as PND2 with auto-encoding per column
    let blob = pnd2_encode_i64_auto(columns);
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

    // 1. Encode data as PND2 blob
    let blob = pnd2_encode_i64_auto(columns);
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

    // Encode as PND2 with per-type encoding
    let blob = pnd2_encode_multi_typed(&final_columns);
    let data_hash = kernel.write(&blob)
        .map_err(|e| format!("Failed to write PND2 blob: {}", e))?;

    // Get parent commit
    let parent = kernel.resolve(&branch_ref(collection, active_branch));
    let parent_index = parent.as_ref()
        .and_then(|p| commit::read_commit(kernel, p))
        .map(|c| c.index + 1)
        .unwrap_or(0);

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
        let blob = pnd2_encode_i64_auto(rg);
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

    // 3. Encode as PSLB slab
    let slab_bytes = slab::encode_slab(&slab_inputs);
    let slab_hash = kernel.write(&slab_bytes)
        .map_err(|e| format!("Failed to write slab blob: {}", e))?;

    // 4. Decode slab footer to get exact byte offsets for each RG
    //    First get the tail to find footer_offset, then extract footer bytes
    let tail = slab::decode_slab_tail(&slab_bytes)
        .ok_or_else(|| "Failed to decode slab tail after encode".to_string())?;
    let footer_offset = tail.0 as usize;
    let footer_end = slab_bytes.len() - slab::PSLB_TAIL_LEN;
    let footer = slab::decode_slab_footer(&slab_bytes[footer_offset..footer_end])
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
        }
    }

    /// Buffer one row group as PND2. Auto-flushes when K RGs or 128 MB reached.
    ///
    /// All calls must have the same schema (same column names, same order).
    /// Returns an error on schema mismatch.
    pub fn write_rows_i64(&mut self, columns: &[(&str, &[i64])]) -> Result<(), String> {
        let n_rows = columns.first().map(|(_, v)| v.len()).unwrap_or(0) as u32;
        let blob = pnd2_encode_i64_auto(columns);

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

        let slab_bytes = slab::encode_slab(&self.buffer);
        let slab_hash = self.kernel.write(&slab_bytes)
            .map_err(|e| format!("SlabWriter: failed to write slab: {}", e))?;

        // Decode footer for exact byte offsets
        let tail = slab::decode_slab_tail(&slab_bytes)
            .ok_or_else(|| "SlabWriter: slab tail decode failed".to_string())?;
        let footer_end = slab_bytes.len() - slab::PSLB_TAIL_LEN;
        let footer = slab::decode_slab_footer(&slab_bytes[tail.0 as usize..footer_end])
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
    /// Returns the commit hash. Consumes self (cannot write more after flush).
    pub fn flush(mut self, message: &str) -> Result<String, String> {
        self.flush_slab()?; // flush partial buffer

        if self.completed_rgs.is_empty() {
            return Err("SlabWriter: no data to flush".to_string());
        }

        let schema = self.schema.unwrap();
        let key_col = self.key_col.unwrap_or_default();
        let mut manifest = CollectionManifest::new(schema, key_col);
        for rg in self.completed_rgs.drain(..) {
            manifest.add_row_group(rg);
        }

        // Commit flow (same as write_rows_i64)
        let parent = self.kernel.resolve(&branch_ref(self.collection, self.active_branch));
        let parent_index = parent.as_ref()
            .and_then(|p| commit::read_commit(self.kernel, p))
            .map(|c| c.index + 1)
            .unwrap_or(0);

        let manifest_bytes = manifest.encode();
        let manifest_hash = self.kernel.write(&manifest_bytes)
            .map_err(|e| format!("SlabWriter: failed to write manifest: {}", e))?;

        let commit_hash = commit::write_commit(
            self.kernel, self.collection, &manifest_hash,
            parent.as_deref(), None,
            if message.is_empty() { "slab_write" } else { message },
            parent_index,
        ).map_err(|e| format!("SlabWriter: commit failed: {}", e))?;

        self.kernel.reference(&branch_ref(self.collection, self.active_branch), &commit_hash)
            .map_err(|e| format!("SlabWriter: branch ref failed: {}", e))?;
        self.kernel.reference(&manifest_ref(self.collection, self.active_branch), &manifest_hash)
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

}
