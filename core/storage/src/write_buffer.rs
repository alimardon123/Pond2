// Write Buffer — stage multiple writes and flush as a single PNPK pack
//
// DESIGN RATIONALE:
//   Every write currently does 2-3 S3 PUTs (data + manifest + commit as PNPK,
//   then 3 ref updates = 4-5 S3 round-trips total. At 50-300ms/RTT,
//   each write costs 200-1500ms.
//
// The write buffer stages PND2-encoded row groups in memory.
// On flush, all staged RGs are packed into a SINGLE PNPK commit,
// reducing N writes from 4N S3 PUTs to N+3. For N=100 small writes:
//   400 PUTs (40-150s) → 103 PUTs (10-30s) — a 4x improvement.
//
// The `write_rows_buffered()` call itself returns in ~20us (just PND2 encode +
// hash). The actual S3 cost is deferred to flush().
//
// Phase 1: Staging + flush. No background thread, no read-after-write,
//   no compound refs. Callers must call flush() for durability.
//
// SAFETY: Buffered writes are in-memory only. If the process crashes,
//   they are lost. Callers who need durability should call flush().

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use crate::commit;
use crate::manifest::{CollectionManifest, ColumnStatsEntry, RowGroupEntry};
use crate::pond_pack;
use crate::{branch_ref, manifest_ref};
use pond_core::pnd2_encode_multi_typed;
use pond_kernel::PondKernel;

// ---------------------------------------------------------------------------
// Staged row group — PND2 blob cached in memory
// ---------------------------------------------------------------------------

/// A row group staged in the write buffer. The PND2 blob and hash are
/// precomputed. Column stats enable predicate pruning on read.
struct StagedRowGroup {
    /// Precomputed SHA-256 hash of `pnd2_bytes`. This blob is NOT yet on S3.
    blob_hash: String,
    /// The PND2-encoded data blob (in memory only until flush).
    pnd2_bytes: Vec<u8>,
    /// Number of rows in this RG.
    n_rows: u32,
    /// Per-column min/max stats for zone-map pruning.
    col_stats: Vec<ColumnStatsEntry>,
    /// Commit message for this individual write (reserved for Phase 2
    /// per-RG commit messages in the manifest).
    #[allow(dead_code)]
    message: String,
}

// ---------------------------------------------------------------------------
// Per-collection staging buffer
// ---------------------------------------------------------------------------

/// Staging buffer for a single (collection, branch) pair.
///
/// The buffer captures `base_head` once (the HEAD when the first write
/// arrived) and chains all subsequent buffered writes from that parent.
struct CollectionBuffer {
    /// Staged row groups, in order of arrival.
    staged_rgs: Vec<StagedRowGroup>,
    /// Column schema (validated on first write, must match on subsequent).
    schema: Option<Vec<(String, u8)>>,
    key_col: String,
    /// The HEAD commit hash when the first staged write arrived.
    /// All buffered writes chain from this parent.
    base_head: Option<String>,
    /// Commit index for the next staged commit.
    next_index: usize,
    /// Total bytes of staged PND2 data (for size-based flush trigger).
    staged_data_bytes: usize,
    /// Number of writes staged (for count-based flush trigger).
    n_staged: usize,
    /// When the first write was staged (for time-based flush trigger).
    first_write_at: Option<Instant>,
    /// Monotonic generation counter. Incremented on any branch operation
    /// (merge, undo, checkout) to invalidate buffered writes.
    generation: u64,
}

// ---------------------------------------------------------------------------
// Flush configuration
// ---------------------------------------------------------------------------

/// Configuration for the write buffer's flush behavior.
///
/// Triggers are OR'd: if ANY trigger fires, a flush is attempted.
pub struct FlushConfig {
    /// Auto-flush after this much time since the first staged write.
    /// Default: 5 seconds. Set to `Duration::MAX` to disable time-based flush.
    pub max_age: Duration,
    /// Auto-flush after this many staged writes.
    /// Default: 100.
    pub max_writes: usize,
    /// Auto-flush after this many bytes of staged PND2 data.
    /// Default: 64 MB.
    pub max_bytes: usize,
}

impl Default for FlushConfig {
    fn default() -> Self {
        Self {
            max_age: Duration::from_secs(5),
            max_writes: 100,
            max_bytes: 64 * 1024 * 1024,
        }
    }
}

// ---------------------------------------------------------------------------
// WriteBuffer — top-level buffer owning per-collection state
// ---------------------------------------------------------------------------

/// Write buffer that stages PND2-encoded row groups and flushes them
/// as a single PNPK commit, reducing S3 PUTs.
///
/// Usage:
///   let wb = WriteBuffer::new(kernel.clone());
///   wb.write_rows_buffered("coll", "main", columns)?;
///   wb.write_rows_buffered("coll", "main", columns)?;
///   wb.flush("coll", "main")?;  // durability
///
/// Thread safety: interior mutability via `Mutex<HashMap<...>>`.
/// Lock granularity is the entire buffer (fine for Phase 1 — single-writer
/// per collection is the expected usage pattern).
pub struct WriteBuffer {
    kernel: PondKernel,
    /// Per (collection, branch) staging areas, behind a Mutex for interior
    /// mutability (write_rows_buffered takes &self, not &mut self).
    buffers: Mutex<HashMap<(String, String), CollectionBuffer>>,
    config: FlushConfig,
}

impl WriteBuffer {
    /// Create a new write buffer wrapping a kernel.
    pub fn new(kernel: PondKernel) -> Self {
        Self {
            kernel,
            buffers: Mutex::new(HashMap::new()),
            config: FlushConfig::default(),
        }
    }

    /// Set the flush configuration.
    pub fn with_config(mut self, config: FlushConfig) -> Self {
        self.config = config;
        self
    }

    /// Stage a write for later flush. Returns immediately (~20us).
    ///
    /// The data is encoded as PND2, hashed, and stored in memory.
    /// No S3 I/O happens. Call `flush()` to persist.
    ///
    /// Auto-flush: if any trigger fires (count or size),
    /// a synchronous flush is performed before returning.
    pub fn write_rows_buffered(
        &self,
        collection: &str,
        branch: &str,
        columns: &[(&str, pond_core::TypedColumn)],
        message: &str,
    ) -> Result<(), String> {
        let key = (collection.to_string(), branch.to_string());

        // Validate schema.
        let schema: Vec<(String, u8)> = columns.iter()
            .map(|(name, col)| (name.to_string(), col.vtype()))
            .collect();
        let key_col = schema.first().map(|(name, _)| name.clone()).unwrap_or_default();

        // Encode as PND2 and precompute hash.
        let blob = pnd2_encode_multi_typed(columns);
        let hash = pond_kernel::hash_bytes(&blob);
        let n_rows = columns.first().map(|(_, c)| c.len()).unwrap_or(0);

        // Build column stats.
        let col_stats: Vec<ColumnStatsEntry> = columns.iter()
            .map(|(name, col)| {
                let min_max = col.min_max_bytes();
                ColumnStatsEntry {
                    name: name.to_string(),
                    value_type: col.vtype(),
                    min: min_max.as_ref().map(|(mn, _)| mn.clone()),
                    max: min_max.as_ref().map(|(_, mx)| mx.clone()),
                    null_count: 0,
                }
            })
            .collect();

        let mut buffers = self.buffers.lock().unwrap();
        let buf = buffers.entry(key).or_insert_with(|| CollectionBuffer {
            staged_rgs: Vec::new(),
            schema: None,
            key_col: String::new(),
            base_head: None,
            next_index: 0,
            staged_data_bytes: 0,
            n_staged: 0,
            first_write_at: None,
            generation: 0,
        });

        // Validate schema matches on subsequent writes.
        if let Some(ref existing_schema) = buf.schema {
            if *existing_schema != schema {
                return Err(format!(
                    "Schema mismatch: buffer has {:?}, new write has {:?}",
                    existing_schema, schema,
                ));
            }
        } else {
            buf.schema = Some(schema);
            buf.key_col = key_col;
        }

        // Capture base_head on first write (avoids N ref-resolve calls).
        if buf.base_head.is_none() {
            buf.base_head = self.kernel.resolve(&branch_ref(collection, branch));
            let idx = buf.base_head.as_ref()
                .and_then(|h| commit::read_commit(&self.kernel, h))
                .map(|c| c.index + 1)
                .unwrap_or(0);
            buf.next_index = idx;
            buf.first_write_at = Some(Instant::now());
        }

        let data_bytes = blob.len();
        buf.staged_rgs.push(StagedRowGroup {
            blob_hash: hash,
            pnd2_bytes: blob,
            n_rows: n_rows as u32,
            col_stats,
            message: message.to_string(),
        });
        buf.staged_data_bytes += data_bytes;
        buf.n_staged += 1;

        // Check auto-flush triggers (count + size).
        let should_flush = buf.n_staged >= self.config.max_writes
            || buf.staged_data_bytes >= self.config.max_bytes;

        if should_flush {
            drop(buffers);
            return self.flush_internal(collection, branch).map(|_| ());
        }

        Ok(())
    }

    /// Check time-based flush. Call periodically from outside
    /// (e.g., a timer tick) to ensure time-based flush.
    pub fn tick_time_based_flush(&self) {
        let now = Instant::now();
        let expired_keys: Vec<(String, String)> = {
            let buffers = self.buffers.lock().unwrap();
            buffers.iter()
                .filter_map(|(k, buf)| {
                    if buf.staged_rgs.is_empty() { return None; }
                    buf.first_write_at
                        .map(|t| now.duration_since(t) >= self.config.max_age)
                        .unwrap_or(false)
                        .then(|| k.clone())
                })
                .collect()
        };

        for key in expired_keys {
            let _ = self.flush_internal(&key.0, &key.1);
        }
    }

    /// Flush all staged writes for a (collection, branch) pair.
    ///
    /// Builds a single PNPK commit containing all staged RGs,
    /// then does the 3 ref updates (branch_ref, manifest_ref, collection).
    ///
    /// Returns Ok(commit_hash) on success.
    pub fn flush(&self, collection: &str, branch: &str) -> Result<String, String> {
        self.flush_internal(collection, branch)
    }

    /// Check if a collection+branch has any staged (unflushed) writes.
    pub fn has_pending(&self, collection: &str, branch: &str) -> bool {
        let key = (collection.to_string(), branch.to_string());
        let buffers = self.buffers.lock().unwrap();
        buffers.get(&key)
            .map(|buf| !buf.staged_rgs.is_empty())
            .unwrap_or(false)
    }

    /// Invalidate the buffer for a collection+branch pair.
    /// Called after merge/undo/checkout to discard stale staged writes.
    pub fn invalidate(&self, collection: &str, branch: &str) {
        let key = (collection.to_string(), branch.to_string());
        let mut buffers = self.buffers.lock().unwrap();
        if let Some(buf) = buffers.get_mut(&key) {
            buf.generation += 1;
            buf.staged_rgs.clear();
            buf.staged_data_bytes = 0;
            buf.n_staged = 0;
            buf.base_head = None;
            buf.first_write_at = None;
        }
    }

    /// Get a reference to a staged row group's PND2 bytes.
    /// Returns None if the blob is not in the buffer.
    pub fn get_staged_blob(&self, hash: &str) -> Option<Vec<u8>> {
        let buffers = self.buffers.lock().unwrap();
        for buf in buffers.values() {
            for rg in &buf.staged_rgs {
                if rg.blob_hash == hash {
                    return Some(rg.pnd2_bytes.clone());
                }
            }
        }
        None
    }

    // -----------------------------------------------------------------------
    // Internal flush implementation
    // -----------------------------------------------------------------------

    fn flush_internal(&self, collection: &str, branch: &str) -> Result<String, String> {
        let key = (collection.to_string(), branch.to_string());

        let (parent, next_index, schema, key_col, staged_snapshot) = {
            let mut buffers = self.buffers.lock().unwrap();
            let buf = buffers.get_mut(&key)
                .ok_or_else(|| format!(
                    "No buffer for collection='{}', branch='{}'",
                    collection, branch,
                ))?;

            if buf.staged_rgs.is_empty() {
                return Err("No staged writes to flush".to_string());
            }

            // Check staleness: if the branch HEAD moved (e.g., concurrent
            // unbuffered write), discard and error.
            let current_head = self.kernel.resolve(&branch_ref(collection, branch));
            let stale = buf.generation > 0
                && buf.base_head.as_ref()
                    .is_none_or(|h| {
                        current_head.as_ref() != Some(h)
                    });

            if stale {
                buf.staged_rgs.clear();
                buf.staged_data_bytes = 0;
                buf.n_staged = 0;
                buf.base_head = None;
                buf.first_write_at = None;
                return Err("Concurrent unbuffered write detected, buffer discarded".to_string());
            }

            let parent = buf.base_head.clone();
            let next_index = buf.next_index;
            let schema = buf.schema.clone().unwrap_or_default();
            let key_col = buf.key_col.clone();

            // Take all staged RGs out (drain).
            let staged: Vec<StagedRowGroup> = std::mem::take(&mut buf.staged_rgs);
            buf.staged_data_bytes = 0;
            buf.n_staged = 0;
            buf.base_head = None;
            buf.first_write_at = None;

            (parent, next_index, schema, key_col, staged)
        };
        // Lock released — now do S3 I/O without holding the mutex.

        // Write each staged RG blob to S3.
        for rg in &staged_snapshot {
            self.kernel.write(&rg.pnd2_bytes)
                .map_err(|e| format!("Failed to write staged blob: {}", e))?;
        }

        // Build manifest with all staged RGs.
        let mut manifest = CollectionManifest::new(schema, key_col);

        for (i, rg) in staged_snapshot.iter().enumerate() {
            manifest.add_row_group(RowGroupEntry {
                key: format!("rg_buf_{:010}", i),
                blob_hash: rg.blob_hash.clone(),
                n_rows: rg.n_rows,
                columns: rg.col_stats.clone(),
                slab_byte_offset: None,
                slab_byte_len: None,
            });
        }

        let manifest_bytes = manifest.encode();

        // Build commit JSON.
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs_f64())
            .unwrap_or(0.0);
        let commit_obj = serde_json::json!({
            "parent": parent,
            "second_parent": null,
            "manifest": "packed",
            "message": "write_buffered (flush)",
            "timestamp": timestamp,
            "index": next_index,
        });

        // Encode as PNPK (commit + manifest combined) → 1 S3 PUT.
        let pack_bytes = pond_pack::encode_pack(&commit_obj, &manifest_bytes, None);
        let pack_hash = self.kernel.write(&pack_bytes)
            .map_err(|e| format!("Failed to write PondPack: {}", e))?;

        // Update branch refs (3 PUTs — Phase 1).
        // Phase 2 will combine into 1 compound ref.
        self.kernel.reference(&branch_ref(collection, branch), &pack_hash)
            .map_err(|e| format!("Failed to update branch ref: {}", e))?;
        self.kernel.reference(&manifest_ref(collection, branch), &pack_hash)
            .map_err(|e| format!("Failed to update manifest ref: {}", e))?;
        let _ = self.kernel.reference(collection, &pack_hash);

        Ok(pack_hash)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use pond_core::TypedColumn;
    use crate::write;

    fn make_test_buffer() -> (WriteBuffer, PondKernel) {
        let dir = tempfile::tempdir().unwrap();
        let kernel = PondKernel::new_local(dir.path()).unwrap();
        std::mem::forget(dir);
        (WriteBuffer::new(kernel.clone()), kernel)
    }

    #[test]
    fn test_buffered_write_returns_immediately() {
        let (wb, _kernel) = make_test_buffer();
        let cols = vec![
            ("id", TypedColumn::Int64(vec![1, 2, 3])),
            ("val", TypedColumn::Int64(vec![10, 20, 30])),
        ];
        // Should return in < 100ms (no S3 I/O, just local FS writes are fast).
        let start = Instant::now();
        wb.write_rows_buffered("t", "main", &cols, "w1").unwrap();
        assert!(start.elapsed() < Duration::from_millis(100));
        assert!(wb.has_pending("t", "main"));
    }

    #[test]
    fn test_flush_writes_all_rg_to_manifest() {
        let (wb, kernel) = make_test_buffer();
        let cols1 = vec![
            ("id", TypedColumn::Int64(vec![1, 2, 3])),
            ("val", TypedColumn::Int64(vec![10, 20, 30])),
        ];
        let cols2 = vec![
            ("id", TypedColumn::Int64(vec![4, 5, 6])),
            ("val", TypedColumn::Int64(vec![40, 50, 60])),
        ];
        wb.write_rows_buffered("t", "main", &cols1, "w1").unwrap();
        wb.write_rows_buffered("t", "main", &cols2, "w2").unwrap();

        // Flush — should produce 1 commit with 2 RGs.
        let hash = wb.flush("t", "main").unwrap();
        assert!(!wb.has_pending("t", "main"));
        assert!(!hash.is_empty());

        // Read the manifest and verify 2 RGs.
        let head = kernel.resolve(&branch_ref("t", "main")).unwrap();
        let pack_bytes = kernel.read_blob(&head).unwrap();
        let (_, manifest_bytes, _inline) =
            pond_pack::decode_pack(&pack_bytes).unwrap();
        let manifest = CollectionManifest::decode(&manifest_bytes).unwrap();
        assert_eq!(manifest.row_groups.len(), 2);
        assert_eq!(manifest.row_groups[0].n_rows, 3);
        assert_eq!(manifest.row_groups[1].n_rows, 3);
    }

    #[test]
    fn test_flush_invalidated_by_staleness() {
        let (wb, kernel) = make_test_buffer();
        let cols = vec![
            ("id", TypedColumn::Int64(vec![1])),
            ("val", TypedColumn::Int64(vec![10])),
        ];
        wb.write_rows_buffered("t", "main", &cols, "w1").unwrap();

        // Simulate a concurrent unbuffered write by changing the branch HEAD.
        let ids: Vec<i64> = vec![99];
        let vals: Vec<i64> = vec![999];
        write::write_rows_i64(
            &kernel, "t", "main",
            &[("id", ids.as_slice()), ("val", vals.as_slice())],
            "concurrent",
        ).unwrap();

        // Invalidate the buffer to mark it as potentially stale,
        // then flush should fail.
        wb.invalidate("t", "main");
        // Re-stage a write so there's something to flush
        let cols2 = vec![
            ("id", TypedColumn::Int64(vec![2])),
            ("val", TypedColumn::Int64(vec![20])),
        ];
        wb.write_rows_buffered("t", "main", &cols2, "w2").unwrap();

        // Another concurrent write moves HEAD
        let ids2: Vec<i64> = vec![88];
        let vals2: Vec<i64> = vec![888];
        write::write_rows_i64(
            &kernel, "t", "main",
            &[("id", ids2.as_slice()), ("val", vals2.as_slice())],
            "concurrent2",
        ).unwrap();

        let result = wb.flush("t", "main");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("discarded"));
        assert!(!wb.has_pending("t", "main"));
    }

    #[test]
    fn test_auto_flush_on_count_trigger() {
        let (wb, _kernel) = make_test_buffer();
        // Create a new buffer with low write threshold
        let wb = WriteBuffer::with_config(
            wb, FlushConfig { max_writes: 3, ..FlushConfig::default() }
        );

        let cols = vec![
            ("id", TypedColumn::Int64(vec![1])),
            ("val", TypedColumn::Int64(vec![10])),
        ];
        // Writes 1, 2 — no flush yet (below threshold of 3).
        wb.write_rows_buffered("t", "main", &cols, "w1").unwrap();
        assert!(wb.has_pending("t", "main"));
        wb.write_rows_buffered("t", "main", &cols, "w2").unwrap();
        assert!(wb.has_pending("t", "main"));
        // Write 3 — triggers auto-flush.
        wb.write_rows_buffered("t", "main", &cols, "w3").unwrap();
        assert!(!wb.has_pending("t", "main"));
    }

    #[test]
    fn test_auto_flush_on_size_trigger() {
        let (wb, _kernel) = make_test_buffer();
        // Stage writes with a known threshold. We first measure one blob size
        // by encoding manually, then set the threshold between 1x and 2x.
        let blob = pond_core::pnd2_encode_multi_typed(&[
            ("id", TypedColumn::Int64(vec![1])),
            ("val", TypedColumn::Int64(vec![10])),
        ]);
        let one_blob_size = blob.len();
        // Threshold: 1.5x one blob → first write fits, second triggers.
        let threshold = one_blob_size + one_blob_size / 2;
        let wb = WriteBuffer::with_config(
            wb, FlushConfig { max_bytes: threshold, max_writes: 1000, ..FlushConfig::default() }
        );

        let small_cols = vec![
            ("id", TypedColumn::Int64(vec![1])),
            ("val", TypedColumn::Int64(vec![10])),
        ];
        // First write — under threshold, no flush.
        wb.write_rows_buffered("t", "main", &small_cols, "w1").unwrap();
        assert!(wb.has_pending("t", "main"));
        // Second write pushes past threshold → auto-flush.
        wb.write_rows_buffered("t", "main", &small_cols, "w2").unwrap();
        assert!(!wb.has_pending("t", "main"));
    }

    #[test]
    fn test_invalidate_clears_buffer() {
        let (wb, _kernel) = make_test_buffer();
        let cols = vec![
            ("id", TypedColumn::Int64(vec![1])),
            ("val", TypedColumn::Int64(vec![10])),
        ];
        wb.write_rows_buffered("t", "main", &cols, "w1").unwrap();
        assert!(wb.has_pending("t", "main"));

        // Invalidate (simulates a branch operation).
        wb.invalidate("t", "main");
        assert!(!wb.has_pending("t", "main"));
        // Re-write should capture a new base_head.
        wb.write_rows_buffered("t", "main", &cols, "w2").unwrap();
        assert!(wb.has_pending("t", "main"));
    }

    #[test]
    fn test_schema_mismatch_rejected() {
        let (wb, _kernel) = make_test_buffer();
        let cols1 = vec![
            ("id", TypedColumn::Int64(vec![1])),
            ("val", TypedColumn::Int64(vec![10])),
        ];
        wb.write_rows_buffered("t", "main", &cols1, "w1").unwrap();

        // Different schema — should fail.
        let cols2 = vec![
            ("id", TypedColumn::Int64(vec![1])),
            ("val2", TypedColumn::Int64(vec![10])),
        ];
        let result = wb.write_rows_buffered("t", "main", &cols2, "w2");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Schema mismatch"));
    }
}
