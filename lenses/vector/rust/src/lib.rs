// VectorLens — vector storage with ANN search.
//
// This is a LENS (workload model for vectors). It provides:
//   - insert: Buffer vector inserts (id + vector + metadata)
//   - commit: Flush buffered vectors to PND2 storage
//   - get_vector: Read a single vector by ID
//   - get_all: Read all vectors
//   - search: Find k nearest neighbors (auto-accelerated)
//   - count: Count vectors in a collection
//
// The search method USES index extensions for acceleration:
//   1. Try HNSW index (O(log N)) — from extensions/indexing/hnsw_index/
//   2. Try IVF index (O(n_probe × cluster_size)) — from extensions/indexing/rust/
//   3. Fall back to linear scan (O(N))
//
// The IVF and HNSW implementations are INDEPENDENT extensions that work
// with ANY collection. VectorLens is the workload-specific lens that
// provides a vector API and optionally uses those indexes for acceleration.
//
// USAGE:
//   use pond_vector_lens::VectorLens;
//   use pond_storage::UnifiedStorage;
//
//   let storage = UnifiedStorage::new_local("/var/lib/pond").unwrap();
//   let lens = VectorLens::new(storage);
//
//   lens.insert("vectors", "vec:1", &[0.1, 0.2, 0.3], None);
//   lens.insert("vectors", "vec:2", &[0.4, 0.5, 0.6], None);
//   lens.commit("vectors", "init").unwrap();
//
//   let results = lens.search("vectors", &[0.15, 0.25, 0.35], 5).unwrap();
//   // → [(distance, id), ...]

use pond_core::{TypedColumn, VT_INT64};
use pond_storage::UnifiedStorage;
use pond_storage::write as storage_write;
use pond_storage::manifest::CollectionManifest;
use std::collections::HashMap;
use std::sync::Mutex;

/// VectorLens — vector storage with auto-accelerated ANN search.
///
/// Buffer entry: (id, vector, metadata_json).
type BufferEntry = (String, Vec<f64>, String);

/// Buffer map: collection → list of buffered entries.
type BufferMap = HashMap<String, Vec<BufferEntry>>;

/// This lens stores vectors as PND2 columns (id + dim_0, dim_1, ... as FLOAT64).
/// Search automatically uses HNSW → IVF → linear scan, whichever is available.
pub struct VectorLens {
    storage: UnifiedStorage,
    /// Buffer: collection → Vec<(id, vector, metadata_json)>
    buffer: Mutex<BufferMap>,
}

/// A search result: (distance, vector_id).
pub type SearchResult = (f64, String);

impl VectorLens {
    /// Create a new VectorLens.
    pub fn new(storage: UnifiedStorage) -> Self {
        Self {
            storage,
            buffer: Mutex::new(HashMap::new()),
        }
    }

    /// Insert (or replace) a vector. Buffers the insert; commits on explicit
    /// commit() call (or auto-commits when buffer reaches 10,000 entries).
    ///
    /// Args:
    ///   - collection: Collection name
    ///   - id: Vector ID (string)
    ///   - vector: Vector values (f64)
    ///   - metadata: Optional JSON metadata string
    pub fn insert(&self, collection: &str, id: &str, vector: &[f64], metadata: Option<&str>) {
        let should_flush = {
            let mut buf = self.buffer.lock().unwrap();
            let entry = buf.entry(collection.to_string()).or_default();
            entry.push((id.to_string(), vector.to_vec(), metadata.unwrap_or("{}").to_string()));
            entry.len() >= 10000
        };

        if should_flush {
            let _ = self.commit(collection, "auto-commit (buffer full)");
        }
    }

    /// Commit buffered inserts to PND2 storage.
    ///
    /// Converts buffered vectors to PND2 columns (id as INT64 or STRING,
    /// dim_0, dim_1, ... as FLOAT64) and writes via write_rows.
    ///
    /// Args:
    ///   - collection: Collection name
    ///   - message: Commit message
    ///
    /// Returns: commit hash
    pub fn commit(&self, collection: &str, message: &str) -> Result<String, String> {
        let buffer = {
            let mut buf = self.buffer.lock().unwrap();
            buf.remove(collection).unwrap_or_default()
        };

        if buffer.is_empty() {
            return Err(format!("No staged data for collection '{}'", collection));
        }

        let n_dims = buffer.first().map(|(_, v, _)| v.len()).unwrap_or(0);
        if n_dims == 0 {
            return Err("Cannot commit: vectors have 0 dimensions".to_string());
        }

        // Build PND2 columns: id (INT64 or STRING) + dim_0..dim_N (FLOAT64) + metadata (STRING)
        let mut columns: Vec<(&str, TypedColumn)> = Vec::new();

        // Try to use INT64 IDs (if all IDs are numeric)
        let all_numeric = buffer.iter().all(|(id, _, _)| id.parse::<i64>().is_ok());

        let ids: Vec<String> = buffer.iter().map(|(id, _, _)| id.clone()).collect();
        let id_col = if all_numeric {
            let id_vals: Vec<i64> = ids.iter().map(|id| id.parse::<i64>().unwrap()).collect();
            TypedColumn::Int64(id_vals)
        } else {
            TypedColumn::String(ids)
        };
        columns.push(("id", id_col));

        // Dimension columns
        for d in 0..n_dims {
            let dim_vals: Vec<f64> = buffer.iter().map(|(_, v, _)| v[d]).collect();
            let col_name = format!("dim_{}", d);
            // Need to leak the name — this is a known pattern for dynamic column names
            // In production, we'd use a different API that owns the names
            let leaked = Box::leak(col_name.into_boxed_str());
            columns.push((leaked, TypedColumn::Float64(dim_vals)));
        }

        // Metadata column
        let meta_vals: Vec<String> = buffer.iter().map(|(_, _, m)| m.clone()).collect();
        let leaked_meta = Box::leak("metadata".to_string().into_boxed_str());
        columns.push((leaked_meta, TypedColumn::String(meta_vals)));

        let active = self.storage.get_active_branch(collection);
        storage_write::write_rows(
            self.storage.kernel(),
            collection,
            &active,
            &columns,
            if message.is_empty() { "vector commit" } else { message },
        )
    }

    /// Search for k nearest neighbors.
    ///
    /// Auto-accelerated: tries HNSW index first (O(log N)), then IVF index
    /// (O(n_probe × cluster_size)), then linear scan (O(N)).
    ///
    /// Args:
    ///   - collection: Collection name
    ///   - query: Query vector
    ///   - k: Number of nearest neighbors to return
    ///   - n_probe: IVF clusters to search (default 10)
    ///   - ef: HNSW beam width (default 50)
    ///
    /// Returns: Vec<(distance, vector_id)> sorted by distance
    pub fn search(
        &self,
        collection: &str,
        query: &[f64],
        k: usize,
        n_probe: usize,
        ef: usize,
    ) -> Result<Vec<SearchResult>, String> {
        let kernel = self.storage.kernel();

        // 1. Try HNSW index (O(log N))
        let hnsw = pond_hnsw_index::HNSWIndex::new(kernel);
        if hnsw.stats(collection).is_some() {
            return hnsw.search(collection, query, k, ef);
        }

        // 2. Try IVF index (O(n_probe × cluster_size))
        let ivf = pond_ivf_index::IVFIndex::new(kernel);
        if ivf.stats(collection).is_some() {
            return ivf.search(collection, query, k, n_probe);
        }

        // 3. Linear scan fallback (O(N))
        self.linear_scan(collection, query, k)
    }

    /// Build an IVF index on a collection (uses the IVF extension).
    pub fn build_ivf_index(&self, collection: &str, n_clusters: usize, metric: &str) -> Result<String, String> {
        let ivf = pond_ivf_index::IVFIndex::new(self.storage.kernel());
        ivf.build(collection, n_clusters, metric)
    }

    /// Build an HNSW index on a collection (uses the HNSW extension).
    pub fn build_hnsw_index(&self, collection: &str, m: usize, ef_construction: usize, metric: &str) -> Result<String, String> {
        let hnsw = pond_hnsw_index::HNSWIndex::new(self.storage.kernel());
        hnsw.build(collection, m, ef_construction, None, metric)
    }

    /// Get all vectors from a collection.
    ///
    /// Returns: HashMap<id, (vector, metadata_json)>
    pub fn get_all(&self, collection: &str) -> Result<HashMap<String, (Vec<f64>, String)>, String> {
        let active = self.storage.get_active_branch(collection);

        // JOURNAL-AWARE (ARCHITECTURE.md D3/D6): the branch ref is a CACHE of
        // the last folded snapshot, not the current state — plain journal
        // writes never move it. resolve_packs yields the RG-level read plan
        // (snapshot + live entries, stale compaction packs filtered at RG
        // granularity) so vectors inserted after the last compaction are
        // visible; ids are last-write-wins per insert order.
        let plans = pond_storage::journal::resolve_packs(
            self.storage.kernel(), collection, &active, false,
        )?;
        if plans.is_empty() {
            return Err(format!("Collection '{}' has no commits", collection));
        }

        let mut result: HashMap<String, (Vec<f64>, String)> = HashMap::new();

        for plan in &plans {
            let manifest_bytes = pond_storage::commit::resolve_manifest_bytes(self.storage.kernel(), &plan.pack_hash)
                .map_err(|e| format!("Failed to read manifest: {}", e))?;

            let mut manifest = CollectionManifest::decode(&manifest_bytes)
                .ok_or_else(|| "Failed to decode manifest".to_string())?;

            // D6 plan filter (C11): a partially-covered compaction pack
            // contributes only its NOVEL row groups.
            if let Some(only) = &plan.only_rgs {
                manifest.row_groups.retain(|rg|
                    only.contains(&(rg.blob_hash.clone(), rg.slab_byte_offset)));
            }
            // D7: skip CRDT-update RGs (upsert/delete packs, folded CRDT
            // RGs — stats carry `_deleted`): this columnar pipeline has no
            // CRDT merge, so base + update copies would DUPLICATE rows.
            // Pre-D7 equivalence (shards were never read here); the
            // CRDT-merged surface is read_rows_json_pruned.
            manifest.row_groups.retain(|rg| !rg.is_crdt_update_rg());

            for rg in &manifest.row_groups {
                let blob_data = self.storage.kernel().read_blob(&rg.blob_hash)
                    .map_err(|e| format!("Failed to read data blob: {}", e))?;

                let cols = pond_core::pnd2_decode(&blob_data)
                    .map_err(|e| format!("Failed to decode PND2: {}", e))?;

                // Find ID column (INT64 or STRING)
                let ids: Vec<String> = if let Some(id_col) = cols.iter().find(|c| c.name.to_string_lossy() == "id") {
                    if id_col.vtype == VT_INT64 {
                        id_col.i64_data.iter().map(|v| v.to_string()).collect()
                    } else {
                        id_col.str_data.iter().map(|s| s.to_string_lossy().to_string()).collect()
                    }
                } else {
                    Vec::new()
                };

                // Find dimension columns
                let mut dim_cols: Vec<&pond_core::PondColumn> = cols.iter()
                    .filter(|c| c.name.to_string_lossy().starts_with("dim_"))
                    .collect();
                dim_cols.sort_by_key(|c| c.name.to_string_lossy().to_string());

                // Find metadata column
                let meta_col = cols.iter().find(|c| c.name.to_string_lossy() == "metadata");

                for (i, id) in ids.iter().enumerate() {
                    let vector: Vec<f64> = dim_cols.iter()
                        .filter_map(|c| c.f64_data.get(i).copied())
                        .collect();
                    let metadata = meta_col
                        .and_then(|c| c.str_data.get(i))
                        .map(|s| s.to_string_lossy().to_string())
                        .unwrap_or_else(|| "{}".to_string());
                    result.insert(id.clone(), (vector, metadata));
                }
            }
        }

        Ok(result)
    }

    /// Count vectors in a collection.
    pub fn count(&self, collection: &str) -> Result<usize, String> {
        Ok(self.get_all(collection)?.len())
    }

    /// Get a single vector by ID.
    pub fn get_vector(&self, collection: &str, id: &str) -> Result<Option<(Vec<f64>, String)>, String> {
        Ok(self.get_all(collection)?.remove(id))
    }

    /// Linear scan search (fallback when no index exists).
    fn linear_scan(&self, collection: &str, query: &[f64], k: usize) -> Result<Vec<SearchResult>, String> {
        let all = self.get_all(collection)?;

        let mut scored: Vec<(f64, String)> = all.iter()
            .filter_map(|(id, (vec, _))| {
                if vec.len() != query.len() {
                    return None;
                }
                let dist: f64 = query.iter().zip(vec.iter())
                    .map(|(a, b)| (a - b).powi(2))
                    .sum::<f64>()
                    .sqrt();
                Some((dist, id.clone()))
            })
            .collect();

        scored.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
        scored.truncate(k);
        Ok(scored)
    }

    /// Get a reference to the underlying UnifiedStorage.
    pub fn storage(&self) -> &UnifiedStorage {
        &self.storage
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn make_test_lens() -> (VectorLens, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let storage = UnifiedStorage::new_local(dir.path()).unwrap();
        (VectorLens::new(storage), dir)
    }

    #[test]
    fn test_insert_and_commit() {
        let (lens, _dir) = make_test_lens();

        lens.insert("vectors", "1", &[0.0, 0.0], None);
        lens.insert("vectors", "2", &[10.0, 10.0], None);
        lens.insert("vectors", "3", &[20.0, 20.0], None);

        let hash = lens.commit("vectors", "init").unwrap();
        assert!(!hash.is_empty());
    }

    #[test]
    fn test_get_all() {
        let (lens, _dir) = make_test_lens();

        lens.insert("vectors", "1", &[0.0, 0.0], None);
        lens.insert("vectors", "2", &[10.0, 10.0], None);
        lens.commit("vectors", "init").unwrap();

        let all = lens.get_all("vectors").unwrap();
        assert_eq!(all.len(), 2);
        assert!(all.contains_key("1"));
        assert!(all.contains_key("2"));
        assert_eq!(all.get("1").unwrap().0, vec![0.0, 0.0]);
        assert_eq!(all.get("2").unwrap().0, vec![10.0, 10.0]);
    }

    #[test]
    fn test_count() {
        let (lens, _dir) = make_test_lens();

        lens.insert("vectors", "1", &[0.0], None);
        lens.insert("vectors", "2", &[1.0], None);
        lens.insert("vectors", "3", &[2.0], None);
        lens.commit("vectors", "init").unwrap();

        assert_eq!(lens.count("vectors").unwrap(), 3);
    }

    #[test]
    fn test_get_vector() {
        let (lens, _dir) = make_test_lens();

        lens.insert("vectors", "1", &[1.0, 2.0, 3.0], Some("{\"label\":\"test\"}"));
        lens.commit("vectors", "init").unwrap();

        let vec = lens.get_vector("vectors", "1").unwrap();
        assert!(vec.is_some());
        let (v, meta) = vec.unwrap();
        assert_eq!(v, vec![1.0, 2.0, 3.0]);
        assert!(meta.contains("test"));
    }

    #[test]
    fn test_get_vector_not_found() {
        let (lens, _dir) = make_test_lens();

        lens.insert("vectors", "1", &[0.0], None);
        lens.commit("vectors", "init").unwrap();

        let result = lens.get_vector("vectors", "999").unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_linear_scan_search() {
        let (lens, _dir) = make_test_lens();

        lens.insert("vecs", "1", &[0.0, 0.0], None);
        lens.insert("vecs", "2", &[1.0, 1.0], None);
        lens.insert("vecs", "3", &[5.0, 5.0], None);
        lens.insert("vecs", "4", &[10.0, 10.0], None);
        lens.commit("vecs", "init").unwrap();

        // Search for nearest to [0.5, 0.5]
        let results = lens.search("vecs", &[0.5, 0.5], 2, 10, 50).unwrap();
        assert_eq!(results.len(), 2);
        // Nearest should be id "1" (distance ~0.707) or "2" (distance ~0.707)
        assert!(results[0].1 == "1" || results[0].1 == "2");
        // Distance should be small
        assert!(results[0].0 < 2.0);
    }

    #[test]
    fn test_search_empty_collection() {
        let (lens, _dir) = make_test_lens();

        lens.insert("vecs", "1", &[0.0], None);
        lens.commit("vecs", "init").unwrap();

        let results = lens.search("vecs", &[1.0], 5, 10, 50).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].1, "1");
    }

    #[test]
    fn test_metadata_storage() {
        let (lens, _dir) = make_test_lens();

        lens.insert("vecs", "1", &[0.0, 0.0], Some(r#"{"label":"alpha"}"#));
        lens.insert("vecs", "2", &[1.0, 1.0], Some(r#"{"label":"beta"}"#));
        lens.commit("vecs", "init").unwrap();

        let all = lens.get_all("vecs").unwrap();
        assert!(all.get("1").unwrap().1.contains("alpha"));
        assert!(all.get("2").unwrap().1.contains("beta"));
    }
}
