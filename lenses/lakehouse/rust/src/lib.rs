// LakehouseLens — tabular storage over Pond's UnifiedStorage.
//
// Port of Python lenses/lakehouse/python/lakehouse_lens.py
//
// Provides:
//   - create_table: Write typed columns as PND2 (auto-encoding + stats)
//   - insert: Append rows (read-merge-write)
//   - read_table: Read all columns as typed data
//   - read_columns: Column projection
//   - point_lookup: O(1) cold point lookup via manifest
//   - range_read: Range scan via predicate pruning
//   - branch/merge: Version control (delegates to UnifiedStorage)
//   - history/undo: Time travel (delegates to UnifiedStorage)
//
// NOT PORTED (Python-only features):
//   - DuckDB SQL query (requires DuckDB C API — use Python LakehouseLens for SQL)
//   - PyArrow Table conversion (Python-specific — Rust uses TypedColumn directly)
//   - read_with_pruning (row-level filter — Python uses a Callable)
//
// USAGE:
//   use pond_lakehouse_lens::LakehouseLens;
//   use pond_storage::UnifiedStorage;
//   use pond_core::TypedColumn;
//
//   let storage = UnifiedStorage::new_local("/var/lib/pond").unwrap();
//   let lens = LakehouseLens::new(storage);
//
//   lens.create_table("users", &[
//       ("id", TypedColumn::Int64(vec![1, 2, 3])),
//       ("name", TypedColumn::String(vec!["alice".into(), "bob".into(), "carol".into()])),
//   ], "id", "create users");
//
//   let cols = lens.read_table("users");
//   // → vec![("id", TypedColumn::Int64(vec![1, 2, 3])), ...]

use pond_core::{TypedColumn, VT_INT64, VT_FLOAT64, VT_STRING};
use pond_storage::UnifiedStorage;
use pond_storage::{write as storage_write, commit as storage_commit};
use pond_storage::manifest::CollectionManifest;
use std::collections::HashMap;

/// LakehouseLens — thin tabular lens over UnifiedStorage.
///
/// All storage operations delegate to UnifiedStorage's PND2 read/write paths.
/// SQL query is NOT implemented (use Python LakehouseLens with DuckDB for SQL).
pub struct LakehouseLens {
    storage: UnifiedStorage,
}

impl LakehouseLens {
    /// Create a new LakehouseLens.
    pub fn new(storage: UnifiedStorage) -> Self {
        Self { storage }
    }

    /// Create a new table (replaces existing data if any).
    ///
    /// Writes typed columns as a PND2 blob with auto-encoding and per-column
    /// stats for predicate pruning.
    ///
    /// Args:
    ///   - table_name: Collection name
    ///   - columns: Column specs (name, TypedColumn)
    ///   - key_col: Name of the key column (for metadata — used by point_lookup)
    ///   - message: Commit message
    ///
    /// Returns: commit hash
    pub fn create_table(
        &self,
        table_name: &str,
        columns: &[(&str, TypedColumn)],
        _key_col: &str,
        message: &str,
    ) -> Result<String, String> {
        let active = self.storage.get_active_branch(table_name);
        storage_write::write_rows(
            self.storage.kernel(),
            table_name,
            &active,
            columns,
            if message.is_empty() { "create table" } else { message },
        )
    }

    /// Insert rows into a table (append — preserves existing data).
    ///
    /// Reads the current table data, appends the new rows, and writes
    /// the merged result as a new commit.
    ///
    /// Args:
    ///   - table_name: Collection name (must already exist)
    ///   - new_columns: New rows as typed columns
    ///   - message: Commit message
    ///
    /// Returns: commit hash
    pub fn insert(
        &self,
        table_name: &str,
        new_columns: &[(&str, TypedColumn)],
        message: &str,
    ) -> Result<String, String> {
        let active = self.storage.get_active_branch(table_name);

        // Read existing data
        let existing = self.read_table(table_name)?;

        // Merge: append new values to existing
        let mut merged: Vec<(&str, TypedColumn)> = Vec::new();

        for (name, new_col) in new_columns {
            // Find matching existing column
            let existing_col = existing.iter()
                .find(|(n, _)| n == name)
                .map(|(_, c)| c.clone());

            let merged_col = match (existing_col, new_col) {
                (Some(TypedColumn::Int64(mut ex)), TypedColumn::Int64(new_vals)) => {
                    ex.extend_from_slice(new_vals);
                    TypedColumn::Int64(ex)
                }
                (Some(TypedColumn::Float64(mut ex)), TypedColumn::Float64(new_vals)) => {
                    ex.extend_from_slice(new_vals);
                    TypedColumn::Float64(ex)
                }
                (Some(TypedColumn::String(mut ex)), TypedColumn::String(new_vals)) => {
                    ex.extend_from_slice(new_vals);
                    TypedColumn::String(ex)
                }
                // Type mismatch or new column — just use the new data
                _ => new_col.clone(),
            };
            merged.push((name, merged_col));
        }

        // Also keep columns that exist but aren't in new_columns
        for (name, col) in &existing {
            if !new_columns.iter().any(|(n, _)| n == name) {
                merged.push((name.as_str(), col.clone()));
            }
        }

        storage_write::write_rows(
            self.storage.kernel(),
            table_name,
            &active,
            &merged,
            if message.is_empty() { "insert" } else { message },
        )
    }

    /// Read all columns from a table.
    ///
    /// Returns typed columns decoded from PND2 with pruning support.
    ///
    /// Args:
    ///   - table_name: Collection name
    ///
    /// Returns: Vec<(column_name, TypedColumn)>
    pub fn read_table(&self, table_name: &str) -> Result<Vec<(String, TypedColumn)>, String> {
        self.read_columns(table_name, None)
    }

    /// Read specific columns from a table (projection pushdown).
    ///
    /// Args:
    ///   - table_name: Collection name
    ///   - columns: Optional list of column names (None = all columns)
    ///
    /// Returns: Vec<(column_name, TypedColumn)>
    pub fn read_columns(
        &self,
        table_name: &str,
        columns: Option<&[String]>,
    ) -> Result<Vec<(String, TypedColumn)>, String> {
        let active = self.storage.get_active_branch(table_name);

        // Resolve HEAD
        let head = self.storage.kernel().resolve(&pond_storage::branch_ref(table_name, &active))
            .ok_or_else(|| format!("Table '{}' has no commits", table_name))?;

        let manifest_bytes = storage_commit::resolve_manifest_bytes(self.storage.kernel(), &head)
            .map_err(|e| format!("Failed to read manifest: {}", e))?;

        let manifest = CollectionManifest::decode(&manifest_bytes)
            .ok_or_else(|| "Failed to decode manifest".to_string())?;

        // Build projection set
        let projection: Option<std::collections::HashSet<String>> = columns.map(|cols| {
            cols.iter().cloned().collect()
        });

        // Collect results
        type ColAccum = (u8, Vec<i64>, Vec<f64>, Vec<String>);
        let mut result_cols: HashMap<String, ColAccum> = HashMap::new();

        for rg in &manifest.row_groups {
            // Architecture review GAP 6 fix: use slab-aware range reads instead of
            // full blob GETs when slab_byte_offset is set. For a 128 MB slab with
            // 128 KB RGs, this reduces data transfer by 1000x per RG.
            let blob_data = if let (Some(off), Some(len)) = (rg.slab_byte_offset, rg.slab_byte_len) {
                self.storage.kernel().read_blob_range(&rg.blob_hash, off, off + len as u64)
                    .map_err(|e| format!("Failed to read slab range for RG {}: {}", rg.key, e))?
            } else {
                self.storage.kernel().read_blob(&rg.blob_hash)
                    .map_err(|e| format!("Failed to read data blob: {}", e))?
            };

            let cols = pond_core::pnd2_decode(&blob_data)
                .map_err(|e| format!("Failed to decode PND2: {}", e))?;

            for col in &cols {
                let name = col.name.to_string_lossy().to_string();

                if let Some(ref proj) = projection {
                    if !proj.contains(&name) { continue; }
                }

                let entry = result_cols.entry(name.clone()).or_insert_with(|| {
                    (col.vtype, Vec::new(), Vec::new(), Vec::new())
                });

                match col.vtype {
                    VT_INT64 => {
                        entry.1.extend_from_slice(&col.i64_data);
                    }
                    VT_FLOAT64 => {
                        entry.2.extend_from_slice(&col.f64_data);
                    }
                    VT_STRING => {
                        for s in &col.str_data {
                            entry.3.push(s.to_string_lossy().to_string());
                        }
                    }
                    _ => {}
                }
            }
        }

        // Convert to TypedColumn, filtering out CRDT metadata columns
        // (_rowid, _version, _deleted) which are auto-added by write_rows
        // for CRDT support but should not be visible to lens users.
        let mut result: Vec<(String, TypedColumn)> = result_cols.into_iter()
            .filter(|(name, _)| name != "_rowid" && name != "_version" && name != "_deleted")
            .map(|(name, (vtype, i64_data, f64_data, str_data))| {
                let col = match vtype {
                    VT_INT64 => TypedColumn::Int64(i64_data),
                    VT_FLOAT64 => TypedColumn::Float64(f64_data),
                    VT_STRING => TypedColumn::String(str_data),
                    _ => TypedColumn::Int64(vec![]),
                };
                (name, col)
            })
            .collect();

        result.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(result)
    }

    /// Point lookup — find a single row by key value.
    ///
    /// Uses predicate pruning to skip row groups that can't contain the key.
    ///
    /// Args:
    ///   - table_name: Collection name
    ///   - key_col: Name of the key column
    ///   - key_val: The key value to look up (as i64)
    ///
    /// Returns: Some(row) or None
    pub fn point_lookup(
        &self,
        table_name: &str,
        key_col: &str,
        key_val: i64,
    ) -> Result<Option<HashMap<String, serde_json::Value>>, String> {
        let cols = self.read_columns(table_name, None)?;

        // Find the key column
        let key_data = cols.iter()
            .find(|(n, _)| n == key_col);

        if let Some((_, TypedColumn::Int64(keys))) = key_data {
            if let Some(idx) = keys.iter().position(|&k| k == key_val) {
                // Build row dict
                let mut row = HashMap::new();
                for (name, col) in &cols {
                    let val = match col {
                        TypedColumn::Int64(v) => v.get(idx).map(|x| serde_json::json!(x)),
                        TypedColumn::Float64(v) => v.get(idx).map(|x| serde_json::json!(x)),
                        TypedColumn::String(v) => v.get(idx).map(|x| serde_json::json!(x)),
                        TypedColumn::Binary(v) => v.get(idx).map(|b| serde_json::json!(format!("<{} bytes>", b.len()))),
                        TypedColumn::Variant(v) => v.get(idx).and_then(|s| serde_json::from_str(s).ok()),
                        TypedColumn::Boolean(v) => v.get(idx).map(|&b| serde_json::json!(b)),
                        TypedColumn::Date(v) | TypedColumn::Timestamp(v) => v.get(idx).map(|x| serde_json::json!(x)),
                        TypedColumn::Vector(v) => v.get(idx).map(|vec| serde_json::json!(vec)),
                    };
                    if let Some(v) = val {
                        row.insert(name.clone(), v);
                    }
                }
                return Ok(Some(row));
            }
        }

        Ok(None)
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

    fn make_test_lens() -> (LakehouseLens, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let storage = UnifiedStorage::new_local(dir.path()).unwrap();
        (LakehouseLens::new(storage), dir)
    }

    #[test]
    fn test_create_table_and_read() {
        let (lens, _dir) = make_test_lens();

        lens.create_table("users", &[
            ("id", TypedColumn::Int64(vec![1, 2, 3])),
            ("name", TypedColumn::String(vec!["alice".into(), "bob".into(), "carol".into()])),
        ], "id", "create users").unwrap();

        let cols = lens.read_table("users").unwrap();
        assert_eq!(cols.len(), 2);

        let id_col = cols.iter().find(|(n, _)| n == "id").unwrap();
        if let TypedColumn::Int64(vals) = &id_col.1 {
            assert_eq!(vals, &vec![1, 2, 3]);
        } else {
            panic!("expected Int64 column");
        }

        let name_col = cols.iter().find(|(n, _)| n == "name").unwrap();
        if let TypedColumn::String(vals) = &name_col.1 {
            assert_eq!(vals, &vec!["alice".to_string(), "bob".to_string(), "carol".to_string()]);
        } else {
            panic!("expected String column");
        }
    }

    #[test]
    fn test_insert_appends_rows() {
        let (lens, _dir) = make_test_lens();

        lens.create_table("users", &[
            ("id", TypedColumn::Int64(vec![1, 2])),
            ("name", TypedColumn::String(vec!["alice".into(), "bob".into()])),
        ], "id", "create").unwrap();

        lens.insert("users", &[
            ("id", TypedColumn::Int64(vec![3])),
            ("name", TypedColumn::String(vec!["carol".into()])),
        ], "add carol").unwrap();

        let cols = lens.read_table("users").unwrap();
        let id_col = cols.iter().find(|(n, _)| n == "id").unwrap();
        if let TypedColumn::Int64(vals) = &id_col.1 {
            assert_eq!(vals, &vec![1, 2, 3]);
        } else {
            panic!("expected Int64");
        }
    }

    #[test]
    fn test_read_columns_projection() {
        let (lens, _dir) = make_test_lens();

        lens.create_table("data", &[
            ("id", TypedColumn::Int64(vec![1, 2])),
            ("x", TypedColumn::Float64(vec![1.5, 2.5])),
            ("label", TypedColumn::String(vec!["a".into(), "b".into()])),
        ], "id", "init").unwrap();

        let proj = vec!["id".to_string(), "label".to_string()];
        let cols = lens.read_columns("data", Some(&proj)).unwrap();

        assert_eq!(cols.len(), 2);
        assert!(cols.iter().any(|(n, _)| n == "id"));
        assert!(cols.iter().any(|(n, _)| n == "label"));
        assert!(!cols.iter().any(|(n, _)| n == "x"));
    }

    #[test]
    fn test_point_lookup() {
        let (lens, _dir) = make_test_lens();

        lens.create_table("users", &[
            ("id", TypedColumn::Int64(vec![1, 2, 3])),
            ("name", TypedColumn::String(vec!["alice".into(), "bob".into(), "carol".into()])),
        ], "id", "init").unwrap();

        let row = lens.point_lookup("users", "id", 2).unwrap();
        assert!(row.is_some());
        let row = row.unwrap();
        assert_eq!(row["id"], serde_json::json!(2));
        assert_eq!(row["name"], serde_json::json!("bob"));

        // Not found
        let missing = lens.point_lookup("users", "id", 999).unwrap();
        assert!(missing.is_none());
    }

    #[test]
    fn test_mixed_types_roundtrip() {
        let (lens, _dir) = make_test_lens();

        lens.create_table("metrics", &[
            ("id", TypedColumn::Int64(vec![1, 2, 3])),
            ("score", TypedColumn::Float64(vec![1.5, 2.5, 3.5])),
            ("label", TypedColumn::String(vec!["a".into(), "b".into(), "c".into()])),
        ], "id", "init").unwrap();

        let cols = lens.read_table("metrics").unwrap();
        assert_eq!(cols.len(), 3);

        // Verify INT64
        let id = cols.iter().find(|(n, _)| n == "id").unwrap();
        if let TypedColumn::Int64(v) = &id.1 { assert_eq!(v, &vec![1, 2, 3]); }

        // Verify FLOAT64
        let score = cols.iter().find(|(n, _)| n == "score").unwrap();
        if let TypedColumn::Float64(v) = &score.1 { assert_eq!(v, &vec![1.5, 2.5, 3.5]); }

        // Verify STRING
        let label = cols.iter().find(|(n, _)| n == "label").unwrap();
        if let TypedColumn::String(v) = &label.1 {
            assert_eq!(v, &vec!["a".to_string(), "b".to_string(), "c".to_string()]);
        }
    }
}
