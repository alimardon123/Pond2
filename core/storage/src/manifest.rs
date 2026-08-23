// Manifest module — CollectionManifest (RowGroupEntry, ColumnStats, encode/decode)
//
// FAITHFUL PORT of Python's collection_manifest.py.
//
// The manifest is a binary blob that lists all row groups in a collection
// with their blob hashes, row counts, and per-column statistics (min/max/null_count).
// It enables zone-map pruning (skip row groups that can't match a predicate).
//
// Binary format (PMAN):
//   Magic: "PMAN" (4B)
//   Version: 1 (1B)
//   n_columns: u16 LE (2B)
//   key_col_len: u8 (1B)
//   key_col: bytes (key_col_len)
//   Schema: per col: name_len(1B) + name + vtype(1B)
//   n_row_groups: u32 LE (4B)
//   Row groups: per rg: rg_key_len(1B) + rg_key + blob_hash(64B string) + n_rows(4B)
//               + per col: has_stats(1B) + [min + max] + null_count(4B)
//   Optional: partition_spec (u32 LE length + JSON bytes)
//   Optional: schema_version (u32 LE)
//   Optional: bloom_filter_ref (u32 LE length + string)
//   Optional: parent_manifest (u32 LE length + string)

const PMAN_MAGIC: &[u8] = b"PMAN";
const PMAN_VERSION: u8 = 1;

// Value types (match pond_core)
const VT_INT64: u8 = 1;
const VT_FLOAT64: u8 = 2;
#[allow(dead_code)]
const VT_STRING: u8 = 3;
#[allow(dead_code)]
const VT_NULL: u8 = 4;
#[allow(dead_code)]
const VT_BINARY: u8 = 5;

// ---------------------------------------------------------------------------
// ColumnStatsEntry — per-column statistics for a row group
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default)]
pub struct ColumnStatsEntry {
    pub name: String,
    pub value_type: u8,
    pub min: Option<Vec<u8>>,    // raw bytes (i64/f64 LE, or string bytes)
    pub max: Option<Vec<u8>>,
    pub null_count: u32,
}

impl ColumnStatsEntry {
    /// Can this row group be pruned for the given predicate?
    /// Returns true if the row group CANNOT match (should be skipped).
    pub fn can_prune(&self, op: &str, value: &[u8]) -> bool {
        // Only prune if we have min/max stats
        let (min, max) = match (&self.min, &self.max) {
            (Some(m), Some(x)) => (m, x),
            _ => return false,
        };

        match (self.value_type, op) {
            (VT_INT64, "=") | (VT_FLOAT64, "=") => {
                // Prune if value is outside [min, max]
                let val = if self.value_type == VT_INT64 {
                    i64_from_le_bytes(value)
                } else {
                    // For f64, compare as bytes (works for same-endian)
                    0
                };
                let min_val = if self.value_type == VT_INT64 {
                    i64_from_le_bytes(min)
                } else { 0 };
                let max_val = if self.value_type == VT_INT64 {
                    i64_from_le_bytes(max)
                } else { 0 };
                if self.value_type == VT_INT64 {
                    return val < min_val || val > max_val;
                }
                false
            }
            (VT_INT64, "<") | (VT_INT64, "<=") => {
                let val = i64_from_le_bytes(value);
                let min_val = i64_from_le_bytes(min);
                val < min_val
            }
            (VT_INT64, ">") | (VT_INT64, ">=") => {
                let val = i64_from_le_bytes(value);
                let max_val = i64_from_le_bytes(max);
                val > max_val
            }
            _ => false, // don't prune for string/binary or unknown ops
        }
    }
}

fn i64_from_le_bytes(b: &[u8]) -> i64 {
    if b.len() >= 8 {
        i64::from_le_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]])
    } else {
        0
    }
}

// ---------------------------------------------------------------------------
// RowGroupEntry — one row group in the manifest
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct RowGroupEntry {
    pub key: String,
    pub blob_hash: String,
    pub n_rows: u32,
    pub columns: Vec<ColumnStatsEntry>,
}

impl RowGroupEntry {
    /// Can this row group be pruned given a list of predicates?
    /// Returns true if the row group CANNOT match ANY predicate.
    pub fn can_prune(&self, predicates: &[(String, String, Vec<u8>)]) -> bool {
        for (col_name, op, value) in predicates {
            if let Some(col) = self.columns.iter().find(|c| c.name == *col_name) {
                if col.can_prune(op, value) {
                    return true; // this predicate prunes the row group
                }
            }
        }
        false
    }
}

// ---------------------------------------------------------------------------
// CollectionManifest — the full manifest for a collection
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct CollectionManifest {
    pub columns: Vec<(String, u8)>,  // (name, value_type)
    pub key_col: String,
    pub row_groups: Vec<RowGroupEntry>,
    pub partition_spec: Option<String>,  // JSON string
    pub schema_version: Option<u32>,
    pub bloom_filter_ref: Option<String>,
    pub parent_manifest: Option<String>,
}

impl CollectionManifest {
    pub fn new(columns: Vec<(String, u8)>, key_col: String) -> Self {
        Self {
            columns,
            key_col,
            row_groups: Vec::new(),
            partition_spec: None,
            schema_version: None,
            bloom_filter_ref: None,
            parent_manifest: None,
        }
    }

    pub fn add_row_group(&mut self, entry: RowGroupEntry) {
        self.row_groups.push(entry);
    }

    pub fn n_row_groups(&self) -> usize {
        self.row_groups.len()
    }

    /// Iterate row groups, applying pruning if predicates are given.
    /// Returns only row groups that might match the predicates.
    pub fn scan_with_pruning(&self, predicates: Option<&[(String, String, Vec<u8>)]>) -> Vec<&RowGroupEntry> {
        match predicates {
            Some(preds) => self.row_groups.iter()
                .filter(|rg| !rg.can_prune(preds))
                .collect(),
            None => self.row_groups.iter().collect(),
        }
    }

    /// Encode the manifest to PMAN binary format.
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(PMAN_MAGIC);
        buf.push(PMAN_VERSION);

        // Schema
        buf.extend_from_slice(&(self.columns.len() as u16).to_le_bytes());
        let key_col_bytes = self.key_col.as_bytes();
        buf.push(key_col_bytes.len() as u8);
        buf.extend_from_slice(key_col_bytes);
        for (name, vtype) in &self.columns {
            let name_bytes = name.as_bytes();
            buf.push(name_bytes.len() as u8);
            buf.extend_from_slice(name_bytes);
            buf.push(*vtype);
        }

        // Row groups
        buf.extend_from_slice(&(self.row_groups.len() as u32).to_le_bytes());
        for rg in &self.row_groups {
            let key_bytes = rg.key.as_bytes();
            buf.push(key_bytes.len() as u8);
            buf.extend_from_slice(key_bytes);
            let hash_bytes = rg.blob_hash.as_bytes();
            buf.extend_from_slice(&(hash_bytes.len() as u32).to_le_bytes());
            buf.extend_from_slice(hash_bytes);
            buf.extend_from_slice(&rg.n_rows.to_le_bytes());

            // Per-column stats
            for col in &rg.columns {
                if let (Some(min), Some(max)) = (&col.min, &col.max) {
                    buf.push(1); // has stats
                    buf.extend_from_slice(&(min.len() as u32).to_le_bytes());
                    buf.extend_from_slice(min);
                    buf.extend_from_slice(&(max.len() as u32).to_le_bytes());
                    buf.extend_from_slice(max);
                } else {
                    buf.push(0); // no stats
                }
                buf.extend_from_slice(&col.null_count.to_le_bytes());
            }
        }

        // Optional: partition_spec
        if let Some(ref spec) = self.partition_spec {
            let spec_bytes = spec.as_bytes();
            buf.extend_from_slice(&(spec_bytes.len() as u32).to_le_bytes());
            buf.extend_from_slice(spec_bytes);
        } else {
            buf.extend_from_slice(&0u32.to_le_bytes());
        }

        // Optional: schema_version
        if let Some(v) = self.schema_version {
            buf.extend_from_slice(&v.to_le_bytes());
        } else {
            buf.extend_from_slice(&0u32.to_le_bytes());
        }

        buf
    }

    /// Decode a PMAN binary blob into a CollectionManifest.
    pub fn decode(data: &[u8]) -> Option<Self> {
        if data.len() < 7 || &data[0..4] != PMAN_MAGIC {
            return None;
        }
        let mut pos = 5; // skip magic + version

        // Read schema
        let n_columns = u16::from_le_bytes([data[pos], data[pos+1]]) as usize;
        pos += 2;
        let key_col_len = data[pos] as usize;
        pos += 1;
        let key_col = String::from_utf8_lossy(&data[pos..pos+key_col_len]).to_string();
        pos += key_col_len;

        let mut columns = Vec::with_capacity(n_columns);
        for _ in 0..n_columns {
            let name_len = data[pos] as usize;
            pos += 1;
            let name = String::from_utf8_lossy(&data[pos..pos+name_len]).to_string();
            pos += name_len;
            let vtype = data[pos];
            pos += 1;
            columns.push((name, vtype));
        }

        // Read row groups
        let n_row_groups = u32::from_le_bytes([data[pos], data[pos+1], data[pos+2], data[pos+3]]) as usize;
        pos += 4;

        let mut row_groups = Vec::with_capacity(n_row_groups);
        for _ in 0..n_row_groups {
            let key_len = data[pos] as usize;
            pos += 1;
            let key = String::from_utf8_lossy(&data[pos..pos+key_len]).to_string();
            pos += key_len;
            let hash_len = u32::from_le_bytes([data[pos], data[pos+1], data[pos+2], data[pos+3]]) as usize;
            pos += 4;
            let blob_hash = String::from_utf8_lossy(&data[pos..pos+hash_len]).to_string();
            pos += hash_len;
            let n_rows = u32::from_le_bytes([data[pos], data[pos+1], data[pos+2], data[pos+3]]);
            pos += 4;

            // Per-column stats
            let mut col_stats = Vec::with_capacity(n_columns);
            for _ in 0..n_columns {
                let has_stats = data[pos];
                pos += 1;
                let (min, max) = if has_stats != 0 {
                    let min_len = u32::from_le_bytes([data[pos], data[pos+1], data[pos+2], data[pos+3]]) as usize;
                    pos += 4;
                    let min = data[pos..pos+min_len].to_vec();
                    pos += min_len;
                    let max_len = u32::from_le_bytes([data[pos], data[pos+1], data[pos+2], data[pos+3]]) as usize;
                    pos += 4;
                    let max = data[pos..pos+max_len].to_vec();
                    pos += max_len;
                    (Some(min), Some(max))
                } else {
                    (None, None)
                };
                let null_count = u32::from_le_bytes([data[pos], data[pos+1], data[pos+2], data[pos+3]]);
                pos += 4;
                col_stats.push(ColumnStatsEntry {
                    name: String::new(), // filled in below
                    value_type: 0,
                    min, max, null_count,
                });
            }

            // Fill in column names and types from schema
            for (i, col) in col_stats.iter_mut().enumerate() {
                if i < columns.len() {
                    col.name = columns[i].0.clone();
                    col.value_type = columns[i].1;
                }
            }

            row_groups.push(RowGroupEntry {
                key, blob_hash, n_rows, columns: col_stats,
            });
        }

        // Optional fields (may not be present in older manifests)
        let partition_spec = if pos + 4 <= data.len() {
            let spec_len = u32::from_le_bytes([data[pos], data[pos+1], data[pos+2], data[pos+3]]) as usize;
            pos += 4;
            if spec_len > 0 && pos + spec_len <= data.len() {
                let spec = String::from_utf8_lossy(&data[pos..pos+spec_len]).to_string();
                pos += spec_len;
                Some(spec)
            } else {
                None
            }
        } else {
            None
        };

        let schema_version = if pos + 4 <= data.len() {
            Some(u32::from_le_bytes([data[pos], data[pos+1], data[pos+2], data[pos+3]]))
        } else {
            None
        };

        Some(Self {
            columns, key_col, row_groups,
            partition_spec, schema_version,
            bloom_filter_ref: None,
            parent_manifest: None,
        })
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_manifest_encode_decode() {
        let mut manifest = CollectionManifest::new(
            vec![("id".to_string(), VT_INT64), ("name".to_string(), VT_STRING)],
            "id".to_string(),
        );
        manifest.add_row_group(RowGroupEntry {
            key: "rg_0000000000".to_string(),
            blob_hash: "abc123def456".to_string(),
            n_rows: 1000,
            columns: vec![
                ColumnStatsEntry {
                    name: "id".to_string(),
                    value_type: VT_INT64,
                    min: Some(0i64.to_le_bytes().to_vec()),
                    max: Some(999i64.to_le_bytes().to_vec()),
                    null_count: 0,
                },
                ColumnStatsEntry {
                    name: "name".to_string(),
                    value_type: VT_STRING,
                    min: None, max: None, null_count: 0,
                },
            ],
        });

        let encoded = manifest.encode();
        let decoded = CollectionManifest::decode(&encoded).unwrap();

        assert_eq!(decoded.columns.len(), 2);
        assert_eq!(decoded.columns[0].0, "id");
        assert_eq!(decoded.columns[0].1, VT_INT64);
        assert_eq!(decoded.key_col, "id");
        assert_eq!(decoded.row_groups.len(), 1);
        assert_eq!(decoded.row_groups[0].key, "rg_0000000000");
        assert_eq!(decoded.row_groups[0].blob_hash, "abc123def456");
        assert_eq!(decoded.row_groups[0].n_rows, 1000);
        assert_eq!(decoded.row_groups[0].columns[0].min, Some(0i64.to_le_bytes().to_vec()));
    }

    #[test]
    fn test_pruning() {
        let rg = RowGroupEntry {
            key: "rg_0".to_string(),
            blob_hash: "hash".to_string(),
            n_rows: 100,
            columns: vec![
                ColumnStatsEntry {
                    name: "age".to_string(),
                    value_type: VT_INT64,
                    min: Some(0i64.to_le_bytes().to_vec()),
                    max: Some(50i64.to_le_bytes().to_vec()),
                    null_count: 0,
                },
            ],
        };

        // age > 100 → should prune (max is 50)
        let preds = vec![("age".to_string(), ">".to_string(), 100i64.to_le_bytes().to_vec())];
        assert!(rg.can_prune(&preds));

        // age > 25 → should NOT prune (max is 50, so 25 is in range)
        let preds = vec![("age".to_string(), ">".to_string(), 25i64.to_le_bytes().to_vec())];
        assert!(!rg.can_prune(&preds));
    }
}
