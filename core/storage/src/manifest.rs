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
//   Version: 2 (1B)  [v2 adds slab_byte_offset + slab_byte_len per RG]
//   n_columns: u16 LE (2B)
//   key_col_len: u8 (1B)
//   key_col: bytes (key_col_len)
//   Schema: per col: name_len(1B) + name + vtype(1B)
//   n_row_groups: u32 LE (4B)
//   Row groups: per rg:
//     rg_key_len(1B) + rg_key
//     blob_hash_len(u32 LE) + blob_hash
//     n_rows(u32 LE)
//     per col: has_stats(1B) + [min + max] + null_count(4B)
//     [v2 only] slab_byte_offset(u64 LE) + slab_byte_len(u32 LE)
//   Optional: partition_spec (u32 LE length + JSON bytes)
//   Optional: schema_version (u32 LE)
//   Optional: bloom_filter_ref (u32 LE length + string)
//   Optional: parent_manifest (u32 LE length + string)

const PMAN_MAGIC: &[u8] = b"PMAN";
// --- PMAN v2 write constants ---
const PMAN_VERSION: u8 = 2;
/// PMAN v3 — root manifest pointing to leaf manifests.
/// Used when a collection has more row groups than fit in one leaf (MAX_LEAF_RGS).
const PMAN_VERSION_ROOT: u8 = 3;

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
    /// Content hash of the blob holding this row group's data.
    /// For slab-backed RGs, this is the PSLB slab hash (not an individual PND2 hash).
    pub blob_hash: String,
    pub n_rows: u32,
    pub columns: Vec<ColumnStatsEntry>,
    /// If this RG lives inside a PSLB slab, the byte offset within the slab blob.
    /// `None` = standalone blob (read via `get_blob(blob_hash)`).
    /// `Some(offset)` = slab-backed (read via `get_blob_range(blob_hash, offset, offset + len)`).
    pub slab_byte_offset: Option<u64>,
    /// If this RG lives inside a PSLB slab, the byte length of the RG's PND2 payload.
    pub slab_byte_len: Option<u32>,
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

    /// Encode the manifest to PMAN binary format (version 2).
    ///
    /// Version 2 adds `slab_byte_offset` (u64 LE) and `slab_byte_len`
    /// (u32 LE) per row group, enabling slab-backed reads via
    /// `get_blob_range` instead of full `get_blob`.
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(PMAN_MAGIC);
        buf.push(PMAN_VERSION); // v2

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

            // v2: slab byte offset + len (12 bytes per RG)
            buf.extend_from_slice(&rg.slab_byte_offset.unwrap_or(0).to_le_bytes());
            buf.extend_from_slice(&rg.slab_byte_len.unwrap_or(0).to_le_bytes());
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
    ///
    /// Supports both v1 (no slab fields) and v2 (slab_byte_offset +
    /// slab_byte_len per row group). v1 manifests decode with
    /// `slab_byte_offset: None, slab_byte_len: None` for all RGs.
    pub fn decode(data: &[u8]) -> Option<Self> {
        if data.len() < 7 || &data[0..4] != PMAN_MAGIC {
            return None;
        }
        let version = data[4];
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
                slab_byte_offset: None,
                slab_byte_len: None,
            });

            // v2: read slab byte offset + len (12 bytes per RG)
            if version >= 2 {
                let offset = u64::from_le_bytes([
                    data[pos], data[pos+1], data[pos+2], data[pos+3],
                    data[pos+4], data[pos+5], data[pos+6], data[pos+7],
                ]);
                pos += 8;
                let len = u32::from_le_bytes([data[pos], data[pos+1], data[pos+2], data[pos+3]]);
                pos += 4;
                let rg = row_groups.last_mut().unwrap();
                if offset != 0 || len != 0 {
                    rg.slab_byte_offset = Some(offset);
                    rg.slab_byte_len = Some(len);
                }
            }
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
// PMAN v3 — Root Manifest (two-level manifest tree)
//
// At PB scale a flat PMAN v2 manifest grows to ~670 MB (8192+ RGs × 82 KB/entry),
// exceeding practical S3 GET reliability. The two-level tree splits this into:
//   ROOT MANIFEST (PMAN v3, ~100 B/leaf): schema + leaf pointer entries
//   LEAF MANIFESTS (PMAN v2, unchanged): per-leaf row group lists
//
// Read path: 1 GET (root) → prune leaves by key range → parallel GET leaves
//   → prune RGs within leaves → range-read slabs (existing code).
//
// At 8.2M RGs (1 TB, 128 MB/slab, 1024 RGs/slab):
//   - 8,000 leaves × 100 B = 800 KB root (fits in one S3 GET)
//   - Each leaf: 1,024 RGs × ~400 B = ~400 KB (fits in one S3 GET)
//   - Selective 1% query: 1 root + 8 leaf GETs (32-way parallel) ≈ 150 ms
//
// Binary format (PMAN v3):
//   Magic: "PMAN" (4B)
//   Version: 3 (1B)
//   n_columns: u16 LE (2B)
//   key_col_len: u8 (1B) + key_col
//   Schema: per col: name_len(1B) + name + vtype(1B)
//   n_leaves: u32 LE (4B)
//   Leaves: per leaf:
//     leaf_hash_len(u32 LE) + leaf_hash
//     n_row_groups(u32 LE)
//     total_data_bytes(u64 LE)
//     key_min_len(u16 LE) + key_min (optional, 0 = None)
//     key_max_len(u16 LE) + key_max (optional, 0 = None)
//   Optional: partition_spec (u32 LE length + JSON)
//   Optional: schema_version (u32 LE)
// ---------------------------------------------------------------------------

/// Maximum row groups per leaf manifest. Matches SLAB_TARGET_RG_COUNT in write.rs.
/// At 1024 RGs/leaf, each leaf is ~400 KB — fits in one S3 GET.
pub const MAX_LEAF_RGS: usize = 1024;

/// Entry in a PMAN v3 root manifest pointing to a PMAN v2 leaf manifest.
#[derive(Debug, Clone)]
pub struct LeafEntry {
    /// Content hash of the leaf manifest blob (PMAN v2 format).
    pub leaf_hash: String,
    /// Number of row groups in this leaf.
    pub n_row_groups: u32,
    /// Sum of all PND2 data bytes in this leaf (for size estimation).
    pub total_data_bytes: u64,
    /// Min value of the key column across all RGs in this leaf.
    /// Used for partition pruning — skip entire leaves whose key range
    /// doesn't overlap the query predicates. `None` if unknown.
    pub key_min: Option<Vec<u8>>,
    /// Max value of the key column across all RGs in this leaf.
    pub key_max: Option<Vec<u8>>,
}

/// A PMAN v3 root manifest — points to leaf manifests instead of row groups.
///
/// This is the top level of a two-level manifest tree. Each leaf is a
/// standard PMAN v2 `CollectionManifest` that holds the actual row group
/// entries. The root enables O(log_B N) manifest fetch (B=1024) instead of
/// O(N) for a flat manifest.
#[derive(Debug, Clone)]
pub struct RootManifest {
    pub columns: Vec<(String, u8)>,
    pub key_col: String,
    pub leaves: Vec<LeafEntry>,
    pub partition_spec: Option<String>,
    pub schema_version: Option<u32>,
}

impl RootManifest {
    /// Create a new empty root manifest.
    pub fn new(columns: Vec<(String, u8)>, key_col: String) -> Self {
        Self {
            columns,
            key_col,
            leaves: Vec::new(),
            partition_spec: None,
            schema_version: None,
        }
    }

    /// Encode the root manifest to PMAN v3 binary format.
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(PMAN_MAGIC);
        buf.push(PMAN_VERSION_ROOT);

        // Schema (same layout as v2)
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

        // Leaves
        buf.extend_from_slice(&(self.leaves.len() as u32).to_le_bytes());
        for leaf in &self.leaves {
            let hash_bytes = leaf.leaf_hash.as_bytes();
            buf.extend_from_slice(&(hash_bytes.len() as u32).to_le_bytes());
            buf.extend_from_slice(hash_bytes);
            buf.extend_from_slice(&leaf.n_row_groups.to_le_bytes());
            buf.extend_from_slice(&leaf.total_data_bytes.to_le_bytes());

            // key_min
            match &leaf.key_min {
                Some(min) => {
                    buf.extend_from_slice(&(min.len() as u16).to_le_bytes());
                    buf.extend_from_slice(min);
                }
                None => {
                    buf.extend_from_slice(&0u16.to_le_bytes());
                }
            }
            // key_max
            match &leaf.key_max {
                Some(max) => {
                    buf.extend_from_slice(&(max.len() as u16).to_le_bytes());
                    buf.extend_from_slice(max);
                }
                None => {
                    buf.extend_from_slice(&0u16.to_le_bytes());
                }
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

    /// Decode a PMAN v3 binary blob into a RootManifest.
    /// Returns `None` if the data is not a valid PMAN v3 blob.
    pub fn decode(data: &[u8]) -> Option<Self> {
        if data.len() < 7 || &data[0..4] != PMAN_MAGIC {
            return None;
        }
        let version = data[4];
        if version != PMAN_VERSION_ROOT {
            return None;
        }
        let mut pos = 5;

        // Read schema (same layout as v2)
        let n_columns = u16::from_le_bytes([data[pos], data[pos + 1]]) as usize;
        pos += 2;
        let key_col_len = data[pos] as usize;
        pos += 1;
        let key_col = String::from_utf8_lossy(&data[pos..pos + key_col_len]).to_string();
        pos += key_col_len;

        let mut columns = Vec::with_capacity(n_columns);
        for _ in 0..n_columns {
            let name_len = data[pos] as usize;
            pos += 1;
            let name = String::from_utf8_lossy(&data[pos..pos + name_len]).to_string();
            pos += name_len;
            let vtype = data[pos];
            pos += 1;
            columns.push((name, vtype));
        }

        // Read leaves
        let n_leaves = u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]) as usize;
        pos += 4;

        let mut leaves = Vec::with_capacity(n_leaves);
        for _ in 0..n_leaves {
            // leaf_hash
            let hash_len = u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]) as usize;
            pos += 4;
            if pos + hash_len > data.len() { return None; }
            let leaf_hash = String::from_utf8_lossy(&data[pos..pos + hash_len]).to_string();
            pos += hash_len;

            // n_row_groups + total_data_bytes (12 bytes)
            if pos + 12 > data.len() { return None; }
            let n_row_groups = u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
            pos += 4;
            let total_data_bytes = u64::from_le_bytes([
                data[pos], data[pos + 1], data[pos + 2], data[pos + 3],
                data[pos + 4], data[pos + 5], data[pos + 6], data[pos + 7],
            ]);
            pos += 8;

            // key_min (u16 len + bytes; 0 = None)
            if pos + 2 > data.len() { return None; }
            let key_min_len = u16::from_le_bytes([data[pos], data[pos + 1]]) as usize;
            pos += 2;
            let key_min = if key_min_len > 0 {
                if pos + key_min_len > data.len() { return None; }
                let min = data[pos..pos + key_min_len].to_vec();
                pos += key_min_len;
                Some(min)
            } else {
                None
            };

            // key_max (u16 len + bytes; 0 = None)
            if pos + 2 > data.len() { return None; }
            let key_max_len = u16::from_le_bytes([data[pos], data[pos + 1]]) as usize;
            pos += 2;
            let key_max = if key_max_len > 0 {
                if pos + key_max_len > data.len() { return None; }
                let max = data[pos..pos + key_max_len].to_vec();
                pos += key_max_len;
                Some(max)
            } else {
                None
            };

            leaves.push(LeafEntry {
                leaf_hash, n_row_groups, total_data_bytes, key_min, key_max,
            });
        }

        // Optional: partition_spec
        let partition_spec = if pos + 4 <= data.len() {
            let spec_len = u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]) as usize;
            pos += 4;
            if spec_len > 0 && pos + spec_len <= data.len() {
                let spec = String::from_utf8_lossy(&data[pos..pos + spec_len]).to_string();
                pos += spec_len;
                Some(spec)
            } else {
                None
            }
        } else {
            None
        };

        // Optional: schema_version
        let schema_version = if pos + 4 <= data.len() {
            Some(u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]))
        } else {
            None
        };

        Some(Self {
            columns, key_col, leaves, partition_spec, schema_version,
        })
    }

    /// Prune leaves by key column predicates.
    /// Returns indices of leaves that MAY match (cannot be pruned).
    ///
    /// For a leaf to be prunable, BOTH conditions must hold:
    ///   - The leaf has key_min and key_max set
    ///   - The predicate's value is entirely outside [key_min, key_max]
    ///
    /// Only supports INT64 key column predicates (op: >, >=, <, <=, =).
    /// Leaves without key stats are never pruned (conservative).
    pub fn prune_leaves(&self, predicates: &[(String, String, Vec<u8>)]) -> Vec<usize> {
        if predicates.is_empty() {
            return (0..self.leaves.len()).collect();
        }

        // Only key-column predicates can prune at the leaf level.
        // For simplicity, we check ALL predicates against the key column.
        // If ANY predicate eliminates the leaf, it's pruned.
        (0..self.leaves.len()).filter(|&i| {
            let leaf = &self.leaves[i];
            let (min, max) = match (&leaf.key_min, &leaf.key_max) {
                (Some(m), Some(x)) => (m, x),
                _ => return true, // no stats → can't prune
            };

            for (col_name, op, value) in predicates {
                // Only prune on the key column
                if col_name.as_str() != self.key_col { continue; }

                let val = i64_from_le_bytes(value);
                let min_val = i64_from_le_bytes(min);
                let max_val = i64_from_le_bytes(max);

                let pruned = match op.as_str() {
                    ">" | ">=" => val > max_val,
                    "<" | "<=" => val < min_val,
                    "=" => val < min_val || val > max_val,
                    _ => false,
                };

                if pruned { return false; }
            }
            true
        }).collect()
    }

    /// Total row groups across all leaves.
    pub fn total_row_groups(&self) -> u64 {
        self.leaves.iter().map(|l| l.n_row_groups as u64).sum()
    }

    /// Total data bytes across all leaves.
    pub fn total_data_bytes(&self) -> u64 {
        self.leaves.iter().map(|l| l.total_data_bytes).sum()
    }
}

/// Detect the PMAN version from the first 5 bytes (magic + version).
/// Returns `None` if data is too short or magic doesn't match.
/// Returns `Some(version)` otherwise (1, 2, or 3).
pub fn pman_version(data: &[u8]) -> Option<u8> {
    if data.len() < 5 || &data[0..4] != PMAN_MAGIC {
        return None;
    }
    Some(data[4])
}

/// Compute the key range (min, max) across a set of row group entries.
/// Scans the key column's stats in each RG. Returns (min, max) as LE bytes.
/// Returns `None` if no RGs have key column stats.
pub fn compute_key_range(
    rgs: &[RowGroupEntry],
    key_col: &str,
) -> (Option<Vec<u8>>, Option<Vec<u8>>) {
    let mut min_val: Option<Vec<u8>> = None;
    let mut max_val: Option<Vec<u8>> = None;

    for rg in rgs {
        if let Some(col) = rg.columns.iter().find(|c| c.name == key_col) {
            if let (Some(rg_min), Some(rg_max)) = (&col.min, &col.max) {
                min_val = Some(match &min_val {
                    Some(existing) if existing < rg_min => existing.clone(),
                    _ => rg_min.clone(),
                });
                max_val = Some(match &max_val {
                    Some(existing) if existing > rg_max => existing.clone(),
                    _ => rg_max.clone(),
                });
            }
        }
    }

    (min_val, max_val)
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
            slab_byte_offset: None,
            slab_byte_len: None,
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
            slab_byte_offset: None,
            slab_byte_len: None,
        };

        // age > 100 → should prune (max is 50)
        let preds = vec![("age".to_string(), ">".to_string(), 100i64.to_le_bytes().to_vec())];
        assert!(rg.can_prune(&preds));

        // age > 25 → should NOT prune (max is 50, so 25 is in range)
        let preds = vec![("age".to_string(), ">".to_string(), 25i64.to_le_bytes().to_vec())];
        assert!(!rg.can_prune(&preds));
    }

    // ------------------------------------------------------------------
    // PMAN v3 RootManifest tests
    // ------------------------------------------------------------------

    fn make_leaf_entry(hash: &str, n_rgs: u32, data_bytes: u64, key_min: i64, key_max: i64) -> LeafEntry {
        LeafEntry {
            leaf_hash: hash.to_string(),
            n_row_groups: n_rgs,
            total_data_bytes: data_bytes,
            key_min: Some(key_min.to_le_bytes().to_vec()),
            key_max: Some(key_max.to_le_bytes().to_vec()),
        }
    }

    #[test]
    fn test_root_manifest_encode_decode_roundtrip() {
        let mut root = RootManifest::new(
            vec![("id".to_string(), VT_INT64), ("val".to_string(), VT_INT64)],
            "id".to_string(),
        );
        root.leaves.push(make_leaf_entry("leaf_aaa", 100, 50_000_000, 0, 999));
        root.leaves.push(make_leaf_entry("leaf_bbb", 200, 100_000_000, 1000, 2999));

        let encoded = root.encode();
        assert_eq!(&encoded[0..4], b"PMAN");
        assert_eq!(encoded[4], 3); // v3

        let decoded = RootManifest::decode(&encoded).unwrap();
        assert_eq!(decoded.columns.len(), 2);
        assert_eq!(decoded.key_col, "id");
        assert_eq!(decoded.leaves.len(), 2);
        assert_eq!(decoded.leaves[0].leaf_hash, "leaf_aaa");
        assert_eq!(decoded.leaves[0].n_row_groups, 100);
        assert_eq!(decoded.leaves[0].total_data_bytes, 50_000_000);
        assert_eq!(decoded.leaves[0].key_min, Some(0i64.to_le_bytes().to_vec()));
        assert_eq!(decoded.leaves[0].key_max, Some(999i64.to_le_bytes().to_vec()));
        assert_eq!(decoded.leaves[1].leaf_hash, "leaf_bbb");
        assert_eq!(decoded.leaves[1].n_row_groups, 200);
    }

    #[test]
    fn test_root_manifest_empty_leaves() {
        let root = RootManifest::new(
            vec![("id".to_string(), VT_INT64)],
            "id".to_string(),
        );
        let encoded = root.encode();
        let decoded = RootManifest::decode(&encoded).unwrap();
        assert_eq!(decoded.leaves.len(), 0);
        assert_eq!(decoded.total_row_groups(), 0);
        assert_eq!(decoded.total_data_bytes(), 0);
    }

    #[test]
    fn test_root_manifest_v2_not_decoded_as_v3() {
        // A v2 manifest should NOT decode as RootManifest
        let manifest = CollectionManifest::new(
            vec![("id".to_string(), VT_INT64)],
            "id".to_string(),
        );
        let encoded = manifest.encode();
        assert!(RootManifest::decode(&encoded).is_none());
    }

    #[test]
    fn test_pman_version_detection() {
        let v2 = CollectionManifest::new(
            vec![("id".to_string(), VT_INT64)], "id".to_string(),
        ).encode();
        assert_eq!(pman_version(&v2), Some(2));

        let v3 = RootManifest::new(
            vec![("id".to_string(), VT_INT64)], "id".to_string(),
        ).encode();
        assert_eq!(pman_version(&v3), Some(3));

        assert_eq!(pman_version(b"junk"), None);
        assert_eq!(pman_version(b""), None);
    }

    #[test]
    fn test_prune_leaves_selective_query() {
        // 3 leaves: [0,999], [1000,1999], [2000,2999]
        // Query: id > 1500 → should skip leaf 0 (max 999 < 1500)
        let mut root = RootManifest::new(
            vec![("id".to_string(), VT_INT64)], "id".to_string(),
        );
        root.leaves.push(make_leaf_entry("l0", 100, 1000, 0, 999));
        root.leaves.push(make_leaf_entry("l1", 100, 1000, 1000, 1999));
        root.leaves.push(make_leaf_entry("l2", 100, 1000, 2000, 2999));

        let preds = vec![
            ("id".to_string(), ">".to_string(), 1500i64.to_le_bytes().to_vec()),
        ];
        let surviving = root.prune_leaves(&preds);
        assert_eq!(surviving, vec![1, 2]); // leaf 0 pruned
    }

    #[test]
    fn test_prune_leaves_all_survive() {
        let mut root = RootManifest::new(
            vec![("id".to_string(), VT_INT64)], "id".to_string(),
        );
        root.leaves.push(make_leaf_entry("l0", 100, 1000, 0, 9999));

        // id > 500 → within range
        let preds = vec![
            ("id".to_string(), ">".to_string(), 500i64.to_le_bytes().to_vec()),
        ];
        let surviving = root.prune_leaves(&preds);
        assert_eq!(surviving, vec![0]);
    }

    #[test]
    fn test_prune_leaves_no_key_stats() {
        // Leaf without key stats should never be pruned
        let mut root = RootManifest::new(
            vec![("id".to_string(), VT_INT64)], "id".to_string(),
        );
        root.leaves.push(LeafEntry {
            leaf_hash: "l0".to_string(),
            n_row_groups: 100,
            total_data_bytes: 1000,
            key_min: None,
            key_max: None,
        });

        let preds = vec![
            ("id".to_string(), ">".to_string(), 99999i64.to_le_bytes().to_vec()),
        ];
        let surviving = root.prune_leaves(&preds);
        assert_eq!(surviving, vec![0]); // not pruned (no stats)
    }

    #[test]
    fn test_prune_leaves_no_predicates() {
        let mut root = RootManifest::new(
            vec![("id".to_string(), VT_INT64)], "id".to_string(),
        );
        root.leaves.push(make_leaf_entry("l0", 100, 1000, 0, 999));
        root.leaves.push(make_leaf_entry("l1", 100, 1000, 1000, 1999));

        let surviving = root.prune_leaves(&[]);
        assert_eq!(surviving, vec![0, 1]); // all survive
    }

    #[test]
    fn test_prune_leaves_non_key_column_ignored() {
        // Predicates on non-key columns should NOT prune leaves
        let mut root = RootManifest::new(
            vec![("id".to_string(), VT_INT64), ("age".to_string(), VT_INT64)],
            "id".to_string(),
        );
        root.leaves.push(make_leaf_entry("l0", 100, 1000, 0, 999));

        // Predicate on "age" (not the key column) should not prune
        let preds = vec![
            ("age".to_string(), ">".to_string(), 99999i64.to_le_bytes().to_vec()),
        ];
        let surviving = root.prune_leaves(&preds);
        assert_eq!(surviving, vec![0]);
    }

    #[test]
    fn test_compute_key_range() {
        let rgs = vec![
            RowGroupEntry {
                key: "rg_0".to_string(), blob_hash: "h".to_string(), n_rows: 10,
                columns: vec![
                    ColumnStatsEntry { name: "id".to_string(), value_type: VT_INT64,
                        min: Some(10i64.to_le_bytes().to_vec()), max: Some(50i64.to_le_bytes().to_vec()), null_count: 0 },
                ],
                slab_byte_offset: None, slab_byte_len: None,
            },
            RowGroupEntry {
                key: "rg_1".to_string(), blob_hash: "h".to_string(), n_rows: 10,
                columns: vec![
                    ColumnStatsEntry { name: "id".to_string(), value_type: VT_INT64,
                        min: Some(30i64.to_le_bytes().to_vec()), max: Some(100i64.to_le_bytes().to_vec()), null_count: 0 },
                ],
                slab_byte_offset: None, slab_byte_len: None,
            },
        ];
        let (min, max) = compute_key_range(&rgs, "id");
        assert_eq!(min, Some(10i64.to_le_bytes().to_vec()));
        assert_eq!(max, Some(100i64.to_le_bytes().to_vec()));
    }

    #[test]
    fn test_compute_key_range_empty() {
        let rgs: Vec<RowGroupEntry> = Vec::new();
        let (min, max) = compute_key_range(&rgs, "id");
        assert!(min.is_none());
        assert!(max.is_none());
    }

    #[test]
    fn test_root_manifest_size_scaling() {
        // Verify that a root with 8000 leaves (PB-scale) fits in ~800 KB
        let mut root = RootManifest::new(
            vec![("id".to_string(), VT_INT64), ("ts".to_string(), VT_INT64)],
            "id".to_string(),
        );
        for i in 0..8000 {
            root.leaves.push(make_leaf_entry(
                &format!("leaf_{:04}", i),
                1024,
                128_000_000,
                (i * 1024) as i64,
                ((i + 1) * 1024 - 1) as i64,
            ));
        }
        let encoded = root.encode();
        // 8000 leaves × ~45 bytes/leaf (short test hashes) + header ≈ 360 KB
        // At PB scale with real SHA-256 hashes (64 chars), it would be ~800 KB.
        assert!(encoded.len() < 500_000, "root manifest too large: {} bytes", encoded.len());
        assert!(encoded.len() > 300_000, "root manifest too small: {} bytes", encoded.len());

        // Verify decode works at scale
        let decoded = RootManifest::decode(&encoded).unwrap();
        assert_eq!(decoded.leaves.len(), 8000);
        assert_eq!(decoded.total_row_groups(), 8_192_000);
    }
}
