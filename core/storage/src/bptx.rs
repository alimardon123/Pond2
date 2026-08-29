// BPTX — B+ Tree Index for O(log N) point lookups
//
// Binary format (single content-addressed blob):
//   Magic: "BPTX" (4B)
//   Version: 1 (1B)
//   Flags: 1B  (bit 0: key_type 0=i64 1=string, bit 1: has_bloom)
//   Header: 48 bytes total (see BptxHeader fields)
//   Internal nodes section: contiguous block
//   Leaf nodes section: contiguous block
//   [Optional bloom filter]
//
// Key design property: all internal nodes occupy one contiguous section.
// A single Range GET loads every internal node (typically < 1 MB),
// reducing S3 lookups to bounded 2-3 RTTs regardless of tree size.
//
// Value stored per key: (rg_index: u32, row_offset: u32) = 8 bytes.
// rg_index indexes into the manifest's row_groups[] vector.
//
// Phase 1: i64 keys only, full rebuild, no bloom on tree.

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const BPTX_MAGIC: &[u8] = b"BPTX";
const BPTX_VERSION: u8 = 1;
const DEFAULT_FANOUT_I64: usize = 128;
const HEADER_SIZE: usize = 48;

const NODE_LEAF: u8 = 0x00;
const NODE_INTERNAL: u8 = 0x01;

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// A key that can be indexed. Phase 1: i64 only.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum BptxKey {
    Int64(i64),
    // String(Vec<u8>),  // Phase 3
}

/// Lookup result — identifies the exact row group and row within it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexHit {
    /// Index into the manifest's row_groups[] vector.
    pub rg_index: u32,
    /// 0-based row index within that row group.
    pub row_offset: u32,
}

/// Parsed B+ tree header (48 bytes).
#[derive(Debug, Clone)]
pub struct BptxHeader {
    pub key_type: u8,       // 0=i64, 1=string
    pub has_bloom: bool,
    pub n_entries: u32,
    pub tree_height: u32,
    pub fanout: u16,
    pub internal_section_offset: u32,
    pub internal_section_len: u32,
    pub leaf_section_offset: u32,
    pub leaf_section_len: u32,
    pub root_node_offset: u32,
    pub n_leaf_nodes: u32,
    pub n_internal_nodes: u32,
    pub bloom_offset: u32,
}

// ---------------------------------------------------------------------------
// Header encode / decode
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
fn encode_header(
    key_type: u8,
    has_bloom: bool,
    n_entries: u32,
    tree_height: u32,
    fanout: u16,
    internal_offset: u32,
    internal_len: u32,
    leaf_offset: u32,
    leaf_len: u32,
    root_offset: u32,
    n_leaves: u32,
    n_internals: u32,
    bloom_offset: u32,
) -> [u8; HEADER_SIZE] {
    let mut h = [0u8; HEADER_SIZE];
    h[0..4].copy_from_slice(BPTX_MAGIC);
    h[4] = BPTX_VERSION;
    let mut flags: u8 = key_type;
    if has_bloom { flags |= 0x02; }
    h[5] = flags;
    h[6..10].copy_from_slice(&n_entries.to_le_bytes());
    h[10..14].copy_from_slice(&tree_height.to_le_bytes());
    h[14..16].copy_from_slice(&fanout.to_le_bytes());
    h[16..20].copy_from_slice(&internal_offset.to_le_bytes());
    h[20..24].copy_from_slice(&internal_len.to_le_bytes());
    h[24..28].copy_from_slice(&leaf_offset.to_le_bytes());
    h[28..32].copy_from_slice(&leaf_len.to_le_bytes());
    h[32..36].copy_from_slice(&root_offset.to_le_bytes());
    h[36..40].copy_from_slice(&n_leaves.to_le_bytes());
    h[40..44].copy_from_slice(&n_internals.to_le_bytes());
    h[44..48].copy_from_slice(&bloom_offset.to_le_bytes());
    h
}

fn decode_header(data: &[u8]) -> Option<BptxHeader> {
    if data.len() < HEADER_SIZE { return None; }
    if &data[0..4] != BPTX_MAGIC { return None; }
    if data[4] != BPTX_VERSION { return None; }
    let flags = data[5];
    Some(BptxHeader {
        key_type: flags & 0x01,
        has_bloom: (flags & 0x02) != 0,
        n_entries: u32::from_le_bytes(data[6..10].try_into().ok()?),
        tree_height: u32::from_le_bytes(data[10..14].try_into().ok()?),
        fanout: u16::from_le_bytes(data[14..16].try_into().ok()?),
        internal_section_offset: u32::from_le_bytes(data[16..20].try_into().ok()?),
        internal_section_len: u32::from_le_bytes(data[20..24].try_into().ok()?),
        leaf_section_offset: u32::from_le_bytes(data[24..28].try_into().ok()?),
        leaf_section_len: u32::from_le_bytes(data[28..32].try_into().ok()?),
        root_node_offset: u32::from_le_bytes(data[32..36].try_into().ok()?),
        n_leaf_nodes: u32::from_le_bytes(data[36..40].try_into().ok()?),
        n_internal_nodes: u32::from_le_bytes(data[40..44].try_into().ok()?),
        bloom_offset: u32::from_le_bytes(data[44..48].try_into().ok()?),
    })
}

// ---------------------------------------------------------------------------
// Leaf node encode / decode (i64 keys)
// ---------------------------------------------------------------------------

/// Encode a leaf node from sorted (key, rg_index, row_offset) triples.
/// Returns (node_bytes, max_key).
fn encode_leaf_i64(entries: &[(i64, u32, u32)]) -> (Vec<u8>, i64) {
    let n = entries.len() as u16;
    let cap = 3 + (n as usize) * 16;
    let mut buf = Vec::with_capacity(cap);
    buf.push(NODE_LEAF);
    buf.extend_from_slice(&n.to_le_bytes());
    for (key, _, _) in entries {
        buf.extend_from_slice(&key.to_le_bytes());
    }
    for (_, rg, row) in entries {
        buf.extend_from_slice(&rg.to_le_bytes());
        buf.extend_from_slice(&row.to_le_bytes());
    }
    let max_key = entries.last().map(|(k, _, _)| *k).unwrap_or(i64::MIN);
    (buf, max_key)
}

/// Decode a leaf node from bytes. Returns (keys, values_as_pairs, max_key).
#[allow(clippy::type_complexity)]
fn decode_leaf_i64(data: &[u8]) -> Option<(Vec<i64>, Vec<(u32, u32)>, i64)> {
    if data.is_empty() || data[0] != NODE_LEAF { return None; }
    if data.len() < 3 { return None; }
    let n = u16::from_le_bytes(data[1..3].try_into().ok()?) as usize;
    let expected = 3 + n * 16;
    if data.len() < expected { return None; }

    let mut keys = Vec::with_capacity(n);
    let mut values = Vec::with_capacity(n);
    for i in 0..n {
        let key = i64::from_le_bytes(data[3 + i * 8..3 + i * 8 + 8].try_into().ok()?);
        keys.push(key);
    }
    let val_start = 3 + n * 8;
    for i in 0..n {
        let rg = u32::from_le_bytes(data[val_start + i * 8..val_start + i * 8 + 4].try_into().ok()?);
        let row = u32::from_le_bytes(data[val_start + i * 8 + 4..val_start + i * 8 + 8].try_into().ok()?);
        values.push((rg, row));
    }
    let max_key = keys.last().copied().unwrap_or(i64::MIN);
    Some((keys, values, max_key))
}

// ---------------------------------------------------------------------------
// Internal node encode / decode (i64 keys)
// ---------------------------------------------------------------------------

/// Encode an internal node from (separator_keys, child_offsets) where
/// len(separator_keys) = len(child_offsets) - 1.
fn encode_internal_i64(sep_keys: &[i64], child_offsets: &[u32]) -> Vec<u8> {
    let n_children = child_offsets.len() as u16;
    let n_keys = (n_children - 1) as usize;
    let cap = 3 + n_keys * 8 + (n_children as usize) * 4;
    let mut buf = Vec::with_capacity(cap);
    buf.push(NODE_INTERNAL);
    buf.extend_from_slice(&n_children.to_le_bytes());
    for key in sep_keys {
        buf.extend_from_slice(&key.to_le_bytes());
    }
    for off in child_offsets {
        buf.extend_from_slice(&off.to_le_bytes());
    }
    buf
}

/// Decode an internal node from bytes. Returns (separator_keys, child_offsets).
fn decode_internal_i64(data: &[u8]) -> Option<(Vec<i64>, Vec<u32>)> {
    if data.is_empty() || data[0] != NODE_INTERNAL { return None; }
    if data.len() < 3 { return None; }
    let n_children = u16::from_le_bytes(data[1..3].try_into().ok()?) as usize;
    let n_keys = n_children - 1;
    let expected = 3 + n_keys * 8 + n_children * 4;
    if data.len() < expected { return None; }

    let mut keys = Vec::with_capacity(n_keys);
    for i in 0..n_keys {
        let key = i64::from_le_bytes(data[3 + i * 8..3 + i * 8 + 8].try_into().ok()?);
        keys.push(key);
    }
    let off_start = 3 + n_keys * 8;
    let mut offsets = Vec::with_capacity(n_children);
    for i in 0..n_children {
        let off = u32::from_le_bytes(
            data[off_start + i * 4..off_start + i * 4 + 4].try_into().ok()?
        );
        offsets.push(off);
    }
    Some((keys, offsets))
}

// ---------------------------------------------------------------------------
// Build (bulk load from sorted i64 entries)
// ---------------------------------------------------------------------------

/// Internal node template — stores separator keys and child count
/// for offset computation before final encoding.
struct InternalNodeTemplate {
    sep_keys: Vec<i64>,
    n_children: usize,
}

/// Build a BPTX blob from sorted (key, rg_index, row_offset) triples.
///
/// Input MUST be sorted by key ascending. Duplicates are resolved by
/// keeping the last entry per key (CRDT last-writer-wins).
///
/// Returns the complete blob bytes.
pub fn build_bptx_i64(entries: &mut Vec<(i64, u32, u32)>) -> Vec<u8> {
    // Deduplicate: keep last entry per key
    if !entries.is_empty() {
        let mut write_idx = 1;
        for read_idx in 1..entries.len() {
            if entries[read_idx].0 != entries[write_idx - 1].0 {
                if write_idx != read_idx {
                    entries[write_idx] = entries[read_idx];
                }
                write_idx += 1;
            } else {
                entries[write_idx - 1] = entries[read_idx];
            }
        }
        entries.truncate(write_idx);
    }

    let n_entries = entries.len() as u32;
    let fanout = DEFAULT_FANOUT_I64;

    // Empty case
    if entries.is_empty() {
        let h = encode_header(
            0, false, 0, 1, fanout as u16,
            HEADER_SIZE as u32, 0,
            HEADER_SIZE as u32, 0,
            HEADER_SIZE as u32, 0, 0, 0,
        );
        return h.to_vec();
    }

    // Phase 1: Build all leaf nodes
    struct LeafInfo { bytes: Vec<u8>, max_key: i64 }
    let leaves: Vec<LeafInfo> = entries.chunks(fanout)
        .map(|chunk| {
            let (bytes, max_key) = encode_leaf_i64(chunk);
            LeafInfo { bytes, max_key }
        })
        .collect();

    // Single leaf → no internal nodes
    if leaves.len() == 1 {
        let h = encode_header(
            0, false, n_entries, 1, fanout as u16,
            HEADER_SIZE as u32, 0,
            HEADER_SIZE as u32,
            leaves[0].bytes.len() as u32,
            HEADER_SIZE as u32,
            1, 0, 0,
        );
        let mut blob = Vec::with_capacity(HEADER_SIZE + leaves[0].bytes.len());
        blob.extend_from_slice(&h);
        blob.extend_from_slice(&leaves[0].bytes);
        return blob;
    }

    // Phase 2: Build internal levels bottom-up (templates with separator keys)
    let mut prev_level_max: Vec<i64> = leaves.iter().map(|l| l.max_key).collect();
    let mut all_internal_levels: Vec<Vec<InternalNodeTemplate>> = Vec::new();

    loop {
        if prev_level_max.len() <= 1 { break; }
        let mut level: Vec<InternalNodeTemplate> = Vec::new();
        let mut cur_max: Vec<i64> = Vec::new();

        for chunk in prev_level_max.chunks(fanout) {
            let sep_keys: Vec<i64> = chunk.iter()
                .take(chunk.len() - 1)
                .copied()
                .collect();
            cur_max.push(*chunk.last().unwrap());
            level.push(InternalNodeTemplate {
                sep_keys,
                n_children: chunk.len(),
            });
        }

        all_internal_levels.push(level);
        prev_level_max = cur_max;
    }

    let tree_height = (all_internal_levels.len() + 1) as u32;
    let n_leaf_nodes = leaves.len() as u32;
    let n_internal_nodes: u32 = all_internal_levels.iter().map(|l| l.len() as u32).sum();

    // Phase 3: Compute sizes of all internal nodes → determine absolute offsets
    let mut internal_total_len: usize = 0;
    for level in &all_internal_levels {
        for tmpl in level {
            internal_total_len += 3 + (tmpl.n_children - 1) * 8 + tmpl.n_children * 4;
        }
    }

    let internal_offset = HEADER_SIZE as u32;
    let leaf_start: u32 = (HEADER_SIZE + internal_total_len) as u32;

    // Compute absolute offsets for internal nodes
    let mut internal_abs_offsets: Vec<Vec<u32>> = Vec::new();
    let mut abs_off: u32 = internal_offset;
    for level in &all_internal_levels {
        let mut level_offsets: Vec<u32> = Vec::new();
        for tmpl in level {
            level_offsets.push(abs_off);
            abs_off += (3 + (tmpl.n_children - 1) * 8 + tmpl.n_children * 4) as u32;
        }
        internal_abs_offsets.push(level_offsets);
    }

    // Compute absolute offsets for leaf nodes
    let mut leaf_abs_offsets: Vec<u32> = Vec::new();
    let mut leaf_abs: u32 = leaf_start;
    for leaf in &leaves {
        leaf_abs_offsets.push(leaf_abs);
        leaf_abs += leaf.bytes.len() as u32;
    }
    let leaf_total_len = (leaf_abs - leaf_start) as usize;

    // Root offset: last node in the last internal level
    let root_offset = internal_abs_offsets.last()
        .and_then(|l| l.last().copied())
        .unwrap_or(leaf_start);

    // Phase 4: Build internal nodes with correct child offsets
    let mut internal_bytes: Vec<u8> = Vec::with_capacity(internal_total_len);
    for (li, level) in all_internal_levels.iter().enumerate() {
        let mut child_idx = 0;
        for tmpl in level {
            let child_offsets: Vec<u32> = if li == 0 {
                leaf_abs_offsets[child_idx..child_idx + tmpl.n_children].to_vec()
            } else {
                let prev = &internal_abs_offsets[li - 1];
                prev[child_idx..child_idx + tmpl.n_children].to_vec()
            };
            let node = encode_internal_i64(&tmpl.sep_keys, &child_offsets);
            internal_bytes.extend_from_slice(&node);
            child_idx += tmpl.n_children;
        }
    }

    // Phase 5: Assemble final blob
    let h = encode_header(
        0, false, n_entries, tree_height, fanout as u16,
        internal_offset,
        internal_total_len as u32,
        leaf_start,
        leaf_total_len as u32,
        root_offset,
        n_leaf_nodes,
        n_internal_nodes,
        0,
    );

    let mut blob = Vec::with_capacity(HEADER_SIZE + internal_total_len + leaf_total_len);
    blob.extend_from_slice(&h);
    blob.extend_from_slice(&internal_bytes);
    for leaf in &leaves {
        blob.extend_from_slice(&leaf.bytes);
    }
    blob
}

// ---------------------------------------------------------------------------
// Lookup
// ---------------------------------------------------------------------------

/// Point lookup in a BPTX blob by i64 key.
/// Returns the (rg_index, row_offset) if found, or None.
pub fn lookup_i64(blob: &[u8], key: i64) -> Option<IndexHit> {
    let header = decode_header(blob)?;
    if header.n_entries == 0 { return None; }

    // If no internal nodes, root is a leaf
    if header.n_internal_nodes == 0 {
        let leaf_data = &blob[header.leaf_section_offset as usize..];
        return lookup_leaf_i64(leaf_data, key);
    }

    // Load internal section
    let int_start = header.internal_section_offset as usize;
    let int_end = int_start + header.internal_section_len as usize;
    let internal_bytes = &blob[int_start..int_end];

    // Walk from root down to leaf
    let mut node_abs_offset = header.root_node_offset as usize;
    for _level in 0..header.tree_height - 1 {
        let node_rel_offset = node_abs_offset - int_start;
        let (sep_keys, child_offsets) = decode_internal_i64(&internal_bytes[node_rel_offset..])?;
        let child_idx = match sep_keys.binary_search(&key) {
            Ok(i) => i,       // key == separator → LEFT child (separator = max of left)
            Err(i) => i,       // key < separator → LEFT child
        };
        node_abs_offset = *child_offsets.get(child_idx)? as usize;
    }

    // Now node_abs_offset points to a leaf
    let leaf_data = &blob[node_abs_offset..];
    lookup_leaf_i64(leaf_data, key)
}

/// Binary search a single leaf node for the key.
fn lookup_leaf_i64(data: &[u8], key: i64) -> Option<IndexHit> {
    let (keys, values, _) = decode_leaf_i64(data)?;
    match keys.binary_search(&key) {
        Ok(i) => {
            let (rg, row) = *values.get(i)?;
            Some(IndexHit { rg_index: rg, row_offset: row })
        }
        Err(_) => None,
    }
}

/// Find the target leaf offset for a key using pre-loaded header + internal bytes.
/// Returns (leaf_abs_offset, estimated_leaf_size).
pub fn lookup_i64_find_leaf(
    header: &BptxHeader,
    internal_bytes: &[u8],
    key: i64,
) -> Option<(u32, usize)> {
    if header.n_entries == 0 { return None; }
    if header.n_internal_nodes == 0 {
        return Some((header.leaf_section_offset, header.leaf_section_len as usize));
    }
    let int_start = header.internal_section_offset as usize;
    let mut node_abs_offset = header.root_node_offset as usize;
    for _level in 0..header.tree_height - 1 {
        let node_rel_offset = node_abs_offset - int_start;
        let (sep_keys, child_offsets) = decode_internal_i64(&internal_bytes[node_rel_offset..])?;
        let child_idx = match sep_keys.binary_search(&key) {
            Ok(i) => i,       // key == separator → LEFT child
            Err(i) => i,
        };
        node_abs_offset = *child_offsets.get(child_idx)? as usize;
    }
    let leaf_end = (header.leaf_section_offset + header.leaf_section_len) as usize;
    let remaining = leaf_end - node_abs_offset;
    Some((node_abs_offset as u32, remaining))
}

// ---------------------------------------------------------------------------
// Index metadata (JSON, stored as a ref)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct BptxIndexMeta {
    pub index_type: String,
    pub key_column: String,
    pub key_type: String,
    pub n_entries: u32,
    pub blob_hash: String,
    pub blob_size_bytes: u32,
    pub tree_height: u32,
    pub fanout: u16,
    pub manifest_hash: String,
    pub created_at: f64,
}

impl BptxIndexMeta {
    pub fn to_json_bytes(&self) -> Vec<u8> {
        serde_json::json!({
            "index_type": self.index_type,
            "key_column": self.key_column,
            "key_type": self.key_type,
            "n_entries": self.n_entries,
            "blob_hash": self.blob_hash,
            "blob_size_bytes": self.blob_size_bytes,
            "tree_height": self.tree_height,
            "fanout": self.fanout,
            "manifest_hash": self.manifest_hash,
            "created_at": self.created_at,
        }).to_string().into_bytes()
    }

    pub fn from_json_bytes(data: &[u8]) -> Option<Self> {
        let v: serde_json::Value = serde_json::from_slice(data).ok()?;
        Some(BptxIndexMeta {
            index_type: v.get("index_type")?.as_str()?.to_string(),
            key_column: v.get("key_column")?.as_str()?.to_string(),
            key_type: v.get("key_type")?.as_str()?.to_string(),
            n_entries: v.get("n_entries")?.as_u64()? as u32,
            blob_hash: v.get("blob_hash")?.as_str()?.to_string(),
            blob_size_bytes: v.get("blob_size_bytes")?.as_u64()? as u32,
            tree_height: v.get("tree_height")?.as_u64()? as u32,
            fanout: v.get("fanout")?.as_u64()? as u16,
            manifest_hash: v.get("manifest_hash")?.as_str()?.to_string(),
            created_at: v.get("created_at")?.as_f64().unwrap_or(0.0),
        })
    }

    pub fn blob_ref(collection: &str, column: &str) -> String {
        format!("collections/{}/indexes/bptx_{}", collection, column)
    }

    pub fn meta_ref(collection: &str, column: &str) -> String {
        format!("collections/{}/_index_meta/bptx_{}", collection, column)
    }
}

// ---------------------------------------------------------------------------
// Build from manifest (high-level API)
// ---------------------------------------------------------------------------

use pond_kernel::PondKernel;
use super::manifest::CollectionManifest;
use super::commit;
use pond_core::pnd2_decode;
use std::time::{SystemTime, UNIX_EPOCH};

/// Build a B+ tree index on a column for a collection's HEAD.
/// Returns Ok(index_blob_hash) on success.
pub fn build_index_for_collection(
    kernel: &PondKernel,
    collection: &str,
    column: &str,
    active_branch: &str,
) -> Result<String, String> {
    let commit_ref = super::branch_ref(collection, active_branch);
    let commit_hash = kernel.resolve(&commit_ref)
        .map_err(|e| format!(
            "Failed to read branch ref for collection '{}': {}", collection, e))?
        .ok_or_else(|| format!("No commits in '{}' on branch '{}'", collection, active_branch))?;

    let manifest_hash = commit::resolve_manifest_hash(kernel, &commit_hash)
        .ok_or_else(|| "Cannot resolve manifest from HEAD commit".to_string())?;

    let manifest_bytes = kernel.read_blob(&manifest_hash)
        .map_err(|e| format!("Failed to read manifest: {}", e))?;
    let manifest = CollectionManifest::decode(&manifest_bytes)
        .ok_or_else(|| "Failed to decode manifest".to_string())?;

    let col_idx = manifest.columns.iter()
        .position(|(name, _)| name == column)
        .ok_or_else(|| format!("Column '{}' not found in manifest", column))?;
    let col_vtype = manifest.columns[col_idx].1;

    if col_vtype != 1 {
        return Err(format!("BPTX Phase 1 supports i64 columns only (column '{}' is type {})", column, col_vtype));
    }

    // Extract all (key, rg_index, row_offset) triples
    let mut entries: Vec<(i64, u32, u32)> = Vec::new();
    for (rg_idx, rg) in manifest.row_groups.iter().enumerate() {
        let rg_data = if let (Some(off), Some(len)) = (rg.slab_byte_offset, rg.slab_byte_len) {
            kernel.read_blob_range(&rg.blob_hash, off, off + len as u64)
                .map_err(|e| format!("Failed to read slab range for RG {}: {}", rg.key, e))?
        } else {
            kernel.read_blob(&rg.blob_hash)
                .map_err(|e| format!("Failed to read blob for RG {}: {}", rg.key, e))?
        };

        let cols = pnd2_decode(&rg_data)
            .map_err(|e| format!("Failed to decode PND2 for RG {}: {}", rg.key, e))?;

        if let Some(pond_col) = cols.get(col_idx) {
            for (row_off, &val) in pond_col.i64_data.iter().enumerate() {
                entries.push((val, rg_idx as u32, row_off as u32));
            }
        }
    }

    entries.sort_by_key(|(k, _, _)| *k);
    let mut entries_mut = entries;
    let blob = build_bptx_i64(&mut entries_mut);
    let blob_hash = kernel.write(&blob)
        .map_err(|e| format!("Failed to write BPTX blob: {}", e))?;

    let header = decode_header(&blob).unwrap();
    let meta = BptxIndexMeta {
        index_type: "bptx".to_string(),
        key_column: column.to_string(),
        key_type: "i64".to_string(),
        n_entries: header.n_entries,
        blob_hash: blob_hash.clone(),
        blob_size_bytes: blob.len() as u32,
        tree_height: header.tree_height,
        fanout: header.fanout,
        manifest_hash,
        created_at: SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs_f64()).unwrap_or(0.0),
    };

    let meta_bytes = meta.to_json_bytes();
    let meta_hash = kernel.write(&meta_bytes)
        .map_err(|e| format!("Failed to write index metadata: {}", e))?;

    kernel.reference(&BptxIndexMeta::blob_ref(collection, column), &blob_hash)
        .map_err(|e| format!("Failed to set index ref: {}", e))?;
    kernel.reference(&BptxIndexMeta::meta_ref(collection, column), &meta_hash)
        .map_err(|e| format!("Failed to set meta ref: {}", e))?;

    Ok(blob_hash)
}

/// Point lookup using the B+ tree index.
/// Returns (rg_index, row_offset) if the key is found, or None.
///
/// NOTE: This does NOT check staleness. Use `index_lookup_checked` for
/// production reads that need consistency guarantees.
pub fn index_lookup(
    kernel: &PondKernel,
    collection: &str,
    column: &str,
    key: i64,
) -> Result<Option<IndexHit>, String> {
    let meta_ref = BptxIndexMeta::meta_ref(collection, column);
    let meta_hash = kernel.resolve(&meta_ref)
        .map_err(|e| format!(
            "Failed to read BPTX index meta ref for '{}.{}': {}", collection, column, e))?
        .ok_or_else(|| format!("No BPTX index on column '{}' in '{}'", column, collection))?;
    let meta_bytes = kernel.read_blob(&meta_hash)
        .map_err(|e| format!("Failed to read index metadata: {}", e))?;
    let _meta = BptxIndexMeta::from_json_bytes(&meta_bytes)
        .ok_or_else(|| "Failed to parse index metadata".to_string())?;

    let blob_ref = BptxIndexMeta::blob_ref(collection, column);
    let blob_hash = kernel.resolve(&blob_ref)
        .map_err(|e| format!(
            "Failed to read BPTX index blob ref for '{}.{}': {}", collection, column, e))?
        .ok_or_else(|| "Index blob ref not found".to_string())?;
    let blob = kernel.read_blob(&blob_hash)
        .map_err(|e| format!("Failed to read index blob: {}", e))?;

    Ok(lookup_i64(&blob, key))
}

/// Point lookup with staleness detection.
///
/// Checks that the index's `manifest_hash` matches the current HEAD's
/// manifest hash. If stale, returns `Err` so the caller falls back to
/// a full scan.
///
/// Returns `Ok(None)` if the index is fresh but the key is absent.
/// Returns `Err` if the index is stale or any I/O fails — the caller
/// should fall back to the non-indexed path.
pub fn index_lookup_checked(
    kernel: &PondKernel,
    collection: &str,
    branch: &str,
    column: &str,
    key: i64,
) -> Result<Option<IndexHit>, String> {
    // 1. Resolve current HEAD manifest hash
    let commit_ref = super::branch_ref(collection, branch);
    let commit_hash = kernel.resolve(&commit_ref)
        .map_err(|e| format!(
            "Failed to read branch ref for collection '{}': {}", collection, e))?
        .ok_or_else(|| format!("No commits in '{}' on branch '{}'", collection, branch))?;
    let current_manifest_hash = commit::resolve_manifest_hash(kernel, &commit_hash)
        .ok_or_else(|| "Cannot resolve manifest from HEAD commit".to_string())?;

    // 2. Read index metadata and check staleness
    let meta_ref = BptxIndexMeta::meta_ref(collection, column);
    let meta_hash = match kernel.resolve(&meta_ref) {
        Ok(Some(h)) => h,
        // No index — caller falls through to the un-indexed path.
        Ok(None) => return Err("no_bptx_index".to_string()),
        // C17: a FAILED meta-ref read must not masquerade as "no index".
        Err(e) => return Err(format!(
            "Failed to read BPTX index meta ref for '{}.{}': {}", collection, column, e)),
    };
    let meta_bytes = kernel.read_blob(&meta_hash)
        .map_err(|e| format!("Failed to read index metadata: {}", e))?;
    let meta = BptxIndexMeta::from_json_bytes(&meta_bytes)
        .ok_or_else(|| "Failed to parse index metadata".to_string())?;

    // Staleness check: if the index was built on a different manifest,
    // the rg_index values may be wrong. Fall back to full scan.
    if meta.manifest_hash != current_manifest_hash {
        return Err(format!("BPTX index stale (index manifest={}, current manifest={})",
            &meta.manifest_hash[..8.min(meta.manifest_hash.len())],
            &current_manifest_hash[..8.min(current_manifest_hash.len())]
        ));
    }

    // 3. Two-step lookup: header + internal nodes first (1 Range GET),
    //    then only the target leaf (1 Range GET).
    let blob_ref = BptxIndexMeta::blob_ref(collection, column);
    let blob_hash = kernel.resolve(&blob_ref)
        .map_err(|e| format!(
            "Failed to read BPTX index blob ref for '{}.{}': {}", collection, column, e))?
        .ok_or_else(|| "Index blob ref not found".to_string())?;

    // Step 1: Load header (48 bytes) to get tree geometry
    let header_bytes = kernel.read_blob_range(&blob_hash, 0, HEADER_SIZE as u64)
        .map_err(|e| format!("Failed to read BPTX header: {}", e))?;
    let header = decode_header(&header_bytes)
        .ok_or_else(|| "Failed to decode BPTX header".to_string())?;

    if header.n_entries == 0 {
        return Ok(None);
    }

    // Step 2a: If tree has internal nodes, load them + walk to find leaf offset
    if header.n_internal_nodes > 0 {
        let int_start = header.internal_section_offset;
        let int_end = int_start + header.internal_section_len;
        let internal_bytes = kernel.read_blob_range(&blob_hash, int_start as u64, int_end as u64)
            .map_err(|e| format!("Failed to read BPTX internal nodes: {}", e))?;

        let (leaf_offset, leaf_est_size) = lookup_i64_find_leaf(&header, &internal_bytes, key)
            .ok_or_else(|| "BPTX internal node walk failed".to_string())?;

        // Step 2b: Load only the target leaf
        let leaf_end = (leaf_offset as u64) + (leaf_est_size as u64)
            .min((header.leaf_section_offset + header.leaf_section_len) as u64);
        let leaf_bytes = kernel.read_blob_range(&blob_hash, leaf_offset as u64, leaf_end)
            .map_err(|e| format!("Failed to read BPTX leaf: {}", e))?;

        // Search within the leaf
        let (keys, values, _) = decode_leaf_i64(&leaf_bytes)
            .ok_or_else(|| "Failed to decode BPTX leaf".to_string())?;
        match keys.binary_search(&key) {
            Ok(i) => Ok(Some(IndexHit {
                rg_index: values[i].0,
                row_offset: values[i].1,
            })),
            Err(_) => Ok(None),
        }
    } else {
        // No internal nodes — single leaf tree. Load the whole leaf section.
        let leaf_start = header.leaf_section_offset;
        let leaf_end = leaf_start + header.leaf_section_len;
        let leaf_bytes = kernel.read_blob_range(&blob_hash, leaf_start as u64, leaf_end as u64)
            .map_err(|e| format!("Failed to read BPTX leaf: {}", e))?;
        let (keys, values, _) = decode_leaf_i64(&leaf_bytes)
            .ok_or_else(|| "Failed to decode BPTX leaf".to_string())?;
        match keys.binary_search(&key) {
            Ok(i) => Ok(Some(IndexHit {
                rg_index: values[i].0,
                row_offset: values[i].1,
            })),
            Err(_) => Ok(None),
        }
    }
}

/// Check if a BPTX index exists for the given collection+column.
/// Returns Ok(true) if the meta ref resolves, Ok(false) if unbound.
/// C17: a FAILED read is an Err — an outage is not "no index". (No
/// callers today; the Result signature is kept C17-honest.)
pub fn has_bptx_index(
    kernel: &PondKernel,
    collection: &str,
    column: &str,
) -> Result<bool, String> {
    let meta_ref = BptxIndexMeta::meta_ref(collection, column);
    Ok(kernel.resolve(&meta_ref)
        .map_err(|e| format!(
            "Failed to read BPTX index meta ref for '{}.{}': {}", collection, column, e))?
        .is_some())
}

// ---------------------------------------------------------------------------
// Range Scan
// ---------------------------------------------------------------------------

/// Range scan in a BPTX blob by i64 key range [start_key, end_key].
/// Returns all (rg_index, row_offset) hits whose keys fall in the range.
/// Results are ordered by key (ascending), as leaves are stored contiguously.
///
/// For small trees (single leaf or no internal nodes), decodes directly.
/// For multi-level trees, walks internal nodes to find the first and last
/// relevant leaves, then scans all leaves in between.
pub fn range_scan_i64(blob: &[u8], start_key: i64, end_key: i64) -> Vec<IndexHit> {
    let header = match decode_header(blob) {
        Some(h) => h,
        None => return Vec::new(),
    };
    if header.n_entries == 0 {
        return Vec::new();
    }

    // Determine the range of leaf offsets to scan
    let leaf_section_start = header.leaf_section_offset as usize;
    let leaf_section_end = leaf_section_start + header.leaf_section_len as usize;

    if header.n_internal_nodes == 0 {
        // Single leaf — scan it directly
        let leaf_data = &blob[leaf_section_start..leaf_section_end];
        return scan_leaf_i64_range(leaf_data, start_key, end_key);
    }

    // Multi-level tree: find first and last leaf offsets via internal nodes
    let int_start = header.internal_section_offset as usize;
    let int_end = int_start + header.internal_section_len as usize;
    let internal_bytes = &blob[int_start..int_end];

    // Find the leaf offset for start_key
    let first_leaf_off = match find_leaf_offset(&header, internal_bytes, start_key) {
        Some(off) => off as usize,
        None => return Vec::new(),
    };

    // Find the leaf offset for end_key
    let last_leaf_off = match find_leaf_offset(&header, internal_bytes, end_key) {
        Some(off) => off as usize,
        None => return Vec::new(),
    };

    // Clamp to leaf section bounds
    let scan_start = first_leaf_off.max(leaf_section_start);
    let scan_end = (last_leaf_off + 4096).min(leaf_section_end); // 4096 = max leaf size

    // Scan all leaf nodes from first_leaf_off to scan_end
    let mut results = Vec::new();
    let mut pos = scan_start;
    while pos < scan_end {
        let remaining = &blob[pos..scan_end];
        if remaining.is_empty() || remaining[0] != NODE_LEAF {
            break;
        }
        let (keys, values, _max_key) = match decode_leaf_i64(remaining) {
            Some(decoded) => decoded,
            None => break,
        };

        // Check if this leaf's min key exceeds end_key — we're done
        if let Some(&first_key) = keys.first() {
            if first_key > end_key {
                break;
            }
        }

        // Collect matching entries from this leaf
        for (i, &key) in keys.iter().enumerate() {
            if key < start_key {
                continue;
            }
            if key > end_key {
                break; // keys are sorted within a leaf
            }
            let (rg, row) = *values.get(i).unwrap_or(&(0, 0));
            results.push(IndexHit { rg_index: rg, row_offset: row });
        }

        // Advance past this leaf node
        if keys.is_empty() {
            break;
        }
        let leaf_size = 3 + keys.len() * 16;
        pos += leaf_size;
    }

    results
}

/// Find the leaf node absolute offset for a given key (same walk as point lookup).
fn find_leaf_offset(header: &BptxHeader, internal_bytes: &[u8], key: i64) -> Option<u32> {
    if header.n_internal_nodes == 0 {
        return Some(header.leaf_section_offset);
    }
    let int_start = header.internal_section_offset as usize;
    let mut node_abs_offset = header.root_node_offset as usize;
    for _level in 0..header.tree_height - 1 {
        let node_rel_offset = node_abs_offset - int_start;
        let (sep_keys, child_offsets) = decode_internal_i64(&internal_bytes[node_rel_offset..])?;
        let child_idx = match sep_keys.binary_search(&key) {
            Ok(i) => i,
            Err(i) => i,
        };
        node_abs_offset = *child_offsets.get(child_idx)? as usize;
    }
    Some(node_abs_offset as u32)
}

/// Scan a single leaf node for keys in [start_key, end_key].
fn scan_leaf_i64_range(data: &[u8], start_key: i64, end_key: i64) -> Vec<IndexHit> {
    let (keys, values, _) = match decode_leaf_i64(data) {
        Some(decoded) => decoded,
        None => return Vec::new(),
    };

    let mut results = Vec::new();
    for (i, &key) in keys.iter().enumerate() {
        if key < start_key {
            continue;
        }
        if key > end_key {
            break;
        }
        let (rg, row) = *values.get(i).unwrap_or(&(0, 0));
        results.push(IndexHit { rg_index: rg, row_offset: row });
    }
    results
}

/// Range scan with staleness detection (high-level API).
///
/// Returns Ok(hits) if the index is fresh, Err if stale or missing.
/// S3 round-trips (cold, multi-level tree):
///   1. Resolve HEAD commit + manifest (1-2 RTTs)
///   2. Check index staleness (1 RTT for meta, cached after first read)
///   3. Read BPTX header (1 RTT)
///   4. Read internal nodes (1 RTT)
///   5. Read relevant leaf range (1 RTT, contiguous leaves in one Range GET)
///   - Total: 5-6 RTTs for ANY selectivity range query
///
/// vs. full scan: O(N) RG reads where N = total RGs.
pub fn range_scan_checked(
    kernel: &PondKernel,
    collection: &str,
    branch: &str,
    column: &str,
    start_key: i64,
    end_key: i64,
) -> Result<Vec<IndexHit>, String> {
    // 1. Resolve current HEAD manifest hash
    let commit_ref = super::branch_ref(collection, branch);
    let commit_hash = kernel.resolve(&commit_ref)
        .map_err(|e| format!(
            "Failed to read branch ref for collection '{}': {}", collection, e))?
        .ok_or_else(|| format!("No commits in '{}' on branch '{}'", collection, branch))?;
    let current_manifest_hash = commit::resolve_manifest_hash(kernel, &commit_hash)
        .ok_or_else(|| "Cannot resolve manifest from HEAD commit".to_string())?;

    // 2. Read index metadata and check staleness
    let meta_ref = BptxIndexMeta::meta_ref(collection, column);
    let meta_hash = match kernel.resolve(&meta_ref) {
        Ok(Some(h)) => h,
        // No index — caller falls through to the un-indexed path.
        Ok(None) => return Err("no_bptx_index".to_string()),
        // C17: a FAILED meta-ref read must not masquerade as "no index".
        Err(e) => return Err(format!(
            "Failed to read BPTX index meta ref for '{}.{}': {}", collection, column, e)),
    };
    let meta_bytes = kernel.read_blob(&meta_hash)
        .map_err(|e| format!("Failed to read index metadata: {}", e))?;
    let meta = BptxIndexMeta::from_json_bytes(&meta_bytes)
        .ok_or_else(|| "Failed to parse index metadata".to_string())?;

    if meta.manifest_hash != current_manifest_hash {
        return Err(format!(
            "BPTX index stale (index manifest={}, current manifest={})",
            &meta.manifest_hash[..8.min(meta.manifest_hash.len())],
            &current_manifest_hash[..8.min(current_manifest_hash.len())]
        ));
    }

    // 3. Load the full BPTX blob (typically small: <1MB for 100K entries)
    let blob_ref = BptxIndexMeta::blob_ref(collection, column);
    let blob_hash = kernel.resolve(&blob_ref)
        .map_err(|e| format!(
            "Failed to read BPTX index blob ref for '{}.{}': {}", collection, column, e))?
        .ok_or_else(|| "Index blob ref not found".to_string())?;
    let blob = kernel.read_blob(&blob_hash)
        .map_err(|e| format!("Failed to read index blob: {}", e))?;

    // 4. Perform range scan
    Ok(range_scan_i64(&blob, start_key, end_key))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_tree() {
        let mut entries: Vec<(i64, u32, u32)> = Vec::new();
        let blob = build_bptx_i64(&mut entries);
        let header = decode_header(&blob).unwrap();
        assert_eq!(header.n_entries, 0);
        assert_eq!(header.tree_height, 1);
        assert_eq!(lookup_i64(&blob, 42), None);
    }

    #[test]
    fn test_single_entry() {
        let mut entries = vec![(42i64, 0u32, 0u32)];
        let blob = build_bptx_i64(&mut entries);
        let header = decode_header(&blob).unwrap();
        assert_eq!(header.n_entries, 1);
        assert_eq!(header.tree_height, 1);
        assert_eq!(header.n_leaf_nodes, 1);
        assert_eq!(header.n_internal_nodes, 0);

        let hit = lookup_i64(&blob, 42).unwrap();
        assert_eq!(hit.rg_index, 0);
        assert_eq!(hit.row_offset, 0);
        assert_eq!(lookup_i64(&blob, 99), None);
    }

    #[test]
    fn test_fanout_entries() {
        let mut entries: Vec<(i64, u32, u32)> = (0..DEFAULT_FANOUT_I64 as i64)
            .map(|i| (i, i as u32, (i * 10) as u32))
            .collect();
        let blob = build_bptx_i64(&mut entries);
        let header = decode_header(&blob).unwrap();
        assert_eq!(header.n_entries, DEFAULT_FANOUT_I64 as u32);
        assert_eq!(header.n_leaf_nodes, 1);
        assert_eq!(header.n_internal_nodes, 0);

        for i in 0..DEFAULT_FANOUT_I64 as i64 {
            let hit = lookup_i64(&blob, i).unwrap();
            assert_eq!(hit.rg_index, i as u32);
            assert_eq!(hit.row_offset, (i * 10) as u32);
        }
        assert_eq!(lookup_i64(&blob, -1), None);
        assert_eq!(lookup_i64(&blob, DEFAULT_FANOUT_I64 as i64), None);
    }

    #[test]
    fn test_two_leaves() {
        let n = DEFAULT_FANOUT_I64 + 1;
        let mut entries: Vec<(i64, u32, u32)> = (0..n as i64)
            .map(|i| (i, i as u32, (i * 10) as u32))
            .collect();
        let blob = build_bptx_i64(&mut entries);
        let header = decode_header(&blob).unwrap();
        assert_eq!(header.n_entries, n as u32);
        assert_eq!(header.n_leaf_nodes, 2);
        assert_eq!(header.n_internal_nodes, 1);
        assert_eq!(header.tree_height, 2);

        for i in 0..n as i64 {
            let hit = lookup_i64(&blob, i).unwrap();
            assert_eq!(hit.rg_index, i as u32, "key {} rg mismatch", i);
            assert_eq!(hit.row_offset, (i * 10) as u32, "key {} row mismatch", i);
        }
        assert_eq!(lookup_i64(&blob, -1), None);
        assert_eq!(lookup_i64(&blob, n as i64), None);
    }

    #[test]
    fn test_large_tree() {
        let n = 10_000;
        let mut entries: Vec<(i64, u32, u32)> = (0..n as i64)
            .map(|i| (i * 7, i as u32, i as u32))
            .collect();
        let blob = build_bptx_i64(&mut entries);
        let header = decode_header(&blob).unwrap();
        assert_eq!(header.n_entries, n as u32);
        assert!(header.tree_height >= 2, "tree height should be >= 2 for {} entries", n);

        for i in [0, 100, 500, 999, 5000, 9999] {
            let key = i * 7i64;
            let hit = lookup_i64(&blob, key).unwrap();
            assert_eq!(hit.rg_index, i as u32, "key {} rg mismatch", key);
        }

        assert_eq!(lookup_i64(&blob, -7), None);
        assert_eq!(lookup_i64(&blob, 1), None);
        assert_eq!(lookup_i64(&blob, 70_000), None);
    }

    #[test]
    fn test_very_large_tree() {
        // 100K entries → 3 levels
        let n = 100_000;
        let mut entries: Vec<(i64, u32, u32)> = (0..n as i64)
            .map(|i| (i, i as u32, i as u32))
            .collect();
        let blob = build_bptx_i64(&mut entries);
        let header = decode_header(&blob).unwrap();
        assert_eq!(header.n_entries, n as u32);
        assert!(header.tree_height >= 3, "expected >= 3 levels for 100K entries, got {}", header.tree_height);

        // Spot-check
        for i in [0, 1, 999, 50_000, 99_999] {
            let hit = lookup_i64(&blob, i as i64).unwrap();
            assert_eq!(hit.rg_index, i as u32);
        }
    }

    #[test]
    fn test_deduplication() {
        let mut entries = vec![
            (10i64, 0u32, 0u32),
            (10i64, 1u32, 5u32),
            (20i64, 2u32, 0u32),
        ];
        let blob = build_bptx_i64(&mut entries);
        let header = decode_header(&blob).unwrap();
        assert_eq!(header.n_entries, 2);

        let hit = lookup_i64(&blob, 10).unwrap();
        assert_eq!(hit.rg_index, 1);
        assert_eq!(hit.row_offset, 5);
    }

    #[test]
    fn test_negative_keys() {
        let mut entries = vec![(-100i64, 0u32, 0u32), (-1i64, 1u32, 1u32), (0i64, 2u32, 2u32), (50i64, 3u32, 3u32)];
        let blob = build_bptx_i64(&mut entries);

        let hit = lookup_i64(&blob, -100).unwrap();
        assert_eq!(hit.rg_index, 0);
        let hit = lookup_i64(&blob, -1).unwrap();
        assert_eq!(hit.rg_index, 1);
        let hit = lookup_i64(&blob, 0).unwrap();
        assert_eq!(hit.rg_index, 2);
        let hit = lookup_i64(&blob, 50).unwrap();
        assert_eq!(hit.rg_index, 3);
        assert_eq!(lookup_i64(&blob, -50), None);
    }

    #[test]
    fn test_header_roundtrip() {
        let h = encode_header(0, false, 1000, 3, 128, 48, 1024, 1072, 2048, 200, 78, 20, 0);
        let parsed = decode_header(&h).unwrap();
        assert_eq!(parsed.key_type, 0);
        assert!(!parsed.has_bloom);
        assert_eq!(parsed.n_entries, 1000);
        assert_eq!(parsed.tree_height, 3);
        assert_eq!(parsed.fanout, 128);
        assert_eq!(parsed.internal_section_offset, 48);
        assert_eq!(parsed.internal_section_len, 1024);
        assert_eq!(parsed.leaf_section_offset, 1072);
        assert_eq!(parsed.leaf_section_len, 2048);
        assert_eq!(parsed.root_node_offset, 200);
        assert_eq!(parsed.n_leaf_nodes, 78);
        assert_eq!(parsed.n_internal_nodes, 20);
    }

    #[test]
    fn test_leaf_roundtrip() {
        let entries = vec![(10i64, 1u32, 2u32), (20i64, 3u32, 4u32), (30i64, 5u32, 6u32)];
        let (bytes, max_key) = encode_leaf_i64(&entries);
        assert_eq!(max_key, 30);
        let (keys, values, decoded_max) = decode_leaf_i64(&bytes).unwrap();
        assert_eq!(keys, vec![10, 20, 30]);
        assert_eq!(values, vec![(1, 2), (3, 4), (5, 6)]);
        assert_eq!(decoded_max, 30);
    }

    #[test]
    fn test_internal_roundtrip() {
        let sep_keys = vec![10i64, 20i64, 30i64];
        let child_offsets = vec![100u32, 200u32, 300u32, 400u32];
        let bytes = encode_internal_i64(&sep_keys, &child_offsets);
        let (decoded_keys, decoded_offsets) = decode_internal_i64(&bytes).unwrap();
        assert_eq!(decoded_keys, sep_keys);
        assert_eq!(decoded_offsets, child_offsets);
    }

    #[test]
    fn test_index_meta_roundtrip() {
        let meta = BptxIndexMeta {
            index_type: "bptx".to_string(),
            key_column: "id".to_string(),
            key_type: "i64".to_string(),
            n_entries: 1000,
            blob_hash: "abc123".to_string(),
            blob_size_bytes: 50000,
            tree_height: 2,
            fanout: 128,
            manifest_hash: "def456".to_string(),
            created_at: 1700000000.0,
        };
        let bytes = meta.to_json_bytes();
        let parsed = BptxIndexMeta::from_json_bytes(&bytes).unwrap();
        assert_eq!(parsed.key_column, "id");
        assert_eq!(parsed.n_entries, 1000);
        assert_eq!(parsed.tree_height, 2);
    }

    #[test]
    fn test_blob_size_efficiency() {
        // 10K entries: ~160 KB blob (16 bytes/key leaf + small internal overhead)
        let n = 10_000i64;
        let mut entries: Vec<(i64, u32, u32)> = (0..n)
            .map(|i| (i, (i / 100) as u32, (i % 100) as u32))
            .collect();
        let blob = build_bptx_i64(&mut entries);
        let bytes_per_entry = blob.len() as f64 / n as f64;
        assert!(bytes_per_entry < 20.0,
            "BPTX should use < 20 bytes/entry, got {:.1}", bytes_per_entry);
    }

    // ------------------------------------------------------------------
    // Range scan tests
    // ------------------------------------------------------------------

    #[test]
    fn test_range_scan_empty() {
        let mut entries: Vec<(i64, u32, u32)> = Vec::new();
        let blob = build_bptx_i64(&mut entries);
        assert!(range_scan_i64(&blob, 0, 100).is_empty());
    }

    #[test]
    fn test_range_scan_single_leaf() {
        // 50 entries fit in one leaf (fanout=128)
        let mut entries: Vec<(i64, u32, u32)> = (0..50i64)
            .map(|i| (i, i as u32, i as u32))
            .collect();
        let blob = build_bptx_i64(&mut entries);

        // Full range
        let hits = range_scan_i64(&blob, 0, 49);
        assert_eq!(hits.len(), 50);

        // Partial range [10, 20]
        let hits = range_scan_i64(&blob, 10, 20);
        assert_eq!(hits.len(), 11);
        assert_eq!(hits[0].rg_index, 10);
        assert_eq!(hits[10].rg_index, 20);

        // Range with no matches
        let hits = range_scan_i64(&blob, 100, 200);
        assert!(hits.is_empty());

        // Single-key range
        let hits = range_scan_i64(&blob, 25, 25);
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].rg_index, 25);
    }

    #[test]
    fn test_range_scan_multi_leaf() {
        // 500 entries = 4 leaves (fanout=128)
        let n = 500i64;
        let mut entries: Vec<(i64, u32, u32)> = (0..n)
            .map(|i| (i, (i / 100) as u32, (i % 100) as u32))
            .collect();
        let blob = build_bptx_i64(&mut entries);

        // Range spanning all leaves
        let hits = range_scan_i64(&blob, 0, 499);
        assert_eq!(hits.len(), 500);

        // Range spanning first two leaves [0, 255]
        let hits = range_scan_i64(&blob, 0, 255);
        assert_eq!(hits.len(), 256);

        // Range entirely within second leaf [130, 200]
        let hits = range_scan_i64(&blob, 130, 200);
        assert_eq!(hits.len(), 71);

        // Range in last leaf only [450, 499]
        let hits = range_scan_i64(&blob, 450, 499);
        assert_eq!(hits.len(), 50);
    }

    #[test]
    fn test_range_scan_three_level_tree() {
        // 100K entries → 3+ levels
        let n = 100_000i64;
        let mut entries: Vec<(i64, u32, u32)> = (0..n)
            .map(|i| (i, i as u32, i as u32))
            .collect();
        let blob = build_bptx_i64(&mut entries);

        // Small range in the middle
        let hits = range_scan_i64(&blob, 50_000, 50_099);
        assert_eq!(hits.len(), 100);
        for (i, hit) in hits.iter().enumerate() {
            assert_eq!(hit.rg_index, (50_000 + i) as u32);
        }

        // Full range
        let hits = range_scan_i64(&blob, 0, 99_999);
        assert_eq!(hits.len(), 100_000);
    }

    #[test]
    fn test_range_scan_negative_keys() {
        let mut entries = vec![
            (-100i64, 0u32, 0u32),
            (-50i64, 1u32, 1u32),
            (0i64, 2u32, 2u32),
            (50i64, 3u32, 3u32),
            (100i64, 4u32, 4u32),
        ];
        let blob = build_bptx_i64(&mut entries);

        let hits = range_scan_i64(&blob, -75, 25);
        assert_eq!(hits.len(), 2); // -50, 0 are in [-75, 25]
        assert_eq!(hits[0].rg_index, 1); // -50
        assert_eq!(hits[1].rg_index, 2); // 0
    }

    #[test]
    fn test_range_scan_sparse_keys() {
        // Keys with gaps: 0, 10, 20, ..., 1000
        let mut entries: Vec<(i64, u32, u32)> = (0..101)
            .map(|i| ((i * 10) as i64, i as u32, i as u32))
            .collect();
        let blob = build_bptx_i64(&mut entries);

        // Range [50, 150] should find keys 50, 60, 70, ..., 150
        let hits = range_scan_i64(&blob, 50, 150);
        assert_eq!(hits.len(), 11); // 50,60,70,80,90,100,110,120,130,140,150
    }
}
