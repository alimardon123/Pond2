// PondSlab (PSLB v1) — pack multiple row groups into ONE object with a
// footer index + tail magic. The whole point: read N row groups via 1
// (or 1 + parallel Range GETs) instead of N separate full-object GETs.
//
// WHY THIS EXISTS (the layout-level motivation):
//
// Without slabs, Pond2 stores every PND2 row group as its own blob:
//   blobs/ab/<hash1>   ← 1 S3 GET
//   blobs/cd/<hash2>   ← 1 S3 GET
//   ...
//   blobs/ef/<hashN>   ← 1 S3 GET
// At PB scale (8M+ row groups for a 1 PB / 128 MB table), a 1% selective
// query touching 82K row groups would issue 82K sequential S3 GETs.
// At 50 ms per GET, that's ~68 minutes per cold query. DuckDB-on-S3 does
// the same query in ~120 ms because it stores many row groups in ONE
// Parquet file and uses HTTP Range reads.
//
// PSLB brings Pond2 to parity WITHOUT requiring a local cache:
//   - One slab packs K row groups (target K = 1024, ~128 MB total).
//   - Footer carries per-RG offsets + ColumnStatsEntry (zone maps).
//   - Reader fetches 12-byte tail → footer_offset → footer bytes.
//   - Reader filters entries by predicates (zone-map pruning).
//   - Reader issues parallel Range GETs for surviving RG byte ranges.
//   - Total cold-query cost: 1 (tail) + 1 (footer) + M (parallel) GETs.
//   - For M=8 surviving RGs out of 1024 with 32-way parallelism:
//     1 + 1 + 1 = 3 sequential RTTs ≈ 150 ms.
//   - vs 82K sequential GETs ≈ 68 minutes — a 27,000x speedup cold.
//
// FORMAT (PSLB v1):
//
//   HEADER (10 bytes fixed, at offset 0):
//     0   4   Magic: "PSLB"
//     4   1   Version: 1
//     5   1   Flags: bit0=has_footer_index (always 1 in v1)
//     6   4   n_row_groups: u32 LE
//
//   PAYLOAD (variable, starts at offset 10):
//     for each RG (in order, rg_index = 0, 1, 2, ...):
//       4   rg_len: u32 LE    (length of rg_bytes ONLY, not the prefix)
//       var rg_bytes          (a complete PND2 blob — decodable standalone)
//
//   FOOTER (variable, at offset footer_offset):
//     4   n_entries: u32 LE  (must equal n_row_groups)
//     for each entry (rg_index ascending):
//       4   rg_index: u32 LE
//       8   byte_offset: u64 LE  (offset from slab start to rg_bytes start;
//                                 i.e. 10 + sum(prev rg_len + 4))
//       4   byte_len: u32 LE     (length of rg_bytes ONLY)
//       4   n_rows: u32 LE
//       1   n_columns: u8
//       for each column:
//         1   name_len: u8
//         var name (UTF-8)
//         1   vtype: u8  (matches pond_core VT_*)
//         1   has_stats: u8  (0 or 1)
//         [if has_stats == 1]:
//           4   min_len: u32 LE
//           var min_bytes
//           4   max_len: u32 LE
//           var max_bytes
//         4   null_count: u32 LE
//
//   TAIL (12 bytes fixed, at the end of the slab):
//     0   4   Magic: "PSLB"   (echo of header magic)
//     4   8   footer_offset: u64 LE
//
// READER ALGORITHM (the "Range-Read" path):
//   1. Range GET [total_len - 12, total_len)        → 12 bytes (tail)
//   2. Verify tail magic == "PSLB"; read footer_offset (u64 LE)
//   3. Range GET [footer_offset, total_len - 12)    → footer bytes
//   4. Decode footer → Vec<SlabEntry> (offsets, lens, stats)
//   5. Filter entries via predicate zone-map pruning
//   6. For each surviving entry: Range GET [byte_offset, byte_offset + byte_len)
//   7. Decode each range as a PND2 blob (pond_core::pnd2_decode)
//
// WRITER ALGORITHM (used by future write_rows_slab() — not this cycle):
//   1. Encode each row group as a PND2 blob (pnd2_encode_i64_auto etc.)
//   2. Compute ColumnStatsEntry for each column of each RG
//   3. Concatenate: header + Σ(rg_len_prefix + rg_bytes) + footer + tail
//   4. Compute SHA-256 → slab hash → put_blob
//   5. Manifest entry: { slab_hash, byte_offset, byte_len, n_rows, columns }
//      (replaces the current one-RG-per-blob RowGroupEntry layout)
//
// BACKWARD COMPAT:
//   PSLB is a NEW magic. Existing PNPK / PMAN / PND2 blobs are untouched.
//   `get_blob_range` has a default impl on the ObjectStore trait (full GET
//   + slice) so all backends continue to work — slabs just fall back to
//   one full-object GET, which is the same cost as today. Backends that
//   override `get_blob_range` (LocalFS, S3) get the speedup.

use crate::manifest::ColumnStatsEntry;

const PSLB_MAGIC: &[u8; 4] = b"PSLB";
const PSLB_VERSION: u8 = 1;
const PSLB_FLAG_HAS_FOOTER: u8 = 0x01;

/// Total size of the fixed TAIL block at the end of every PSLB slab.
/// Readers fetch exactly this many bytes in step 1 of the read algorithm.
pub const PSLB_TAIL_LEN: usize = 12;

/// Total size of the fixed HEADER at the start of every PSLB slab.
pub const PSLB_HEADER_LEN: usize = 10;

// ---------------------------------------------------------------------------
// Slab structures (mirror RowGroupEntry shape, but with byte ranges instead
// of blob_hash — one slab blob holds many RGs, so we need offsets not hashes)
// ---------------------------------------------------------------------------

/// A single row-group entry in a slab footer.
///
/// `byte_offset` and `byte_len` together describe a half-open byte range
/// inside the slab blob: `[byte_offset, byte_offset + byte_len)`. The reader
/// fetches exactly those bytes via `get_blob_range(slab_hash, off, off+len)`
/// and decodes them as a standalone PND2 blob.
#[derive(Debug, Clone)]
pub struct SlabEntry {
    pub rg_index: u32,
    pub byte_offset: u64,
    pub byte_len: u32,
    pub n_rows: u32,
    pub columns: Vec<ColumnStatsEntry>,
}

impl SlabEntry {
    /// Can this entry be pruned given a list of `(column, op, value)` predicates?
    /// Returns true if the entry CANNOT match ANY predicate (should be skipped).
    /// Reuses `ColumnStatsEntry::can_prune` so pruning logic stays in one place.
    pub fn can_prune(&self, predicates: &[(String, String, Vec<u8>)]) -> bool {
        for (col_name, op, value) in predicates {
            if let Some(col) = self.columns.iter().find(|c| c.name == *col_name) {
                if col.can_prune(op, value) {
                    return true;
                }
            }
        }
        false
    }
}

/// The slab footer — a list of `SlabEntry` records.
#[derive(Debug, Clone)]
pub struct SlabFooter {
    pub entries: Vec<SlabEntry>,
}

impl SlabFooter {
    pub fn n_entries(&self) -> usize {
        self.entries.len()
    }

    /// Plan the byte ranges to fetch given optional predicates.
    ///
    /// Without predicates: returns ALL RG byte ranges (full scan).
    /// With predicates: skips any entry whose zone-map stats prove it cannot
    /// match — same `can_prune` logic as the existing manifest path.
    ///
    /// Returns a Vec of `(byte_offset, byte_len)` tuples for the reader to
    /// fetch via `get_blob_range(slab_hash, byte_offset, byte_offset + byte_len)`.
    pub fn plan_ranges(
        &self,
        predicates: Option<&[(String, String, Vec<u8>)]>,
    ) -> Vec<(u64, u32)> {
        match predicates {
            None => self.entries.iter()
                .map(|e| (e.byte_offset, e.byte_len))
                .collect(),
            Some(preds) => self.entries.iter()
                .filter(|e| !e.can_prune(preds))
                .map(|e| (e.byte_offset, e.byte_len))
                .collect(),
        }
    }
}

/// In-memory representation of a fully-decoded slab. The row group bytes
/// are kept as raw `Vec<u8>` so the caller can pass them to `pnd2_decode`
/// (or skip decoding entirely if only metadata is needed).
#[derive(Debug, Clone)]
pub struct Slab {
    pub row_groups: Vec<Vec<u8>>,
    pub footer: SlabFooter,
}

// ---------------------------------------------------------------------------
// Encoding
// ---------------------------------------------------------------------------

/// Encode a list of row-group byte blobs + per-RG column stats into a
/// single PSLB v1 blob.
///
/// `row_groups[i].0` is the PND2 bytes; `row_groups[i].1` is the per-column
/// stats (can be empty if stats are not available — reader won't prune);
/// `row_groups[i].2` is the row count for that RG.
///
/// Returns the full slab bytes (header + payloads + footer + tail). The
/// caller should `kernel.write(&slab_bytes)` to get a content-addressed hash.
pub fn encode_slab(row_groups: &[(Vec<u8>, Vec<ColumnStatsEntry>, u32)]) -> Vec<u8> {
    let n = row_groups.len();

    // Pre-compute total size to avoid reallocations.
    // Payload = 4-byte length prefix + RG bytes (NO footer entry here —
    // footer entries are appended after all payloads, see below).
    let payload_size: usize = row_groups.iter()
        .map(|(bytes, _, _)| 4 + bytes.len())
        .sum();
    let footer_size = 4 + row_groups.iter()
        .map(|(_, cols, _)| footer_entry_size(cols))
        .sum::<usize>();
    let total = PSLB_HEADER_LEN + payload_size + footer_size + PSLB_TAIL_LEN;

    let mut buf = Vec::with_capacity(total);

    // ---- Header ----
    buf.extend_from_slice(PSLB_MAGIC);
    buf.push(PSLB_VERSION);
    buf.push(PSLB_FLAG_HAS_FOOTER);
    buf.extend_from_slice(&(n as u32).to_le_bytes());

    // ---- Payloads + record per-RG offsets ----
    // We need the offset of each rg_bytes (the byte AFTER the 4-byte length prefix).
    let mut offsets: Vec<u64> = Vec::with_capacity(n);
    let mut cursor = PSLB_HEADER_LEN as u64;
    for (bytes, _, _) in row_groups {
        // rg_len prefix
        buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
        cursor += 4;
        // rg_bytes start
        offsets.push(cursor);
        buf.extend_from_slice(bytes);
        cursor += bytes.len() as u64;
    }
    let footer_offset = cursor;

    // ---- Footer ----
    buf.extend_from_slice(&(n as u32).to_le_bytes());
    for (i, (bytes, cols, n_rows)) in row_groups.iter().enumerate() {
        encode_footer_entry(&mut buf, i as u32, offsets[i], bytes.len() as u32, *n_rows, cols);
    }

    // ---- Tail ----
    buf.extend_from_slice(PSLB_MAGIC);
    buf.extend_from_slice(&footer_offset.to_le_bytes());

    debug_assert_eq!(buf.len(), total, "PSLB size prediction was wrong");
    buf
}

/// Size of one footer entry given its column stats (used for size prediction).
fn footer_entry_size(cols: &[ColumnStatsEntry]) -> usize {
    let mut s = 4 + 8 + 4 + 4 + 1; // rg_index + byte_offset + byte_len + n_rows + n_columns
    for c in cols {
        s += 1 + c.name.len()   // name_len + name
           + 1                  // vtype
           + 1;                 // has_stats flag
        if let (Some(min), Some(max)) = (&c.min, &c.max) {
            s += 4 + min.len()
               + 4 + max.len();
        }
        s += 4; // null_count
    }
    s
}

/// Encode one footer entry. Mirrors the per-column stats encoding used by
/// `manifest.rs` (same `has_stats` flag + min/max/null_count layout) so the
/// existing `ColumnStatsEntry::can_prune` logic works on slab entries too.
fn encode_footer_entry(
    buf: &mut Vec<u8>,
    rg_index: u32,
    byte_offset: u64,
    byte_len: u32,
    n_rows: u32,
    cols: &[ColumnStatsEntry],
) {
    buf.extend_from_slice(&rg_index.to_le_bytes());
    buf.extend_from_slice(&byte_offset.to_le_bytes());
    buf.extend_from_slice(&byte_len.to_le_bytes());
    buf.extend_from_slice(&n_rows.to_le_bytes());
    // Validate n_columns fits in u8 — names > 255 cols are rejected loudly
    // (silent truncation would produce a corrupt slab).
    assert!(cols.len() <= u8::MAX as usize,
        "PSLB encode_footer_entry: cols.len() {} exceeds u8::MAX (255) — \
         slab would be corrupt. Split the slab or reduce column count.", cols.len());
    buf.push(cols.len() as u8);
    for col in cols {
        let name_bytes = col.name.as_bytes();
        // Same: validate name fits in u8 (255-byte column names are absurd
        // but we should still fail loudly, not silently truncate).
        assert!(name_bytes.len() <= u8::MAX as usize,
            "PSLB encode_footer_entry: column name '{}' is {} bytes, exceeds u8::MAX (255)",
            col.name, name_bytes.len());
        buf.push(name_bytes.len() as u8);
        buf.extend_from_slice(name_bytes);
        buf.push(col.value_type);
        if let (Some(min), Some(max)) = (&col.min, &col.max) {
            buf.push(1); // has_stats
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

// ---------------------------------------------------------------------------
// Decoding
// ---------------------------------------------------------------------------

/// Decode the 12-byte tail of a PSLB slab.
///
/// Returns `(footer_offset, valid_magic)` where `valid_magic` is true if the
/// tail starts with the "PSLB" magic. Callers should reject slabs whose
/// magic doesn't match.
///
/// `tail` MUST be exactly `PSLB_TAIL_LEN` (12) bytes long, taken from the
/// last 12 bytes of the slab blob.
pub fn decode_slab_tail(tail: &[u8]) -> Option<(u64, bool)> {
    if tail.len() < PSLB_TAIL_LEN {
        return None;
    }
    // If caller passes more than 12 bytes (e.g. the whole slab), use the
    // LAST 12 bytes — the tail is always at the end of the blob. This
    // matches the documented contract: "taken from the last 12 bytes".
    let tail_start = tail.len() - PSLB_TAIL_LEN;
    let magic = &tail[tail_start..tail_start + 4];
    let valid = magic == PSLB_MAGIC;
    let footer_offset = u64::from_le_bytes([
        tail[tail_start + 4], tail[tail_start + 5],
        tail[tail_start + 6], tail[tail_start + 7],
        tail[tail_start + 8], tail[tail_start + 9],
        tail[tail_start + 10], tail[tail_start + 11],
    ]);
    Some((footer_offset, valid))
}

/// Decode the footer bytes (everything between `footer_offset` and
/// `total_len - PSLB_TAIL_LEN`) into a `SlabFooter`.
///
/// `footer_bytes` should be the exact bytes fetched via
/// `get_blob_range(slab_hash, footer_offset, total_len - PSLB_TAIL_LEN)`.
pub fn decode_slab_footer(footer_bytes: &[u8]) -> Option<SlabFooter> {
    if footer_bytes.len() < 4 {
        return None;
    }
    let n_entries = u32::from_le_bytes([
        footer_bytes[0], footer_bytes[1], footer_bytes[2], footer_bytes[3],
    ]) as usize;
    let mut pos = 4;
    let mut entries = Vec::with_capacity(n_entries);

    for _ in 0..n_entries {
        if pos + 4 + 8 + 4 + 4 + 1 > footer_bytes.len() {
            return None;
        }
        let rg_index = u32::from_le_bytes([
            footer_bytes[pos], footer_bytes[pos+1],
            footer_bytes[pos+2], footer_bytes[pos+3],
        ]);
        pos += 4;
        let byte_offset = u64::from_le_bytes([
            footer_bytes[pos], footer_bytes[pos+1], footer_bytes[pos+2],
            footer_bytes[pos+3], footer_bytes[pos+4], footer_bytes[pos+5],
            footer_bytes[pos+6], footer_bytes[pos+7],
        ]);
        pos += 8;
        let byte_len = u32::from_le_bytes([
            footer_bytes[pos], footer_bytes[pos+1],
            footer_bytes[pos+2], footer_bytes[pos+3],
        ]);
        pos += 4;
        let n_rows = u32::from_le_bytes([
            footer_bytes[pos], footer_bytes[pos+1],
            footer_bytes[pos+2], footer_bytes[pos+3],
        ]);
        pos += 4;
        let n_columns = footer_bytes[pos] as usize;
        pos += 1;

        let mut columns = Vec::with_capacity(n_columns);
        for _ in 0..n_columns {
            if pos + 1 > footer_bytes.len() { return None; }
            let name_len = footer_bytes[pos] as usize;
            pos += 1;
            // Bounds check: we need name bytes + vtype byte + has_stats byte.
            // That's name_len + 2 bytes after pos. (Off-by-one fix: previously
            // checked +1 which let has_stats read past EOF.)
            if pos + name_len + 2 > footer_bytes.len() { return None; }
            let name = String::from_utf8_lossy(
                &footer_bytes[pos..pos+name_len]
            ).to_string();
            pos += name_len;
            let vtype = footer_bytes[pos];
            pos += 1;
            let has_stats = footer_bytes[pos];
            pos += 1;

            let (min, max) = if has_stats == 1 {
                if pos + 4 > footer_bytes.len() { return None; }
                let min_len = u32::from_le_bytes([
                    footer_bytes[pos], footer_bytes[pos+1],
                    footer_bytes[pos+2], footer_bytes[pos+3],
                ]) as usize;
                pos += 4;
                if pos + min_len + 4 > footer_bytes.len() { return None; }
                let min = Some(footer_bytes[pos..pos+min_len].to_vec());
                pos += min_len;
                let max_len = u32::from_le_bytes([
                    footer_bytes[pos], footer_bytes[pos+1],
                    footer_bytes[pos+2], footer_bytes[pos+3],
                ]) as usize;
                pos += 4;
                if pos + max_len > footer_bytes.len() { return None; }
                let max = Some(footer_bytes[pos..pos+max_len].to_vec());
                pos += max_len;
                (min, max)
            } else {
                (None, None)
            };

            if pos + 4 > footer_bytes.len() { return None; }
            let null_count = u32::from_le_bytes([
                footer_bytes[pos], footer_bytes[pos+1],
                footer_bytes[pos+2], footer_bytes[pos+3],
            ]);
            pos += 4;

            columns.push(ColumnStatsEntry {
                name,
                value_type: vtype,
                min,
                max,
                null_count,
            });
        }

        entries.push(SlabEntry {
            rg_index,
            byte_offset,
            byte_len,
            n_rows,
            columns,
        });
    }

    Some(SlabFooter { entries })
}

/// Fully decode a PSLB slab from its full bytes (header + payloads + footer + tail).
///
/// This is the "decode everything" path — used by tests and by callers that
/// already have the full slab bytes in memory. The production read path
/// should use `decode_slab_tail` + `decode_slab_footer` + `plan_ranges` +
/// `get_blob_range` to avoid fetching the whole slab.
pub fn decode_slab(blob: &[u8]) -> Option<Slab> {
    if blob.len() < PSLB_HEADER_LEN + PSLB_TAIL_LEN {
        return None;
    }
    if &blob[0..4] != PSLB_MAGIC {
        return None;
    }
    if blob[4] != PSLB_VERSION {
        return None;
    }

    let n_row_groups = u32::from_le_bytes([
        blob[6], blob[7], blob[8], blob[9],
    ]) as usize;

    // Decode the tail to find footer_offset.
    let tail_start = blob.len() - PSLB_TAIL_LEN;
    let tail = &blob[tail_start..];
    let (footer_offset, valid_magic) = decode_slab_tail(tail)?;
    if !valid_magic {
        return None;
    }
    if footer_offset as usize + PSLB_TAIL_LEN > blob.len() {
        return None;
    }

    // Decode footer.
    let footer_bytes = &blob[footer_offset as usize..tail_start];
    let footer = decode_slab_footer(footer_bytes)?;
    if footer.n_entries() != n_row_groups {
        return None;
    }

    // Decode payloads using the footer's offsets.
    // Use checked_add to prevent integer overflow on attacker-controlled
    // byte_offset / byte_len (a malformed slab could claim byte_offset near
    // u64::MAX, causing `start + byte_len` to wrap to a small value that
    // passes the `end > tail_start` check, then panics on the slice op.
    let mut row_groups = Vec::with_capacity(n_row_groups);
    for entry in &footer.entries {
        let start = entry.byte_offset as usize;
        let end = start.checked_add(entry.byte_len as usize)?;
        if end > tail_start {
            return None;
        }
        row_groups.push(blob[start..end].to_vec());
    }

    Some(Slab { row_groups, footer })
}

/// Detect whether a byte slice is a PSLB slab (checks header magic).
pub fn is_slab(blob: &[u8]) -> bool {
    blob.len() >= PSLB_HEADER_LEN && &blob[0..4] == PSLB_MAGIC
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::ColumnStatsEntry;

    fn make_stats(name: &str, vtype: u8, min: i64, max: i64) -> ColumnStatsEntry {
        ColumnStatsEntry {
            name: name.to_string(),
            value_type: vtype,
            min: Some(min.to_le_bytes().to_vec()),
            max: Some(max.to_le_bytes().to_vec()),
            null_count: 0,
        }
    }

    fn make_rg(payload: &[u8]) -> Vec<u8> {
        // Pretend this is a PND2 blob — for slab tests we only care about
        // byte-packing, not actual PND2 decoding.
        payload.to_vec()
    }

    #[test]
    fn test_encode_decode_roundtrip_3_rgs() {
        let rgs = vec![
            (make_rg(b"rg0-data"), vec![make_stats("id", 1, 0, 100)], 10),
            (make_rg(b"rg1-bytes-here"), vec![make_stats("id", 1, 101, 200)], 20),
            (make_rg(b"rg2"), vec![make_stats("id", 1, 201, 300)], 5),
        ];
        let blob = encode_slab(&rgs);
        assert!(is_slab(&blob));

        let slab = decode_slab(&blob).expect("decode failed");
        assert_eq!(slab.row_groups.len(), 3);
        assert_eq!(slab.row_groups[0], b"rg0-data");
        assert_eq!(slab.row_groups[1], b"rg1-bytes-here");
        assert_eq!(slab.row_groups[2], b"rg2");
        assert_eq!(slab.footer.n_entries(), 3);

        // Verify per-RG offsets point to the correct bytes.
        for (i, entry) in slab.footer.entries.iter().enumerate() {
            let start = entry.byte_offset as usize;
            let end = start + entry.byte_len as usize;
            assert_eq!(&blob[start..end], &*rgs[i].0,
                "RG {} byte range mismatch", i);
        }
    }

    #[test]
    fn test_tail_decode_yields_footer_offset() {
        let rgs = vec![
            (make_rg(b"first-rg"), vec![], 0),
            (make_rg(b"second-rg"), vec![], 0),
        ];
        let blob = encode_slab(&rgs);
        let total = blob.len();

        // Simulate the read path: fetch ONLY the last 12 bytes.
        let tail = &blob[total - PSLB_TAIL_LEN..];
        let (footer_offset, valid) = decode_slab_tail(tail).expect("tail decode");
        assert!(valid, "tail magic should be PSLB");

        // Now fetch the footer (between footer_offset and total - 12).
        let footer_bytes = &blob[footer_offset as usize..total - PSLB_TAIL_LEN];
        let footer = decode_slab_footer(footer_bytes).expect("footer decode");
        assert_eq!(footer.n_entries(), 2);
        // byte_offset points to the rg_bytes start (AFTER the 4-byte length prefix).
        // First RG: header(10) + 4-byte length prefix → rg_bytes at offset 14.
        assert_eq!(footer.entries[0].byte_offset, (PSLB_HEADER_LEN + 4) as u64);
        assert_eq!(footer.entries[0].byte_len, 8);  // "first-rg"
        // Second RG starts after: header(10) + 4(len0) + 8(rg0) + 4(len1) = 26
        assert_eq!(footer.entries[1].byte_offset, (PSLB_HEADER_LEN + 4 + 8 + 4) as u64);
        assert_eq!(footer.entries[1].byte_len, 9); // "second-rg"
    }

    #[test]
    fn test_plan_ranges_no_predicates() {
        let rgs = vec![
            (make_rg(b"a"), vec![make_stats("id", 1, 0, 100)], 50),
            (make_rg(b"b"), vec![make_stats("id", 1, 101, 200)], 50),
            (make_rg(b"c"), vec![make_stats("id", 1, 201, 300)], 50),
        ];
        let blob = encode_slab(&rgs);
        let slab = decode_slab(&blob).unwrap();

        // Verify n_rows is preserved in footer
        assert_eq!(slab.footer.entries[0].n_rows, 50);
        assert_eq!(slab.footer.entries[1].n_rows, 50);
        assert_eq!(slab.footer.entries[2].n_rows, 50);

        let ranges = slab.footer.plan_ranges(None);
        assert_eq!(ranges.len(), 3, "no predicates → all 3 RGs");
    }

    #[test]
    fn test_plan_ranges_with_predicate_prunes() {
        // Three RGs with id ranges [0,100], [101,200], [201,300].
        // Predicate id = 150 should prune RG 0 (id<101) and RG 2 (id>200),
        // leaving only RG 1.
        let rgs = vec![
            (make_rg(b"a"), vec![make_stats("id", 1, 0, 100)], 100),
            (make_rg(b"b"), vec![make_stats("id", 1, 101, 200)], 100),
            (make_rg(b"c"), vec![make_stats("id", 1, 201, 300)], 100),
        ];
        let blob = encode_slab(&rgs);
        let slab = decode_slab(&blob).unwrap();

        // id = 150 (i64 LE bytes)
        let val = 150i64.to_le_bytes().to_vec();
        let preds: Vec<(String, String, Vec<u8>)> = vec![
            ("id".to_string(), "=".to_string(), val),
        ];
        let ranges = slab.footer.plan_ranges(Some(&preds));
        assert_eq!(ranges.len(), 1, "predicate id=150 should prune 2 of 3 RGs");
        // The surviving RG should be RG 1 ("b").
        // byte_offset = header(10) + 4(len0) + 1(rg0) + 4(len1) = 19
        assert_eq!(ranges[0].0, (PSLB_HEADER_LEN + 4 + 1 + 4) as u64);
        assert_eq!(ranges[0].1, 1);  // "b"
    }

    #[test]
    fn test_plan_ranges_with_range_predicate() {
        // Predicate id >= 150 should prune only RG 0 ([0,100]).
        let rgs = vec![
            (make_rg(b"a"), vec![make_stats("id", 1, 0, 100)], 100),
            (make_rg(b"b"), vec![make_stats("id", 1, 101, 200)], 100),
            (make_rg(b"c"), vec![make_stats("id", 1, 201, 300)], 100),
        ];
        let blob = encode_slab(&rgs);
        let slab = decode_slab(&blob).unwrap();

        let val = 150i64.to_le_bytes().to_vec();
        let preds: Vec<(String, String, Vec<u8>)> = vec![
            ("id".to_string(), ">=".to_string(), val),
        ];
        let ranges = slab.footer.plan_ranges(Some(&preds));
        assert_eq!(ranges.len(), 2, "id>=150 should prune only RG 0");
    }

    #[test]
    fn test_empty_slab() {
        let rgs: Vec<(Vec<u8>, Vec<ColumnStatsEntry>, u32)> = vec![];
        let blob = encode_slab(&rgs);
        assert!(is_slab(&blob));

        let slab = decode_slab(&blob).expect("empty slab should decode");
        assert_eq!(slab.row_groups.len(), 0);
        assert_eq!(slab.footer.n_entries(), 0);

        // Plan with no predicates should yield 0 ranges.
        let ranges = slab.footer.plan_ranges(None);
        assert!(ranges.is_empty());
    }

    #[test]
    fn test_single_rg_slab() {
        let rgs = vec![
            (make_rg(b"only-rg"), vec![make_stats("v", 1, 42, 42)], 1),
        ];
        let blob = encode_slab(&rgs);

        let slab = decode_slab(&blob).expect("single-RG slab should decode");
        assert_eq!(slab.row_groups.len(), 1);
        assert_eq!(slab.row_groups[0], b"only-rg");
        // byte_offset = header(10) + 4-byte length prefix = 14
        assert_eq!(slab.footer.entries[0].byte_offset, (PSLB_HEADER_LEN + 4) as u64);
        assert_eq!(slab.footer.entries[0].byte_len, 7);  // "only-rg"
    }

    #[test]
    fn test_decode_rejects_non_slab() {
        assert!(!is_slab(b"not a slab"));
        assert!(!is_slab(b"PSLB"));  // too short
        assert!(decode_slab(b"random bytes").is_none());
        assert!(decode_slab(b"PMAN manifest bytes here").is_none());
    }

    #[test]
    fn test_decode_rejects_wrong_magic_in_tail() {
        // Build a valid slab, then corrupt the tail magic.
        let rgs = vec![(make_rg(b"x"), vec![], 0)];
        let mut blob = encode_slab(&rgs);
        let total = blob.len();
        // Corrupt the last 12 bytes' magic.
        blob[total - 12] = b'X';
        blob[total - 11] = b'X';
        blob[total - 10] = b'X';
        blob[total - 9] = b'X';
        // Header magic is still valid, but the tail magic is not.
        // decode_slab should reject this.
        assert!(decode_slab(&blob).is_none(),
            "corrupted tail magic should be rejected");
    }

    #[test]
    fn test_slab_with_multiple_columns_and_partial_stats() {
        // Mix of stats: some columns have stats, some don't.
        let rgs = vec![
            (make_rg(b"rg0"), vec![
                make_stats("id", 1, 0, 100),
                ColumnStatsEntry {
                    name: "name".to_string(),
                    value_type: 3, // VT_STRING
                    min: None,
                    max: None,
                    null_count: 5,
                },
                make_stats("score", 2, 0, 100),  // f64 stored as bytes
            ], 15),
        ];
        let blob = encode_slab(&rgs);
        let slab = decode_slab(&blob).expect("decode failed");

        assert_eq!(slab.footer.entries[0].columns.len(), 3);
        // Verify n_rows is preserved
        assert_eq!(slab.footer.entries[0].n_rows, 15);
        assert_eq!(slab.footer.entries[0].columns[0].name, "id");
        assert!(slab.footer.entries[0].columns[0].min.is_some());
        assert_eq!(slab.footer.entries[0].columns[1].name, "name");
        assert!(slab.footer.entries[0].columns[1].min.is_none());
        assert_eq!(slab.footer.entries[0].columns[1].null_count, 5);
        assert_eq!(slab.footer.entries[0].columns[2].name, "score");
        assert!(slab.footer.entries[0].columns[2].min.is_some());
    }

    #[test]
    fn test_slab_preserves_payload_byte_for_byte() {
        // Verify that the bytes returned by plan_ranges, when fetched from
        // the slab, exactly match the original RG bytes. This is the
        // invariant the production read path depends on.
        let rgs = vec![
            (make_rg(b"\x00\x01\x02\x03PND2 header bytes here"), vec![], 0),
            (make_rg(b"\xff\xfe\xfdmore bytes"), vec![], 0),
            (make_rg(b"third"), vec![], 0),
        ];
        let blob = encode_slab(&rgs);
        let slab = decode_slab(&blob).unwrap();

        let ranges = slab.footer.plan_ranges(None);
        assert_eq!(ranges.len(), 3);
        for (i, (off, len)) in ranges.iter().enumerate() {
            let start = *off as usize;
            let end = start + *len as usize;
            assert_eq!(&blob[start..end], &*rgs[i].0,
                "RG {} bytes don't match original", i);
        }
    }

    #[test]
    fn test_decode_rejects_truncated_footer() {
        // Regression test for C1: a malformed slab whose footer is truncated
        // mid-column-name must NOT panic — it must return None.
        let rgs = vec![
            (make_rg(b"x"), vec![make_stats("id", 1, 0, 100)], 0),
        ];
        let blob = encode_slab(&rgs);

        // Find the footer offset (from the tail).
        let total = blob.len();
        let tail = &blob[total - PSLB_TAIL_LEN..];
        let (footer_offset, _) = decode_slab_tail(tail).unwrap();
        let footer_len = total - PSLB_TAIL_LEN - footer_offset as usize;

        // Truncate the footer by 5 bytes — should NOT panic, just return None.
        let mut truncated = blob.clone();
        truncated.truncate(total - PSLB_TAIL_LEN - 5);
        // Re-append the tail so decode_slab_tail still works.
        truncated.extend_from_slice(&blob[total - PSLB_TAIL_LEN..]);

        let result = decode_slab(&truncated);
        assert!(result.is_none(),
            "truncated footer must return None, not panic. footer_len was {}", footer_len);
    }

    #[test]
    fn test_decode_rejects_massive_byte_offset_no_overflow() {
        // Regression test for C2: a slab claiming byte_offset = u64::MAX
        // must NOT cause integer overflow in `start + byte_len`. Must return
        // None instead of panicking.
        let rgs = vec![(make_rg(b"x"), vec![], 0)];
        let mut blob = encode_slab(&rgs);

        // Locate the footer (single entry) and corrupt its byte_offset to u64::MAX.
        let total = blob.len();
        let tail = &blob[total - PSLB_TAIL_LEN..];
        let (footer_offset, _) = decode_slab_tail(tail).unwrap();

        // Footer layout: n_entries(4) + entry: rg_index(4) + byte_offset(8) + ...
        // byte_offset starts at footer_offset + 4 (n_entries) + 4 (rg_index) = +8.
        let boff_pos = footer_offset as usize + 8;
        let max_bytes = u64::MAX.to_le_bytes();
        blob[boff_pos..boff_pos + 8].copy_from_slice(&max_bytes);

        // decode_slab must NOT panic — it must return None due to the
        // checked_add failing on byte_offset + byte_len overflow.
        let result = std::panic::catch_unwind(|| decode_slab(&blob));
        assert!(result.is_ok(), "decode_slab must not panic on u64::MAX byte_offset");
        assert!(result.unwrap().is_none(),
            "decode_slab must reject slab with byte_offset = u64::MAX");
    }

    #[test]
    fn test_decode_tail_uses_last_12_bytes_when_input_longer() {
        // Regression test for H4: if caller passes more than 12 bytes to
        // decode_slab_tail, it must use the LAST 12 bytes (where the tail
        // actually lives), not the first 12.
        let rgs = vec![(make_rg(b"x"), vec![], 0)];
        let blob = encode_slab(&rgs);

        // Pass the whole blob to decode_slab_tail.
        let (footer_offset, valid) = decode_slab_tail(&blob).expect("must decode");
        assert!(valid, "tail magic must be PSLB even when input is the whole blob");

        // footer_offset must point to the actual footer, not garbage.
        let total = blob.len();
        let footer_bytes = &blob[footer_offset as usize..total - PSLB_TAIL_LEN];
        let footer = decode_slab_footer(footer_bytes).expect("footer must decode");
        assert_eq!(footer.n_entries(), 1, "footer must have 1 entry");
    }
}
