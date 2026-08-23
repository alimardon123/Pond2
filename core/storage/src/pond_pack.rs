// PondPack (PNPK) — ONE blob containing commit + manifest.
//
// Combines commit JSON + manifest bytes into ONE blob. The HEAD ref points
// to the pack hash. Reading the pack gives you both the commit metadata AND
// the manifest in ONE GET (saves 1-2 GETs per cold read, 1 PUT per write).
//
// FORMAT (PNPK v2):
//   Magic:              "PNPK" (4 bytes)
//   Version:            2 (1 byte)
//   Flags:              has_inline_data (1 byte)
//   commit_json_len:    4 bytes (u32 LE)
//   commit_json:        variable (UTF-8 JSON)
//   manifest_len:       4 bytes (u32 LE)
//   manifest_bytes:     variable (PMAN format)
//   [if has_inline_data]:
//     n_data_blobs:     2 bytes (u16 LE)
//     for each blob:
//       data_len:       4 bytes (u32 LE)
//       data_bytes:     variable
//
// The pack's hash = SHA-256(pack_bytes). Content-addressed, immutable.
// HEAD ref → pack_hash. manifest_ref → pack_hash (same blob).
//
// BACKWARD COMPATIBILITY:
//   Old collections have separate commit (JSON) + manifest (PMAN) blobs.
//   The read path checks the magic bytes:
//     "PNPK" → pack format, extract commit + manifest from one blob
//     "{"    → old JSON commit, read manifest separately via manifest_ref
//     "PMAN" → old standalone manifest

use serde_json::Value;

pub const PNPK_MAGIC: &[u8; 4] = b"PNPK";
const PNPK_VERSION: u8 = 2;

const FLAG_HAS_INLINE_DATA: u8 = 0x01;

/// Encode a commit + manifest into a single PNPK blob.
///
/// Args:
///   - commit: The commit object as a serde_json::Value
///   - manifest_bytes: The encoded manifest bytes (PMAN format)
///   - inline_data: Optional list of inline data blobs (for small collections)
///
/// Returns: The PNPK blob bytes.
pub fn encode_pack(
    commit: &Value,
    manifest_bytes: &[u8],
    inline_data: Option<&[Vec<u8>]>,
) -> Vec<u8> {
    let commit_json = serde_json::to_vec(commit).unwrap_or_default();
    let has_inline = inline_data.map(|d| !d.is_empty()).unwrap_or(false);
    let flags = if has_inline { FLAG_HAS_INLINE_DATA } else { 0u8 };

    let mut buf = Vec::with_capacity(
        4 + 1 + 1 + 4 + commit_json.len() + 4 + manifest_bytes.len() +
        if has_inline { 2 + inline_data.unwrap().iter().map(|d| 4 + d.len()).sum::<usize>() } else { 0 }
    );

    // Header
    buf.extend_from_slice(PNPK_MAGIC);
    buf.push(PNPK_VERSION);
    buf.push(flags);

    // Commit JSON
    buf.extend_from_slice(&(commit_json.len() as u32).to_le_bytes());
    buf.extend_from_slice(&commit_json);

    // Manifest bytes
    buf.extend_from_slice(&(manifest_bytes.len() as u32).to_le_bytes());
    buf.extend_from_slice(manifest_bytes);

    // Inline data blobs (optional)
    if has_inline {
        let data = inline_data.unwrap();
        buf.extend_from_slice(&(data.len() as u16).to_le_bytes());
        for blob in data {
            buf.extend_from_slice(&(blob.len() as u32).to_le_bytes());
            buf.extend_from_slice(blob);
        }
    }

    buf
}

/// Type alias for inline row data (list of row-payload byte vectors).
type InlineRows = Option<Vec<Vec<u8>>>;

/// Decode a PNPK blob into (commit, manifest_bytes, inline_data).
///
/// Returns None if the blob is not a valid PNPK pack.
pub fn decode_pack(blob: &[u8]) -> Option<(Value, Vec<u8>, InlineRows)> {
    if blob.len() < 10 || &blob[0..4] != PNPK_MAGIC {
        return None;
    }

    let version = blob[4];
    if version != PNPK_VERSION && version != 1 {
        return None; // unsupported version
    }

    let flags = blob[5];
    let has_inline = (flags & FLAG_HAS_INLINE_DATA) != 0;

    let mut pos = 6;

    // Read commit JSON
    if pos + 4 > blob.len() { return None; }
    let commit_len = u32::from_le_bytes([
        blob[pos], blob[pos+1], blob[pos+2], blob[pos+3]
    ]) as usize;
    pos += 4;
    if pos + commit_len > blob.len() { return None; }
    let commit: Value = serde_json::from_slice(&blob[pos..pos + commit_len]).ok()?;
    pos += commit_len;

    // Read manifest bytes
    if pos + 4 > blob.len() { return None; }
    let manifest_len = u32::from_le_bytes([
        blob[pos], blob[pos+1], blob[pos+2], blob[pos+3]
    ]) as usize;
    pos += 4;
    if pos + manifest_len > blob.len() { return None; }
    let manifest_bytes = blob[pos..pos + manifest_len].to_vec();
    pos += manifest_len;

    // Read inline data blobs (if present)
    let inline_data = if has_inline && version >= 2 {
        if pos + 2 > blob.len() { return None; }
        let n_blobs = u16::from_le_bytes([blob[pos], blob[pos+1]]) as usize;
        pos += 2;

        let mut blobs = Vec::with_capacity(n_blobs);
        for _ in 0..n_blobs {
            if pos + 4 > blob.len() { return None; }
            let data_len = u32::from_le_bytes([
                blob[pos], blob[pos+1], blob[pos+2], blob[pos+3]
            ]) as usize;
            pos += 4;
            if pos + data_len > blob.len() { return None; }
            blobs.push(blob[pos..pos + data_len].to_vec());
            pos += data_len;
        }
        Some(blobs)
    } else {
        None
    };

    Some((commit, manifest_bytes, inline_data))
}

/// Check if a blob is a PNPK pack (by magic bytes).
pub fn is_pack(blob: &[u8]) -> bool {
    blob.len() >= 4 && &blob[0..4] == PNPK_MAGIC
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_encode_decode_pack_basic() {
        let commit = json!({
            "parent": null,
            "manifest": "abc123",
            "message": "first commit",
            "timestamp": 1234567890,
            "index": 0
        });
        let manifest = b"PMAN\x01\x00\x00test manifest data".to_vec();

        let blob = encode_pack(&commit, &manifest, None);
        assert!(is_pack(&blob));

        let (dec_commit, dec_manifest, inline) = decode_pack(&blob).unwrap();
        assert_eq!(dec_commit["message"], "first commit");
        assert_eq!(dec_commit["manifest"], "abc123");
        assert_eq!(dec_manifest, manifest);
        assert!(inline.is_none());
    }

    #[test]
    fn test_encode_decode_pack_with_inline_data() {
        let commit = json!({"message": "with inline", "index": 1});
        let manifest = b"PMAN manifest".to_vec();
        let inline = vec![b"blob1".to_vec(), b"blob2_data".to_vec()];

        let blob = encode_pack(&commit, &manifest, Some(&inline));
        let (dec_commit, dec_manifest, dec_inline) = decode_pack(&blob).unwrap();

        assert_eq!(dec_commit["message"], "with inline");
        assert_eq!(dec_manifest, manifest);
        assert!(dec_inline.is_some());
        let inline_data = dec_inline.unwrap();
        assert_eq!(inline_data.len(), 2);
        assert_eq!(inline_data[0], b"blob1");
        assert_eq!(inline_data[1], b"blob2_data");
    }

    #[test]
    fn test_decode_rejects_non_pack() {
        assert!(!is_pack(b"not a pack"));
        assert!(!is_pack(b"PMAN"));
        assert!(decode_pack(b"random bytes").is_none());
    }

    #[test]
    fn test_pack_roundtrip_preserves_all_fields() {
        let commit = json!({
            "parent": "prev_hash_abc",
            "manifest": "manifest_hash_def",
            "message": "test commit with \"quotes\"",
            "timestamp": 999999,
            "index": 42
        });
        let manifest = vec![0u8; 100]; // 100 bytes of zeros

        let blob = encode_pack(&commit, &manifest, None);
        let (dec_commit, dec_manifest, _) = decode_pack(&blob).unwrap();

        assert_eq!(dec_commit, commit);
        assert_eq!(dec_manifest, manifest);
    }
}
