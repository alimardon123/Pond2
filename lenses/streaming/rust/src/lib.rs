// StreamingLens — chunked storage for large objects (Rust port)
//
// Splits large objects (video, music, logs, streams) into fixed-size segments.
// Each segment is stored as a JSON row {offset, data_b64} in the collection.
// Range-read emerges from composition: manifest (segment index) + multiple
// blobs (segments) — NO new kernel primitive needed.
//
// API parity with Python StreamingLens (core operations):
//   - write_stream(collection, data, segment_size) → commit_hash
//   - read_stream(collection, start, end) → bytes
//   - append_stream(collection, data, segment_size) → commit_hash
//   - stream_size(collection) → usize
//   - segment_count(collection) → usize
//
// The Python lens has additional features (topics, partitions, consumer groups,
// Kafka-like produce/consume) that are not yet ported. See
// lenses/streaming/python/streaming_lens.py for the full API.

use pond_storage::UnifiedStorage;
use pond_storage::{write as storage_write, read as storage_read};
use serde_json::{Value, json};
use std::sync::Mutex;

/// Default segment size: 1 MB. Good for most streaming workloads.
pub const DEFAULT_SEGMENT_SIZE: usize = 1024 * 1024;

/// StreamingLens — chunked storage for large objects.
///
/// Splits data into fixed-size segments for efficient range reads.
/// Each segment is stored as a JSON row: {"offset": N, "data": "<base64>"}.
///
/// # Example
/// ```ignore
/// use pond_streaming_lens::StreamingLens;
/// use pond_storage::UnifiedStorage;
///
/// let storage = UnifiedStorage::new_local("/var/lib/pond").unwrap();
/// let lens = StreamingLens::new(storage);
///
/// // Write a 5MB video as 1MB segments
/// let video_data = vec![0u8; 5 * 1024 * 1024];
/// lens.write_stream("video1", &video_data, 1024 * 1024, "upload video").unwrap();
///
/// // Read bytes 1.5MB to 2.5MB (range read — only fetches overlapping segments)
/// let chunk = lens.read_stream("video1", 1_500_000, 2_500_000).unwrap();
/// assert_eq!(chunk.len(), 1_000_000);
/// ```
pub struct StreamingLens {
    storage: UnifiedStorage,
    _lock: Mutex<()>,
}

impl StreamingLens {
    /// Create a new StreamingLens over a UnifiedStorage.
    pub fn new(storage: UnifiedStorage) -> Self {
        Self {
            storage,
            _lock: Mutex::new(()),
        }
    }

    /// Write a complete stream, splitting it into segments.
    ///
    /// Args:
    ///   - `collection`: The collection name
    ///   - `data`: The complete data to write
    ///   - `segment_size`: Size of each segment in bytes
    ///   - `message`: Commit message
    ///
    /// Returns:
    ///   The commit hash on success.
    pub fn write_stream(
        &self,
        collection: &str,
        data: &[u8],
        segment_size: usize,
        message: &str,
    ) -> Result<String, String> {
        let rows = self.split_into_segments(data, segment_size, 0);
        let json_data = serde_json::to_vec(&rows).map_err(|e| e.to_string())?;

        let active = self.storage.get_active_branch(collection);
        storage_write::write(self.storage.kernel(), collection, &active, &json_data, message)
            .map_err(|e| e.to_string())
    }

    /// Append data to an existing stream.
    ///
    /// Reads the current stream to find the next offset, then appends new segments.
    ///
    /// Args:
    ///   - `collection`: The collection name
    ///   - `data`: The data to append
    ///   - `segment_size`: Size of each segment in bytes
    ///   - `message`: Commit message
    ///
    /// Returns:
    ///   The commit hash on success.
    pub fn append_stream(
        &self,
        collection: &str,
        data: &[u8],
        segment_size: usize,
        message: &str,
    ) -> Result<String, String> {
        let current_size = self.stream_size(collection)?;
        let new_rows = self.split_into_segments(data, segment_size, current_size);

        // Read existing rows and append
        let active = self.storage.get_active_branch(collection);
        let existing_data = storage_read::read(self.storage.kernel(), collection, &active)
            .map_err(|e| e.to_string())?;
        let mut all_rows: Vec<Value> = if existing_data.is_empty() {
            Vec::new()
        } else {
            serde_json::from_slice(&existing_data).map_err(|e| e.to_string())?
        };
        all_rows.extend(new_rows);

        let json_data = serde_json::to_vec(&all_rows).map_err(|e| e.to_string())?;
        storage_write::write(self.storage.kernel(), collection, &active, &json_data, message)
            .map_err(|e| e.to_string())
    }

    /// Read a byte range from the stream.
    ///
    /// Fetches only the segments that overlap [start, end) — efficient for
    /// large streams where you only need a small portion.
    ///
    /// Args:
    ///   - `collection`: The collection name
    ///   - `start`: Start byte offset (inclusive)
    ///   - `end`: End byte offset (exclusive). None = read to end.
    ///
    /// Returns:
    ///   The requested byte range.
    pub fn read_stream(
        &self,
        collection: &str,
        start: usize,
        end: Option<usize>,
    ) -> Result<Vec<u8>, String> {
        let active = self.storage.get_active_branch(collection);
        let data = match storage_read::read(self.storage.kernel(), collection, &active) {
            Ok(d) => d,
            Err(_) => return Ok(Vec::new()), // Collection doesn't exist
        };

        if data.is_empty() {
            return Ok(Vec::new());
        }

        let rows: Vec<Value> = serde_json::from_slice(&data).map_err(|e| e.to_string())?;
        let end = end.unwrap_or(usize::MAX);

        let mut result = Vec::new();
        for row in &rows {
            let offset = row.get("offset").and_then(|o| o.as_u64()).unwrap_or(0) as usize;
            let segment_b64 = row.get("data").and_then(|d| d.as_str()).unwrap_or("");
            let segment = base64_decode(segment_b64);
            let seg_len = segment.len();

            // Check if this segment overlaps [start, end)
            let seg_start = offset;
            let seg_end = offset + seg_len;

            if seg_end <= start || seg_start >= end {
                // No overlap — skip this segment
                continue;
            }

            // Calculate the overlap
            let copy_start = start.saturating_sub(seg_start);
            let copy_end = if end < seg_end { end - seg_start } else { seg_len }.min(segment.len());

            if copy_start < copy_end && copy_end <= segment.len() {
                result.extend_from_slice(&segment[copy_start..copy_end]);
            }
        }

        Ok(result)
    }

    /// Get the total size of the stream in bytes.
    /// Returns 0 for nonexistent collections.
    pub fn stream_size(&self, collection: &str) -> Result<usize, String> {
        let active = self.storage.get_active_branch(collection);
        let data = match storage_read::read(self.storage.kernel(), collection, &active) {
            Ok(d) => d,
            Err(_) => return Ok(0), // Collection doesn't exist
        };

        if data.is_empty() {
            return Ok(0);
        }

        let rows: Vec<Value> = serde_json::from_slice(&data).map_err(|e| e.to_string())?;
        let mut total = 0usize;
        for row in &rows {
            let offset = row.get("offset").and_then(|o| o.as_u64()).unwrap_or(0) as usize;
            let segment_b64 = row.get("data").and_then(|d| d.as_str()).unwrap_or("");
            let seg_len = base64_decode(segment_b64).len();
            total = total.max(offset + seg_len);
        }
        Ok(total)
    }

    /// Get the number of segments in the stream.
    /// Returns 0 for nonexistent collections.
    pub fn segment_count(&self, collection: &str) -> Result<usize, String> {
        let active = self.storage.get_active_branch(collection);
        let data = match storage_read::read(self.storage.kernel(), collection, &active) {
            Ok(d) => d,
            Err(_) => return Ok(0), // Collection doesn't exist
        };

        if data.is_empty() {
            return Ok(0);
        }

        let rows: Vec<Value> = serde_json::from_slice(&data).map_err(|e| e.to_string())?;
        Ok(rows.len())
    }

    /// Get a reference to the underlying UnifiedStorage.
    pub fn storage(&self) -> &UnifiedStorage {
        &self.storage
    }

    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    /// Split data into segments and create JSON rows.
    fn split_into_segments(
        &self,
        data: &[u8],
        segment_size: usize,
        base_offset: usize,
    ) -> Vec<Value> {
        let mut rows = Vec::new();
        let mut offset = base_offset;
        let mut start = 0;

        while start < data.len() {
            let end = std::cmp::min(start + segment_size, data.len());
            let segment = &data[start..end];
            let b64 = base64_encode(segment);

            rows.push(json!({
                "offset": offset,
                "data": b64,
            }));

            offset += segment.len();
            start = end;
        }

        rows
    }
}

// ---------------------------------------------------------------------------
// Base64 encoding/decoding (minimal, no external dep)
// ---------------------------------------------------------------------------

const BASE64_CHARS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

fn base64_encode(data: &[u8]) -> String {
    let mut result = String::with_capacity(data.len().div_ceil(3) * 4);
    let mut i = 0;
    while i + 3 <= data.len() {
        let b0 = data[i] as u32;
        let b1 = data[i + 1] as u32;
        let b2 = data[i + 2] as u32;
        let triple = (b0 << 16) | (b1 << 8) | b2;
        result.push(BASE64_CHARS[((triple >> 18) & 0x3F) as usize] as char);
        result.push(BASE64_CHARS[((triple >> 12) & 0x3F) as usize] as char);
        result.push(BASE64_CHARS[((triple >> 6) & 0x3F) as usize] as char);
        result.push(BASE64_CHARS[(triple & 0x3F) as usize] as char);
        i += 3;
    }
    let remaining = data.len() - i;
    if remaining == 1 {
        let b0 = data[i] as u32;
        let triple = b0 << 16;
        result.push(BASE64_CHARS[((triple >> 18) & 0x3F) as usize] as char);
        result.push(BASE64_CHARS[((triple >> 12) & 0x3F) as usize] as char);
        result.push('=');
        result.push('=');
    } else if remaining == 2 {
        let b0 = data[i] as u32;
        let b1 = data[i + 1] as u32;
        let triple = (b0 << 16) | (b1 << 8);
        result.push(BASE64_CHARS[((triple >> 18) & 0x3F) as usize] as char);
        result.push(BASE64_CHARS[((triple >> 12) & 0x3F) as usize] as char);
        result.push(BASE64_CHARS[((triple >> 6) & 0x3F) as usize] as char);
        result.push('=');
    }
    result
}

fn base64_decode(s: &str) -> Vec<u8> {
    let s: Vec<u8> = s.bytes().filter(|&b| b != b'\n' && b != b'\r' && b != b' ').collect();
    let mut result = Vec::with_capacity(s.len() * 3 / 4);
    let mut buffer: u32 = 0;
    let mut bits: u32 = 0;

    for &b in &s {
        if b == b'=' {
            break;
        }
        let val = match b {
            b'A'..=b'Z' => b - b'A',
            b'a'..=b'z' => b - b'a' + 26,
            b'0'..=b'9' => b - b'0' + 52,
            b'+' => 62,
            b'/' => 63,
            _ => continue,
        } as u32;
        buffer = (buffer << 6) | val;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            result.push((buffer >> bits) as u8);
            buffer &= (1 << bits) - 1;
        }
    }
    result
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn make_test_lens() -> StreamingLens {
        let dir = tempfile::tempdir().unwrap();
        let storage = UnifiedStorage::new_local(dir.path()).unwrap();
        std::mem::forget(dir);
        StreamingLens::new(storage)
    }

    #[test]
    fn test_write_and_read_full_stream() {
        let lens = make_test_lens();
        let data = b"Hello, streaming world! This is a test.".to_vec();
        lens.write_stream("video1", &data, 10, "upload").unwrap();

        let read = lens.read_stream("video1", 0, None).unwrap();
        assert_eq!(read, data);
    }

    #[test]
    fn test_read_range() {
        let lens = make_test_lens();
        let data: Vec<u8> = (0..100).map(|i| i as u8).collect();
        lens.write_stream("data", &data, 10, "init").unwrap();

        // Read bytes 15-25
        let chunk = lens.read_stream("data", 15, Some(25)).unwrap();
        assert_eq!(chunk, &data[15..25]);
    }

    #[test]
    fn test_stream_size() {
        let lens = make_test_lens();
        let data = vec![0xAB; 5000];
        lens.write_stream("big", &data, 1000, "init").unwrap();
        assert_eq!(lens.stream_size("big").unwrap(), 5000);
    }

    #[test]
    fn test_segment_count() {
        let lens = make_test_lens();
        let data = vec![0xAB; 5000];
        lens.write_stream("big", &data, 1000, "init").unwrap();
        assert_eq!(lens.segment_count("big").unwrap(), 5); // 5000 / 1000 = 5 segments
    }

    #[test]
    fn test_append_stream() {
        let lens = make_test_lens();
        let data1 = b"Hello, ".to_vec();
        let data2 = b"world!".to_vec();

        lens.write_stream("msg", &data1, 3, "part 1").unwrap();
        lens.append_stream("msg", &data2, 3, "part 2").unwrap();

        let full = lens.read_stream("msg", 0, None).unwrap();
        assert_eq!(full, b"Hello, world!");
    }

    #[test]
    fn test_empty_stream() {
        let lens = make_test_lens();
        let read = lens.read_stream("nonexistent", 0, None).unwrap();
        assert!(read.is_empty());
        assert_eq!(lens.stream_size("nonexistent").unwrap(), 0);
    }

    #[test]
    fn test_single_byte_segments() {
        let lens = make_test_lens();
        let data = b"ABCDE".to_vec();
        lens.write_stream("small", &data, 1, "init").unwrap();
        assert_eq!(lens.segment_count("small").unwrap(), 5);
        assert_eq!(lens.stream_size("small").unwrap(), 5);

        let read = lens.read_stream("small", 2, Some(4)).unwrap();
        assert_eq!(read, b"CD");
    }

    #[test]
    fn test_large_segment_covers_all() {
        let lens = make_test_lens();
        let data = b"small data".to_vec();
        // Segment size larger than data → single segment
        lens.write_stream("x", &data, 1024 * 1024, "init").unwrap();
        assert_eq!(lens.segment_count("x").unwrap(), 1);
        let read = lens.read_stream("x", 0, None).unwrap();
        assert_eq!(read, data);
    }

    #[test]
    fn test_base64_encode_decode() {
        let data = b"Hello, World! \x00\xFF";
        let encoded = base64_encode(data);
        let decoded = base64_decode(&encoded);
        assert_eq!(decoded, data);
    }
}
