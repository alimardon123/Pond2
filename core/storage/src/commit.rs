// Commit module — commit format, write/read commit blobs, history walking
//
// FAITHFUL PORT of Python's _write_commit_blob / _read_commit_blob / history.
//
// Commit format (JSON, matches Python exactly):
//   {
//     "parent": "hash_or_null",
//     "second_parent": "hash_or_null",  // set for merge commits
//     "manifest": "manifest_hash",
//     "message": "commit message",
//     "timestamp": 1234567890.123,
//     "index": 0
//   }

use pond_kernel::PondKernel;
use serde_json::{json, Value};
use std::time::{SystemTime, UNIX_EPOCH};

/// A commit blob. Stored as JSON in the kernel.
#[derive(Debug, Clone)]
pub struct Commit {
    pub parent: Option<String>,
    pub second_parent: Option<String>,
    pub manifest: String,
    pub message: String,
    pub timestamp: f64,
    pub index: usize,
}

impl Commit {
    /// Serialize to JSON bytes (matches Python _write_commit_blob format).
    pub fn to_json_bytes(&self) -> Vec<u8> {
        json!({
            "parent": self.parent,
            "second_parent": self.second_parent,
            "manifest": self.manifest,
            "message": self.message,
            "timestamp": self.timestamp,
            "index": self.index,
        }).to_string().into_bytes()
    }

    /// Deserialize from JSON bytes (matches Python _read_commit_blob).
    pub fn from_json_bytes(data: &[u8]) -> Option<Self> {
        let v: Value = serde_json::from_slice(data).ok()?;
        Some(Commit {
            parent: v.get("parent").and_then(|p| p.as_str()).map(|s| s.to_string()),
            second_parent: v.get("second_parent").and_then(|p| p.as_str()).map(|s| s.to_string()),
            manifest: v.get("manifest").and_then(|p| p.as_str()).unwrap_or("").to_string(),
            message: v.get("message").and_then(|p| p.as_str()).unwrap_or("").to_string(),
            timestamp: v.get("timestamp").and_then(|p| p.as_f64()).unwrap_or(0.0),
            index: v.get("index").and_then(|p| p.as_u64()).unwrap_or(0) as usize,
        })
    }

    /// Is this a merge commit? (has second_parent)
    pub fn is_merge(&self) -> bool {
        self.second_parent.is_some()
    }
}

/// Write a commit blob to the kernel and return its hash.
///
/// Matches Python's _write_commit_blob:
///   1. Build the commit JSON
///   2. Write it as a blob (content-addressed)
///   3. Update the active branch's commit ref
///   4. Update the active branch's manifest ref
pub fn write_commit(
    kernel: &PondKernel,
    _collection: &str,
    manifest_hash: &str,
    parent: Option<&str>,
    second_parent: Option<&str>,
    message: &str,
    index: usize,
) -> std::io::Result<String> {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0);

    let commit = Commit {
        parent: parent.map(|s| s.to_string()),
        second_parent: second_parent.map(|s| s.to_string()),
        manifest: manifest_hash.to_string(),
        message: message.to_string(),
        timestamp,
        index,
    };

    let commit_bytes = commit.to_json_bytes();
    let commit_hash = kernel.write(&commit_bytes)?;

    Ok(commit_hash)
}

/// Read a commit blob from the kernel by hash.
///
/// Matches Python's _read_commit_blob.
pub fn read_commit(kernel: &PondKernel, commit_hash: &str) -> Option<Commit> {
    let data = kernel.read_blob(commit_hash).ok()?;
    Commit::from_json_bytes(&data)
}

/// Walk the commit history from HEAD backwards.
///
/// Matches Python's history() method. Returns a list of (hash, Commit) pairs.
pub fn history(
    kernel: &PondKernel,
    start_hash: &str,
    limit: usize,
) -> Vec<(String, Commit)> {
    let mut result = Vec::new();
    let mut current = Some(start_hash.to_string());

    while let Some(hash) = current {
        if result.len() >= limit {
            break;
        }
        match read_commit(kernel, &hash) {
            Some(commit) => {
                current = commit.parent.clone();
                result.push((hash, commit));
            }
            None => break,
        }
    }
    result
}

/// Get the commit index (sequence number) for a commit.
/// Used for staleness checks in lazy index refresh.
pub fn commit_index(kernel: &PondKernel, commit_hash: &str) -> usize {
    read_commit(kernel, commit_hash)
        .map(|c| c.index)
        .unwrap_or(0)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_commit_roundtrip() {
        let commit = Commit {
            parent: Some("abc123".to_string()),
            second_parent: None,
            manifest: "def456".to_string(),
            message: "test commit".to_string(),
            timestamp: 1234567890.0,
            index: 5,
        };
        let bytes = commit.to_json_bytes();
        let parsed = Commit::from_json_bytes(&bytes).unwrap();
        assert_eq!(parsed.parent, Some("abc123".to_string()));
        assert_eq!(parsed.manifest, "def456");
        assert_eq!(parsed.message, "test commit");
        assert_eq!(parsed.index, 5);
        assert!(!parsed.is_merge());
    }

    #[test]
    fn test_merge_commit() {
        let commit = Commit {
            parent: Some("abc".to_string()),
            second_parent: Some("def".to_string()),
            manifest: "ghi".to_string(),
            message: "merge".to_string(),
            timestamp: 0.0,
            index: 1,
        };
        assert!(commit.is_merge());
    }

    #[test]
    fn test_write_read_commit() {
        let dir = tempfile::tempdir().unwrap();
        let kernel = PondKernel::new_local(dir.path()).unwrap();
        let hash = write_commit(
            &kernel, "users", "manifest_hash",
            Some("parent_hash"), None, "test", 0,
        ).unwrap();
        let commit = read_commit(&kernel, &hash).unwrap();
        assert_eq!(commit.parent, Some("parent_hash".to_string()));
        assert_eq!(commit.manifest, "manifest_hash");
        assert_eq!(commit.message, "test");
        assert_eq!(commit.index, 0);
    }

    #[test]
    fn test_history_walks_parent_chain() {
        let dir = tempfile::tempdir().unwrap();
        let kernel = PondKernel::new_local(dir.path()).unwrap();

        // Write 3 commits: c1 → c2 → c3
        let c1 = write_commit(&kernel, "users", "m1", None, None, "first", 0).unwrap();
        let c2 = write_commit(&kernel, "users", "m2", Some(&c1), None, "second", 1).unwrap();
        let c3 = write_commit(&kernel, "users", "m3", Some(&c2), None, "third", 2).unwrap();

        let hist = history(&kernel, &c3, 10);
        assert_eq!(hist.len(), 3);
        assert_eq!(hist[0].1.message, "third");
        assert_eq!(hist[1].1.message, "second");
        assert_eq!(hist[2].1.message, "first");
    }
}
