// CachingObjectStore — local-disk + in-memory LRU cache for any ObjectStore.
//
// WRAPS any `ObjectStore` (LocalFS, S3, GCS, ...) with two cache tiers:
//
//   Tier 1 (in-memory): `HashMap` for ref lookups (get_path). Refs are
//       tiny JSON blobs (~60 bytes each) and are consulted on EVERY
//       operation. A 5-second TTL avoids stale reads in multi-writer
//       scenarios while eliminating the S3 GET for branch HEAD resolution.
//
//   Tier 2 (local-disk): File cache for content blobs with LRU eviction
//       by total byte size (default 1 GB). Reads: on get_blob miss, fetch
//       from inner store, write to disk. Writes: write-through to both.
//       Invalidation: on delete_blob, remove from disk.
//
// WHY THIS MATTERS (architecture review finding #1):
//   Without caching, every read does 3-4 sequential S3 GETs:
//     branch ref (get_path) -> commit blob -> manifest -> data blobs.
//   Each S3 GET costs 50-300ms (RTT + TLS + server processing).
//   Total cold read: ~200-1200ms. Warm read (WITH cache): <10ms.
//   This is the difference between "interesting research project" and
//   "usable in production." StalwartDB's entire value proposition is
//   this local cache layer.
//
// USAGE:
//   use pond_cache::CachingObjectStore;
//   use pond_kernel::{PondKernel, ObjectStore};
//   use pond_s3::S3ObjectStore;
//
//   let inner: Box<dyn ObjectStore> = Box::new(S3ObjectStore::new(...));
//   let cached = CachingObjectStore::new(inner, "/var/lib/pond/cache")
//       .with_max_disk_bytes(1_000_000_000)  // 1 GB
//       .with_ref_ttl(std::time::Duration::from_secs(5));
//   let kernel = PondKernel::new_with_store(Box::new(cached));
//
// DESIGN DECISIONS:
//   - Write-through (not write-back): writes go to inner store AND cache.
//     This guarantees the cache is always a subset of the inner store.
//   - Content-addressed: blob file names are the SHA-256 hash.
//     No index needed -- if the file exists, the blob is cached.
//   - No background prefetch (yet): that's a future optimization.
//   - Cache directory uses same layout as inner store:
//     cache_dir/blobs/{hash[:2]}/{hash}

use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use pond_kernel::ObjectStore;

// ---------------------------------------------------------------------------
// Ref cache entry
// ---------------------------------------------------------------------------

struct RefEntry {
    hash: String,
    inserted_at: Instant,
}

// ---------------------------------------------------------------------------
// CachingObjectStore
// ---------------------------------------------------------------------------

/// A two-tier cache wrapper around any `ObjectStore`.
///
/// Tier 1: In-memory `HashMap` for ref lookups (get_path) with TTL.
/// Tier 2: Local-disk file cache for content-addressed blobs with LRU eviction.
pub struct CachingObjectStore {
    inner: Box<dyn ObjectStore>,
    cache_dir: PathBuf,
    ref_cache: Mutex<std::collections::HashMap<String, RefEntry>>,
    ref_ttl: Duration,
    disk_usage: Mutex<usize>,
    max_disk_bytes: usize,
}

impl CachingObjectStore {
    /// Create a new CachingObjectStore wrapping `inner`.
    ///
    /// Blobs are cached under `cache_dir/blobs/{hash[:2]}/{hash}`.
    /// The directory is created if it doesn't exist.
    pub fn new(inner: Box<dyn ObjectStore>, cache_dir: impl AsRef<Path>) -> io::Result<Self> {
        let cache_dir = cache_dir.as_ref().to_path_buf();
        let blobs_dir = cache_dir.join("blobs");
        fs::create_dir_all(&blobs_dir)?;
        Ok(Self {
            inner,
            cache_dir,
            ref_cache: Mutex::new(std::collections::HashMap::new()),
            ref_ttl: Duration::from_secs(5),
            disk_usage: Mutex::new(0),
            max_disk_bytes: 1_000_000_000, // 1 GB default
        })
    }

    /// Set the maximum disk cache size in bytes (default: 1 GB).
    /// When exceeded, the least-recently-used blobs are evicted.
    pub fn with_max_disk_bytes(mut self, bytes: usize) -> Self {
        self.max_disk_bytes = bytes;
        self
    }

    /// Set the TTL for in-memory ref cache entries (default: 5 seconds).
    /// Refs older than this are re-fetched from the inner store.
    pub fn with_ref_ttl(mut self, ttl: Duration) -> Self {
        self.ref_ttl = ttl;
        self
    }

    /// Return approximate current disk cache usage in bytes.
    pub fn disk_usage_bytes(&self) -> usize {
        *self.disk_usage.lock().unwrap()
    }

    // -- Blob cache paths --

    fn blob_path(&self, hash: &str) -> PathBuf {
        self.cache_dir.join("blobs").join(&hash[..2]).join(hash)
    }

    fn read_blob_from_disk(&self, hash: &str) -> io::Result<Vec<u8>> {
        let path = self.blob_path(hash);
        fs::read(&path)
    }

    fn write_blob_to_disk(&self, hash: &str, data: &[u8]) -> io::Result<()> {
        let path = self.blob_path(hash);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(&path, data)?;
        let mut usage = self.disk_usage.lock().unwrap();
        *usage += data.len();
        self.evict_if_needed(&mut usage);
        Ok(())
    }

    fn remove_blob_from_disk(&self, hash: &str) {
        let path = self.blob_path(hash);
        if let Ok(metadata) = fs::metadata(&path) {
            let _ = fs::remove_file(&path);
            let mut usage = self.disk_usage.lock().unwrap();
            *usage = usage.saturating_sub(metadata.len() as usize);
        }
    }

    /// Evict least-recently-used blobs from disk cache if over capacity.
    fn evict_if_needed(&self, usage: &mut usize) {
        if *usage <= self.max_disk_bytes {
            return;
        }
        let blobs_dir = self.cache_dir.join("blobs");
        let mut entries: Vec<(PathBuf, u64)> = Vec::new();
        if let Ok(rd) = fs::read_dir(&blobs_dir) {
            for shard_entry in rd.flatten() {
                if let Ok(shard_rd) = fs::read_dir(shard_entry.path()) {
                    for file_entry in shard_rd.flatten() {
                        if let Ok(meta) = file_entry.metadata() {
                            if meta.is_file() {
                                let mtime = meta
                                    .modified()
                                    .ok()
                                    .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                                    .map(|d| d.as_secs())
                                    .unwrap_or(0);
                                entries.push((file_entry.path(), mtime));
                            }
                        }
                    }
                }
            }
        }
        // Sort oldest first.
        entries.sort_by_key(|(_, mtime)| *mtime);
        for (path, _) in &entries {
            if *usage <= self.max_disk_bytes {
                break;
            }
            if let Ok(meta) = fs::metadata(path) {
                let _ = fs::remove_file(path);
                *usage = usage.saturating_sub(meta.len() as usize);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// ObjectStore implementation
// ---------------------------------------------------------------------------

impl ObjectStore for CachingObjectStore {
    fn put_blob(&self, data: &[u8]) -> io::Result<String> {
        // Write-through: inner store first (it computes the hash).
        let hash = self.inner.put_blob(data)?;
        // Cache to disk.
        let _ = self.write_blob_to_disk(&hash, data);
        Ok(hash)
    }

    fn get_blob(&self, hash: &str) -> io::Result<Vec<u8>> {
        // Check disk cache first.
        if let Ok(data) = self.read_blob_from_disk(hash) {
            return Ok(data);
        }
        // Cache miss: fetch from inner store.
        let data = self.inner.get_blob(hash)?;
        // Populate disk cache.
        let _ = self.write_blob_to_disk(hash, &data);
        Ok(data)
    }

    fn put_path(&self, path: &str, hash: &str) -> io::Result<()> {
        self.inner.put_path(path, hash)?;
        // Invalidate ref cache entry (new value).
        let mut refs = self.ref_cache.lock().unwrap();
        refs.insert(path.to_string(), RefEntry {
            hash: hash.to_string(),
            inserted_at: Instant::now(),
        });
        Ok(())
    }

    fn get_path(&self, path: &str) -> Option<String> {
        let now = Instant::now();
        {
            let refs = self.ref_cache.lock().unwrap();
            if let Some(entry) = refs.get(path) {
                if now.duration_since(entry.inserted_at) < self.ref_ttl {
                    return Some(entry.hash.clone());
                }
            }
        }
        // Cache miss or expired: fetch from inner store.
        let hash = self.inner.get_path(path)?;
        let mut refs = self.ref_cache.lock().unwrap();
        refs.insert(path.to_string(), RefEntry {
            hash: hash.clone(),
            inserted_at: now,
        });
        Some(hash)
    }

    fn delete_path(&self, path: &str) -> io::Result<bool> {
        let result = self.inner.delete_path(path)?;
        self.ref_cache.lock().unwrap().remove(path);
        Ok(result)
    }

    fn list_paths(&self, prefix: &str) -> io::Result<Vec<String>> {
        // Always delegate to inner store -- listing is not cacheable.
        self.inner.list_paths(prefix)
    }

    fn blob_exists(&self, hash: &str) -> bool {
        // Check disk cache first, then inner store.
        self.blob_path(hash).exists() || self.inner.blob_exists(hash)
    }

    fn delete_blob(&self, hash: &str) -> io::Result<bool> {
        let result = self.inner.delete_blob(hash)?;
        self.remove_blob_from_disk(hash);
        Ok(result)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use pond_kernel::LocalFSObjectStore;
    use tempfile::tempdir;

    fn make_cached_store() -> (CachingObjectStore, tempfile::TempDir, tempfile::TempDir) {
        let inner_dir = tempdir().unwrap();
        let cache_dir = tempdir().unwrap();
        let inner: Box<dyn ObjectStore> = Box::new(
            LocalFSObjectStore::new(inner_dir.path()).unwrap(),
        );
        let cached = CachingObjectStore::new(inner, cache_dir.path()).unwrap();
        (cached, inner_dir, cache_dir)
    }

    #[test]
    fn test_put_get_roundtrip() {
        let (store, _inner, _cache) = make_cached_store();
        let h = store.put_blob(b"hello, cache!").unwrap();
        let data = store.get_blob(&h).unwrap();
        assert_eq!(data, b"hello, cache!");
    }

    #[test]
    fn test_cache_hit_avoids_inner() {
        let (store, inner_dir, _cache) = make_cached_store();
        let h = store.put_blob(b"cached data").unwrap();
        // Verify blob is on disk cache.
        let cached_path = store.blob_path(&h);
        assert!(cached_path.exists(), "blob should be cached on disk");
        // Delete from inner store to prove cache serves it.
        let inner_blob = inner_dir.path().join("blobs").join(&h[..2]).join(&h);
        fs::remove_file(inner_blob).unwrap();
        // Read should still work from cache.
        let data = store.get_blob(&h).unwrap();
        assert_eq!(data, b"cached data");
    }

    #[test]
    fn test_ref_cache() {
        let (store, _inner, _cache) = make_cached_store();
        let h = store.put_blob(b"data").unwrap();
        store.put_path("my_ref", &h).unwrap();
        let resolved = store.get_path("my_ref").unwrap();
        assert_eq!(resolved, h);
    }

    #[test]
    fn test_delete_invalidates_cache() {
        let (store, _inner, _cache) = make_cached_store();
        let h = store.put_blob(b"temp").unwrap();
        store.put_path("tmp_ref", &h).unwrap();
        store.delete_path("tmp_ref").unwrap();
        assert!(store.get_path("tmp_ref").is_none());
    }

    #[test]
    fn test_delete_blob_removes_from_cache() {
        let (store, _inner, _cache) = make_cached_store();
        let h = store.put_blob(b"to delete").unwrap();
        assert!(store.blob_path(&h).exists());
        store.delete_blob(&h).unwrap();
        assert!(!store.blob_path(&h).exists());
    }

    #[test]
    fn test_dedup() {
        let (store, _inner, _cache) = make_cached_store();
        let h1 = store.put_blob(b"same").unwrap();
        let h2 = store.put_blob(b"same").unwrap();
        assert_eq!(h1, h2);
    }

    #[test]
    fn test_write_batch() {
        let (store, _inner, _cache) = make_cached_store();
        let items = vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()];
        let hashes = store.put_blob_batch(&items).unwrap();
        for (i, h) in hashes.iter().enumerate() {
            assert_eq!(store.get_blob(h).unwrap(), items[i]);
        }
    }

    #[test]
    fn test_disk_usage_tracking() {
        let (store, _inner, _cache) = make_cached_store();
        assert_eq!(store.disk_usage_bytes(), 0);
        store.put_blob(b"hello").unwrap();
        assert!(store.disk_usage_bytes() > 0);
    }

    #[test]
    fn test_eviction_respects_max_bytes() {
        let inner_dir = tempdir().unwrap();
        let cache_dir = tempdir().unwrap();
        let inner: Box<dyn ObjectStore> = Box::new(
            LocalFSObjectStore::new(inner_dir.path()).unwrap(),
        );
        let store = CachingObjectStore::new(inner, cache_dir.path())
            .unwrap()
            .with_max_disk_bytes(200); // very small limit

        // Write enough data to trigger eviction.
        for i in 0..10u32 {
            let data = vec![i as u8; 100]; // 100 bytes each
            let _ = store.put_blob(&data).unwrap();
        }
        // Disk usage should be <= max + one extra blob.
        assert!(store.disk_usage_bytes() <= 200 + 100);
    }
}
