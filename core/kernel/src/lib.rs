// Pond Storage Kernel — the 3 primitives (Write, Read, Ref) in pure Rust
//
// ARCHITECTURE:
//   ObjectStore trait (put_blob, get_blob, put_path, get_path, ...)
//       ↓ implemented by
//   LocalFSObjectStore  ←→  S3ObjectStore (future)  ←→  GCSObjectStore (future)
//       ↓ used by
//   PondKernel (Write, Read, Ref — the 3 primitives)
//
// PATH LAYOUT (same on ALL backends — local FS, S3, GCS):
//   blobs/{hash[:2]}/{hash}                          — content-addressed blobs
//   collections/{name}/_branches/{branch}/commit     — branch commit refs
//   collections/{name}/_branches/{branch}/shards/...  — CRDT shards
//   collections/{name}/_active_branch                 — active branch name
//   collections/{name}/definition                     — collection schema
//   transactions/{tx_id}                              — transaction markers
//
// This layout works identically on local FS and S3 — migrating is a
// straight `aws s3 sync` or `rsync`. No backend-specific path logic.

pub mod crdt;
pub mod object_store;
pub mod c_abi;

pub use object_store::{ObjectStore, LocalFSObjectStore, StoreStats};
#[cfg(feature = "async")]
pub use object_store::AsyncObjectStore;

use std::io;
use std::sync::{Arc, Mutex};

use sha2::{Digest, Sha256};

// ---------------------------------------------------------------------------
// Hashing
// ---------------------------------------------------------------------------

/// Compute the SHA-256 hash of a byte slice, returned as a lowercase
/// hex string. This is the canonical content-address for Pond blobs.
pub fn hash_bytes(data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data);
    let result = hasher.finalize();
    let mut hex = String::with_capacity(64);
    for byte in result.iter() {
        hex.push_str(&format!("{:02x}", byte));
    }
    hex
}

// ---------------------------------------------------------------------------
// PondKernel — the 3 primitives (uses ObjectStore trait)
// ---------------------------------------------------------------------------

/// The storage kernel. Owns an ObjectStore (local FS now, S3 later).
///
/// The kernel is the ONLY stateful component. Everything above it
/// (lenses, UnifiedStorage, manifests, commits) is a pattern over
/// these 3 primitives:
///   Write(bytes) → hash     — content-addressed immutable blob
///   Read(hash_or_name) → bytes — read by hash or by name
///   Ref(name, hash)         — mutable name → hash mapping
pub struct PondKernel {
    // `Arc` (not `Box`) so async methods can clone the store into a
    // `spawn_blocking` closure without owning the kernel. The public API
    // (`new_with_store`) still accepts `Box<dyn ObjectStore>` for back-compat.
    store: Arc<dyn ObjectStore>,
    stats: Mutex<KernelStats>,
}

impl Clone for PondKernel {
    fn clone(&self) -> Self {
        Self {
            store: Arc::clone(&self.store),
            stats: Mutex::new(self.stats.lock().unwrap().clone()),
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct KernelStats {
    pub writes: u64,
    pub reads: u64,
    pub references: u64,
}

impl PondKernel {
    /// Create a kernel with a local FS backend.
    pub fn new_local(base_dir: impl AsRef<std::path::Path>) -> io::Result<Self> {
        let store = LocalFSObjectStore::new(base_dir)?;
        Ok(Self {
            store: Arc::new(store),
            stats: Mutex::new(KernelStats::default()),
        })
    }

    /// Create a kernel with a custom ObjectStore (for S3, GCS, etc.).
    pub fn new_with_store(store: Box<dyn ObjectStore>) -> Self {
        Self {
            store: Arc::from(store),
            stats: Mutex::new(KernelStats::default()),
        }
    }

    /// Create a kernel from an ALREADY-SHARED store handle.
    ///
    /// For callers that hold `Arc<dyn ObjectStore>` and also want a
    /// `PondKernel` over the SAME store instance (e.g. the pyo3
    /// `pond.ObjectStore` class shares its handle with Storage kernels):
    /// both keep talking to one store — one set of connection pools,
    /// one journal writer registry slot (`store_id` is the same).
    pub fn new_with_arc(store: Arc<dyn ObjectStore>) -> Self {
        Self {
            store,
            stats: Mutex::new(KernelStats::default()),
        }
    }

    // ------------------------------------------------------------------
    // Primitive 1: Write
    // ------------------------------------------------------------------

    pub fn write(&self, data: &[u8]) -> io::Result<String> {
        let h = self.store.put_blob(data)?;
        self.stats.lock().unwrap().writes += 1;
        Ok(h)
    }

    pub fn write_batch(&self, items: &[Vec<u8>]) -> io::Result<Vec<String>> {
        let hashes = self.store.put_blob_batch(items)?;
        self.stats.lock().unwrap().writes += hashes.len() as u64;
        Ok(hashes)
    }

    // ------------------------------------------------------------------
    // Primitive 2: Read
    // ------------------------------------------------------------------

    pub fn read(&self, hash_or_name: &str) -> io::Result<Vec<u8>> {
        if is_hash(hash_or_name) {
            return self.read_blob(hash_or_name);
        }
        let h = self.resolve(hash_or_name)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound,
                format!("Name '{}' not found", hash_or_name)))?;
        self.read_blob(&h)
    }

    pub fn read_blob(&self, h: &str) -> io::Result<Vec<u8>> {
        let data = self.store.get_blob(h)?;
        self.stats.lock().unwrap().reads += 1;
        Ok(data)
    }

    pub fn read_blob_batch(&self, hashes: &[String]) -> io::Result<Vec<Vec<u8>>> {
        let results = self.store.get_blob_batch(hashes)?;
        self.stats.lock().unwrap().reads += results.len() as u64;
        Ok(results)
    }

    /// Read a byte range `[start, end)` from a content-addressed blob.
    ///
    /// Thin wrapper over `ObjectStore::get_blob_range` — delegates to the
    /// backend's native range support (LocalFS seek+read, S3 `Range:`
    /// header, CachingObjectStore disk-then-inner). This is the primitive
    /// that PondSlab readers use to fetch the 12-byte tail, the footer,
    /// and individual row-group byte ranges without fetching the whole slab.
    ///
    /// **Half-open interval**: `end` is exclusive. `end == 0` or
    /// `start >= end` returns an empty Vec without an I/O round-trip.
    pub fn read_blob_range(&self, h: &str, start: u64, end: u64) -> io::Result<Vec<u8>> {
        let data = self.store.get_blob_range(h, start, end)?;
        self.stats.lock().unwrap().reads += 1;
        Ok(data)
    }

    /// Read the last `n` bytes of a content-addressed blob.
    ///
    /// Delegates to `ObjectStore::get_blob_suffix` — S3 uses `Range: bytes=-N`,
    /// LocalFS uses `SeekFrom::End(-N)`. Single RTT regardless of blob size.
    pub fn read_blob_suffix(&self, h: &str, n: u64) -> io::Result<Vec<u8>> {
        let data = self.store.get_blob_suffix(h, n)?;
        self.stats.lock().unwrap().reads += 1;
        Ok(data)
    }

    // ------------------------------------------------------------------
    // Primitive 3: Ref (mutable name → hash mapping)
    // ------------------------------------------------------------------

    pub fn reference(&self, name: &str, h: &str) -> io::Result<()> {
        // Content-addressed store: the caller always has the hash from a
        // preceding write() call, so the blob is guaranteed to exist.
        // Skipping the blob_exists() HEAD request saves one S3 round-trip
        // per ref (3 refs per write = 3 saved round-trips on S3/R2).
        // On local FS the cost was negligible; on S3 it is 20-50ms per HEAD.
        self.store.put_path(name, h)?;
        self.stats.lock().unwrap().references += 1;
        Ok(())
    }

    /// CAS (Compare-And-Swap) a named reference.
    ///
    /// Sets `name` to `new_hash` ONLY if the current value is `expected_hash`.
    /// Returns `Ok(true)` on success, `Ok(false)` if stale (caller should retry).
    ///
    /// This enables safe multi-writer concurrency: two writers that both
    /// read HEAD=H1 can use CAS to avoid clobbering each other.
    /// The second writer gets `Ok(false)`, re-reads, and retries.
    ///
    /// For `expected_hash = None`: creates the ref only if it doesn't exist.
    pub fn reference_if(&self, name: &str, expected_hash: Option<&str>, new_hash: &str) -> io::Result<bool> {
        let ok = self.store.put_path_if(name, expected_hash, new_hash)?;
        if ok {
            self.stats.lock().unwrap().references += 1;
        }
        Ok(ok)
    }

    pub fn resolve(&self, name: &str) -> Option<String> {
        self.store.get_path(name)
    }

    pub fn list_names(&self) -> Vec<String> {
        self.store.list_paths("").unwrap_or_default()
    }

    pub fn list_names_prefix(&self, prefix: &str) -> Vec<String> {
        self.store.list_paths(prefix).unwrap_or_default()
    }

    /// One-level directory listing under a prefix (journal writer discovery).
    ///
    /// Delegates to `ObjectStore::list_dirs` — on S3/R2 this is a
    /// delimiter-LIST (O(child dirs)), on localfs a single `read_dir`.
    /// Returns `Err` if the backend doesn't support directory listing.
    pub fn list_dirs(&self, prefix: &str) -> std::io::Result<Vec<String>> {
        self.store.list_dirs(prefix)
    }

    /// Stable identity of the backing store (see `ObjectStore::store_id`).
    ///
    /// Keys the process-local journal writer registry and discovery cache:
    /// same store ⇒ same journal state, different stores ⇒ isolated state.
    pub fn store_id(&self) -> String {
        self.store.store_id()
    }

    pub fn delete_ref(&self, name: &str) -> io::Result<bool> {
        self.store.delete_path(name)
    }

    /// Physically delete a blob (maintenance operation, NOT a kernel primitive).
    pub fn delete_blob(&self, hash: &str) -> io::Result<bool> {
        self.store.delete_blob(hash)
    }

    // ------------------------------------------------------------------
    // Stats
    // ------------------------------------------------------------------

    pub fn stats(&self) -> KernelStats {
        self.stats.lock().unwrap().clone()
    }

    /// Get the objects directory path (for prefix-matching in cat).
    pub fn objects_dir(&self) -> &std::path::Path {
        // This is only used by the CLI for prefix matching.
        // The ObjectStore trait doesn't expose this, so we return empty.
        // The CLI uses list_blobs_prefix instead.
        std::path::Path::new("")
    }

    /// List all blob hashes with a given prefix (for `cat` prefix matching).
    pub fn list_blobs_prefix(&self, prefix: &str) -> Vec<String> {
        if prefix.len() < 2 {
            return Vec::new();
        }
        let shard = &prefix[..2];
        self.store.list_paths(&format!("blobs/{}/", shard))
            .unwrap_or_default()
            .into_iter()
            .filter_map(|p| {
                let parts: Vec<&str> = p.split('/').collect();
                parts.get(2).map(|s| s.to_string())
            })
            .filter(|h| h.starts_with(prefix))
            .collect()
    }
}

// ---------------------------------------------------------------------------
// Async API — behind `feature = "async"`.
//
// Strategy: `PondKernel.store` is `Arc<dyn ObjectStore>`, so async methods
// clone the Arc into a `spawn_blocking` closure and await the join handle.
// This gives async callers a non-blocking API without duplicating the sync
// backend logic. Backends with native async I/O (LocalFS via tokio::fs,
// S3 via reqwest) can be used directly via the `AsyncObjectStore` trait.
// ---------------------------------------------------------------------------

#[cfg(feature = "async")]
impl PondKernel {
    /// Async variant of [`write`](Self::write). Writes bytes via the sync
    /// `ObjectStore::put_blob` on a blocking thread.
    ///
    /// The `??` unwraps both `JoinError` (panic in the blocking task) and
    /// `io::Error` (from the store).
    pub async fn write_async(&self, data: Vec<u8>) -> io::Result<String> {
        let store = self.store.clone();
        let h = tokio::task::spawn_blocking(move || store.put_blob(&data)).await??;
        self.stats.lock().unwrap().writes += 1;
        Ok(h)
    }

    /// Async variant of [`read_blob`](Self::read_blob).
    pub async fn read_blob_async(&self, hash: &str) -> io::Result<Vec<u8>> {
        let store = self.store.clone();
        let hash = hash.to_string();
        let data = tokio::task::spawn_blocking(move || store.get_blob(&hash)).await??;
        self.stats.lock().unwrap().reads += 1;
        Ok(data)
    }

    /// Async variant of [`read`](Self::read) — resolves a name to a hash
    /// (sync, fast — just a ref lookup) and then reads the blob async.
    pub async fn read_async(&self, hash_or_name: &str) -> io::Result<Vec<u8>> {
        if is_hash(hash_or_name) {
            return self.read_blob_async(hash_or_name).await;
        }
        let h = self.resolve(hash_or_name)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound,
                format!("Name '{}' not found", hash_or_name)))?;
        self.read_blob_async(&h).await
    }

    /// Async variant of [`reference`](Self::reference).
    pub async fn reference_async(&self, name: &str, h: &str) -> io::Result<()> {
        let store = self.store.clone();
        let name = name.to_string();
        let h = h.to_string();
        tokio::task::spawn_blocking(move || {
            store.put_path(&name, &h)
        }).await??;
        self.stats.lock().unwrap().references += 1;
        Ok(())
    }

    /// Async variant of [`delete_blob`](Self::delete_blob).
    pub async fn delete_blob_async(&self, hash: &str) -> io::Result<bool> {
        let store = self.store.clone();
        let hash = hash.to_string();
        // `await?` flattens `Result<Result<bool, io::Error>, JoinError>` →
        // `Result<bool, io::Error>` (JoinError auto-converts to io::Error).
        tokio::task::spawn_blocking(move || store.delete_blob(&hash)).await?
    }

    /// Async variant of [`list_blobs_prefix`](Self::list_blobs_prefix).
    pub async fn list_blobs_prefix_async(&self, prefix: &str) -> Vec<String> {
        let store = self.store.clone();
        let prefix = prefix.to_string();
        match tokio::task::spawn_blocking(move || {
            let shard = match prefix.get(..2) {
                Some(s) => s.to_string(),
                None => return Vec::new(),
            };
            let list_key = format!("blobs/{}/", shard);
            store.list_paths(&list_key)
                .unwrap_or_default()
                .into_iter()
                .filter_map(|p| {
                    let parts: Vec<&str> = p.split('/').collect();
                    parts.get(2).map(|s| s.to_string())
                })
                .filter(|h| h.starts_with(&prefix))
                .collect::<Vec<_>>()
        }).await {
            Ok(v) => v,
            Err(_) => Vec::new(),
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn is_hash(s: &str) -> bool {
    s.len() == 64 && s.chars().all(|c| c.is_ascii_hexdigit())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_write_read_roundtrip() {
        let dir = tempdir().unwrap();
        let kernel = PondKernel::new_local(dir.path()).unwrap();
        let h = kernel.write(b"hello, pond!").unwrap();
        assert_eq!(kernel.read_blob(&h).unwrap(), b"hello, pond!");
    }

    #[test]
    fn test_dedup() {
        let dir = tempdir().unwrap();
        let kernel = PondKernel::new_local(dir.path()).unwrap();
        let h1 = kernel.write(b"same").unwrap();
        let h2 = kernel.write(b"same").unwrap();
        assert_eq!(h1, h2);
    }

    #[test]
    fn test_reference_resolve() {
        let dir = tempdir().unwrap();
        let kernel = PondKernel::new_local(dir.path()).unwrap();
        let h = kernel.write(b"data").unwrap();
        kernel.reference("my_coll", &h).unwrap();
        assert_eq!(kernel.resolve("my_coll"), Some(h.clone()));
        assert_eq!(kernel.resolve("nope"), None);
    }

    #[test]
    fn test_read_by_name() {
        let dir = tempdir().unwrap();
        let kernel = PondKernel::new_local(dir.path()).unwrap();
        let h = kernel.write(b"by name").unwrap();
        kernel.reference("coll", &h).unwrap();
        assert_eq!(kernel.read("coll").unwrap(), b"by name");
    }

    #[test]
    fn test_hierarchical_refs() {
        let dir = tempdir().unwrap();
        let kernel = PondKernel::new_local(dir.path()).unwrap();
        let h = kernel.write(b"branch").unwrap();
        kernel.reference("collections/users/_branches/main/commit", &h).unwrap();
        kernel.reference("collections/users/_branches/exp/commit", &h).unwrap();
        assert_eq!(
            kernel.resolve("collections/users/_branches/main/commit"),
            Some(h.clone())
        );
        let branches = kernel.list_names_prefix("collections/users/_branches");
        assert_eq!(branches.len(), 2);
    }

    #[test]
    fn test_persistence() {
        let dir = tempdir().unwrap();
        let path = dir.path().to_path_buf();
        {
            let kernel = PondKernel::new_local(&path).unwrap();
            let h = kernel.write(b"persistent").unwrap();
            kernel.reference("my_coll", &h).unwrap();
        }
        {
            let kernel = PondKernel::new_local(&path).unwrap();
            assert!(kernel.resolve("my_coll").is_some());
            assert_eq!(kernel.read("my_coll").unwrap(), b"persistent");
        }
    }

    #[test]
    fn test_delete_ref() {
        let dir = tempdir().unwrap();
        let kernel = PondKernel::new_local(dir.path()).unwrap();
        let h = kernel.write(b"data").unwrap();
        kernel.reference("temp", &h).unwrap();
        assert!(kernel.resolve("temp").is_some());
        kernel.delete_ref("temp").unwrap();
        assert!(kernel.resolve("temp").is_none());
        assert_eq!(kernel.read_blob(&h).unwrap(), b"data");
    }

    #[test]
    fn test_write_batch() {
        let dir = tempdir().unwrap();
        let kernel = PondKernel::new_local(dir.path()).unwrap();
        let items = vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()];
        let hashes = kernel.write_batch(&items).unwrap();
        for (i, h) in hashes.iter().enumerate() {
            assert_eq!(kernel.read_blob(h).unwrap(), items[i]);
        }
    }

    #[test]
    fn test_blob_path_layout() {
        let dir = tempdir().unwrap();
        let kernel = PondKernel::new_local(dir.path()).unwrap();
        let h = kernel.write(b"check layout").unwrap();
        let expected = dir.path().join("blobs").join(&h[..2]).join(&h);
        assert!(expected.exists(), "blob should be at blobs/{}/{}, got: {}",
                &h[..2], h, expected.display());
    }

    #[test]
    fn test_ref_path_layout() {
        let dir = tempdir().unwrap();
        let kernel = PondKernel::new_local(dir.path()).unwrap();
        let h = kernel.write(b"ref layout").unwrap();
        kernel.reference("collections/users/_branches/main/commit", &h).unwrap();
        let expected = dir.path().join("collections/users/_branches/main/commit");
        assert!(expected.exists(), "ref should be at {}", expected.display());
    }

    #[test]
    fn test_ref_stores_json_not_raw_hash() {
        let dir = tempdir().unwrap();
        let kernel = PondKernel::new_local(dir.path()).unwrap();
        let h = kernel.write(b"json format").unwrap();
        kernel.reference("my_ref", &h).unwrap();
        let ref_file = dir.path().join("my_ref");
        let content = std::fs::read_to_string(&ref_file).unwrap();
        assert!(content.contains(r#""hash":"#), "ref must store JSON, got: {}", content);
        assert!(content.contains(&h), "ref must contain the hash, got: {}", content);
    }

    #[test]
    fn test_read_blob_range_full_window() {
        // Range covering the whole blob should match get_blob.
        let dir = tempdir().unwrap();
        let kernel = PondKernel::new_local(dir.path()).unwrap();
        let payload = b"0123456789ABCDEFGHIJKLMN"; // 24 bytes
        let h = kernel.write(payload).unwrap();
        let r = kernel.read_blob_range(&h, 0, payload.len() as u64).unwrap();
        assert_eq!(r.as_slice(), payload);
    }

    #[test]
    fn test_read_blob_range_partial_window() {
        // Middle slice — LocalFS must seek, not load whole file.
        let dir = tempdir().unwrap();
        let kernel = PondKernel::new_local(dir.path()).unwrap();
        let payload = b"0123456789ABCDEFGHIJKLMN"; // 24 bytes
        let h = kernel.write(payload).unwrap();
        let r = kernel.read_blob_range(&h, 5, 15).unwrap();
        assert_eq!(r, b"56789ABCDE", "bytes [5,15) should be '56789ABCDE'");
    }

    #[test]
    fn test_read_blob_range_end_past_size_clamps() {
        // end > blob_len should be clamped (no error).
        let dir = tempdir().unwrap();
        let kernel = PondKernel::new_local(dir.path()).unwrap();
        let payload = b"hello"; // 5 bytes
        let h = kernel.write(payload).unwrap();
        let r = kernel.read_blob_range(&h, 2, 100).unwrap();
        assert_eq!(r, b"llo", "should return [2, 5) = 'llo'");
    }

    #[test]
    fn test_read_blob_range_empty_returns_empty() {
        // start >= end should return empty without I/O.
        let dir = tempdir().unwrap();
        let kernel = PondKernel::new_local(dir.path()).unwrap();
        let h = kernel.write(b"some bytes here").unwrap();
        let r = kernel.read_blob_range(&h, 5, 5).unwrap();
        assert!(r.is_empty(), "[5,5) should be empty");
        let r2 = kernel.read_blob_range(&h, 0, 0).unwrap();
        assert!(r2.is_empty(), "[0,0) should be empty");
    }

    #[test]
    fn test_read_blob_range_start_past_end_returns_empty() {
        // start >= blob_len should return empty.
        let dir = tempdir().unwrap();
        let kernel = PondKernel::new_local(dir.path()).unwrap();
        let h = kernel.write(b"abc").unwrap();
        let r = kernel.read_blob_range(&h, 100, 200).unwrap();
        assert!(r.is_empty(), "start past EOF should return empty");
    }

    #[test]
    fn test_read_blob_range_tail_fetch_pattern() {
        // Simulate the slab reader's step 1: fetch the last 12 bytes (the
        // PSLB tail). This is the exact pattern the slab reader uses.
        let dir = tempdir().unwrap();
        let kernel = PondKernel::new_local(dir.path()).unwrap();
        // Build a fake slab: header(10) + payload(20) + footer(8) + tail(12)
        let mut blob = Vec::new();
        blob.extend_from_slice(b"PSLB\x01\x01\x01\x00\x00\x00"); // header
        blob.extend_from_slice(b"PAYLOAD_20_BYTES!!!");           // 20 bytes payload
        blob.extend_from_slice(b"FOOTER8!");                      // 8 bytes footer
        blob.extend_from_slice(b"PSLB");                          // tail magic
        blob.extend_from_slice(&[0u8; 8]);                        // footer_offset
        let total = blob.len();
        let h = kernel.write(&blob).unwrap();
        let tail = kernel.read_blob_range(&h, (total - 12) as u64, total as u64).unwrap();
        assert_eq!(tail.len(), 12);
        assert_eq!(&tail[0..4], b"PSLB", "tail must start with PSLB magic");
    }
}

// ---------------------------------------------------------------------------
// Async tests — only compiled when `feature = "async"` is on.
// ---------------------------------------------------------------------------

#[cfg(all(test, feature = "async"))]
mod async_tests {
    use super::*;
    use tempfile::tempdir;

    /// Round-trip: write_async → read_blob_async.
    #[tokio::test]
    async fn test_async_write_read_roundtrip() {
        let dir = tempdir().unwrap();
        let kernel = PondKernel::new_local(dir.path()).unwrap();
        let h = kernel.write_async(b"hello, async pond!".to_vec()).await.unwrap();
        let data = kernel.read_blob_async(&h).await.unwrap();
        assert_eq!(data, b"hello, async pond!");
    }

    /// Async read by name (reference_async + read_async).
    #[tokio::test]
    async fn test_async_reference_and_read_by_name() {
        let dir = tempdir().unwrap();
        let kernel = PondKernel::new_local(dir.path()).unwrap();
        let h = kernel.write_async(b"by name async".to_vec()).await.unwrap();
        kernel.reference_async("my_coll", &h).await.unwrap();
        assert_eq!(kernel.resolve("my_coll"), Some(h.clone()));
        let data = kernel.read_async("my_coll").await.unwrap();
        assert_eq!(data, b"by name async");
    }

    /// Async dedup: same bytes → same hash (matches sync behavior).
    #[tokio::test]
    async fn test_async_dedup() {
        let dir = tempdir().unwrap();
        let kernel = PondKernel::new_local(dir.path()).unwrap();
        let h1 = kernel.write_async(b"same".to_vec()).await.unwrap();
        let h2 = kernel.write_async(b"same".to_vec()).await.unwrap();
        assert_eq!(h1, h2);
    }

    /// Async delete_blob returns true on existing, false after deletion.
    #[tokio::test]
    async fn test_async_delete_blob() {
        let dir = tempdir().unwrap();
        let kernel = PondKernel::new_local(dir.path()).unwrap();
        let h = kernel.write_async(b"to be deleted".to_vec()).await.unwrap();
        assert!(kernel.delete_blob_async(&h).await.unwrap());
        // Second delete returns false (already gone).
        assert!(!kernel.delete_blob_async(&h).await.unwrap());
    }

    /// Async list_blobs_prefix matches sync list_blobs_prefix for the
    /// same set of written blobs.
    #[tokio::test]
    async fn test_async_list_blobs_prefix_matches_sync() {
        let dir = tempdir().unwrap();
        let kernel = PondKernel::new_local(dir.path()).unwrap();
        // Write many blobs so at least 2 land in the same shard (blobs/{xx}/).
        // With 20 blobs, the probability of all 20 having distinct 2-char
        // prefixes is essentially zero (256 shards, birthday paradox).
        let mut all_hashes = Vec::new();
        for i in 0..20u32 {
            let h = kernel
                .write_async(format!("blob-{:04}", i).into_bytes())
                .await
                .unwrap();
            all_hashes.push(h);
        }

        // Pick the first 2 chars of the first hash as the prefix. The sync
        // and async paths should both return every blob whose hash starts
        // with that prefix — and they should agree exactly.
        let prefix = all_hashes[0][..2].to_string();
        let matching: Vec<String> = all_hashes.iter()
            .filter(|h| h.starts_with(&prefix))
            .cloned()
            .collect();
        assert!(!matching.is_empty(), "expected at least 1 blob in shard {}", prefix);

        let sync_list = kernel.list_blobs_prefix(&prefix);
        let async_list = kernel.list_blobs_prefix_async(&prefix).await;

        let mut s = sync_list.clone();
        let mut a = async_list.clone();
        s.sort();
        a.sort();
        assert_eq!(s, a, "sync and async prefix listing must match");

        // And the list must contain every matching hash we wrote.
        for h in &matching {
            assert!(s.contains(h), "list must contain {}", h);
        }
    }

    /// AsyncObjectStore impl on LocalFSObjectStore: direct trait call,
    /// bypassing the kernel. Verifies the trait is usable standalone.
    #[tokio::test]
    async fn test_async_object_store_local_fs() {
        use crate::AsyncObjectStore;
        let dir = tempdir().unwrap();
        let store = LocalFSObjectStore::new(dir.path()).unwrap();
        let h = store.put_blob_async(b"via trait".to_vec()).await.unwrap();
        let data = store.get_blob_async(&h).await.unwrap();
        assert_eq!(data, b"via trait");
        assert!(store.delete_blob_async(&h).await.unwrap());
        // After deletion, get_blob_async returns NotFound.
        let err = store.get_blob_async(&h).await.unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::NotFound);
    }

    /// Async read of a non-existent hash returns NotFound.
    #[tokio::test]
    async fn test_async_read_blob_not_found() {
        let dir = tempdir().unwrap();
        let kernel = PondKernel::new_local(dir.path()).unwrap();
        let fake = "0".repeat(64);
        let err = kernel.read_blob_async(&fake).await.unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::NotFound);
    }

    /// Concurrent async writes from many tasks don't deadlock or lose data.
    #[tokio::test]
    async fn test_async_concurrent_writes() {
        let dir = tempdir().unwrap();
        // We can't clone PondKernel itself (it's not Clone by design), so
        // wrap it in Arc and have each task use `&PondKernel` via the Arc.
        let kernel = std::sync::Arc::new(PondKernel::new_local(dir.path()).unwrap());
        let n = 32;
        let mut handles = Vec::with_capacity(n);
        for i in 0..n {
            let k = kernel.clone();
            handles.push(tokio::spawn(async move {
                let payload = format!("payload-{}", i);
                // Borrow through the Arc — async methods take &self.
                k.write_async(payload.into_bytes()).await.unwrap()
            }));
        }
        let mut hashes = Vec::with_capacity(n);
        for h in handles {
            hashes.push(h.await.unwrap());
        }
        // Each hash should read back its own payload.
        for (i, h) in hashes.iter().enumerate() {
            let data = kernel.read_blob_async(h).await.unwrap();
            assert_eq!(data, format!("payload-{}", i).into_bytes());
        }
        // All hashes should be distinct (different content → different hash).
        let mut unique = hashes.clone();
        unique.sort();
        unique.dedup();
        assert_eq!(unique.len(), n, "all {} hashes must be distinct", n);
    }
}
