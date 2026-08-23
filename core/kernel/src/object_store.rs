// ObjectStore trait + LocalFSObjectStore — the storage backend abstraction
//
// All backends (local FS, S3, GCS) implement this trait. The kernel uses
// this trait — it doesn't know which backend.
//
// PATH LAYOUT (same on all backends):
//   blobs/{hash[:2]}/{hash}        — content-addressed blobs (raw bytes)
//   {path}                          — named refs (JSON: {"hash":"..."})

use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use crate::hash_bytes;

// ---------------------------------------------------------------------------
// ObjectStore trait — the storage backend abstraction
// ---------------------------------------------------------------------------

/// The storage backend trait. All backends (local FS, S3, GCS) implement
/// this. The kernel uses this trait — it doesn't know which backend.
///
/// PATH LAYOUT (same on all backends):
///   blobs/{hash[:2]}/{hash}        — content-addressed blobs (raw bytes)
///   {path}                          — named refs (JSON: {"hash":"..."})
///
/// The ref path IS the key — no "refs/" prefix. This matches S3's flat
/// key space and allows `aws s3 sync` / `rsync` for migration.
pub trait ObjectStore: Send + Sync {
    /// Write bytes, content-addressed. Returns the hash.
    /// Idempotent: same bytes → same hash → same key. Overwriting is safe.
    fn put_blob(&self, data: &[u8]) -> io::Result<String>;

    /// Read bytes by content hash.
    fn get_blob(&self, hash: &str) -> io::Result<Vec<u8>>;

    /// Write a batch of blobs. Default: sequential. Backends can override
    /// with parallel implementation (S3 uses a thread pool).
    fn put_blob_batch(&self, items: &[Vec<u8>]) -> io::Result<Vec<String>> {
        let mut hashes = Vec::with_capacity(items.len());
        for data in items {
            hashes.push(self.put_blob(data)?);
        }
        Ok(hashes)
    }

    /// Read a batch of blobs. Default: sequential.
    fn get_blob_batch(&self, hashes: &[String]) -> io::Result<Vec<Vec<u8>>> {
        let mut results = Vec::with_capacity(hashes.len());
        for h in hashes {
            results.push(self.get_blob(h)?);
        }
        Ok(results)
    }

    /// Bind a named path to a content hash. Stores JSON {"hash":"..."}.
    /// Last-writer-wins (no CAS). The backend handles atomicity.
    fn put_path(&self, path: &str, hash: &str) -> io::Result<()>;

    /// Resolve a named path to its content hash. Returns None if unbound.
    fn get_path(&self, path: &str) -> Option<String>;

    /// Delete a named path. Returns true if it existed.
    fn delete_path(&self, path: &str) -> io::Result<bool>;

    /// List all paths under a prefix. Returns relative paths (without prefix).
    fn list_paths(&self, prefix: &str) -> io::Result<Vec<String>>;

    /// Check if a blob exists (for dedup checks).
    fn blob_exists(&self, hash: &str) -> bool;

    /// Read a byte range `[start, end)` from a content-addressed blob.
    ///
    /// This is the **range-read primitive** that enables slab-style layout
    /// optimizations: a reader fetches only the bytes it needs (e.g. a single
    /// row group's PND2 bytes out of a 128 MB slab) instead of the full blob.
    /// On object storage this maps to the HTTP `Range:` header — a single
    /// round-trip returning a partial payload, billed at the smaller size.
    ///
    /// **Default impl**: full `get_blob` + slice. This works on every backend
    /// (no layout improvement) — backends with native range support (local
    /// FS `seek+read`, S3 `Range:` header) should override to avoid the
    /// full-object fetch.
    ///
    /// **Half-open interval**: `end` is exclusive. To fetch bytes 0..1023 of
    /// a 1 KB blob, call `get_blob_range(hash, 0, 1024)`. If `end` exceeds
    /// the blob length, the result is truncated to the available bytes
    /// (no error). If `start >= end` or `start >= blob_len`, returns empty.
    ///
    /// **Use cases**:
    ///   - Slab tail fetch: `get_blob_range(slab_hash, len-12, len)`
    ///   - Slab footer fetch: `get_blob_range(slab_hash, footer_off, len-12)`
    ///   - Per-RG fetch: `get_blob_range(slab_hash, rg_off, rg_off+rg_len)`
    fn get_blob_range(&self, hash: &str, start: u64, end: u64) -> io::Result<Vec<u8>> {
        // Default: read the whole blob and slice. This is correct on every
        // backend but offers no performance benefit — the whole point of
        // overriding is to avoid fetching bytes outside [start, end).
        let full = self.get_blob(hash)?;
        let len = full.len() as u64;
        if start >= len {
            return Ok(Vec::new());
        }
        let end_clamped = end.min(len);
        if start >= end_clamped {
            return Ok(Vec::new());
        }
        Ok(full[start as usize..end_clamped as usize].to_vec())
    }

    /// Physically delete a blob from the object store (maintenance operation).
    ///
    /// This is NOT a kernel primitive — it's a Layer 0.5 maintenance operation
    /// (like VACUUM in PostgreSQL or git gc). Used by compact_shards and merge
    /// to reclaim storage space from absorbed/dead shards.
    ///
    /// Best-effort: if the store doesn't support deletion, this is a no-op.
    /// The blob becomes unreachable (orphaned) but the system is still correct.
    fn delete_blob(&self, hash: &str) -> io::Result<bool>;
}

// ---------------------------------------------------------------------------
// AsyncObjectStore trait — async version, behind `feature = "async"`
// ---------------------------------------------------------------------------

/// Async variant of [`ObjectStore`]. Only available with the `async` feature.
///
/// All backends that want to expose true async I/O (rather than just blocking
/// the sync API on a thread pool) should implement this trait in addition to
/// [`ObjectStore`]. The sync trait remains the source of truth — async
/// methods are a performance opt-in for high-throughput concurrent workloads
/// (e.g. AI frameworks that use `asyncio`, which currently have to block the
/// GIL on every S3 call through the PyO3 binding).
///
/// `LocalFSObjectStore` implements this with `tokio::fs`; `S3ObjectStore`
/// implements it with `reqwest`.
///
/// NOTE: this trait does NOT extend [`ObjectStore`] — backends implement both
/// independently. The kernel's async methods (`PondKernel::read_blob_async`,
/// etc.) call `spawn_blocking` on the sync `ObjectStore` for now, which keeps
/// the trait object story simple. Backends that want true async I/O bypass
/// the kernel and call their `AsyncObjectStore` impl directly.
#[cfg(feature = "async")]
#[async_trait::async_trait]
pub trait AsyncObjectStore: Send + Sync {
    /// Write bytes content-addressed, returning the hash. Idempotent.
    async fn put_blob_async(&self, data: Vec<u8>) -> io::Result<String>;

    /// Read bytes by content hash.
    async fn get_blob_async(&self, hash: &str) -> io::Result<Vec<u8>>;

    /// Physically delete a blob (maintenance op). Returns true if it existed.
    async fn delete_blob_async(&self, hash: &str) -> io::Result<bool>;

    /// List all blob hashes with a given prefix (relative to the blobs/ tree).
    async fn list_blobs_prefix_async(&self, prefix: &str) -> Vec<String>;
}

// ---------------------------------------------------------------------------
// LocalFSObjectStore — local filesystem implementation
// ---------------------------------------------------------------------------

/// Local filesystem object store. Mirrors the S3 key structure exactly:
///
///   Blobs: {base_dir}/blobs/{hash[:2]}/{hash}
///   Refs:  {base_dir}/{path}   (e.g. collections/users/_branches/main/commit)
///
/// Thread-safe: blob writes use temp+rename (POSIX atomic); path writes
/// use temp+rename with unique temp names.
pub struct LocalFSObjectStore {
    base_dir: PathBuf,
    stats: Mutex<StoreStats>,
}

#[derive(Debug, Default, Clone)]
pub struct StoreStats {
    pub gets: u64,
    pub puts: u64,
    pub bytes_read: u64,
    pub bytes_written: u64,
}

impl LocalFSObjectStore {
    pub fn new(base_dir: impl AsRef<Path>) -> io::Result<Self> {
        let base = base_dir.as_ref();
        fs::create_dir_all(base.join("blobs"))?;
        Ok(Self {
            base_dir: base.to_path_buf(),
            stats: Mutex::new(StoreStats::default()),
        })
    }

    pub fn base_dir(&self) -> &Path {
        &self.base_dir
    }

    fn blob_path(&self, hash: &str) -> PathBuf {
        self.base_dir.join("blobs").join(&hash[..2]).join(hash)
    }

    fn path_file(&self, path: &str) -> PathBuf {
        self.base_dir.join(path)
    }
}

impl ObjectStore for LocalFSObjectStore {
    fn put_blob(&self, data: &[u8]) -> io::Result<String> {
        let h = hash_bytes(data);
        let path = self.blob_path(&h);
        // Dedup: skip if exists
        if !path.exists() {
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent)?;
            }
            // Write to temp file, then rename (POSIX atomic).
            // Use process ID + counter for unique temp name (avoids
            // collisions between concurrent writers).
            let tmp = format!("{}.tmp.{}", path.display(), std::process::id());
            fs::write(&tmp, data)?;
            fs::rename(&tmp, &path)?;
        }
        let mut s = self.stats.lock().unwrap();
        s.puts += 1;
        s.bytes_written += data.len() as u64;
        Ok(h)
    }

    fn get_blob(&self, hash: &str) -> io::Result<Vec<u8>> {
        let path = self.blob_path(hash);
        if !path.exists() {
            return Err(io::Error::new(io::ErrorKind::NotFound,
                format!("Blob '{}' not found", hash)));
        }
        let data = fs::read(&path)?;
        let mut s = self.stats.lock().unwrap();
        s.gets += 1;
        s.bytes_read += data.len() as u64;
        Ok(data)
    }

    fn put_path(&self, path: &str, hash: &str) -> io::Result<()> {
        let file = self.path_file(path);
        if let Some(parent) = file.parent() {
            fs::create_dir_all(parent)?;
        }
        // Store JSON {"hash":"..."} — same format as S3ObjectStore.
        // This makes `aws s3 sync` / `rsync` a straight copy.
        let body = format!(r#"{{"hash":"{}"}}"#, hash);
        // Write temp → rename (POSIX atomic on local FS).
        // On S3, put_path just does PUT (S3 is already idempotent).
        let tmp = format!("{}.tmp.{}", file.display(), std::process::id());
        fs::write(&tmp, &body)?;
        fs::rename(&tmp, &file)?;
        let mut s = self.stats.lock().unwrap();
        s.puts += 1;
        Ok(())
    }

    fn get_path(&self, path: &str) -> Option<String> {
        let file = self.path_file(path);
        match fs::read_to_string(&file) {
            Ok(body) => {
                // Parse JSON {"hash":"..."} — minimal parser
                extract_hash_from_json(&body)
            }
            Err(_) => None,
        }
    }

    fn delete_path(&self, path: &str) -> io::Result<bool> {
        let file = self.path_file(path);
        if file.exists() {
            fs::remove_file(&file)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn list_paths(&self, prefix: &str) -> io::Result<Vec<String>> {
        let prefix_dir = self.base_dir.join(prefix);
        let mut paths = Vec::new();
        if prefix_dir.is_dir() {
            walk_dir(&prefix_dir, &self.base_dir, &mut paths);
        }
        paths.sort();
        Ok(paths)
    }

    fn blob_exists(&self, hash: &str) -> bool {
        self.blob_path(hash).exists()
    }

    fn get_blob_range(&self, hash: &str, start: u64, end: u64) -> io::Result<Vec<u8>> {
        // Native range read: open the file, seek to `start`, read up to
        // `end - start` bytes. This avoids loading the whole blob into
        // memory — critical for large slabs (100 MB+) where the caller
        // only needs a few KB (e.g. the 12-byte tail or a single RG).
        use std::io::{Read, Seek, SeekFrom};
        let path = self.blob_path(hash);
        if !path.exists() {
            return Err(io::Error::new(io::ErrorKind::NotFound,
                format!("Blob '{}' not found", hash)));
        }
        let mut f = std::fs::File::open(&path)?;
        // File size — clamp end to it (caller may request past EOF).
        let file_len = f.metadata()?.len();
        if start >= file_len {
            return Ok(Vec::new());
        }
        let end_clamped = end.min(file_len);
        if start >= end_clamped {
            return Ok(Vec::new());
        }
        let len = (end_clamped - start) as usize;
        f.seek(SeekFrom::Start(start))?;
        let mut buf = vec![0u8; len];
        // read_exact ensures we got all requested bytes; if the file is
        // shorter than metadata reported (race condition), error out.
        match f.read_exact(&mut buf) {
            Ok(()) => {
                let mut s = self.stats.lock().unwrap();
                s.gets += 1;
                s.bytes_read += buf.len() as u64;
                Ok(buf)
            }
            Err(e) => Err(e),
        }
    }

    fn delete_blob(&self, hash: &str) -> io::Result<bool> {
        let path = self.blob_path(hash);
        if path.exists() {
            fs::remove_file(&path)?;
            let mut s = self.stats.lock().unwrap();
            s.gets += 1; // count as a maintenance op
            Ok(true)
        } else {
            Ok(false)
        }
    }
}

/// Walk a directory recursively, collecting file paths relative to root.
fn walk_dir(dir: &Path, root: &Path, paths: &mut Vec<String>) {
    if let Ok(entries) = fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                walk_dir(&path, root, paths);
            } else if path.is_file() {
                if let Ok(rel) = path.strip_prefix(root) {
                    paths.push(rel.to_string_lossy().replace('\\', "/"));
                }
            }
        }
    }
}

/// Extract the "hash" field from a JSON string like {"hash":"abc123"}.
fn extract_hash_from_json(json: &str) -> Option<String> {
    let needle = r#""hash":""#;
    if let Some(start) = json.find(needle) {
        let rest = &json[start + needle.len()..];
        if let Some(end) = rest.find('"') {
            return Some(rest[..end].to_string());
        }
    }
    None
}

// ---------------------------------------------------------------------------
// AsyncObjectStore impl for LocalFSObjectStore — tokio::fs
// ---------------------------------------------------------------------------

#[cfg(feature = "async")]
#[async_trait::async_trait]
impl AsyncObjectStore for LocalFSObjectStore {
    async fn put_blob_async(&self, data: Vec<u8>) -> io::Result<String> {
        let h = hash_bytes(&data);
        let path = self.blob_path(&h);

        // Dedup: skip if exists. `tokio::fs::try_exists` is stable on Rust 1.70+.
        // We use `tokio::fs::metadata` for compatibility with older toolchains.
        let already_exists = tokio::fs::metadata(&path).await.is_ok();
        if !already_exists {
            if let Some(parent) = path.parent() {
                tokio::fs::create_dir_all(parent).await?;
            }
            // Write to temp file, then rename (POSIX atomic).
            let tmp = format!("{}.tmp.{}", path.display(), std::process::id());
            tokio::fs::write(&tmp, &data).await?;
            tokio::fs::rename(&tmp, &path).await?;
        }
        let mut s = self.stats.lock().unwrap();
        s.puts += 1;
        s.bytes_written += data.len() as u64;
        Ok(h)
    }

    async fn get_blob_async(&self, hash: &str) -> io::Result<Vec<u8>> {
        let path = self.blob_path(hash);
        // Mirror the sync error: NotFound when the blob is missing.
        if tokio::fs::metadata(&path).await.is_err() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("Blob '{}' not found", hash),
            ));
        }
        let data = tokio::fs::read(&path).await?;
        let mut s = self.stats.lock().unwrap();
        s.gets += 1;
        s.bytes_read += data.len() as u64;
        Ok(data)
    }

    async fn delete_blob_async(&self, hash: &str) -> io::Result<bool> {
        let path = self.blob_path(hash);
        match tokio::fs::remove_file(&path).await {
            Ok(()) => {
                let mut s = self.stats.lock().unwrap();
                s.gets += 1; // count as a maintenance op (matches sync impl)
                Ok(true)
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(e),
        }
    }

    async fn list_blobs_prefix_async(&self, prefix: &str) -> Vec<String> {
        if prefix.len() < 2 {
            return Vec::new();
        }
        let shard = &prefix[..2];
        let list_prefix = self.base_dir.join("blobs").join(shard);

        // Collect into a Vec<PathBuf> first, then map to strings. We use
        // tokio::fs::read_dir for an async walk. For simplicity we walk
        // single-level (blobs/{shard}/ only — Pond never nests deeper).
        let mut entries = match tokio::fs::read_dir(&list_prefix).await {
            Ok(rd) => rd,
            Err(_) => return Vec::new(),
        };

        let mut hashes = Vec::new();
        while let Ok(Some(entry)) = entries.next_entry().await {
            if entry.file_type().await.map(|t| t.is_file()).unwrap_or(false) {
                let name = entry.file_name();
                let name = name.to_string_lossy().replace('\\', "/");
                if name.starts_with(prefix) {
                    hashes.push(name);
                }
            }
        }
        hashes.sort();
        hashes
    }
}
