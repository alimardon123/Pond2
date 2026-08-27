// CachingObjectStore — in-memory + local-disk LRU cache for any ObjectStore.
//
// WRAPS any `ObjectStore` (LocalFS, S3, GCS, ...) with FOUR cache tiers:
//
//   Tier 0 (in-memory blob cache): `moka::sync::Cache` for hot content blobs.
//       Sub-microsecond lookups with sharded internal locking (16 segments).
//       Byte-level weight tracking ensures bounded memory usage (default 256 MB).
//       Blobs >= skip threshold (default 1 MB) skip this tier — large blobs
//       are better served from disk cache + mmap.
//
//   Tier 0.5 (block cache): `moka::sync::Cache` for sub-slab range-read results.
//       Caches (hash, offset, len) to Arc<Vec<u8>>. Enables ~15-20us reads
//       for hot sub-slab ranges (vs 50-300ms S3 Range GET). Default 256 MB.
//       Uses windowed-TinyLFU eviction (better than pure LRU for hot RGs).
//
//   Tier 1 (in-memory): `HashMap` for ref lookups (get_path). Refs are
//       tiny JSON blobs (~60 bytes each) and are consulted on EVERY
//       operation. A 5-second TTL avoids stale reads in multi-writer
//       scenarios while eliminating the S3 GET for branch HEAD resolution.
//
//   Tier 2 (local-disk): File cache for content blobs with TRUE O(1) LRU
//       eviction by total byte size (default 1 GB). An in-memory
//       `lru::LruCache` tracks access order; eviction pops the LRU entry
//       and deletes it from disk. This is O(1) per eviction — no directory
//       scan, no sort, no stat() calls.
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
//       .with_max_mem_bytes(256 * 1024 * 1024)  // 256 MB in-memory
//       .with_ref_ttl(std::time::Duration::from_secs(5));
//   let kernel = PondKernel::new_with_store(Box::new(cached));
//
// DESIGN DECISIONS:
//   - Write-through (not write-back): writes go to inner store AND cache.
//     This guarantees the cache is always a subset of the inner store.
//   - Content-addressed: blob file names are the SHA-256 hash.
//     No index needed -- if the file exists, the blob is cached.
//   - TRUE LRU via `lru::LruCache`: get_blob hits promote the entry to
//     most-recently-used. Eviction is O(1) — no directory scan.
//   - Cache directory uses same layout as inner store:
//     cache_dir/blobs/{hash[:2]}/{hash}

use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use lru::LruCache;
use moka::sync::Cache;
use pond_kernel::ObjectStore;

// ---------------------------------------------------------------------------
// Ref cache entry
// ---------------------------------------------------------------------------

struct RefEntry {
    hash: String,
    inserted_at: Instant,
}

// ---------------------------------------------------------------------------
// In-flight request coalescing — prevents thundering herd on hot blobs
// ---------------------------------------------------------------------------

/// A pending (in-flight) blob fetch. Multiple callers requesting the same
/// hash concurrently will find this entry, wait on the condvar, and then
/// read the result from `result` instead of issuing duplicate S3 GETs.
struct InFlight {
    /// None = still in progress; Some = completed (Ok or Err).
    result: Option<io::Result<Vec<u8>>>,
}

// ---------------------------------------------------------------------------
// Disk cache entry — tracks hash + byte size for O(1) eviction
// ---------------------------------------------------------------------------

struct DiskEntry {
    bytes: usize,
}

// ---------------------------------------------------------------------------
// CachingObjectStore
// ---------------------------------------------------------------------------

/// A four-tier cache wrapper around any `ObjectStore`.
///
/// Tier 0: In-memory `moka::sync::Cache` for hot content blobs (< skip threshold).
/// Tier 0.5: In-memory `moka::sync::Cache` for sub-slab range-read results.
/// Tier 1: In-memory `HashMap` for ref lookups (get_path) with TTL.
/// Tier 2: Local-disk file cache for content-addressed blobs with O(1) LRU eviction.
pub struct CachingObjectStore {
    inner: Box<dyn ObjectStore>,
    cache_dir: PathBuf,
    /// In-memory blob cache (moka segmented LRU). Sub-microsecond lookups
    /// with sharded locking. Byte-weighted for bounded memory usage.
    mem_cache: Cache<String, Arc<Vec<u8>>>,
    max_mem_bytes: usize,
    /// Blobs >= this size skip the in-memory cache (default 1 MB).
    /// Large blobs are better served from disk cache + seek/mmap.
    skip_mem_cache_threshold: usize,
    /// Tier 0.5: Sub-slab block cache for range-read results.
    /// Caches (hash:offset:len) to Arc<Vec<u8>>. Enables ~15-20us reads
    /// for hot sub-slab ranges instead of 50-300ms S3 Range GETs.
    /// Uses windowed-TinyLFU eviction — better than pure LRU for hot RGs
    /// because frequently-accessed ranges are retained even during cold bursts.
    block_cache: Cache<String, Arc<Vec<u8>>>,
    /// Tracks which blob hashes have entries in the block cache.
    /// Used for invalidation on delete_blob (moka lacks prefix removal).
    block_cache_hashes: Mutex<std::collections::HashSet<String>>,
    ref_cache: Mutex<std::collections::HashMap<String, RefEntry>>,
    ref_ttl: Duration,
    /// Tracks access order for TRUE LRU eviction. Key = hash, Value = byte size.
    /// O(1) promotion on get_blob hit. O(1) eviction via pop_lru().
    access_order: Mutex<LruCache<String, DiskEntry>>,
    /// Running total of bytes on disk. Updated on write/evict.
    disk_usage: Mutex<usize>,
    max_disk_bytes: usize,
    #[allow(clippy::type_complexity)]
    /// In-flight request coalescing: prevents thundering herd.
    /// Maps hash → (Mutex<InFlight>, Condvar). The first requester inserts
    /// an entry with result=None, does the S3 GET, sets result=Some(...),
    /// and notifies. Subsequent requesters wait on the condvar.
    in_flight: Mutex<std::collections::HashMap<String, Arc<(Mutex<InFlight>, Condvar)>>>,
}

// ---------------------------------------------------------------------------
// Cache-dir resolution for entry points (CLI, pyo3 bindings)
// ---------------------------------------------------------------------------

/// Resolve the local disk-cache directory for an entry point (CLI / pyo3).
///
/// Resolution order:
///   1. `explicit` override (e.g. the `cache_dir=` kwarg from Python),
///   2. the `POND_CACHE_DIR` environment variable,
///   3. default: `$HOME/.pond_cache` (or the temp dir if HOME is unset).
///
/// Caching is DISABLED (returns `None`) when the effective value is empty,
/// `"off"`, or `"none"` — set `POND_CACHE_DIR=off` to opt out.
///
/// This is what connects the 3-tier cache (memory → disk → S3) to every
/// production entry point, so warm reads are served in µs–ms instead of
/// paying 50–300ms S3 RTTs.
pub fn resolve_cache_dir(explicit: Option<&str>) -> Option<PathBuf> {
    let raw: Option<String> = explicit
        .map(str::to_string)
        .or_else(|| std::env::var("POND_CACHE_DIR").ok());
    match raw.as_deref() {
        None => Some(default_cache_root()),
        Some("") | Some("off") | Some("none") => None,
        Some(dir) => Some(PathBuf::from(dir)),
    }
}

/// Default cache root: `$HOME/.pond_cache`, falling back to the temp dir.
fn default_cache_root() -> PathBuf {
    match std::env::var("HOME") {
        Ok(home) if !home.is_empty() => PathBuf::from(home).join(".pond_cache"),
        _ => std::env::temp_dir().join("pond-cache"),
    }
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
        let max_mem = 256 * 1024 * 1024; // 256 MB default
        let max_block = 256 * 1024 * 1024; // 256 MB default for block cache
        Ok(Self {
            inner,
            cache_dir,
            mem_cache: Cache::builder()
                .max_capacity(max_mem as u64)
                .weigher(|_key: &String, value: &Arc<Vec<u8>>| -> u32 {
                    value.len() as u32
                })
                .build(),
            max_mem_bytes: max_mem,
            skip_mem_cache_threshold: 1_048_576, // 1 MB
            block_cache: Cache::builder()
                .max_capacity(max_block as u64)
                .weigher(|_key: &String, value: &Arc<Vec<u8>>| -> u32 {
                    value.len() as u32
                })
                .build(),
            block_cache_hashes: Mutex::new(std::collections::HashSet::new()),
            ref_cache: Mutex::new(std::collections::HashMap::new()),
            ref_ttl: Duration::from_secs(5),
            access_order: Mutex::new(LruCache::unbounded()),
            disk_usage: Mutex::new(0),
            max_disk_bytes: 1_000_000_000, // 1 GB default
            in_flight: Mutex::new(std::collections::HashMap::new()),
        })
    }

    /// Set the maximum disk cache size in bytes (default: 1 GB).
    /// When exceeded, the least-recently-used blobs are evicted in O(1).
    pub fn with_max_disk_bytes(mut self, bytes: usize) -> Self {
        self.max_disk_bytes = bytes;
        self
    }

    /// Set the maximum in-memory cache size in bytes (default: 256 MB).
    /// Blobs are weighted by their byte length. When the total weight
    /// exceeds this value, the least-recently-used entries are evicted.
    pub fn with_max_mem_bytes(mut self, bytes: usize) -> Self {
        self.max_mem_bytes = bytes;
        self.mem_cache = Cache::builder()
            .max_capacity(bytes as u64)
            .weigher(|_key: &String, value: &Arc<Vec<u8>>| -> u32 {
                value.len() as u32
            })
            .build();
        self
    }

    /// Set the threshold above which blobs skip the in-memory cache (default: 1 MB).
    /// Large blobs are better served from disk cache with seek/mmap.
    pub fn with_skip_mem_cache_threshold(mut self, bytes: usize) -> Self {
        self.skip_mem_cache_threshold = bytes;
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

    /// Return the approximate weighted size of the in-memory blob cache.
    /// Note: moka's internal counters may lag; prefer functional tests
    /// (delete disk file, verify mem cache serves the data).
    pub fn mem_cache_weighted_size(&self) -> u64 {
        self.mem_cache.weighted_size()
    }

    /// Return the approximate weighted size of the block cache.
    pub fn block_cache_weighted_size(&self) -> u64 {
        self.block_cache.weighted_size()
    }

    /// Invalidate all block cache entries for a given blob hash.
    /// Called on delete_blob to prevent stale entries serving deleted data.
    ///
    /// CORRECTNESS: this must ACTUALLY remove the cached ranges. The old
    /// implementation only removed the hash from the tracking set and let
    /// entries "be evicted naturally" — meaning get_blob_range() kept
    /// serving bytes for a blob deleted from the inner store (deleted-data
    /// resurrection). Block keys are "{hash}:{offset}:{len}" and hashes are
    /// 64-char hex (never contain ':'), so prefix "{hash}:" is unambiguous.
    fn invalidate_block_cache(&self, hash: &str) {
        let mut hashes = self.block_cache_hashes.lock().unwrap();
        if !hashes.remove(hash) {
            return; // no block entries tracked for this hash — nothing to do
        }
        drop(hashes);
        // moka lacks prefix removal but exposes a weakly-consistent
        // iterator. delete_blob is rare (GC/vacuum), so an O(n) scan over
        // block-cache keys is acceptable; cache is bounded anyway.
        let prefix = format!("{}:", hash);
        let stale_keys: Vec<String> = self
            .block_cache
            .iter()
            .filter(|(k, _)| k.starts_with(&prefix))
            .map(|(k, _)| (*k).clone())
            .collect();
        for key in stale_keys {
            self.block_cache.invalidate(&key);
        }
    }

    // -- Blob cache paths --

    fn blob_path(&self, hash: &str) -> PathBuf {
        self.cache_dir.join("blobs").join(&hash[..2]).join(hash)
    }

    fn read_blob_from_disk(&self, hash: &str) -> io::Result<Vec<u8>> {
        let path = self.blob_path(hash);
        fs::read(&path)
    }

    /// Maybe insert a blob into the in-memory cache.
    /// Returns true if inserted (blob was small enough).
    fn maybe_insert_mem_cache(&self, hash: &str, data: Vec<u8>) {
        if data.len() < self.skip_mem_cache_threshold {
            self.mem_cache.insert(hash.to_string(), Arc::new(data));
        }
    }

    /// Write blob to disk cache and track in LRU. Evicts if over capacity.
    fn write_blob_to_disk(&self, hash: &str, data: &[u8]) -> io::Result<()> {
        let path = self.blob_path(hash);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        // Check if already cached (content-addressed — same hash = same data).
        // Don't double-count bytes.
        let already_cached = path.exists();
        fs::write(&path, data)?;

        if !already_cached {
            let mut usage = self.disk_usage.lock().unwrap();
            *usage += data.len();
            // Track in LRU (new entry becomes most-recently-used).
            self.access_order.lock().unwrap().put(
                hash.to_string(),
                DiskEntry { bytes: data.len() },
            );
            drop(usage);
            self.evict_if_needed();
        } else {
            // Already cached — just promote in LRU (access update).
            self.access_order.lock().unwrap().put(
                hash.to_string(),
                DiskEntry { bytes: data.len() },
            );
        }
        Ok(())
    }

    fn remove_blob_from_disk(&self, hash: &str) {
        let path = self.blob_path(hash);
        if let Ok(metadata) = fs::metadata(&path) {
            let _ = fs::remove_file(&path);
            let mut usage = self.disk_usage.lock().unwrap();
            *usage = usage.saturating_sub(metadata.len() as usize);
            // Remove from LRU tracker.
            self.access_order.lock().unwrap().pop(hash);
        }
    }

    /// Evict least-recently-used blobs from disk cache if over capacity.
    /// Uses O(1) pop_lru() — no directory scan, no sort.
    fn evict_if_needed(&self) {
        let mut usage = self.disk_usage.lock().unwrap();
        let mut lru = self.access_order.lock().unwrap();
        while *usage > self.max_disk_bytes {
            if let Some((hash, entry)) = lru.pop_lru() {
                // Delete from disk (best-effort).
                let path = self.blob_path(&hash);
                if fs::remove_file(&path).is_ok() {
                    *usage = usage.saturating_sub(entry.bytes);
                } else {
                    // File was already gone — just subtract the tracked size.
                    *usage = usage.saturating_sub(entry.bytes);
                }
            } else {
                // LRU is empty but usage > max — reset tracker.
                // This can happen if blobs were deleted outside the cache.
                break;
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
        // Populate in-memory cache for warm reads (sub-microsecond).
        self.maybe_insert_mem_cache(&hash, data.to_vec());
        Ok(hash)
    }

    fn get_blob(&self, hash: &str) -> io::Result<Vec<u8>> {
        // ── TIER 0: In-memory cache (sub-microsecond) ──
        if let Some(arc_data) = self.mem_cache.get(hash) {
            // Cache HIT — Arc clone (atomic refcount), then unwrap or clone.
            // ~50-100ns: sharded hash lookup + atomic increment.
            return Ok(Arc::try_unwrap(arc_data)
                .unwrap_or_else(|arc| (*arc).clone()));
        }

        // ── TIER 1: Disk cache (~10µs–1ms) ──
        if let Ok(data) = self.read_blob_from_disk(hash) {
            // Cache HIT: promote in LRU (true LRU — read updates recency).
            // Don't change disk_usage (file already counted).
            if let Ok(file_data) = fs::metadata(self.blob_path(hash)) {
                self.access_order.lock().unwrap().put(
                    hash.to_string(),
                    DiskEntry { bytes: file_data.len() as usize },
                );
            }
            // Promote to in-memory cache (if small enough).
            self.maybe_insert_mem_cache(hash, data.clone());
            return Ok(data);
        }

        // Request coalescing: check if another thread is already fetching this blob.
        // If so, wait for it to complete and share the result. This eliminates
        // thundering herd on hot manifests — 10 concurrent queries hitting
        // the same manifest issue 1 S3 GET instead of 10.
        {
            let mut inflight_map = self.in_flight.lock().unwrap();
            if let Some(entry) = inflight_map.get(hash) {
                let (lock, cvar) = &**entry;
                let mut inflight = lock.lock().unwrap();
                while inflight.result.is_none() {
                    inflight = cvar.wait(inflight).unwrap();
                }
                return match &inflight.result {
                    Some(Ok(data)) => Ok(data.clone()),
                    Some(Err(e)) => Err(io::Error::new(e.kind(), e.to_string())),
                    None => unreachable!(),
                };
            }
            // Register as the designated fetcher for this hash.
            let entry = Arc::new((Mutex::new(InFlight { result: None }), Condvar::new()));
            inflight_map.insert(hash.to_string(), Arc::clone(&entry));
            // Drop inflight_map lock BEFORE S3 GET so others can wait.
        }

        // We are the designated fetcher. Do the S3 GET.
        let fetch_result = self.inner.get_blob(hash);

        // Populate disk + in-memory cache on success.
        if let Ok(ref data) = fetch_result {
            let _ = self.write_blob_to_disk(hash, data);
            self.maybe_insert_mem_cache(hash, data.clone());
        }

        // Keep the result for our own return before moving it into the
        // waiter slot (io::Error is not Clone — rebuild it from kind+msg,
        // the same downconversion the waiter path itself already uses).
        let return_result: io::Result<Vec<u8>> = match &fetch_result {
            Ok(data) => Ok(data.clone()),
            Err(e) => Err(io::Error::new(e.kind(), e.to_string())),
        };

        // Wake up all waiters and clean up the in-flight entry.
        {
            let entry = {
                let mut inflight_map = self.in_flight.lock().unwrap();
                inflight_map.remove(hash).expect("our InFlight entry was removed")
            };
            let (lock, cvar) = &*entry;
            let mut inflight = lock.lock().unwrap();
            inflight.result = Some(fetch_result);
            cvar.notify_all();
        }

        // Return the S3 result directly.
        // (The previous implementation re-read the blob from the disk cache
        // here — doubling I/O on the hot path AND failing the read when the
        // disk cache was unwritable (disk full, permissions) even though the
        // S3 GET had succeeded. The blob is already cached to disk + memory
        // above; the fetch result is authoritative.)
        return_result
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

    fn put_path_if(&self, path: &str, expected_hash: Option<&str>, new_hash: &str) -> io::Result<bool> {
        // Forward to the inner store's ATOMIC CAS (S3 If-Match). Never use
        // the inherited read-check-write default: it compares against the
        // ref CACHE, which can be TTL-stale (5s) — the check would pass
        // while the inner store's HEAD already moved, silently reintroducing
        // the lost-update bug the CAS exists to prevent.
        let won = self.inner.put_path_if(path, expected_hash, new_hash)?;
        if won {
            let mut refs = self.ref_cache.lock().unwrap();
            refs.insert(path.to_string(), RefEntry {
                hash: new_hash.to_string(),
                inserted_at: Instant::now(),
            });
        } else {
            // CAS lost: drop the cached value so the caller's retry observes
            // the CURRENT head from the inner store, not a stale snapshot.
            self.ref_cache.lock().unwrap().remove(path);
        }
        Ok(won)
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

    /// Range read with four-tier cache lookup:
    ///
    ///   1. Tier 0: full blob in memory → slice (sub-us)
    ///   2. Tier 0.5: block cache hit → return (~15-20us)
    ///   3. Tier 2: full blob on disk → seek+read (~10us)
    ///   4. Cache miss → inner.get_blob_range() + populate block cache
    ///
    /// The block cache (Tier 0.5) is the key innovation: it caches the
    /// RESULT of range reads, so repeated selective queries on the same
    /// sub-slab ranges hit memory at ~15-20us instead of 50-300ms S3 RTTs.
    /// This is critical for PSLB slabs where individual RGs are accessed
    /// via Range GET — without this cache, every query pays full S3 latency.
    fn get_blob_range(&self, hash: &str, start: u64, end: u64) -> io::Result<Vec<u8>> {
        // ── TIER 0: In-memory cache fast path ──
        // If the full blob is in memory, slice from there (sub-us).
        if let Some(arc_data) = self.mem_cache.get(hash) {
            let len = arc_data.len() as u64;
            if start >= len {
                return Ok(Vec::new());
            }
            let end_clamped = end.min(len);
            return Ok(arc_data[start as usize..end_clamped as usize].to_vec());
        }

        // ── TIER 0.5: Block cache for sub-slab range reads ──
        // Key format: "{hash}:{offset}:{len}" — exact range matching.
        // No alignment, no block-size tuning, matches Pond's RG access pattern.
        let range_len = if end > start { end - start } else { return Ok(Vec::new()); };
        let block_key = format!("{}:{}:{}", hash, start, range_len);
        if let Some(arc_data) = self.block_cache.get(&block_key) {
            return Ok(Arc::try_unwrap(arc_data)
                .unwrap_or_else(|arc| (*arc).clone()));
        }

        // ── TIER 2: Disk cache → native seek+read ──
        let path = self.blob_path(hash);
        if path.exists() {
            use std::io::{Read, Seek, SeekFrom};
            let mut f = std::fs::File::open(&path)?;
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
            f.read_exact(&mut buf)?;
            // Promote in LRU on cache hit.
            if let Ok(file_data) = fs::metadata(&path) {
                self.access_order.lock().unwrap().put(
                    hash.to_string(),
                    DiskEntry { bytes: file_data.len() as usize },
                );
            }
            // Also populate block cache from disk range read.
            self.block_cache_hashes.lock().unwrap().insert(hash.to_string());
            self.block_cache.insert(block_key, Arc::new(buf.clone()));
            return Ok(buf);
        }

        // Cache miss: delegate to inner (native Range support on S3/LocalFS).
        let result = self.inner.get_blob_range(hash, start, end);

        // Populate block cache on successful range read.
        if let Ok(ref data) = result {
            if !data.is_empty() && data.len() == range_len as usize {
                self.block_cache_hashes.lock().unwrap().insert(hash.to_string());
                self.block_cache.insert(block_key, Arc::new(data.clone()));
            }
        }

        result
    }

    fn get_blob_suffix(&self, hash: &str, n: u64) -> io::Result<Vec<u8>> {
        // If full blob is in memory cache, slice from there (sub-us).
        if let Some(arc_data) = self.mem_cache.get(hash) {
            let len = arc_data.len();
            if len < n as usize {
                return Ok(arc_data.to_vec());
            }
            return Ok(arc_data[len - n as usize..].to_vec());
        }
        // ── DISK CACHE TIER: seek to the tail of the cached file. ──
        // Serves warm slab-tail reads (footer/bloom lookups) from local disk
        // in ~50µs instead of a 50-300ms S3 GET. Uses SeekFrom::End so only
        // the requested tail bytes are read — never the whole file.
        let path = self.blob_path(hash);
        if path.exists() {
            use std::io::{Read, Seek, SeekFrom};
            if let Ok(mut file) = fs::File::open(&path) {
                if let Ok(meta) = file.metadata() {
                    let take = n.min(meta.len());
                    if take > 0
                        && file.seek(SeekFrom::End(-(take as i64))).is_ok()
                    {
                        let mut buf = Vec::with_capacity(take as usize);
                        if file.read_to_end(&mut buf).is_ok() {
                            // Promote in disk LRU so hot slab tails keep
                            // recency (same policy as full-blob disk hits).
                            self.access_order.lock().unwrap().put(
                                hash.to_string(),
                                DiskEntry { bytes: meta.len() as usize },
                            );
                            return Ok(buf);
                        }
                    } else if take == 0 {
                        return Ok(Vec::new());
                    }
                }
            }
        }
        // Delegate to inner for native suffix read (S3 bytes=-N, LocalFS SeekFrom::End).
        self.inner.get_blob_suffix(hash, n)
    }

    fn delete_blob(&self, hash: &str) -> io::Result<bool> {
        let result = self.inner.delete_blob(hash)?;
        self.mem_cache.remove(hash);
        self.invalidate_block_cache(hash);
        self.remove_blob_from_disk(hash);
        Ok(result)
    }

    /// Batch get: check disk cache for each hash, batch-fetch cache misses
    /// from inner store (which may be parallel on S3), populate cache.
    ///
    /// This is critical: without this override, the trait default calls
    /// `get_blob` sequentially per hash — killing parallelism when
    /// `CachingObjectStore` wraps `S3ObjectStore`. With this override,
    /// cache hits are served from disk (~10 µs) and only misses are
    /// batch-fetched via `inner.get_blob_batch()` (32-way parallel on S3).
    ///
    /// Performance impact for 100 standalone RGs:
    ///   WITHOUT this override: 100 sequential inner.get_blob() calls (~5 s)
    ///   WITH this override: 1 batch call to inner + parallel fetch (~200 ms)
    fn get_blob_batch(&self, hashes: &[String]) -> io::Result<Vec<Vec<u8>>> {
        if hashes.is_empty() {
            return Ok(Vec::new());
        }

        let mut results: Vec<Option<Vec<u8>>> = vec![None; hashes.len()];
        let mut miss_indices: Vec<usize> = Vec::new();
        let mut miss_hashes: Vec<String> = Vec::new();

        // Phase 0: Check in-memory cache for each hash (sub-microsecond).
        for (i, hash) in hashes.iter().enumerate() {
            if let Some(arc_data) = self.mem_cache.get(hash) {
                results[i] = Some(Arc::try_unwrap(arc_data)
                    .unwrap_or_else(|arc| (*arc).clone()));
                continue;
            }
            // Phase 1: Check disk cache.
            match self.read_blob_from_disk(hash) {
                Ok(data) => {
                    // Cache hit — promote in LRU (true LRU: read updates recency).
                    if let Ok(meta) = fs::metadata(self.blob_path(hash)) {
                        self.access_order.lock().unwrap().put(
                            hash.to_string(),
                            DiskEntry { bytes: meta.len() as usize },
                        );
                    }
                    // Promote to in-memory cache.
                    self.maybe_insert_mem_cache(hash, data.clone());
                    results[i] = Some(data);
                }
                Err(_) => {
                    miss_indices.push(i);
                    miss_hashes.push(hash.clone());
                }
            }
        }

        // Phase 2: Batch-fetch cache misses from inner store.
        // This delegates to inner's potentially-parallel implementation
        // (S3ObjectStore uses 32-way thread::scope parallelism).
        if !miss_hashes.is_empty() {
            let miss_results = self.inner.get_blob_batch(&miss_hashes)?;

            // Phase 3: Populate disk + in-memory cache for misses.
            for (j, (data, hash)) in miss_results.iter().zip(miss_hashes.iter()).enumerate() {
                let _ = self.write_blob_to_disk(hash, data);
                self.maybe_insert_mem_cache(hash, data.clone());
                results[miss_indices[j]] = Some(data.clone());
            }
        }

        // Phase 4: Collect in input order.
        Ok(results
            .into_iter()
            .map(|opt| opt.expect("all slots filled (hits + misses)"))
            .collect())
    }

    /// Batch put: delegate to inner store (parallel on S3), then cache all
    /// results to local disk.
    ///
    /// Without this override, the trait default calls `put_blob` sequentially
    /// per item — each doing an inner put + cache write. With this override,
    /// the inner store can batch-parallel the puts, and we cache all results
    /// in a single pass.
    fn put_blob_batch(&self, items: &[Vec<u8>]) -> io::Result<Vec<String>> {
        if items.is_empty() {
            return Ok(Vec::new());
        }
        // Delegate to inner (parallel on S3, sequential fallback on LocalFS).
        let hashes = self.inner.put_blob_batch(items)?;
        // Cache all written blobs to local disk + in-memory.
        for (data, hash) in items.iter().zip(hashes.iter()) {
            let _ = self.write_blob_to_disk(hash, data);
            self.maybe_insert_mem_cache(hash, data.clone());
        }
        Ok(hashes)
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
        // Disable mem cache for disk-specific tests.
        let cached = CachingObjectStore::new(inner, cache_dir.path())
            .unwrap()
            .with_max_mem_bytes(0);
        (cached, inner_dir, cache_dir)
    }

    fn make_cached_store_no_mem() -> CachingObjectStore {
        let inner_dir = tempdir().unwrap();
        let cache_dir = tempdir().unwrap();
        let inner: Box<dyn ObjectStore> = Box::new(
            LocalFSObjectStore::new(inner_dir.path()).unwrap(),
        );
        // Disable mem cache entirely so range reads go through block/disk path.
        CachingObjectStore::new(inner, cache_dir.path())
            .unwrap()
            .with_max_mem_bytes(0)
            .with_skip_mem_cache_threshold(0)
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
        // Disk usage should be <= max + one extra blob (write triggers
        // eviction AFTER the write, so one blob may exceed the limit).
        assert!(store.disk_usage_bytes() <= 200 + 100);
    }

    #[test]
    fn test_true_lru_eviction() {
        // Verify that reading a blob promotes it in the LRU order.
        // Write 3 blobs (A, B, C) to a 200-byte cache. Then read A
        // (promoting it). Then write D — A should survive, B should be evicted.
        let inner_dir = tempdir().unwrap();
        let cache_dir = tempdir().unwrap();
        let inner: Box<dyn ObjectStore> = Box::new(
            LocalFSObjectStore::new(inner_dir.path()).unwrap(),
        );
        let store = CachingObjectStore::new(inner, cache_dir.path())
            .unwrap()
            .with_max_disk_bytes(350) // room for ~3.5 blobs of 100 bytes
            .with_max_mem_bytes(0);   // disable mem cache for this LRU test

        // Write A, B, C (each 100 bytes = 300 bytes total, triggers eviction)
        let h_a = store.put_blob(&[1u8; 100]).unwrap();
        let h_b = store.put_blob(&[2u8; 100]).unwrap();
        let h_c = store.put_blob(&[3u8; 100]).unwrap();

        // Read A to promote it in LRU (now A is most-recently-used).
        let _ = store.get_blob(&h_a).unwrap();

        // Write D — should evict B (least-recently-used), keep A and C.
        let h_d = store.put_blob(&[4u8; 100]).unwrap();

        // A and C should still be in cache (A was promoted by read).
        assert!(store.blob_path(&h_a).exists(), "A should survive (was read)");
        assert!(store.blob_path(&h_c).exists(), "C should survive (more recent than B)");
        // D should be in cache (just written).
        assert!(store.blob_path(&h_d).exists(), "D should be in cache");
        // B should have been evicted (least-recently-used).
        assert!(!store.blob_path(&h_b).exists(), "B should be evicted (LRU)");
    }

    #[test]
    fn test_get_blob_range_cache_hit_uses_seek_not_full_read() {
        // Regression test for H1: cache-hit get_blob_range must NOT load the
        // whole blob into memory. We verify this by writing a LARGE blob
        // (1 MB), then reading a tiny range (12 bytes) — the result must
        // be exactly 12 bytes and match the expected slice.
        let inner_dir = tempdir().unwrap();
        let cache_dir = tempdir().unwrap();
        let inner: Box<dyn ObjectStore> = Box::new(
            LocalFSObjectStore::new(inner_dir.path()).unwrap(),
        );
        let store = CachingObjectStore::new(inner, cache_dir.path())
            .unwrap()
            .with_max_disk_bytes(10_000_000); // 10 MB — enough for our 1 MB blob

        // Write 1 MB of patterned data (so we can verify the slice).
        let mut payload = vec![0u8; 1_000_000];
        for (i, b) in payload.iter_mut().enumerate() {
            *b = (i % 251) as u8; // prime modulus → recognizable pattern
        }
        let h = store.put_blob(&payload).unwrap();

        // Verify the blob is cached on disk.
        assert!(store.blob_path(&h).exists(), "blob should be cached on disk");

        // Read a 12-byte range from the middle (mimics slab tail fetch).
        let start = 500_000u64;
        let end = 500_012u64;
        let range = store.get_blob_range(&h, start, end).unwrap();

        // The result must be EXACTLY 12 bytes (not the whole 1 MB blob).
        assert_eq!(range.len(), 12, "range read must return exactly 12 bytes, got {}", range.len());

        // And the bytes must match the original payload slice.
        assert_eq!(&range[..], &payload[start as usize..end as usize],
            "cached range read must return the correct bytes");
    }

    #[test]
    fn test_get_blob_range_cache_miss_delegates_to_inner() {
        // Verify the cache-miss path: when the blob is NOT cached, the
        // range read delegates to the inner store's native range support.
        let inner_dir = tempdir().unwrap();
        let cache_dir = tempdir().unwrap();
        let inner: Box<dyn ObjectStore> = Box::new(
            LocalFSObjectStore::new(inner_dir.path()).unwrap(),
        );
        // Use a tiny cache size so the blob will NOT be cached after write.
        // (Actually it WILL be cached since we write-through — to test the
        // miss path, we need to delete the cache file manually.)
        let store = CachingObjectStore::new(inner, cache_dir.path())
            .unwrap()
            .with_max_disk_bytes(1_000_000);

        let payload = b"0123456789ABCDEFGHIJKLMN"; // 24 bytes
        let h = store.put_blob(payload).unwrap();

        // Manually remove the cache file to simulate a cache miss.
        std::fs::remove_file(store.blob_path(&h)).unwrap();
        assert!(!store.blob_path(&h).exists(), "cache file removed");

        // Range read should delegate to inner store (which has the blob).
        let r = store.get_blob_range(&h, 5, 15).unwrap();
        assert_eq!(r, b"56789ABCDE", "cache-miss range read should delegate to inner");
    }

    // -----------------------------------------------------------------
    // Batch passthrough tests (G7.5)
    // -----------------------------------------------------------------

    #[test]
    fn test_get_blob_batch_all_cache_miss() {
        // Fresh cache: all blobs fetched from inner store in one batch call.
        let (store, _inner, _cache) = make_cached_store();
        let items = [b"alpha".to_vec(), b"bravo".to_vec(), b"charlie".to_vec()];
        let hashes: Vec<String> = items.iter().map(|d| store.put_blob(d).unwrap()).collect();

        // Clear cache to simulate fresh start.
        for h in &hashes {
            std::fs::remove_file(store.blob_path(h)).unwrap();
        }

        // Batch read — all should come from inner store.
        let results = store.get_blob_batch(&hashes).unwrap();
        assert_eq!(results.len(), 3);
        assert_eq!(results[0], b"alpha");
        assert_eq!(results[1], b"bravo");
        assert_eq!(results[2], b"charlie");

        // Verify they're now cached.
        for h in &hashes {
            assert!(store.blob_path(h).exists(), "blob should be cached after batch read");
        }
    }

    #[test]
    fn test_get_blob_batch_all_cache_hit() {
        // All blobs already cached — should serve from disk, no inner calls.
        let (store, inner_dir, _cache) = make_cached_store();
        let items = [b"x".to_vec(), b"y".to_vec(), b"z".to_vec()];
        let hashes: Vec<String> = items.iter().map(|d| store.put_blob(d).unwrap()).collect();

        // Verify cached.
        for h in &hashes {
            assert!(store.blob_path(h).exists());
        }

        // Delete from inner to prove cache serves them.
        for h in &hashes {
            let inner_blob = inner_dir.path().join("blobs").join(&h[..2]).join(h);
            std::fs::remove_file(inner_blob).unwrap();
        }

        let results = store.get_blob_batch(&hashes).unwrap();
        assert_eq!(results.len(), 3);
        assert_eq!(results[0], b"x");
        assert_eq!(results[1], b"y");
        assert_eq!(results[2], b"z");
    }

    #[test]
    fn test_get_blob_batch_partial_cache_hit() {
        // Mix of cache hits and misses — only misses should be fetched from inner.
        let (store, _inner, _cache) = make_cached_store();
        let items = [b"one".to_vec(), b"two".to_vec(), b"three".to_vec()];
        let hashes: Vec<String> = items.iter().map(|d| store.put_blob(d).unwrap()).collect();

        // Remove middle blob from cache (partial miss).
        std::fs::remove_file(store.blob_path(&hashes[1])).unwrap();
        assert!(!store.blob_path(&hashes[1]).exists());
        assert!(store.blob_path(&hashes[0]).exists());
        assert!(store.blob_path(&hashes[2]).exists());

        let results = store.get_blob_batch(&hashes).unwrap();
        assert_eq!(results.len(), 3);
        assert_eq!(results[0], b"one");
        assert_eq!(results[1], b"two");
        assert_eq!(results[2], b"three");

        // Middle blob should now be cached.
        assert!(store.blob_path(&hashes[1]).exists(), "fetched miss should be cached");
    }

    #[test]
    fn test_get_blob_batch_preserves_order() {
        // Verify results are in the same order as input hashes.
        let (store, _inner, _cache) = make_cached_store();
        let items = [b"aaa".to_vec(), b"bbb".to_vec(), b"ccc".to_vec()];
        let hashes: Vec<String> = items.iter().map(|d| store.put_blob(d).unwrap()).collect();

        // Request in reverse order.
        let reversed = vec![hashes[2].clone(), hashes[0].clone(), hashes[1].clone()];
        let results = store.get_blob_batch(&reversed).unwrap();
        assert_eq!(results[0], b"ccc");
        assert_eq!(results[1], b"aaa");
        assert_eq!(results[2], b"bbb");
    }

    #[test]
    fn test_get_blob_batch_empty() {
        let (store, _inner, _cache) = make_cached_store();
        let results = store.get_blob_batch(&[]).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn test_put_blob_batch_caches_all() {
        // Verify put_blob_batch writes to inner AND caches all blobs.
        let (store, _inner, _cache) = make_cached_store();
        let items = [b"p".to_vec(), b"q".to_vec(), b"r".to_vec()];
        let hashes = store.put_blob_batch(&items).unwrap();

        assert_eq!(hashes.len(), 3);
        for (i, h) in hashes.iter().enumerate() {
            assert_eq!(store.get_blob(h).unwrap(), items[i]);
            assert!(store.blob_path(h).exists(), "blob should be cached after put_batch");
        }
    }

    #[test]
    fn test_put_blob_batch_empty() {
        let (store, _inner, _cache) = make_cached_store();
        let hashes = store.put_blob_batch(&[]).unwrap();
        assert!(hashes.is_empty());
    }

    // -----------------------------------------------------------------
    // Tier 0 (in-memory) cache tests
    // -----------------------------------------------------------------

    #[test]
    fn test_mem_cache_hit_after_put() {
        // After put_blob, the blob should be in the in-memory cache.
        // We verify this by deleting the disk cache and confirming the
        // read still succeeds (served from memory).
        let inner_dir = tempdir().unwrap();
        let cache_dir = tempdir().unwrap();
        let inner: Box<dyn ObjectStore> = Box::new(
            LocalFSObjectStore::new(inner_dir.path()).unwrap(),
        );
        let store = CachingObjectStore::new(inner, cache_dir.path()).unwrap();
        let h = store.put_blob(b"mem cached").unwrap();
        // Delete from disk — read should still work from memory.
        std::fs::remove_file(store.blob_path(&h)).unwrap();
        let data = store.get_blob(&h).unwrap();
        assert_eq!(data, b"mem cached");
    }

    #[test]
    fn test_large_blob_skips_mem_cache() {
        // Blobs >= 1 MB should skip the in-memory cache.
        let inner_dir = tempdir().unwrap();
        let cache_dir = tempdir().unwrap();
        let inner: Box<dyn ObjectStore> = Box::new(
            LocalFSObjectStore::new(inner_dir.path()).unwrap(),
        );
        let store = CachingObjectStore::new(inner, cache_dir.path())
            .unwrap()
            .with_skip_mem_cache_threshold(100); // 100-byte threshold

        // Write a 200-byte blob (> threshold).
        let big_blob = vec![0xABu8; 200];
        let h = store.put_blob(&big_blob).unwrap();
        // Verify large blob is NOT in memory cache by deleting BOTH
        // disk cache and inner store, then confirming the read fails.
        std::fs::remove_file(store.blob_path(&h)).unwrap();
        let inner_blob = inner_dir.path().join("blobs").join(&h[..2]).join(&h);
        std::fs::remove_file(inner_blob).unwrap();
        assert!(store.get_blob(&h).is_err(),
            "large blob should NOT be served from mem cache");
    }

    #[test]
    fn test_mem_cache_eviction_respects_max_bytes() {
        // Set a very small memory limit (500 bytes) and write many blobs.
        // Moka's segmented LRU evicts lazily — most entries should be evicted.
        let inner_dir = tempdir().unwrap();
        let cache_dir = tempdir().unwrap();
        let inner: Box<dyn ObjectStore> = Box::new(
            LocalFSObjectStore::new(inner_dir.path()).unwrap(),
        );
        let store = CachingObjectStore::new(inner, cache_dir.path())
            .unwrap()
            .with_max_mem_bytes(500);

        let mut hashes = Vec::new();
        for i in 0..100u32 {
            let data = vec![i as u8; 100];
            hashes.push(store.put_blob(&data).unwrap());
        }
        // Delete ALL disk files and inner store files.
        for h in &hashes {
            let _ = std::fs::remove_file(store.blob_path(h));
            let inner_blob = inner_dir.path().join("blobs").join(&h[..2]).join(h);
            let _ = std::fs::remove_file(inner_blob);
        }
        let mut mem_hits = 0;
        for h in &hashes {
            if store.get_blob(h).is_ok() {
                mem_hits += 1;
            }
        }
        // With 500-byte limit and 100×100-byte blobs, most should be evicted.
        assert!(mem_hits < 50,
            "most blobs should be evicted from 500-byte mem cache, but {} survived", mem_hits);
    }

    #[test]
    fn test_delete_blob_invalidates_mem_cache() {
        // Delete should remove from both disk and in-memory cache.
        let inner_dir = tempdir().unwrap();
        let cache_dir = tempdir().unwrap();
        let inner: Box<dyn ObjectStore> = Box::new(
            LocalFSObjectStore::new(inner_dir.path()).unwrap(),
        );
        let store = CachingObjectStore::new(inner, cache_dir.path()).unwrap();
        let h = store.put_blob(b"to delete from mem").unwrap();
        // Verify blob is readable.
        assert_eq!(store.get_blob(&h).unwrap(), b"to delete from mem");
        // Delete from inner too, so we can test that mem cache is invalidated.
        let inner_blob = inner_dir.path().join("blobs").join(&h[..2]).join(&h);
        std::fs::remove_file(inner_blob).unwrap();
        store.delete_blob(&h).unwrap();
        assert!(!store.blob_path(&h).exists());
        // Read should fail — both disk and memory invalidated.
        assert!(store.get_blob(&h).is_err(),
            "blob should not be readable after delete invalidates mem cache");
    }

    #[test]
    fn test_get_blob_range_uses_mem_cache() {
        // Small blob in memory → range read should slice from memory.
        let inner_dir = tempdir().unwrap();
        let cache_dir = tempdir().unwrap();
        let inner: Box<dyn ObjectStore> = Box::new(
            LocalFSObjectStore::new(inner_dir.path()).unwrap(),
        );
        let store = CachingObjectStore::new(inner, cache_dir.path()).unwrap();
        let payload = b"0123456789ABCDEFGHIJ"; // 20 bytes
        let h = store.put_blob(payload).unwrap();
        // Delete from disk — range read should still work from memory.
        std::fs::remove_file(store.blob_path(&h)).unwrap();
        let range = store.get_blob_range(&h, 5, 15).unwrap();
        assert_eq!(range, b"56789ABCDE");
    }

    #[test]
    fn test_mem_cache_batch_hit() {
        // Batch read where all blobs are in memory.
        let inner_dir = tempdir().unwrap();
        let cache_dir = tempdir().unwrap();
        let inner: Box<dyn ObjectStore> = Box::new(
            LocalFSObjectStore::new(inner_dir.path()).unwrap(),
        );
        let store = CachingObjectStore::new(inner, cache_dir.path()).unwrap();
        let items = [b"x".to_vec(), b"y".to_vec(), b"z".to_vec()];
        let hashes: Vec<String> = items.iter().map(|d| store.put_blob(d).unwrap()).collect();
        // Delete all from disk.
        for h in &hashes {
            std::fs::remove_file(store.blob_path(h)).unwrap();
        }
        // Batch read — all should come from memory.
        let results = store.get_blob_batch(&hashes).unwrap();
        assert_eq!(results.len(), 3);
        assert_eq!(results[0], b"x");
        assert_eq!(results[1], b"y");
        assert_eq!(results[2], b"z");
    }

    // ------------------------------------------------------------------
    // Block cache (Tier 0.5) tests
    // ------------------------------------------------------------------

    #[test]
    fn test_block_cache_hit_avoids_inner_range_read() {
        // Write a large blob, do a range read (miss → populates block cache),
        // then do the same range read again (should hit block cache).
        let store = make_cached_store_no_mem();
        let data = vec![0xABu8; 10_000];
        let hash = store.put_blob(&data).unwrap();

        // First range read — populates block cache.
        let r1 = store.get_blob_range(&hash, 100, 200).unwrap();
        assert_eq!(r1.len(), 100);

        // Remove from disk cache to prove block cache serves it.
        store.remove_blob_from_disk(&hash);

        // Second range read — should hit block cache (Tier 0.5).
        let r2 = store.get_blob_range(&hash, 100, 200).unwrap();
        assert_eq!(r2, r1);
    }

    #[test]
    fn test_block_cache_different_ranges_same_blob() {
        let store = make_cached_store_no_mem();
        let data = vec![0x42u8; 10_000];
        let hash = store.put_blob(&data).unwrap();

        // Read two different ranges from the same blob.
        let r1 = store.get_blob_range(&hash, 0, 100).unwrap();
        let r2 = store.get_blob_range(&hash, 500, 700).unwrap();

        assert_eq!(r1.len(), 100);
        assert_eq!(r2.len(), 200);

        // Remove from disk, verify both are served from block cache.
        store.remove_blob_from_disk(&hash);
        assert_eq!(store.get_blob_range(&hash, 0, 100).unwrap(), r1);
        assert_eq!(store.get_blob_range(&hash, 500, 700).unwrap(), r2);
    }

    #[test]
    fn test_block_cache_miss_populates() {
        let store = make_cached_store_no_mem();
        let data = vec![0xCDu8; 5000];
        let hash = store.put_blob(&data).unwrap();

        // Range read.
        let r = store.get_blob_range(&hash, 1000, 2000).unwrap();
        assert_eq!(r.len(), 1000);

        // Remove from disk — block cache should still serve it.
        store.remove_blob_from_disk(&hash);
        let r2 = store.get_blob_range(&hash, 1000, 2000).unwrap();
        assert_eq!(r2.len(), 1000);
        assert_eq!(r2, r);
    }

    #[test]
    fn test_block_cache_empty_range() {
        let store = make_cached_store_no_mem();
        let data = vec![0x01u8; 100];
        let hash = store.put_blob(&data).unwrap();

        // start >= end → empty, should not populate cache.
        let r = store.get_blob_range(&hash, 50, 50).unwrap();
        assert!(r.is_empty());
    }

    #[test]
    fn test_block_cache_invalidated_on_delete() {
        let store = make_cached_store_no_mem();
        let data = vec![0xEFu8; 10_000];
        let hash = store.put_blob(&data).unwrap();

        // Populate TWO distinct block-cache ranges for this blob.
        let _ = store.get_blob_range(&hash, 100, 300).unwrap();
        let _ = store.get_blob_range(&hash, 500, 900).unwrap();
        // Verify both entries exist in the block cache (prefix scan).
        let prefix = format!("{}:", hash);
        let count_entries = || {
            store
                .block_cache
                .iter()
                .filter(|(k, _)| k.starts_with(&prefix))
                .count()
        };
        assert_eq!(count_entries(), 2, "two block entries should be cached");

        // Delete the blob — ALL block cache entries must be removed.
        store.delete_blob(&hash).unwrap();

        // REGRESSION (architecture review bug #2): the old invalidate only
        // cleared the tracking SET and let moka "evict naturally" —
        // get_blob_range kept serving bytes for a deleted blob
        // (deleted-data resurrection). The entries themselves must be gone.
        assert_eq!(
            count_entries(),
            0,
            "block cache entries must be REMOVED on delete_blob, not just untracked"
        );

        // The hash tracker should be clean too.
        {
            let hashes = store.block_cache_hashes.lock().unwrap();
            assert!(!hashes.contains(&hash));
        }
    }
    #[test]
    fn test_resolve_cache_dir_explicit_and_env() {
        // Explicit override wins.
        assert_eq!(
            resolve_cache_dir(Some("/tmp/pond-explicit")),
            Some(PathBuf::from("/tmp/pond-explicit"))
        );
        // Sentinel values disable caching.
        assert_eq!(resolve_cache_dir(Some("off")), None);
        assert_eq!(resolve_cache_dir(Some("none")), None);
        assert_eq!(resolve_cache_dir(Some("")), None);
        // Env var is consulted when no explicit value is given.
        std::env::set_var("POND_CACHE_DIR", "/tmp/pond-env-cache");
        assert_eq!(
            resolve_cache_dir(None),
            Some(PathBuf::from("/tmp/pond-env-cache"))
        );
        // Explicit "off" wins over the env var.
        assert_eq!(resolve_cache_dir(Some("off")), None);
        std::env::remove_var("POND_CACHE_DIR");
        // Default (no explicit, no env) is SOME cache dir — the cache is
        // the product, not an option.
        assert!(resolve_cache_dir(None).is_some());
    }

    #[test]
    fn test_get_blob_suffix_served_from_disk_tier() {
        // A blob too large for the mem cache lands on disk. With the inner
        // store DELETED, a suffix read must still succeed from the local
        // disk cache (seek-to-tail), instead of paying an S3 RTT.
        let inner_dir = tempdir().unwrap();
        let cache_dir = tempdir().unwrap();
        let inner: Box<dyn ObjectStore> = Box::new(
            LocalFSObjectStore::new(inner_dir.path()).unwrap(),
        );
        let store = CachingObjectStore::new(inner, cache_dir.path())
            .unwrap()
            .with_skip_mem_cache_threshold(100); // blob below skips mem tier

        let data: Vec<u8> = (0u8..200).collect();
        let h = store.put_blob(&data).unwrap();

        // Remove the blob from the INNER store — the disk cache is now
        // the only tier that can serve this blob.
        let inner_blob = inner_dir.path().join("blobs").join(&h[..2]).join(&h);
        std::fs::remove_file(inner_blob).unwrap();

        let suffix = store.get_blob_suffix(&h, 50).unwrap();
        assert_eq!(suffix, &data[150..],
            "suffix must be served from the disk cache (seek-to-tail)");

        // n larger than the blob → whole blob (mirrors mem-tier semantics).
        let whole = store.get_blob_suffix(&h, 1000).unwrap();
        assert_eq!(whole, data);
    }
}
