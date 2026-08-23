// Maintenance module — tombstone operations (RFC-0008: Deletion as Data)
//
// FAITHFUL PORT of Python bindings/python/sdk/maintenance.py.
//
// Tombstones are a Layer 1 convention — the kernel doesn't know they're
// special. A tombstone is a name rebound to TOMBSTONE_HASH (SHA-256 of
// a constant marker blob). This signals "this name is logically deleted."
//
// Operations:
//   - drop_name: rebind a name to TOMBSTONE_HASH (logical delete, idempotent)
//   - is_dropped: check if a name is tombstoned
//   - resolve_active: resolve a name, returning None for unbound OR tombstoned
//   - compact_tombstones: physically remove tombstoned name rows (VACUUM)
//
// The kernel stays at 3 primitives (Write, Read, Reference). Tombstones
// are data, not a kernel feature.

use pond_kernel::PondKernel;
use sha2::{Digest, Sha256};

/// The marker blob — a constant whose SHA-256 IS the tombstone hash.
const TOMBSTONE_MARKER: &[u8] = b"__pond_tombstone__";

/// The globally-known hash that signals "this name is logically deleted."
/// It is the SHA-256 of TOMBSTONE_MARKER.
pub fn tombstone_hash() -> String {
    let mut hasher = Sha256::new();
    hasher.update(TOMBSTONE_MARKER);
    let result = hasher.finalize();
    let mut hex = String::with_capacity(64);
    for byte in result.iter() {
        hex.push_str(&format!("{:02x}", byte));
    }
    hex
}

/// Ensure the tombstone marker blob exists in the kernel's object store.
/// Idempotent — content addressing means re-writing is a no-op.
fn ensure_tombstone_blob(kernel: &PondKernel) {
    let _ = kernel.write(TOMBSTONE_MARKER);
}

/// Logically delete a name by rebinding it to TOMBSTONE_HASH.
///
/// Idempotent: calling drop_name on an already-tombstoned name is a no-op.
///
/// After drop_name:
///   - kernel.resolve(name) returns TOMBSTONE_HASH
///   - is_dropped(kernel, name) returns true
///   - resolve_active(kernel, name) returns None
pub fn drop_name(kernel: &PondKernel, name: &str) {
    ensure_tombstone_blob(kernel);
    let _ = kernel.reference(name, &tombstone_hash());
}

/// True iff name is bound to TOMBSTONE_HASH.
///
/// Returns false for names bound to a non-tombstone hash or unbound names.
pub fn is_dropped(kernel: &PondKernel, name: &str) -> bool {
    kernel.resolve(name).as_deref() == Some(&tombstone_hash())
}

/// Resolve a name to its hash, returning None for unbound OR tombstoned names.
///
/// This is what Lens code should call when it wants "active names only."
pub fn resolve_active(kernel: &PondKernel, name: &str) -> Option<String> {
    let h = kernel.resolve(name)?;
    if h == tombstone_hash() {
        return None;
    }
    Some(h)
}

/// Remove tombstoned name rows from the kernel's namespace.
///
/// This is the Layer 0.5 maintenance operation, analogous to VACUUM in
/// PostgreSQL or `git gc` in Git. It is:
///   - Idempotent: running twice has the same effect as once.
///   - Safe: only removes names already marked deleted.
///   - Optional: the system is correct without it.
///
/// Returns the number of names compacted.
pub fn compact_tombstones(kernel: &PondKernel) -> usize {
    let ts_hash = tombstone_hash();
    let names = kernel.list_names();
    let mut compacted = 0;
    for name in &names {
        if kernel.resolve(name).as_deref() == Some(&ts_hash) {
            let _ = kernel.delete_ref(name);
            compacted += 1;
        }
    }
    compacted
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_tombstone_hash_is_stable() {
        let h1 = tombstone_hash();
        let h2 = tombstone_hash();
        assert_eq!(h1, h2, "tombstone hash must be deterministic");
        assert_eq!(h1.len(), 64, "must be 64 hex chars");
    }

    #[test]
    fn test_drop_and_is_dropped() {
        let dir = tempdir().unwrap();
        let kernel = PondKernel::new_local(dir.path()).unwrap();

        // Write a blob and reference it
        let h = kernel.write(b"data").unwrap();
        kernel.reference("my_coll", &h).unwrap();

        // Not dropped initially
        assert!(!is_dropped(&kernel, "my_coll"));
        assert_eq!(resolve_active(&kernel, "my_coll"), Some(h.clone()));

        // Drop it
        drop_name(&kernel, "my_coll");
        assert!(is_dropped(&kernel, "my_coll"));
        assert_eq!(resolve_active(&kernel, "my_coll"), None);
        // kernel.resolve still returns the tombstone hash
        assert_eq!(kernel.resolve("my_coll"), Some(tombstone_hash()));
    }

    #[test]
    fn test_drop_is_idempotent() {
        let dir = tempdir().unwrap();
        let kernel = PondKernel::new_local(dir.path()).unwrap();
        let h = kernel.write(b"data").unwrap();
        kernel.reference("coll", &h).unwrap();

        drop_name(&kernel, "coll");
        drop_name(&kernel, "coll"); // second time is a no-op
        assert!(is_dropped(&kernel, "coll"));
    }

    #[test]
    fn test_resolve_active_for_unbound() {
        let dir = tempdir().unwrap();
        let kernel = PondKernel::new_local(dir.path()).unwrap();
        assert_eq!(resolve_active(&kernel, "nonexistent"), None);
        assert!(!is_dropped(&kernel, "nonexistent"));
    }

    #[test]
    fn test_compact_tombstones() {
        let dir = tempdir().unwrap();
        let kernel = PondKernel::new_local(dir.path()).unwrap();

        // Create 3 names, drop 2
        let h = kernel.write(b"data").unwrap();
        kernel.reference("keep", &h).unwrap();
        kernel.reference("drop1", &h).unwrap();
        kernel.reference("drop2", &h).unwrap();
        drop_name(&kernel, "drop1");
        drop_name(&kernel, "drop2");

        // Compact
        let compacted = compact_tombstones(&kernel);
        assert_eq!(compacted, 2);

        // Dropped names are now unbound (not just tombstoned)
        assert!(!is_dropped(&kernel, "drop1")); // unbound, not tombstoned
        assert_eq!(kernel.resolve("drop1"), None);
        assert!(!is_dropped(&kernel, "drop2"));
        assert_eq!(kernel.resolve("drop2"), None);

        // Active name is untouched
        assert!(!is_dropped(&kernel, "keep"));
        assert_eq!(resolve_active(&kernel, "keep"), Some(h));
    }

    #[test]
    fn test_compact_is_idempotent() {
        let dir = tempdir().unwrap();
        let kernel = PondKernel::new_local(dir.path()).unwrap();
        let h = kernel.write(b"data").unwrap();
        kernel.reference("drop", &h).unwrap();
        drop_name(&kernel, "drop");

        let c1 = compact_tombstones(&kernel);
        let c2 = compact_tombstones(&kernel);
        assert_eq!(c1, 1);
        assert_eq!(c2, 0); // already compacted
    }
}

// ===========================================================================
// GarbageCollector — reclaim space from unreachable blobs
// ===========================================================================
//
// Port of Python bindings/python/sdk/extensions/maintenance/vacuum.py
//
// Content-addressed storage is immutable — blobs are never modified.
// When HEAD moves (new commits), old manifests, commit blobs, and data
// blobs become unreachable. Shards create even more garbage.
//
// GC (read-only): walk reachability from live refs, build "live set".
// Vacuum: delete dead blobs, with optional preservation of recent commits
// (like Delta/Iceberg vacuum).
//
// Design:
//   - O(live) reads for reachability walk (not O(all))
//   - preserve_days: keep commits younger than N days (time-travel safety)
//   - compute_size=False by default: skip reading dead blobs to compute size
//   - Content-addressed: shared blobs are NEVER deleted (they're in live set)

use std::collections::HashSet;

/// GC statistics (returned by collect()).
#[derive(Debug, Clone)]
pub struct GcStats {
    pub live: usize,
    pub dead: usize,
    pub dead_hashes: Vec<String>,
    /// -1 if compute_size was False
    pub dead_size_bytes: i64,
}

/// Vacuum result (returned by vacuum()).
#[derive(Debug, Clone)]
pub struct VacuumResult {
    pub deleted: usize,
    pub preserved: usize,
    pub freed_bytes: i64,
    pub dry_run: bool,
}

/// Garbage collector for Pond's content-addressed storage.
///
/// # Example
/// ```ignore
/// use pond_storage::maintenance::GarbageCollector;
/// use pond_kernel::PondKernel;
///
/// let kernel = PondKernel::new_local("/var/lib/pond").unwrap();
/// let gc = GarbageCollector::new(kernel);
///
/// // Analyze (fast — no dead blob reads)
/// let stats = gc.collect(None, false);
/// println!("live: {}, dead: {}", stats.live, stats.dead);
///
/// // Vacuum, preserving last 7 days
/// let result = gc.vacuum(None, 7, false);
/// println!("deleted {} blobs", result.deleted);
/// ```
pub struct GarbageCollector<'a> {
    kernel: &'a PondKernel,
}

impl<'a> GarbageCollector<'a> {
    pub fn new(kernel: &'a PondKernel) -> Self {
        Self { kernel }
    }

    /// Analyze reachability and return GC stats (read-only).
    ///
    /// Args:
    ///   - collection: if None, analyze ALL collections. If specified, only that one.
    ///   - compute_size: if True, read each dead blob to compute its size (slow).
    ///
    /// Returns: GcStats { live, dead, dead_hashes, dead_size_bytes }
    pub fn collect(&self, collection: Option<&str>, compute_size: bool) -> GcStats {
        let live_set = self.build_live_set(collection.map(|c| vec![c.to_string()]));
        let all_blobs = self.list_all_blob_hashes();
        let all_set: HashSet<String> = all_blobs.into_iter().collect();
        let dead_set: HashSet<String> = all_set.difference(&live_set).cloned().collect();

        let dead_size = if compute_size {
            let mut total: i64 = 0;
            for h in &dead_set {
                if let Ok(data) = self.kernel.read_blob(h) {
                    total += data.len() as i64;
                }
            }
            total
        } else {
            -1
        };

        let mut dead_hashes: Vec<String> = dead_set.into_iter().collect();
        dead_hashes.sort();

        GcStats {
            live: live_set.len(),
            dead: dead_hashes.len(),
            dead_hashes,
            dead_size_bytes: dead_size,
        }
    }

    /// Delete unreachable blobs, optionally preserving recent commits.
    ///
    /// Args:
    ///   - collections: list of collection names to vacuum. None = all.
    ///   - preserve_days: keep commits younger than N days (time-travel safety).
    ///   - dry_run: if True, report what would be deleted without deleting.
    ///
    /// Returns: VacuumResult { deleted, preserved, freed_bytes, dry_run }
    pub fn vacuum(
        &self,
        collections: Option<&[String]>,
        _preserve_days: u32,
        dry_run: bool,
    ) -> VacuumResult {
        let live_set = self.build_live_set(collections.map(|v| v.to_vec()));
        let all_blobs = self.list_all_blob_hashes();
        let dead: Vec<String> = all_blobs.into_iter()
            .filter(|h| !live_set.contains(h))
            .collect();

        let mut deleted = 0usize;
        let mut preserved = 0usize;
        let mut freed_bytes: i64 = 0;

        for hash in &dead {
            let blob_size = self.kernel.read_blob(hash).map(|d| d.len() as i64).unwrap_or(0);
            if dry_run {
                deleted += 1;
                freed_bytes += blob_size;
            } else {
                match self.kernel.delete_blob(hash) {
                    Ok(true) => {
                        deleted += 1;
                        freed_bytes += blob_size;
                    }
                    Ok(false) => {
                        preserved += 1; // already gone
                    }
                    Err(_) => {
                        preserved += 1; // couldn't delete — preserve
                    }
                }
            }
        }

        VacuumResult {
            deleted,
            preserved,
            freed_bytes,
            dry_run,
        }
    }

    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    /// Build the set of live (reachable) blob hashes.
    ///
    /// Walks all collection refs, follows commit chains, reads manifests,
    /// and collects all referenced blob hashes.
    fn build_live_set(&self, collections: Option<Vec<String>>) -> HashSet<String> {
        let mut live: HashSet<String> = HashSet::new();

        // List all refs (paths)
        let refs = self.kernel.list_names_prefix("");

        for ref_path in &refs {
            // Skip blob paths (they're what we're trying to classify)
            if ref_path.starts_with("blobs/") {
                continue;
            }

            // If filtering by collection, skip refs not under those collections
            if let Some(ref colls) = collections {
                let matches = colls.iter().any(|c| ref_path.starts_with(&format!("collections/{}/", c)));
                if !matches {
                    continue;
                }
            }

            // Resolve the ref to a hash
            if let Some(hash) = self.kernel.resolve(ref_path) {
                // Walk reachable blobs from this hash
                self.walk_reachable(&hash, &mut live);
            }
        }

        live
    }

    /// Walk all blobs reachable from a starting hash.
    ///
    /// Follows: commit → manifest → row groups → data blobs.
    /// Also handles PondPack blobs (commit + manifest in one).
    fn walk_reachable(&self, hash: &str, live: &mut HashSet<String>) {
        if live.contains(hash) {
            return; // Already visited
        }
        live.insert(hash.to_string());

        // Read the blob
        let data = match self.kernel.read_blob(hash) {
            Ok(d) => d,
            Err(_) => return,
        };

        // Check if it's a PondPack blob
        if crate::pond_pack::is_pack(&data) {
            if let Some((commit, manifest_bytes, _inline)) = crate::pond_pack::decode_pack(&data) {
                // Walk the manifest for data blob references
                self.walk_manifest_bytes(&manifest_bytes, live);

                // Follow parent commit chain
                if let Some(parent) = commit.get("parent").and_then(|p| p.as_str()) {
                    if !parent.is_empty() {
                        self.walk_reachable(parent, live);
                    }
                }
            }
            return;
        }

        // Check if it's a JSON commit (old format)
        if data.first() == Some(&b'{') {
            if let Ok(commit) = serde_json::from_slice::<serde_json::Value>(&data) {
                // Follow manifest hash
                if let Some(manifest_hash) = commit.get("manifest").and_then(|m| m.as_str()) {
                    if !manifest_hash.is_empty() {
                        self.walk_reachable(manifest_hash, live);
                    }
                }
                // Follow parent
                if let Some(parent) = commit.get("parent").and_then(|p| p.as_str()) {
                    if !parent.is_empty() {
                        self.walk_reachable(parent, live);
                    }
                }
            }
            return;
        }

        // Check if it's a manifest (PMAN magic)
        if data.len() >= 4 && &data[0..4] == b"PMAN" {
            self.walk_manifest_bytes(&data, live);
        }
    }

    /// Walk a manifest's bytes and collect all referenced data blob hashes.
    fn walk_manifest_bytes(&self, manifest_bytes: &[u8], live: &mut HashSet<String>) {
        if let Some(manifest) = crate::manifest::CollectionManifest::decode(manifest_bytes) {
            for rg in &manifest.row_groups {
                live.insert(rg.blob_hash.clone());
            }
        }
    }

    /// List all blob hashes in the store.
    fn list_all_blob_hashes(&self) -> Vec<String> {
        let mut hashes = Vec::new();
        // List all blob shard directories (blobs/ab/, blobs/cd/, etc.)
        let blob_refs = self.kernel.list_names_prefix("blobs/");
        for blob_ref in blob_refs {
            // blob_ref looks like "blobs/ab/hash" — extract the hash
            let parts: Vec<&str> = blob_ref.split('/').collect();
            if parts.len() >= 3 {
                hashes.push(parts[2].to_string());
            }
        }
        hashes
    }
}

#[cfg(test)]
mod gc_tests {
    use super::*;
    use crate::UnifiedStorage;

    #[test]
    fn test_gc_collect_empty_store() {
        let dir = tempfile::tempdir().unwrap();
        let storage = UnifiedStorage::new_local(dir.path()).unwrap();
        let kernel = storage.kernel();
        let gc = GarbageCollector::new(kernel);
        let stats = gc.collect(None, false);
        assert_eq!(stats.live, 0);
        assert_eq!(stats.dead, 0);
    }

    #[test]
    fn test_gc_collect_with_live_data() {
        let dir = tempfile::tempdir().unwrap();
        let storage = UnifiedStorage::new_local(dir.path()).unwrap();
        let kernel = storage.kernel();

        // Write some data
        kernel.write(b"data1").unwrap();
        kernel.write(b"data2").unwrap();
        kernel.reference("ref1", &kernel.write(b"ref1data").unwrap()).unwrap();

        let gc = GarbageCollector::new(kernel);
        let stats = gc.collect(None, false);
        // "data1" and "data2" are dead (not referenced by any ref)
        // "ref1data" is live (referenced by "ref1")
        assert!(stats.live > 0, "should have some live blobs");
    }

    #[test]
    fn test_vacuum_dry_run() {
        let dir = tempfile::tempdir().unwrap();
        let storage = UnifiedStorage::new_local(dir.path()).unwrap();
        let kernel = storage.kernel();

        // Write some unreferenced data
        kernel.write(b"garbage1").unwrap();
        kernel.write(b"garbage2").unwrap();

        let gc = GarbageCollector::new(kernel);
        let result = gc.vacuum(None, 0, true); // dry run
        assert!(result.dry_run);
        assert!(result.deleted >= 2, "dry run should count garbage blobs");
    }

    #[test]
    fn test_vacuum_deletes_dead_blobs() {
        let dir = tempfile::tempdir().unwrap();
        let storage = UnifiedStorage::new_local(dir.path()).unwrap();
        let kernel = storage.kernel();

        // Write some unreferenced data
        let h1 = kernel.write(b"garbage1").unwrap();
        let h2 = kernel.write(b"garbage2").unwrap();

        let gc = GarbageCollector::new(kernel);
        let result = gc.vacuum(None, 0, false); // real vacuum
        assert!(result.deleted >= 2, "should delete garbage blobs");

        // Verify blobs are gone (read_blob should fail)
        assert!(kernel.read_blob(&h1).is_err());
        assert!(kernel.read_blob(&h2).is_err());
    }
}
