"""
PondStorage — the ONE unified storage SDK.

This is the single entry point for all storage operations in Pond.
It unifies what was previously three separate classes:
  - PondLens (namespace ops: list_collections, set_definition, etc.)
  - ProllyLensBase (commit/branch/merge/history) — LEGACY, removed
  - UnifiedStorage (PND2 write/read/point_lookup)

Into ONE class with three clear sections:

  ┌─────────────────────────────────────────────────────────┐
  │  PondStorage                                             │
  │  ┌─────────────┐  ┌───────────────┐  ┌────────────────┐ │
  │  │ Namespace   │  │ Commit/Branch │  │ Data I/O       │ │
  │  │ (list, def) │  │ (history)     │  │ (write/read)   │ │
  │  └─────────────┘  └───────────────┘  └────────────────┘ │
  └─────────────────────────────────────────────────────────┘

ARCHITECTURE:
  Lenses (Lakehouse, KV, Vector) compose PondStorage.
  PondStorage delegates to UnifiedStorage (PND2 + CollectionManifest)
  for both data I/O and commit/branch/history (manifest-based JSON
  commit blobs — no ProllyTree involved).
  The kernel (PondMinimal or ObjectStoreNativeKernel) is FROZEN.

USAGE:
    from pond_sdk import PondStorage, PondMinimal

    storage = PondStorage(PondMinimal("/path/to/.pond"))

    # Write any workload — same API
    storage.write("users", [{"id": 1, "name": "alice"}], key_col="id")

    # Read any workload — same API
    rows = storage.read("users", predicates=[("id", "=", 1)])
    row = storage.point_lookup("users", key="1")

    # Version control — same API
    storage.branch("users", "dev")
    storage.merge("users", "dev")
    storage.history("users")

This class is a thin orchestrator — it delegates to UnifiedStorage
internally. The legacy ProllyLensBase path has been removed;
UnifiedStorage is the ONLY storage backend.

MIGRATION: Existing lenses that use PondLens/UnifiedStorage directly
still work. PondStorage is the recommended new API. Over time, lenses
will migrate to compose PondStorage instead of the individual classes.
"""

from __future__ import annotations

import os
import sys
import json
from typing import Optional, Any, Iterator, Callable

# Make bindings/python/core and bindings/python/sdk importable
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                  "..", "bindings/python/core"))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                  "extensions", "physical_structures"))

from kernel import PondMinimal  # noqa: E402
from base_lens import PondLens  # noqa: E402

# Import the unified storage layer (PND2 + CollectionManifest).
# This is the ONLY storage backend — the legacy ProllyTreeIndex /
# ProllyLensBase path has been removed.
try:
    from unified_storage import UnifiedStorage, PND2  # noqa: E402
    from collection_manifest import CollectionManifest  # noqa: E402
    _HAVE_UNIFIED = True
except ImportError:
    _HAVE_UNIFIED = False


class PondStorage:
    """The ONE unified storage SDK for Pond.

    Three sections:
      1. Namespace: list_collections, collection_exists, set_definition,
         get_definition
      2. Commit/Branch: commit, branch, checkout, list_branches, merge,
         undo, history, diff
      3. Data I/O: write, append, read, read_as_columns, point_lookup,
         scan_with_pruning

    Lenses compose this class. They don't inherit from it. The lens
    provides workload-specific APIs (SQL, k-NN, JSON encode/decode) on
    top of the unified storage operations.

    Example:
        storage = PondStorage(kernel)
        storage.write("users", [{"id": 1, "name": "alice"}], key_col="id")
        row = storage.point_lookup("users", key="1")
        storage.branch("users", "dev")
        storage.merge("users", "dev")
    """

    def __init__(self, kernel: PondMinimal):
        """Create a PondStorage instance.

        Args:
            kernel: the PondMinimal or ObjectStoreNativeKernel instance
        """
        self.kernel = kernel
        # The namespace base (for list_collections, set_definition, etc.)
        self._lens = PondLens(kernel)
        # The unified storage layer (PND2 + CollectionManifest)
        self._unified: Optional[UnifiedStorage] = None
        if _HAVE_UNIFIED:
            self._unified = UnifiedStorage(kernel)

    # ==================================================================
    # Section 1: Namespace operations (was PondLens)
    # ==================================================================

    def list_collections(self, namespace: Optional[str] = None) -> list[str]:
        """List all collections (any lens, any format).

        Args:
            namespace: optional namespace prefix to filter by.
                e.g., "dev" returns ["dev/events", "dev/users"].
        """
        return self._lens.list_collections(namespace)

    def list_namespaces(self, parent: Optional[str] = None) -> list[str]:
        """List namespaces (one level deep) under a parent namespace.

        Examples:
            list_namespaces() → ["dev", "logs", "prod"]
            list_namespaces("dev") → ["events", "users"]
        """
        return self._lens.list_namespaces(parent)

    def collection_exists(self, name: str) -> bool:
        """Check if a collection has a HEAD ref."""
        return self._lens.collection_exists(name)

    def set_definition(self, name: str, definition: dict) -> str:
        """Store lens-specific metadata for a collection."""
        return self._lens.set_definition(name, definition)

    def get_definition(self, name: str) -> Optional[dict]:
        """Read lens-specific metadata for a collection."""
        return self._lens.get_definition(name)

    def stamp_collection_metadata(self, name: str, **kwargs) -> str:
        """Stamp cross-lens metadata on a collection. See base_lens.PondLens."""
        return self._lens.stamp_collection_metadata(name, **kwargs)

    def get_collection_metadata(self, name: str) -> dict:
        """Read cross-lens metadata for a collection. See base_lens.PondLens."""
        return self._lens.get_collection_metadata(name)

    def list_collections_with_metadata(self) -> list[dict]:
        """List ALL collections with their cross-lens metadata.

        Returns a list of {"name", "lens_type", "key_col", "schema_hint",
        "created_at"} for every collection in the pond, regardless of
        which lens created it. Any lens can call this to see the entire
        pond.
        """
        return self._lens.list_collections_with_metadata()

    def resolve_ref(self, name: str) -> Optional[str]:
        """Resolve a ref name to its current hash."""
        return self.kernel.resolve(name)

    # ==================================================================
    # Section 2: Commit / branch / history (manifest-based — no ProllyTree)
    #
    # All version control operations delegate to UnifiedStorage, which
    # uses a simple JSON commit blob format:
    #   {parent, second_parent, manifest, message, timestamp, index}
    #
    # The commit chain is: HEAD ref → commit blob → manifest blob → data blobs
    # Branches are ref copies. Merges create two-parent commits.
    # History walks parent pointers. No ProllyTree involved.
    # ==================================================================

    def commit(self, name: str, message: str = "") -> str:
        """Commit staged changes for a collection.

        With the unified manifest-based architecture, commits are
        created automatically by write()/append(). This method is kept
        for API compatibility — it's a no-op that returns the current
        active branch's commit hash.
        """
        if self._unified is not None:
            head = self.kernel.resolve(self._unified._active_commit_ref(name))
        else:
            head = self.kernel.resolve(f"collections/{name}/_branches/main/commit")
        return head or ""

    def branch(self, name: str, branch_name: str) -> str:
        """Create a branch on a collection — O(1) ref copy."""
        if self._unified is None:
            raise RuntimeError("UnifiedStorage not available")
        return self._unified.branch(name, branch_name)

    def checkout(self, name: str, branch_name: str) -> None:
        """Checkout a branch — point HEAD at the branch's commit."""
        if self._unified is None:
            raise RuntimeError("UnifiedStorage not available")
        self._unified.checkout(name, branch_name)

    def checkout_new(self, name: str, branch_name: str) -> str:
        """Create a branch AND checkout — like `git checkout -b`.

        Combines branch() + checkout() in one call.
        """
        if self._unified is None:
            raise RuntimeError("UnifiedStorage not available")
        return self._unified.checkout_new(name, branch_name)

    def list_branches(self, name: str) -> list[str]:
        """List all branches for a collection."""
        if self._unified is None:
            return []
        return self._unified.list_branches(name)

    def merge(self, name: str, source_branch: str,
              target_branch: Optional[str] = None,
              message: str = "") -> str:
        """Merge a source branch into a target branch.

        Args:
            name: collection name
            source_branch: the branch to merge FROM
            target_branch: the branch to merge INTO. If None, uses the
                currently active branch.
            message: commit message for the merge

        Examples:
            # Merge feature1 into the currently active branch
            storage.merge("events", "feature1")

            # Merge feature1 into main explicitly (no checkout needed)
            storage.merge("events", "feature1", "main")
        """
        if self._unified is None:
            raise RuntimeError("UnifiedStorage not available")
        return self._unified.merge(name, source_branch, target_branch, message)

    def undo(self, name: str, steps: int = 1) -> str:
        """Undo the last N commits — walk parent pointers."""
        if self._unified is None:
            raise RuntimeError("UnifiedStorage not available")
        return self._unified.undo(name, steps)

    def revert(self, name: str, commit_hash: str) -> str:
        """Revert HEAD to a specific commit — like `git revert` / `git reset`.

        Points HEAD at the given commit_hash, regardless of how many
        steps back it is. Unlike undo (which walks N steps), revert
        takes an explicit commit hash.

        Args:
            name: collection name
            commit_hash: the commit hash to revert to (must be in history)

        Returns:
            The commit hash that HEAD now points to.

        Example:
            # Get a specific commit from history
            hist = storage.history("users")
            old_commit = hist[5]["hash"]  # 5 commits ago

            # Revert to that commit
            storage.revert("users", old_commit)
        """
        if self._unified is None:
            raise RuntimeError("UnifiedStorage not available")
        return self._unified.revert(name, commit_hash)

    def history(self, name: str, limit: int = 100) -> list[dict]:
        """Walk the commit history for a collection."""
        if self._unified is None:
            raise RuntimeError("UnifiedStorage not available")
        return self._unified.history(name, limit)

    def diff(self, name: str, commit_a: str, commit_b: str) -> dict:
        """Compute the diff between two commits."""
        if self._unified is None:
            raise RuntimeError("UnifiedStorage not available")
        return self._unified.diff(name, commit_a, commit_b)

    # ==================================================================
    # Section 3: Data I/O (was UnifiedStorage)
    # ==================================================================

    def write(self, collection: str, rows,
              key_col: Optional[str] = None,
              row_group_size: int = 10_000,
              encoding_hints: Optional[dict[str, str]] = None,
              message: str = "") -> str:
        """Write rows to a collection as PND2 blobs.

        ONE write path for ALL workloads. Splits rows into row groups,
        encodes each as a PND2 blob (auto-selects encoding per column),
        builds a CollectionManifest, and commits atomically.

        Args:
            collection: collection name
            rows: a ColumnSource, PyArrow Table, or list[dict]
            key_col: column to use as the sort key (None = row index)
            row_group_size: rows per row group (default 10_000)
            encoding_hints: optional dict {col_name: "auto"|"rle"|...}
            message: commit message

        Returns:
            The new HEAD commit hash.

        Round trips: N + 3 S3 PUTs (N data blobs + manifest + root ref + root pointer)
        """
        if self._unified is None:
            raise RuntimeError("UnifiedStorage not available — install the physical_structures extension")
        commit_hash = self._unified.write(collection, rows, key_col=key_col,
                                     row_group_size=row_group_size,
                                     encoding_hints=encoding_hints,
                                     message=message)
        # Round 26: no need to save commit→manifest mapping separately.
        # The commit blob stores the manifest hash directly, and
        # _resolve_commit_manifest reads it from there (1 GET).
        return commit_hash

    def append(self, collection: str, rows,
               key_col: Optional[str] = None,
               row_group_size: int = 10_000,
               encoding_hints: Optional[dict[str, str]] = None,
               message: str = "") -> str:
        """Append rows to an existing collection WITHOUT rewriting it.

        Non-destructive: reads the existing manifest (1 GET), keeps all
        existing row group entries, adds new row groups, writes a new
        manifest + commit.

        Args:
            collection: collection name (must already exist)
            rows: new rows to append
            key_col: sort key column (should match existing)
            row_group_size: rows per new row group
            encoding_hints: optional encoding hints
            message: commit message

        Returns:
            The new HEAD commit hash.
        """
        if self._unified is None:
            raise RuntimeError("UnifiedStorage not available")
        commit_hash = self._unified.append(collection, rows, key_col=key_col,
                                      row_group_size=row_group_size,
                                      encoding_hints=encoding_hints,
                                      message=message)
        # Round 26: no need to save commit→manifest mapping separately.
        # The commit blob stores the manifest hash directly.
        return commit_hash


    def append_shard(self, collection: str, rows,
                      key_col: Optional[str] = None,
                      row_group_size: int = 10_000,
                      encoding_hints: Optional[dict[str, str]] = None,
                      message: str = "",
                      tx_id: Optional[str] = None) -> str:
        """Concurrent-safe append — NO CAS, NO retry, NO coordination.

        Atomic publication: pass tx_id from begin_tx() to make the shard tentative
        (invisible until commit_tx is called). Without tx_id, the shard
        is immediately visible (normal CRDT).
        """
        if self._unified is None:
            raise RuntimeError("UnifiedStorage not available")
        return self._unified.append_shard(
            collection, rows, key_col=key_col,
            row_group_size=row_group_size,
            encoding_hints=encoding_hints, message=message, tx_id=tx_id)

    def append_shard_batch(self, collection: str,
                            shards: list[list[dict]],
                            key_col: Optional[str] = None,
                            row_group_size: int = 10_000,
                            tx_id: Optional[str] = None) -> list[str]:
        """Append MULTIPLE shards in ONE parallel batch — 1 RTT wall-clock.

        For N shards, this turns N × 2 sequential PUTs into 1 parallel batch.
        Example: 20 appends × 300ms = 6000ms → ~300ms with batch.

        Args:
            collection: collection name
            shards: list of row-lists, one per shard
            key_col: sort key column
            row_group_size: rows per row group within each shard
            tx_id: optional transaction ID (makes all shards tentative)
        """
        if self._unified is None:
            raise RuntimeError("UnifiedStorage not available")
        return self._unified.append_shard_batch(
            collection, shards, key_col=key_col,
            row_group_size=row_group_size, tx_id=tx_id)

    # ==================================================================
    # Atomic Publication — commit markers on top of CRDT shards
    # ==================================================================

    def begin_tx(self) -> str:
        """Begin a transaction. Returns a tx_id.

        Pass tx_id to append_shard() to make shards tentative.
        Call commit_tx(tx_id) to make them visible atomically.

        begin_tx is FREE — no storage operation. Just generates a unique ID.
        """
        if self._unified is None:
            raise RuntimeError("UnifiedStorage not available")
        return self._unified.begin_tx()

    def commit_tx(self, tx_id: str, message: str = "") -> str:
        """Commit a transaction — ALL tentative shards become visible.

        1 PUT (commit marker) + 1 ref. Atomic: crash before = invisible,
        crash after = all visible. No coordinator, no 2PC, no CAS.

        Args:
            tx_id: from begin_tx()
            message: optional commit message

        Returns:
            The commit marker hash.

        Example:
            tx = storage.begin_tx()
            storage.append_shard("users", user_rows, tx_id=tx)
            storage.append_shard("orders", order_rows, tx_id=tx)
            storage.commit_tx(tx)  # both visible atomically
        """
        if self._unified is None:
            raise RuntimeError("UnifiedStorage not available")
        return self._unified.commit_tx(tx_id, message)

    def abort_tx(self, tx_id: str) -> None:
        """Abort a transaction — tentative shards stay invisible.

        Simply don't commit. Tentative shards remain in storage but are
        invisible to readers. GC cleans them up after a timeout.
        """
        if self._unified is None:
            raise RuntimeError("UnifiedStorage not available")
        self._unified.abort_tx(tx_id)

    def is_tx_committed(self, tx_id: str) -> bool:
        """Check if a transaction has been committed."""
        if self._unified is None:
            return False
        return self._unified.is_tx_committed(tx_id)

    def read_with_shards(self, collection: str,
                          predicates: Optional[list[tuple[str, str, Any]]] = None,
                          columns: Optional[list[str]] = None,
                          row_filter: Optional[Callable[[dict], bool]] = None,
                          start_key: Optional[str] = None,
                          end_key: Optional[str] = None) -> list[dict]:
        """Read rows merging HEAD + all shards (CRDT union).

        Use this instead of read() when shards exist (after append_shard).
        Falls back to plain read() if no shards exist.
        """
        if self._unified is None:
            raise RuntimeError("UnifiedStorage not available")
        # If no shards, fall back to plain read (faster)
        if self._unified.shard_count(collection) == 0:
            return self.read(collection, predicates=predicates,
                              columns=columns, row_filter=row_filter,
                              start_key=start_key, end_key=end_key)
        return self._unified.read_with_shards(
            collection, predicates=predicates, columns=columns,
            row_filter=row_filter, start_key=start_key, end_key=end_key)

    def compact_shards(self, collection: str,
                        target_row_group_size: int = 100_000) -> Optional[str]:
        """Merge all shards into HEAD, then clear the shards.

        Idempotent — multiple compactors produce the same result.
        Should be called periodically (e.g., after every N shards) to
        bound read amplification.

        Args:
            collection: collection name
            target_row_group_size: row group size for row-level compaction
                re-encoding (default 100_000). Larger row groups reduce
                read amplification. Manifest-level compaction ignores
                this parameter.
        """
        if self._unified is None:
            raise RuntimeError("UnifiedStorage not available")
        return self._unified.compact_shards(
            collection, target_row_group_size=target_row_group_size)

    def shard_count(self, collection: str) -> int:
        """Return the number of unmerged shards for a collection."""
        if self._unified is None:
            return 0
        return self._unified.shard_count(collection)

    def invalidate_all_caches(self, collection: Optional[str] = None) -> None:
        """Invalidate ALL process-local caches for strong consistency.

        Call this before a read that MUST see the latest state from other
        processes. By default, the SDK's caches are process-local and may
        return stale data for up to `cache_ttl_seconds` (kernel path cache
        TTL, default 5s) after another process writes.

        Args:
            collection: if None, invalidate ALL collections' caches.
                If a collection name, invalidate only that collection.

        This is the "I want strong consistency" escape hatch. It's expensive
        (forces re-reads from storage) but correct.

        Example:
            # Process A writes
            storage_a.write("users", rows)

            # Process B reads — MUST call this to see A's write immediately
            storage_b.invalidate_all_caches()
            rows = storage_b.read("users")
        """
        if self._unified is not None:
            self._unified.invalidate_all_caches(collection)

    def wait_for_background_tasks(self, timeout: float = 30.0) -> None:
        """Wait for all background tombstone/vacuum threads to complete.

        Async tombstoning (in merge + compact) runs in daemon threads.
        This method blocks until all of them finish (or timeout).

        Call this when you need to ensure all shard refs are cleaned up
        before checking shard_count() or doing another operation that
        depends on the tombstoning being complete.
        """
        if self._unified is not None:
            self._unified.wait_for_background_tasks(timeout=timeout)

    def upsert_shard(self, collection: str, rows: list[dict],
                      key_col: Optional[str] = None,
                      row_group_size: int = 10_000) -> str:
        """Concurrent-safe upsert (insert-or-update) with row-level CRDT.

        Each row gets a _rowid (stable across updates) and _version
        (new per write). On merge, the row with the later _version wins.

        For NEW rows: caller does NOT provide _rowid — we generate one.
        For UPDATES: caller provides _rowid (from the original read),
                     we generate a new _version.

        Merge semantics (deterministic, eventually consistent):
          - INSERT + INSERT (same _rowid): later _version wins
          - UPDATE + UPDATE (same _rowid): later _version wins
          - DELETE + anything: later _version wins (tombstone if DELETE is later)
        """
        if self._unified is None:
            raise RuntimeError("UnifiedStorage not available")
        return self._unified.upsert_shard(collection, rows, key_col=key_col,
                                            row_group_size=row_group_size)

    def delete_shard(self, collection: str, rowids: list[str],
                      key_col: Optional[str] = None,
                      row_group_size: int = 10_000,
                      keys: Optional[list[str]] = None) -> str:
        """Concurrent-safe row-level delete with tombstones.

        Each deleted _rowid gets a tombstone with _deleted=True and a new
        _version. On merge, if the tombstone's _version is later than any
        live row's _version, the row is suppressed.

        Args:
            collection: collection name
            rowids: list of _rowid strings to delete
            key_col: sort key column (for range scans)
            row_group_size: rows per row group
            keys: optional list of key_col values, one per rowid. If
                provided, each tombstone's key_col is set to the actual
                key value. This ensures tombstones match legacy rows
                during CRDT merge (type-coerced via str()).
        """
        if self._unified is None:
            raise RuntimeError("UnifiedStorage not available")
        return self._unified.delete_shard(collection, rowids, key_col=key_col,
                                            row_group_size=row_group_size,
                                            keys=keys)

    def read(self, collection: str,
             predicates: Optional[list[tuple[str, str, Any]]] = None,
             columns: Optional[list[str]] = None,
             row_filter: Optional[Callable[[dict], bool]] = None,
             start_key: Optional[str] = None,
             end_key: Optional[str] = None,
             commit_hash: Optional[str] = None) -> list[dict]:
        """Read rows from a collection.

        ONE read path for ALL workloads. Reads the manifest (1 GET),
        evaluates predicates IN MEMORY against inline stats, fetches
        only surviving row groups.

        Args:
            collection: collection name
            predicates: list of (column, op, value) tuples. All ANDed.
            columns: projection pushdown (None = all columns)
            row_filter: exact row-level filter
            start_key: range scan lower bound
            end_key: range scan upper bound
            commit_hash: time-travel — resolves to the manifest at this commit.
                Fix (Round 11 Issue #4): now properly resolves to manifest_hash.

        Returns:
            List of row dicts.

        Round trips: 3 + K S3 GETs cold (root pointer + root ref + manifest + K data blobs)
        """
        if self._unified is None:
            raise RuntimeError("UnifiedStorage not available")
        manifest_hash = self._resolve_commit_manifest(collection, commit_hash) if commit_hash else None
        return self._unified.read(collection, predicates=predicates,
                                    columns=columns, row_filter=row_filter,
                                    start_key=start_key, end_key=end_key,
                                    manifest_hash=manifest_hash)

    def read_as_columns(self, collection: str,
                         predicates: Optional[list[tuple[str, str, Any]]] = None,
                         columns: Optional[list[str]] = None,
                         commit_hash: Optional[str] = None
                         ) -> dict[str, list]:
        """Read rows as column-oriented data (faster for columnar callers).

        Uses PARALLEL blob fetch for surviving row groups — K blobs fetched
        in ~1 RTT wall-clock instead of K × RTT.

        Fix (Round 12 Issue #2): resolves commit_hash to manifest_hash.
        Fix (Round 12 Issue #1): applies multi-predicate filter.
        """
        if self._unified is None:
            raise RuntimeError("UnifiedStorage not available")
        manifest_hash = self._resolve_commit_manifest(collection, commit_hash) if commit_hash else None
        return self._unified.read_as_columns(collection, predicates=predicates,
                                               columns=columns,
                                               manifest_hash=manifest_hash)

    def read_as_arrow(self, collection: str,
                       predicates: Optional[list[tuple[str, str, Any]]] = None,
                       columns: Optional[list[str]] = None,
                       commit_hash: Optional[str] = None) -> "pa.Table":
        """Read rows as a PyArrow Table — FASTEST read path for tabular.

        1. Manifest pruning (in-memory, 0 GETs)
        2. Parallel blob fetch (K GETs in ~1 RTT wall-clock)
        3. Zero-copy Arrow construction from column data

        Fix (Round 12 Issue #2): resolves commit_hash to manifest_hash.
        Fix (Round 12 Issue #1): applies multi-predicate filter.
        """
        if self._unified is None:
            raise RuntimeError("UnifiedStorage not available")
        manifest_hash = self._resolve_commit_manifest(collection, commit_hash) if commit_hash else None
        # read_as_arrow delegates to read_as_columns, so pass manifest_hash
        col_data = self._unified.read_as_columns(collection, predicates=predicates,
                                                   columns=columns,
                                                   manifest_hash=manifest_hash)
        if not col_data:
            import pyarrow as pa
            return pa.table({})
        import pyarrow as pa
        arrays = []
        names = []
        for col_name, values in col_data.items():
            arrays.append(pa.array(values))
            names.append(col_name)
        return pa.Table.from_arrays(arrays, names=names)

    def point_lookup(self, collection: str, key: str,
                      columns: Optional[list[str]] = None) -> Optional[dict]:
        """Point lookup — O(1) regardless of collection scale.

        Round trips: 4 S3 GETs cold (root pointer + root ref + manifest + 1 data blob)
        """
        if self._unified is None:
            raise RuntimeError("UnifiedStorage not available")
        return self._unified.point_lookup(collection, key=key, columns=columns)

    def scan_with_pruning(self, collection: str,
                           predicates: Optional[list[tuple[str, str, Any]]] = None
                           ) -> Iterator[tuple[str, str, dict]]:
        """Low-level scan — yields (rg_key, blob_hash, stats_dict) for surviving row groups."""
        if self._unified is None:
            raise RuntimeError("UnifiedStorage not available")
        yield from self._unified.scan_with_pruning(collection, predicates=predicates)

    def iter_rows(self, collection: str,
                  predicates: Optional[list[tuple[str, str, Any]]] = None,
                  columns: Optional[list[str]] = None,
                  batch_size: int = 1000) -> Iterator[list[dict]]:
        """Streaming read — yields rows in batches without loading all into memory.

        Memory-safe for 1B+ row collections. O(batch_size) memory per yield.
        """
        if self._unified is None:
            raise RuntimeError("UnifiedStorage not available")
        yield from self._unified.iter_rows(collection, predicates=predicates,
                                             columns=columns, batch_size=batch_size)

    # ==================================================================
    # Diagnostics
    # ==================================================================

    def _resolve_commit_manifest(self, collection: str,
                                  commit_hash: str) -> Optional[str]:
        """Resolve a commit hash to its manifest hash for time-travel reads.

        The manifest hash is stored directly IN the commit blob (JSON
        format). Read it from there (1 GET).
        """
        if self._unified is None:
            raise RuntimeError("UnifiedStorage not available")
        return self._unified._resolve_commit_manifest(collection, commit_hash)

    def _save_commit_manifest(self, name: str, commit_hash: str) -> None:
        """Save the current manifest hash keyed by commit hash for time-travel."""
        manifest_hash = self.kernel.resolve(f"collections/{name}/manifest")
        if manifest_hash is not None:
            self.kernel.reference(
                f"collections/{name}/commits/{commit_hash}__manifest",
                manifest_hash)

    def count(self, collection: str,
              predicates: Optional[list] = None) -> int:
        """Count rows in a collection WITHOUT fetching data blobs.

        Sums n_rows from surviving row groups in the manifest.
        O(1) S3 GETs if manifest is cached, O(1) GET otherwise.
        """
        manifest = self._unified._load_manifest(collection) if self._unified else None
        if manifest is None:
            return 0
        return sum(rg.n_rows for rg in manifest.scan_with_pruning(predicates))

    # ==================================================================
    # GC / Vacuum — reclaim space from unreachable blobs
    # ==================================================================

    def gc(self, collection: Optional[str] = None,
           compute_size: bool = False) -> dict:
        """Analyze reachability — returns live/dead blob counts (read-only).

        Args:
            collection: if None, analyze ALL collections. If specified,
                analyze only that collection.
            compute_size: if True, read each dead blob to compute its size.
                Default False — skips O(dead) reads. At PB scale, this is
                the difference between seconds and hours.

        Returns:
            {"live": int, "dead": int, "dead_hashes": list, "dead_size_bytes": int}
            dead_size_bytes is -1 if compute_size=False.
        """
        try:
            sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                              "extensions", "maintenance"))
            from vacuum import GarbageCollector
            gc = GarbageCollector(self.kernel)
            return gc.collect(collection, compute_size)
        except ImportError:
            return {"live": 0, "dead": 0, "dead_hashes": [], "dead_size_bytes": -1}

    def vacuum(self, collections: Optional[list] = None,
               preserve_days: int = 0,
               dry_run: bool = False,
               tentative_ttl_seconds: int = 3600) -> dict:
        """Delete unreachable blobs — reclaim storage space.

        Delta/Iceberg-style vacuum with preservation:

        Args:
            collections: list of collection names to vacuum. If None,
                vacuum ALL collections. Example: ["events", "users"].
            preserve_days: keep commits younger than N days. Commits
                older than this are eligible for deletion. Default 0
                (only current HEAD + live refs preserved).

                Like Delta/Iceberg vacuum — preserves recent history
                for time-travel queries. Set to 7 to keep last week.

                Content-addressed blobs shared between preserved and
                non-preserved commits are NEVER deleted (they're live).
            dry_run: if True, report what would be deleted without deleting.
            tentative_ttl_seconds: preserve tentative shards from in-flight
                atomic publication transactions younger than this many seconds. Default
                3600 (1 hour). A long-running transaction has no commit
                marker until commit_tx runs — without this TTL, a concurrent
                vacuum would delete its tentative shards. Set to 0 to
                disable (delete immediately, old behavior).

        Returns:
            {"deleted": int, "preserved": int, "freed_bytes": int, ...}

        Examples:
            # Vacuum everything (aggressive)
            storage.vacuum()

            # Vacuum specific collections
            storage.vacuum(collections=["events", "users"])

            # Vacuum but keep last 7 days for time-travel
            storage.vacuum(preserve_days=7)

            # Dry run — see what would be deleted
            storage.vacuum(dry_run=True)
        """
        try:
            sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                              "extensions", "maintenance"))
            from vacuum import GarbageCollector
            gc = GarbageCollector(self.kernel)
            return gc.vacuum(collections, preserve_days, dry_run,
                             tentative_ttl_seconds)
        except ImportError:
            return {"deleted": 0, "preserved": 0, "freed_bytes": -1,
                    "dry_run": dry_run, "collections": collections,
                    "preserve_days": preserve_days}

    def optimize(self, collection: Optional[str] = None) -> dict:
        """Optimize storage — compact shards + flatten delta manifests.

        Delta/Iceberg-style optimize: merges small files into larger ones
        for better read performance.

        Does TWO things:
          1. compact_shards: merge all shards into HEAD (clears shard list)
          2. compact_manifest: flatten delta-manifest chains into one flat manifest

        Args:
            collection: if None, optimize ALL collections. If specified,
                optimize only that collection.

        Returns:
            {"collections_optimized": int, "shards_compacted": int,
             "manifests_flattened": int}
        """
        if self._unified is None:
            return {"collections_optimized": 0, "shards_compacted": 0,
                    "manifests_flattened": 0}

        if collection:
            collections = [collection]
        else:
            collections = self.list_collections()

        shards_compacted = 0
        manifests_flattened = 0
        optimized = 0

        for coll in collections:
            # Check active branch (default main)
            try:
                # Compact shards on the active branch
                shard_count = self._unified.shard_count(coll)
                if shard_count > 0:
                    self._unified.compact_shards(coll)
                    shards_compacted += shard_count
                # Flatten delta-manifest chain if deep
                result = self._unified.compact_manifest(coll)
                if result is not None:
                    manifests_flattened += 1
                optimized += 1
            except Exception:
                pass

        return {
            "collections_optimized": optimized,
            "shards_compacted": shards_compacted,
            "manifests_flattened": manifests_flattened,
        }

    def alter_collection(self, collection: str,
                     add_columns: Optional[list] = None,
                     drop_columns: Optional[list[str]] = None,
                     rename: Optional[dict[str, str]] = None) -> dict:
        """Schema evolution — add/drop/rename columns (Iceberg-style).

        Does NOT rewrite data — just updates the manifest's schema.
        New columns appear as None in old row groups. Dropped columns
        are removed from the schema (old data remains but is invisible).

        GENERIC for ALL data types:
          - add_columns accepts either:
            - str: column name (type auto-detected on next write = NULL type)
            - tuple: (name, type_str) where type_str is one of:
              "int64", "float64", "string", "binary", "null"
            - dict: {name: type_str} for batch add with types
          - This works for structured (int, float, string),
            semi-structured (JSON stored as string/binary),
            and unstructured data (blobs stored as binary).

        Args:
            collection: collection name
            add_columns: list of column names, (name, type) tuples, or
                         {name: type} dicts to add
            drop_columns: list of column names to drop
            rename: dict of {old_name: new_name}

        Returns:
            {"added": int, "dropped": int, "renamed": int, "schema_version": int}
        """
        if self._unified is None:
            raise RuntimeError("UnifiedStorage not available")
        manifest = self._unified._load_manifest(collection, skip_cache=False)
        if manifest is None:
            raise ValueError(f"Collection '{collection}' not found")

        added = dropped = renamed_count = 0
        old_columns = list(manifest.columns)
        new_columns = []
        existing_names = {name for name, _ in old_columns}

        # Process drops
        drop_set = set(drop_columns or [])
        # Process renames
        rename_map = rename or {}

        for name, vtype in old_columns:
            if name in drop_set:
                dropped += 1
                continue
            if name in rename_map:
                new_columns.append((rename_map[name], vtype))
                renamed_count += 1
            else:
                new_columns.append((name, vtype))

        # Process adds — supports str, (name, type) tuple, or {name: type} dict
        TYPE_MAP = {"int64": 1, "float64": 2, "string": 3, "binary": 4, "null": 4}
        for item in (add_columns or []):
            if isinstance(item, str):
                # Just a name — type auto-detected on next write (NULL)
                col_name, col_type = item, 4
            elif isinstance(item, tuple) and len(item) == 2:
                # (name, type_str) — explicit type
                col_name, type_str = item
                col_type = TYPE_MAP.get(type_str.lower(), 4)
            elif isinstance(item, dict):
                # {name: type_str} — batch
                for name, type_str in item.items():
                    if name not in {n for n, _ in new_columns}:
                        new_columns.append((name, TYPE_MAP.get(str(type_str).lower(), 4)))
                        added += 1
                continue
            else:
                continue  # skip invalid entries

            if col_name not in {n for n, _ in new_columns}:
                new_columns.append((col_name, col_type))
                added += 1

        # Build a new manifest with the updated schema
        new_version = manifest.schema_version + 1
        manifest_entries = []
        for rg in manifest.scan_with_pruning():
            manifest_entries.append({
                "rg_key": rg.key,
                "blob_hash": rg.blob_hash,
                "n_rows": rg.n_rows,
                "col_stats": [(c.name, c.value_type, c.min, c.max, c.null_count)
                                for c in rg.columns],
            })

        manifest_hash, new_manifest = self._unified._build_manifest_with_return(
            collection, manifest_entries, new_columns,
            manifest.key_col, manifest.row_group_size)
        new_manifest.set_schema_version(new_version)

        # Write commit
        parent = self.kernel.resolve(self._unified._active_commit_ref(collection))
        commit_index = 0
        if parent:
            pc = self._unified._read_commit_blob(parent)
            if pc:
                commit_index = pc.get("index", 0) + 1
        commit_hash = self._unified._write_commit_blob(
            collection, manifest_hash, parent=parent,
            message=f"alter_collection: +{added} -{dropped} ~{renamed_count}",
            index=commit_index)

        self._unified._update_caches_after_write(
            collection, new_manifest, manifest_hash, commit_hash,
            commit_index, is_delta=False)

        return {"added": added, "dropped": dropped,
                "renamed": renamed_count, "schema_version": new_version}

    def set_partition_spec(self, collection: str,
                            partition_cols: list[str],
                            transform: str = "identity") -> str:
        """Set hidden partitioning spec on a collection (Iceberg-style).

        Hidden partitions are stored in the manifest — no separate
        partition collections. Reads with predicates on partition columns
        get automatic partition pruning via the manifest's inline stats.

        Args:
            collection: collection name
            partition_cols: columns to partition by
            transform: "identity", "hour", "day", "month", "bucket:N"

        Returns:
            The new commit hash.
        """
        if self._unified is None:
            raise RuntimeError("UnifiedStorage not available")
        manifest = self._unified._load_manifest(collection, skip_cache=False)
        if manifest is None:
            raise ValueError(f"Collection '{collection}' not found")

        spec = {"columns": partition_cols, "transform": transform}
        manifest_entries = []
        for rg in manifest.scan_with_pruning():
            manifest_entries.append({
                "rg_key": rg.key,
                "blob_hash": rg.blob_hash,
                "n_rows": rg.n_rows,
                "col_stats": [(c.name, c.value_type, c.min, c.max, c.null_count)
                                for c in rg.columns],
            })

        manifest_hash, new_manifest = self._unified._build_manifest_with_return(
            collection, manifest_entries, manifest.columns,
            manifest.key_col, manifest.row_group_size)
        new_manifest.set_partition_spec(spec)
        new_manifest.set_schema_version(manifest.schema_version)

        parent = self.kernel.resolve(self._unified._active_commit_ref(collection))
        commit_index = 0
        if parent:
            pc = self._unified._read_commit_blob(parent)
            if pc:
                commit_index = pc.get("index", 0) + 1
        commit_hash = self._unified._write_commit_blob(
            collection, manifest_hash, parent=parent,
            message=f"set_partition_spec: {partition_cols} {transform}",
            index=commit_index)

        self._unified._update_caches_after_write(
            collection, new_manifest, manifest_hash, commit_hash,
            commit_index, is_delta=False)

        return commit_hash

    def delete_collection(self, collection: str) -> bool:
        """Delete a collection by tombstoning its definition + branch commit refs.

        Fix (Round 12 Issue #3): previously a no-op that returned True.
        Now uses the RFC-0008 tombstone pattern from maintenance.py to
        actually rebind the definition ref (collection-level marker) and
        each branch's commit ref to TOMBSTONE_HASH, making the collection
        unreadable. Underlying blobs are NOT deleted (content-addressed,
        may be shared). Use vacuum() for blob cleanup (not yet implemented).
        """
        deleted = False
        # Collect all refs to tombstone: the definition ref (collection-level
        # marker) + each branch's commit ref (so list_branches / checkout
        # see the collection as gone).
        refs_to_tombstone: list[str] = []
        definition_ref = f"collections/{collection}/definition"
        if self.kernel.resolve(definition_ref) is not None:
            refs_to_tombstone.append(definition_ref)
        # Tombstone every branch's commit ref under branches/{branch}/commit.
        branch_prefix = f"collections/{collection}/_branches/"
        for n in self.kernel.list_names():
            if not n.startswith(branch_prefix):
                continue
            # Match collections/{c}/_branches/{branch}/commit exactly — skip
            # /manifest and /shards/ subpaths (their parent commit ref going
            # away is enough to make the branch unreachable).
            rest = n[len(branch_prefix):]
            parts = rest.split("/")
            if len(parts) == 2 and parts[1] == "commit":
                refs_to_tombstone.append(n)
        # If no definition ref existed (legacy collection), still tombstone
        # the default branch's commit ref so collection_exists returns False.
        if not refs_to_tombstone:
            main_commit_ref = f"collections/{collection}/_branches/main/commit"
            if self.kernel.resolve(main_commit_ref) is not None:
                refs_to_tombstone.append(main_commit_ref)

        try:
            from maintenance import drop_name, TOMBSTONE_HASH
            for ref in refs_to_tombstone:
                if self.kernel.resolve(ref) is not None:
                    drop_name(self.kernel, ref)
                    deleted = True
        except ImportError:
            # maintenance.py not available — manual tombstone
            # Write a zero-length blob and point the ref at it
            for ref in refs_to_tombstone:
                if self.kernel.resolve(ref) is not None:
                    empty_blob = self.kernel.write(b"")
                    self.kernel.reference(ref, empty_blob)
                    deleted = True

        if self._unified:
            self._unified._invalidate_manifest_cache(collection)
        return deleted

    def compact(self, collection: str) -> Optional[str]:
        """Compact a delta-manifest chain into a single flat manifest.

        Call after many appends to prevent read amplification from deep
        parent chains. Auto-triggered after 8 appends, but can be called
        manually for finer control.
        """
        if self._unified is None:
            return None
        return self._unified.compact_manifest(collection)

    def get_round_trip_count(self, collection: str,
                              predicates: Optional[list] = None) -> dict:
        """Estimate S3 round trips for a read (without performing it)."""
        if self._unified is None:
            return {"error": "UnifiedStorage not available"}
        manifest = self._unified._load_manifest(collection)
        if manifest is None:
            return {"error": "no manifest for collection"}
        total = len(manifest.row_groups)
        surviving = list(manifest.scan_with_pruning(predicates))
        k = len(surviving)
        return {
            "manifest_fetches": 1,
            "data_blob_fetches": k,
            "total_fetches": 1 + k,
            "total_row_groups": total,
            "pruned_row_groups": total - k,
            "selectivity": k / total if total else 0.0,
        }
