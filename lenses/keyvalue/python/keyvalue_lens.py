"""
KeyValueLens — the app-facing KEY-VALUE lens.

This is one of three peer app-facing lenses in Pond:
  - KeyValueLens  (this file)        — per-row key→blob storage over UnifiedStorage
  - LakehouseLens (lenses/lakehouse)  — whole-table Parquet I/O + range read/write
  - FeatureStoreLens (pond-labs)      — versioned ML feature store on Parquet

All three extend PondLens (base_lens.py), the thin shared-namespace base.
PondLens provides only ref-namespace operations (branch, list_collections,
set_definition, get_definition, history) — no format awareness. Each
app-facing lens owns its OWN read/write API.

COLLECTION-AGNOSTIC: KeyValueLens is a STATELESS read/write engine. It does
NOT bind to a single collection in __init__. You pass the collection name to
each operation:

    lens = KeyValueLens(kernel)
    lens.put("users", "user:1", {"name": "alice"})
    lens.get("users", "user:1")
    lens.commit("users", "msg")

This matches LakehouseLens's API (create_table(name, data), read_table(name)).
The same lens instance can operate on ANY collection.

KeyValueLens stores each row as a single value column in a PND2 row group
(keyed by a user-supplied primary key, or auto-generated UUIDv7 for
KeylessLens). This makes it suitable for:
  - OLTP workloads (point lookups via manifest, O(1) cold)
  - Streaming/event logs (KeylessLens variant with auto-UUIDv7 keys)
  - Document storage (each blob is a JSON document)
  - Cross-lens row sharing (any lens can read any collection via metadata)

Backward-compat: the old API `KeyValueLens(kernel, name)` still works via
a compatibility wrapper that binds to a single collection. New code should
use the collection-agnostic API.

STORAGE: There is exactly ONE storage path — the UnifiedStorage backend
(PND2 blobs + CollectionManifest + JSON commit blobs). The legacy
ProllyTreeIndex / ProllyLensBase path has been removed. If UnifiedStorage
is not available, all I/O methods raise RuntimeError.
"""

from __future__ import annotations

import json
import time
import sys
import os
import hashlib
import uuid
from typing import Optional, Any, Callable, Union

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "..", "bindings/python/core"))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "..", "bindings/python/sdk"))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "..", "bindings/python/sdk", "extensions", "physical_structures"))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from kernel import PondMinimal
from maintenance import (drop_name, is_dropped, resolve_active,
                         TOMBSTONE_HASH)
from row_query import LensQuery
from base_lens import PondLens
from uuid7 import uuidv7


# ===========================================================================
# KeyValueLens — the app-facing key-value lens (collection-agnostic)
# ===========================================================================

class KeyValueLens(PondLens):
    """App-facing KEY-VALUE lens with UnifiedStorage backing.

    COLLECTION-AGNOSTIC: This lens is a stateless read/write engine. It
    does NOT bind to a single collection. Pass the collection name to
    each operation:

        lens = KeyValueLens(kernel)
        lens.put("users", "user:1", {"name": "alice"})
        lens.commit("users", "insert alice")
        lens.get("users", "user:1")  # → {"name": "alice"}

    The same lens instance can operate on ANY collection. This matches
    LakehouseLens's API design.

    Key-value operations (all take collection as first arg):
      - put(collection, key, data): stage a key→blob mapping
      - put_auto(collection, data): stage with auto-generated UUIDv7 key
      - get(collection, key): read a single value by key (O(1) cold)
      - get_raw(collection, key): read raw bytes (no decode)
      - delete(collection, key): stage a deletion
      - commit(collection, message): atomically commit all staged changes
      - keys(collection), count(collection), exists(collection, key)
      - get_all(collection)

    Lazy query API:
      - where(collection, predicate=None, **kwargs)
      - select(collection, *fields)
      - map(collection, fn)
      - join(collection, other, on='field')

    Version control (delegated to UnifiedStorage):
      - branch(collection, branch_name), checkout(collection, branch_name)
      - list_branches(collection)
      - merge(collection, branch_name) [union merge with 2-parent commit]
      - undo(collection, steps), history(collection, limit)
      - diff(collection, a, b)
    """

    def __init__(self, kernel: PondMinimal, name: Optional[str] = None,
                 use_unified_storage: bool = True,
                 compact_after_commit: bool = True):
        """Create a KeyValueLens.

        Args:
            kernel: the PondMinimal kernel instance
            name: OPTIONAL. If provided, enables backward-compatible
                  single-collection API.
            use_unified_storage: IGNORED (kept for backward compat).
                  There is now only ONE storage path — the unified
                  manifest-based architecture. All lenses use PND2
                  blobs + CollectionManifest + JSON commit blobs.
            compact_after_commit: if True (default), shards ARE compacted
                  after every commit. For multi-writer or high-throughput
                  workloads, set to False and compact periodically via
                  compact_shards() or a background job.
                  See VETERAN_ARCHITECT_REVIEW.md §3.7 for the tradeoff.
        """
        super().__init__(kernel)
        self._default_collection = name
        if name is not None:
            self.name = name
        # Attached indexer for auto-notify on commit (set via attach_indexer)
        self._attached_indexer = None
        self._compact_after_commit = compact_after_commit

        # Unified storage backend (the ONLY storage path)
        self._unified_storage = None
        try:
            from unified_storage import UnifiedStorage
            self._unified_storage = UnifiedStorage(kernel)
        except ImportError:
            pass  # _require_unified() will raise RuntimeError on first I/O
        # Unified storage write buffer: collection → {key → value}
        self._unified_buffer: dict[str, dict[str, Any]] = {}
        # Cross-lens metadata cache: collection → key_col (so cold lookup
        # costs 1 GET, subsequent lookups are free).
        self._key_col_cache: dict[str, str] = {}

    def _require_unified(self) -> None:
        """Raise RuntimeError if UnifiedStorage is not available.

        The legacy ProllyTreeIndex / ProllyLensBase path has been removed.
        UnifiedStorage is the ONLY storage path. If it is None (because
        the physical_structures extension is not importable), every I/O
        method must fail loudly rather than silently fall back.
        """
        if self._unified_storage is None:
            raise RuntimeError(
                "UnifiedStorage is not available — the legacy "
                "ProllyTreeIndex path has been removed. Install the "
                "physical_structures extension (bindings/python/sdk/extensions/"
                "physical_structures) to enable KV I/O."
            )

    def _resolve_key_col(self, collection: str) -> str:
        """Resolve the key column for a collection — cross-lens aware.

        KV collections use "_key" as the key column. But a KV lens can
        read/write ANY collection (lakehouse, vector, streaming, etc.).
        For those, the key column comes from the collection's metadata.

        Falls back to "_key" if no metadata is found (KV's own default).

        CACHED: the metadata lookup is cached per-collection on the lens
        instance, so subsequent reads pay 0 extra GETs. The first cold
        lookup pays 1 extra GET (the metadata blob fetch).
        """
        if collection in self._key_col_cache:
            return self._key_col_cache[collection]
        md = self.get_collection_metadata(collection)
        kc = md.get("key_col") or "_key"
        self._key_col_cache[collection] = kc
        return kc

    def _resolve_collection(self, *args) -> tuple:
        """Resolve the collection name from args or default.

        If _default_collection is set (backward compat mode), the first
        arg is NOT the collection — it's the key. We prepend the default.
        Otherwise, the first arg IS the collection.

        Returns (collection, remaining_args).
        """
        if self._default_collection is not None:
            return self._default_collection, args
        else:
            if not args:
                raise TypeError("Collection name required (lens is not bound to a default collection)")
            return args[0], args[1:]

    # --- Write path ---

    def put(self, *args) -> str:
        """Stage a key→blob mapping.

        Collection-agnostic API: put(collection, key, data)
        Backward compat API:    put(key, data)  [requires name in __init__]
        """
        collection, rest = self._resolve_collection(*args)
        key, data = rest[0], rest[1]
        self._require_unified()

        # Buffer the put; commit later writes a PND2 row group.
        if collection not in self._unified_buffer:
            self._unified_buffer[collection] = {}
        self._unified_buffer[collection][key] = data
        return key  # placeholder — real hash assigned at commit

    def put_auto(self, *args) -> str:
        """Stage data with an auto-generated UUIDv7 key. Returns the key.

        Collection-agnostic API: put_auto(collection, data)
        Backward compat API:    put_auto(data)  [requires name in __init__]
        """
        collection, rest = self._resolve_collection(*args)
        data = rest[0]
        self._require_unified()

        key = uuidv7()
        if collection not in self._unified_buffer:
            self._unified_buffer[collection] = {}
        self._unified_buffer[collection][key] = data
        return key

    def put_raw(self, *args) -> None:
        """Stage a pre-existing blob hash under the given key.

        Collection-agnostic API: put_raw(collection, key, blob_hash)
        Backward compat API:    put_raw(key, blob_hash)  [requires name in __init__]

        NOTE: The legacy ProllyTreeIndex path supported zero-copy hash
        sharing (staging a blob_hash without re-encoding). UnifiedStorage
        writes PND2 row groups, so this method now reads the blob's bytes
        and stages them as the value. Cross-collection zero-copy hash
        sharing is no longer meaningful — each collection has its own
        PND2 blobs. Cross-lens reads happen at the row level via
        UnifiedStorage.read().
        """
        collection, rest = self._resolve_collection(*args)
        key, blob_hash = rest[0], rest[1]
        self._require_unified()

        if collection not in self._unified_buffer:
            self._unified_buffer[collection] = {}
        # Read the existing blob bytes and stage them as the value.
        # The bytes are re-encoded at commit (encode() passes bytes through).
        raw_bytes = self.kernel.read_blob(blob_hash)
        self._unified_buffer[collection][key] = raw_bytes

    def delete(self, *args) -> None:
        """Stage a deletion for the given key.

        Collection-agnostic API: delete(collection, key)
        Backward compat API:    delete(key)  [requires name in __init__]

        In unified mode, deletes are staged by marking the key in the
        buffer with a tombstone sentinel (value=None). commit() then
        performs a full rewrite: read existing data, drop deleted keys,
        add new puts, write the result via write() (overwrite).
        """
        collection, rest = self._resolve_collection(*args)
        key = rest[0]
        self._require_unified()

        if collection not in self._unified_buffer:
            self._unified_buffer[collection] = {}
        # Tombstone: set value to None (commit skips None values)
        self._unified_buffer[collection][key] = None

    def commit(self, *args) -> str:
        """Atomically commit all staged changes for the collection.

        Collection-agnostic API: commit(collection, message="")
        Backward compat API:    commit(message="")  [requires name in __init__]

        After committing, if any indexers are registered (via
        CollectionMetadata.register_eager_index), they are notified
        via notify_write(). This enables EAGER index auto-refresh
        without coupling the lens to the indexer.
        """
        collection, rest = self._resolve_collection(*args)
        message = rest[0] if rest else ""
        self._require_unified()

        if collection not in self._unified_buffer:
            raise ValueError(f"No staged data for collection '{collection}'")
        buffer = self._unified_buffer[collection]

        # Bug 8 fix: use CRDT delete_shard (row-level tombstones) for
        # deletes instead of a full rewrite. The full rewrite bypassed
        # CRDT — any concurrent writer's shards were invisible and lost.
        #
        # Flow:
        #   1. For deletes: read existing rows to get their _rowid values,
        #      then call delete_shard(rowids) to write tombstone shards.
        #   2. For puts: use append_shard (same as the puts-only path) —
        #      CRDT-safe, concurrent writers unaffected.
        #   3. For mixed puts+deletes: do both.
        has_deletes = any(v is None for v in buffer.values())
        puts_only = {k: v for k, v in buffer.items() if v is not None}

        if has_deletes:
            deleted_keys = {k for k, v in buffer.items() if v is None}

            # Read existing rows to find _rowid values for deleted keys.
            # Only rows with _rowid (from upsert_shard) can be tombstoned
            # via delete_shard. Legacy rows (from append_shard) have no
            # _rowid — they're left for a future compaction to reclaim.
            existing_rows = self._unified_storage.read_with_shards(
                collection, columns=["_key", "_rowid"])
            rowids_to_delete = []
            keys_for_tombstones = []
            for row in existing_rows:
                if row.get("_key") in deleted_keys and row.get("_rowid"):
                    rowids_to_delete.append(row["_rowid"])
                    # Pass the _key value so tombstones get distinct
                    # rg_keys (avoids compact_shards dropping them).
                    keys_for_tombstones.append(row["_key"])

            # Write tombstones for deletes (CRDT-safe — concurrent
            # writers' shards are unaffected; tombstones only suppress
            # matching _rowid rows on merge).
            commit_hash = ""
            if rowids_to_delete:
                commit_hash = self._unified_storage.delete_shard(
                    collection, rowids_to_delete, key_col="_key",
                    keys=keys_for_tombstones)

            # Append puts (CRDT-safe — same as the puts-only path below).
            if puts_only:
                rows = [{"_key": k, "value": self.encode(v)}
                         for k, v in puts_only.items()]
                rows.sort(key=lambda r: r["_key"])
                existing_manifest = self._unified_storage._load_manifest(collection)
                if existing_manifest is None:
                    # NEW collection — write() creates the first manifest.
                    put_hash = self._unified_storage.write(
                        collection, rows, key_col="_key",
                        row_group_size=10_000,
                        message=message or f"{collection} unified commit")
                    self.stamp_collection_metadata(
                        collection, lens_type="keyvalue", key_col="_key",
                        schema_hint={"_key": "string", "value": "bytes"})
                    commit_hash = put_hash
                else:
                    # EXISTING collection — append via CRDT shard
                    put_hash = self._unified_storage.append(
                        collection, rows, key_col="_key",
                        row_group_size=10_000,
                        message=message or f"{collection} unified commit (puts)")
                    # Compact shards into HEAD so branch/merge/history see
                    # the latest data (append uses shards, but version
                    # control needs HEAD to be current). Tombstones survive
                    # compaction because delete_shard was called with keys=
                    # (distinct rg_keys per tombstone).
                    #
                    # NOTE: this is O(N) per commit. For high-write workloads,
                    # set compact_after_commit=False and compact periodically
                    # via a background job. See VETERAN_ARCHITECT_REVIEW.md §3.7.
                    if self._compact_after_commit:
                        self._unified_storage.compact_shards(collection)
                    if not commit_hash:
                        commit_hash = put_hash

            # Stamp metadata if this is a new collection (deletes-only on
            # a non-existent collection is a no-op, but be defensive).
            if self.get_collection_metadata(collection).get("lens_type") is None:
                self.stamp_collection_metadata(
                    collection, lens_type="keyvalue", key_col="_key",
                    schema_hint={"_key": "string", "value": "bytes"})
        elif puts_only:
            # No deletes — just append new puts
            rows = [{"_key": k, "value": self.encode(v)}
                     for k, v in puts_only.items()]
            rows.sort(key=lambda r: r["_key"])
            # Decide: append (existing collection) or write (new)?
            existing_manifest = self._unified_storage._load_manifest(collection)
            if existing_manifest is None:
                # NEW collection — write() creates the first manifest.
                # Stamp cross-lens metadata so other lenses know this
                # is a KV collection with key_col="_key".
                commit_hash = self._unified_storage.write(
                    collection, rows, key_col="_key",
                    row_group_size=10_000,
                    message=message or f"{collection} unified commit")
                self.stamp_collection_metadata(
                    collection, lens_type="keyvalue", key_col="_key",
                    schema_hint={"_key": "string", "value": "bytes"})
            else:
                # EXISTING collection — append via CRDT shard
                commit_hash = self._unified_storage.append(
                    collection, rows, key_col="_key",
                    row_group_size=10_000,
                    message=message or f"{collection} unified commit")
                # Compact shards into HEAD so branch/merge/history see
                # the latest data (append uses shards, but version
                # control needs HEAD to be current).
                # See NOTE above about compact_after_commit flag.
                if self._compact_after_commit:
                    self._unified_storage.compact_shards(collection)
        else:
            commit_hash = ""

        del self._unified_buffer[collection]

        # Notify attached indexer (EAGER mode auto-refresh).
        # This is a no-op if no indexer is attached.
        if self._attached_indexer is not None:
            try:
                self._attached_indexer.notify_write(collection)
            except Exception:
                pass  # indexer notification is best-effort

        return commit_hash

    def attach_indexer(self, indexer) -> None:
        """Attach a CollectionMetadata or CollectionIndexer for auto-notify.

        After attaching, every commit() call will automatically notify
        the indexer (triggering EAGER refresh or LAZY staleness increment).

        Usage:
            meta = CollectionMetadata(kernel)
            meta.register_eager_index('users', 'by_name', extractor, scan_fn)
            lens.attach_indexer(meta)
            # Now every lens.commit('users', ...) auto-refreshes EAGER indexes
        """
        self._attached_indexer = indexer

    def build_zone_maps(self, *args) -> None:
        """Build zone maps for a KV collection (explicit, not auto).

        Collection-agnostic API: build_zone_maps(collection)
        Backward compat API:    build_zone_maps()  [uses default collection]

        DEPRECATED: Zone maps were a legacy pruning extension for the
        ProllyTreeIndex backend (per-blob min/max stats). The unified
        storage architecture uses manifest-level inline stats for
        pruning instead (1 zone map per row group of 10K rows = negligible
        overhead, auto-built at write time).

        This method is kept for API compatibility but is a no-op in
        unified mode. The legacy pruning extension (collection_metadata,
        pruning, pruning_reader) has been moved to archive/.
        """
        collection, rest = self._resolve_collection(*args)
        # No-op: zone maps are superseded by manifest-level stats.
        # Kept for API compatibility — callers that invoke this method
        # will not crash, but no zone maps are built.
        return

    # --- Read path ---

    def get(self, *args) -> Optional[Any]:
        """Read a single value by key. O(1) cold via manifest point_lookup.

        Collection-agnostic API: get(collection, key)
        Backward compat API:    get(key)  [requires name in __init__]

        Unified storage path: 4 GETs cold point lookup via manifest +
        encoded predicate eval.

        CROSS-LENS: if the collection was created by another lens (e.g.
        lakehouse "users" with key_col="id"), this reads metadata.key_col
        and looks up the row by that column. The returned value is the
        FULL ROW (as a dict), not just a "value" field — because a
        lakehouse row has many columns, not just key+value.
        """
        collection, rest = self._resolve_collection(*args)
        key = rest[0]
        self._require_unified()

        key_col = self._resolve_key_col(collection)
        row = self._unified_storage.point_lookup(collection, key=key)
        if row is None:
            return None
        # KV-created collection: return the decoded "value" field
        if key_col == "_key" and "value" in row:
            return self.decode(row["value"])
        # Cross-lens: return the full row dict (caller sees all columns)
        return row

    def get_raw(self, *args) -> Optional[bytes]:
        """Read raw bytes by key (no decode).

        Reads the row via point_lookup and returns the raw value bytes.
        """
        collection, rest = self._resolve_collection(*args)
        key = rest[0]
        self._require_unified()

        row = self._unified_storage.point_lookup(collection, key=key)
        if row is None:
            return None
        # Return the value bytes if present, else None
        return row.get("value")

    def get_all(self, *args) -> dict[str, Any]:
        """Read all key→value pairs from the collection.

        CROSS-LENS: for a KV-created collection, returns {key: value}.
        For any other lens's collection (lakehouse, vector, streaming),
        returns {row[key_col]: full_row_dict} — the full row is the
        "value" because the collection has more than just key+value.
        """
        collection, rest = self._resolve_collection(*args)
        self._require_unified()

        key_col = self._resolve_key_col(collection)
        rows = self._unified_storage.read_with_shards(collection)
        if key_col == "_key":
            # KV-created: return decoded values
            return {r["_key"]: self.decode(r["value"])
                    for r in rows if r.get("_key")}
        # Cross-lens: return full rows keyed by key_col
        return {str(r.get(key_col)): r for r in rows if r.get(key_col) is not None}

    def keys(self, *args) -> list[str]:
        """List all user keys in the collection (excludes internal _ keys).

        CROSS-LENS: works on any collection — returns the values of the
        key_col column (from metadata), or "_key" if KV-created.
        Uses read_with_shards to merge HEAD + all shards (CRDT).
        """
        collection, rest = self._resolve_collection(*args)
        self._require_unified()

        key_col = self._resolve_key_col(collection)
        rows = self._unified_storage.read_with_shards(collection, columns=[key_col])
        return [str(r[key_col]) for r in rows if r.get(key_col) is not None]

    def exists(self, *args) -> bool:
        """Check if a key exists in the collection (merges HEAD + shards)."""
        collection, rest = self._resolve_collection(*args)
        key = rest[0]
        self._require_unified()

        # Use read_with_shards to see HEAD + all shards
        key_col = self._resolve_key_col(collection)
        rows = self._unified_storage.read_with_shards(collection, columns=[key_col])
        return any(str(r.get(key_col)) == str(key) for r in rows)

    def count(self, *args) -> int:
        """Count user keys in the collection."""
        collection, rest = self._resolve_collection(*args)
        self._require_unified()
        return len(self.keys(collection))

    # ------------------------------------------------------------------
    # Collection-like API — make a collection feel like an iterable of rows.
    # Uses the LensQuery lazy query API (row_query.py).
    # ------------------------------------------------------------------

    def iterate(self, *args):
        """Iterate over decoded rows in the collection.

        Collection-agnostic: iterate(collection)
        Backward compat:    iterate()  [uses default collection]
        """
        collection, rest = self._resolve_collection(*args)
        self._require_unified()

        rows = self._unified_storage.read(collection,
                                            columns=["_key", "value"])
        for row in rows:
            yield self.decode(row["value"])

    def __iter__(self):
        """Backward compat: iterate over default collection."""
        if self._default_collection is None:
            raise TypeError("lens is not bound to a default collection; use iterate(collection)")
        return self.iterate()

    def __len__(self):
        """Backward compat: len(lens) == lens.count()."""
        if self._default_collection is None:
            raise TypeError("lens is not bound to a default collection; use count(collection)")
        return self.count()

    def __contains__(self, key: str):
        """Backward compat: key in lens == lens.exists(key)."""
        if self._default_collection is None:
            raise TypeError("lens is not bound to a default collection; use exists(collection, key)")
        return self.exists(key)

    def where(self, *args, **kwargs) -> LensQuery:
        """Start a lazy query that filters rows.

        Collection-agnostic: where(collection, predicate=None, **kwargs)
        Backward compat:    where(predicate=None, **kwargs)  [uses default]
        """
        collection, rest = self._resolve_collection(*args)
        adapter = _CollectionAdapter(self, collection)
        # rest may contain the predicate if in compat mode
        if rest and callable(rest[0]):
            return LensQuery(adapter).where(rest[0], **kwargs)
        elif rest and isinstance(rest[0], dict):
            return LensQuery(adapter).where(rest[0], **kwargs)
        else:
            return LensQuery(adapter).where(**kwargs)

    def select(self, *args) -> LensQuery:
        """Start a lazy query that projects rows to only these fields."""
        collection, rest = self._resolve_collection(*args)
        adapter = _CollectionAdapter(self, collection)
        return LensQuery(adapter).select(*rest)

    def map(self, *args) -> LensQuery:
        """Start a lazy query that transforms each row via fn(row)."""
        collection, rest = self._resolve_collection(*args)
        adapter = _CollectionAdapter(self, collection)
        return LensQuery(adapter).map(rest[0])

    def join(self, *args):
        """JOIN this collection with another collection or query."""
        collection, rest = self._resolve_collection(*args)
        adapter = _CollectionAdapter(self, collection)
        return LensQuery(adapter).join(rest[0], rest[1])

    # --- Pruning-accelerated read (Vortex-style predicate pushdown) ---

    def read_with_pruning(self, *args, **kwargs):
        """Scan a collection with Vortex-style predicate pushdown.

        Collection-agnostic API: read_with_pruning(collection, predicates=None, row_filter=None)
        Backward compat API:    read_with_pruning(predicates=None, row_filter=None)

        Reads zone maps first (small, cheap), evaluates the pruning
        predicate, and only fetches + decodes data blobs that MIGHT match.
        Skips blobs whose zone maps prove they can't match — WITHOUT
        reading or decoding the data blob.

        Args:
            collection: collection name (or omitted if using default)
            predicates: list of (column, op, value) tuples for pruning.
                Example: [("age", ">", 30), ("region", "=", "US")]
                All predicates are ANDed together.
                If None, no pruning (reads all blobs).
            row_filter: optional function(row_dict) -> bool for exact
                row-level filtering after pruning.

        Yields:
            Rows (dicts) from non-pruned blobs (optionally filtered).
        """
        collection, rest = self._resolve_collection(*args)
        predicates = rest[0] if len(rest) > 0 else kwargs.get("predicates")
        row_filter = rest[1] if len(rest) > 1 else kwargs.get("row_filter")

        try:
            from collection_metadata import CollectionMetadata
            from pruning import PruningPredicate, ColumnPredicate
            from pruning_reader import PruningReader
            meta = CollectionMetadata(self.kernel)
            zm_index = meta.zm_index
        except ImportError:
            zm_index = None

        if zm_index is None or not zm_index.has_zone_maps(collection):
            # No pruning extension or no zone maps — fall back to full scan
            for row in self.iterate(collection):
                if row_filter is None or row_filter(row):
                    yield row
            return

        # Build pruning predicate
        predicate = None
        if predicates:
            col_preds = [ColumnPredicate(column=c, op=o, value=v)
                         for c, o, v in predicates]
            predicate = PruningPredicate(col_preds, combine="and")

        reader = PruningReader(self.kernel, zm_index, collection, predicate)

        # Decode function: JSON bytes → row dict
        for row in reader.scan(decode_fn=self.decode, row_filter=row_filter):
            yield row

    # --- Version control (delegates to UnifiedStorage) ---

    def branch(self, *args) -> str:
        """Create a branch on the collection. O(1) — just a ref copy."""
        collection, rest = self._resolve_collection(*args)
        self._require_unified()
        return self._unified_storage.branch(collection, rest[0])

    def checkout(self, *args) -> None:
        """Checkout a branch on the collection."""
        collection, rest = self._resolve_collection(*args)
        self._require_unified()
        self._unified_storage.checkout(collection, rest[0])

    def list_branches(self, *args) -> list[str]:
        """List all branches on the collection."""
        collection, rest = self._resolve_collection(*args)
        self._require_unified()
        return self._unified_storage.list_branches(collection)

    def merge(self, *args) -> str:
        """Merge a branch into the collection's HEAD. Union merge with 2-parent commit."""
        collection, rest = self._resolve_collection(*args)
        msg = rest[1] if len(rest) > 1 else ""
        self._require_unified()
        return self._unified_storage.merge(collection, rest[0], msg)

    def undo(self, *args) -> str:
        """Undo the last N commits on the collection."""
        collection, rest = self._resolve_collection(*args)
        steps = rest[0] if rest else 1
        self._require_unified()
        return self._unified_storage.undo(collection, steps)

    def history(self, *args) -> list[dict]:
        """Walk the commit chain for the collection."""
        collection, rest = self._resolve_collection(*args)
        limit = rest[0] if rest else 100
        self._require_unified()
        return self._unified_storage.history(collection, limit)

    def diff(self, *args) -> dict:
        """Diff two commits on the collection."""
        collection, rest = self._resolve_collection(*args)
        self._require_unified()
        return self._unified_storage.diff(collection, rest[0], rest[1])

    # --- Serialization (override in subclass) ---

    def encode(self, data: Any) -> bytes:
        # Handle raw bytes natively for git blobs, notebook attachments,
        # video segments, etc.
        if isinstance(data, (bytes, bytearray)):
            return bytes(data)
        return json.dumps(data, sort_keys=True).encode()

    def decode(self, data: bytes) -> Any:
        # Return raw bytes if not JSON
        try:
            return json.loads(data)
        except (json.JSONDecodeError, UnicodeDecodeError, ValueError):
            return bytes(data)


# ---------------------------------------------------------------------------
# _CollectionAdapter — adapts a (KeyValueLens, collection) pair to the
# LensQuery interface (keys()/get()). This lets LensQuery work with the
# collection-agnostic lens API.
# ---------------------------------------------------------------------------

class _CollectionAdapter:
    """Adapter that exposes keys()/get() for a specific collection.

    LensQuery uses duck-typing (hasattr source 'keys' and 'get'). This
    adapter wraps a (lens, collection) pair to provide those methods.
    """

    def __init__(self, lens: KeyValueLens, collection: str):
        self._lens = lens
        self._collection = collection

    def keys(self) -> list[str]:
        return self._lens.keys(self._collection)

    def get(self, key: str):
        return self._lens.get(self._collection, key)


# ===========================================================================
# KeylessLens — KeyValueLens variant that auto-generates UUIDv7 keys.
#
# The "auto-key" pattern only makes sense for KV-style storage (per-row
# keyed). KeylessLens stays in this file as a thin subclass.
# ===========================================================================

class KeylessLens(KeyValueLens):
    """KeyValueLens variant that auto-generates UUIDv7 primary keys.

    Use this when your data does not have a natural primary key:
    event logs, time-series, metrics, append-only streams, audit
    trails. The lens generates a UUIDv7 for each row; the caller
    receives the key from put() and can use it for later retrieval.

    UUIDv7 is time-ordered, making it suitable for distributed generation
    and range scans via UnifiedStorage.

    COLLECTION-AGNOSTIC: Like KeyValueLens, KeylessLens is a stateless
    engine. Pass the collection name to each operation:

        lens = KeylessLens(kernel)
        key = lens.put("events", {"event": "click", "user": "u1"})
        lens.commit("events", "log click")
    """

    def put(self, *args) -> str:
        """Stage data with an auto-generated UUIDv7 key. Returns the key.

        Collection-agnostic API: put(collection, key=None, data)
        Backward compat API:    put(key=None, data)  [requires name in __init__]

        The key MUST be None for KeylessLens. If you want to supply your
        own keys, use the regular KeyValueLens class.
        """
        collection, rest = self._resolve_collection(*args)
        if len(rest) == 2:
            # put(collection, key, data) or put(key, data) in compat mode
            key, data = rest[0], rest[1]
            if key is not None:
                raise TypeError(
                    "KeylessLens.put() does not accept a key. "
                    "Pass key=None, or use the regular KeyValueLens class."
                )
        elif len(rest) == 1:
            # put(data) — key omitted
            data = rest[0]
        else:
            raise TypeError(f"put() expects 1-3 args, got {len(rest)}")
        return self.put_auto(collection, data)

    def put_many(self, *args) -> list[str]:
        """Stage multiple rows, each with an auto-generated key.

        Collection-agnostic: put_many(collection, rows)
        Backward compat:    put_many(rows)  [requires name in __init__]
        """
        collection, rest = self._resolve_collection(*args)
        rows = rest[0]
        return [self.put_auto(collection, row) for row in rows]


# SemanticLens/OssieAdapter are in extensions/semantic/. CollectionIndexer is in
# extensions/indexing/. Both are imported lazily on attribute access to
# avoid circular imports.
def __getattr__(name):
    if name in ("SemanticLens", "SemanticView", "OssieLens", "OssieSemanticLens",
                "OssieAdapter", "SemanticModelAdapter"):
        try:
            from extensions.semantic.ossie import SemanticLens, OssieAdapter
            from extensions.semantic.base import SemanticModelAdapter
            if name == "SemanticLens" or name == "SemanticView":
                return SemanticLens
            if name in ("OssieLens", "OssieSemanticLens"):
                return SemanticLens
            if name == "OssieAdapter":
                return OssieAdapter
            if name == "SemanticModelAdapter":
                return SemanticModelAdapter
        except ImportError:
            pass
    if name in ("IndexedLens", "IndexedView"):
        # DEPRECATED: IndexedLens has been removed. Use CollectionMetadata.
        import warnings
        warnings.warn(
            "IndexedLens has been removed. Use CollectionMetadata instead: "
            "from collection_metadata import CollectionMetadata",
            DeprecationWarning, stacklevel=2
        )
        raise AttributeError(
            "IndexedLens has been removed. Use CollectionMetadata: "
            "from collection_metadata import CollectionMetadata"
        )
    raise AttributeError(f"module 'keyvalue_lens' has no attribute '{name}'")
