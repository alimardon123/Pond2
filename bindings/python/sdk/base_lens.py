"""
PondLens — the shared base class for all Python Lenses.

All 5 production Python lenses extend this class: KeyValueLens, LakehouseLens,
VectorLens, StreamingLens, OLTPLens. PondLens provides shared convenience methods
for branching, collection listing, history, and definition management that would
otherwise be duplicated across every lens.

Rust lenses do NOT use this class — they own a UnifiedStorage instance
directly and add workload-specific methods.

This class is kept for backward compatibility with existing Python lenses
that extend it. New Python lenses should follow the Rust pattern:

    class MyLens:
        def __init__(self, storage: UnifiedStorage):
            self.storage = storage
        # ... workload-specific methods ...

Existing lenses (KeyValueLens, LakehouseLens, etc.) still extend PondLens
but this is a historical artifact, not a requirement. The base class
adds indirection without value — all its methods just delegate to
UnifiedStorage.

This is NOT a format-aware base class. Per the design goals:

  - UnifiedStorage (PND2 + CollectionManifest) is the universal
    storage backend for ALL workloads. It supports OLTP, OLAP,
    streaming, vector, KV, and point-lookup workloads.
  - App-facing lenses (KeyValueLens, LakehouseLens, VectorLens,
    FeatureStoreLens) inherit from PondLens and add their OWN
    read/write APIs. The base class does NOT decide what to write —
    each lens decides for itself.
  - LakehouseLens ADDS SQL query (DuckDB) on top of the unified
    storage as a lens-specific extension. Other lenses do not get it.

What this base provides:
  - Shared ref namespace:
      collections/{name}/definition                   → schema hash (collection-level)
      collections/{name}/_branches/{branch}/commit     → commit hash
      collections/{name}/_branches/{branch}/manifest   → manifest hash
      collections/{name}/_branches/{branch}/shards/{uuid} → shard refs
  - Generic ref-level operations that work on ANY collection's refs,
    regardless of what is inside the blobs:
      - branch(name, branch_name)        — O(1) ref copy
      - list_collections()               — lists all collection names
      - collection_exists(name)
      - set_definition(name, definition) — optional lens-specific metadata
      - get_definition(name)
      - history(name)                    — walks the commit chain

What this base does NOT provide (deliberately):
  - read_collection(name)  — no universal read; each lens reads its own format
  - write_parquet(...)     — Lakehouse-specific, lives on LakehouseLens
  - put/get/commit(...)    — KV-specific, lives on Lens (via ProllyLensBase)
  - _detect_format(...)    — there is no format detection at this layer

History works for both binary commits (ProllyLensBase KV collections)
and JSON commits (Lakehouse/FeatureStore Parquet collections) because
the commit chain is just a parent-pointer walk — the encoding of each
commit blob does not matter at this layer.
"""

from __future__ import annotations

import os
import sys
import json
import time
from typing import Optional, Any

# Make bindings/python/core importable
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "bindings/python/core"))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from kernel import PondMinimal  # noqa: E402

# BinaryProllyTree import removed — unified architecture uses JSON commits only.
# Legacy binary commits are handled by UnifiedStorage.history() which tries
# JSON first, then falls back to binary decode if needed.
BinaryProllyTree = None


class PondLens:
    """Shared namespace base for all Pond Lenses.

    This class is deliberately small. It only owns:
      1. The ref namespace conventions (collections/{name}/...).
      2. Generic operations that operate on REFS, not on blob contents.
      3. A history() walker that handles both binary and JSON commits.

    App-facing subclasses (Lens, LakehouseLens, FeatureStoreLens, ...)
    inherit from this class and add their OWN read/write APIs. The base
    class does not know whether a collection stores Parquet blobs, KV
    pairs, or anything else — it only knows about the commit chain
    (HEAD → commit → parent → ...).

    See DESIGN_GOALS.md §3 (the seven principles) and the worklog entry
    for this refactor.
    """

    def __init__(self, kernel: PondMinimal):
        self.kernel = kernel

    # ==================================================================
    # Ref namespace helpers (shared by ALL lenses)
    # ==================================================================

    @staticmethod
    def _head_ref(name: str) -> str:
        """DEPRECATED: HEAD ref is eliminated.

        Returns the default branch's commit ref (branches/main/commit).
        With the HEAD ref eliminated, the 'current commit' for a collection
        is whatever the active branch points at — and the default active
        branch is 'main'. External callers that need the active branch's
        commit ref should use UnifiedStorage._active_commit_ref() instead.
        """
        return f"collections/{name}/_branches/main/commit"

    @staticmethod
    def _branch_ref(name: str, branch: str) -> str:
        return f"collections/{name}/_branches/{branch}/commit"

    @staticmethod
    def _definition_ref(name: str) -> str:
        return f"collections/{name}/definition"

    # ==================================================================
    # Generic operations on the namespace (no format awareness)
    # ==================================================================

    def branch(self, name: str, branch_name: str) -> str:
        """Create a branch on ANY collection. O(1) — just a ref copy.

        Works for any collection regardless of whether its blobs are
        Parquet, KV, or something else, because branching only copies
        the active branch's commit ref to a new branch ref. The blobs
        are not touched.
        """
        head = self.kernel.resolve(self._head_ref(name))
        if head is None:
            raise KeyError(f"Collection '{name}' not found")
        self.kernel.reference(self._branch_ref(name, branch_name), head)
        return head

    def collection_exists(self, name: str) -> bool:
        """Check if a collection exists (has a definition or a main branch commit)."""
        # The definition ref is stamped by every lens via
        # stamp_collection_metadata() — it's the canonical "this collection
        # exists" marker. Fall back to the default branch's commit ref for
        # collections created by old code paths that don't stamp a definition.
        if self.kernel.resolve(self._definition_ref(name)) is not None:
            return True
        return self.kernel.resolve(self._head_ref(name)) is not None

    def list_collections(self, namespace: Optional[str] = None) -> list[str]:
        """List ALL collections (any lens, any format).

        Collections are identified by the `collections/{name}/definition` ref
        (stamped by every lens via stamp_collection_metadata()). This works
        for any lens because they all share the same namespace convention.

        HIERARCHICAL NAMESPACES:
            Collection names can contain `/` for hierarchical organization:
              "events"              → top-level
              "dev/events"          → under "dev" namespace
              "dev/team1/events"    → under "dev/team1" namespace
              "prod/analytics/2024/events"  → 4 levels deep

            This is dynamic depth — use as many levels as you need.

        Args:
            namespace: optional namespace prefix to filter by.
                e.g., "dev" returns ["dev/events", "dev/users"] but not
                "prod/events" or "events".

        Returns:
            Sorted list of collection names.
        """
        names = self.kernel.list_names()
        collections = set()
        for n in names:
            # CURRENT format: collections/{name}/definition
            if n.startswith("collections/") and n.endswith("/definition"):
                coll = n[len("collections/"):-len("/definition")]
                if not coll or coll in ("_branches", "branches"):
                    continue
                if namespace and not (coll == namespace or coll.startswith(namespace + "/")):
                    continue
                collections.add(coll)
                continue
            # CURRENT format: collections/{name}/_branches/main/commit (fallback)
            if n.startswith("collections/") and n.endswith("/_branches/main/commit"):
                coll = n[len("collections/"):-len("/_branches/main/commit")]
                if not coll:
                    continue
                if namespace and not (coll == namespace or coll.startswith(namespace + "/")):
                    continue
                collections.add(coll)
                continue
            # LEGACY format: collections/{name}/branches/main/commit (without underscore)
            if n.startswith("collections/") and n.endswith("/branches/main/commit"):
                coll = n[len("collections/"):-len("/branches/main/commit")]
                if not coll:
                    continue
                if namespace and not (coll == namespace or coll.startswith(namespace + "/")):
                    continue
                collections.add(coll)
                continue
            # LEGACY format: r/{name}/definition (previous short layout)
            if n.startswith("r/") and n.endswith("/definition"):
                coll = n[len("r/"):-len("/definition")]
                if not coll:
                    continue
                if namespace and not (coll == namespace or coll.startswith(namespace + "/")):
                    continue
                collections.add(coll)
                continue
            # LEGACY format: r/{name}/main/commit
            if n.startswith("r/") and n.endswith("/main/commit"):
                coll = n[len("r/"):-len("/main/commit")]
                if not coll:
                    continue
                if namespace and not (coll == namespace or coll.startswith(namespace + "/")):
                    continue
                collections.add(coll)
        return sorted(collections)

    def list_namespaces(self, parent: Optional[str] = None) -> list[str]:
        """List namespaces (one level deep) under a parent namespace.

        HIERARCHICAL NAMESPACES:
            Namespaces are derived from collection names using `/` as
            the separator. This method returns the distinct namespace
            names at the next level.

        Examples:
            Collections: ["dev/events", "dev/users", "prod/events", "logs"]
            list_namespaces() → ["dev", "logs", "prod"]
            list_namespaces("dev") → ["events", "users"]
            list_namespaces("dev/events") → []  (no sub-namespaces)

        Args:
            parent: optional parent namespace. If None, returns top-level
                namespaces.

        Returns:
            Sorted list of namespace names at the next level.
        """
        all_collections = self.list_collections()
        namespaces = set()
        for coll in all_collections:
            parts = coll.split("/")
            if parent:
                # Filter: collection must be under the parent namespace
                parent_parts = parent.split("/")
                if len(parts) <= len(parent_parts):
                    continue
                if parts[:len(parent_parts)] != parent_parts:
                    continue
                # The next level is parts[len(parent_parts)]
                namespaces.add(parts[len(parent_parts)])
            else:
                # Top level: first path segment
                if len(parts) > 1:
                    namespaces.add(parts[0])
                else:
                    # Top-level collection (no namespace) — include it
                    namespaces.add(coll)
        return sorted(namespaces)

    def set_definition(self, name: str, definition: dict) -> str:
        """Store Lens-specific metadata for a collection (optional).

        This is the only "metadata" the base class knows about. The
        definition blob is a JSON dict stored at
        `collections/{name}/definition`. Each lens decides what to put
        in it (feature definitions, table schema, vector index config,
        etc.). The base class treats it as opaque JSON.

        Cross-lens contract: when a lens creates a collection, it SHOULD
        call stamp_collection_metadata() so other lenses can identify
        the collection's type. See get_collection_metadata().
        """
        defn_bytes = json.dumps(definition, sort_keys=True).encode()
        defn_hash = self.kernel.write(defn_bytes)
        self.kernel.reference(self._definition_ref(name), defn_hash)
        return defn_hash

    def get_definition(self, name: str) -> Optional[dict]:
        """Read Lens-specific metadata for a collection."""
        h = self.kernel.resolve(self._definition_ref(name))
        if h is None:
            return None
        return json.loads(self.kernel.read(h))

    # ==================================================================
    # Cross-lens collection metadata
    #
    # Every collection in Pond is just PND2 blobs + a manifest at
    # collections/{name}/manifest. ANY lens can read ANY collection
    # through PondStorage. To make this pleasant, the creating lens
    # stamps a small metadata blob so other lenses know what shape
    # to expect (which lens created it, what the key column is, etc).
    #
    # This is the ONLY cross-lens contract. There is no CrossLens
    # helper, no adapter, no special-case code path. Every lens reads
    # every collection through the same PondStorage.read() API.
    # ==================================================================

    def stamp_collection_metadata(self, name: str, *,
                                   lens_type: str,
                                   key_col: Optional[str] = None,
                                   schema_hint: Optional[dict] = None,
                                   extra: Optional[dict] = None) -> str:
        """Stamp cross-lens metadata onto a collection.

        Called by every lens when it creates a collection. The metadata
        is stored at collections/{name}/definition (merged with any
        existing definition) so other lenses can identify the
        collection's type without parsing the data.

        Standard keys (recognized by ALL lenses):
          - lens_type: str — which lens created this collection
              ("lakehouse", "keyvalue", "vector", "streaming", "feature_store", ...)
          - lens_version: str — semver of the creating lens
          - key_col: Optional[str] — the column to use as the sort key
              for point lookups (None = row index)
          - schema_hint: Optional[dict] — {column_name: type_str} for
              the columns the lens expects to find
          - created_at: float — unix timestamp
          - extra: Optional[dict] — lens-specific fields (vector
              dimensions, streaming segment_size, etc.)

        Returns the definition blob hash.
        """
        defn = self.get_definition(name) or {}
        # Merge cross-lens fields without clobbering lens-specific ones
        defn["lens_type"] = lens_type
        defn["lens_version"] = "1.0"
        if key_col is not None:
            defn["key_col"] = key_col
        if schema_hint is not None:
            defn["schema_hint"] = schema_hint
        defn["created_at"] = time.time()
        if extra:
            defn.setdefault("extra", {}).update(extra)
        return self.set_definition(name, defn)

    def get_collection_metadata(self, name: str) -> dict:
        """Read cross-lens metadata for a collection.

        Returns a dict with at least {"lens_type": str|None}. If the
        collection has no metadata (e.g. created by an old lens version
        before this contract), returns {"lens_type": None}.

        Any lens can call this on ANY collection to learn what shape
        to expect before reading. This is the "small metadata about
        which lens created it" the user asked for.
        """
        defn = self.get_definition(name)
        if defn is None:
            return {"lens_type": None}
        return {
            "lens_type": defn.get("lens_type"),
            "lens_version": defn.get("lens_version"),
            "key_col": defn.get("key_col"),
            "schema_hint": defn.get("schema_hint"),
            "created_at": defn.get("created_at"),
            "extra": defn.get("extra", {}),
            "raw": defn,  # full definition for lens-specific fields
        }

    def list_collections_with_metadata(self) -> list[dict]:
        """List ALL collections with their cross-lens metadata.

        Returns a list of dicts, one per collection:
          {"name": str, "lens_type": str|None, "key_col": str|None,
           "schema_hint": dict|None, "created_at": float|None}

        Any lens can call this to see the entire pond: every collection
        created by any lens, with type info so the user knows what
        shape each collection is in.
        """
        out = []
        for name in self.list_collections():
            md = self.get_collection_metadata(name)
            out.append({
                "name": name,
                "lens_type": md.get("lens_type"),
                "key_col": md.get("key_col"),
                "schema_hint": md.get("schema_hint"),
                "created_at": md.get("created_at"),
            })
        return out

    # ==================================================================
    # History — walks commit chain for ANY collection
    # ==================================================================

    def history(self, name: str, limit: int = 100) -> list[dict]:
        """Walk the commit chain for ANY collection.

        Works for JSON commits (the unified commit format).
        Legacy binary commits are no longer supported (BinaryProllyTree
        import removed).

        Returns a unified list of dicts:
          {hash, message, parent, second_parent, timestamp, type, ...}

        The walk stops at the first commit that cannot be decoded
        (e.g. a tombstone or a foreign format) to avoid silent
        corruption.
        """
        head = self.kernel.resolve(self._head_ref(name))
        if head is None:
            return []

        history: list[dict] = []
        current: Optional[str] = head
        seen: set[str] = set()  # cycle guard

        while current and current not in seen and len(history) < limit:
            seen.add(current)
            raw = self.kernel.read_blob(current)
            entry = self._decode_commit_entry(current, raw)
            if entry is None:
                # Cannot decode — stop the walk to avoid silent corruption.
                history.append({
                    "hash": current,
                    "message": "(undecodable commit)",
                    "parent": None,
                    "second_parent": None,
                    "timestamp": None,
                    "type": "unknown",
                })
                break
            history.append(entry)
            current = entry.get("parent")

        return history

    @staticmethod
    def _decode_commit_entry(commit_hash: str, raw: bytes) -> Optional[dict]:
        """Decode a commit blob into a unified history entry.

        Tries JSON first (the unified commit format). Returns None if
        the commit cannot be decoded as JSON.
        """
        # JSON commit (unified architecture — the only format)
        try:
            commit = json.loads(raw)
            if isinstance(commit, dict):
                entry_type = "merge" if commit.get("second_parent") else "commit"
                return {
                    "hash": commit_hash,
                    "message": commit.get("message", ""),
                    "parent": commit.get("parent"),
                    "second_parent": commit.get("second_parent"),
                    "timestamp": commit.get("timestamp"),
                    "row_count": commit.get("row_count"),
                    "type": entry_type,
                }
        except (json.JSONDecodeError, UnicodeDecodeError):
            pass

        return None
