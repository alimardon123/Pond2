"""
Pond minimal kernel — searching for the smallest primitive basis.

The experiment (per architecture review):
  Remove one primitive at a time. For each:
    - Remove it.
    - Rebuild all Lenses.
    - If all Lenses still work, the primitive is not fundamental.
    - Continue until nothing else can be removed.

This is searching for a minimal basis, like finding the smallest CPU
instruction set or the smallest relational algebra.

Current hypothesis: the minimal basis is 3 primitives:
  1. Write(bytes) -> hash     (create immutable content-addressed blob)
  2. Read(hash_or_name) -> bytes  (fetch blob by hash, or resolve name then fetch)
  3. Reference(name, hash)    (mutable name -> hash mapping)

Everything else is derived:
  - Tree   = blob containing serialized {name -> hash}
  - Commit = blob containing serialized {tree_hash, parent_hash, ...}
  - Tag    = Reference(name, commit_hash)
  - Branch = Reference(name, commit_hash)
  - OPEN/SEALED = Lens-level buffer + Write
  - Snapshot = Read at a hash
  - Time travel = walk parent pointers (Lens-level)
  - GC = walk reachability from root References

This file implements the minimal kernel. The Views (in views_minimal.py)
build Tree/Commit/etc. as patterns over these 3 primitives — without
any kernel support for those concepts.
"""

from __future__ import annotations

import os
import json
import time
import sqlite3
import hashlib
from typing import Optional


# ---------------------------------------------------------------------------
# Content addressing
# ---------------------------------------------------------------------------

def hash_bytes(data: bytes) -> str:
    """SHA-256 of bytes, hex-encoded. The content address."""
    return hashlib.sha256(data).hexdigest()


# ---------------------------------------------------------------------------
# The minimal kernel — 3 primitives
# ---------------------------------------------------------------------------

class PondMinimal:
    """
    The minimal Pond kernel. Three primitives only:

      1. Write(bytes) -> hash
         Create an immutable, content-addressed blob. Returns its hash.
         The same bytes always produce the same hash (dedup for free).

      2. Read(hash_or_name) -> bytes
         If given a 64-char hex hash, return that blob's bytes.
         If given a name, resolve via the root namespace, then read.

      3. Reference(name, hash)
         Set a mutable name -> hash mapping in the root namespace.
         This is the ONLY mutable operation in the kernel.

    That's it. No Tree. No Commit. No OPEN/SEALED. No lifecycle.
    Those are all Lens-level patterns built from these 3 primitives.

    v0: single-node, local filesystem, SQLite root store, no replication.
    """

    def __init__(self, base_dir: str):
        self.base_dir = os.path.abspath(base_dir)
        self.pond_dir = os.path.join(self.base_dir, ".pond")
        self.objects_dir = os.path.join(self.pond_dir, "objects")
        self.root_store_path = os.path.join(self.pond_dir, "roots.sqlite")

        os.makedirs(self.objects_dir, exist_ok=True)

        # Root pointer namespace — the ONLY mutable state.
        # check_same_thread=False allows the connection to be used from
        # worker threads (UnifiedStorage uses a ThreadPoolExecutor for
        # parallel I/O). We guard all writes with _db_lock to serialize
        # SQLite mutations (SQLite handles concurrent reads fine via WAL,
        # but cross-thread writes without a lock raise ProgrammingError).
        import threading
        self._db_lock = threading.RLock()
        self._closed = False
        self.root_db = sqlite3.connect(
            self.root_store_path, isolation_level=None,
            check_same_thread=False,
        )
        self.root_db.execute("""
            CREATE TABLE IF NOT EXISTS roots (
                name TEXT PRIMARY KEY,
                hash TEXT NOT NULL,
                updated_at REAL NOT NULL
            )
        """)

        self.stats = {"writes": 0, "reads": 0, "references": 0}

    def _ensure_open(self) -> None:
        """Raise RuntimeError if the kernel has been closed.

        MUST be called while holding _db_lock so that the check and the
        subsequent root_db access are atomic with respect to close().
        """
        if self._closed:
            raise RuntimeError(
                "PondMinimal kernel is closed — the roots.sqlite connection "
                "was closed by a previous close() call. Create a new "
                "PondMinimal instance to continue."
            )

    # ------------------------------------------------------------------
    # Primitive 1: Write
    # ------------------------------------------------------------------

    def write(self, data: bytes) -> str:
        """Create an immutable, content-addressed blob. Returns its hash.
        The same bytes always produce the same hash (dedup for free).
        There is no OPEN state, no lifecycle — bytes go in, hash comes out."""
        h = hash_bytes(data)
        shard_dir = os.path.join(self.objects_dir, h[:2])
        os.makedirs(shard_dir, exist_ok=True)
        path = os.path.join(shard_dir, h + ".bin")
        if not os.path.exists(path):  # dedup
            with open(path, "wb") as f:
                f.write(data)
        self.stats["writes"] += 1
        return h

    def write_batch(self, items: list[bytes]) -> list[str]:
        """Write a batch of blobs in parallel (thread pool).

        This is a SAME-COLLECTION I/O performance primitive — it batches
        multiple `write()` calls into a thread pool to amortize per-call
        overhead. It is NOT cross-collection atomicity (A7 law still
        holds: the kernel provides no way to atomically update multiple
        refs/names). Each blob is written independently; a crash mid-batch
        leaves some blobs written and others not.

        Local FS is fast, but parallel writes still help when there are
        many blobs to write.
        """
        if not items:
            return []
        if len(items) == 1:
            return [self.write(items[0])]

        from concurrent.futures import ThreadPoolExecutor
        results: list[Optional[str]] = [None] * len(items)
        errors: list[Optional[Exception]] = [None] * len(items)

        def _put_one(idx, data):
            try:
                results[idx] = self.write(data)
            except Exception as e:
                errors[idx] = e

        workers = min(16, len(items))
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = [pool.submit(_put_one, i, d)
                        for i, d in enumerate(items)]
            for f in futures:
                f.result()

        for e in errors:
            if e is not None:
                raise e
        return results

    # ------------------------------------------------------------------
    # Primitive 2: Read
    # ------------------------------------------------------------------

    def read(self, hash_or_name: str) -> bytes:
        """Read a blob. If given a 64-char hex hash, read that blob directly.
        If given a name, resolve it via the root namespace, then read."""
        self.stats["reads"] += 1

        if len(hash_or_name) == 64 and all(c in "0123456789abcdef" for c in hash_or_name):
            h = hash_or_name
        else:
            h = self.resolve(hash_or_name)
            if h is None:
                raise ValueError(f"Name '{hash_or_name}' not bound in root namespace")

        path = self._blob_path(h)
        if not os.path.exists(path):
            raise ValueError(f"Blob {h} not found on disk")
        with open(path, "rb") as f:
            return f.read()

    def read_blob(self, h: str) -> bytes:
        """Read a blob by hash directly (no name resolution)."""
        self.stats["reads"] += 1
        path = self._blob_path(h)
        if not os.path.exists(path):
            raise ValueError(f"Blob {h} not found on disk")
        with open(path, "rb") as f:
            return f.read()

    def read_blob_batch(self, hashes: list[str]) -> list[bytes]:
        """Fetch a batch of blobs in parallel (thread pool).

        Same-collection I/O performance primitive (like write_batch).
        NOT cross-collection atomicity — each blob is read independently.
        """
        if not hashes:
            return []
        if len(hashes) == 1:
            return [self.read_blob(hashes[0])]

        from concurrent.futures import ThreadPoolExecutor
        results: list[Optional[bytes]] = [None] * len(hashes)
        errors: list[Optional[Exception]] = [None] * len(hashes)

        def _get_one(idx, h):
            try:
                results[idx] = self.read_blob(h)
            except Exception as e:
                errors[idx] = e

        workers = min(16, len(hashes))
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = [pool.submit(_get_one, i, h)
                        for i, h in enumerate(hashes)]
            for f in futures:
                f.result()

        for e in errors:
            if e is not None:
                raise e
        return results

    # ------------------------------------------------------------------
    # Primitive 3: Reference
    # ------------------------------------------------------------------

    def reference(self, name: str, h: str) -> None:
        """Set a mutable name -> hash mapping. The ONLY mutable operation.
        The hash must refer to an existing blob (we verify)."""
        if not os.path.exists(self._blob_path(h)):
            raise ValueError(f"Hash {h} does not refer to an existing blob")
        with self._db_lock:
            self._ensure_open()
            self.root_db.execute(
                "INSERT OR REPLACE INTO roots (name, hash, updated_at) VALUES (?, ?, ?)",
                (name, h, time.time())
            )
        self.stats["references"] += 1

    def resolve(self, name: str) -> Optional[str]:
        """Resolve a name to its current hash. Returns None if unbound."""
        with self._db_lock:
            self._ensure_open()
            cur = self.root_db.execute("SELECT hash FROM roots WHERE name = ?", (name,))
            row = cur.fetchone()
        return row[0] if row else None

    def list_names(self) -> list[str]:
        with self._db_lock:
            self._ensure_open()
            cur = self.root_db.execute("SELECT name FROM roots ORDER BY name")
            return [row[0] for row in cur.fetchall()]

    def delete_reference(self, name: str) -> bool:
        """Delete a name -> hash binding. Returns True if it existed.

        Maintenance primitive (used by SDK compact_tombstones) so the SDK
        no longer needs to reach into kernel.root_db with raw SQL — that
        bypassed _db_lock and raced with concurrent resolve()/close().
        """
        with self._db_lock:
            self._ensure_open()
            cur = self.root_db.execute(
                "DELETE FROM roots WHERE name = ?", (name,)
            )
            return cur.rowcount > 0

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _blob_path(self, h: str) -> str:
        return os.path.join(self.objects_dir, h[:2], h + ".bin")

    def storage_stats(self) -> dict:
        data_bytes = 0
        blob_count = 0
        for shard in os.listdir(self.objects_dir):
            shard_path = os.path.join(self.objects_dir, shard)
            if not os.path.isdir(shard_path):
                continue
            for f in os.listdir(shard_path):
                if f.endswith(".bin"):
                    data_bytes += os.path.getsize(os.path.join(shard_path, f))
                    blob_count += 1
        return {
            **self.stats,
            "data_bytes": data_bytes,
            "blob_count": blob_count,
            "name_count": len(self.list_names()),
        }

    def close(self) -> None:
        """Close the kernel's root store. Thread-safe and idempotent.

        Takes _db_lock so it can never interleave with a concurrent
        resolve()/reference()/list_names() call from a background thread
        (UnifiedStorage fires daemon tombstone/vacuum threads from
        compact() — closing the SQLite connection mid-execute used to
        SEGFAULT the whole process). Late callers raise RuntimeError
        (via _ensure_open) instead of touching the closed connection.
        """
        with self._db_lock:
            if self._closed:
                return
            self._closed = True
            self.root_db.close()
