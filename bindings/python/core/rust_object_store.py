"""RustObjectStore — the pure-Python kernel stack's object store, backed by
the Rust core via pyo3 (`pond.ObjectStore`).

This is PHASE 1 of C5-python (ARCHITECTURE.md D1/D8 — substrate
delegation): the Python world keeps its formats and semantics; what it
delegates is the I/O layer. ObjectStoreNativeKernel, UnifiedStorage, the
SDK and the lens world run UNCHANGED on top of this store and inherit
the Rust core's LocalFS + S3/R2 backends (SigV4 signing, connection
pooling, the 3-tier disk cache) instead of Python's own implementations.

BYTE-IDENTICAL LAYOUT with LocalFSObjectStore/S3ObjectStore:
  Blobs:  blobs/{hash[:2]}/{hash}    (content-addressed, sha256 — the
                                      Rust put_blob computes the same
                                      sha256 hex hash as kernel.hash_bytes)
  Refs:   {path}                       (JSON body {"hash": "..."})

OLD-layout fallback reads (pre-layout-change stores) go through the Rust
store's raw-key escape hatch (`get_raw`/`delete_raw`/`list_raw`):
  Old blobs: b/{hash[:2]}/{hash}
  Old refs:  paths/{path}

Usage:
    from rust_object_store import RustObjectStore

    store = RustObjectStore.local("/path/to/.pond")       # local FS
    store = RustObjectStore.from_s3("s3://bucket/prefix?region=us-east-1")

    kernel = ObjectStoreNativeKernel(store)               # same kernel
    storage = PondStorage(kernel)                          # same SDK

    # Or via the factory (auto-detects Rust availability):
    kernel = make_kernel("file:///path/to/.pond", backend="auto")

Duck-compatibility: every method LocalFSObjectStore/S3ObjectStore expose
to the kernel + SDK is implemented here with the same semantics —
including the exact exception types (a missing blob raises KeyError,
matching LocalFSObjectStore.get_blob; callers catch specific types).
"""
from __future__ import annotations

import json
import sys
import os
import threading
from typing import Optional

# Make bindings/python/core importable when this module is imported from
# elsewhere (same pattern as local_fs_object_store.py).
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def _looks_like_not_found(exc: BaseException) -> bool:
    """True when an OSError from the Rust store means 'key absent'.

    The Rust backends surface NotFound as an io::Error whose message is
    `Blob '{h}' not found` (LocalFS) or `S3 returned 404: ...` (S3).
    Real I/O failures (permissions, transport errors, 5xx) don't match
    and are propagated to the caller unchanged.
    """
    msg = str(exc).lower()
    return "not found" in msg or " 404" in msg or "returned 404" in msg


def _is_unsupported(exc: BaseException) -> bool:
    """True when the Rust store's raw escape hatch is Unsupported.

    The trait's default impls raise exactly
    `"{op} not supported by this ObjectStore"` — e.g. from
    CachingObjectStore, which deliberately does not implement raw ops
    (they would bypass the cache layers).
    """
    return "not supported by this objectstore" in str(exc).lower()


class RustObjectStore:
    """Object store backed by the Rust core (pond.ObjectStore) via pyo3.

    Byte-identical layouts to LocalFSObjectStore/S3ObjectStore:
      blobs at blobs/{h[:2]}/{h}, refs at {path} as JSON {"hash": ...}.
    Old-layout fallback reads (b/{h[:2]}/{h}, paths/{path}) go through
    the Rust store's raw-key escape hatch.
    """

    def __init__(self, rust_store, base_dir: Optional[str] = None,
                 bucket: Optional[str] = None, prefix: Optional[str] = None):
        """Wrap a `pond.ObjectStore` instance.

        Prefer the `local()` / `from_s3()` classmethods — they construct
        the Rust handle AND record the location metadata (base_dir /
        bucket / prefix) that kernel.base_dir duck-compat needs. Direct
        construction is for callers that already hold a pond.ObjectStore
        (tests, share-with-Storage setups).
        """
        self._rs = rust_store
        self._base_dir = os.path.abspath(base_dir) if base_dir else None
        # _bucket/_prefix only exist on S3-flavored instances so that
        # hasattr()-based duck checks (ObjectStoreNativeKernel.base_dir)
        # classify this store exactly like they classify S3ObjectStore.
        if bucket is not None:
            self._bucket = bucket
            self._prefix = prefix or ""
        # Capability probe state for the raw escape hatch: None = unknown
        # (probe on first raw op), True = supported, False = unsupported
        # (skip all raw fallbacks thereafter — raise the normal error).
        self._raw_ok: Optional[bool] = None

        self._lock = threading.Lock()

        # Honest stats (same shape as LocalFSObjectStore / S3ObjectStore /
        # InMemoryObjectStore — the kernel prints these; the Rust store's
        # internal counters are not exposed, so they are maintained here).
        self.stats = {
            "gets": 0,
            "puts": 0,
            "bytes_read": 0,
            "bytes_written": 0,
            "latency_ms_total": 0.0,
        }

    # ------------------------------------------------------------------
    # Constructors
    # ------------------------------------------------------------------

    @classmethod
    def local(cls, base_dir: str) -> "RustObjectStore":
        """Create a store over a local directory (Rust LocalFS backend)."""
        import pond  # local import so absence of the module raises here
        rs = pond.ObjectStore(base_dir)
        return cls(rs, base_dir=base_dir)

    @classmethod
    def from_s3(cls, url: str, cache_dir: Optional[str] = None) -> "RustObjectStore":
        """Create a store over S3/R2 (Rust S3 backend + 3-tier cache).

        `url` is a full Rust S3 URL:
            s3://bucket/prefix?region=us-east-1&endpoint=http://...
        Credentials come from AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY
        (set them in the environment before calling). `cache_dir` follows
        resolve_cache_dir (None → POND_CACHE_DIR → ~/.pond_cache;
        'off'/'none' disables).

        NOTE: with the cache wrapper active the raw escape hatch is
        unsupported by design — old-layout fallback reads then read as
        absent. Pass cache_dir='off' if legacy-layout reads must work.
        """
        import pond
        rs = pond.ObjectStore.from_s3(url, cache_dir=cache_dir)
        # Parse bucket/prefix for kernel.base_dir duck-compat.
        from urllib.parse import urlparse, unquote
        parsed = urlparse(url)
        bucket = parsed.netloc or ""
        prefix = unquote(parsed.path or "").strip("/")
        return cls(rs, bucket=bucket, prefix=prefix)

    # ------------------------------------------------------------------
    # Location duck-compat (ObjectStoreNativeKernel.base_dir property)
    # ------------------------------------------------------------------

    @property
    def base_dir(self) -> str:
        """The base directory (local stores) — mirrors LocalFSObjectStore.

        Only present on local instances (construct via `local()`); raises
        AttributeError otherwise so hasattr() duck-checks behave exactly
        like LocalFSObjectStore's (the kernel's base_dir property then
        falls through to the _bucket branch for S3 stores).
        """
        if self._base_dir is None:
            raise AttributeError(
                "base_dir is only available on local RustObjectStore "
                "instances (constructed via RustObjectStore.local)")
        return self._base_dir

    # ------------------------------------------------------------------
    # Raw escape hatch helpers (capability-probed)
    # ------------------------------------------------------------------

    def _get_raw_or_none(self, key: str) -> Optional[bytes]:
        """get_raw with the Unsupported capability probe.

        Returns the bytes, or None when the key is absent. Returns None
        (and permanently disables raw fallbacks) when the backend doesn't
        support raw ops at all.
        """
        if self._raw_ok is False:
            return None
        try:
            data = self._rs.get_raw(key)
        except OSError as e:
            if _is_unsupported(e):
                self._raw_ok = False
                return None
            raise
        self._raw_ok = True
        return data

    def _delete_raw_quiet(self, key: str) -> bool:
        """delete_raw with the capability probe; False when unsupported."""
        if self._raw_ok is False:
            return False
        try:
            deleted = self._rs.delete_raw(key)
        except OSError as e:
            if _is_unsupported(e):
                self._raw_ok = False
                return False
            raise
        self._raw_ok = True
        return bool(deleted)

    # ------------------------------------------------------------------
    # Content-addressed blob operations
    # ------------------------------------------------------------------

    def put_blob(self, data: bytes) -> str:
        """Write bytes, content-addressed. Returns the content hash.

        The Rust put_blob computes sha256(data) hex — the SAME hash
        kernel.hash_bytes returns for the same bytes (parity pinned by
        tests), so ids are interchangeable across the Python/Rust worlds.
        """
        h = self._rs.put_blob(data)
        with self._lock:
            self.stats["puts"] += 1
            self.stats["bytes_written"] += len(data)
        return h

    def get_blob(self, hash_val: str) -> bytes:
        """Read bytes by content hash.

        Falls back to the OLD blob layout (b/{h[:2]}/{h}) via the raw
        escape hatch, exactly like LocalFSObjectStore. A missing blob
        raises KeyError — the SAME exception type (and message shape)
        LocalFSObjectStore raises; kernel/SDK callers catch that.
        """
        try:
            data = self._rs.get_blob(hash_val)
        except OSError as e:
            if not _looks_like_not_found(e):
                raise  # real I/O failure — propagate (LocalFS would too)
            # OLD-layout fallback read (b/{h[:2]}/{h}) via the raw hatch.
            data = self._get_raw_or_none(f"b/{hash_val[:2]}/{hash_val}")
            if data is None:
                raise KeyError(f"Blob {hash_val} not found on disk") from None
        with self._lock:
            self.stats["gets"] += 1
            self.stats["bytes_read"] += len(data)
        return data

    def put_blob_batch(self, items: list[bytes],
                       max_workers: int = 16) -> list[str]:
        """Write a batch of blobs — the Rust side parallelizes off-GIL
        (S3 uses a native thread pool), so no Python thread pool is needed.
        """
        if not items:
            return []
        hashes = self._rs.put_blob_batch(list(items))
        with self._lock:
            self.stats["puts"] += len(items)
            self.stats["bytes_written"] += sum(len(d) for d in items)
        return hashes

    def get_blob_batch(self, hash_vals: list[str],
                       max_workers: int = 32) -> list[bytes]:
        """Fetch a batch of blobs (Rust side parallelizes off-GIL).

        Mirrors LocalFSObjectStore's per-blob semantics: on failure the
        batch degrades to per-blob get_blob so the old-layout fallback
        applies per blob and a genuinely missing blob raises the same
        KeyError LocalFSObjectStore's batch path raises.
        """
        if not hash_vals:
            return []
        try:
            results = self._rs.get_blob_batch(list(hash_vals))
        except OSError:
            # Per-blob degradation: get_blob already accounts stats per
            # blob (identical to LocalFSObjectStore's batch path, which is
            # just per-blob get_blob) — don't double-count here.
            return [self.get_blob(h) for h in hash_vals]
        with self._lock:
            self.stats["gets"] += len(hash_vals)
            self.stats["bytes_read"] += sum(len(r) for r in results)
        return results

    def has_blob(self, hash_val: str) -> bool:
        """Check if a blob exists.

        The Rust blob_exists checks the NEW layout only (a HEAD is much
        cheaper than fetching the body, so no raw probe here); OLD-layout
        existence still surfaces through get_blob's fallback path — the
        same trade LocalFSObjectStore makes for anything but exact reads.
        """
        return bool(self._rs.blob_exists(hash_val))

    def delete_blob(self, hash_val: str) -> bool:
        """Delete a blob by hash — from BOTH new and old layouts
        (matching LocalFSObjectStore, which removes both files)."""
        deleted = bool(self._rs.delete_blob(hash_val))
        if self._delete_raw_quiet(f"b/{hash_val[:2]}/{hash_val}"):
            deleted = True
        return deleted

    def list_all_blob_hashes(self) -> list[str]:
        """List all blob hashes in the store (for GC reachability).

        Scans both NEW (blobs/) and OLD (b/) locations through the raw
        escape hatch. When raw listing is unsupported (cache-wrapped S3)
        or the blob trees are absent, returns [] — the same result
        LocalFSObjectStore produces when its blob directories are missing.
        """
        hashes: list[str] = []
        for prefix in ("blobs/", "b/"):
            if self._raw_ok is False:
                break
            try:
                keys = self._rs.list_raw(prefix)
            except OSError as e:
                if _is_unsupported(e):
                    self._raw_ok = False
                    break
                raise
            self._raw_ok = True
            for key in keys:
                # key shape: "{tree}/{shard}/{hash}"
                parts = key.split("/", 2)
                if len(parts) == 3 and parts[2]:
                    hashes.append(parts[2])
        return list(set(hashes))

    # ------------------------------------------------------------------
    # Named path operations (well-known refs)
    # ------------------------------------------------------------------

    def put_path(self, path: str, hash_val: str) -> None:
        """Bind a well-known path to a content hash.

        The Rust put_path writes the same JSON ref body ({"hash": "..."})
        at the same key ({path}) — byte-identical layout.
        """
        self._rs.put_path(path, hash_val)
        with self._lock:
            self.stats["puts"] += 1

    def get_path(self, path: str) -> Optional[str]:
        """Resolve a well-known path to its current content hash.

        Resolution order (mirrors LocalFSObjectStore):
          1. NEW ref at {path}           (Rust get_path — handles the
                                          canonical no-space JSON spelling)
          2. NEW ref body via raw hatch  (Python json.loads — handles
                                          Python-written refs, which carry
                                          a space after the colon)
          3. OLD ref at paths/{path}     (raw hatch + Python parse)
        """
        h = self._rs.get_path(path)
        if h is None:
            # Re-read the NEW-layout ref body through the raw hatch and
            # parse it in Python: json.loads accepts BOTH the Rust
            # ({"hash":"x"}) and Python ({"hash": "x"}) spellings, and
            # the pure-Python stores write the latter.
            body = self._get_raw_or_none(path)
            if body is not None:
                h = self._parse_ref_body(body)
        if h is None:
            # OLD-layout ref: paths/{path}
            body = self._get_raw_or_none(f"paths/{path}")
            if body is not None:
                h = self._parse_ref_body(body)
        if h is not None:
            with self._lock:
                self.stats["gets"] += 1
        return h

    @staticmethod
    def _parse_ref_body(body: bytes) -> Optional[str]:
        """Parse a JSON ref body ({"hash": "..."}) leniently."""
        try:
            data = json.loads(body)
        except (ValueError, TypeError):
            return None
        if isinstance(data, dict):
            return data.get("hash")
        return None

    def delete_path(self, path: str) -> bool:
        """Delete a named path from BOTH new and old layouts
        (matching LocalFSObjectStore). True if either existed."""
        deleted = bool(self._rs.delete_path(path))
        if self._delete_raw_quiet(f"paths/{path}"):
            deleted = True
        return deleted

    def list_paths(self, prefix: str = "") -> list[str]:
        """List all paths (refs) with the given prefix.

        Shape-parity with LocalFSObjectStore.list_paths (that's what
        ObjectStoreNativeKernel.list_names / list_paths_with_prefix
        consume):
          - A prefix under a known top-level dir (collections/,
            transactions/, r/, paths/) scans DIRECTLY under it; results
            are store-relative (the prefix is included).
          - Any other prefix scans the known ref trees (collections/,
            transactions/, r/) with the prefix appended, PLUS the legacy
            paths/ tree (whose results are listed WITHOUT the paths/
            component, like LocalFS does).
          - Blob keys (blobs/, b/) are always excluded; results are
            sorted + deduplicated.
        """
        paths: list[str] = []
        known_prefixes = ("collections/", "transactions/", "r/", "paths/")
        if prefix.startswith(known_prefixes):
            for p in self._rs.list_paths(prefix):
                if p.startswith("blobs/") or p.startswith("b/"):
                    continue
                paths.append(p)
        else:
            for dirname in ("collections", "transactions", "r"):
                sub = f"{dirname}/{prefix}" if prefix else dirname
                for p in self._rs.list_paths(sub):
                    if p.startswith("blobs/") or p.startswith("b/"):
                        continue
                    paths.append(p)
            # Legacy "paths/" tree — results relative to paths/ (matches
            # LocalFSObjectStore's old-layout listing).
            old_sub = f"paths/{prefix}" if prefix else "paths"
            for p in self._rs.list_paths(old_sub):
                paths.append(p[len("paths/"):] if p.startswith("paths/") else p)
        return sorted(set(paths))

    # ------------------------------------------------------------------
    # Stats (same interface as LocalFSObjectStore / S3ObjectStore)
    # ------------------------------------------------------------------

    def reset_stats(self) -> None:
        """Reset the I/O stats (for benchmarking)."""
        with self._lock:
            self.stats = {
                "gets": 0, "puts": 0,
                "bytes_read": 0, "bytes_written": 0,
                "latency_ms_total": 0.0,
            }

    def print_stats(self, label: str = "") -> None:
        """Print I/O stats."""
        if label:
            print(f"  [{label}]")
        print(f"    GETs:           {self.stats['gets']:,}")
        print(f"    PUTs:           {self.stats['puts']:,}")
        print(f"    Bytes read:     {self.stats['bytes_read']:,}")
        print(f"    Bytes written:  {self.stats['bytes_written']:,}")
