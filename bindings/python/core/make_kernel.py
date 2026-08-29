"""Unified kernel factory — one entry point for all storage backends.

Switching between local FS and S3 is ONE line:

    # Local filesystem (pure files, no SQLite):
    kernel = make_kernel("file:///path/to/.pond")

    # S3:
    kernel = make_kernel("s3://my-bucket/prod", region="us-east-1")

Both return an ObjectStoreNativeKernel backed by the appropriate
store. The kernel code, SDK, lenses — everything else is identical.

URL schemes:
  file://      — local filesystem store
  s3://        — S3-compatible object store
  memory://    — in-memory store (pure Python, for tests)

BACKEND SELECTION (C5-python phase 1 — substrate delegation):
  The object-store LAYER can run on the Rust core instead of the
  pure-Python stores. `pond.ObjectStore` (pyo3) exposes the Rust core's
  LocalFS + S3/R2 backends, and RustObjectStore
  (bindings/python/core/rust_object_store.py) implements the exact
  LocalFSObjectStore/S3ObjectStore duck interface on top of it — the
  LAYOUT IS BYTE-IDENTICAL (blobs at blobs/{h[:2]}/{h}, refs at {path}
  as JSON {"hash": ...}), so stores written by either backend are
  readable by the other (and by the Rust kernel/CLI).

  backend="auto" (default):
      Use the Rust backend when the compiled `pond` module is importable
      and construction succeeds; fall back to the pure-Python stores
      (with a ONE-TIME stderr note) otherwise.
  backend="rust":
      Require the Rust backend — import/construction errors propagate.
  backend="python":
      Always use the pure-Python stores (LocalFSObjectStore /
      S3ObjectStore on boto3). Byte-identical behavior to the
      pre-delegation code paths.
  Environment: when the kwarg is "auto" (the default),
      POND_PY_BACKEND=python|rust|auto overrides it. An explicit
      backend= kwarg always wins over the environment variable.
  memory:// is NEVER routed through Rust (InMemoryObjectStore, pure
      Python — tests rely on it).

For tests, use file:// with a tempdir — local FS is fast enough and
exercises the real on-disk code path (catches layout bugs, validates
restart persistence).

For S3, credentials are picked up from the environment (AWS_ACCESS_KEY_ID,
AWS_SECRET_ACCESS_KEY, AWS_REGION) or the backend's default credential
chain. You can override with explicit kwargs.
"""
from __future__ import annotations

import os
import sys
from typing import Optional
from urllib.parse import urlparse

# Make bindings/python/core importable
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# One-time stderr note when the Rust backend is requested-but-unavailable
# and we fall back to pure Python (auto mode only). Repeating it on every
# make_kernel() call would spam long-lived services.
_FALLBACK_NOTE_EMITTED = False


def _note_python_fallback(reason: BaseException) -> None:
    global _FALLBACK_NOTE_EMITTED
    if not _FALLBACK_NOTE_EMITTED:
        print(
            f"pond: Rust object store unavailable ({reason}); "
            f"using pure-Python backend",
            file=sys.stderr,
        )
        _FALLBACK_NOTE_EMITTED = True


def _resolve_backend(backend: str) -> str:
    """Apply POND_PY_BACKEND: the env var is the DEFAULT when the kwarg is
    'auto' (an explicit kwarg always wins). Validates the final value."""
    requested = backend
    if requested == "auto":
        env = (os.environ.get("POND_PY_BACKEND") or "auto").strip().lower()
        requested = env or "auto"
    if requested not in ("auto", "rust", "python"):
        raise ValueError(
            f"Invalid backend {backend!r} "
            f"(POND_PY_BACKEND={os.environ.get('POND_PY_BACKEND')!r}). "
            f"Use 'auto', 'rust', or 'python'."
        )
    return requested


def _try_rust_local(base_dir: str, requested: str):
    """Rust LocalFS store, or None on auto-fallback (note on stderr)."""
    try:
        from rust_object_store import RustObjectStore
        return RustObjectStore.local(base_dir)
    except Exception as e:  # ImportError (pond missing) or construction error
        if requested == "rust":
            raise
        _note_python_fallback(e)
        return None


def _rust_s3_url(parsed, kwargs) -> str:
    """Build the Rust S3 URL from the boto3-style kwargs make_kernel accepts.

    Mirrors pond.Storage's URL building: region/endpoint become query
    params; credentials are NOT embedded in the URL (they go to env vars
    in _try_rust_s3, like Storage::new does).
    """
    bucket = parsed.netloc
    prefix = parsed.path.lstrip("/")
    region = kwargs.get("region") or os.environ.get("AWS_REGION", "us-east-1")
    endpoint_url = kwargs.get("endpoint_url")
    url = f"s3://{bucket}/{prefix}"
    params = []
    if region:
        params.append(f"region={region}")
    if endpoint_url:
        params.append(f"endpoint={endpoint_url}")
    if params:
        url += "?" + "&".join(params)
    return url


def _try_rust_s3(parsed, kwargs, requested: str):
    """Rust S3 store (SigV4 + 3-tier cache), or None on auto-fallback."""
    try:
        from rust_object_store import RustObjectStore
        url = _rust_s3_url(parsed, kwargs)
        # Credentials → env vars (S3ObjectStore::from_url reads them;
        # same pattern as pond.Storage's constructor).
        ak = kwargs.get("aws_access_key_id")
        sk = kwargs.get("aws_secret_access_key")
        if ak and sk:
            os.environ["AWS_ACCESS_KEY_ID"] = ak
            os.environ["AWS_SECRET_ACCESS_KEY"] = sk
        cache_dir = kwargs.get("cache_dir")
        return RustObjectStore.from_s3(url, cache_dir=cache_dir)
    except Exception as e:
        if requested == "rust":
            raise
        _note_python_fallback(e)
        return None


def _python_s3_store(parsed, kwargs):
    """The original boto3-backed S3ObjectStore path (byte-identical to the
    pre-delegation behavior)."""
    from s3_object_store import S3ObjectStore
    import boto3
    from botocore.config import Config
    bucket = parsed.netloc
    prefix = parsed.path.lstrip("/")
    region = kwargs.get("region") or os.environ.get("AWS_REGION", "us-east-1")
    endpoint_url = kwargs.get("endpoint_url")
    aws_access_key_id = kwargs.get("aws_access_key_id")
    aws_secret_access_key = kwargs.get("aws_secret_access_key")
    # Production retry/timeout config (overrides boto3 defaults)
    max_retries = kwargs.get("max_retries", 10)
    connect_timeout = kwargs.get("connect_timeout", 5.0)
    read_timeout = kwargs.get("read_timeout", 30.0)
    max_pool_connections = kwargs.get("max_pool_connections", 50)
    config = Config(
        connect_timeout=connect_timeout,
        read_timeout=read_timeout,
        max_pool_connections=max_pool_connections,
        retries={"max_attempts": max_retries, "mode": "adaptive"},
    )
    client = boto3.client(
        "s3",
        region_name=region,
        endpoint_url=endpoint_url,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        config=config,
    )
    return S3ObjectStore(client, bucket=bucket, prefix=prefix)


def make_kernel(url: str, backend: str = "auto", **kwargs) -> "ObjectStoreNativeKernel":
    """Create a Pond kernel backed by the storage backend identified by the URL.

    Args:
        url: storage URL. Supported schemes:
            "file:///path/to/.pond"  — local filesystem
            "s3://bucket/prefix"    — S3-compatible object store
            "memory://"             — in-memory (pure Python, for tests)
        backend: object-store backend for the file:// and s3:// paths:
            "auto"    — prefer the Rust core (pond.ObjectStore via pyo3)
                        when importable, fall back to the pure-Python
                        stores with a one-time stderr note (default; the
                        POND_PY_BACKEND env var overrides this default)
            "rust"    — require the Rust backend (errors propagate)
            "python"  — always the pure-Python stores (byte-identical to
                        the pre-delegation code paths)
        **kwargs: backend-specific options:
            For S3: region, endpoint_url, aws_access_key_id,
            aws_secret_access_key, cache_dir (Rust backend only)
            For local/memory: ignored

    Returns:
        An ObjectStoreNativeKernel instance backed by the appropriate store.

    Examples:
        # Local FS (auto: Rust store when the pond module is built):
        kernel = make_kernel("file:///var/lib/pond")

        # Force the pure-Python stores:
        kernel = make_kernel("file:///var/lib/pond", backend="python")

        # S3 (auto: Rust S3 client + 3-tier disk cache when built):
        kernel = make_kernel("s3://my-pond/prod", region="us-east-1")

        # Then use PondStorage as usual:
        from pond_storage import PondStorage
        storage = PondStorage(kernel)
    """
    from object_store_native_kernel import ObjectStoreNativeKernel

    requested = _resolve_backend(backend)

    parsed = urlparse(url)
    scheme = parsed.scheme

    if scheme == "memory":
        # Pure-Python in-memory store — never routed through Rust.
        from object_store_native_kernel import InMemoryObjectStore
        return ObjectStoreNativeKernel(InMemoryObjectStore())

    if scheme == "file" or (not scheme and parsed.path):
        # Local filesystem
        base_dir = parsed.path if parsed.path else url
        store = None
        if requested in ("auto", "rust"):
            store = _try_rust_local(base_dir, requested)
        if store is None:
            from local_fs_object_store import LocalFSObjectStore
            store = LocalFSObjectStore(base_dir)
        return ObjectStoreNativeKernel(store)

    elif scheme == "s3":
        store = None
        if requested in ("auto", "rust"):
            store = _try_rust_s3(parsed, kwargs, requested)
        if store is None:
            store = _python_s3_store(parsed, kwargs)
        return ObjectStoreNativeKernel(store)

    else:
        raise ValueError(
            f"Unsupported storage URL scheme '{scheme}'. "
            f"Use 'file://' or 's3://'."
        )
