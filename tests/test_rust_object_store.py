#!/usr/bin/env python3
"""Tests for the Rust-object-store substrate delegation (C5-python phase 1).

Covers the `pond.ObjectStore` pyo3 surface, the `RustObjectStore` Python
adapter, and the `make_kernel(backend=...)` wiring:

  1. Byte-interop LocalFS⇄Rust in BOTH directions (identical bytes AND
     identical on-disk file trees).
  2. hash equality across the boundary (sha256 == kernel.hash_bytes).
  3. Old-layout fallback reads (b/{h[:2]}/{h} blobs, paths/{p} refs).
  4. ObjectStoreNativeKernel duck-parity on the Rust store (incl. stats).
  5. UnifiedStorage write/read/point-lookup/predicate/append round trip.
  6. PondStorage (SDK) round trip via make_kernel(backend="rust").
  7. make_kernel backend selection (python/rust/auto + POND_PY_BACKEND).
  8. Batch operations (put_blob_batch/get_blob_batch) + stats accounting.
  9. Moto-mocked S3 via RustObjectStore.from_s3 (the Rust S3 client —
     proves the Python world needs NO boto3 client of its own).
  10. Exception-type parity for missing blobs (KeyError, like LocalFS).

All tests SKIP GRACEFULLY when `import pond` fails (pure-Python-only
environments stay green). The moto test additionally skips when
moto/boto3 are not installed.

Run:
    PYTHONPATH=target/release:bindings/python/core:bindings/python/sdk:\
bindings/python/sdk/extensions/physical_structures \
        python3 -m pytest tests/test_rust_object_store.py -v
"""

import json
import os
import shutil
import socket
import sys
import tempfile
import time

import pytest

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Make the pure-Python core + SDK + the compiled pond module importable
# even when PYTHONPATH is not preset (CI sets it; local runs may not).
for _p in (
    os.path.join(REPO_ROOT, "bindings", "python", "core"),
    os.path.join(REPO_ROOT, "bindings", "python", "sdk"),
    os.path.join(REPO_ROOT, "bindings", "python", "sdk", "extensions", "physical_structures"),
    os.path.join(REPO_ROOT, "target", "release"),
):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from local_fs_object_store import LocalFSObjectStore          # noqa: E402
from object_store_native_kernel import ObjectStoreNativeKernel  # noqa: E402
from kernel import hash_bytes                                  # noqa: E402


def _pond():
    """Import the compiled pond module or skip the test."""
    try:
        import pond
    except ImportError as e:
        pytest.skip(
            f"pond module not importable ({e}) — build it with "
            f"`cargo build --release -p pond_python` (and symlink "
            f"target/release/pond.so if needed)"
        )
    return pond


@pytest.fixture()
def rust_store(tmp_path):
    """A RustObjectStore over a fresh temp dir."""
    _pond()
    from rust_object_store import RustObjectStore
    return RustObjectStore.local(str(tmp_path / "store"))


# ---------------------------------------------------------------------------
# 1 + 2. Byte interop LocalFS ⇄ Rust (both directions) + hash equality
# ---------------------------------------------------------------------------

BLOBS = [b"alpha", b"beta-beta", b"gamma" * 1000, b""]
REFS = [
    ("collections/t/_branches/main/commit", 0),   # → BLOBS[0]
    ("collections/t/_branches/dev/commit", 1),    # → BLOBS[1]
    ("transactions/tx-1", 2),                     # → BLOBS[2]
]


def _write_store(store):
    """Write the standard blob + ref pattern through any duck store."""
    hashes = [store.put_blob(b) for b in BLOBS]
    for path, idx in REFS:
        store.put_path(path, hashes[idx])
    return hashes


def _relative_files(root):
    """The set of relative file paths under a store dir (walked)."""
    found = set()
    for dirpath, _dirs, files in os.walk(root):
        for f in files:
            full = os.path.join(dirpath, f)
            rel = os.path.relpath(full, root).replace(os.sep, "/")
            found.add(rel)
    return found


def test_byte_interop_localfs_to_rust(tmp_path):
    """A store written by pure-Python LocalFSObjectStore reads identically
    through RustObjectStore (same dir, same files, same values)."""
    _pond()
    from rust_object_store import RustObjectStore

    dir_a = str(tmp_path / "a")
    py_store = LocalFSObjectStore(dir_a)
    hashes = _write_store(py_store)

    rs = RustObjectStore.local(dir_a)
    for i, blob in enumerate(BLOBS):
        assert rs.get_blob(hashes[i]) == blob, f"blob {i} mismatch"
    for path, idx in REFS:
        assert rs.get_path(path) == hashes[idx], f"ref {path} mismatch"
    # has_blob / list parity
    for h in hashes:
        assert rs.has_blob(h)
    assert set(rs.list_all_blob_hashes()) == set(hashes)
    # list_paths shape parity with the pure-Python store
    assert rs.list_paths("") == py_store.list_paths("")
    assert rs.list_paths("collections/t/") == py_store.list_paths("collections/t/")


def test_byte_interop_rust_to_localfs(tmp_path):
    """A store written through RustObjectStore reads identically through
    the pure-Python LocalFSObjectStore — AND the on-disk file trees are
    IDENTICAL (same relative paths, same blob bytes)."""
    _pond()
    from rust_object_store import RustObjectStore

    dir_py = str(tmp_path / "via_python")
    dir_rs = str(tmp_path / "via_rust")

    py_hashes = _write_store(LocalFSObjectStore(dir_py))
    rs_hashes = _write_store(RustObjectStore.local(dir_rs))

    # Hash equality across the boundary (same data ⇒ same content address).
    assert py_hashes == rs_hashes
    assert py_hashes == [hash_bytes(b) for b in BLOBS]

    # The pure-Python store reads everything the Rust store wrote.
    py_reader = LocalFSObjectStore(dir_rs)
    for i, blob in enumerate(BLOBS):
        assert py_reader.get_blob(rs_hashes[i]) == blob
    for path, idx in REFS:
        assert py_reader.get_path(path) == rs_hashes[idx]

    # Identical on-disk file trees (relative path sets).
    tree_py = _relative_files(dir_py)
    tree_rs = _relative_files(dir_rs)
    assert tree_py == tree_rs, (
        f"file trees differ:\n  only-python: {sorted(tree_py - tree_rs)}\n"
        f"  only-rust:   {sorted(tree_rs - tree_py)}"
    )
    # Blob file BYTES identical.
    for rel in sorted(tree_py):
        if rel.startswith("blobs/"):
            with open(os.path.join(dir_py, rel), "rb") as f:
                a = f.read()
            with open(os.path.join(dir_rs, rel), "rb") as f:
                b = f.read()
            assert a == b, f"blob bytes differ at {rel}"


def test_hash_bytes_equality(rust_store):
    """put_blob returns the SAME sha256 hex hash kernel.hash_bytes computes."""
    for blob in BLOBS:
        h = rust_store.put_blob(blob)
        assert h == hash_bytes(blob)
        assert len(h) == 64


# ---------------------------------------------------------------------------
# 3. Old-layout fallback reads (pre-layout-change stores)
# ---------------------------------------------------------------------------

def test_old_layout_blob_fallback(tmp_path):
    """A hand-created b/{h[:2]}/{h} blob resolves through get_blob."""
    _pond()
    from rust_object_store import RustObjectStore

    base = str(tmp_path / "old")
    os.makedirs(base)
    store = RustObjectStore.local(base)

    data = b"legacy-layout-payload"
    h = hash_bytes(data)
    old_blob = os.path.join(base, "b", h[:2], h)
    os.makedirs(os.path.dirname(old_blob))
    with open(old_blob, "wb") as f:
        f.write(data)

    # The NEW layout does not have it; the OLD layout does.
    assert not store.has_blob(h)   # blob_exists checks the new layout only
    assert store.get_blob(h) == data


def test_old_layout_ref_fallback(tmp_path):
    """Hand-created OLD refs (paths/{p}) AND Python-spelled NEW refs
    ({base}/{p} with json.dump's space after the colon) both resolve."""
    _pond()
    from rust_object_store import RustObjectStore

    base = str(tmp_path / "oldrefs")
    os.makedirs(base)
    store = RustObjectStore.local(base)

    data = b"ref-target"
    h = hash_bytes(data)

    # OLD layout ref: {base}/paths/{path} — Python json spelling.
    old_ref = os.path.join(base, "paths", "collections", "legacy", "HEAD")
    os.makedirs(os.path.dirname(old_ref))
    with open(old_ref, "w") as f:
        json.dump({"hash": h}, f)          # → {"hash": "<h>"} (with space)

    assert store.get_path("collections/legacy/HEAD") == h

    # NEW-layout ref written in PYTHON spelling (a pure-Python store wrote it).
    new_ref = os.path.join(base, "collections", "pywritten", "HEAD")
    os.makedirs(os.path.dirname(new_ref))
    with open(new_ref, "w") as f:
        json.dump({"hash": h}, f)

    assert store.get_path("collections/pywritten/HEAD") == h

    # The blob itself is only in the OLD layout → get_blob fallback.
    old_blob = os.path.join(base, "b", h[:2], h)
    os.makedirs(os.path.dirname(old_blob))
    with open(old_blob, "wb") as f:
        f.write(data)
    assert store.get_blob(h) == data
    # Old-layout blobs appear in list_all_blob_hashes (like LocalFS).
    assert h in store.list_all_blob_hashes()

    # delete_path removes BOTH layouts (like LocalFSObjectStore).
    assert store.delete_path("collections/legacy/HEAD")
    assert store.get_path("collections/legacy/HEAD") is None
    assert not os.path.exists(old_ref)


# ---------------------------------------------------------------------------
# 4. Kernel duck-parity on the Rust store (incl. stats)
# ---------------------------------------------------------------------------

def test_kernel_on_rust_store(rust_store):
    kernel = ObjectStoreNativeKernel(rust_store)

    data = b"kernel-payload"
    h = kernel.write(data)
    assert kernel.stats["writes"] == 1

    assert kernel.read_blob(h) == data
    assert kernel.stats["reads"] == 1

    ref = "collections/users/_branches/main/commit"
    kernel.reference(ref, h)
    assert kernel.stats["ref_writes"] == 1
    # reference() updates the path cache → resolve is a warm hit.
    assert kernel.resolve(ref) == h

    # read-your-write via name resolution (invalidate to force a cold GET).
    kernel.invalidate_root_cache()
    assert kernel.read(ref) == data
    assert kernel.stats["ref_reads"] == 1

    # list_names sees the ref.
    assert ref in kernel.list_names()

    # Store-level stats counted every op (duck-compat with LocalFS).
    s = rust_store.stats
    assert s["puts"] >= 2          # 1 blob PUT + 1 ref PUT
    assert s["gets"] >= 2          # 1 blob GET + ≥1 ref GET
    assert s["bytes_written"] >= len(data)
    assert s["bytes_read"] >= len(data)
    rust_store.reset_stats()
    assert rust_store.stats["puts"] == 0 and rust_store.stats["gets"] == 0


def test_kernel_base_dir_property(tmp_path, monkeypatch):
    """kernel.base_dir duck-compat: local Rust stores report the dir;
    S3-style stores report s3://bucket/prefix."""
    _pond()
    from rust_object_store import RustObjectStore

    k = ObjectStoreNativeKernel(RustObjectStore.local(str(tmp_path / "d")))
    assert k.base_dir == os.path.abspath(str(tmp_path / "d"))

    # S3-flavored store: construction only PARSES the URL (no network
    # I/O) but needs credentials in the environment.
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
    s3_store = RustObjectStore(
        _pond().ObjectStore("s3://fake-bucket/prod", cache_dir="off"),
        bucket="fake-bucket", prefix="prod")
    k2 = ObjectStoreNativeKernel(s3_store)
    assert k2.base_dir == "s3://fake-bucket/prod"


# ---------------------------------------------------------------------------
# 5. UnifiedStorage on the Rust store
# ---------------------------------------------------------------------------

def test_unified_storage_round_trip(rust_store):
    from unified_storage import UnifiedStorage

    kernel = ObjectStoreNativeKernel(rust_store)
    storage = UnifiedStorage(kernel)

    rows = [{"id": i, "age": i % 50, "name": f"user-{i}"} for i in range(100)]
    storage.write("t", rows, key_col="id", row_group_size=10)

    # Full read.
    got = storage.read("t")
    assert len(got) == 100
    assert {r["id"] for r in got} == {r["id"] for r in rows}

    # Point lookup.
    row = storage.point_lookup("t", key="9")
    assert row is not None and row["id"] == 9

    # Predicate read.
    got = storage.read("t", predicates=[("age", ">", 47)])
    assert {r["id"] for r in got} == {48, 49, 98, 99}

    # Append — history must NOT be lost (both commits' rows visible).
    more = [{"id": 100 + i, "age": i, "name": f"late-{i}"} for i in range(20)]
    storage.append("t", more, key_col="id", row_group_size=10)
    got = storage.read("t")
    assert len(got) == 120, f"append lost history: {len(got)} rows"
    assert {r["id"] for r in got} >= {r["id"] for r in rows} | {r["id"] for r in more}

    # A FRESH kernel over the same dir sees the same state (persistence
    # through the Rust store's on-disk layout).
    from rust_object_store import RustObjectStore
    base = rust_store.base_dir
    storage2 = UnifiedStorage(ObjectStoreNativeKernel(RustObjectStore.local(base)))
    assert len(storage2.read("t")) == 120


# ---------------------------------------------------------------------------
# 6. PondStorage SDK round trip via make_kernel(backend="rust")
# ---------------------------------------------------------------------------

def test_sdk_round_trip_make_kernel_rust(tmp_path):
    _pond()
    from make_kernel import make_kernel
    from pond_storage import PondStorage

    kernel = make_kernel(f"file://{tmp_path}/pond", backend="rust")
    assert type(kernel.store).__name__ == "RustObjectStore"

    storage = PondStorage(kernel)
    rows = [{"id": i, "name": f"u{i}"} for i in range(30)]
    storage.write("users", rows, key_col="id")

    got = storage.read("users")
    assert len(got) == 30
    assert {r["name"] for r in got} == {r["name"] for r in rows}

    assert storage.point_lookup("users", key="7")["name"] == "u7"
    assert storage.collection_exists("users")
    assert "users" in storage.list_collections()


def test_sdk_round_trip_make_kernel_auto(tmp_path):
    """backend="auto" must behave identically to "rust" when the module is
    importable (this test) and fall back cleanly otherwise (skipped then)."""
    _pond()
    from make_kernel import make_kernel
    from pond_storage import PondStorage

    kernel = make_kernel(f"file://{tmp_path}/pond", backend="auto")
    assert type(kernel.store).__name__ == "RustObjectStore"

    storage = PondStorage(kernel)
    rows = [{"id": 1, "v": "x"}, {"id": 2, "v": "y"}]
    storage.write("kv", rows, key_col="id")
    assert len(storage.read("kv")) == 2


# ---------------------------------------------------------------------------
# 7. make_kernel backend selection (incl. POND_PY_BACKEND)
# ---------------------------------------------------------------------------

def test_make_kernel_backend_python(tmp_path):
    from make_kernel import make_kernel
    from local_fs_object_store import LocalFSObjectStore

    kernel = make_kernel(f"file://{tmp_path}/py", backend="python")
    assert isinstance(kernel.store, LocalFSObjectStore)
    # Pure-Python path still works end to end.
    h = kernel.write(b"pure-python")
    assert kernel.read_blob(h) == b"pure-python"


def test_make_kernel_backend_rust(tmp_path):
    _pond()
    from make_kernel import make_kernel
    from rust_object_store import RustObjectStore

    kernel = make_kernel(f"file://{tmp_path}/rs", backend="rust")
    assert isinstance(kernel.store, RustObjectStore)
    h = kernel.write(b"rust-backed")
    assert kernel.read_blob(h) == b"rust-backed"


def test_make_kernel_env_var(monkeypatch, tmp_path):
    _pond()
    from make_kernel import make_kernel
    from local_fs_object_store import LocalFSObjectStore
    from rust_object_store import RustObjectStore

    # POND_PY_BACKEND=python wins over the default auto.
    monkeypatch.setenv("POND_PY_BACKEND", "python")
    kernel = make_kernel(f"file://{tmp_path}/env_py")
    assert isinstance(kernel.store, LocalFSObjectStore)

    # POND_PY_BACKEND=rust selects Rust under the same default kwarg.
    monkeypatch.setenv("POND_PY_BACKEND", "rust")
    kernel = make_kernel(f"file://{tmp_path}/env_rs")
    assert isinstance(kernel.store, RustObjectStore)

    # An EXPLICIT backend kwarg beats the env var.
    monkeypatch.setenv("POND_PY_BACKEND", "python")
    kernel = make_kernel(f"file://{tmp_path}/kw_wins", backend="rust")
    assert isinstance(kernel.store, RustObjectStore)

    # Invalid values raise.
    monkeypatch.setenv("POND_PY_BACKEND", "bogus")
    with pytest.raises(ValueError):
        make_kernel(f"file://{tmp_path}/bad")


def test_make_kernel_memory_unchanged():
    from make_kernel import make_kernel
    from object_store_native_kernel import InMemoryObjectStore

    kernel = make_kernel("memory://")
    assert isinstance(kernel.store, InMemoryObjectStore)
    h = kernel.write(b"in-memory")
    assert kernel.read_blob(h) == b"in-memory"


# ---------------------------------------------------------------------------
# 8. Batch operations
# ---------------------------------------------------------------------------

def test_batches_round_trip_and_stats(rust_store):
    items = [b"batch-a", b"batch-b" * 50, b"batch-c"]
    hashes = rust_store.put_blob_batch(items)
    assert hashes == [hash_bytes(d) for d in items]

    got = rust_store.get_blob_batch(hashes)
    assert got == items

    s = rust_store.stats
    assert s["puts"] == 3
    assert s["gets"] == 3
    assert s["bytes_written"] == sum(len(d) for d in items)
    assert s["bytes_read"] == sum(len(d) for d in items)

    # Empty batches are no-ops.
    assert rust_store.put_blob_batch([]) == []
    assert rust_store.get_blob_batch([]) == []

    # A missing blob in a batch raises the SAME exception type as a
    # single missing get_blob (KeyError — LocalFS parity).
    rust_store.reset_stats()
    with pytest.raises(KeyError):
        rust_store.get_blob_batch([hashes[0], "ff" * 32])
    # Fallback degrades to per-blob get_blob: stats count each blob ONCE
    # (the first blob was fetched, the second raised) — no double counting.
    assert rust_store.stats["gets"] == 1
    assert rust_store.stats["bytes_read"] == len(items[0])


# ---------------------------------------------------------------------------
# 9. Exception-type parity for missing blobs
# ---------------------------------------------------------------------------

def test_missing_blob_exception_parity(tmp_path):
    """A missing blob raises the SAME exception type LocalFSObjectStore
    raises (KeyError with the same message shape)."""
    _pond()
    from rust_object_store import RustObjectStore

    base = str(tmp_path / "miss")
    os.makedirs(base)
    rust_store = RustObjectStore.local(base)

    missing = "ab" * 32
    # What LocalFS raises (the contract callers rely on):
    with pytest.raises(KeyError) as local_exc:
        LocalFSObjectStore(str(tmp_path / "py")).get_blob(missing)
    # What the Rust-backed store raises:
    with pytest.raises(KeyError) as rust_exc:
        rust_store.get_blob(missing)
    assert type(rust_exc.value) is type(local_exc.value)
    assert "not found" in str(rust_exc.value)

    # get_path on a missing ref → None (no exception), like LocalFS.
    assert rust_store.get_path("no/such/ref") is None


# ---------------------------------------------------------------------------
# 10. Moto-mocked S3 via RustObjectStore.from_s3 (Rust S3 client, no boto3
#     client of the Python world's own)
# ---------------------------------------------------------------------------

def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture()
def moto_s3(monkeypatch, tmp_path):
    pytest.importorskip("moto")
    pytest.importorskip("boto3")
    from moto.server import ThreadedMotoServer
    import boto3

    port = _free_port()
    server = ThreadedMotoServer(ip_address="127.0.0.1", port=port)
    server.start()
    url = f"http://127.0.0.1:{port}"
    # Wait for readiness.
    deadline = time.time() + 10
    while time.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                break
        except OSError:
            time.sleep(0.1)
    else:
        server.stop()
        pytest.fail("moto server did not become ready")

    bucket = f"pond-rust-store-{int(time.time()) % 100000}"
    client = boto3.client("s3", endpoint_url=url, region_name="us-east-1",
                          aws_access_key_id="test", aws_secret_access_key="test")
    client.create_bucket(Bucket=bucket)

    # The Rust from_url reads credentials from the environment.
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")

    yield url, bucket
    server.stop()


def test_moto_s3_round_trip_via_rust_client(moto_s3, tmp_path):
    """Blob/ref round trips through ObjectStoreNativeKernel on
    RustObjectStore.from_s3(moto endpoint) — the Python world's object
    I/O runs entirely through the Rust S3 client (SigV4 + HTTP), no boto3
    client of its own."""
    _pond()
    from rust_object_store import RustObjectStore

    url, bucket = moto_s3
    prefix = f"rust-store-itest/{int(time.time() * 1000) % 10**9}"
    s3_url = f"s3://{bucket}/{prefix}?region=us-east-1&endpoint={url}"

    # cache_dir="off": the moto data must not pollute the real disk cache,
    # and the raw escape hatch (old-layout fallback) needs the raw store.
    store = RustObjectStore.from_s3(s3_url, cache_dir="off")
    assert store._bucket == bucket
    assert store._prefix == prefix

    # Direct store ops.
    h = store.put_blob(b"s3-payload")
    assert h == hash_bytes(b"s3-payload")
    assert store.get_blob(h) == b"s3-payload"
    assert store.has_blob(h)
    store.put_path("collections/c/HEAD", h)
    assert store.get_path("collections/c/HEAD") == h
    assert store.list_paths("collections/") == ["collections/c/HEAD"]

    # Raw hatch on S3: keys are relative to the store root.
    keys = store._rs.list_raw("blobs/")
    assert keys == [f"blobs/{h[:2]}/{h}"], keys
    assert store._rs.get_raw(f"blobs/{h[:2]}/{h}") == b"s3-payload"
    assert store._rs.get_raw("b/xx/missing") is None
    # Old-layout fallback through the raw hatch (b/{h[:2]}/{h} blobs).
    legacy = b"legacy-s3"
    other = hash_bytes(legacy)
    assert other != h
    store._rs.put_raw(f"b/{other[:2]}/{other}", legacy)
    assert store.get_blob(other) == legacy

    # Kernel read-your-write over S3.
    kernel = ObjectStoreNativeKernel(store)
    h2 = kernel.write(b"kernel-s3-write")
    kernel.reference("collections/c2/HEAD", h2)
    assert kernel.read("collections/c2/HEAD") == b"kernel-s3-write"

    # list_all_blob_hashes through the raw hatch (blobs/ + b/ trees).
    assert {h, h2, other} <= set(store.list_all_blob_hashes())

    # Deletes.
    assert store.delete_path("collections/c/HEAD")
    assert store.get_path("collections/c/HEAD") is None
    assert store.delete_blob(h)
    assert not store.has_blob(h)


def test_moto_s3_unified_storage_round_trip(moto_s3):
    """UnifiedStorage write/read/point-lookup on the Rust S3 store (moto)."""
    _pond()
    from rust_object_store import RustObjectStore
    from unified_storage import UnifiedStorage

    url, bucket = moto_s3
    s3_url = f"s3://{bucket}/unified-itest?region=us-east-1&endpoint={url}"
    store = RustObjectStore.from_s3(s3_url, cache_dir="off")
    kernel = ObjectStoreNativeKernel(store)
    storage = UnifiedStorage(kernel)

    rows = [{"id": i, "v": i * 2} for i in range(25)]
    storage.write("t", rows, key_col="id", row_group_size=10)
    assert len(storage.read("t")) == 25
    assert storage.point_lookup("t", key="3")["v"] == 6
    got = storage.read("t", predicates=[("v", ">=", 46)])
    assert {r["id"] for r in got} == {23, 24}


def test_from_s3_cache_wrapped_raw_unsupported(tmp_path, monkeypatch):
    """With the 3-tier cache active (the default from_s3 wiring), the raw
    escape hatch is Unsupported BY DESIGN — raw ops through the caching
    wrapper would bypass the cache layers. Construction only parses the
    URL (no network I/O); the capability surface is asserted directly."""
    _pond()
    import pond

    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
    rs = pond.ObjectStore.from_s3(
        "s3://some-bucket/x?region=us-east-1&endpoint=http://127.0.0.1:1",
        cache_dir=str(tmp_path / "cache"),   # cache ON → CachingObjectStore
    )
    # The raw hatch is unsupported through the cache wrapper.
    with pytest.raises(OSError, match="not supported"):
        rs.get_raw("b/ab/abc")
    with pytest.raises(OSError, match="not supported"):
        rs.list_raw("blobs/")


def test_adapter_capability_probe_unit():
    """The adapter's capability probe: when the Rust store's raw hatch is
    Unsupported, old-layout reads degrade to the NORMAL error (KeyError,
    LocalFS parity) and blob enumeration matches LocalFS with missing
    blob dirs ([]) — after which the probe latches and skips raw calls."""
    _pond()
    from rust_object_store import RustObjectStore

    class _NoRawHatch:
        """A pond.ObjectStore stand-in whose raw ops are Unsupported
        (exactly what CachingObjectStore exposes)."""

        def get_blob(self, h):
            raise OSError(f"Blob '{h}' not found")

        def delete_blob(self, h):
            return False

        def get_raw(self, key):
            raise OSError("get_raw not supported by this ObjectStore")

        def delete_raw(self, key):
            raise OSError("delete_raw not supported by this ObjectStore")

        def list_raw(self, prefix):
            raise OSError("list_raw not supported by this ObjectStore")

    store = RustObjectStore(_NoRawHatch())
    assert store._raw_ok is None            # unknown until first probe

    # Missing blob → the normal error (NOT the raw-hatch OSError).
    with pytest.raises(KeyError):
        store.get_blob("ab" * 32)
    assert store._raw_ok is False            # probe latched

    # Blob enumeration degrades to [] like LocalFS with missing dirs.
    assert store.list_all_blob_hashes() == []

    # delete_blob doesn't blow up on the unsupported hatch.
    assert store.delete_blob("ab" * 32) is False
