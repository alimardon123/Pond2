# Builder Spec — Python-Substrate Delegation (Task cron-2026-08-29-1640-a)

You are the SUBSTRATE BUILDER for Pond2 at /home/z/Pond-review.
Your contract: ACCEPTANCE.md §"This-cycle acceptance (crucible iteration
N+5 — the Python-substrate delegation cycle)" items 1–6 (the orchestrator
owns items 7–11: C8, C13, docs, state files, commit).

## Context (why this cycle exists)

The pure-Python kernel stack (bindings/python/core + bindings/python/sdk +
the Python lens world) is a SEPARATE storage world from the Rust core. It
implements its own object stores (LocalFSObjectStore ~443 lines,
S3ObjectStore ~519 lines on boto3, InMemoryObjectStore) under the
ObjectStoreNativeKernel. CRITIQUE.md C5-python + ARCHITECTURE.md D1 say the
endgame is delegation to the Rust core via pyo3 — never a Python port of
Rust semantics. This cycle lands PHASE 1: the I/O substrate. The Python
world keeps its formats and semantics; its object-store layer gains a Rust
backend.

VERIFIED FACTS (do not re-derive, they are pinned):
- The Python NEW layout and the Rust layout are BYTE-IDENTICAL:
  blobs at `blobs/{hash[:2]}/{hash}` (Python LocalFS `_blobs_dir` =
  `{base}/blobs`; Rust LocalFS `blob_path` = `{base}/blobs/{h[:2]}/{h}`),
  refs at `{path}` with JSON body `{"hash":"..."}` (both sides), S3 keys
  `{prefix}/blobs/{h[:2]}/{h}` and `{prefix}/{path}` (both sides).
- Python's sha256 `hash_bytes` == Rust's `hash_bytes` == the hash in the
  blob key. put_blob returns the hash on BOTH sides.
- The Rust ObjectStore trait lives at core/kernel/src/object_store.rs
  (`pub trait ObjectStore`); PondKernel holds `Arc<dyn ObjectStore>`.
- The pyo3 Storage class (bindings/python/pyo3/src/lib.rs ~1422) shows the
  constructor patterns to mirror: `s3_kernel_cached(url, cache_dir)` wires
  CachingObjectStore (feature "cache"); `PondKernel::new_local(path)` /
  `PondKernel::new_with_store(Box<dyn ObjectStore>)`.
- CI's pytest job builds the whole workspace (incl. pond_python) and runs
  pytest with PYTHONPATH=target/release:bindings/python/core:... — so
  `import pond` works in tests and the new surface IS CI-covered.
- Python LocalFSObjectStore keeps OLD-layout fallbacks: blobs at
  `b/{h[:2]}/{h}` (`_old_blobs_dir`), refs at `paths/{path}`
  (`_old_path_file`). The Rust-backed adapter must preserve these reads.

## Deliverable 1 — Rust trait: raw-key escape hatch

core/kernel/src/object_store.rs — add to `pub trait ObjectStore` (default
impl returns Unsupported, exactly like `list_dirs`):

```rust
/// Read raw bytes at a store-relative key (NO content addressing, NO
/// JSON ref wrapping). Key space includes blobs/ keys. Used by foreign
/// bindings (Python adapter) for legacy-layout fallback reads and blob
/// enumeration. Not implemented by CachingObjectStore (raw ops bypass
/// the cache by design).
fn get_raw(&self, key: &str) -> io::Result<Option<Vec<u8>>> { ... Unsupported }
fn put_raw(&self, key: &str, data: &[u8]) -> io::Result<()> { ... Unsupported }
fn delete_raw(&self, key: &str) -> io::Result<bool> { ... Unsupported }
fn list_raw(&self, prefix: &str) -> io::Result<Vec<String>> { ... Unsupported }
```

Implement for LocalFSObjectStore (key = path under base_dir; list_raw =
walk like list_paths but over raw keys, RELATIVE to base_dir, sorted;
refuse keys that escape the base_dir via `..` — return InvalidInput) and
S3ObjectStore (key under the store's prefix; list_raw paginates like
list_paths; get_raw maps 404 → Ok(None)). Do NOT implement on
CachingObjectStore or test stores (default Unsupported is correct; a raw
op through the caching wrapper hitting the inner store directly would
bypass cache layers — document this in the trait comment).

Semantics: get_raw returns Ok(None) for missing keys (like get_path, NOT
an error — the Python adapter uses it for existence probes). delete_raw
returns Ok(false) when the key was absent. list_raw returns keys RELATIVE
to the store root (i.e. INCLUDING any `blobs/` component), sorted,
recursive under the prefix.

## Deliverable 2 — pyo3 class `pond.ObjectStore`

bindings/python/pyo3/src/lib.rs — new #[pyclass] `RawObjectStore` exposed
to Python as `pond.ObjectStore` (name it ObjectStore in #[pyclass(name =
"ObjectStore")]; keep the Rust struct name RawObjectStore to avoid
clashing with the trait). Holds `store: Arc<dyn ObjectStore>` — NOT a
PondKernel, NOT UnifiedStorage. Construct:

- `#[new] fn new(location: &str, cache_dir: Option<&str>)` — if location
  starts with s3:// → same construction as Storage::new's S3 arm (env-cred
  kwargs NOT needed — copy the from_url pattern; cache via
  s3_kernel_cached); else LocalFS via PondKernel::new_local (or
  LocalFSObjectStore::new directly + Arc). Accept `file://` prefix strip.
- `#[staticmethod] from_s3(url: &str, cache_dir: Option<&str>)` — mirror
  Storage::from_s3 exactly (feature-gated #[cfg(feature = "s3")]; without
  the feature return PyIOError like Storage does).

Methods (ALL I/O-bound ones wrapped in `py.allow_threads` so the Python
kernel's ThreadPoolExecutor batches parallelize; map io::Error →
PyIOError, except where noted):

- `put_blob(data: bytes) -> str`
- `get_blob(py, hash: &str) -> bytes` (PyBytes)
- `get_blob_range(hash: &str, start: u64, end: u64) -> bytes`
- `get_blob_suffix(hash: &str, n: u64) -> bytes`
- `put_blob_batch(items: Vec<Vec<u8>>) -> Vec<String>` (delegate to the
  trait's put_blob_batch — S3 impl parallelizes)
- `get_blob_batch(py, hashes: Vec<String>) -> Vec<PyBytes>` — build the
  Vec<Vec<u8>> off-GIL, convert after
- `blob_exists(hash: &str) -> bool`
- `delete_blob(hash: &str) -> bool`
- `put_path(path: &str, hash: &str) -> None`
- `get_path(path: &str) -> Option<String>`
- `delete_path(path: &str) -> bool`
- `list_paths(prefix: &str) -> Vec<String>`
- `list_dirs(prefix: &str) -> Vec<String>`
- `store_id() -> str`
- Raw escape hatch: `get_raw(py, key: &str) -> Option<bytes>` (None when
  key absent; Err(PyIOError) only on real I/O errors), `put_raw(key: &str,
  data: bytes) -> None`, `delete_raw(key: &str) -> bool`,
  `list_raw(prefix: &str) -> Vec<String>`. The trait methods return
  Unsupported errors through the default impl — surface those as
  PyIOError (the Python adapter checks capability with try/except on the
  FIRST raw call and remembers).

Also append the class + all methods to bindings/python/pyo3/pond.pyi
(match the stub's existing style — read it first).

## Deliverable 3 — Python adapter `RustObjectStore`

New file bindings/python/core/rust_object_store.py. Read
bindings/python/core/local_fs_object_store.py FIRST and mirror its exact
duck interface (the kernel + any callers rely on: put_blob, get_blob,
put_blob_batch, get_blob_batch, has_blob, delete_blob,
list_all_blob_hashes, put_path, get_path, delete_path, list_paths,
stats dict {gets, puts, bytes_read, bytes_written, latency_ms_total},
reset_stats(), print_stats(label=""), and the `base_dir` property for
LocalFS-style construction).

```python
class RustObjectStore:
    """Object store backed by the Rust core (pond.ObjectStore) via pyo3.

    Byte-identical layouts to LocalFSObjectStore/S3ObjectStore:
      blobs at blobs/{h[:2]}/{h}, refs at {path} as JSON {"hash": ...}.
    Old-layout fallback reads (b/{h[:2]}/{h}, paths/{path}) go through
    the Rust store's raw-key escape hatch.
    """
```

Key semantics:
- `put_blob(data)`: `h = self._rs.put_blob(data)`; assert h == sha256 hex
  (trust but verify is overkill — just return h; the Rust impl IS the
  same sha256); stats puts += 1, bytes_written += len(data).
- `get_blob(hash)`: try `self._rs.get_blob(hash)`; on PyIOError that
  looks like NotFound (or any error — match LocalFS's error shape: it
  raises ValueError(f"Blob '{h}' not found") — reproduce that message
  type for kernel compatibility) fall back to OLD key via
  `self._rs.get_raw(f"b/{hash[:2]}/{hash}")`; if that is None → raise the
  same error LocalFSObjectStore raises (check its get_blob: KeyError?
  ValueError? — MATCH IT EXACTLY; the kernel and SDK catch specific
  exceptions).
- `put_path/get_path/delete_path`: delegate directly (Rust put_path
  writes the same JSON body). get_path OLD fallback: if new get_path
  returns None, try `get_raw(f"paths/{path}")` and parse its JSON
  {"hash": ...}. delete_path: delete new key; also try delete old key
  (LocalFS deletes both) — return True if either existed.
- `has_blob`: `self._rs.blob_exists(hash)` (note: blob_exists checks the
  NEW layout only — acceptable; old-layout existence via get_blob path).
- `delete_blob`: delegate.
- `list_all_blob_hashes()`: `self._rs.list_raw("blobs/")` → strip
  "blobs/{shard}/" prefix → return hashes. If list_raw is Unsupported
  (PyIOError) → fall back to [] like LocalFS does when dir missing? NO —
  check LocalFS behavior and match it.
- `list_paths(prefix)`: delegate to `self._rs.list_paths(prefix)`. VERIFY
  the return-shape parity: Rust LocalFS list_paths returns paths relative
  to base_dir (recursive, sorted, excluding nothing? read walk_dir);
  Python LocalFS list_paths returns ... read its impl and make the
  adapter MATCH THE PYTHON SHAPE (that's what ObjectStoreNativeKernel's
  list_paths_with_prefix consumes — check how the kernel uses it, e.g.
  whether blob keys must be excluded). If Rust's shape differs (e.g.
  includes blobs/), normalize in the adapter.
- `put_blob_batch/get_blob_batch`: delegate to the Rust batch methods;
  thread-pool on the Python side is NOT needed (Rust parallelizes
  off-GIL) — but keep stats accounting identical.
- `stats`: maintain the dict on the Python side from call counts/bytes
  (the Rust store's internal stats are not exposed; adapter-level stats
  are what the kernel prints).
- `reset_stats()` / `print_stats(label="")`: mirror LocalFS.
- Constructor: `RustObjectStore(rust_store)` where rust_store is a
  `pond.ObjectStore` instance (the caller constructs it — make_kernel or
  tests); ALSO convenience classmethods `local(base_dir)` and
  `from_s3(url, cache_dir=None)` that construct the pond.ObjectStore and
  wrap it.
- Capability probe: on first raw-op failure with Unsupported, set
  `self._raw_ok = False` and skip raw fallbacks thereafter (raise the
  normal error instead).

## Deliverable 4 — make_kernel wiring

bindings/python/core/make_kernel.py — add `backend: str = "auto"` kwarg
to make_kernel (values "auto" | "rust" | "python"; env override
POND_PY_BACKEND wins over the kwarg? NO — kwarg wins, env is the default
when kwarg == "auto"; document in the docstring). Behavior:

- memory:// → unchanged (InMemoryObjectStore — pure Python, tests rely on
  it).
- file:// (or bare path) with backend rust-or-auto: try
  `from rust_object_store import RustObjectStore` + `pond.ObjectStore`
  import; construct `RustObjectStore.local(base_dir)`. On ImportError or
  construction exception with backend=="auto": fall back to
  LocalFSObjectStore and print ONE stderr note
  ("pond: Rust object store unavailable (<err>); using pure-Python
  backend"). With backend=="rust": let the exception propagate.
- s3:// with backend rust-or-auto: build the Rust s3 URL with query
  params from the boto3-style kwargs the function already accepts
  (region, endpoint_url, aws_access_key_id, aws_secret_access_key —
  mirror pond.Storage's URL building; creds → env vars AWS_ACCESS_KEY_ID/
  AWS_SECRET_ACCESS_KEY like Storage::new does) →
  `RustObjectStore.from_s3(url, cache_dir=...)`; cache_dir from kwargs or
  POND_CACHE_DIR (leave None otherwise — resolve_cache_dir handles
  defaults). Same auto-fallback to boto3 S3ObjectStore with the stderr
  note.
- Keep ALL existing pure-Python paths byte-identical when
  backend=="python" (the default when the module is missing must behave
  EXACTLY as today).
- Update the module docstring: document backend selection + the
  byte-identical layout guarantee + POND_PY_BACKEND.

## Deliverable 5 — tests

New file tests/test_rust_object_store.py (pytest style matching
tests/test_all.py conventions — read it first; it shells out to scripts,
but YOUR tests import directly and run in-process; add plain pytest
functions). The pytest CI job has PYTHONPATH set (target/release has
pond.so) — the tests must SKIP GRACEFULLY (pytest.skip with a clear
reason) when `import pond` fails, so local pure-Python runs stay green.

Cover, at minimum:
1. Byte-interop LocalFS⇄Rust: write N blobs + M refs via
   LocalFSObjectStore in dir A; read all through RustObjectStore(A);
   write via RustObjectStore in dir B; read all through
   LocalFSObjectStore(B); assert identical bytes AND identical on-disk
   file paths (walk both trees; compare relative path sets).
2. hash equality: put_blob(same data) on both backends → same hash.
3. Old-layout fallback: hand-create `{base}/b/{h[:2]}/{h}` file and
   `{base}/paths/{ref}` JSON file (pre-layout-change shapes);
   RustObjectStore.get_blob/get_path resolve them.
4. Duck-interface parity: kernel = ObjectStoreNativeKernel(RustObjectStore)
   — write/read blobs, resolve refs, list_names, read-your-write; assert
   kernel.stats counts GETs/PUTs.
5. UnifiedStorage on the Rust store:
   UnifiedStorage(kernel).write("t", table, key_col="id") → read back
   full + point_lookup + predicate read; write AGAIN (append) → both
   commits' rows visible (history not lost).
6. PondStorage SDK round trip on make_kernel("file://…", backend="rust").
7. make_kernel backend selection: backend="python" → LocalFSObjectStore;
   backend="rust" → RustObjectStore (skip if pond missing);
   POND_PY_BACKEND env honored.
8. Batches: put_blob_batch/get_blob_batch round trip + stats.
9. Moto S3 (skip if moto/boto3 missing): start moto_server (look at how
   scripts/test_rust_s3.py or tests/test_all.py's moto tests do it —
   reuse the pattern), create bucket, RustObjectStore.from_s3 with
   endpoint_url → blob/ref round trips through ObjectStoreNativeKernel,
   list_paths, delete. This test proves the Python world needs NO boto3
   client of its own for object I/O.
10. Capability fallback: get_blob on a missing hash raises the SAME
    exception type LocalFSObjectStore raises (compat for callers that
    catch it).

ALSO: run the existing pytest suite (pytest tests/test_all.py -v) and the
lens laws (python tests/lens_algebra/run_lens_laws_ci.py or however CI
invokes it — check .github/workflows/view-laws.yml lens job) to prove
nothing regressed. If any EXISTING test breaks because it constructs
kernels in a way your changes altered, fix the TEST only if the behavior
contract is unchanged; otherwise report it (do not paper over).

## Build & validation commands (run ALL, report results)

```bash
source ~/.cargo/env
cargo build --release -p pond_python            # builds libpond.so
ls -la target/release/libpond.so
cargo clippy --release -p pond_python -p pond_kernel -p pond_s3 --all-targets -- -D warnings
cargo test --release -p pond_kernel -p pond_s3  # trait changes green
cd /home/z/Pond-review && PYTHONPATH=target/release:bindings/python/core:bindings/python/sdk:bindings/python/sdk/extensions/physical_structures python -m pytest tests/test_rust_object_store.py -v
PYTHONPATH=... python -m pytest tests/test_all.py -v   # full suite
```

NOTE: target/release/libpond.so must be importable as `pond` — if the
built name differs, symlink target/release/pond.so (CI does this).
Python is 3.12/3.13 (both available).

## Constraints

- NO new dependencies. NO changes to data formats. NO changes to the
  pure-Python stores' behavior (they must stay byte-identical for the
  fallback path). Do not touch journal/CRDT/read-pipeline code.
- pond_python stays excluded from `cargo test --workspace` (CI policy) —
  but `cargo clippy -p pond_python` MUST be clean.
- Do not commit. The orchestrator reviews, integrates, and commits.
- Report back: files changed with line counts, test results (each command
  + pass/fail counts), any deviations from this spec WITH reasons, and
  anything you found that the orchestrator should know (bugs, surprises).
