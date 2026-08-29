# Builder Spec — C17: get_path Error Channel (Task cron-2026-08-30-a)

You are the SUBSTRATE BUILDER for Pond2 at /home/z/Pond-review.
Your contract: ACCEPTANCE.md §"This-cycle acceptance (crucible iteration
N+6)" items 1, 2, and the get_path-dependent parts of 6 (the orchestrator
owns C13 routing, the R2 harness, state files, commit, tribunal).

Before starting, read /home/z/my-project/worklog.md (the last two cycles)
for context. DO NOT touch ACCEPTANCE.md / CRITIQUE.md / ARCHITECTURE.md /
CHANGELOG.md / SCORECARD.md — the orchestrator owns the state files. You
may create/update docs/builder-spec-c17-notes.md if you need to record
decisions (optional).

## Context (why this exists)

CRITIQUE.md C17: `ObjectStore::get_path` returns `Option<String>` with NO
error channel. A failed ref GET (transient S3 500/429/timeout, localfs
permission error) is indistinguishable from an absent ref for EVERY
caller — journal snapshot resolution, epoch probes, branch reads, CAS
pre-reads, the SQL executor, the Python adapter. Empirically proven
blindness: a store whose ref reads fail returned EMPTY SQL results, not
errors. The C8 fix (N+5) closed the DATA-blob half (`get_blob` has an
error channel and the executor propagates it); this refactor closes the
REF half.

## The refactor (mechanical but wide — 112 call sites)

### 1. Trait + backends

`core/kernel/src/object_store.rs`:
- Trait method: `fn get_path(&self, path: &str) -> Option<String>;` →
  `fn get_path(&self, path: &str) -> io::Result<Option<String>>;`
  Update the doc comment: `Ok(None)` = unbound, `Err` = I/O failure
  (transient or permanent — the caller sees and decides).
- Default `put_path_if` (line ~72): `let current = self.get_path(path)?;`
  (a failed pre-read is now an Err, not a phantom "absent").
- LocalFS impl (~line 424): today ALL fs errors → None. After:
  `Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None)`, any other
  error → `Err(e)`. The JSON parse result stays `Ok(extract_hash...)`.

`core/s3/src/lib.rs`:
- `get_path` (~1181): today ALL errors → None. After: use the EXISTING
  `is_s3_not_found` helper (same one delete_path/list use) — not-found →
  `Ok(None)`, anything else → `Err`. The body-read error (`read_to_string`
  fails) becomes `Err` too (corrupt ref body — kind InvalidData, message
  names the path).
- `get_path_with_etag` (~646): same treatment →
  `io::Result<Option<(String, String)>>`.
- `get_path_async` (~1600): same treatment (it has no callers today —
  keep it coherent: `io::Result<Option<String>>`).

`core/cache/src/lib.rs` CachingObjectStore (~516):
- Cache-hit path: unchanged (returns `Ok(Some(hash))` for unexpired TTL).
- Miss: `let hash = self.inner.get_path(path)?;` — `Ok(None)` returns
  `Ok(None)` WITHOUT inserting into the ref cache (absence is not
  cacheable — same as today); `Err` propagates WITHOUT touching the cache
  (an error must not poison or evict anything).

### 2. Kernel + pyo3 surface

`core/kernel/src/lib.rs`:
- `PondKernel::resolve` (~221): → `io::Result<Option<String>>`
  (`self.store.get_path(name)` — a bare delegation).

`bindings/python/pyo3/src/lib.rs`:
- `RawObjectStore::get_path` (~5806): →
  `fn get_path(&self, py: Python<'_>, path: String) -> PyResult<Option<String>>`
  with `py.allow_threads(move || self.store.get_path(&path)).map_err(py_io_err)`
  — matching every other fallible method in that class. `py_io_err`
  already exists. Update the doc comment (raises IOError on failure,
  None = unbound) and `bindings/python/pyo3/pond.pyi` if it names the
  return type.

`bindings/python/core/rust_object_store.py`:
- `get_path` (~345): now exceptions propagate from the Rust side (that is
  CORRECT — parity with LocalFSObjectStore which raises OSError on real
  I/O failures). Add/adjust the docstring: raises IOError on storage
  failure, returns None if unbound. If any of the fallback logic around
  `self._rs.get_path(path)` should NOT swallow errors, make sure it
  doesn't (only KeyError-based fallbacks stay).

### 3. The caller sweep (rg '\.resolve\(' — 112 sites, 14 files)

`core/storage/src/journal.rs` — the critical file:
- `probe_writer` (~415): becomes
  `fn probe_writer(...) -> Result<Vec<JournalEntry>, String>`. The epoch
  loop: a probe that ERRORS returns `Err(format!("journal entry probe
  failed (writer {} seq {}): {}", writer, seq, e))` — a failed probe is
  a potentially TRUNCATED journal view, never an empty suffix. First
  `Ok(None)` still terminates the epoch (a real gap).
- The parallel scope caller (~490): the existing `probe_error` plumbing
  becomes `Ok(Ok(entries)) => probed.extend(entries), Ok(Err(e)) =>
  return Some(e), Err(_) => return Some("journal probe thread
  panicked")`.
- `resolve_packs` (~471): `let snapshot = kernel.resolve(&branch_ref(...))`
  → `let snapshot = kernel.resolve(&branch_ref(collection, branch))
  .map_err(|e| format!("Failed to read branch ref for collection
  '{}': {}", collection, e))?;` — Err propagates (today a failed read
  masquerades as "no snapshot").
- ~840 (`bootstrap` probe): `.map_err(...)?.is_none()` with the same
  contextual message style.
- Sweep EVERY other resolve call in the file the same way.

`core/storage/src/read.rs` (~10 sites), `branch.rs` (~10), `bptx.rs` (~8),
`transaction.rs` (~2), `maintenance.rs`, `shard.rs`, `write.rs`,
`write_buffer.rs` (incl. its test store impl at ~764):
- Production paths: propagate with context
  (`.map_err(|e| format!("Failed to read ...: {}", e))?` — match the
  surrounding error type: most storage fns return `Result<_, String>`,
  so map to String; io-Result fns use `?` directly).
- `read.rs:22` and `read.rs:645` (`read` / `read_rows_async` HEAD
  resolution): the Err case gets its own arm —
  `Err(e) => return Err(format!("Failed to read branch ref for
  collection '{}': {}", collection, e))` — DISTINCT from the
  "has no commits" arm (an outage is not a fresh collection). NOTE: the
  orchestrator will re-route these through the journal AFTER you land;
  keep the arms clean and labeled.
- Existence probes (`kernel.resolve(x).is_some()` →
  `kernel.resolve(x).map_err(...)?.is_some()` — the map_err context
  names what was being probed).

`core/storage/tests/journal_test.rs` (+ its test store at ~180),
`core/storage/tests/laws_journal.rs`, `core/sql/tests/sql_integration.rs`:
- Test stores' `get_path` impls: new signature; the delegating stores
  pass through the inner Result. The BlobOutage store (sql_integration
  ~1129) keeps ref reads HEALTHY (its comment says so — the C8 test
  isolates blob outages).
- Test callers: `.unwrap()` on resolve (or contextual expect). Where a
  test asserts `is_none()`, becomes `.unwrap().is_none()`.
- `core/kernel/src/c_abi.rs` `pond_kernel_resolve` (~120): the C ABI has
  NO error channel — map `Err(_)` to `ptr::null_mut()` (same as None)
  and add a two-line comment documenting the ABI limitation (C callers
  cannot distinguish transient failure from absence; recorded in
  CRITIQUE by the orchestrator).

`cli/src/main.rs` (~518): `store.get_path(".pond/config")` →
`.map_err(...)?`-style handling matching the surrounding fn's error type
(it's in a command path — print-and-exit is fine if that's the local
pattern).

`mcp-server`, `lenses/*`, `extensions/*`: rg for `.resolve(` there too —
update with the same patterns (they mostly call `storage_read::read`
which you've already fixed; only direct resolve calls matter).

### 4. New tests (REVERT-VERIFIED — the acceptance hinge)

In `core/sql/tests/sql_integration.rs` (next to the C8 tests):
- A `RefOutageStore` wrapper: healthy blobs, FAILING `get_path`
  (io::Error kind Other, message "simulated ref outage"). Follow the
  BlobOutageStore pattern.
- `test_c17_ref_outage_errors_not_empty`: write rows via SQL INSERT (or
  the storage API), then query through the RefOutage-wrapped store → the
  query must return `Err` containing "ref" (not empty rows). REVERT
  CHECK: describe in a comment that pre-C17 this returned Ok(vec![]) —
  the blindness C17 kills.
- `test_c17_ref_outage_recovery`: outage off → the same query returns
  the rows intact (the failure was transient; nothing was corrupted).
- In `core/storage/tests/journal_test.rs`:
  `test_c17_probe_outage_is_error_not_truncation`: a store wrapper that
  fails `get_path` ONLY for journal entry paths (prefix-match the entry
  path shape) while keeping the branch ref healthy →
  `journal::resolve_view` (or resolve_packs) returns Err, NOT a view
  missing the tail entries. Recovery → full view.

Also update `KNOWLEDGE_GRAPH.md`: add rows for the touched
core files/functions per the existing format (check its header for the
format), ONLY if your changes introduce new test files/functions worth
indexing (the KG script enforces coverage).

### 5. Validation (all must be green before you report)

```
cargo build --workspace
cargo test --workspace --exclude pond_python --exclude pond_sql   # CI command
cargo test -p pond_sql
cargo clippy --workspace --all-targets -- -D warnings
cd /home/z/Pond-review && PYTHONPATH=target/release:bindings/python/core:bindings/python/sdk \
  python3 -m pytest tests/test_all.py tests/test_rust_object_store.py -x -q
```
The pyo3 surface change means pond_python must REBUILD for pytest —
build the workspace first. If a pytest case relied on get_path returning
None for failures, fix the test to expect the exception (that is the
new correct contract) and say so in your report.

## Report back (your single final message)

1. Every file you touched + what changed (one line each).
2. The exact count of resolve call sites you converted, split
   production vs test.
4. The revert-verification story for each new test (what fails
   pre-refactor, what passes post).
5. Full validation output tail (build/test/clippy/pytest pass counts).
6. Any judgment calls (error message wording, a caller where you
   chose propagate-vs-default and why) — the orchestrator reviews
   line-by-line; surprises up front are cheaper.
