# lenses/oltp/

The **OLTPLens** — in-memory memtable + batch flush to CRDT shards.

## What it is

OLTP lens provides fast key-value operations with an in-memory memtable.
Writes go to the memtable (sub-µs latency). When the memtable is full (or
`flush()` is called), it flushes to storage as a commit — amortizing
S3/network latency across N writes. This is the LSM-tree pattern:

- SST files → Pond commits (concurrent-safe via CRDT)
- Compaction → `compact_shards` (in UnifiedStorage)
- Multi-process → each instance flushes independently (CRDT handles conflicts)

## Capabilities

- `put(key, value)` — fast write to in-memory memtable
- `get(key)` — read (memtable first, then storage)
- `delete(key)` — tombstone in memtable
- `exists(key)` — check existence
- `keys()` — list all keys (memtable + storage)
- `count()` — count entries
- `flush()` — flush memtable to storage as a commit
- `pending_count()` — count unflushed entries
- Auto-flush at configurable threshold

## Files

| Path | Language | Purpose |
|---|---|---|
| `python/oltp_lens.py` | Python | Reference implementation (~198 LOC), extends PondLens |
| `python/__init__.py` | Python | Package exports |
| `rust/` | Rust | Core API port, owns UnifiedStorage directly |
| `rust/README.md` | — | Rust-specific docs and test list |

## Architecture

```
OLTPLens (Python extends PondLens / Rust owns UnifiedStorage directly)
  ↓ uses
UnifiedStorage (Rust core or Python reference)
  ↓ calls
PondKernel (3 ops: Write, Read, Ref)
  ↓ calls
ObjectStore trait (LocalFS or S3)
```

## OLTP vs KeyValue

Both are key-value lenses. The difference:

| Aspect | KeyValueLens | OLTPLens |
|---|---|---|
| Write path | Direct to storage (1 PUT per write) | Memtable → batch flush (N writes per PUT) |
| Read path | Storage only | Memtable first, then storage |
| Best for | Low-frequency writes, strong consistency | High-frequency writes, bursty workloads |
| Flush | N/A (every write persists) | Explicit or auto-flush at threshold |

## Tests

- Python: covered by `tests/test_all.py` (via property tests)
- Rust: 8 unit tests (`cargo test -p pond_oltp_lens`)
  See `rust/README.md` for the full test list
