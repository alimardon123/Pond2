# ARCHITECTURE.md — Pond2 Architecture as Contract

> Crucible state file. Settled decisions stay settled; reopen only with new
> evidence, logged in CHANGELOG.md.

## Product shape (settled)

```
┌────────────────────────────────────────────────────────────┐
│                    USERS                                    │
│  pyo3 (Python)   CLI binary   Go SDK   MCP server   …      │
└───────┬──────────────┬───────────┬──────────┬──────────────┘
        │ thin bindings│ 1st-class │ thin     │ thin
        ▼              ▼           ▼          ▼
┌────────────────────────────────────────────────────────────┐
│  RUST CORE (the main language, all semantics live here)    │
│  core/kernel    — refs, commits, branches, GC              │
│  core/storage   — manifests (PMAN), slabs (PSLB), shards,  │
│                   pruned read pipeline, write paths        │
│  core/codec     — PND2 columnar codec (bitpack, zstd)      │
│  core/s3        — hand-written SigV4 S3/R2 client          │
│  core/cache     — CachingObjectStore (disk + moka memory)  │
│  core/sql       — SQL planning/execution (pushdown)        │
│  lenses/*/rust  — KV, Lakehouse, Vector, Streaming, OLTP   │
│  cli/           — single lightweight-but-powerful binary   │
└────────────────────────────────────────────────────────────┘
        ▼ content-addressed blobs, refs
┌────────────────────────────────────────────────────────────┐
│  OBJECT STORAGE (S3 / R2 / localfs via kernel backends)    │
│  + local disk smart cache (~/.pond_cache) — staledb-class  │
└────────────────────────────────────────────────────────────┘
```

**D1 — Rust-first (settled).** All semantics (CRDT merge, pruning, codecs,
transactions-as-publish) live in the Rust core. Bindings are FFI shells. When
a feature exists in Python but not Rust, the fix is to move it into Rust, not
to grow the Python side.

**D2 — CLI as a first-class product (settled).** DuckDB methodology: one
binary that is simultaneously an embedded engine (library) and a complete
user tool (`pond write|read|sql|shell|branch|merge|gc`). Lightweight = fast
startup, no runtime deps; powerful = full lens surface from the terminal.
New user-visible capabilities must be reachable from the CLI, not only from
Python.

**D3 — No CAS as the concurrency architecture (settled direction).**

Background: compare-and-swap (S3 `If-Match` conditional writes) creates
central boilerplate (one contended object per branch), retry storms under
multi-writer load, and has no equivalent on localfs — the same code cannot
be tested/used locally. The owner's design intent is CRDT-based, and the
architecture must solve concurrency *beautifully* without CAS.

Target design — **immutable journal, CRDT merge, no overwrites**:

1. A commit NEVER overwrites a shared pointer. It writes:
   - data blobs (content-addressed, immutable, dedup by hash), then
   - ONE commit record at a **unique path**:
     `collections/<c>/journal/<seq>-<writer_id>.pcommit`
     (PutIfAbsent semantics; always succeeds because the path is unique —
     no retries by construction).
2. Readers resolve "current state" = merge all journal records ≥ the last
   compaction watermark (CRDT union; per-row LWW by `(_version, writer_id)`
   tiebreak; tombstones suppress). Deterministic, order-independent.
3. Visibility without per-read LIST: probe `journal/<seq>` forward from the
   cached max seq (Delimited/staledb-style epoch probe): O(1) GETs when
   nothing changed, O(k) when k new commits landed. Both are cacheable.
4. Compaction folds the journal tail into an immutable snapshot record
   (unique path), advancing the watermark; never mutates old objects.
5. localfs gets identical semantics for free (unique files, no rename
   races) — one code path, testable everywhere.

Transitional state: `write_rows_inner` currently commits through an S3
conditional-write CAS retry loop (172a3da). It is CORRECT and tested; it is
NOT the target architecture. It gets superseded by the journal design in a
dedicated write-path cycle — not surgically replaced mid-read-cycle. No NEW
CAS dependencies may be added.

**D4 — One pruned read pipeline (settled).** There is exactly ONE production
read pipeline: leaf pruning (PMAN v3) → zone-map pruning → parallel bloom
pre-check → slab-aware range GETs + coalescing → projection pushdown →
columnar predicate evaluation. `read_rows_i64` pioneered it; every
binding/CLI/SQL path must route through it. Full-scan JSON fallbacks are
debt, not alternatives.

**D5 — Atomic publish, not ACID (settled).** Commits are atomic publishes
(immutable snapshot + ref move); readers never see partial state. No
cross-collection transactions.

## Components and interfaces (as-built)

| Component | Interface | Notes |
|---|---|---|
| `core/kernel` | `write`, `read_blob`, `read_blob_batch`, `get_blob_range`, `reference`, `resolve`, `list_names_prefix`, `delete_ref` | content-addressed blob store + ref namespace; LocalFS/S3 backends |
| `core/storage::read` | `read_rows_i64`, `read_rows_i64_indexed`, `read_rows_i64_range_indexed` | THE pruned pipeline (i64 today; this cycle generalizes it) |
| `core/storage::manifest` | `CollectionManifest` (PMAN v1/v2 flat, v3 root+leaves) | zone-map stats, bloom refs, slab offsets |
| `core/storage::shard` | `write_shard`, `list_shards`, `read_with_shards`, `clear_shards` | CRDT shard layer; per-read LIST is known debt (CRITIQUE C2) |
| `core/storage::commit` | `resolve_manifest_bytes`, `read_commit` | HEAD → manifest, single-GET resolve |
| `core/codec` | `pnd2_encode/decode` | columnar codec: bitpack + zstd (native, ruzstd fallback) |
| `core/s3` | SigV4 client, `put_path_if` (If-None-Match / If-Match) | hand-written; R2-validated |
| `core/cache` | `CachingObjectStore` (disk tier + moka memory tier) | wired into CLI + pyo3 |
| `core/sql` | SQL parse/plan → pushdown into storage reader | WHERE + projection pushdown |
| `bindings/python/pyo3` | `write_rows`, `read_rows`, `read_rows_stream`, SQL, RLS | flagship user API |
| `cli` | `pond <cmd>` — write/read/rows/sql/shell/branch/merge/gc/... | 1.8k lines; D2's product surface |

## Ownership map (for fan-out discipline)

| Concern | Class | Owner |
|---|---|---|
| Pruned read pipeline (read.rs + pyo3 routing) | **COUPLED** — one owner, sequential | read-path builder subagent |
| Manifest/shard/slab format (PMAN/PSLB/PNPK bytes) | **COUPLED** with the read pipeline | same owner, same pass |
| No-CAS journal write path (future cycle) | COUPLED with kernel ref semantics | next write-path cycle |
| CLI surface | INDEPENDENT once core APIs settle | any cycle |
| Codec (PND2/zstd) | INDEPENDENT | any cycle |
| S3/SigV4/R2 client | INDEPENDENT | any cycle |
| Docs/laws/CI | INDEPENDENT | any cycle |

Rule: parallel subagents only touch INDEPENDENT concerns. Anything that
changes read.rs + lib.rs read routing is one owner, one sequential pass.

## Shared data vocabulary

- **Blob**: content-addressed immutable bytes (`sha256`-style hash).
- **Ref**: `<namespace>/<name>` → hash pointer (kernel namespace).
- **Commit** (`.pcommit` JSON): `{manifest, parents, message, ts}`.
- **Manifest** (PMAN): columns, key_col, row_groups[] with per-RG zone-map
  stats + slab offset/len; v3 = root (leaf index + key ranges) + leaves.
- **Slab** (PSLB): packed sequential RGs + footer (bloom, offsets).
- **Shard**: JSON array of row updates keyed by `_rowid`, unique-path ref;
  CRDT-merged over HEAD at read time.
- **Row identity**: `_rowid` (string); conflict resolution by `_version`
  LWW + tombstones.
