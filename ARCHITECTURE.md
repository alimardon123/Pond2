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

**D3 — No CAS as the concurrency architecture (settled; landing this cycle).**

Background: compare-and-swap (S3 `If-Match` conditional writes) creates
central boilerplate (one contended object per branch), retry storms under
multi-writer load, and has no equivalent on localfs — the same code cannot
be tested/used locally. The owner's design intent is CRDT-based, and the
architecture must solve concurrency *beautifully* without CAS.

Settled design — **per-writer immutable journal, benign snapshot cache**:

1. **Writes append, never overwrite.** A commit writes data blobs
   (content-addressed, immutable), builds its pack (PNPK: commit + manifest),
   then appends ONE pointer at a unique path:
   `collections/<c>/_branches/<b>/journal/<writer_id>/<seq:012>`
   via plain `put_path` — unique path ⇒ always succeeds, zero retries by
   construction, identical on localfs/S3/R2. `writer_id` = fresh UUIDv7 per
   writer instance (process boot); `seq` is the writer's own local counter —
   no coordination, no CAS, no lost updates possible.
2. **The pack's commit JSON carries journal metadata**: `journal:
   {writer, seq, upto: {writer → seq}}`. `upto` states what the pack's
   manifest already folds (compaction snapshots only; data entries omit it).
   The invariant: *a pack + probes above its `upto` = complete state*.
3. **Reads = snapshot ∪ live entries.** The branch ref is a CACHE of the
   last folded snapshot (not a serialization point). Readers resolve:
   base = branch-ref pack manifest → probe each discovered writer's log
   forward from `max(snapshot.upto[w], local seen[w]) + 1` (epoch probes:
   parallel GETs at computable paths; positive hits are immutable and
   content-cacheable; first miss ends that writer's log) → run the ONE
   pruned pipeline per entry pack → CRDT-merge rows (LWW by `_version`,
   total tiebreak `(_version, _rowid)`, tombstones suppress).
   Legacy shards union in as before (compat; python lenses still write them).
4. **Writer discovery** = one delimiter-LIST of `journal/` (returns writer
   dirs only — changes only when a NEW writer process appears, not per
   write), TTL-cached in-process (default ~1s; `POND_JOURNAL_TTL_MS=0` for
   exact freshness). Own-process appends are visible immediately.
5. **Compaction folds and advances the cache.** `compact` = read full state
   (snapshot + entries + shards) → write ONE folded pack → LWW-update the
   branch ref (benign: every value is a valid folded state; racing
   compactors merely pick different valid bases — probes above each `upto`
   reconstruct completeness) → delete folded entries (≤ `upto` only) and
   clear folded shards. Auto-compaction triggers on live-entry threshold
   (fixes the keyvalue-lens compact-after-every-write P0 pattern).
6. **Writes touch zero shared objects** — no branch-ref write, no derived
   refs, no retries, no contended key. This is the linearization-free
   design the owner asked for: correctness from CRDT merge + unique paths,
   not from serialization.

Transitional state (superseded THIS cycle): `write_rows_inner`'s S3 CAS
retry loop (172a3da) — correct for ref races but semantically vacuous (the
rebuilt pack still excluded the winner's data; HEAD-only reads meant every
commit after the first hid its parent's rows — CRITIQUE C9). It is REMOVED
by the journal path. `put_path_if`/`reference_if` remain kernel primitives
with existing tests but NO production callers.

**D4 — One pruned read pipeline (settled).** There is exactly ONE production
read pipeline: leaf pruning (PMAN v3) → zone-map pruning → parallel bloom
pre-check → slab-aware range GETs + coalescing → projection pushdown →
columnar predicate evaluation. `read_rows_i64` pioneered it; every
binding/CLI/SQL path must route through it. Full-scan JSON fallbacks are
debt, not alternatives.

**D5 — Atomic publish, not ACID (settled).** Commits are atomic publishes
(immutable snapshot + ref move); readers never see partial state. No
cross-collection transactions.

**D6 — The RG-level read plan (settled this cycle; supersedes resolve_view's
pack-granular F2 drop).**

Readers never resolve the journal themselves. ONE entry point,
`journal::resolve_packs(kernel, collection, branch, force_refresh) ->
Vec<PackPlan>`, where `PackPlan = { pack_hash, only_rgs:
Option<BTreeSet<RgIdentity>> }` and `RgIdentity = (blob_hash,
slab_byte_offset)`:

1. **`resolve_view` returns the RAW view** (snapshot + every live entry
   above `upto`) — the pack-granular coverage drop (tribunal-r1 F2 fix)
   moves OUT of resolution and into the plan, upgraded to RG granularity.
   Side effect (deliberate): a stale loser-compactor entry now stays
   visible as live, so the NEXT compact's `upto` covers its seq and the
   delete loop finally removes it (pre-F2 zombie entries were re-probed
   and re-dropped forever).
2. **Common path** (no COMPACT entries live — the steady state): plans =
   `[snapshot?] + entries`, all `only_rgs: None`. Zero extra reads vs
   today; the classification short-circuits.
3. **With compact entries**: `covered` = snapshot RGs ∪ data-entry RGs;
   each compact entry (in deterministic (writer, seq) order) contributes
   only its NOVEL RG identities — `None` (keep whole) when all are novel,
   `Some(novel)` when partially covered, dropped when nothing is novel;
   `covered` absorbs each entry's novel set. This closes C11: partial
   overlap duplicates vanish for the CONCATENATING readers too.
4. **RG identity is `(blob_hash, slab_byte_offset)`** — stable across
   folds because compaction copies RG entries verbatim (only `key` is
   re-sequenced). blob-hash-only identity would conflate co-slab RGs.
5. **`compact` uses BOTH**: the raw view for `upto`/delete accounting,
   the plans for the union manifest — and dedups union RGs by identity
   (self-heals pre-D6 snapshots that already carry duplicated RGs).
6. Readers apply `only_rgs` with one `retain` after `resolve_manifest`.
   The 5 duplicated "snapshot + entries" loops (read.rs ×3, lakehouse,
   vector) all delegate to `resolve_packs` (C7 closed).

## Components and interfaces (as-built)

| Component | Interface | Notes |
|---|---|---|
| `core/kernel` | `write`, `read_blob`, `read_blob_batch`, `get_blob_range`, `reference`, `resolve`, `list_names_prefix`, `delete_ref` | content-addressed blob store + ref namespace; LocalFS/S3 backends |
| `core/storage::read` | `read_rows_i64`, `read_rows_i64_indexed`, `read_rows_i64_range_indexed` | THE pruned pipeline (i64 today; this cycle generalizes it) |
| `core/storage::journal` | `resolve_packs` (D6 read plan), `append_pack`, `compact`, `status`, `history` | per-writer immutable journal; ONE reader entry point |
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
