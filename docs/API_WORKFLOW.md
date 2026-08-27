# Pond — Full API Workflow

> **Audience**: Application developers using Pond as their storage backbone.
> This document shows the complete end-to-end API surface with working
> examples for every operation.
>
> **Language**: Python (via the `pond` PyO3 module). The same operations are
> available from the Rust CLI (`pond` command), Go SDK, and C ABI — see
> the cross-language section at the end.

---

## 0. The 30-second mental model

```
                    ┌──────────────────────────────────┐
                    │          Storage                 │
                    │  (one connection, any backend)   │
                    └──────────────┬───────────────────┘
                                   │
        ┌──────────────┬───────────┼────────────┬────────────────┐
        ▼              ▼           ▼            ▼                ▼
   Data I/O       Versioning   Indexing    Semantic Layer    Maintenance
   write          branch       build_index  s.layer()         gc_stats
   read           checkout     search_index .add_metrics()    vacuum
   write_rows     merge        lookup_index .add_dimensions()
   read_rows      history      drop_index   .add_relationships()
                                          .add_adapter()
```

Everything is **one `Storage` object**. You create it once, point it at
local disk or S3, and use it for data, versioning, indexing, semantic
layers, and maintenance.

### Universal data support — all types are first-class

| Data type | Store | Query | Example |
|---|---|---|---|
| **Structured** | `write_rows()` — INT64, FLOAT64, STRING | `read_rows()`, `.sql()`, SIMD predicates | Tables, metrics |
| **Semi-structured** | `write_rows()` with JSON in STRING cols, or `write()` raw JSON | `.sql()` on metadata + `json.loads()` on payload | Events, logs, documents |
| **Unstructured** | `write()` — raw bytes (any format) | `read()` — get bytes by name | Images, PDFs, model weights |

All types get: versioning (branch/merge), CRDT (concurrent writes),
content-addressed dedup, and storage-independence (local FS / S3).

---

## 1. Setup — create a Storage connection

```python
from pond import Storage

# Local filesystem (auto-creates the directory)
s = Storage('/var/lib/pond')

# S3 (AWS S3, Cloudflare R2, MinIO, Wasabi, DigitalOcean Spaces, ...)
s = Storage(
    's3://my-bucket/prod',
    access_key='AKIA...',
    secret_key='secret...',
    region='us-east-1',
    # endpoint='https://<account>.r2.cloudflarestorage.com',  # for R2
    # endpoint='http://localhost:9000',                       # for MinIO
)

# S3 with credentials from env (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
s = Storage('s3://my-bucket/prod?region=us-east-1')
```

**S3 reads go through the local smart cache (3 tiers).** Every S3-backed
`Storage` wraps object storage with an in-memory cache (256 MB), a local
disk cache (1 GB, LRU-evicted), and in-flight request coalescing — warm
reads resolve in µs–ms instead of paying 50–300ms S3 round-trips:

```python
# Cache location (default: ~/.pond_cache):
s = Storage('s3://my-bucket/prod', cache_dir='/fast-nvme/pond-cache')

# Or via the environment (CLI honors this too):
#   export POND_CACHE_DIR=/fast-nvme/pond-cache

# Disable the cache entirely:
s = Storage('s3://my-bucket/prod', cache_dir='off')   # or POND_CACHE_DIR=off
```

The cache is write-through and content-addressed (blob names are SHA-256
hashes), so it is always consistent with object storage. Refs (branch
heads) are cached in-memory with a 5-second TTL for multi-writer safety.

**One `Storage` object serves all workloads.** No per-workload clients,
no per-format handles. The storage doesn't know or care whether you're
storing KV pairs, vectors, streaming events, or lakehouse tables.

---

## 2. Data I/O — the layered write/read model

Pond has a **two-tier API** for data operations:

### Lower-level (shard primitives — for advanced use)

These give you explicit control over shard naming. Use them when you need
to manage shards directly (e.g., multi-writer concurrency with custom
shard names).

| Method | Purpose |
|---|---|
| `append_shard(collection, shard_name, data)` | Append raw bytes as a shard |
| `upsert_shard(collection, shard_name, rows, key_col?)` | CRDT upsert (adds _rowid + _version) |
| `delete_shard(collection, shard_name, rowids, key_col?)` | CRDT delete (tombstones by _rowid) |
| `read_with_shards(collection)` | Read HEAD + all shards (raw bytes) |

### Higher-level (row operations — beautiful, SQL-like, recommended)

These are built on top of the shard primitives. They auto-generate shard
names, support predicate filtering (like SQL WHERE), and have an optional
`crdt=True` flag. **This is the recommended API for most users.**

| Method | SQL equivalent | Purpose |
|---|---|---|
| `write_rows(collection, columns, message, crdt=True)` | `CREATE TABLE` / `INSERT` | Bulk load (snapshot) |
| `update_rows(collection, updates, where?, key_col?, crdt=True)` | `UPDATE ... WHERE` | Update matching rows |
| `delete_rows(collection, where?, key_col?, crdt=True)` | `DELETE FROM ... WHERE` | Delete matching rows |
| `merge_rows(collection, rows, key_col?, crdt=True)` | `MERGE` / `INSERT ON CONFLICT` | Upsert by key |
| `read_rows(collection, columns?, predicates?)` | `SELECT ... WHERE` | Read with projection + pruning |

### The `crdt=True` flag (default)

When `crdt=True` (the default), all operations use **CRDT shards**:
- `write_rows` auto-adds `_rowid` (UUIDv7) + `_version` (HLC) columns
- `update_rows` / `delete_rows` / `merge_rows` write shards (no HEAD rewrite)
- Multiple writers can operate concurrently without coordination
- `read_rows` auto-merges HEAD + all shards (latest `_version` wins, tombstones suppress)

When `crdt=False`, operations use **snapshot semantics** (rewrite HEAD):
- No `_rowid` / `_version` columns added
- `update_rows` / `delete_rows` / `merge_rows` rewrite the entire HEAD
- Not concurrent-safe (last writer wins)
- Use for immutable historical data or bulk loads that won't be updated

### The inner columns: `_rowid`, `_version`, `_deleted`

These are always used internally when `crdt=True`:
- `_rowid` (UUIDv7) — stable row identity across updates
- `_version` (HLC) — clock-skew-safe version for CRDT merge (latest wins)
- `_deleted` (bool) — tombstone marker for deletes

`read_rows` **auto-filters** these from results unless you explicitly request
them via `columns=['_rowid', ...]`.

### How they compose

```
write_rows()      → bulk load (creates snapshot + adds _rowid/_version)
    ↓
update_rows()     → incremental update (shard, matches by _rowid)
delete_rows()     → incremental delete (tombstone shard)
merge_rows()      → upsert (shard, matches by key_col → _rowid)
    ↓
read_rows()       → reads merged result (HEAD + shards, latest _version wins)
```

```python
# Bulk load — auto-adds _rowid + _version
s.write_rows('users', [('id', [1, 2, 3]), ('name', ['a', 'b', 'c'])], 'init')

# SQL-like UPDATE — filter-based, not just rowids
s.update_rows('users', {'status': 'active'}, where={'city': 'NYC'})
# → UPDATE users SET status='active' WHERE city='NYC'

# SQL-like DELETE — filter-based
s.delete_rows('users', where={'status': 'inactive'})
# → DELETE FROM users WHERE status='inactive'

# SQL-like MERGE — upsert by key
s.merge_rows('users', [
    {'id': 1, 'name': 'alice_updated'},
    {'id': 99, 'name': 'new_user'},
], key_col='id')

# Read — auto-merges HEAD + shards, filters _rowid/_version/_deleted
cols = s.read_rows('users')
```

To opt out of CRDT metadata (raw bulk load, no updates/deletes later):
```python
s.write_rows('logs', [('event', ['click', 'view'])], 'init', crdt=False)
```

### 2.1 Raw bytes — unstructured data (images, PDFs, any binary)

```python
# Write raw bytes — any format: images, PDFs, audio, video, archives
s.write('images/logo.png', png_bytes, 'upload logo')
s.write('docs/report.pdf', pdf_bytes, 'upload report')
s.write('models/bert.pt', model_weights, 'save model')

# Read raw bytes back from HEAD
data = s.read('images/logo.png')   # → raw bytes
```

Raw bytes get the same benefits as structured data: versioning (branch/merge),
content-addressed dedup, and storage-independence. But they do NOT get
_rowid/_version (no structure to tag) — use `write_rows` for CRDT compatibility.

### 2.2 Semi-structured data (JSON in columns or as raw blobs)

```python
import json

# Option A: JSON in STRING columns (queryable metadata + flexible payload)
s.write_rows('events', [
    ('id', [1, 2, 3]),
    ('event', ['click', 'view', 'purchase']),
    ('payload', [
        json.dumps({'button': 'buy', 'color': 'red'}),
        json.dumps({'page': '/home', 'duration': 5.2}),
        json.dumps({'item': 'widget', 'price': 9.99, 'qty': 2}),
    ]),
], 'init')

# Query metadata columns with SQL + SIMD, then parse payload in Python
cols = s.read_rows('events', predicates=[('event', '=', 'click')])
payload = json.loads(cols['payload'][0])  # → {'button': 'buy', 'color': 'red'}

# Or use .sql() for the metadata filter
result = s.sql("SELECT * FROM events WHERE event = 'click'")

# Option B: Raw JSON blobs (for document-style data)
s.write('documents', json.dumps([
    {'id': 1, 'name': 'alice', 'tags': ['dev', 'admin'], 'meta': {'team': 'eng'}},
]).encode(), 'init')

# CRDT works on semi-structured data too:
s.upsert_shard('events', 'w1', rows=[
    {'id': 4, 'event': 'signup', 'payload': json.dumps({'source': 'web', 'plan': 'pro'})},
], key_col='id')
```

### 2.3 Structured columns (PND2 — auto-encoded, auto-pruned, CRDT by default)

This is the recommended path for tabular data. Pond's PND2 format
auto-selects the best encoding per column (RLE / DICT / BITPACK / RAW),
embeds column statistics, and prunes row groups at read time. It also
auto-adds `_rowid` + `_version` columns for CRDT compatibility.

```python
# Write structured columns — auto-detects INT64 / FLOAT64 / STRING
# Auto-adds _rowid (UUIDv7) + _version (HLC) by default
s.write_rows('metrics', [
    ('id',    [1, 2, 3, 4, 5]),
    ('score', [1.5, 2.5, 3.5, 4.5, 5.5]),
    ('name',  ['alice', 'bob', 'carol', 'dave', 'eve']),
], 'init metrics')
# → stored columns: id, score, name, _rowid, _version

# Read all columns (excludes _rowid/_version from results by default)
cols = s.read_rows('metrics')
# → {'id': [1,2,3,4,5], 'score': [1.5,...], 'name': ['alice',...]}

# Projection — only decode the columns you need
cols = s.read_rows('metrics', columns=['score', 'name'])

# Predicate pruning — skip row groups whose stats don't match
cols = s.read_rows('metrics', predicates=[('id', '>', 2)])
# → {'id': [3,4,5], 'score': [3.5,4.5,5.5], 'name': ['carol','dave','eve']}

# Combine projection + predicate
cols = s.read_rows('metrics',
                   columns=['name'],
                   predicates=[('id', '=', 3)])
# → {'name': ['carol']}
```

**Supported predicates**: `=`, `==`, `!=`, `<>`, `<`, `<=`, `>`, `>=`.

**Auto-index acceleration**: if you build a simple index on a column
(see §4.1) and then query with `('col', '=', value)`, the read path
automatically uses the index for O(1) lookup. If the key isn't in the
index, the read returns empty immediately — no row-group scan.

**Auto-shard merge**: `read_rows` automatically reads HEAD + all live
shards and merges them by `_rowid` (latest `_version` wins, tombstones
suppress). You don't need to call `read_with_shards` manually.

---

## 3. Version control — git for your data

Every `write` / `write_rows` creates a commit. Branches are O(1) ref
copies. Merges use CRDT semantics (no CAS, no conflicts).

```python
# Create a branch from the current HEAD
s.branch('users', 'dev')

# Switch the active branch (subsequent reads/writes go to 'dev')
s.checkout('users', 'dev')

# Create AND checkout in one call (like `git checkout -b`)
s.checkout_new('users', 'feature-x')

# Write on the dev branch
s.write('users', b'[{"id":2,"name":"bob"}]', 'add bob on dev')

# Switch back to main
s.checkout('users', 'main')

# Merge dev into main (or any target branch)
s.merge('users', source='dev', target='main', message='merge dev')

# Walk commit history
for commit in s.history('users', limit=10):
    print(commit)
# → [{'hash': 'abc123', 'parent': 'def456', 'message': 'merge dev', 'index': 3, ...}, ...]

# Undo the last N commits
s.undo('users', steps=2)

# Revert to a specific commit hash (from history())
s.revert('users', commit_hash='abc123...')

# See which branch is active
print(s.get_active_branch('users'))   # → 'main'

# Explicitly set the active branch (alternative to checkout)
s.set_active_branch('users', 'dev')
```

### 3.1 List collections

```python
# List all collections in the storage
for coll in s.ls():
    print(coll)
# → [{'name': 'users', 'head': 'abc123', ...}, {'name': 'metrics', ...}]
```

---

## 4. Indexing — accelerate reads

### 4.1 Simple secondary index (composite multi-key)

A simple index maps key values → rowids. Supports composite keys
(multiple columns joined into one index key).

```python
# Build a single-column index — pass rows explicitly
rows = [
    ('user:1', {'name': 'alice', 'city': 'NYC', 'id': 1}),
    ('user:2', {'name': 'bob',   'city': 'LA',  'id': 2}),
    ('user:3', {'name': 'carol', 'city': 'NYC', 'id': 3}),
]
s.build_index('users', 'by_name', 'simple',
              config={'key_field': 'name'},
              rows=rows)

# Build a composite multi-key index
s.build_index('users', 'by_name_city', 'simple',
              config={'key_fields': ['name', 'city']},
              rows=rows)

# O(1) exact lookup
rowid = s.lookup_index('users', 'by_name', 'alice')
# → 'user:1'

# Auto-acceleration: read_rows with an equality predicate on an indexed
# column will use the index automatically
result = s.read_rows('users', predicates=[('name', '=', 'bob')])
# → uses 'by_name' index internally; returns empty immediately if not found
```

### 4.2 IVF vector index (k-means clusters)

For approximate nearest neighbor (ANN) search on vector collections.

```python
# First, write vectors to a collection (as PND2 columns)
s.write_rows('vectors', [
    ('id', [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]),
    ('vec', [[0.1, 0.2], [0.3, 0.4], [0.5, 0.6], [0.7, 0.8],
             [0.9, 1.0], [1.1, 1.2], [1.3, 1.4], [1.5, 1.6],
             [1.7, 1.8], [1.9, 2.0]]),
], 'init vectors')

# Build an IVF index (k-means clusters)
s.build_index('vectors', 'ann', 'ivf',
              config={'n_clusters': 3, 'metric': 'euclidean'})

# Search — returns [(distance, vector_id), ...] sorted by distance
results = s.search_index('vectors', 'ivf',
                          query=[0.2, 0.3],
                          k=5,
                          n_probe=2)   # clusters to search
# → [(0.14, 1), (0.28, 2), (0.42, 3), ...]
```

### 4.3 HNSW vector index (hierarchical navigable small world)

For O(log N) ANN search — better recall than IVF at small-to-medium scale.

```python
s.build_index('vectors', 'ann', 'hnsw',
              config={'m': 16, 'ef_construction': 200, 'metric': 'l2'})

results = s.search_index('vectors', 'hnsw',
                          query=[0.2, 0.3],
                          k=5,
                          ef=50)   # beam width
# → [(0.14, 1), (0.28, 2), ...]
```

### 4.4 Index management

```python
# List all indexes on a collection
print(s.list_indexes('vectors'))
# → ['ann']

# Drop an index (works for all index types)
s.drop_index('vectors', 'ann')   # → True
```

---

## 5. Semantic Layer — metrics, dimensions, relationships

A **Semantic Layer** is a coherent set of metrics/dimensions/relationships
over Pond collections, exposed to external systems (BI tools, query
engines, AI agents) via one or more adapters.

### Why "layer" (not "model")

The word "model" collides with ML models, which Pond may host in the
future. "Semantic Layer" is the industry-standard term (dbt Semantic
Layer, Cube Semantic Layer, Looker LookML).

### 5.1 Create a layer

```python
# Create a layer with default adapter (['ossie']) and reflection off
m = s.layer('sales')

# Create with explicit adapters + reflection enabled
m = s.layer('sales', adapters=['ossie', 'cube'], enable_reflection=True)
# Note: only 'ossie' is built-in; 'cube' must be registered first.

# List all layers
print(s.layers())   # → ['sales']
```

### 5.2 Batch-add datasets, metrics, dimensions, relationships

```python
# Add datasets (collections that the layer reads from)
m.add_datasets(['orders', 'users', 'products'])

# Add metrics — dict of {name: SQL expression}
m.add_metrics({
    'revenue':         'SUM(orders.amount)',
    'order_count':     'COUNT(orders.id)',
    'avg_order_value': 'revenue / order_count',
})

# Add dimensions — dict of {name: (dataset, field, data_type)}
m.add_dimensions({
    'country':    ('users',  'country',    'string'),
    'order_date': ('orders', 'created_at', 'datetime'),
})

# Add relationships — dict of {name: (from, to, join_condition)}
m.add_relationships({
    'user_orders':     ('users',    'orders',   'users.id = orders.user_id'),
    'product_orders':  ('products', 'orders',   'products.id = orders.product_id'),
})
```

### 5.3 Independent adapter management (multi-adapter)

A layer can be exposed via multiple adapters simultaneously. External
systems query the same layer through whichever adapter they speak.

```python
# List currently enabled adapters
print(m.adapters())   # → ['ossie', 'cube']

# Add another adapter (idempotent — safe to call repeatedly)
m.add_adapter('dbt')

# Remove an adapter (independent of the spec)
m.remove_adapter('cube')   # → True
m.remove_adapter('cube')   # → False (already removed)

print(m.adapters())   # → ['ossie', 'dbt']
```

**Auto-exposure**: there is no explicit "export" step. When you register
an adapter, the layer is queryable via that adapter's protocol. Adapters
read the layer's spec directly from storage at query time.

### 5.4 Reflection (Dremio-style query acceleration)

```python
# Enable/disable reflection (idempotent)
m.enable_reflection()
m.disable_reflection()
```

When reflection is enabled, the layer is registered with the reflection
subsystem so reflection-aware query engines can find it and use it for
query acceleration.

### 5.5 Inspect the layer

```python
# Full overview — returns a dict
info = m.info()
# → {
#     'name': 'sales',
#     'adapters': ['ossie', 'dbt'],
#     'reflection_enabled': True,
#     'datasets': ['orders', 'users', 'products'],
#     'metrics': ['revenue', 'order_count', 'avg_order_value'],
#     'dimensions': ['country', 'order_date'],
#     'relationships': ['user_orders', 'product_orders'],
#   }

# Individual listings
m.datasets()        # → ['orders', 'users', 'products']
m.metrics()         # → ['revenue', 'order_count', 'avg_order_value']
m.dimensions()      # → ['country', 'order_date']
m.relationships()   # → ['user_orders', 'product_orders']
```

### 5.6 Optional one-shot export

`export()` is OPTIONAL. It's for one-shot snapshots (file export,
debugging, migration). Adapters can read the layer's spec directly from
storage at query time — that's the default "auto-exposure" path.

```python
# Export in a specific adapter's format
ossie_spec = m.export('ossie')
# → {'name': 'sales', 'datasets': [...], 'metrics': [...], ...} in Ossie format

# Export using the first adapter in the layer's adapters list
default_spec = m.export()   # equivalent to m.export(m.adapters()[0])
```

### 5.7 Multiple layers coexist

```python
sales   = s.layer('sales',   adapters=['ossie'])
product = s.layer('product', adapters=['ossie'])
finance = s.layer('finance', adapters=['ossie'])

# Each layer is independent
sales.add_datasets(['orders'])
product.add_datasets(['products'])
finance.add_datasets(['invoices'])

print(s.layers())   # → ['sales', 'product', 'finance']

# Each layer's spec is stored separately under semantic_layers/{name}/
```

---

## 6. CRDT Shards — incremental update, delete, concurrent multi-writer

These are the **incremental** write primitives. They compose with
`write_rows` (the bulk load primitive):

- **`write_rows`** — bulk initial load. Creates a snapshot (new HEAD commit)
  with `_rowid` + `_version` auto-added. Overwrites previous HEAD.
- **`upsert_shard`** — incremental update. Appends a shard alongside HEAD
  with the same `_rowid` semantics. Doesn't overwrite HEAD.
- **`delete_shard`** — incremental delete. Appends a tombstone shard.
- **`read_rows`** — reads HEAD + all shards, merges by `_rowid`
  (latest `_version` wins, tombstones suppress).

Multiple writers can call `upsert_shard` / `delete_shard` concurrently
without coordination — the merge is deterministic (CRDT G-Set union).

### 6.1 How updates work — `upsert_shard`

```python
# After a bulk load via write_rows (which added _rowid + _version):
s.write_rows('users', [('id', [1, 2]), ('name', ['alice', 'bob'])], 'init')

# Incremental update — match by _rowid (auto-generated by write_rows)
# If you don't know the _rowid, upsert_shard generates one for new rows
# and uses key_col to find existing rows.
s.upsert_shard('users', 'writer1_001',
               rows=[{'id': 1, 'name': 'ALICE', 'age': 31}],  # update alice
               key_col='id')
# → adds _rowid (matches existing row by key_col=id) + _version (new HLC)

# Concurrent writer — no coordination needed
s.upsert_shard('users', 'writer2_001',
               rows=[{'id': 1, 'name': 'Alice Updated', 'age': 32}],
               key_col='id')
```

Each row gets:
- `_rowid`: UUIDv7 (stable across updates, generated if not present)
- `_version`: HLC (Hybrid Logical Clock — new per write, clock-skew-safe)
- `_deleted`: false

On merge, rows with the same `_rowid` are deduplicated — **latest `_version`
wins**. Multiple writers can update the same row concurrently without
conflicts; the merge is deterministic.

### 6.2 How deletes work — `delete_shard`

```python
# Delete rows by writing a tombstone shard
s.delete_shard('users', 'writer1_del',
               rowids=['user:1', 'user:2'],
               key_col='id')
```

Each deleted `_rowid` gets a tombstone with `_deleted=true` and a new
`_version`. On merge, if the tombstone's `_version` is later than any live
row's `_version`, the row is suppressed. If a live row has a later
`_version` (written after the delete), it overrides the tombstone.

### 6.3 How reads merge — `read_with_shards`

```python
# Read HEAD + all live shards (raw bytes for each)
shards = s.read_with_shards('users')
# → [('__head__', b'...PND2...'), ('writer1_001', b'[...]'), ('writer2_001', b'[...]')]

# For structured reads with auto-merge, use read_rows:
cols = s.read_rows('users')
# → HEAD + all shards merged by _rowid (latest _version wins, tombstones suppress)
```

`read_rows` automatically merges HEAD + all shards using the CRDT rules.
You don't need to call `read_with_shards` manually unless you want raw
bytes.

### 6.4 Raw shard append (no CRDT metadata)

```python
# For non-CRDT use cases (e.g., append-only event logs)
s.append_shard('events', 'producer1_001', b'{"event":"click","ts":123}')
```

`append_shard` writes raw bytes without adding `_rowid`/`_version`. Use
this for append-only data where you don't need row-level CRDT merge.

### 6.5 Compact shards

```python
# Merge all shards into HEAD and clear the shard list
n = s.compact_shards('users')
print(f"Compacted {n} shards into HEAD")

# Count live shards
print(s.shard_count('users'))  # → 0 (after compact)
```

After compaction, all shard data is absorbed into HEAD (a new commit),
and the shard refs are deleted. This reclaims storage space and simplifies
future reads.

### 6.6 How branch merge works

```python
# Branch + write + merge
s.branch('users', 'dev')
s.checkout('users', 'dev')
s.upsert_shard('users', 'dev_w1', rows=[{'id': 5, 'name': 'eve'}], key_col='id')
s.checkout('users', 'main')
s.merge('users', source='dev', target='main', message='merge dev')
```

Branch merge uses **CRDT union**: the target branch's manifest is updated
to include all row groups + shards from both branches. No CAS, no conflict
detection — the merge is deterministic (G-Set union). If both branches
updated the same `_rowid`, the one with the latest `_version` wins after
`read_with_shards` merges.

### 6.7 merge_rows — full SQL MERGE reference

`merge_rows` implements complete SQL MERGE semantics with multi-action,
multi-key, column mapping, and conditional execution.

#### Parameters

| Parameter | Default | Description |
|---|---|---|
| `collection` | (required) | Target collection name |
| `rows` | (required) | List of source row dicts |
| `on` | `'_rowid'` | Key specification for matching (see below) |
| `key_col` | `None` | Shorthand for `on='col'` (deprecated — use `on`) |
| `crdt` | `True` | Use CRDT shards (True) or rewrite HEAD (False) |
| `where` | `None` | SQL WHERE filter on INCOMING (source) rows |
| `on_match` | `'UPDATE'` | Action(s) for WHEN MATCHED |
| `on_miss` | `'INSERT'` | Action(s) for WHEN NOT MATCHED BY TARGET |
| `on_miss_target` | `'SKIP'` | Action(s) for WHEN NOT MATCHED BY SOURCE |

#### `on=` — key matching

```python
on='id'                           # single key, same name both sides
on=['id', 'email']                # multi-key, same names
on=[('user_id', 'id')]            # different names (target, source)
on='t.user_id = s.id'             # SQL-like (recommended)
on='t.id = s.id AND t.code = s.code'  # multi-key SQL-like
```

#### `on_match` / `on_miss` / `on_miss_target` — action plans

All three accept the same formats:

```python
# String — single action
on_match='UPDATE'
on_match='DELETE'
on_match='SKIP'

# SQL-style string — with condition + SET
on_match="UPDATE WHERE s.age >= 18 SET t.status = 'adult'"
on_match="DELETE WHERE s.age < 18"
on_miss="INSERT WHERE s.age >= 18"

# Multi-action — semicolon-separated
on_match="UPDATE WHERE s.age >= 18; DELETE WHERE s.age < 18"

# List of strings — multi-action
on_match=['update', 'delete']

# List of tuples — (action, where) pairs
on_match=[('update', 's.age >= 18'), ('delete', 's.age < 18')]

# Dict — {action: where}
on_match={'update': 's.age >= 18', 'delete': 's.age < 18'}
```

#### SET clause — column mapping

Three modes:

```python
# No SET → copy ALL source columns (default)
on_match='UPDATE'

# SET without * → ONLY update listed columns, keep rest from target
on_match="UPDATE SET t.name = s.full_name, t.status = 'active'"

# SET *, ... → copy ALL source columns, THEN override specific ones
on_match="UPDATE SET *, t.status = 'active', t.updated_at = 999"
```

SET value specs:
- `s.col_name` — copy from source column
- `t.col_name` — keep target's existing value
- `'static'` — set to static string
- `999` — set to static number
- `true` / `false` — set to static boolean
- `null` — set to null

#### WHEN clauses

| Parameter | SQL clause | Fires when |
|---|---|---|
| `on_match` | WHEN MATCHED | Source row matches a target row |
| `on_miss` | WHEN NOT MATCHED BY TARGET | Source row has no matching target |
| `on_miss_target` | WHEN NOT MATCHED BY SOURCE | Target row has no matching source |

#### t./s. prefixes in WHERE

Conditions can reference both target and source columns:

```python
on_match="UPDATE WHERE t.status = 'active' AND s.amount > 100"
# → only update when target status is active AND source amount > 100
```

#### Returns

```python
{'matched': 2, 'updated': 1, 'deleted': 1, 'inserted': 1, 'skipped': 2}
```

#### Complete example

```python
s.merge_rows('inventory', [
    {'id': 2, 'new_qty': 100, 'remove': False},
    {'id': 3, 'new_qty': 0, 'remove': True},
    {'id': 5, 'new_qty': 50, 'remove': False},
], on='t.id = s.id',
   on_match="UPDATE WHERE t.status = 'low' SET t.qty = s.new_qty, t.status = 'stocked'; "
            "DELETE WHERE s.remove = true",
   on_miss="INSERT WHERE s.new_qty > 0",
   on_miss_target="DELETE WHERE t.status = 'discontinued'")
```

---

## 7. Atomic Publication (Transactions)

Pond provides **atomic publication** — all-or-nothing visibility across
multiple writes. This is NOT full ACID.

```python
# Begin a transaction
tx_id = s.begin_tx()
# → 'tx_0123456789abcdef'

# Write to multiple collections (tagged with tx_id — tentative, invisible)
s.append_shard('users',    f'{tx_id}_users',    b'{"id":3,"name":"carol"}')
s.append_shard('orders',   f'{tx_id}_orders',   b'{"id":3,"amount":50.0}')

# Commit — writes a commit marker. Once the marker exists, all
# tentative shards become visible atomically.
s.commit_tx(tx_id, 'add carol + her order')

# OR abort — no-op (tentative shards are orphaned until GC)
# s.abort_tx(tx_id)

# Check if a transaction has been committed
print(s.is_tx_committed(tx_id))  # → True
```

### What this provides

- ✅ **Atomicity of publication**: once the commit marker exists, all
  tentative shards become visible together.
- ✅ **Durability**: committed data survives crashes (content-addressed
  blobs are immutable).

### What this does NOT provide

- ❌ **No isolation**: readers can see committed state from other
  transactions mid-read.
- ❌ **No rollback**: `abort_tx` is a no-op. Tentative shards are orphaned
  until GC cleans them up.
- ❌ **No conflict detection**: two transactions can write the same
  `_rowid`; merge is LWW (latest `_version` wins).

This is the honest trade-off for a CRDT-based, CAS-free, storage-independent
system. For full ACID, you'd need a coordination service (Zookeeper, etcd)
which Pond deliberately avoids.

---

## 8. Maintenance — GC, vacuum, optimize

```python
# Read-only reachability analysis (no deletion)
stats = s.gc_stats(compute_size=False)
# → {'live': 150, 'dead': 12, 'dead_size_bytes': -1}

stats = s.gc_stats(compute_size=True)
# → {'live': 150, 'dead': 12, 'dead_size_bytes': 4096}

# Vacuum — delete unreachable blobs with time-travel safety
result = s.vacuum(preserve_days=7, dry_run=True)
# → {'deleted': 0, 'preserved': 12, 'dry_run': True}

# Real vacuum (actually deletes)
result = s.vacuum(preserve_days=7, dry_run=False)
# → {'deleted': 8, 'preserved': 4, 'dry_run': False}

# Optimize — compact shards + flatten delta manifests
result = s.optimize()                      # all collections
# OR: s.optimize('users')                  # one collection
# → {'collections_optimized': 3, 'shards_compacted': 12, 'manifests_flattened': 0}
```

- `preserve_days` keeps blobs referenced by commits younger than N days,
  so time-travel reads still work for recent history.
- `optimize` merges shards into HEAD (Delta/Iceberg-style small-file
  compaction). Manifest flattening is pending port from Python.

### About Reflection (semantic layers)

`m.enable_reflection()` / `m.disable_reflection()` on a SemanticLayer
set a boolean flag in the layer's `_meta` JSON. This is a **registration
hook** for external query engines (like Dremio) — it is NOT an incremental
subsystem inside Pond.

When reflection is enabled, the layer is discoverable via the
`reflections/semantic/{name}` ref. An external reflection-aware query
engine can find it, read the spec, and build its own accelerations
(materialized views, aggregates, etc.). Pond itself does not build or
maintain reflection data structures.

---

## 9. Complete end-to-end example

```python
from pond import Storage

# === Setup ===
s = Storage('/var/lib/pond')

# === Data: write structured columns ===
s.write_rows('orders', [
    ('id',         [1, 2, 3, 4]),
    ('user_id',    [1, 2, 1, 3]),
    ('amount',     [50.0, 75.0, 20.0, 100.0]),
    ('created_at', [1718000000, 1718000100, 1718000200, 1718000300]),
], 'init orders')

s.write_rows('users', [
    ('id',      [1, 2, 3]),
    ('name',    ['alice', 'bob', 'carol']),
    ('country', ['USA', 'UK', 'USA']),
], 'init users')

# === Version control: branch + merge ===
s.branch('orders', 'dev')
s.checkout('orders', 'dev')
s.write_rows('orders', [
    ('id',         [5]),
    ('user_id',    [2]),
    ('amount',     [200.0]),
    ('created_at', [1718000400]),
], 'add order 5 on dev')
s.checkout('orders', 'main')
s.merge('orders', source='dev', target='main', message='merge dev')

# === Indexing: build a secondary index ===
# (must provide rows for simple indexes — the indexer doesn't auto-read yet)
rows = [(str(r['id']), r) for r in [
    {'id': 1, 'user_id': 1, 'amount': 50.0, 'created_at': 1718000000},
    {'id': 2, 'user_id': 2, 'amount': 75.0, 'created_at': 1718000100},
    {'id': 3, 'user_id': 1, 'amount': 20.0, 'created_at': 1718000200},
    {'id': 4, 'user_id': 3, 'amount': 100.0, 'created_at': 1718000300},
    {'id': 5, 'user_id': 2, 'amount': 200.0, 'created_at': 1718000400},
]]
s.build_index('orders', 'by_user', 'simple',
              config={'key_field': 'user_id'},
              rows=rows)

# O(1) lookup
order_id = s.lookup_index('orders', 'by_user', '1')
print(f"First order for user 1: {order_id}")

# Auto-accelerated read (uses the index)
user1_orders = s.read_rows('orders', predicates=[('user_id', '=', 1)])
print(f"User 1's orders: {user1_orders}")

# === Semantic Layer: define metrics over the data ===
sales = s.layer('sales', adapters=['ossie'], enable_reflection=True)
sales.add_datasets(['orders', 'users'])
sales.add_metrics({
    'total_revenue':   'SUM(orders.amount)',
    'order_count':     'COUNT(orders.id)',
    'avg_order_value': 'total_revenue / order_count',
})
sales.add_dimensions({
    'country':    ('users',  'country',    'string'),
    'order_date': ('orders', 'created_at', 'datetime'),
})
sales.add_relationships({
    'user_orders': ('users', 'orders', 'users.id = orders.user_id'),
})

# Inspect
print(sales.info())

# Optional one-shot export to Ossie format
ossie_spec = sales.export('ossie')
print(f"Ossie spec: {ossie_spec}")

# === Maintenance: GC + vacuum ===
stats = s.gc_stats(compute_size=True)
print(f"GC stats: {stats}")

result = s.vacuum(preserve_days=7, dry_run=True)
print(f"Vacuum (dry run): {result}")
```

---

## 10. Cross-language equivalents

### 8.1 Rust CLI (`pond` command)

```bash
# Init + write + read
pond init /var/lib/pond
pond write users --json '[{"id":1,"name":"alice"}]' -m "init"
pond read users

# Version control
pond branch users dev
pond checkout -b users dev
pond merge users dev -m "merge dev"
pond history users --limit 10
pond undo users 2
pond ls
```

### 8.2 Go SDK

```go
import "github.com/pond/pond-go/pond"

store, _ := pond.OpenStorage("/var/lib/pond")
defer store.Free()

hash, _ := store.Write("users", []byte(`[{"id":1}]`), "init")
data, _  := store.Read("users")

store.Branch("users", "dev")
store.Checkout("users", "dev")
store.Merge("users", "dev", "main", "merge dev")
```

### 8.3 C ABI (`pond.h`)

```c
#include "pond.h"

PondKernel* k = pond_kernel_new("/var/lib/pond");
char hash[65];
pond_kernel_write(k, data, len, hash);

PondStorage* s = pond_storage_new(k);
pond_storage_write(s, "users", data, len, "init", hash);
pond_storage_branch(s, "users", "dev");
pond_storage_merge(s, "users", "dev", "main", "merge dev");
```

### 8.4 Python reference SDK (legacy — being phased out)

```python
import sys
sys.path.insert(0, "bindings/python/core")
sys.path.insert(0, "bindings/python/sdk")

from make_kernel import make_kernel
from pond_storage import PondStorage

kernel = make_kernel("file:///var/lib/pond")
storage = PondStorage(kernel)

storage.write("users", [{"id": 1, "name": "alice"}], key_col="id")
storage.branch("users", "dev")
storage.merge("users", "dev")
```

---

## 11. API reference (quick lookup)

### `Storage` (the main class)

| Method | Signature | Returns | Purpose |
|---|---|---|---|
| `Storage` | `(location, access_key?, secret_key?, region?, endpoint?)` | `Storage` | Create a connection (local FS or S3) |
| `write` | `(collection, data: bytes, message: str)` | `str` (commit hash) | Write raw bytes |
| `read` | `(collection)` | `bytes` | Read raw bytes from HEAD |
| `write_rows` | `(collection, columns, message, crdt=True)` | `str` | Write PND2 columns (auto-adds _rowid + _version by default) |
| `update_rows` | `(collection, updates, where?, key_col?, crdt=True)` | `int` | SQL-like UPDATE ... WHERE → count updated |
| `delete_rows` | `(collection, where?, key_col?, crdt=True)` | `int` | SQL-like DELETE FROM ... WHERE → count deleted |
| `merge_rows` | `(collection, rows, key_col?, crdt=True)` | `int` | SQL-like MERGE / INSERT ON CONFLICT → count merged |
| `read_rows` | `(collection, columns?, predicates?)` | `dict` | Read with projection + pruning (auto-merges shards) |
| `branch` | `(collection, branch_name)` | `str` | Create a branch |
| `checkout` | `(collection, branch_name)` | `None` | Switch active branch |
| `checkout_new` | `(collection, branch_name)` | `None` | Create + checkout (like `git -b`) |
| `merge` | `(collection, source, target?, message)` | `str` | Merge source → target |
| `history` | `(collection, limit=20)` | `list[dict]` | Walk commit history |
| `undo` | `(collection, steps=1)` | `str` | Undo last N commits |
| `revert` | `(collection, commit_hash)` | `None` | Revert to specific commit |
| `ls` | `()` | `list[dict]` | List all collections |
| `get_active_branch` | `(collection)` | `str` | Get active branch name |
| `set_active_branch` | `(collection, branch_name)` | `None` | Set active branch |
| `build_index` | `(collection, index_name, index_type, config?)` | `str` | Build index (`simple`/`ivf`/`hnsw`) — reads from collection directly |
| `lookup_index` | `(collection, index_name, index_key)` | `str?` | O(1) exact lookup (simple indexes) |
| `search_index` | `(collection, index_type, query, k=10, n_probe=10, ef=50)` | `list[(dist, id)]` | ANN search (ivf/hnsw) |
| `drop_index` | `(collection, index_name)` | `bool` | Drop an index |
| `list_indexes` | `(collection)` | `list[str]` | List indexes on a collection |
| `append_shard` | `(collection, shard_name, data: bytes)` | `str` | Append a raw CRDT shard |
| `upsert_shard` | `(collection, shard_name, rows, key_col?)` | `str` | Upsert CRDT rows (adds _rowid + _version) |
| `delete_shard` | `(collection, shard_name, rowids, key_col?)` | `str` | Write tombstone shard (deletes by _rowid) |
| `read_with_shards` | `(collection)` | `list[(name, bytes)]` | Read HEAD + all shards (raw bytes) |
| `shard_count` | `(collection)` | `int` | Count live shards |
| `compact_shards` | `(collection)` | `int` | Merge shards into HEAD, clear shard list |
| `begin_tx` | `()` | `str` | Begin a transaction (returns tx_id) |
| `commit_tx` | `(tx_id, message)` | `str` | Commit transaction (atomic visibility) |
| `abort_tx` | `(tx_id)` | `None` | Abort transaction (no-op; orphaned until GC) |
| `is_tx_committed` | `(tx_id)` | `bool` | Check if transaction is committed |
| `gc_stats` | `(compute_size=False)` | `dict` | Read-only GC analysis |
| `vacuum` | `(preserve_days, dry_run)` | `dict` | Delete unreachable blobs |
| `optimize` | `(collection?)` | `dict` | Compact shards + flatten manifests |
| `layer` | `(name, adapters?, enable_reflection=False)` | `SemanticLayer` | Get/create a semantic layer handle |
| `layers` | `()` | `list[str]` | List all semantic layer names |

### `SemanticLayer` (handle returned by `s.layer()`)

| Method | Signature | Returns | Purpose |
|---|---|---|---|
| `add_datasets` | `(datasets: list[str])` | `None` | Batch-add datasets |
| `add_metrics` | `(metrics: dict[str, str])` | `None` | Batch-add metrics `{name: expr}` |
| `add_dimensions` | `(dimensions: dict[str, (dataset, field, type)])` | `None` | Batch-add dimensions |
| `add_relationships` | `(relationships: dict[str, (from, to, join)])` | `None` | Batch-add relationships |
| `info` | `()` | `dict` | Full overview |
| `datasets` | `()` | `list[str]` | List datasets |
| `metrics` | `()` | `list[str]` | List metric names |
| `dimensions` | `()` | `list[str]` | List dimension names |
| `relationships` | `()` | `list[str]` | List relationship names |
| `adapters` | `()` | `list[str]` | List enabled adapters |
| `add_adapter` | `(adapter: str)` | `None` | Add adapter (idempotent) |
| `remove_adapter` | `(adapter: str)` | `bool` | Remove adapter (True if present) |
| `export` | `(adapter=None)` | `dict` | One-shot export in adapter format |
| `enable_reflection` | `()` | `None` | Enable reflection |
| `disable_reflection` | `()` | `None` | Disable reflection |

---

## 12. Storage layout (for debugging)

```
.pond/                                    (local FS) or s3://bucket/prefix/
├── blobs/                                content-addressed blobs (SHA-256 hash)
│   ├── ab/abc123...                      first 2 hex chars = directory
│   └── ...
├── collections/
│   └── {name}/
│       └── _branches/
│           └── {branch}/
│               └── commit                → commit hash (HEAD pointer)
├── collections/{name}/indexes/{idx}      simple index JSON blob
├── collections/{name}/indexes/ivf        IVF index binary blob
├── collections/{name}/indexes/hnsw       HNSW index binary blob
├── semantic_layers/
│   └── {layer}/
│       ├── _meta                         → {name, adapters, enable_reflection}
│       ├── datasets/{ds}                 → {name, source}
│       ├── metrics/{name}                → {name, expression, description, format}
│       ├── dimensions/{name}             → {name, dataset, field, data_type}
│       └── relationships/{name}          → {name, from, to, condition}
└── config                                (optional) PondConfig JSON
```

All paths are kernel refs (mutable name → immutable hash mappings).
The kernel's 3 primitives are: `write(bytes) → hash`, `read(hash) → bytes`,
`reference(name, hash) → ()`.

---

## 13. Design principles (why the API looks like this)

1. **Simple** — ONE storage format (PND2), ONE commit format (JSON), ONE concurrency model (CRDT)
2. **Powerful** — branch/merge + CRDT + IVF + HNSW + streaming + semantic layers + GC
3. **Performant** — O(1) point lookup, O(1) warm writes, O(1) shard writes
4. **Scalable** — linear PUTs, flat GETs, PB-scale via StatsTree
5. **Efficient** — immutable blobs (deduped), O(live) GC, parallel fetch
6. **Beautiful** — shards ARE branches, CRDT = G-Set union, no CAS
7. **Functional** — lakehouse, KV, vector, streaming, semantic, OLTP
8. **Storage-Independent** — no CAS, works on local FS / S3 / R2 / MinIO / GCS

The API surface is deliberately small: **one `Storage` class** with
methods grouped into 5 sections (Data I/O, Versioning, Indexing,
Semantic Layer, Maintenance). Everything else is an implementation
detail.

---

## 14. Performance architecture — SIMD, parallelism, CRDT

### SIMD-accelerated INT64 predicate evaluation

When `read_rows` filters by INT64 predicates (e.g., `predicates=[('age', '>', 18)]`),
the filter runs through **AVX2 SIMD instructions** that compare 4× `i64` values
per instruction. On non-x86_64 or pre-AVX2 CPUs, it falls back to scalar
(which LLVM may still auto-vectorize).

The SIMD path is used when:
- The predicate column has INT64 values in all rows
- The predicate value is numeric
- The operator is one of: `=`, `==`, `!=`, `<>`, `>`, `>=`, `<`, `<=`

For string columns, float columns, or mixed types, the scalar JSON comparison
path is used (with bool/int coercion for equality checks).

```python
# This uses AVX2 SIMD (4x i64 per instruction):
s.read_rows('users', predicates=[('age', '>', 18), ('dept', '=', 'eng')])
# age > 18 → SIMD (INT64 column)
# dept = 'eng' → scalar (string column)
```

### Parallel row group decoding

When a collection has >2 row groups, decoding runs in **parallel threads**
via `std::thread::scope` (stable since Rust 1.63, no external dependencies).
Each row group is decoded in a separate thread, then results are merged.

Small collections (≤2 row groups) decode sequentially to avoid thread spawn
overhead.

### Parallel S3 batch GETs

S3 batch reads (`read_blob_batch`) use a thread pool to issue parallel HTTP
requests, reducing wall-clock from N×RTT to ~1 RTT for N blobs.

### CRDT merge

The CRDT merge (dedup by `_rowid`, latest `_version` wins, tombstones suppress)
runs sequentially — it's typically fast because it operates on in-memory JSON
rows after decode. The merge is deterministic and O(N) in the number of rows.

### Where parallelism helps most

| Operation | Parallelism | Speedup |
|---|---|---|
| S3 batch GETs | Thread pool (32 parallel) | N× → ~1× wall-clock |
| S3 batch PUTs | Thread pool (32 parallel) | N× → ~1× wall-clock |
| Row group decode | `std::thread::scope` (>2 row groups) | Up to #cores |
| INT64 predicate filter | AVX2 SIMD (4× i64/instruction) | ~4× per instruction |
| FLOAT64 predicate filter | AVX2 SIMD (4× f64/instruction) | ~4× per instruction |
| Columnar predicate eval | SIMD filter BEFORE JSON conversion | 2-4× vs JSON-first |
| CRDT merge | Sequential (fast enough — in-memory) | N/A |
| PND2 encode | Sequential (LLVM auto-vectorizes) | ~2× via auto-vec |

### Columnar predicate evaluation benchmark

Filtering 100K rows (10 reads, average per read):

| Operation | Time | Speedup vs full scan |
|---|---|---|
| Full scan (no filter) | 257ms | 1× |
| INT64 filter (50% selectivity) | 116ms | **2.2×** |
| FLOAT64 filter (33% selectivity) | 66ms | **3.9×** |

The columnar filter skips JSON conversion for filtered-out rows — the biggest
performance win for selective queries on large datasets.

### Future performance work

- **Parallel CRDT merge**: use rayon for chunked merge (currently sequential)
- **`std::simd`**: portable SIMD API (currently nightly-only, will stabilize)
- **Parquet support**: add parquet file reading in .sql() (currently CSV/JSON only)
- **Columnar output**: skip JSON conversion entirely for simple SELECT queries
