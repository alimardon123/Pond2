"""
CollectionManifest — ONE blob per commit, ALL pruning, ANY workload.

THE MANDATE (per architecture review #2):
  "Make sure we will have less round trips possible with object storage
   for all interactions/access with our storage."

THE DESIGN:
  ONE blob per commit that contains EVERYTHING a reader needs:
    - Schema (column names + types)
    - Sort order (key column, row_group_size, chunk_size)
    - Per-row-group entries with INLINE stats + blob hashes
    - Optional hierarchical stats tree root (for PB scale)

READ PATH (3 + K round trips, irreducible on content-addressed stores):
    1. HEAD ref          (cheap, SDK-cached)
    2. commit blob       (~200 bytes — gives us manifest_hash)
    3. manifest blob     (~200 bytes/row group — gives us all blob hashes + stats)
    4. K data blobs      (parallelizable — the only "real" I/O)

WRITE PATH (N + 2 writes):
    1. N data blob writes (parallelizable)
    2. 1 manifest blob write
    3. 1 commit blob write  (contains manifest_hash)
    4. 1 HEAD ref update

This replaces:
  - ZoneMapIndex (460 LOC) — manifest has inline stats, no separate tree
  - StatsIndex (177 LOC)   — manifest is the stats index, in binary not JSON
  - zone_map_manifest blob — manifest IS the manifest, lives in commit
  - Per-row-group zone-map blobs — stats are INLINE in the manifest
  - column_chunk manifest JSON blobs — chunk hashes are INLINE in the manifest

WHAT STAYS:
  - PruningPredicate / ColumnPredicate — evaluate against manifest entries
  - ColumnSource — format-agnostic data access
  - encode_fn / decode_fn — lens's format contract
  - All 4 encodings + compression — unchanged
  - embedded_stats.py — third-level pruning in chunk blob headers
  - ColumnChunkZoneMap / ColumnChunkStats — used by manifest entries

GENERIC:
  Works for ANY workload:
    - Tabular: columns are table columns; row groups are PK ranges
    - KV:      columns are JSON fields; row groups are key ranges
    - Vector:  columns are dimensions + vector_id; stats are bounding boxes
    - Streaming: columns are segment metadata; row groups are byte ranges
    - Notebooks: columns are cell metadata; row groups are cell ranges

BINARY FORMAT (PND1-manifest v1):
    +-----------------------------+
    | Magic (4B): b"PMAN"         |
    | Version (1B): 1             |
    | Flags (1B):                 |
    |   bit 0: has_stats_tree     |
    |   bit 1: has_bloom          |
    |   bit 2-7: reserved         |
    | n_row_groups (4B uint32)    |
    | n_columns (2B uint16)       |
    +-----------------------------+
    | Schema section:             |
    |   For each column:          |
    |     name_len (1B)           |
    |     name (UTF-8)            |
    |     value_type (1B)         |
    +-----------------------------+
    | Sort order section:         |
    |   key_col_len (1B)          |
    |   key_col (UTF-8)           |
    |   row_group_size (4B)       |
    |   chunk_size (4B)           |
    +-----------------------------+
    | Optional sections:          |
    |   stats_tree_root (32B)     |  if flags bit 0
    |   bloom_filter_ref (32B)    |  if flags bit 1
    +-----------------------------+
    | Row group entries:          |
    |   For each row group:       |
    |     key_len (2B)            |
    |     key (UTF-8)             |
    |     blob_hash (32B binary)  |
    |     n_rows (4B)             |
    |     storage_mode (1B):      |
    |       0=whole_blob          |
    |       1=column_chunks       |
    |       2=encoded             |
    |     For each column (n_columns entries):  |
    |       value_type (1B)       |
    |       has_min (1B)          |
    |       min (8B or var-len)   |
    |       max (8B or var-len)   |
    |       null_count (4B)       |
    |       n_chunks (2B)         |
    |       For each chunk:       |
    |         chunk_blob_hash (32B)|
    |         chunk_min (8B or var)|
    |         chunk_max (8B or var)|
    |         chunk_null_count (4B)|
    |         encoding (1B)       |
    |         encoding_meta_len (2B)|
    |         encoding_meta (var) |

SIZE ESTIMATE:
    - ~50 bytes per row group (whole-blob mode, no chunks)
    - ~80 bytes per column chunk (with hash + stats)
    - 100 row groups × 5 columns × 10 chunks = 100 × (50 + 5×80) = 45 KB
    - ONE fetch on S3.

LAZY HIERARCHICAL STATS TREE (PB scale):
    For >10K row groups (manifest >5MB), the manifest delegates to a
    stats tree: a Prolly tree with aggregated stats in internal nodes.
    Built lazily on first OLAP read; cached via content addressing.
    See `stats_tree.py`.
"""

from __future__ import annotations

import struct
import os
import sys
from dataclasses import dataclass, field
from typing import Optional, Any, Iterator

# Make bindings/python/core importable
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                  "..", "..", "..", "bindings/python/core"))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from kernel import PondMinimal  # noqa: E402

# Reuse value-type constants from embedded_stats for consistency
from embedded_stats import (  # noqa: E402
    VALUE_TYPE_INT64, VALUE_TYPE_FLOAT64, VALUE_TYPE_STRING, VALUE_TYPE_NULL,
)

# Reuse ColumnChunkStats / ColumnChunkZoneMap for chunk-level stats
try:
    from column_chunk_zone_map import ColumnChunkStats, ColumnChunkZoneMap  # noqa: E402
    _HAVE_CCZM = True
except ImportError:
    _HAVE_CCZM = False
    # Define stubs so the code below doesn't break at import time
    class ColumnChunkStats:  # type: ignore
        pass
    class ColumnChunkZoneMap:  # type: ignore
        pass


# ---------------------------------------------------------------------------
# Manifest constants
# ---------------------------------------------------------------------------

_MANIFEST_MAGIC = b"PMAN"
_MANIFEST_VERSION = 1

# Storage modes
STORAGE_WHOLE_BLOB = 0      # one Parquet blob per row group
STORAGE_COLUMN_CHUNKS = 1   # per-column-chunk Parquet blobs (manifest blob has hashes)
STORAGE_ENCODED = 2         # per-column-chunk encoded blobs (PND1 + compression)

# Flags
FLAG_HAS_STATS_TREE = 0x01
FLAG_HAS_BLOOM = 0x02
FLAG_HAS_PARENT_MANIFEST = 0x04  # delta-manifest for O(1) appends
FLAG_HAS_INLINE_BLOOM = 0x08  # bloom filter bitset embedded in manifest


# ---------------------------------------------------------------------------
# Bloom filter helpers (inline, zero-dependency)
# ---------------------------------------------------------------------------

def _bloom_build(keys: list[str], bits_per_key: int = 10) -> bytes:
    """Build a Bloom filter bitset for the given keys.

    Uses double hashing (SHA-256 split) with k=7 hash functions.
    Returns a bytes object whose length * 8 == n_bits exactly
    (n_bits is rounded up to a multiple of 8 so that _bloom_check's
    len(bitset)*8 produces the same modulus).
    """
    import hashlib as _hl
    n = len(keys)
    if n == 0:
        return b""
    # Round n_bits up to a multiple of 8 — _bloom_check computes its
    # modulus as len(bitset)*8, so the two MUST agree or hash indices
    # diverge and produce false negatives.
    raw_bits = max(n * bits_per_key, 64)
    n_bits = ((raw_bits + 7) // 8) * 8
    byte_len = n_bits // 8
    bitset = bytearray(byte_len)
    for key in keys:
        h = _hl.sha256(key.encode("utf-8")).digest()
        h1 = int.from_bytes(h[:16], "little")
        h2 = int.from_bytes(h[16:], "little")
        for i in range(7):
            idx = (h1 + i * h2) % n_bits
            bitset[idx // 8] |= 1 << (idx % 8)
    return bytes(bitset)


def _bloom_check(bitset: bytes, key: str) -> bool:
    """Check if a key might be in the Bloom filter.

    Returns False if the key is DEFINITELY NOT in the set.
    Returns True if the key MIGHT be in the set (may be false positive).
    Uses the same k=7 double hashing as _bloom_build.
    """
    import hashlib as _hl
    if not bitset:
        return True  # empty bloom = match everything
    n_bits = len(bitset) * 8
    h = _hl.sha256(key.encode("utf-8")).digest()
    h1 = int.from_bytes(h[:16], "little")
    h2 = int.from_bytes(h[16:], "little")
    for i in range(7):
        idx = (h1 + i * h2) % n_bits
        if not (bitset[idx // 8] & (1 << (idx % 8))):
            return False
    return True


# ---------------------------------------------------------------------------
# Data classes for manifest entries
# ---------------------------------------------------------------------------

@dataclass
class ColumnChunkEntry:
    """Per-chunk entry in a manifest — chunk-level stats + blob hash."""
    blob_hash: str             # 32-byte hex hash of the chunk blob
    min: Any = None
    max: Any = None
    null_count: int = 0
    encoding: int = 0          # 0=raw, 1=rle, 2=dict, 3=bitpack
    encoding_meta: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "blob_hash": self.blob_hash,
            "min": self.min,
            "max": self.max,
            "null_count": self.null_count,
            "encoding": self.encoding,
            "encoding_meta": self.encoding_meta,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "ColumnChunkEntry":
        return cls(
            blob_hash=d["blob_hash"],
            min=d.get("min"),
            max=d.get("max"),
            null_count=d.get("null_count", 0),
            encoding=d.get("encoding", 0),
            encoding_meta=d.get("encoding_meta", {}),
        )

    @classmethod
    def from_cczm_stats(cls, stats: "ColumnChunkStats",
                         encoding: int = 0,
                         encoding_meta: Optional[dict] = None) -> "ColumnChunkEntry":
        """Build from a ColumnChunkStats object (from column_chunk_zone_map)."""
        return cls(
            blob_hash=stats.blob_hash or "",
            min=stats.min,
            max=stats.max,
            null_count=stats.null_count,
            encoding=encoding,
            encoding_meta=encoding_meta or {},
        )


@dataclass
class ColumnStatsEntry:
    """Per-column stats for one row group — min/max/null_count + optional chunks."""
    name: str
    value_type: int = VALUE_TYPE_NULL
    min: Any = None
    max: Any = None
    null_count: int = 0
    chunks: list[ColumnChunkEntry] = field(default_factory=list)

    def can_prune(self, op: str, value: Any) -> bool:
        """Return True if this column's stats prove NO rows can match.

        Used at row-group level (skip the entire row group).
        """
        if self.min is None or self.max is None:
            return False  # no stats — can't prune
        try:
            if op == ">" and self.max <= value:
                return True
            if op == ">=" and self.max < value:
                return True
            if op == "<" and self.min >= value:
                return True
            if op == "<=" and self.min > value:
                return True
            if op == "=" and (value < self.min or value > self.max):
                return True
            if op == "in":
                if not value:
                    return True
                v_min, v_max = min(value), max(value)
                if self.max < v_min or self.min > v_max:
                    return True
        except TypeError:
            return False  # type mismatch — can't prune
        return False

    def prune_chunks(self, op: str, value: Any) -> Optional[list[int]]:
        """Find which chunk indices MIGHT match a predicate.

        Returns the indices of chunks that cannot be pruned (might match).
        Returns None if no chunk-level pruning is possible (no chunks, or
        chunk stats missing).
        Returns [] if all chunks pruned (caller should skip the row group
        for this column — but be careful: an empty intersection across
        columns means the whole row group is pruned, which is correct).
        """
        if not self.chunks:
            return None
        surviving = []
        for i, chunk in enumerate(self.chunks):
            if chunk.min is None or chunk.max is None:
                surviving.append(i)  # no stats — can't prune
                continue
            try:
                if op == ">" and chunk.max <= value: continue
                if op == ">=" and chunk.max < value: continue
                if op == "<" and chunk.min >= value: continue
                if op == "<=" and chunk.min > value: continue
                if op == "=" and (value < chunk.min or value > chunk.max): continue
                if op == "in":
                    if not value: continue
                    v_min, v_max = min(value), max(value)
                    if chunk.max < v_min or chunk.min > v_max: continue
            except TypeError:
                pass  # type mismatch — can't prune, keep chunk
            surviving.append(i)
        return surviving


@dataclass
class RowGroupEntry:
    """Per-row-group entry in a manifest — key, blob hash, and per-column stats."""
    key: str                        # e.g., "rg/9999"
    blob_hash: str                  # data blob hash (whole-blob mode) or chunk-manifest hash
    n_rows: int = 0
    storage_mode: int = STORAGE_WHOLE_BLOB
    columns: list[ColumnStatsEntry] = field(default_factory=list)

    def can_prune(self, predicates: list[tuple[str, str, Any]]) -> bool:
        """Return True if this row group CANNOT match any predicate (skip it)."""
        if not predicates:
            return False  # no predicates — never prune
        col_lookup = {c.name: c for c in self.columns}
        for col_name, op, val in predicates:
            col = col_lookup.get(col_name)
            if col is None:
                continue  # no stats — can't prune on this column
            if col.can_prune(op, val):
                return True  # this column proves the row group can't match
        return False

    def get_column(self, name: str) -> Optional[ColumnStatsEntry]:
        for c in self.columns:
            if c.name == name:
                return c
        return None

    def to_dict(self) -> dict:
        return {
            "key": self.key,
            "blob_hash": self.blob_hash,
            "n_rows": self.n_rows,
            "storage_mode": self.storage_mode,
            "columns": [c.__dict__ for c in self.columns],
        }


# ---------------------------------------------------------------------------
# CollectionManifest — the main class
# ---------------------------------------------------------------------------

class CollectionManifest:
    """ONE blob per commit with ALL pruning info for a collection.

    Built atomically with each commit. Read in ONE fetch on S3.

    Lifecycle:
      1. writer = CollectionManifest(kernel)
      2. writer.set_schema(columns, key_col, row_group_size, chunk_size)
      3. For each row group: writer.add_row_group(entry)
      4. manifest_hash = writer.commit(collection_name)  # writes ONE blob
      5. Reader: manifest = CollectionManifest.load(kernel, manifest_hash)

    The manifest is content-addressed — the same row groups always produce
    the same manifest bytes (deduplication for free).
    """

    def __init__(self, kernel: PondMinimal):
        self.kernel = kernel
        self._columns: list[tuple[str, int]] = []  # (name, value_type)
        self._key_col: str = ""
        self._row_group_size: int = 0
        self._chunk_size: int = 0
        self._row_groups: list[RowGroupEntry] = []
        self._stats_tree_root: Optional[str] = None
        self._bloom_filter_ref: Optional[str] = None
        self._inline_bloom: Optional[bytes] = None  # embedded bloom bitset
        self._parent_manifest_hash: Optional[str] = None
        # Hidden partitioning: partition spec stored in the manifest
        # (Iceberg-style). None = no partitioning.
        # Format: {"columns": ["date"], "transform": "identity"|"hour"|"day"|"month"|"bucket:N"}
        self._partition_spec: Optional[dict] = None
        # Schema evolution: version number for schema changes
        self._schema_version: int = 0

    # ------------------------------------------------------------------
    # Builder API — write side
    # ------------------------------------------------------------------

    def set_schema(self, columns: list[tuple[str, int]],
                   key_col: str = "",
                   row_group_size: int = 0,
                   chunk_size: int = 0) -> None:
        """Set the schema and sort-order info.

        Args:
            columns: list of (column_name, value_type) tuples
                value_type: 1=INT64, 2=FLOAT64, 3=STRING, 4=NULL
            key_col: name of the sort key column ("" if none)
            row_group_size: rows per row group
            chunk_size: rows per column chunk
        """
        self._columns = list(columns)
        self._key_col = key_col
        self._row_group_size = row_group_size
        self._chunk_size = chunk_size

    def add_row_group(self, entry: RowGroupEntry) -> None:
        """Add a row group entry to the manifest."""
        self._row_groups.append(entry)

    def set_stats_tree_root(self, root_hash: str) -> None:
        """Attach a hierarchical stats tree root (for PB scale)."""
        self._stats_tree_root = root_hash

    def set_partition_spec(self, spec: Optional[dict]) -> None:
        """Set the hidden partition spec (Iceberg-style).

        Args:
            spec: {"columns": ["date"], "transform": "identity"|"day"|"hour"|"month"|"bucket:N"}
                  None = no partitioning
        """
        self._partition_spec = spec

    def set_schema_version(self, version: int) -> None:
        """Set the schema version (for schema evolution tracking)."""
        self._schema_version = version

    def set_bloom_filter_ref(self, ref: str) -> None:
        """Attach a bloom filter ref (for membership queries)."""
        self._bloom_filter_ref = ref

    def set_inline_bloom(self, bloom_bits: bytes) -> None:
        """Embed a bloom filter bitset directly in the manifest.

        Unlike set_bloom_filter_ref (which stores a hash reference to an
        external blob), this embeds the bitset inline — 0 extra GETs for
        bloom membership checks. For 10K keys at 10 bits/key = ~12.5 KB
        overhead. Negative lookups (key not in collection) skip the data
        blob fetch entirely.
        """
        self._inline_bloom = bloom_bits

    def set_parent_manifest(self, parent_hash: str) -> None:
        """Set the parent manifest hash for delta-appends (O(1) at PB scale).

        Fix (Round 9 Issue #5): instead of reading ALL existing row groups
        to rebuild the manifest on every append, a delta-manifest only stores
        the NEW row groups + a pointer to the parent manifest. The reader
        walks the parent chain to find all entries.

        This makes append() O(new_row_groups) instead of O(total_row_groups).
        """
        self._parent_manifest_hash = parent_hash

    def commit(self) -> str:
        """Serialize the manifest and write it as ONE kernel blob.

        Returns:
            The manifest blob hash. The lens writes this hash into the
            commit blob's manifest_hash field.
        """
        data = self.encode()
        return self.kernel.write(data)

    # ------------------------------------------------------------------
    # Encoding — binary PND1-manifest v1
    # ------------------------------------------------------------------

    def encode(self) -> bytes:
        """Encode the manifest as binary bytes.

        Format: see module docstring. Extended with partition_spec and
        schema_version (appended at the end — backward compatible).
        """
        flags = 0
        if self._stats_tree_root:
            flags |= FLAG_HAS_STATS_TREE
        if self._bloom_filter_ref:
            flags |= FLAG_HAS_BLOOM
        if self._parent_manifest_hash:
            flags |= FLAG_HAS_PARENT_MANIFEST
        if self._inline_bloom is not None:
            flags |= FLAG_HAS_INLINE_BLOOM

        buf = bytearray()
        buf += _MANIFEST_MAGIC
        buf += struct.pack("<BB", _MANIFEST_VERSION, flags)
        n_inline_row_groups = 0 if self._stats_tree_root else len(self._row_groups)
        buf += struct.pack("<IH", n_inline_row_groups, len(self._columns))

        # Schema section
        for name, vtype in self._columns:
            name_bytes = name.encode("utf-8")
            buf += struct.pack("<B", len(name_bytes))
            buf += name_bytes
            buf += struct.pack("<B", vtype)

        # Sort order section
        key_col_bytes = self._key_col.encode("utf-8")
        buf += struct.pack("<B", len(key_col_bytes))
        buf += key_col_bytes
        buf += struct.pack("<II", self._row_group_size, self._chunk_size)

        # Optional sections
        if self._stats_tree_root:
            buf += bytes.fromhex(self._stats_tree_root)
        if self._bloom_filter_ref:
            buf += bytes.fromhex(self._bloom_filter_ref)
        if self._parent_manifest_hash:
            buf += bytes.fromhex(self._parent_manifest_hash)
        if self._inline_bloom is not None:
            buf += struct.pack("<I", len(self._inline_bloom))
            buf += self._inline_bloom

        # Row group entries
        if not self._stats_tree_root:
            for rg in self._row_groups:
                buf += self._encode_row_group(rg)

        # Extended fields (appended — backward compatible with older readers)
        # Schema version (4 bytes)
        buf += struct.pack("<I", self._schema_version)
        # Partition spec (optional, JSON-encoded)
        if self._partition_spec:
            spec_json = json.dumps(self._partition_spec).encode("utf-8")
            buf += struct.pack("<I", len(spec_json))
            buf += spec_json
        else:
            buf += struct.pack("<I", 0)  # no partition spec

        return bytes(buf)

    def _encode_row_group(self, rg: RowGroupEntry) -> bytes:
        buf = bytearray()
        key_bytes = rg.key.encode("utf-8")
        buf += struct.pack("<H", len(key_bytes))
        buf += key_bytes
        buf += bytes.fromhex(rg.blob_hash)
        buf += struct.pack("<IB", rg.n_rows, rg.storage_mode)

        # Per-column entries (n_columns total, in schema order)
        col_lookup = {c.name: c for c in rg.columns}
        for col_name, expected_vtype in self._columns:
            col = col_lookup.get(col_name)
            if col is None:
                # Missing column — write empty entry
                buf += struct.pack("<BB", expected_vtype, 0)
                buf += struct.pack("<I", 0)  # null_count
                buf += struct.pack("<H", 0)  # n_chunks
                continue

            buf += struct.pack("<B", col.value_type)
            has_min = col.min is not None and col.max is not None
            buf += struct.pack("<B", 1 if has_min else 0)
            if has_min:
                buf += _encode_value(col.value_type, col.min)
                buf += _encode_value(col.value_type, col.max)
            buf += struct.pack("<I", col.null_count)
            buf += struct.pack("<H", len(col.chunks))

            for chunk in col.chunks:
                buf += bytes.fromhex(chunk.blob_hash)
                # Chunk min/max (same encoding as column min/max)
                chunk_has_min = chunk.min is not None and chunk.max is not None
                # Encode has_min flag (1 byte) + min/max (if present)
                # To keep the format compact, we use the SAME has_min byte
                # convention as column-level stats.
                buf += struct.pack("<B", 1 if chunk_has_min else 0)
                if chunk_has_min:
                    buf += _encode_value(col.value_type, chunk.min)
                    buf += _encode_value(col.value_type, chunk.max)
                buf += struct.pack("<I", chunk.null_count)
                buf += struct.pack("<B", chunk.encoding)
                # encoding_meta as a small JSON blob (length-prefixed)
                import json
                meta_bytes = json.dumps(chunk.encoding_meta,
                                         sort_keys=True,
                                         default=str).encode("utf-8")
                buf += struct.pack("<H", len(meta_bytes))
                buf += meta_bytes

        return bytes(buf)

    # ------------------------------------------------------------------
    # Decoding — read side
    # ------------------------------------------------------------------

    @classmethod
    def load(cls, kernel: PondMinimal, manifest_hash: str) -> "CollectionManifest":
        """Load a manifest from a kernel blob.

        Args:
            kernel: the PondMinimal kernel
            manifest_hash: the manifest blob hash

        Returns:
            A populated CollectionManifest instance.
        """
        data = kernel.read_blob(manifest_hash)
        return cls.decode(kernel, data)

    @classmethod
    def decode(cls, kernel: PondMinimal, data: bytes) -> "CollectionManifest":
        """Decode manifest bytes into a CollectionManifest instance."""
        if data[:4] != _MANIFEST_MAGIC:
            raise ValueError(f"Not a manifest blob (magic={data[:4]!r})")
        version, flags = struct.unpack("<BB", data[4:6])
        if version != _MANIFEST_VERSION:
            raise ValueError(f"Unsupported manifest version: {version}")
        n_row_groups, n_columns = struct.unpack("<IH", data[6:12])
        pos = 12

        manifest = cls(kernel)

        # Schema section
        columns: list[tuple[str, int]] = []
        for _ in range(n_columns):
            name_len = data[pos]; pos += 1
            name = data[pos:pos+name_len].decode("utf-8"); pos += name_len
            vtype = data[pos]; pos += 1
            columns.append((name, vtype))
        manifest._columns = columns

        # Sort order section
        key_col_len = data[pos]; pos += 1
        key_col = data[pos:pos+key_col_len].decode("utf-8"); pos += key_col_len
        row_group_size, chunk_size = struct.unpack("<II", data[pos:pos+8])
        pos += 8
        manifest._key_col = key_col
        manifest._row_group_size = row_group_size
        manifest._chunk_size = chunk_size

        # Optional sections
        if flags & FLAG_HAS_STATS_TREE:
            manifest._stats_tree_root = data[pos:pos+32].hex(); pos += 32
        if flags & FLAG_HAS_BLOOM:
            manifest._bloom_filter_ref = data[pos:pos+32].hex(); pos += 32
        if flags & FLAG_HAS_PARENT_MANIFEST:
            manifest._parent_manifest_hash = data[pos:pos+32].hex(); pos += 32
        if flags & FLAG_HAS_INLINE_BLOOM:
            bloom_len = struct.unpack_from("<I", data, pos)[0]; pos += 4
            manifest._inline_bloom = data[pos:pos+bloom_len]; pos += bloom_len

        # Row group entries
        for _ in range(n_row_groups):
            rg, pos = cls._decode_row_group(data, pos, columns)
            manifest._row_groups.append(rg)

        # Extended fields (appended — backward compatible)
        # Try to read schema_version + partition_spec if available
        try:
            manifest._schema_version = struct.unpack_from("<I", data, pos)[0]
            pos += 4
            spec_len = struct.unpack_from("<I", data, pos)[0]
            pos += 4
            if spec_len > 0:
                manifest._partition_spec = json.loads(data[pos:pos+spec_len].decode("utf-8"))
                pos += spec_len
        except (struct.error, json.JSONDecodeError, UnicodeDecodeError, IndexError):
            # Older manifest without extended fields — use defaults
            manifest._schema_version = 0
            manifest._partition_spec = None

        return manifest

    @classmethod
    def _decode_row_group(cls, data: bytes, pos: int,
                          columns: list[tuple[str, int]]) -> tuple[RowGroupEntry, int]:
        key_len = struct.unpack("<H", data[pos:pos+2])[0]; pos += 2
        key = data[pos:pos+key_len].decode("utf-8"); pos += key_len
        blob_hash = data[pos:pos+32].hex(); pos += 32
        n_rows, storage_mode = struct.unpack("<IB", data[pos:pos+5]); pos += 5

        rg = RowGroupEntry(
            key=key, blob_hash=blob_hash,
            n_rows=n_rows, storage_mode=storage_mode,
        )

        for col_name, expected_vtype in columns:
            vtype = data[pos]; pos += 1
            has_min = data[pos]; pos += 1
            mn = mx = None
            if has_min:
                mn, pos = _decode_value(vtype, data, pos)
                mx, pos = _decode_value(vtype, data, pos)
            null_count = struct.unpack("<I", data[pos:pos+4])[0]; pos += 4
            n_chunks = struct.unpack("<H", data[pos:pos+2])[0]; pos += 2

            chunks: list[ColumnChunkEntry] = []
            for _ in range(n_chunks):
                chunk_blob = data[pos:pos+32].hex(); pos += 32
                chunk_has_min = data[pos]; pos += 1
                c_mn = c_mx = None
                if chunk_has_min:
                    c_mn, pos = _decode_value(vtype, data, pos)
                    c_mx, pos = _decode_value(vtype, data, pos)
                c_null = struct.unpack("<I", data[pos:pos+4])[0]; pos += 4
                c_encoding = data[pos]; pos += 1
                meta_len = struct.unpack("<H", data[pos:pos+2])[0]; pos += 2
                if meta_len:
                    import json
                    c_meta = json.loads(data[pos:pos+meta_len].decode("utf-8"))
                else:
                    c_meta = {}
                pos += meta_len
                chunks.append(ColumnChunkEntry(
                    blob_hash=chunk_blob, min=c_mn, max=c_mx,
                    null_count=c_null, encoding=c_encoding,
                    encoding_meta=c_meta,
                ))

            rg.columns.append(ColumnStatsEntry(
                name=col_name, value_type=vtype,
                min=mn, max=mx, null_count=null_count,
                chunks=chunks,
            ))

        return rg, pos

    # ------------------------------------------------------------------
    # Reader API — read side
    # ------------------------------------------------------------------

    @property
    def columns(self) -> list[tuple[str, int]]:
        return list(self._columns)

    @property
    def column_names(self) -> list[str]:
        return [c[0] for c in self._columns]

    @property
    def key_col(self) -> str:
        return self._key_col

    @property
    def row_group_size(self) -> int:
        return self._row_group_size

    @property
    def chunk_size(self) -> int:
        return self._chunk_size

    @property
    def row_groups(self) -> list[RowGroupEntry]:
        return list(self._row_groups)

    @property
    def stats_tree_root(self) -> Optional[str]:
        return self._stats_tree_root

    @property
    def parent_manifest_hash(self) -> Optional[str]:
        """The parent manifest hash (for delta-appends). None if this is a base manifest."""
        return self._parent_manifest_hash

    @property
    def partition_spec(self) -> Optional[dict]:
        """The hidden partition spec (Iceberg-style). None = no partitioning."""
        return self._partition_spec

    @property
    def schema_version(self) -> int:
        """The schema version (for schema evolution tracking)."""
        return self._schema_version

    def row_group_index(self, rg: RowGroupEntry) -> Optional[int]:
        """Return the index of `rg` in the row group list, or None."""
        target_id = id(rg)
        for i, r in enumerate(self._row_groups):
            if id(r) == target_id:
                return i
        return None

    def find_row_group(self, key: str) -> Optional[RowGroupEntry]:
        """Find the row group whose key matches `key` (smallest key >= target).

        Used for point lookups — the row group with max_pk >= key contains
        the row. Row groups are sorted by key, so we use binary search
        (O(log N) instead of O(N) linear scan).

        At PB scale (stats_tree_root set), this is O(log N) via the
        hierarchical stats tree. At small scale, it's O(log N) via bisect.

        The caller must format the key via `_format_rg_key()` before
        calling this method (row group keys are formatted).

        If an inline bloom filter is present, negative lookups (key not in
        collection) return None immediately without scanning any row groups.
        """
        # Bloom filter: negative lookup fast-path (0 data GETs)
        if self._inline_bloom is not None:
            if not _bloom_check(self._inline_bloom, key):
                return None

        # PB-scale path: walk the stats tree top-down
        if self._stats_tree_root:
            return self._find_row_group_via_stats_tree(key)
        # O(log N) binary search — row groups are sorted by key
        lo, hi = 0, len(self._row_groups)
        while lo < hi:
            mid = (lo + hi) // 2
            if self._row_groups[mid].key < key:
                lo = mid + 1
            else:
                hi = mid
        if lo < len(self._row_groups):
            return self._row_groups[lo]
        return None

    def _find_row_group_via_stats_tree(self, key: str) -> Optional[RowGroupEntry]:
        """O(log N) point lookup via the hierarchical stats tree.

        Walks the tree top-down, descending into the child whose
        [min_key, max_key] range contains `key`. At the leaf, returns
        the matching RowGroupEntry.

        Each tree level is 1 S3 GET (cached by content addressing).
        Total: O(log N) GETs for cold lookup.
        """
        try:
            from stats_tree import StatsTreeReader, InternalChild
        except ImportError:
            # Stats tree not available — fall back to binary search
            lo, hi = 0, len(self._row_groups)
            while lo < hi:
                mid = (lo + hi) // 2
                if self._row_groups[mid].key < key:
                    lo = mid + 1
                else:
                    hi = mid
            if lo < len(self._row_groups):
                return self._row_groups[lo]
            return None

        reader = StatsTreeReader(self.kernel, self._stats_tree_root)
        # Walk the tree to find the smallest leaf entry with key >= target
        # The stats tree's leaves contain RowGroupEntry objects sorted by key.
        # We do a top-down descent: at each internal node, find the first
        # child whose max_key >= target, then descend.
        return reader.find_row_group(key)

    def scan_with_pruning(
            self,
            predicates: Optional[list[tuple[str, str, Any]]] = None,
            start_key: Optional[str] = None,
            end_key: Optional[str] = None,
    ) -> Iterator[RowGroupEntry]:
        """Yield row groups that MIGHT match the predicates.

        Evaluates predicates IN MEMORY against the manifest's inline stats.
        No S3 fetches — just memory work.

        At PB scale (stats_tree_root set), this delegates to the
        StatsTreeReader which walks the tree top-down, pruning subtrees
        whose aggregated stats prove they can't match. O(log N + K) reads.

        For delta-manifests (parent_manifest_hash set), this yields the
        inline row groups AND walks the parent chain to find all entries.
        The parent walk is O(chain_length) GETs — typically 1-3 appends
        before a compaction rebuilds the full manifest.

        Args:
            predicates: list of (column, op, value) tuples. None = no pruning.
            start_key: inclusive lower bound on row group keys (None = no lower)
            end_key: inclusive upper bound on row group keys (None = no upper)

        Yields:
            RowGroupEntry objects for row groups that might match.
            The caller fetches only these data blobs.
        """
        # P10 fix: StatsTree is built during compact/optimize (writer-side),
        # NOT on the read path. Readers just find it pre-built in the manifest.
        # If no stats_tree_root exists, fall through to the flat manifest path
        # (works correctly, just slower at PB scale — O(N) instead of O(log N)).
        # The read path NEVER writes to storage (read-only consumers are safe).

        # PB-scale path: delegate to the stats tree reader
        if self._stats_tree_root:
            try:
                from stats_tree import StatsTreeReader
                reader = StatsTreeReader(self.kernel, self._stats_tree_root)
                yield from reader.scan_with_pruning(
                    predicates, start_key, end_key)
                # Fix (Round 20 Issue #2): DON'T early-return if we also
                # have a parent_manifest_hash. The stats tree only contains
                # the DELTA's new entries. We must also walk the parent
                # chain to get OLD entries.
                if not self._parent_manifest_hash:
                    return  # no parent — stats tree has everything
                # Fall through to parent chain walk below
            except ImportError:
                pass  # fall through to linear scan

        # Inline row groups (or delta entries when stats tree is set)
        for rg in self._row_groups:
            # Key range filter
            # rg.key is the MAX pk in the group. For start_key, we can
            # safely skip groups whose max < start_key (all rows too small).
            if start_key is not None and rg.key < start_key:
                continue
            # For end_key, we CANNOT skip groups whose max > end_key,
            # because the group may still contain rows <= end_key.
            # Fix (Round 16 Issue #1): use the key column's MIN stat
            # to decide if the group can be excluded. If min > end_key,
            # ALL rows in the group are > end_key → safe to skip.
            if end_key is not None:
                # Find the key column's min stat
                key_col_min = None
                if self._key_col:
                    for col in rg.columns:
                        if col.name == self._key_col:
                            key_col_min = col.min
                            break
                if key_col_min is not None:
                    # Fix (Round 17 Issue #2): compare UNFORMATTED values.
                    # end_key is formatted as "rg/..." (e.g., "rg/000...099").
                    # key_col_min is the raw value (e.g., 50 as int).
                    # Fix (Round 17 Issue #2b): str(50) > "000...099" is True
                    # lexicographically ("5" > "0"). Must try numeric first.
                    end_raw = end_key
                    if isinstance(end_key, str) and end_key.startswith("rg/"):
                        end_raw = end_key[3:]
                    try:
                        # Try numeric comparison first
                        end_num = int(end_raw)
                        key_num = int(key_col_min)
                        if key_num > end_num:
                            continue
                    except (ValueError, TypeError):
                        # Fall back to string comparison
                        try:
                            if str(key_col_min) > str(end_raw):
                                continue
                        except TypeError:
                            pass  # can't compare — don't skip (safe)
                # No min stats — don't skip (the row-level filter will handle it)
            # Predicate pruning
            if predicates and rg.can_prune(predicates):
                continue
            yield rg

        # Walk parent chain for delta-manifests.
        # Fix (Round 11 Issue #2): the parent may use a stats tree, in which
        # case parent.scan_with_pruning() delegates to StatsTreeReader and
        # yields ALL parent entries. But the parent's stats tree contains
        # the BASE entries — the inline entries above are the DELTA.
        # So we must NOT re-yield entries that are already in the parent.
        # The parent chain walk handles this correctly: inline entries are
        # new (appended), parent entries are old (existing). They have
        # different rg_keys so no duplicates.
        #
        # HOWEVER: when the parent itself has a parent (delta chain depth > 1),
        # the parent's scan_with_pruning yields BOTH its inline entries AND
        # its parent's entries. If we then also load the grandparent, we get
        # duplicates. Fix: only walk ONE level — the immediate parent — and
        # let the parent's own scan_with_pruning handle its parent chain.
        if self._parent_manifest_hash:
            try:
                parent = CollectionManifest.load(self.kernel, self._parent_manifest_hash)
                # Delegate to parent.scan_with_pruning which handles its own
                # parent chain (or stats tree) recursively. This avoids
                # duplicate yields from multi-level delta chains.
                yield from parent.scan_with_pruning(predicates, start_key, end_key)
            except (ValueError, KeyError):
                pass  # parent manifest not found — return only inline entries

    def scan_column_chunks(
            self,
            column: str,
            op: str,
            value: Any,
    ) -> dict[str, list[int]]:
        """For each row group, compute surviving chunk indices for a column.

        Returns:
            Dict mapping row_group_key → list of surviving chunk indices.
            Row groups not in the dict are pruned entirely.
        """
        result: dict[str, list[int]] = {}
        for rg in self._row_groups:
            col = rg.get_column(column)
            if col is None:
                continue
            if col.can_prune(op, value):
                continue  # row group pruned
            surviving = col.prune_chunks(op, value)
            if surviving is None:
                # No chunk-level stats — must read all chunks for this row group
                surviving = list(range(len(col.chunks)))
            if surviving:
                result[rg.key] = surviving
        return result

    def total_rows(self) -> int:
        """Total rows across all row groups."""
        return sum(rg.n_rows for rg in self._row_groups)


# ---------------------------------------------------------------------------
# Helpers — value encoding (shared with embedded_stats)
# ---------------------------------------------------------------------------

def _encode_value(value_type: int, value: Any) -> bytes:
    """Encode a single min/max value as binary bytes."""
    if value_type == VALUE_TYPE_INT64:
        return struct.pack("<q", int(value))
    if value_type == VALUE_TYPE_FLOAT64:
        return struct.pack("<d", float(value))
    if value_type == VALUE_TYPE_STRING:
        s = str(value).encode("utf-8")
        return struct.pack("<I", len(s)) + s
    # NULL or unknown — 0 bytes
    return b""


def _decode_value(value_type: int, data: bytes, pos: int) -> tuple[Any, int]:
    """Decode a single min/max value from binary bytes."""
    if value_type == VALUE_TYPE_INT64:
        v = struct.unpack("<q", data[pos:pos+8])[0]
        return v, pos + 8
    if value_type == VALUE_TYPE_FLOAT64:
        v = struct.unpack("<d", data[pos:pos+8])[0]
        return v, pos + 8
    if value_type == VALUE_TYPE_STRING:
        slen = struct.unpack("<I", data[pos:pos+4])[0]
        pos += 4
        s = data[pos:pos+slen].decode("utf-8")
        return s, pos + slen
    return None, pos


# ---------------------------------------------------------------------------
# Convenience: build a manifest from a ColumnChunkZoneMap
# ---------------------------------------------------------------------------

def build_manifest_from_zone_map(
        kernel: PondMinimal,
        row_group_key: str,
        data_blob_hash: str,
        n_rows: int,
        zone_map,  # ZoneMap from pruning.py
        cczm: Optional["ColumnChunkZoneMap"] = None,
        encoding_meta_per_col: Optional[dict[str, list[dict]]] = None,
        storage_mode: int = STORAGE_WHOLE_BLOB,
) -> RowGroupEntry:
    """Build a RowGroupEntry from a ZoneMap + optional ColumnChunkZoneMap.

    This is the bridge between the existing zone-map-based code and the
    new manifest-based code. Lenses can call this to convert their
    existing zone-map data into manifest entries.

    Args:
        kernel: the PondMinimal kernel (unused, kept for API symmetry)
        row_group_key: e.g., "rg/9999"
        data_blob_hash: hash of the data blob (whole-blob mode) or chunk
            manifest hash (column-chunk / encoded mode)
        n_rows: number of rows in this row group
        zone_map: ZoneMap from pruning.py — has min/max/null_count dicts
        cczm: optional ColumnChunkZoneMap — if provided, chunk-level stats
            are added to each column entry
        encoding_meta_per_col: optional dict {col_name: [enc_meta per chunk]}
            from EncodedChunkStorage — if provided, encoding metadata is
            attached to each chunk entry. enc_meta is the dict returned
            by encoding.encode_column, e.g.
            {"encoding": "rle", "n_rows": 1000, "n_runs": 5}
        storage_mode: STORAGE_WHOLE_BLOB / STORAGE_COLUMN_CHUNKS / STORAGE_ENCODED

    Returns:
        A RowGroupEntry ready to be added to a CollectionManifest.
    """
    # _detect_value_type lives in encoding.py (not column_source.py).
    # Lazy import to avoid a circular dep at module load time.
    from encoding import _detect_value_type, ColumnEncoding

    rg = RowGroupEntry(
        key=row_group_key,
        blob_hash=data_blob_hash,
        n_rows=n_rows,
        storage_mode=storage_mode,
    )

    # Get column names from zone_map.min (or .max if min is empty)
    all_cols = set(zone_map.min.keys()) | set(zone_map.max.keys()) | set(zone_map.null_count.keys())

    for col_name in sorted(all_cols):
        # Determine value type from the actual value
        sample = zone_map.min.get(col_name)
        if sample is None:
            sample = zone_map.max.get(col_name)
        vtype = _detect_value_type([sample]) if sample is not None else VALUE_TYPE_NULL

        col_entry = ColumnStatsEntry(
            name=col_name,
            value_type=vtype,
            min=zone_map.min.get(col_name),
            max=zone_map.max.get(col_name),
            null_count=zone_map.null_count.get(col_name, 0),
        )

        # Attach chunk-level stats if cczm is provided.
        # ONLY include chunks that have a real blob_hash — phantom chunks
        # (blob_hash=None) are stats-only and don't correspond to actual
        # chunk blobs. They appear when storage_mode=STORAGE_WHOLE_BLOB
        # (whole-blob mode) where the row group is ONE blob, not per-column
        # chunks. In that mode, the manifest entry has the row-group blob
        # hash but no per-chunk entries.
        if cczm is not None and _HAVE_CCZM:
            cczm_chunks = cczm.column_chunks.get(col_name, [])
            enc_metas = (encoding_meta_per_col or {}).get(col_name, [])
            for i, chunk_stats in enumerate(cczm_chunks):
                # Skip phantom chunks (no blob_hash) — they're stats only
                if not chunk_stats.blob_hash:
                    continue
                enc_meta = enc_metas[i] if i < len(enc_metas) else {}
                # Convert string encoding name ("rle", "dict", etc.) to int
                # code (0=raw, 1=rle, 2=dict, 3=bitpack). The encoding meta
                # dict from encoding.py uses string names.
                enc_name = enc_meta.get("encoding", "raw")
                if isinstance(enc_name, str):
                    encoding_code = {"raw": 0, "rle": 1, "dict": 2,
                                       "bitpack": 3}.get(enc_name, 0)
                else:
                    encoding_code = int(enc_name)
                col_entry.chunks.append(ColumnChunkEntry.from_cczm_stats(
                    chunk_stats, encoding=encoding_code, encoding_meta=enc_meta,
                ))

        rg.columns.append(col_entry)

    return rg
