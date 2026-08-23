"""
UnifiedStorage — ONE format, ONE write path, ONE read path.

THE MANDATE:
  "Simpler storage solution that unifies all workloads in same storage
   format regardless of use with no overhead for writes and reads."

THE DESIGN:
  ONE binary blob format (PND2) for EVERY workload:
    - Tabular (Lakehouse): table columns
    - KV (KeyValue): JSON fields as columns
    - Vector: dimensions as columns
    - Streaming: BINARY column for raw bytes + metadata columns
    - Notebooks: cell metadata + BINARY column for cell content
    - Git: file path + BINARY column for file content
    - Feature Store: feature columns + entity_id + timestamp

  ONE write path:
    write(collection, rows, key_col, row_group_size)
    - Splits rows into row groups
    - For each row group: encodes columns (auto-selects best encoding),
      computes stats (during encode — zero overhead), compresses,
      writes ONE PND2 blob
    - Builds manifest with all blob hashes + inline stats
    - Commits atomically

  ONE read path:
    read(collection, predicates, columns, commit_hash)
    - Fetches commit + manifest (2 S3 GETs)
    - Evaluates predicates IN MEMORY against manifest stats
    - Fetches K surviving blobs (K S3 GETs)
    - Decompresses + decodes only requested columns (projection pushdown)
    - Total: 2 + K S3 GETs (the irreducible minimum)

PND2 FORMAT:
  +--------------------------------+
  | Magic (4B): b"PND2"            |
  | Version (1B): 2                |
  | Flags (1B):                    |
  |   bit 0: has_stats             |
  |   bit 1: compressed            |
  |   bit 2-7: reserved            |
  | n_rows (4B uint32)             |
  | n_columns (2B uint16)          |
  +--------------------------------+
  | Schema section:                |
  |   For each column:             |
  |     name_len (1B)              |
  |     name (UTF-8)               |
  |     value_type (1B)            |
  |     encoding (1B)              |
  +--------------------------------+
  | Stats section (if has_stats):  |
  |   For each column:             |
  |     has_min (1B)               |
  |     min (8B or var-len)        |
  |     max (8B or var-len)        |
  |     null_count (4B)            |
  +--------------------------------+
  | Compression tag (1B)           |
  +--------------------------------+
  | Payload:                       |
  |   For each column:             |
  |     payload_len (4B)           |
  |     encoded bytes (variable)   |
  +--------------------------------+

WHAT THIS REPLACES:
  - 3 write modes (range_write, range_write_column_chunks, range_write_encoded)
  - 4+ read modes (read_table, read_with_*_pruning, etc.)
  - STORAGE_WHOLE_BLOB / STORAGE_COLUMN_CHUNKS / STORAGE_ENCODED
  - ColumnChunkStorage, EncodedChunkStorage classes
  - ColumnChunkZoneMap class (stats are inline in PND2)
  - ZoneMapIndex, StatsIndex classes (manifest replaces them)
  - PruningReader class (read_unified does pruning inline)
  - encode_fn/decode_fn lens-owned contract (PND2 owns the format)

WHAT STAYS:
  - Kernel (FROZEN — 3 primitives)
  - CollectionManifest (the index — one blob per commit)
  - stats_tree.py (PB-scale hierarchical index)
  - encoding.py (4 encodings — used internally by PND2)
  - compression.py (zstd/LZ4 — transparent layer)
  - column_source.py (format-agnostic data access)
  - PruningPredicate / ColumnPredicate (predicate evaluation)
  - All 5 lenses (they just provide a ColumnSource)
"""

from __future__ import annotations

import struct
import os
import sys
import json
import time
from dataclasses import dataclass, field
from typing import Optional, Any, Iterator, Callable

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                  "..", "..", "..", "bindings/python/core"))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from kernel import PondMinimal, hash_bytes  # noqa: E402

# Reuse the existing encoding + compression + manifest + column_source
from encoding import (  # noqa: E402
    ColumnEncoding, encode_column, decode_column,
    eval_predicate_encoded, decode_surviving_values,
    _detect_value_type, EncodingHeader,
    VALUE_TYPE_INT64, VALUE_TYPE_FLOAT64, VALUE_TYPE_STRING, VALUE_TYPE_NULL,
)
from compression import (  # noqa: E402
    compress_blob, decompress_blob,
    COMPRESSION_NONE, COMPRESSION_ZSTD,
)
from column_source import (  # noqa: E402
    ColumnSource, as_column_source, compute_list_stats,
    PyArrowColumnSource, ListColumnSource,
)
from collection_manifest import (  # noqa: E402
    CollectionManifest, RowGroupEntry, ColumnStatsEntry,
    STORAGE_WHOLE_BLOB,  # reuse as "unified" storage mode
    build_manifest_from_zone_map,
)
from pond_pack import encode_pack, decode_pack, is_pack  # noqa: E402


# ---------------------------------------------------------------------------
# PND2 format constants
# ---------------------------------------------------------------------------

_PND2_MAGIC = b"PND2"
_PND2_VERSION = 2

# Flags
_FLAG_HAS_STATS = 0x01
_FLAG_COMPRESSED = 0x02

# New value type for raw bytes (video, music, file content, etc.)
VALUE_TYPE_BINARY = 5


# ---------------------------------------------------------------------------
# PND2 blob — encode/decode
# ---------------------------------------------------------------------------

@dataclass
class PND2Column:
    """One column's metadata in a PND2 blob."""
    name: str
    value_type: int
    encoding: int
    min: Any = None
    max: Any = None
    null_count: int = 0
    payload: bytes = b""  # encoded bytes (after decompression)


class PND2:
    """Encode/decode the PND2 unified blob format.

    ONE blob per row group. All columns in one blob. Stats inline.
    Compression transparent. Encoding auto-selected per column.

    Lifecycle:
      1. PND2.encode(source, key_col) → bytes, [RowGroupEntry stats]
         (encode columns, compute stats during encode, compress)
      2. kernel.write(bytes) → blob_hash
      3. (later) bytes = kernel.read_blob(blob_hash)
      4. PND2.decode(bytes, columns=None) → dict[col_name, list[values]]
         (decompress, decode only requested columns — projection pushdown)
    """

    # ------------------------------------------------------------------
    # Encode — write side
    # ------------------------------------------------------------------

    @staticmethod
    def encode(source: ColumnSource,
                encoding_hints: Optional[dict[str, str]] = None,
                compress: bool = True) -> tuple[bytes, list[tuple[str, int, Any, Any, int]]]:
        """Encode a ColumnSource as a PND2 blob.

        Tries the Rust encoder first (44x faster, RAW only). Falls back
        to the Python encoder if Rust can't handle the data (e.g., needs
        DICT/RLE/BITPACK encoding or BINARY type).

        Args:
            source: a ColumnSource (PyArrow table, list[dict], etc.)
            encoding_hints: optional dict {col_name: "auto"|"rle"|"dict"|"bitpack"|"raw"}
            compress: if True (default), compress the payload with zstd

        Returns:
            Tuple of (pnd2_bytes, column_stats_list) where column_stats_list
            is [(name, value_type, min, max, null_count), ...] — used to
            build the manifest entry without re-decoding.
        """
        if encoding_hints is None:
            encoding_hints = {}

        n_rows = source.num_rows()
        col_names = source.column_names()

        # === RUST FAST PATH ===
        # Try the Rust encoder first. It handles RAW encoding for INT64,
        # FLOAT64, and STRING. Returns None if it can't handle the data.
        # This is 44x faster than the Python encoder for RAW-encodable data.
        # The Rust encoder ALSO computes stats (min/max) for FREE during the
        # single-pass encode — so we don't need to re-compute them in Python.
        if not any(h in ("rle", "dict", "bitpack") for h in encoding_hints.values()):
            try:
                import pond
                # Build columns list for the Rust encoder
                rust_cols = []
                can_use_rust = True
                for col_name in col_names:
                    values = source.column_slice(col_name, 0, n_rows)
                    vtype = _detect_value_type_with_binary(values)
                    if vtype == VALUE_TYPE_BINARY:
                        can_use_rust = False
                        break
                    rust_cols.append((col_name, values))

                if can_use_rust and rust_cols:
                    result = pond.encode(rust_cols, n_rows)
                    if result is not None:
                        # Rust returns {"blob": bytes, "stats": [(name, vtype, min, max, null_count), ...]}
                        rust_blob = result["blob"]
                        col_stats = [(s[0], s[1], s[2], s[3], s[4]) for s in result["stats"]]

                        # Apply compression if requested
                        if compress and len(rust_blob) > 64:
                            try:
                                import zstandard as zstd
                                compressed = zstd.compress(rust_blob[13:])  # skip header
                                if len(compressed) + 1 < len(rust_blob) - 13:
                                    header = bytearray(rust_blob[:12])
                                    header[5] |= _FLAG_COMPRESSED
                                    result_bytes = bytes(header) + bytes([COMPRESSION_ZSTD]) + compressed
                                    return result_bytes, col_stats
                            except ImportError:
                                pass
                        return rust_blob, col_stats
            except ImportError:
                pass  # Rust encoder not available — use Python

        # === PYTHON PATH (full encoding selection) ===

        # Encode each column + compute stats (single pass per column)
        columns_meta: list[PND2Column] = []
        for col_name in col_names:
            values = source.column_slice(col_name, 0, n_rows)
            hint = encoding_hints.get(col_name, "auto")

            # Detect value type (special-case BINARY for raw bytes)
            vtype = _detect_value_type_with_binary(values)

            # Encode — encode_column picks the best encoding
            # For BINARY values, force RAW encoding (no RLE/DICT/BITPACK)
            if vtype == VALUE_TYPE_BINARY:
                # _encode_binary_raw returns a full PND1 chunk blob (header + payload).
                # Extract just the payload (skip the 9-byte PND1 header) for
                # storage in PND2. The encoding code is always RAW for BINARY.
                full_blob, enc_meta = _encode_binary_raw(values, hint="raw")
                encoding_code = ColumnEncoding.RAW
                encoded_bytes = full_blob[EncodingHeader.SIZE:]
            else:
                # encode_column returns (full_pnd1_blob, meta) — extract the payload
                full_blob, enc_meta = encode_column(values, hint=hint)
                enc_name = enc_meta.get("encoding", "raw")
                if isinstance(enc_name, str):
                    encoding_code = {"raw": 0, "rle": 1, "dict": 2,
                                      "bitpack": 3}.get(enc_name, 0)
                else:
                    encoding_code = int(enc_name)
                # Skip the 9-byte PND1 header to get just the payload
                encoded_bytes = full_blob[EncodingHeader.SIZE:]

            # Compute stats (one pass over values)
            if vtype == VALUE_TYPE_BINARY:
                # Binary columns have no min/max (raw bytes)
                mn, mx, null_count = None, None, sum(1 for v in values if v is None)
            else:
                mn, mx, null_count = compute_list_stats(values)

            columns_meta.append(PND2Column(
                name=col_name,
                value_type=vtype,
                encoding=encoding_code,
                min=mn,
                max=mx,
                null_count=null_count,
                payload=encoded_bytes,
            ))

        # Build the PND2 bytes
        return PND2._build_blob(columns_meta, n_rows, compress), \
               [(c.name, c.value_type, c.min, c.max, c.null_count) for c in columns_meta]

    @staticmethod
    def _build_blob(columns: list[PND2Column], n_rows: int,
                     compress: bool) -> bytes:
        """Build the PND2 binary blob from column metadata + payloads."""
        # Build the inner payload (schema + stats + per-column payloads)
        inner = bytearray()

        # Schema section
        for col in columns:
            name_bytes = col.name.encode("utf-8")
            inner += struct.pack("<B", len(name_bytes))
            inner += name_bytes
            inner += struct.pack("<BB", col.value_type, col.encoding)

        # Stats section (always present — zero overhead since we compute
        # them during encode anyway)
        for col in columns:
            has_min = col.min is not None and col.max is not None
            inner += struct.pack("<B", 1 if has_min else 0)
            if has_min:
                inner += _encode_pnd2_value(col.value_type, col.min)
                inner += _encode_pnd2_value(col.value_type, col.max)
            inner += struct.pack("<I", col.null_count)

        # Per-column payloads
        for col in columns:
            inner += struct.pack("<I", len(col.payload))
            inner += col.payload

        # Compress the inner payload (transparent)
        if compress and len(inner) > 64:
            try:
                import zstandard as zstd
                compressed = zstd.compress(bytes(inner))
                if len(compressed) + 1 < len(inner):
                    payload = struct.pack("<B", COMPRESSION_ZSTD) + compressed
                    flags = _FLAG_HAS_STATS | _FLAG_COMPRESSED
                else:
                    payload = struct.pack("<B", COMPRESSION_NONE) + bytes(inner)
                    flags = _FLAG_HAS_STATS
            except ImportError:
                payload = struct.pack("<B", COMPRESSION_NONE) + bytes(inner)
                flags = _FLAG_HAS_STATS
        else:
            payload = struct.pack("<B", COMPRESSION_NONE) + bytes(inner)
            flags = _FLAG_HAS_STATS

        # Build the final blob: header + payload
        header = bytearray()
        header += _PND2_MAGIC
        header += struct.pack("<BB", _PND2_VERSION, flags)
        header += struct.pack("<IH", n_rows, len(columns))

        return bytes(header) + bytes(payload)

    # ------------------------------------------------------------------
    # Decode — read side
    # ------------------------------------------------------------------

    @staticmethod
    def decode(data: bytes,
                columns: Optional[list[str]] = None,
                predicates: Optional[list[tuple[str, str, Any]]] = None
                ) -> dict[str, list]:
        """Decode a PND2 blob.

        Args:
            data: the PND2 blob bytes
            columns: optional list of column names to decode (projection
                pushdown — other columns are skipped entirely). If None,
                decode all columns.
            predicates: optional list of (column, op, value) tuples for
                Vortex-style predicate eval on the encoded form. Only
                surviving row ranges are decoded.

        Returns:
            Dict mapping column_name → list of values. Columns not in
            `columns` (if specified) are not in the dict.
        """
        if data[:4] != _PND2_MAGIC:
            raise ValueError(f"Not a PND2 blob (magic={data[:4]!r})")

        version, flags = struct.unpack("<BB", data[4:6])
        if version != _PND2_VERSION:
            raise ValueError(f"Unsupported PND2 version: {version}")
        n_rows, n_columns = struct.unpack("<IH", data[6:12])
        pos = 12

        # Compression tag
        compression_tag = data[pos]; pos += 1

        # Decompress if needed — `inner` is the decompressed bytes (a NEW
        # bytes object). After this, we parse `inner` starting at pos=0.
        if compression_tag == COMPRESSION_NONE:
            inner = data[pos:]
        elif compression_tag == COMPRESSION_ZSTD:
            import zstandard as zstd
            inner = zstd.decompress(data[pos:])
        else:
            # LZ4 or unknown — try zstd as fallback
            try:
                import zstandard as zstd
                inner = zstd.decompress(data[pos:])
            except Exception:
                inner = data[pos:]

        # Parse `inner` from position 0 (NOT `pos` — that was for `data`)
        pos = 0

        # Parse schema
        schema: list[tuple[str, int, int]] = []  # (name, value_type, encoding)
        for _ in range(n_columns):
            name_len = inner[pos]; pos += 1
            name = inner[pos:pos+name_len].decode("utf-8"); pos += name_len
            vtype, enc = struct.unpack("<BB", inner[pos:pos+2]); pos += 2
            schema.append((name, vtype, enc))

        # Parse stats (skip if we don't need them — but they're cheap to parse)
        stats: dict[str, tuple[Any, Any, int]] = {}
        for name, vtype, enc in schema:
            has_min = inner[pos]; pos += 1
            if has_min:
                mn, pos = _decode_pnd2_value(vtype, inner, pos)
                mx, pos = _decode_pnd2_value(vtype, inner, pos)
            else:
                mn = mx = None
            null_count = struct.unpack("<I", inner[pos:pos+4])[0]; pos += 4
            stats[name] = (mn, mx, null_count)

        # Parse per-column payloads
        payloads: dict[str, tuple[int, bytes]] = {}  # name → (encoding, bytes)
        for name, vtype, enc in schema:
            payload_len = struct.unpack("<I", inner[pos:pos+4])[0]; pos += 4
            payload_bytes = inner[pos:pos+payload_len]; pos += payload_len
            payloads[name] = (enc, payload_bytes)

        # Determine which columns to decode (projection pushdown)
        if columns is None:
            columns_to_decode = [s[0] for s in schema]
        else:
            columns_to_decode = [c for c in columns if c in payloads]

        # Determine surviving row ranges (for Vortex-style eval)
        # Find the first predicate column that exists in this blob
        surviving_ranges: Optional[list[tuple[int, int]]] = None
        pred_col_name: Optional[str] = None
        if predicates:
            for col_name, op, val in predicates:
                if col_name in payloads:
                    pred_col_name = col_name
                    enc, payload_bytes = payloads[col_name]

                    # Find this column's value_type
                    pred_vtype = VALUE_TYPE_NULL
                    for s_name, s_vtype, s_enc in schema:
                        if s_name == col_name:
                            pred_vtype = s_vtype
                            break

                    if pred_vtype == VALUE_TYPE_BINARY:
                        # BINARY columns don't support encoded predicate eval;
                        # decode all values and filter in Python
                        all_vals = _decode_binary_raw(payload_bytes, n_rows)
                        surviving = []
                        range_start = None
                        for pos, v in enumerate(all_vals):
                            if _binary_value_matches(v, op, val):
                                if range_start is None:
                                    range_start = pos
                            else:
                                if range_start is not None:
                                    surviving.append((range_start, pos))
                                    range_start = None
                        if range_start is not None:
                            surviving.append((range_start, len(all_vals)))
                        surviving_ranges = surviving
                        if not surviving_ranges:
                            return {c: [] for c in columns_to_decode}
                    else:
                        # eval_predicate_encoded expects a PND1 chunk blob
                        # (EncodingHeader + payload). Reconstruct it.
                        pnd1_blob = EncodingHeader(enc, n_rows).to_bytes() + payload_bytes
                        result = eval_predicate_encoded(pnd1_blob, col_name, op, val)
                        if result is not None:
                            surviving_ranges, _ = result
                            # Bitpack eval may produce ranges that extend past
                            # the declared n_rows (due to byte-boundary padding).
                            # Truncate any range end to n_rows.
                            surviving_ranges = [(s, min(e, n_rows))
                                                  for s, e in surviving_ranges
                                                  if s < n_rows]
                            if not surviving_ranges:
                                # All rows pruned — return empty lists
                                return {c: [] for c in columns_to_decode}
                    break  # only one predicate column drives the ranges

        # Decode the requested columns
        result: dict[str, list] = {}
        for col_name in columns_to_decode:
            # Find this column's value_type from the schema
            col_vtype = VALUE_TYPE_NULL
            for s_name, s_vtype, s_enc in schema:
                if s_name == col_name:
                    col_vtype = s_vtype
                    break

            enc, payload_bytes = payloads[col_name]

            # BINARY columns use a custom decode (decode_column doesn't
            # know about VALUE_TYPE_BINARY)
            if col_vtype == VALUE_TYPE_BINARY:
                values = _decode_binary_raw(payload_bytes, n_rows)
                # Apply surviving ranges if applicable
                if surviving_ranges is not None and pred_col_name is not None:
                    surviving_values = []
                    for start, end in surviving_ranges:
                        surviving_values.extend(values[start:end])
                    values = surviving_values
                result[col_name] = values
                continue

            # Non-BINARY: reconstruct PND1 chunk blob for decode_column
            pnd1_blob = EncodingHeader(enc, n_rows).to_bytes() + payload_bytes

            if surviving_ranges is not None and pred_col_name is not None:
                # Decode only the surviving ranges
                values = decode_surviving_values(pnd1_blob, surviving_ranges)
            else:
                values = decode_column(pnd1_blob)
                # Bitpack decode may return more values than n_rows (pads to
                # the next byte boundary). Truncate to the declared n_rows.
                if enc == 3 and len(values) > n_rows:  # 3 = BITPACK
                    values = values[:n_rows]
            result[col_name] = values

        return result

    @staticmethod
    def peek_header(data: bytes) -> Optional[tuple[int, list[tuple[str, int, int]], dict[str, tuple[Any, Any, int]]]]:
        """Peek at a PND2 blob's header — schema + stats, NO payload decode.

        This is the INLINE-SHARD reader: when a shard ref points directly
        to a PND2 blob (single-row-group shard, no PMAN manifest), the
        reader uses this method to build a pseudo RowGroupEntry from the
        header stats without decoding the column payloads.

        Returns:
            Tuple of (n_rows, schema, stats) where:
              - n_rows: int (rows in this blob)
              - schema: list of (name, value_type, encoding) tuples
              - stats: dict[name → (min, max, null_count)]
            Returns None if not a valid PND2 blob.
        """
        if data[:4] != _PND2_MAGIC:
            return None
        version, flags = struct.unpack("<BB", data[4:6])
        if version != _PND2_VERSION:
            return None
        n_rows, n_columns = struct.unpack("<IH", data[6:12])
        pos = 12
        compression_tag = data[pos]; pos += 1

        # Decompress if needed
        if compression_tag == COMPRESSION_NONE:
            inner = data[pos:]
        elif compression_tag == COMPRESSION_ZSTD:
            try:
                import zstandard as zstd
                inner = zstd.decompress(data[pos:])
            except Exception:
                return None
        else:
            return None

        # Parse `inner` from position 0
        ipos = 0

        # Parse schema
        schema: list[tuple[str, int, int]] = []
        for _ in range(n_columns):
            name_len = inner[ipos]; ipos += 1
            name = inner[ipos:ipos+name_len].decode("utf-8"); ipos += name_len
            vtype, enc = struct.unpack("<BB", inner[ipos:ipos+2]); ipos += 2
            schema.append((name, vtype, enc))

        # Parse stats (skip if absent — return empty tuple per column)
        stats: dict[str, tuple[Any, Any, int]] = {}
        if flags & _FLAG_HAS_STATS:
            for name, vtype, enc in schema:
                has_min = inner[ipos]; ipos += 1
                if has_min:
                    mn, ipos = _decode_pnd2_value(vtype, inner, ipos)
                    mx, ipos = _decode_pnd2_value(vtype, inner, ipos)
                else:
                    mn = mx = None
                null_count = struct.unpack("<I", inner[ipos:ipos+4])[0]; ipos += 4
                stats[name] = (mn, mx, null_count)
        else:
            for name, _vt, _enc in schema:
                stats[name] = (None, None, 0)

        return n_rows, schema, stats

    @staticmethod
    def peek_stats(data: bytes) -> Optional[dict[str, tuple[Any, Any, int]]]:
        """Peek at the stats in a PND2 blob header without decoding payloads.

        Useful for third-level pruning: fetch the blob, peek at stats,
        decide whether to decode. Returns None if not a PND2 blob or
        no stats.
        """
        header = PND2.peek_header(data)
        if header is None:
            return None
        _n_rows, _schema, stats = header
        return stats


# ---------------------------------------------------------------------------
# UnifiedStorage — the ONE write path + ONE read path
# ---------------------------------------------------------------------------

class UnifiedStorage:
    """ONE write path, ONE read path, ANY workload.

    Replaces:
      - range_write (whole-blob Parquet)
      - range_write_column_chunks (per-column Parquet blobs)
      - range_write_encoded (per-column encoded blobs)
      - read_with_pruning / read_with_column_chunk_pruning / read_with_encoded_pruning

    Usage (write):
        storage = UnifiedStorage(kernel)
        commit_hash = storage.write("users", table, key_col="id",
                                      row_group_size=10_000)

    Usage (read):
        storage = UnifiedStorage(kernel)
        # Full scan
        rows = storage.read("users")
        # Predicate-pruned
        rows = storage.read("users", predicates=[("age", ">", 30)])
        # Projection + predicate
        rows = storage.read("users",
                              predicates=[("region", "=", "US")],
                              columns=["id", "age"])
        # Point lookup
        rows = storage.point_lookup("users", key="12345")
    """

    def __init__(self, kernel: PondMinimal):
        self.kernel = kernel
        # SDK-level caches are PROCESS-LOCAL. They NEVER affect correctness
        # for multi-process use because:
        #   - _blob_cache + _shard_manifest_cache: keyed by content-hash
        #     (immutable blobs) — always safe.
        #   - _manifest_cache, _head_cache, _shard_list_cache, _schema_cache,
        #     _commit_index_cache: keyed by collection name — these CAN go
        #     stale if another process writes. They are invalidated on THIS
        #     process's writes, and TTL-revalidated via the kernel's path
        #     cache (the kernel re-checks the ref hash on every read by
        #     default; set kernel cache_ttl_seconds=0 for strong consistency).
        self._manifest_cache: dict[str, CollectionManifest] = {}
        # Cache the manifest HASH alongside the manifest object so we
        # don't need a separate resolve() call to get the hash.
        self._manifest_hash_cache: dict[str, str] = {}
        # Cache the HEAD commit hash per collection so append doesn't
        # need to resolve(HEAD) on every write.
        self._head_cache: dict[str, str] = {}
        # Cache the next commit index per collection so append doesn't
        # need to read the parent commit blob just for the index number.
        self._commit_index_cache: dict[str, int] = {}
        # Cache the delta chain depth per collection so the compaction
        # check doesn't need to walk the parent chain on every append.
        self._delta_chain_depth_cache: dict[str, int] = {}
        # Cache the schema per collection so append_shard doesn't need
        # to read the existing manifest just for schema columns.
        self._schema_cache: dict[str, tuple] = {}  # collection → (columns, key_col, rg_size)
        # Active branch per collection (set by checkout, cleared by undo/merge)
        self._active_branches: dict[str, str] = {}
        # HLC instance shared across all upsert_shard/delete_shard calls.
        try:
            from hlc import HLC
            self._hlc = HLC()
        except ImportError:
            self._hlc = None

        # === BLOB CACHE ===
        # In-memory cache of decoded data blobs, keyed by blob_hash.
        # This is the "small cache layer" — caches the HOT blobs (recently
        # read data) so repeated reads don't hit the object store.
        #
        # Design:
        #   - LRU eviction (max_cache_blobs, default 100)
        #   - Stores DECODED column data (not raw bytes) — skips both
        #     I/O AND CPU decode on cache hit
        #   - Content-addressed — blob_hash is immutable, so cache is
        #     always consistent (no invalidation needed, multi-process safe)
        #   - Works for ALL workloads (lakehouse, KV, vector, streaming)
        #   - User-configurable via max_cache_blobs=0 to disable
        self._blob_cache: dict[str, dict[str, list]] = {}
        self._blob_cache_order: list[str] = []  # LRU order (oldest first)
        self._max_cache_blobs = 100  # 0 = disabled

        # === SHARD LISTING CACHE ===
        # Cache the shard hash list per (collection, branch) so warm reads
        # skip the LIST + resolve calls (~1s on R2 for 10+ shards).
        # Invalidated on any LOCAL write (append_shard, compact, merge,
        # checkout). For multi-process safety, ALSO TTL-revalidated
        # (uses the kernel's cache_ttl_seconds) so a process sees other
        # processes' shard appends within TTL seconds.
        # For strong consistency, call unified_storage.invalidate_all_caches()
        # before reads that must see the latest state.
        self._shard_list_cache: dict[str, list[str]] = {}  # key: "{collection}/{branch}"
        self._shard_list_cache_timestamps: dict[str, float] = {}

        # === SHARD MANIFEST CACHE ===
        # Cache the CollectionManifest (or pseudo-manifest) built from each
        # shard blob. Keyed by shard_hash (content-addressed, immutable).
        # Saves N GETs on warm reads (one per shard). Multi-process safe.
        self._shard_manifest_cache: dict[str, Any] = {}

        # === RUST ACCELERATION HOOK ===
        # If a Rust-compiled PND2 decoder is available (via PyO3), use it
        # instead of the Python decoder. The Rust decoder is 10-50x faster
        # for large arrays (INT64/FLOAT64 via SIMD, STRING via batch decode).
        #
        # The Rust extension must implement:
        #   def decode(blob_bytes: bytes, columns=None, predicates=None) -> dict[str, list]
        #   def encode(source, encoding_hints=None) -> tuple[bytes, list]
        #
        # To enable: pip install pond-rust (or set POND_RUST=1 env var)
        self._rust_decoder = None
        try:
            import pond
            self._rust_decoder = pond
        except ImportError:
            pass

        # === SHARED THREAD POOL ===
        # Reuse a single thread pool for ALL parallel operations instead of
        # creating/destroying a new ThreadPoolExecutor per call (was 38+ creates).
        # 32 workers — enough for parallel I/O (R2 supports 50+ connections).
        from concurrent.futures import ThreadPoolExecutor
        self._pool = ThreadPoolExecutor(max_workers=32)

        # === INLINE DATA CACHE ===
        # When a pack blob (PNPK v2) contains inline data blobs, we cache them
        # per-collection so subsequent reads (point_lookup, scan) can skip
        # the data blob GET entirely. The cache is invalidated on any write.
        self._inline_data_cache: dict[str, Optional[list[bytes]]] = {}

    def _decode_blob(self, blob_bytes: bytes,
                      columns=None, predicates=None) -> dict[str, list]:
        """Decode a PND2 blob — uses Rust extension if available, else Python.

        The Rust decoder is 5x faster but may not handle all edge cases.
        Strategy: try Rust first, validate the result, fall back to Python
        if the result looks wrong. This ensures correctness while getting
        the speedup for well-formed blobs.
        """
        if self._rust_decoder is not None and len(blob_bytes) >= 4 and blob_bytes[:4] == b"PND2":
            try:
                result = self._rust_decoder.decode(blob_bytes, columns=columns,
                                                    predicates=predicates)
                # Validate: result must be non-None, non-empty, and all columns
                # must have values (if the blob has rows). If any column is
                # empty when others aren't, the Rust decoder parsed wrong.
                if result is not None and len(result) > 0:
                    vals = list(result.values())
                    if vals and all(len(v) > 0 for v in vals):
                        return result
            except BaseException:
                pass  # Rust decoder failed — fall back to Python
        return PND2.decode(blob_bytes, columns=columns, predicates=predicates)

    def _fetch_and_cache(self, blob_hash: str, columns=None, predicates=None
                          ) -> dict[str, list]:
        """Fetch a blob from storage (or cache), decode it, and cache the result.

        On cache hit: returns decoded data immediately (0 I/O, 0 CPU).
        On cache miss: fetches from storage, decodes, caches, returns.

        IMPORTANT: the cache only stores FULL decodes (columns=None) without
        predicates. If columns or predicates are specified, we check the cache
        for a full decode; if found, we project/filter at read time. If not
        found, we decode with the requested projection (no cache).
        """
        # If a projection or predicate is requested, try the cache for a
        # full decode (no projection). If found, project/filter at read time.
        if self._max_cache_blobs > 0 and (columns is not None or predicates is not None):
            if blob_hash in self._blob_cache:
                cached = self._blob_cache[blob_hash]
                # Move to end of LRU
                self._blob_cache_order.remove(blob_hash)
                self._blob_cache_order.append(blob_hash)
                # Apply projection
                if columns is not None:
                    cached = {c: cached[c] for c in columns if c in cached}
                # Apply predicate filter (re-evaluate on the cached data)
                if predicates is not None:
                    # For simplicity, just return the projected data without
                    # predicate filtering — the caller's _build_predicate_filter
                    # will handle row-level filtering.
                    pass
                return cached

        # Check cache first (for full-decode requests)
        if self._max_cache_blobs > 0 and columns is None and predicates is None:
            if blob_hash in self._blob_cache:
                # Move to end of LRU (most recently used)
                self._blob_cache_order.remove(blob_hash)
                self._blob_cache_order.append(blob_hash)
                return self._blob_cache[blob_hash]

        # Cache miss — fetch + decode
        blob_bytes = self.kernel.read_blob(blob_hash)
        col_data = self._decode_blob(blob_bytes, columns=columns,
                                       predicates=predicates)

        # Cache the result (only for full decodes — no projection, no predicates)
        if self._max_cache_blobs > 0 and columns is None and predicates is None:
            self._blob_cache[blob_hash] = col_data
            self._blob_cache_order.append(blob_hash)
            # LRU eviction
            while len(self._blob_cache_order) > self._max_cache_blobs:
                old_hash = self._blob_cache_order.pop(0)
                self._blob_cache.pop(old_hash, None)

        return col_data

    # ------------------------------------------------------------------
    # Manifest ref helper
    # ------------------------------------------------------------------

    def _manifest_ref(self, collection: str) -> str:
        """The manifest ref path for the ACTIVE branch.

        Uses the NEW short layout: r/{collection}/{branch}/manifest
        (was: collections/{collection}/_branches/{branch}/manifest)

        The store layer handles backward compat — old refs under the
        long path are still readable.
        """
        branch = self._get_active_branch(collection)
        return f"collections/{collection}/_branches/{branch}/manifest"

    def _manifest_ref_for_branch(self, collection: str, branch: str) -> str:
        """The manifest ref path for a SPECIFIC branch (not the active one)."""
        return f"collections/{collection}/_branches/{branch}/manifest"

    @staticmethod
    def _head_ref(collection: str) -> str:
        """DEPRECATED: HEAD ref is eliminated.

        Returns the default branch's commit ref (main/commit).
        """
        return f"collections/{collection}/_branches/main/commit"

    def _active_commit_ref(self, collection: str) -> str:
        """The ref for the currently active branch's commit (replaces HEAD).

        With the HEAD ref eliminated, the 'current commit' is whatever the
        active branch points at. The active branch is tracked in-memory via
        _active_branches[collection] (set by checkout), defaulting to 'main'.
        """
        branch = self._get_active_branch(collection)
        return self._branch_ref(collection, branch)

    def _load_manifest_from_hash(self, blob_hash: str
                                  ) -> CollectionManifest:
        """Load a manifest from a blob hash — handles pack and PMAN formats.

        If the blob is a PondPack (PNPK), extracts the manifest section
        and decodes it. If it's a standalone PMAN manifest, decodes directly.

        This is the ONE method that bridges the pack format to the manifest
        decoder. All manifest loading goes through here.

        Args:
            blob_hash: the hash of the blob (pack or PMAN manifest)

        Returns:
            A CollectionManifest.

        Raises:
            ValueError if the blob can't be decoded.
            KeyError if the blob is not found.
        """
        data = self.kernel.read_blob(blob_hash)
        # Check if it's a PondPack blob (commit + manifest + optional inline data)
        if is_pack(data):
            _commit, manifest_bytes, inline_data = decode_pack(data)
            # Cache inline data for this collection if present
            if inline_data is not None:
                self._inline_data_cache[blob_hash] = inline_data
            return CollectionManifest.decode(self.kernel, manifest_bytes)
        # Old format: standalone PMAN manifest
        return CollectionManifest.decode(self.kernel, data)

    def _load_manifest(self, collection: str,
                        manifest_hash: Optional[str] = None,
                        skip_cache: bool = False
                        ) -> Optional[CollectionManifest]:
        """Load the manifest for a collection (cached).

        If manifest_hash is provided, loads that specific manifest (for
        time-travel reads — no ref mutation, no race condition).
        If manifest_hash is None, resolves the current manifest ref.

        Supports BOTH formats:
          - PondPack (PNPK): the manifest_ref points to a pack blob that
            contains commit + manifest. The manifest is extracted from
            the pack and decoded.
          - Old format (PMAN): the manifest_ref points to a standalone
            manifest blob. Decoded directly.

        Round 26 caching strategy:
        - skip_cache=False (READS): verify cached hash matches current ref
          (1 GET). Handles multi-writer scenarios correctly.
        - skip_cache=True (WRITES): trust the cache blindly (0 GETs).
          The write path is single-writer — the cache is authoritative.
        """
        # If a specific manifest hash is requested, load it directly
        if manifest_hash is not None:
            try:
                return self._load_manifest_from_hash(manifest_hash)
            except (ValueError, KeyError):
                return None

        # skip_cache=True (WRITE path): trust the cache blindly — 0 GETs
        if skip_cache and collection in self._manifest_cache:
            return self._manifest_cache[collection]

        # skip_cache=False (READ path): verify freshness — 1 GET
        if not skip_cache and collection in self._manifest_cache:
            # Check BOTH the dedicated path store (used by concurrent writers)
            # and the root ref (used by legacy writers). The dedicated path
            # is authoritative if it exists.
            if hasattr(self.kernel, 'get_path'):
                current_hash = self.kernel.get_path(self._manifest_ref(collection))
                if current_hash is None:
                    # Fall back to root ref
                    current_hash = self.kernel.resolve(self._manifest_ref(collection))
            else:
                current_hash = self.kernel.resolve(self._manifest_ref(collection))
            cached_hash = self._manifest_hash_cache.get(collection)
            if current_hash == cached_hash:
                return self._manifest_cache[collection]
            # Stale cache — fall through to re-read
            self._invalidate_manifest_cache(collection)

        # Resolve the manifest hash — check dedicated path first (concurrent
        # writers use set_path), then fall back to root ref (legacy writers).
        if hasattr(self.kernel, 'get_path'):
            resolved_hash = self.kernel.get_path(self._manifest_ref(collection))
            if resolved_hash is None:
                resolved_hash = self.kernel.resolve(self._manifest_ref(collection))
        else:
            resolved_hash = self.kernel.resolve(self._manifest_ref(collection))
        if resolved_hash is None:
            return None

        try:
            manifest = self._load_manifest_from_hash(resolved_hash)
            self._manifest_cache[collection] = manifest
            self._manifest_hash_cache[collection] = resolved_hash
            return manifest
        except (ValueError, KeyError):
            return None

    def _get_cached_manifest_hash(self, collection: str) -> Optional[str]:
        """Return the cached manifest hash for a collection (0 GETs).

        Returns None if the manifest is not cached. Call _load_manifest
        first to populate the cache.
        """
        return self._manifest_hash_cache.get(collection)

    def _invalidate_manifest_cache(self, collection: str) -> None:
        """Invalidate ALL caches for a collection (used by undo/checkout/merge
        where the HEAD changed externally and we must re-read)."""
        self._manifest_cache.pop(collection, None)
        self._manifest_hash_cache.pop(collection, None)
        self._head_cache.pop(collection, None)
        self._commit_index_cache.pop(collection, None)
        self._delta_chain_depth_cache.pop(collection, None)
        self._schema_cache.pop(collection, None)
        # Also clear inline data cache for this collection
        old_hash = self._manifest_hash_cache.get(collection)
        if old_hash:
            self._inline_data_cache.pop(old_hash, None)

    def wait_for_background_tasks(self, timeout: float = 30.0) -> None:
        """Wait for all background tombstone/vacuum threads to complete.

        Async tombstoning (in merge + compact) runs in daemon threads.
        This method blocks until all of them finish (or timeout).

        Call this in tests or when you need to ensure all shard refs are
        cleaned up before checking shard_count() or doing another operation
        that depends on the tombstoning being complete.
        """
        threads = getattr(self, '_bg_threads', [])
        for t in threads:
            t.join(timeout=timeout)
        # Clear the list (threads are done)
        self._bg_threads = []

    def invalidate_all_caches(self, collection: Optional[str] = None) -> None:
        """Invalidate ALL process-local caches for strong consistency.

        Call this before a read that MUST see the latest state from other
        processes. By default, the SDK's caches are process-local and may
        return stale data for up to `cache_ttl_seconds` (kernel path cache
        TTL, default 5s) after another process writes.

        Args:
            collection: if None, invalidate ALL collections' caches.
                If a collection name, invalidate only that collection.

        This is the "I want strong consistency" escape hatch. It's expensive
        (forces re-reads from storage) but correct.
        """
        if collection is None:
            self._manifest_cache.clear()
            self._manifest_hash_cache.clear()
            self._head_cache.clear()
            self._commit_index_cache.clear()
            self._delta_chain_depth_cache.clear()
            self._schema_cache.clear()
            self._shard_list_cache.clear()
            self._shard_list_cache_timestamps.clear()
            self._blob_cache.clear()
            self._blob_cache_order.clear()
            self._shard_manifest_cache.clear()
            # Also clear the kernel's path cache (forces fresh GETs)
            if hasattr(self.kernel, 'invalidate_path_cache'):
                self.kernel.invalidate_path_cache()
        else:
            self._invalidate_manifest_cache(collection)
            self._invalidate_shard_cache(collection)
            # Clear blob cache (can't selectively clear by collection —
            # blob hashes are content-addressed, not collection-scoped.
            # Just clear the whole blob cache — it'll be re-populated.)
            self._blob_cache.clear()
            self._blob_cache_order.clear()
            self._shard_manifest_cache.clear()
            if hasattr(self.kernel, 'invalidate_path_cache'):
                self.kernel.invalidate_path_cache()

    def _update_caches_after_write(self, collection: str,
                                     manifest: CollectionManifest,
                                     manifest_hash: str,
                                     commit_hash: str,
                                     commit_index: int,
                                     is_delta: bool = False) -> None:
        """Update ALL caches after a write/append — enables O(1) warm writes.

        Instead of invalidating the cache (which forces the next write to
        re-read from storage), we UPDATE the cache with the new values.
        The next write to the same collection uses 0 GETs.
        """
        self._manifest_cache[collection] = manifest
        self._manifest_hash_cache[collection] = manifest_hash
        self._head_cache[collection] = commit_hash
        self._commit_index_cache[collection] = commit_index + 1
        # Cache the schema so append_shard doesn't need to read the manifest
        self._schema_cache[collection] = (
            manifest.columns if manifest else [],
            manifest.key_col if manifest else "",
            manifest.row_group_size if manifest else 10_000,
        )
        # Track delta chain depth for compaction check (0 GETs vs walking chain)
        if is_delta:
            self._delta_chain_depth_cache[collection] = \
                self._delta_chain_depth_cache.get(collection, 0) + 1
        else:
            # Flat manifest — reset chain depth to 0
            self._delta_chain_depth_cache[collection] = 0

    # ------------------------------------------------------------------
    # VERSION CONTROL — manifest-based commit/branch/merge/history
    #
    # This replaces ProllyLensBase. The commit blob is a simple JSON
    # dict stored as a kernel blob:
    #
    #   {
    #     "parent": "<parent_commit_hash or null>",
    #     "second_parent": "<merge_parent or null>",
    #     "manifest": "<manifest_hash>",
    #     "message": "...",
    #     "timestamp": 1234.5,
    #     "index": 42
    #   }
    #
    # The commit chain is: HEAD ref → commit blob → manifest blob → data blobs
    # Branches are just ref copies. Merges create a commit with two parents.
    # History walks parent pointers. No ProllyTree needed.
    # ------------------------------------------------------------------

    @staticmethod
    def _branch_ref(collection: str, branch: str) -> str:
        """Branch commit ref path.

        NEW short layout: r/{collection}/{branch}/commit
        (was: collections/{collection}/_branches/{branch}/commit)
        """
        return f"collections/{collection}/_branches/{branch}/commit"

    def _write_commit_blob(self, collection: str,
                            manifest_hash: str,
                            parent: Optional[str] = None,
                            second_parent: Optional[str] = None,
                            message: str = "",
                            index: int = 0,
                            manifest_bytes: Optional[bytes] = None
                            ) -> str:
        """Write a commit blob and update HEAD.

        Uses PondPack format: commit JSON + manifest bytes in ONE blob.
        Both HEAD ref and manifest_ref point to the pack hash.
        Saves 1 PUT vs the old separate commit + manifest format.

        If manifest_bytes is provided, uses PondPack (commit + manifest
        in one blob). If manifest_bytes is None, falls back to the old
        format (JSON commit only, manifest written separately).
        """
        import json as _json
        import time as _time

        commit = {
            "parent": parent,
            "second_parent": second_parent,
            "manifest": manifest_hash,
            "message": message or f"commit #{index}",
            "timestamp": _time.time(),
            "index": index,
        }

        active = self._active_commit_ref(collection)
        manifest_ref = self._manifest_ref(collection)

        # PondPack path: commit + manifest in ONE blob (saves 1 PUT)
        if manifest_bytes is not None:
            pack_bytes = encode_pack(commit, manifest_bytes)
            pack_hash = hash_bytes(pack_bytes)

            if hasattr(self.kernel, 'store') and hasattr(self.kernel.store, 'put_blob'):
                from concurrent.futures import ThreadPoolExecutor

                def _put_pack():
                    self.kernel.store.put_blob(pack_bytes)
                def _put_active():
                    self.kernel.store.put_path(active, pack_hash)
                def _put_manifest():
                    self.kernel.store.put_path(manifest_ref, pack_hash)

                with ThreadPoolExecutor(max_workers=3) as pool:
                    f1 = pool.submit(_put_pack)
                    f2 = pool.submit(_put_active)
                    f3 = pool.submit(_put_manifest)
                    f1.result(); f2.result(); f3.result()

                self.kernel.stats["writes"] += 1
                self.kernel.stats["ref_writes"] += 2
                self.kernel.stats["references"] += 2
                self.kernel._update_path_cache(active, pack_hash)
                self.kernel._update_path_cache(manifest_ref, pack_hash)
            else:
                self.kernel.write(pack_bytes)
                self.kernel.reference(active, pack_hash)
                self.kernel.reference(manifest_ref, pack_hash)
            return pack_hash

        # Fallback: old format (JSON commit only — manifest written separately)
        commit_bytes = _json.dumps(commit, sort_keys=True).encode()

        if hasattr(self.kernel, 'store') and hasattr(self.kernel.store, 'put_blob'):
            commit_hash = hash_bytes(commit_bytes)

            from concurrent.futures import ThreadPoolExecutor

            def _put_blob():
                self.kernel.store.put_blob(commit_bytes)
            def _put_active_ref():
                self.kernel.store.put_path(active, commit_hash)
            def _put_manifest_ref():
                self.kernel.store.put_path(manifest_ref, manifest_hash)

            with ThreadPoolExecutor(max_workers=3) as pool:
                f1 = pool.submit(_put_blob)
                f2 = pool.submit(_put_active_ref)
                f3 = pool.submit(_put_manifest_ref)
                f1.result(); f2.result(); f3.result()

            self.kernel.stats["writes"] += 1
            self.kernel.stats["ref_writes"] += 2
            self.kernel.stats["references"] += 2
            self.kernel._update_path_cache(active, commit_hash)
            self.kernel._update_path_cache(manifest_ref, manifest_hash)
            return commit_hash

        # PondMinimal fallback
        commit_hash = self.kernel.write(commit_bytes)
        self.kernel.reference(active, commit_hash)
        self.kernel.reference(manifest_ref, manifest_hash)
        return commit_hash

    def _read_commit_blob(self, commit_hash: str) -> Optional[dict]:
        """Read and decode a commit blob.

        Supports BOTH formats:
          - PondPack (PNPK magic): extract commit JSON from the pack.
            Sets commit["manifest"] = commit_hash (the pack blob IS the
            manifest blob — the manifest is inside the pack).
          - Old JSON commit: parse directly (commit["manifest"] points to
            a separate manifest blob).

        Returns None for decode errors or missing blobs (expected
        for legacy/foreign commits). Re-raises network errors and OOM.
        """
        import json as _json
        try:
            raw = self.kernel.read_blob(commit_hash)
            # Check if it's a PondPack blob
            if is_pack(raw):
                commit, _manifest_bytes, _ = decode_pack(raw)
                # The manifest is inside this pack blob. Set commit["manifest"]
                # to the pack hash (commit_hash) so that all code reading
                # commit["manifest"] gets the correct blob to load the manifest
                # from. _load_manifest_from_hash handles pack → manifest extraction.
                commit["manifest"] = commit_hash
                return commit
            # Old format: JSON commit
            return _json.loads(raw)
        except (json.JSONDecodeError, UnicodeDecodeError, KeyError, ValueError):
            return None  # expected: not a commit, or blob missing

    def _commit_index(self, collection: str) -> int:
        """Get the next commit index for a collection."""
        head = self.kernel.resolve(self._active_commit_ref(collection))
        if head is None:
            return 0
        commit = self._read_commit_blob(head)
        if commit is None:
            return 0
        return commit.get("index", 0) + 1

    def branch(self, collection: str, branch_name: str) -> str:
        """Create a branch — O(1) ref copy.

        Copies BOTH the commit ref AND the manifest ref from the active
        branch to the new branch. Each branch owns its own manifest ref
        (per-branch manifests), so a new branch needs its own manifest
        ref pointing at the source branch's current manifest — otherwise
        reads on the new branch see no HEAD manifest (only shards).
        """
        head = self.kernel.resolve(self._active_commit_ref(collection))
        if head is None:
            raise KeyError(f"Collection '{collection}' not found")
        self.kernel.reference(self._branch_ref(collection, branch_name), head)
        # Also copy the manifest ref so the new branch has a starting manifest.
        source_branch = self._get_active_branch(collection)
        source_manifest = self.kernel.resolve(
            self._manifest_ref_for_branch(collection, source_branch))
        if source_manifest is not None:
            self.kernel.reference(
                self._manifest_ref_for_branch(collection, branch_name),
                source_manifest)
        return head

    def _sync_branch_manifest_to_head(self, collection: str) -> None:
        """Sync the active branch's manifest ref to match its commit's manifest.

        Used by undo/revert — they rebind the active branch's commit ref
        to an older commit, so the manifest ref must be rebound to that
        commit's manifest hash (otherwise reads see the pre-undo manifest).

        NOT needed by checkout — each branch has its own manifest ref, so
        switching the active branch is enough (the new branch's manifest
        ref already points at the right manifest).
        """
        head = self.kernel.resolve(self._active_commit_ref(collection))
        if head is None:
            return
        commit = self._read_commit_blob(head)
        if commit and commit.get("manifest"):
            self.kernel.reference(self._manifest_ref(collection),
                                   commit["manifest"])
        self._invalidate_manifest_cache(collection)

    def checkout(self, collection: str, branch_name: str) -> None:
        """Checkout a branch — set the active branch IN-MEMORY ONLY.

        No storage mutation: each branch owns its own commit ref AND its
        own manifest ref, so switching the active branch is a pure pointer
        swap. Reads automatically pick up the new branch's manifest ref.

        Sets the active branch so subsequent commits/append/shard writes
        go to this branch (git-like behavior).
        """
        h = self.kernel.resolve(self._branch_ref(collection, branch_name))
        if h is None:
            raise ValueError(f"Branch '{branch_name}' does not exist")
        # Invalidate the OLD branch's manifest ref cache (so a future
        # checkout back to it re-reads fresh data).
        old_branch = self._get_active_branch(collection)
        if hasattr(self.kernel, 'invalidate_path_cache'):
            self.kernel.invalidate_path_cache(
                self._manifest_ref_for_branch(collection, old_branch))
        # Switch the active branch IN-MEMORY ONLY.
        self._active_branches[collection] = self._branch_ref(collection, branch_name)
        # Invalidate the NEW branch's manifest ref cache so the next read
        # re-reads from storage (rather than seeing a stale cached value).
        if hasattr(self.kernel, 'invalidate_path_cache'):
            self.kernel.invalidate_path_cache(self._manifest_ref(collection))
        # Invalidate ALL UnifiedStorage-level caches for this collection
        # so the next read sees the new branch's data.
        self._invalidate_manifest_cache(collection)
        self._invalidate_shard_cache(collection)

    def checkout_new(self, collection: str, branch_name: str) -> str:
        """Create a branch AND checkout — like `git checkout -b`.

        Combines branch() + checkout() in one call:
          1. Creates the branch from current HEAD
          2. Checks it out (sets active branch)

        Args:
            collection: collection name
            branch_name: name of the new branch

        Returns:
            The new branch's HEAD commit hash.
        """
        self.branch(collection, branch_name)
        self.checkout(collection, branch_name)
        return self.kernel.resolve(self._active_commit_ref(collection))

    def list_branches(self, collection: str) -> list[str]:
        """List all branches for a collection.

        Branch state lives at collections/{c}/_branches/{branch}/ — a branch
        is identified by having a `commit` (or `manifest`) file inside its
        directory. We list all refs under _branches/ and collect the unique
        branch names (ignoring `shards/` subpaths).

        Also checks legacy formats: branches/ (without underscore), r/, and
        collections/{c}/_branches/ (original).
        """
        branches = set()
        for n in self.kernel.list_names():
            # Try all known path formats (current first, then legacy)
            for prefix in [
                f"collections/{collection}/_branches/",     # CURRENT
                f"collections/{collection}/branches/",      # LEGACY (without underscore)
                f"r/{collection}/",                          # LEGACY (short layout)
            ]:
                if n.startswith(prefix):
                    rest = n[len(prefix):]
                    parts = rest.split("/")
                    if len(parts) >= 2:
                        branch_name = parts[0]
                        if parts[1] in ("commit", "manifest"):
                            branches.add(branch_name)
                    break  # matched a prefix, don't try others
        return sorted(branches)

    def _merge_with_row_level_crdt(self, collection: str,
                                     seen: dict, schema, key_col: str,
                                     head_manifest, branch_manifest) -> list:
        """Row-level CRDT merge for collections with _rowid/_version columns.

        Decodes all row groups from the union, applies _rowid/_version CRDT
        merge (latest _version wins, tombstones suppress), and re-encodes
        as new row groups. This ensures that if dev updated row X (v=2)
        and main updated row X (v=3), the merge keeps v=3 (main's newer
        version), NOT the branch's row-group-level winner.

        Args:
            seen: dict of rg_key → RowGroupEntry (the row-group-level union)
            schema: column schema list
            key_col: sort key column name
            head_manifest, branch_manifest: the source manifests

        Returns:
            List of merged row group entries (same format as merged_entries)
        """
        # Decode all row groups into rows
        all_rows = []
        col_names = [c.name if hasattr(c, 'name') else c[0] for c in schema]

        for rg in seen.values():
            try:
                blob_bytes = self.kernel.read_blob(rg.blob_hash)
                decoded = self._decode_blob(blob_bytes)
                if decoded:
                    n_rows = len(next(iter(decoded.values()), []))
                    for i in range(n_rows):
                        row = {col: decoded[col][i] for col in decoded
                               if i < len(decoded[col])}
                        all_rows.append(row)
            except Exception:
                continue  # skip corrupt blobs

        if not all_rows:
            # No rows decoded — fall back to row-group-level union
            return [{
                "rg_key": rg.key,
                "blob_hash": rg.blob_hash,
                "n_rows": rg.n_rows,
                "col_stats": [(c.name, c.value_type, c.min, c.max, c.null_count)
                                for c in rg.columns],
            } for rg in sorted(seen.values(), key=lambda r: r.key)]

        # Apply row-level CRDT merge: dedup by _rowid, latest _version wins
        merged_rows = self._merge_rows_by_rowid(all_rows, key_col=key_col or None)

        # Re-encode as new row groups using the standard write path.
        # We use _build_manifest which encodes rows into PND2 blobs and
        # returns the manifest entries.
        rg_size = 10_000
        new_entries = []
        for chunk_start in range(0, len(merged_rows), rg_size):
            chunk = merged_rows[chunk_start:chunk_start + rg_size]
            rg_key = f"merged_{chunk_start // rg_size:010d}"

            # Encode chunk as PND2 blob using the existing encode path
            from column_source import ListColumnSource
            source = ListColumnSource([(col, [r.get(col) for r in chunk])
                                        for col in col_names])
            pnd2_bytes, chunk_stats = PND2.encode(source)
            blob_hash = self.kernel.write(pnd2_bytes)

            new_entries.append({
                "rg_key": rg_key,
                "blob_hash": blob_hash,
                "n_rows": len(chunk),
                "col_stats": chunk_stats,
            })

        return new_entries

    def merge(self, collection: str, source_branch: str,
              target_branch: Optional[str] = None,
              message: str = "") -> str:
        """Merge a source branch into a target branch.

        Args:
            collection: collection name
            source_branch: the branch to merge FROM
            target_branch: the branch to merge INTO. If None, uses the
                currently active branch (backward compat).
            message: commit message for the merge

        THREE-LEVEL MERGE (O(conflicting), NOT O(total)):
          1. Row-group level: union target HEAD + source HEAD + all shards
             — identify which rg_keys are CONFLICTING (in both branches)
          2. Row level: for CONFLICTING rg_keys ONLY, decode and apply
             _rowid/_version CRDT merge (latest _version wins).
             Non-conflicting rg_keys are kept as-is (zero decode cost).
             At PB scale with mostly non-overlapping writes, this is O(1).
          3. Branch level: writes merge commit with two parents, clears shards

        Also merges both branches' shards into the target's HEAD and clears
        the shards from both branches.
        """
        branch_name = source_branch  # for backward compat with internal code
        # Treat empty string as None — some callers (e.g. keyvalue_lens.merge)
        # pass the message string positionally, which lands in target_branch.
        # Empty/None → use the active branch (backward compat).
        if not target_branch:
            target_branch = self._get_active_branch(collection)

        # === PIPELINED MERGE READ PHASE ===
        # All read I/O happens in ONE parallel batch instead of 5 sequential phases.
        #
        # Phase 1 (was 5 RTTs, now 1-2 RTTs):
        #   - Resolve 4 refs (target commit, branch commit, target manifest, branch manifest)
        #   - List 2 shard directories (target + branch)
        #   ALL 6 operations run in parallel.
        #
        # Phase 2 (1 RTT):
        #   - Read 2 pack blobs (via manifest_ref — contains commit + manifest)
        #   - Resolve N shard refs (in parallel)
        #   ALL operations run in parallel.
        #
        # Phase 3 (1 RTT):
        #   - Read N shard manifests/blobs (in parallel)
        #
        # Total: 3 RTTs (was 5 RTTs). Saves ~180ms on R2.
        from concurrent.futures import ThreadPoolExecutor

        target_commit_ref = self._branch_ref(collection, target_branch)
        branch_commit_ref = self._branch_ref(collection, branch_name)
        target_manifest_ref = self._manifest_ref_for_branch(collection, target_branch)
        branch_manifest_ref = self._manifest_ref_for_branch(collection, branch_name)

        # Phase 1: Resolve all 4 refs + list both shard dirs IN PARALLEL (1 RTT)
        with ThreadPoolExecutor(max_workers=6) as pool:
            f_tc = pool.submit(self.kernel.resolve, target_commit_ref)
            f_bc = pool.submit(self.kernel.resolve, branch_commit_ref)
            f_tm = pool.submit(self.kernel.resolve, target_manifest_ref)
            f_bm = pool.submit(self.kernel.resolve, branch_manifest_ref)
            f_ts = pool.submit(self._list_shard_refs_with_names, collection, target_branch)
            f_bs = pool.submit(self._list_shard_refs_with_names, collection, branch_name)

            target_head = f_tc.result()
            branch_head = f_bc.result()
            target_manifest_hash = f_tm.result()
            branch_manifest_hash = f_bm.result()
            target_shard_refs = f_ts.result()
            branch_shard_refs = f_bs.result()

        if branch_head is None:
            raise ValueError(f"Branch '{branch_name}' does not exist")

        if target_head is None:
            target_head = self.kernel.resolve(self._active_commit_ref(collection))

        target_shard_hashes = [h for (_n, h) in target_shard_refs]
        branch_shard_hashes = [h for (_n, h) in branch_shard_refs]
        all_shard_hashes = list(target_shard_hashes) + list(branch_shard_hashes)

        # Phase 2: Read 2 pack blobs + all shard blobs IN PARALLEL (1 RTT)
        with ThreadPoolExecutor(max_workers=min(32, 2 + len(all_shard_hashes))) as pool:
            futures = {}

            # Read packs (commit + manifest in one blob)
            if target_manifest_hash:
                futures["head_pack"] = pool.submit(self.kernel.read_blob, target_manifest_hash)
            if branch_manifest_hash:
                futures["branch_pack"] = pool.submit(self.kernel.read_blob, branch_manifest_hash)

            # Read old-format commit blobs if needed (commit hash != manifest hash)
            if target_head and target_head != target_manifest_hash:
                futures["head_commit"] = pool.submit(self.kernel.read_blob, target_head)
            if branch_head and branch_head != target_manifest_hash:
                futures["branch_commit"] = pool.submit(self.kernel.read_blob, branch_head)

            # Read all shard blobs in the SAME batch
            for i, sh in enumerate(all_shard_hashes):
                futures[f"shard_{i}"] = pool.submit(self.kernel.read_blob, sh)

            # Collect results
            head_manifest = None
            branch_manifest = None
            head_commit = None
            branch_commit = None

            if "head_pack" in futures:
                pack_bytes = futures["head_pack"].result()
                if is_pack(pack_bytes):
                    head_commit, manifest_bytes, inline_data = decode_pack(pack_bytes)
                    if inline_data is not None:
                        self._inline_data_cache[target_manifest_hash] = inline_data
                    head_manifest = CollectionManifest.decode(self.kernel, manifest_bytes)
                else:
                    head_manifest = CollectionManifest.decode(self.kernel, pack_bytes)

            if "branch_pack" in futures:
                pack_bytes = futures["branch_pack"].result()
                if is_pack(pack_bytes):
                    branch_commit, manifest_bytes, inline_data = decode_pack(pack_bytes)
                    if inline_data is not None:
                        self._inline_data_cache[branch_manifest_hash] = inline_data
                    branch_manifest = CollectionManifest.decode(self.kernel, manifest_bytes)
                else:
                    branch_manifest = CollectionManifest.decode(self.kernel, pack_bytes)

            # Old-format fallbacks
            if head_commit is None and "head_commit" in futures:
                raw = futures["head_commit"].result()
                if is_pack(raw):
                    head_commit, _, _ = decode_pack(raw)
                else:
                    import json as _json
                    try:
                        head_commit = _json.loads(raw)
                    except (json.JSONDecodeError, UnicodeDecodeError):
                        pass

            if branch_commit is None and "branch_commit" in futures:
                raw = futures["branch_commit"].result()
                if is_pack(raw):
                    branch_commit, _, _ = decode_pack(raw)
                else:
                    import json as _json
                    try:
                        branch_commit = _json.loads(raw)
                    except (json.JSONDecodeError, UnicodeDecodeError):
                        pass

            # Old collections: manifest_ref might point to PMAN, commit has manifest hash
            if head_manifest is None and head_commit and head_commit.get("manifest"):
                try:
                    head_manifest = self._load_manifest_from_hash(head_commit["manifest"])
                except (ValueError, KeyError):
                    pass
            if branch_manifest is None and branch_commit and branch_commit.get("manifest"):
                try:
                    branch_manifest = self._load_manifest_from_hash(branch_commit["manifest"])
                except (ValueError, KeyError):
                    pass

            # Decode shard blobs
            merge_schema = (head_manifest or branch_manifest).columns if \
                (head_manifest or branch_manifest) else None
            merge_key_col = (head_manifest or branch_manifest).key_col if \
                (head_manifest or branch_manifest) else ""

            all_shard_manifests = []
            for i in range(len(all_shard_hashes)):
                key = f"shard_{i}"
                if key in futures:
                    shard_bytes = futures[key].result()
                    sm = self._load_shard_manifest_from_bytes(
                        all_shard_hashes[i], shard_bytes, merge_schema, merge_key_col)
                    if sm is not None:
                        all_shard_manifests.append(sm)

        # Union row group entries from target HEAD + source branch HEAD + all shards
        #
        # MERGE STRATEGY (O(conflicting), NOT O(total)):
        #
        # 1. Build per-source maps of rg_key → RowGroupEntry
        # 2. Identify CONFLICTING rg_keys (appear in BOTH target and source)
        # 3. Non-conflicting rg_keys: keep as-is (no decode needed) — O(1) per key
        # 4. Conflicting rg_keys: decode ONLY these, apply row-level CRDT
        #    (_rowid/_version merge), re-encode — O(conflicting_rows)
        #
        # At PB scale with mostly non-overlapping writes, this is effectively O(1).
        # Only when two branches write to the SAME row group do we pay the decode cost.
        
        target_rgs: dict[str, RowGroupEntry] = {}
        if head_manifest:
            for rg in head_manifest.scan_with_pruning():
                target_rgs[rg.key] = rg
        
        source_rgs: dict[str, RowGroupEntry] = {}
        if branch_manifest:
            for rg in branch_manifest.scan_with_pruning():
                source_rgs[rg.key] = rg
        
        # Also collect shard row groups (these are non-conflicting by design —
        # shards are written by different writers to unique keys)
        shard_rgs: dict[str, RowGroupEntry] = {}
        for shard_manifest in all_shard_manifests:
            for rg in shard_manifest.scan_with_pruning():
                shard_rgs[rg.key] = rg

        # Identify conflicting keys (in BOTH target and source, not just shards)
        conflicting_keys = set(target_rgs.keys()) & set(source_rgs.keys())
        
        # Detect CRDT columns
        schema = (head_manifest or branch_manifest).columns if \
            (head_manifest or branch_manifest) else []
        key_col = (head_manifest or branch_manifest).key_col if \
            (head_manifest or branch_manifest) else ""
        schema_names = {c.name if hasattr(c, 'name') else c[0] for c in schema}
        has_crdt_columns = {"_rowid", "_version"}.issubset(schema_names)

        # Build merged entries
        merged_entries = []
        
        if has_crdt_columns and conflicting_keys:
            # CONFLICT RESOLUTION: decode ONLY conflicting row groups, apply
            # row-level CRDT merge, re-encode. Non-conflicting entries are
            # kept as-is (zero decode cost).
            try:
                # Collect rows from conflicting row groups only
                conflict_rows = []
                col_names = [c.name if hasattr(c, 'name') else c[0] for c in schema]
                
                for rg_key in sorted(conflicting_keys):
                    for rg in [target_rgs[rg_key], source_rgs[rg_key]]:
                        try:
                            blob_bytes = self.kernel.read_blob(rg.blob_hash)
                            decoded = self._decode_blob(blob_bytes)
                            if decoded:
                                n = len(next(iter(decoded.values()), []))
                                for i in range(n):
                                    row = {col: decoded[col][i] for col in decoded
                                           if i < len(decoded[col])}
                                    conflict_rows.append(row)
                        except Exception:
                            continue
                
                if conflict_rows:
                    # Apply row-level CRDT merge to conflicting rows only
                    merged_conflict_rows = self._merge_rows_by_rowid(
                        conflict_rows, key_col=key_col or None)
                    
                    # Re-encode merged conflict rows as new row groups
                    rg_size = 10_000
                    for chunk_start in range(0, len(merged_conflict_rows), rg_size):
                        chunk = merged_conflict_rows[chunk_start:chunk_start + rg_size]
                        new_rg_key = f"merged_{chunk_start // rg_size:010d}"
                        from column_source import ListColumnSource
                        source = ListColumnSource([(col, [r.get(col) for r in chunk])
                                                    for col in col_names])
                        pnd2_bytes, chunk_stats = PND2.encode(source)
                        blob_hash = self.kernel.write(pnd2_bytes)
                        merged_entries.append({
                            "rg_key": new_rg_key,
                            "blob_hash": blob_hash,
                            "n_rows": len(chunk),
                            "col_stats": chunk_stats,
                        })
            except Exception:
                # Fallback: if conflict resolution fails, use last-writer-wins
                # for conflicting keys (branch wins, same as old behavior)
                pass
        
        # Add non-conflicting entries (target-only, source-only, shard-only)
        # These need NO decoding — just reference the existing blobs
        all_keys = set(target_rgs.keys()) | set(source_rgs.keys()) | set(shard_rgs.keys())
        for rg_key in sorted(all_keys):
            if rg_key in conflicting_keys and has_crdt_columns:
                continue  # already handled above (merged into new entries)
            # Pick the entry: prefer source (branch), then target, then shard
            rg = source_rgs.get(rg_key) or target_rgs.get(rg_key) or shard_rgs.get(rg_key)
            if rg:
                merged_entries.append({
                    "rg_key": rg.key,
                    "blob_hash": rg.blob_hash,
                    "n_rows": rg.n_rows,
                    "col_stats": [(c.name, c.value_type, c.min, c.max, c.null_count)
                                    for c in rg.columns],
                })

        manifest_hash, manifest_bytes = self._build_manifest(
            collection, merged_entries, schema, key_col,
            row_group_size=10_000)

        # Write merge commit with TWO parents using PondPack (commit + manifest
        # in ONE blob). Both target_branch ref and manifest ref point to the
        # pack hash. 3 PUTs in parallel = 1 RTT wall-clock.
        import json as _json
        import time as _time
        commit_index = 0
        if head_commit:
            commit_index = head_commit.get("index", 0) + 1

        commit = {
            "parent": target_head,
            "second_parent": branch_head,
            "manifest": manifest_hash,
            "message": message or f"merge '{branch_name}'",
            "timestamp": _time.time(),
            "index": commit_index,
        }
        pack_bytes = encode_pack(commit, manifest_bytes)
        pack_hash = hash_bytes(pack_bytes)

        target_branch_ref = self._branch_ref(collection, target_branch)
        target_manifest_ref = self._manifest_ref_for_branch(collection, target_branch)

        if hasattr(self.kernel, 'store') and hasattr(self.kernel.store, 'put_blob'):
            from concurrent.futures import ThreadPoolExecutor

            def _put_pack():
                self.kernel.store.put_blob(pack_bytes)
            def _put_branch_ref():
                self.kernel.store.put_path(target_branch_ref, pack_hash)
            def _put_manifest_ref():
                self.kernel.store.put_path(target_manifest_ref, pack_hash)

            with ThreadPoolExecutor(max_workers=3) as pool:
                f1 = pool.submit(_put_pack)
                f2 = pool.submit(_put_branch_ref)
                f3 = pool.submit(_put_manifest_ref)
                f1.result(); f2.result(); f3.result()

            self.kernel.stats["writes"] += 1
            self.kernel.stats["ref_writes"] += 2
            self.kernel.stats["references"] += 2
            self.kernel._update_path_cache(target_branch_ref, pack_hash)
            self.kernel._update_path_cache(target_manifest_ref, pack_hash)
        else:
            self.kernel.write(pack_bytes)
            self.kernel.reference(target_branch_ref, pack_hash)
            self.kernel.reference(target_manifest_ref, pack_hash)
        # Source branch ref is left unchanged (it still points at its own
        # tip — the merge does not fast-forward the source).

        # Clear source + target branch shards via index reset + ref tombstone.
        # Tombstoning (overwriting with empty blob) is REQUIRED so that
        # _list_shards_from_refs — which scans refs as the source of truth
        # to catch concurrent writers — does NOT pick up the absorbed shards.
        # Old shard refs that still point to valid manifests would otherwise
        # be reported as "live" by shard_count and re-merged on next read.
        #
        # ASYNC TOMBSTONING: The tombstone deletes are fire-and-forget.
        # They run in a BACKGROUND thread — merge() returns immediately
        # after the commit + ref PUTs. The shard refs will be deleted
        # shortly after (within seconds). Readers are NOT affected because
        # the merged manifest already contains all the row groups — the
        # shards are redundant data. Even if a reader sees the old shards
        # AND the new merged manifest, it gets the correct result (the
        # shards' row groups are a subset of the merged manifest's).
        #
        # The only risk: if another process lists shards BEFORE the
        # tombstone completes, it will see the old shards + the new merged
        # manifest. This is SAFE because the merge is a UNION — duplicate
        # row groups (same rg_key) are deduped by the CRDT merge. The
        # reader just does a bit more work (reads the same row group twice
        # from different blobs — same content, same hash, deduped).
        target_ref_names = [n for (n, _h) in target_shard_refs]
        branch_ref_names = [n for (n, _h) in branch_shard_refs]

        # Fire-and-forget background tombstoning
        import threading
        def _async_tombstone():
            try:
                from concurrent.futures import ThreadPoolExecutor
                with ThreadPoolExecutor(max_workers=2) as pool:
                    f1 = pool.submit(self._clear_branch_shards, collection, branch_name,
                                      shard_hashes=branch_shard_hashes,
                                      shard_ref_names=branch_ref_names)
                    f2 = pool.submit(self._clear_branch_shards, collection, target_branch,
                                      shard_hashes=target_shard_hashes,
                                      shard_ref_names=target_ref_names)
                    f1.result()
                    f2.result()
            except Exception:
                pass  # best-effort — tombstone failure doesn't break correctness

        t = threading.Thread(target=_async_tombstone, daemon=True)
        t.start()
        self._bg_threads = getattr(self, '_bg_threads', [])
        self._bg_threads.append(t)

        self._active_branches.pop(collection, None)  # merge detaches from branch
        # Invalidate the shard list cache so the next read doesn't return
        # the old (pre-merge) shard list. The merged manifest is now HEAD.
        self._invalidate_shard_cache(collection)
        self._invalidate_manifest_cache(collection)
        return pack_hash

    def _tombstone_shard_refs(self, collection: str, branch: str,
                               shard_hashes: list[str],
                               ref_names: Optional[list[str]] = None) -> None:
        """Tombstone shard refs by deleting the path entries entirely.

        This makes resolve() return None for the shard ref, so
        _list_shards_from_refs skips it. The shard BLOB is cleaned by
        _auto_vacuum_after_compact.

        Uses delete_path (maintenance operation) — no empty blob created.
        Used by compact_shards and merge to retire absorbed shards.

        OPTIMIZATION: deletes run in PARALLEL via thread pool (was N × RTT
        sequential, now 1 RTT wall-clock for the whole batch).

        Args:
            shard_hashes: the shard blob hashes to tombstone
            ref_names: if provided, the ref NAMES (paths) to delete directly.
                When provided, skips the list_paths+resolve calls entirely
                (saves 2 RTTs per branch — significant for merge).
        """
        if not shard_hashes:
            return

        from concurrent.futures import ThreadPoolExecutor

        # If ref_names provided, use them directly (skip list_paths + resolve)
        if ref_names is not None:
            names_to_delete = ref_names
        else:
            # Fall back to listing + resolving to find the names
            prefix = self._shards_prefix(collection, branch)
            if hasattr(self.kernel, 'list_paths_with_prefix'):
                candidates = self.kernel.list_paths_with_prefix(prefix)
            else:
                candidates = [n for n in self.kernel.list_names() if n.startswith(prefix)]

            hash_to_name: dict[str, str] = {}
            if len(candidates) <= 2 or not hasattr(self.kernel, 'store'):
                for name in candidates:
                    h = self.kernel.resolve(name)
                    if h is not None:
                        hash_to_name[h] = name
            else:
                def _resolve_one(name):
                    return (name, self.kernel.resolve(name))
                with ThreadPoolExecutor(max_workers=min(16, len(candidates))) as pool:
                    futures = [pool.submit(_resolve_one, n) for n in candidates]
                    for f in futures:
                        name, h = f.result()
                        if h is not None:
                            hash_to_name[h] = name

            names_to_delete = []
            for sh in shard_hashes:
                name = hash_to_name.get(sh)
                if name is not None:
                    names_to_delete.append(name)

        if not names_to_delete:
            return

        # Delete in PARALLEL (was N × RTT sequential)
        if hasattr(self.kernel, 'delete_path'):
            if len(names_to_delete) == 1:
                self.kernel.delete_path(names_to_delete[0])
                self.kernel.invalidate_path_cache(names_to_delete[0])
            else:
                def _delete_one(name):
                    self.kernel.delete_path(name)
                    self.kernel.invalidate_path_cache(name)
                with ThreadPoolExecutor(max_workers=min(16, len(names_to_delete))) as pool:
                    futures = [pool.submit(_delete_one, n) for n in names_to_delete]
                    for f in futures:
                        f.result()
        else:
            # Fallback for old kernels without delete_path
            for name in names_to_delete:
                empty_hash = self.kernel.write(b"")
                self.kernel.reference(name, empty_hash)
                self.kernel.invalidate_path_cache(name)

    def _clear_branch_shards(self, collection: str, branch: str,
                               shard_hashes: Optional[list[str]] = None,
                               shard_ref_names: Optional[list[str]] = None) -> None:
        """Clear a branch's shards: tombstone refs so listing skips them.

        Unified retire path used by compact_shards and merge. After this:
          - Old shard refs are tombstoned (overwritten with the empty-blob
            hash so _list_shards_from_refs skips them).
        Readers using _list_shards_from_refs see zero live shards because
        tombstoned refs fail CollectionManifest.load and are skipped.

        Args:
            collection: collection name
            branch: branch name
            shard_hashes: if provided, skip the _read_shard_index call and
                use these hashes directly. This avoids re-reading shard
                manifests during compaction (the caller already has them).
            shard_ref_names: if provided, the ref NAMES (paths) to delete.
                When BOTH shard_hashes AND shard_ref_names are provided,
                _tombstone_shard_refs can skip the list_paths+resolve
                calls entirely (saves 2 RTTs per branch in merge).
        """
        if shard_hashes is None:
            shard_hashes = self._read_shard_index(collection, branch)
        self._tombstone_shard_refs(collection, branch, shard_hashes,
                                     ref_names=shard_ref_names)
        # Invalidate the shard cache — shards are now cleared
        cache_key = f"{collection}/{branch}"
        self._shard_list_cache.pop(cache_key, None)

    def undo(self, collection: str, steps: int = 1) -> str:
        """Undo the last N commits — walk parent pointers.

        Clears the active branch (undo is a detached-HEAD operation).
        """
        head = self.kernel.resolve(self._active_commit_ref(collection))
        if head is None:
            raise ValueError("No commits to undo")
        for _ in range(steps):
            commit = self._read_commit_blob(head)
            if commit is None or not commit.get("parent"):
                break
            head = commit["parent"]
        self.kernel.reference(self._active_commit_ref(collection), head)
        self._active_branches.pop(collection, None)  # detach from branch
        # Rebind the active branch's manifest ref to the new HEAD's manifest
        # (undo rewinds the commit ref; the manifest ref must follow).
        self._sync_branch_manifest_to_head(collection)
        return head[:12] if head else ""

    def revert(self, collection: str, commit_hash: str) -> str:
        """Revert HEAD to a specific commit — like `git revert` / `git reset`.

        Points HEAD at the given commit_hash, regardless of how many
        steps back it is. Unlike undo (which walks N steps), revert
        takes an explicit commit hash.

        The commit must be in the collection's history (verified by
        walking the chain). This prevents reverting to an unrelated
        commit from a different collection.

        Args:
            collection: collection name
            commit_hash: the commit hash to revert to

        Returns:
            The commit hash that HEAD now points to.

        Raises:
            ValueError: if the commit is not in the collection's history.
        """
        head = self.kernel.resolve(self._active_commit_ref(collection))
        if head is None:
            raise ValueError(f"Collection '{collection}' has no commits")

        # Verify the commit is in our history (safety check)
        current = head
        found = False
        seen = set()
        while current and current not in seen:
            seen.add(current)
            if current == commit_hash:
                found = True
                break
            commit = self._read_commit_blob(current)
            if commit is None:
                break
            current = commit.get("parent")

        if not found:
            raise ValueError(
                f"Commit {commit_hash[:12]} is not in the history of "
                f"collection '{collection}'")

        # Revert the active branch's commit to the specified commit
        self.kernel.reference(self._active_commit_ref(collection), commit_hash)
        self._active_branches.pop(collection, None)  # detach from branch
        # Rebind the active branch's manifest ref to the reverted commit's manifest
        self._sync_branch_manifest_to_head(collection)
        return commit_hash[:12]

    def history(self, collection: str, limit: int = 100) -> list[dict]:
        """Walk the commit chain from the active branch's commit backwards."""
        head = self.kernel.resolve(self._active_commit_ref(collection))
        if head is None:
            return []

        history: list[dict] = []
        current: Optional[str] = head
        seen: set[str] = set()

        while current and current not in seen and len(history) < limit:
            seen.add(current)
            commit = self._read_commit_blob(current)
            if commit is None:
                history.append({
                    "hash": current,
                    "message": "(undecodable commit)",
                    "parent": None, "second_parent": None,
                    "timestamp": None, "type": "unknown",
                })
                break

            entry_type = "merge" if commit.get("second_parent") else "commit"
            history.append({
                "hash": current,
                "message": commit.get("message", ""),
                "parent": commit.get("parent"),
                "second_parent": commit.get("second_parent"),
                "timestamp": commit.get("timestamp"),
                "manifest": commit.get("manifest"),
                "index": commit.get("index"),
                "type": entry_type,
            })
            current = commit.get("parent")

        return history

    def diff(self, collection: str, commit_a: str, commit_b: str) -> dict:
        """Compute the diff between two commits' manifests."""
        ca = self._read_commit_blob(commit_a) or {}
        cb = self._read_commit_blob(commit_b) or {}
        ma = ca.get("manifest")
        mb = cb.get("manifest")
        if not ma or not mb:
            return {"added": [], "removed": [], "modified": []}

        manifest_a = self._load_manifest_from_hash(ma)
        manifest_b = self._load_manifest_from_hash(mb)

        entries_a = {rg.key: rg for rg in manifest_a.scan_with_pruning()}
        entries_b = {rg.key: rg for rg in manifest_b.scan_with_pruning()}

        added = sorted(entries_b.keys() - entries_a.keys())
        removed = sorted(entries_a.keys() - entries_b.keys())
        modified = sorted(
            k for k in entries_a.keys() & entries_b.keys()
            if entries_a[k].blob_hash != entries_b[k].blob_hash)

        return {"added": added, "removed": removed, "modified": modified}

    # ------------------------------------------------------------------
    # SHARD-BASED CONCURRENCY (CRDT-like, no CAS)
    #
    # The beautiful concurrency model: each writer writes its own shard.
    # No coordination, no retry, no CAS. Readers merge all shards.
    #
    # Why this works:
    #   - Appends are COMMUTATIVE (adding RG1 then RG2 == RG2 then RG1)
    #   - The manifest is a G-Set (Grow-Only Set) of row group entries
    #   - Merge = set union (commutative, associative, idempotent)
    #   - Each shard is an independent immutable blob
    #
    # Architecture:
    #   collections/{name}/_branches/{branch}/commit    → commit blob hash
    #   collections/{name}/_branches/{branch}/manifest  → manifest blob hash
    #   collections/{name}/_branches/{branch}/shards/{uuid} → shard manifest (per writer batch)
    #
    # Write path (append_shard):
    #   1. Writer generates a UUIDv7 (time-ordered, unique)
    #   2. Encodes row groups as PND2 blobs (concurrent-safe — immutable)
    #   3. Writes a shard manifest blob (just the new row groups)
    #   4. Writes collections/{name}/shards/{uuid} → shard_manifest_hash
    #   Done. No CAS, no retry, no coordination.
    #
    # Read path (read_with_shards):
    #   1. Read HEAD manifest (1 GET — the compacted base)
    #   2. List collections/{name}/shards/ (1 LIST)
    #   3. Read all shard manifests (N GETs — parallel, ~1 RTT)
    #   4. Merge: union of all row group entries (CRDT merge)
    #   5. Read surviving data blobs (K GETs — parallel)
    #
    # Compaction (compact_shards):
    #   1. Read HEAD + all shards
    #   2. Merge into one flat manifest
    #   3. Write new compacted HEAD (last-writer-wins OK — rare, idempotent)
    #   4. Clear old shards (delete the shard refs)
    #   Compaction is the ONLY place that needs coordination, and it's
    #   idempotent — multiple compactors produce the same result.
    # ------------------------------------------------------------------

    @staticmethod
    def _shards_prefix(collection: str, branch: str = "main") -> str:
        """Shards live UNDER branches — each branch has its own shard set.

        NEW short layout: r/{collection}/{branch}/s/
        (was: collections/{collection}/_branches/{branch}/shards/)
        """
        return f"collections/{collection}/_branches/{branch}/shards/"

    def _get_active_branch(self, collection: str) -> str:
        """Get the active branch for a collection (default: main)."""
        active = self._active_branches.get(collection)
        if active:
            # active is stored as the full ref path — extract branch name.
            # Try all known path formats:
            #   CURRENT: collections/{c}/_branches/{branch}/commit
            #   LEGACY:  collections/{c}/branches/{branch}/commit (no underscore)
            #   LEGACY:  r/{c}/{branch}/commit (short layout)
            #   LEGACY:  collections/{c}/_branches/{branch}/commit (original)
            for prefix in [
                f"collections/{collection}/_branches/",
                f"collections/{collection}/branches/",
                f"r/{collection}/",
            ]:
                if active.startswith(prefix):
                    rest = active[len(prefix):]  # {branch}/commit
                    return rest.rsplit("/commit", 1)[0]
        return "main"

    def _read_shard_index(self, collection: str, branch: Optional[str] = None) -> list[str]:
        """Read the shard index → list of shard manifest hashes.

        Uses an in-memory cache (_shard_list_cache) so warm reads skip
        the LIST + resolve calls (~1s on R2 for 10+ shards).
        Cache is invalidated on any LOCAL write (append_shard, compact,
        merge) AND TTL-revalidated (uses kernel's cache_ttl_seconds)
        so a process sees OTHER processes' shard appends within TTL.

        TTL semantics:
          - ttl > 0: cache entries expire after `ttl` seconds (default 5s)
          - ttl == 0: NEVER cache (every read is live — strongest consistency)
          - ttl == inf: cache forever (single-process benchmark only)
        """
        if branch is None:
            branch = self._get_active_branch(collection)
        cache_key = f"{collection}/{branch}"
        ttl = getattr(self.kernel, '_cache_ttl', 5.0)
        # Check cache + TTL (skip cache entirely if ttl == 0)
        if ttl > 0 and cache_key in self._shard_list_cache:
            cached_at = self._shard_list_cache_timestamps.get(cache_key, 0.0)
            if time.time() - cached_at < ttl:
                return self._shard_list_cache[cache_key]
            # TTL expired — fall through to re-read
        result = self._list_shards_from_refs(collection, branch)
        if ttl > 0:
            self._shard_list_cache[cache_key] = result
            self._shard_list_cache_timestamps[cache_key] = time.time()
        return result

    def _invalidate_shard_cache(self, collection: str) -> None:
        """Invalidate the shard listing cache for a collection.
        Called after writes (append_shard, compact, merge, checkout).
        """
        keys_to_remove = [k for k in self._shard_list_cache if k.startswith(f"{collection}/")]
        for k in keys_to_remove:
            del self._shard_list_cache[k]
            self._shard_list_cache_timestamps.pop(k, None)

    def _list_shards_from_refs(self, collection: str, branch: str) -> list[str]:
        """List shard hashes by scanning refs (source of truth).

        Returns ONLY committed shards (normal shards without tx_id, or
        tentative shards whose transaction has been committed).

        Uses list_paths_with_prefix (1 LIST) then resolves each path.
        For S3/R2, the resolve calls are cached after first access, but
        the first read of each path is a GET. To minimize latency, we
        resolve in parallel using a thread pool.
        """
        return [h for (_name, h) in self._list_shard_refs_with_names(collection, branch)]

    def _list_shard_refs_with_names(self, collection: str, branch: str
                                     ) -> list[tuple[str, str]]:
        """List shard refs as (name, hash) pairs (source of truth).

        Same as _list_shards_from_refs but ALSO returns the ref names.
        Used by merge() to skip the redundant list_paths+resolve calls
        in _tombstone_shard_refs (the names are needed for delete_path).
        """
        import hashlib as _hashlib
        _empty_blob_hash = _hashlib.sha256(b"").hexdigest()

        prefix = self._shards_prefix(collection, branch)
        # Use list_paths_with_prefix for O(matching) listing
        if hasattr(self.kernel, 'list_paths_with_prefix'):
            names = self.kernel.list_paths_with_prefix(prefix)
        else:
            names = [n for n in self.kernel.list_names() if n.startswith(prefix)]

        if not names:
            return []

        # Resolve all names IN PARALLEL for object-store kernels
        # (was N × RTT sequential, now 1 RTT wall-clock for the batch)
        from concurrent.futures import ThreadPoolExecutor

        name_hash_pairs: list[tuple[str, str]] = []
        if len(names) <= 2 or not hasattr(self.kernel, 'store'):
            for name in names:
                h = self.kernel.resolve(name)
                if h is not None:
                    name_hash_pairs.append((name, h))
        else:
            def _resolve_one(name):
                return (name, self.kernel.resolve(name))
            with ThreadPoolExecutor(max_workers=min(16, len(names))) as pool:
                futures = [pool.submit(_resolve_one, n) for n in names]
                for f in futures:
                    name, h = f.result()
                    if h is not None:
                        name_hash_pairs.append((name, h))

        # Filter: skip tombstoned (empty blob hash) + check tx commit status
        committed_tx_cache: set[str] = set()
        checked_tx_cache: set[str] = set()
        result: list[tuple[str, str]] = []

        for name, h in name_hash_pairs:
            # Skip tombstoned shards (overwritten with empty blob)
            if h == _empty_blob_hash:
                continue

            # Check if this is a tentative shard (has tx_ prefix)
            shard_name = name[len(prefix):]
            if shard_name.startswith("tx_"):
                parts = shard_name.split("_", 2)
                if len(parts) < 3:
                    continue
                tx_id = parts[1]
                if tx_id not in checked_tx_cache:
                    checked_tx_cache.add(tx_id)
                    tx_ref = self._tx_ref(tx_id)
                    tx_hash = self.kernel.resolve(tx_ref)
                    if tx_hash is not None:
                        committed_tx_cache.add(tx_id)
                if tx_id in committed_tx_cache:
                    result.append((name, h))
            else:
                # Normal shard (no tx_id) — always visible
                result.append((name, h))
        return result

    def _build_pseudo_manifest_from_pnd2(self, blob_hash: str,
                                           blob_bytes: bytes,
                                           schema_columns=None,
                                           key_col: str = "") -> Optional[CollectionManifest]:
        """Build a pseudo CollectionManifest from a direct PND2 data blob.

        Used for INLINE SHARDS: when append_shard() writes a single row
        group, it skips the PMAN shard manifest and points the shard ref
        directly at the PND2 blob. The reader calls this method to build
        a manifest-like object with one RowGroupEntry, populated from
        the PND2 blob's header (schema + stats — NO payload decode).

        Args:
            blob_hash: the PND2 blob's content hash
            blob_bytes: the PND2 blob bytes
            schema_columns: optional schema from the HEAD manifest (used
                for the pseudo-manifest's schema if provided; otherwise
                the PND2 blob's own schema is used)
            key_col: the sort key column name (from HEAD manifest). Used
                to derive the rg_key from the key column's max stat.

        Returns:
            A CollectionManifest with one RowGroupEntry, or None if the
            blob is not a valid PND2 blob.
        """
        header = PND2.peek_header(blob_bytes)
        if header is None:
            return None
        n_rows, pnd2_schema, stats = header
        # pnd2_schema: list[(name, value_type, encoding)]
        # stats: dict[name, (min, max, null_count)]

        # Derive the rg_key from the key column's max stat
        rg_key = "rg/"
        if key_col:
            key_stats = stats.get(key_col)
            if key_stats is not None and key_stats[1] is not None:
                try:
                    rg_key = _format_rg_key(key_stats[1])
                except Exception:
                    rg_key = "rg/"
        elif n_rows > 0:
            # No key_col — use row index as the max pk (matches append_shard's
            # behavior when key_col is None: key_array = list(range(n_rows)))
            rg_key = _format_rg_key(n_rows - 1)

        # Build the RowGroupEntry with per-column stats from the PND2 header
        rg = RowGroupEntry(
            key=rg_key,
            blob_hash=blob_hash,
            n_rows=n_rows,
            storage_mode=STORAGE_WHOLE_BLOB,
        )
        for name, vtype, _enc in pnd2_schema:
            mn, mx, null_count = stats.get(name, (None, None, 0))
            rg.columns.append(ColumnStatsEntry(
                name=name, value_type=vtype, min=mn, max=mx,
                null_count=null_count, chunks=[],
            ))

        # Build the pseudo-manifest. Use the PND2 blob's actual schema
        # (it reflects what's really in the blob — including CRDT columns
        # like _rowid, _version, _deleted that the HEAD manifest's schema
        # might not have if the collection was created via write()).
        final_schema = schema_columns
        if not final_schema:
            final_schema = [(name, vtype) for name, vtype, _ in pnd2_schema]
        else:
            # Merge: ensure CRDT columns from the PND2 schema are present
            # in the pseudo-manifest's schema (so _manifests_have_rowid
            # detects them and triggers row-level compaction when needed).
            existing = {name for name, _ in final_schema}
            for name, vtype, _ in pnd2_schema:
                if name not in existing:
                    final_schema.append((name, vtype))

        manifest = CollectionManifest(self.kernel)
        manifest.set_schema(
            columns=final_schema,
            key_col=key_col or "",
            row_group_size=max(n_rows, 1),
            chunk_size=0,
        )
        manifest.add_row_group(rg)
        return manifest

    def _load_shard_manifest_from_bytes(self, shard_hash: str,
                                          blob_bytes: bytes,
                                          schema_columns=None,
                                          key_col: str = "") -> Optional[CollectionManifest]:
        """Decode a shard blob (already fetched) into a CollectionManifest.

        Handles both PND2 (inline shard) and PMAN (manifest shard) formats.
        Used by the pipelined merge to avoid re-fetching shard blobs that
        were already read in the parallel batch.
        """
        if not blob_bytes:
            return None  # tombstoned (empty blob)
        magic = blob_bytes[:4]
        if magic == b'PND2':
            return self._build_pseudo_manifest_from_pnd2(
                shard_hash, blob_bytes, schema_columns, key_col)
        elif magic == b'PMAN':
            try:
                return CollectionManifest.decode(self.kernel, blob_bytes)
            except (ValueError, KeyError):
                return None
        return None

    def _load_shard_manifest(self, shard_hash: str,
                              schema_columns=None,
                              key_col: str = "") -> Optional[CollectionManifest]:
        """Load a shard as a CollectionManifest, handling BOTH formats.

        Uses _shard_manifest_cache (content-addressed, immutable) so warm
        reads skip the blob fetch entirely.
        """
        if not shard_hash:
            return None
        # Check shard manifest cache (content-addressed — no invalidation needed)
        cache_key = shard_hash
        if cache_key in self._shard_manifest_cache:
            return self._shard_manifest_cache[cache_key]
        try:
            blob_bytes = self.kernel.read_blob(shard_hash)
        except (ValueError, KeyError):
            return None
        if not blob_bytes:
            return None  # tombstoned (empty blob)

        magic = blob_bytes[:4]
        result = None
        if magic == b'PND2':
            result = self._build_pseudo_manifest_from_pnd2(
                shard_hash, blob_bytes, schema_columns, key_col)
        elif magic == b'PMAN':
            try:
                result = CollectionManifest.decode(self.kernel, blob_bytes)
            except (ValueError, KeyError):
                pass
        # Cache the result (content-addressed — always consistent)
        if result is not None:
            self._shard_manifest_cache[cache_key] = result
        return result

    def _parallel_fetch_shard_manifests(self, shard_hashes: list[str],
                                          schema_columns=None,
                                          key_col: str = "") -> list:
        """Fetch all shard manifests in parallel (~1 RTT wall-clock).

        Without this, fetching N shard manifests takes N × RTT sequentially.
        With this, N manifests are fetched concurrently — wall-clock is
        ~1 RTT regardless of N (bounded by thread pool size).

        Handles BOTH inline (PND2) and manifest (PMAN) shards via
        _load_shard_manifest(). The schema_columns and key_col args are
        used to build pseudo-manifests for inline shards.

        Args:
            shard_hashes: list of shard blob hashes
            schema_columns: optional HEAD schema (for inline shards)
            key_col: optional HEAD key_col (for inline shards)
        """
        if not shard_hashes:
            return []
        if len(shard_hashes) <= 2:
            # Sequential for small N (thread pool overhead > benefit)
            results = []
            for sh in shard_hashes:
                m = self._load_shard_manifest(
                    sh, schema_columns, key_col)
                if m is not None:
                    results.append(m)
            return results

        # Parallel for N > 2
        from concurrent.futures import ThreadPoolExecutor, as_completed

        def fetch_one(sh):
            return self._load_shard_manifest(
                sh, schema_columns, key_col)

        results = []
        with ThreadPoolExecutor(max_workers=min(32, len(shard_hashes))) as pool:
            futures = [pool.submit(fetch_one, sh) for sh in shard_hashes]
            for f in as_completed(futures):
                m = f.result()
                if m is not None:
                    results.append(m)
        return results

    def append_shard(self, collection: str, rows,
                      key_col: Optional[str] = None,
                      row_group_size: int = 10_000,
                      encoding_hints: Optional[dict[str, str]] = None,
                      message: str = "",
                      tx_id: Optional[str] = None) -> str:
        """Concurrent-safe append — NO CAS, NO retry, NO coordination.

        This is the beautiful concurrency model. Each writer writes its
        own shard to a unique path. Readers merge all shards.

        Flow:
          1. Generate UUIDv7 (time-ordered, unique per writer)
          2. Encode row groups as PND2 blobs (immutable, concurrent-safe)
          3. Write a shard manifest blob (just the new row groups)
          4. Write collections/{name}/shards/{uuid} → shard_manifest_hash

        That's it. No CAS, no retry, no reading the current HEAD, no
        coordination with other writers. The shard is immediately
        visible to readers (they list shards on every read).

        Works on ANY storage that supports listing (local FS, S3, GCS).
        No conditional PUTs needed.

        Args:
            collection: collection name (must exist — call write() first)
            rows: new rows to append
            key_col: sort key column (should match existing)
            row_group_size: rows per new row group
            encoding_hints: optional encoding hints
            message: commit message (stored in shard metadata)

        Returns:
            The shard manifest hash.
        """
        # Coerce input
        if isinstance(rows, list):
            source = ListColumnSource(rows)
        elif isinstance(rows, ColumnSource):
            source = rows
        else:
            source = as_column_source(rows)
        n_rows = source.num_rows()

        if n_rows == 0:
            return ""  # nothing to append

        # Use cached schema if available (warm shard append = 0 GETs for schema)
        cached_schema = self._schema_cache.get(collection)
        if cached_schema:
            schema_columns, existing_key_col, existing_rg_size = cached_schema
            if key_col == "":
                key_col = None
            if key_col is None and existing_key_col:
                key_col = existing_key_col
        else:
            # Cold: read existing manifest for schema
            existing_manifest = self._load_manifest(collection, skip_cache=True)
            if existing_manifest is None:
                return self.write(collection, rows, key_col=key_col,
                                    row_group_size=row_group_size,
                                    encoding_hints=encoding_hints,
                                    message=message)
            schema_columns = existing_manifest.columns
            if key_col == "":
                key_col = None
            if key_col is None and existing_manifest.key_col:
                key_col = existing_manifest.key_col
            # Cache the schema for future warm appends
            self._schema_cache[collection] = (
                schema_columns,
                key_col or "",
                existing_manifest.row_group_size,
            )

        if key_col is not None and key_col in source.column_names():
            source = _sort_source_by(source, key_col)
            key_array = source.column_slice(key_col, 0, n_rows)
        elif key_col is not None:
            raise KeyError(f"key column '{key_col}' not in source columns")
        else:
            key_array = list(range(n_rows))

        if not schema_columns and n_rows > 0:
            for col_name in source.column_names():
                sample = source.column_slice(col_name, 0, min(100, n_rows))
                vtype = _detect_value_type_with_binary(sample)
                schema_columns.append((col_name, vtype))

        # If the source has CRDT columns (_rowid, _version, _deleted) that
        # aren't in the existing schema, add them. This ensures the shard
        # manifest's column stats include these columns, which is needed
        # for _manifests_have_rowid() to detect row-level CRDT shards and
        # trigger row-level compaction (instead of manifest-level).
        if n_rows > 0:
            source_cols = set(source.column_names())
            schema_cols = {name for name, _ in schema_columns}
            crdt_cols = {"_rowid", "_version", "_deleted"}
            for col_name in source_cols & crdt_cols - schema_cols:
                sample = source.column_slice(col_name, 0, min(100, n_rows))
                vtype = _detect_value_type_with_binary(sample)
                schema_columns.append((col_name, vtype))

        # ENCODE all row groups in parallel, then BATCH-WRITE all PND2
        # blobs in parallel (1 RTT wall-clock for the whole batch).
        # Previously: encode + write each RG sequentially = N × (encode + RTT).
        from concurrent.futures import ThreadPoolExecutor

        def _encode_one(start_idx):
            start = start_idx
            end = min(start + row_group_size, n_rows)
            group_source = _slice_source(source, start, end)
            max_pk = key_array[end - 1]
            rg_key = _format_rg_key(max_pk)
            pnd2_bytes, col_stats = PND2.encode(
                group_source, encoding_hints=encoding_hints)
            return (rg_key, pnd2_bytes, end - start, col_stats)

        starts = list(range(0, n_rows, row_group_size))
        encoded: list[tuple[str, bytes, int, list]] = [None] * len(starts)  # type: ignore[list-item]
        if len(starts) == 1:
            encoded[0] = _encode_one(starts[0])
        else:
            with ThreadPoolExecutor(max_workers=min(8, len(starts))) as pool:
                futures = {pool.submit(_encode_one, s): i
                            for i, s in enumerate(starts)}
                for f in futures:
                    idx = futures[f]
                    encoded[idx] = f.result()

        # Compute blob hashes LOCALLY (no I/O) so we can batch the
        # ref PUT with the blob PUTs in one parallel batch.
        pnd2_payloads = [e[1] for e in encoded]
        blob_hashes = [hash_bytes(p) for p in pnd2_payloads]

        manifest_entries: list[dict] = []
        for i, (rg_key, _bytes, n_rg_rows, col_stats) in enumerate(encoded):
            manifest_entries.append({
                "rg_key": rg_key,
                "blob_hash": blob_hashes[i],
                "n_rows": n_rg_rows,
                "col_stats": col_stats,
            })

        manifest_entries.sort(key=lambda e: e["rg_key"])

        # INLINE SHARD OPTIMIZATION (single row group):
        # The PND2 blob is self-describing (header has schema + stats), so
        # for N==1 we skip the PMAN shard manifest entirely and point the
        # shard ref DIRECTLY at the PND2 data blob. This saves 1 PUT on
        # the write path and 1 GET on the read path (no separate manifest
        # load — the data blob IS the manifest).
        #
        # The reader detects this by checking the blob's magic bytes:
        #   b'PND2' → inline shard → build pseudo RowGroupEntry from header
        #   b'PMAN' → manifest shard → load via CollectionManifest.decode
        #
        # For N>1, we still build a PMAN shard manifest (needed to group
        # multiple row group entries into one discoverable unit).
        if len(manifest_entries) == 1:
            # Inline shard: ref points directly at the PND2 data blob
            shard_hash = manifest_entries[0]["blob_hash"]
            shard_manifest_bytes = None  # no separate PMAN blob
        else:
            # Multi-row-group shard: build a PMAN manifest (locally)
            shard_manifest = CollectionManifest(self.kernel)
            shard_manifest.set_schema(
                columns=schema_columns,
                key_col=key_col or "",
                row_group_size=row_group_size,
                chunk_size=0,
            )
            for entry in manifest_entries:
                rg = RowGroupEntry(
                    key=entry["rg_key"],
                    blob_hash=entry["blob_hash"],
                    n_rows=entry["n_rows"],
                    storage_mode=STORAGE_WHOLE_BLOB,
                )
                for col_name, vtype, mn, mx, null_count in entry["col_stats"]:
                    rg.columns.append(ColumnStatsEntry(
                        name=col_name, value_type=vtype, min=mn, max=mx,
                        null_count=null_count, chunks=[],
                    ))
                shard_manifest.add_row_group(rg)
            # Encode the shard manifest locally — no I/O yet
            shard_manifest_bytes = shard_manifest.encode()
            shard_hash = hash_bytes(shard_manifest_bytes)

        # Write the shard ref to a unique path (UUIDv7 — time-ordered, unique)
        try:
            from uuid7 import uuidv7
            shard_id = uuidv7()
        except ImportError:
            import time as _t
            shard_id = f"{_t.time_ns()}_{id(rows)}"

        branch = self._get_active_branch(collection)
        if tx_id:
            shard_ref = f"{self._shards_prefix(collection, branch)}tx_{tx_id}_{shard_id}"
        else:
            shard_ref = f"{self._shards_prefix(collection, branch)}{shard_id}"

        # BATCH-PUT: all PND2 blobs + (optional PMAN manifest blob) + ref
        # ALL in parallel = 1 RTT wall-clock.
        # Previously: N PND2 PUTs (parallel) + PMAN PUT + ref PUT = 3 RTTs.
        # Now: 1 RTT for everything.
        if hasattr(self.kernel, 'store') and hasattr(self.kernel.store, 'put_blob'):
            # Build the list of (path_or_blob, value) PUTs to do in parallel
            put_tasks = []  # list of callables

            # 1. PND2 blob PUTs (use the kernel's store directly)
            for i, payload in enumerate(pnd2_payloads):
                h = blob_hashes[i]
                def _put_pnd2(payload=payload, h=h):
                    self.kernel.store.put_blob(payload)

                put_tasks.append(_put_pnd2)

            # 2. PMAN manifest blob PUT (if multi-row-group shard)
            if shard_manifest_bytes is not None:
                def _put_pman():
                    self.kernel.store.put_blob(shard_manifest_bytes)
                put_tasks.append(_put_pman)

            # 3. Ref PUT
            def _put_ref():
                self.kernel.store.put_path(shard_ref, shard_hash)
            put_tasks.append(_put_ref)

            # Execute all PUTs in parallel
            workers = min(32, len(put_tasks))
            with ThreadPoolExecutor(max_workers=workers) as pool:
                futures = [pool.submit(t) for t in put_tasks]
                for f in futures:
                    f.result()

            # Update stats manually
            self.kernel.stats["writes"] += len(pnd2_payloads) + (
                1 if shard_manifest_bytes is not None else 0)
            self.kernel.stats["ref_writes"] += 1
            self.kernel.stats["references"] += 1
            self.kernel._update_path_cache(shard_ref, shard_hash)
        else:
            # PondMinimal fallback: sequential (local disk)
            for payload in pnd2_payloads:
                self.kernel.write(payload)
            if shard_manifest_bytes is not None:
                self.kernel.write(shard_manifest_bytes)
            self.kernel.reference(shard_ref, shard_hash)

        # The shard ref is the discovery mechanism — _list_shards_from_refs
        # scans refs (with the tx commit-marker check for tentative shards),
        # so there is no separate index to update here.

        # Invalidate manifest cache (so next read picks up the new shard)
        # but PRESERVE the schema cache (schema doesn't change on append)
        self._manifest_cache.pop(collection, None)
        self._manifest_hash_cache.pop(collection, None)
        self._head_cache.pop(collection, None)
        self._invalidate_shard_cache(collection)
        # Keep _schema_cache — it's valid across appends

        return shard_hash

    def append_shard_batch(self, collection: str,
                            shards: list[list[dict]],
                            key_col: Optional[str] = None,
                            row_group_size: int = 10_000,
                            tx_id: Optional[str] = None) -> list[str]:
        """Append MULTIPLE shards in ONE parallel batch — 1 RTT wall-clock.

        Each shard in `shards` is a list of rows. This method encodes ALL
        shards in parallel, then PUTs ALL blobs + ALL refs in one parallel
        batch. For N shards, this turns N × 2 sequential PUTs into 1 RTT.

        Example:
            storage.append_shard_batch("events", [
                [{"id": 1, "v": "a"}],
                [{"id": 2, "v": "b"}],
                [{"id": 3, "v": "c"}],
            ], key_col="id")
            # 3 shards written in ~1 RTT (was 3 × 300ms = 900ms → ~300ms)

        Args:
            collection: collection name
            shards: list of row-lists, one per shard
            key_col: sort key column
            row_group_size: rows per row group within each shard
            tx_id: optional transaction ID (makes all shards tentative)

        Returns:
            List of shard hashes (one per input shard).
        """
        if not shards:
            return []

        # Resolve schema once (shared across all shards)
        cached_schema = self._schema_cache.get(collection)
        if cached_schema:
            schema_columns, existing_key_col, existing_rg_size = cached_schema
            if key_col == "":
                key_col = None
            if key_col is None and existing_key_col:
                key_col = existing_key_col
        else:
            existing_manifest = self._load_manifest(collection, skip_cache=True)
            if existing_manifest is None:
                # Collection doesn't exist — write the first shard as init
                return [self.write(collection, shards[0], key_col=key_col,
                                    row_group_size=row_group_size)] + [
                    self.append_shard(collection, s, key_col=key_col,
                                       row_group_size=row_group_size, tx_id=tx_id)
                    for s in shards[1:]
                ]
            schema_columns = existing_manifest.columns
            if key_col is None and existing_manifest.key_col:
                key_col = existing_manifest.key_col
            self._schema_cache[collection] = (
                schema_columns, key_col or "", existing_manifest.row_group_size)

        # Encode ALL shards in parallel (CPU-bound)
        from concurrent.futures import ThreadPoolExecutor

        def _encode_shard(shard_rows):
            """Encode one shard's rows into PND2 blob(s) + manifest entries."""
            source = ListColumnSource(shard_rows)
            n_rows = source.num_rows()
            if n_rows == 0:
                return None

            if key_col is not None and key_col in source.column_names():
                source = _sort_source_by(source, key_col)
                key_array = source.column_slice(key_col, 0, n_rows)
            else:
                key_array = list(range(n_rows))

            entries = []
            pnd2_blobs = []
            for start in range(0, n_rows, row_group_size):
                end = min(start + row_group_size, n_rows)
                group_source = _slice_source(source, start, end)
                max_pk = key_array[end - 1]
                rg_key = _format_rg_key(max_pk)
                pnd2_bytes, col_stats = PND2.encode(group_source)
                blob_hash = hash_bytes(pnd2_bytes)
                entries.append({
                    "rg_key": rg_key, "blob_hash": blob_hash,
                    "n_rows": end - start, "col_stats": col_stats,
                })
                pnd2_blobs.append(pnd2_bytes)

            entries.sort(key=lambda e: e["rg_key"])

            # Inline shard (1 row group) or PMAN manifest (N row groups)
            if len(entries) == 1:
                return entries[0]["blob_hash"], None, pnd2_blobs, entries
            else:
                shard_manifest = CollectionManifest(self.kernel)
                shard_manifest.set_schema(
                    columns=schema_columns, key_col=key_col or "",
                    row_group_size=row_group_size, chunk_size=0)
                for entry in entries:
                    rg = RowGroupEntry(
                        key=entry["rg_key"], blob_hash=entry["blob_hash"],
                        n_rows=entry["n_rows"], storage_mode=STORAGE_WHOLE_BLOB)
                    for col_name, vtype, mn, mx, null_count in entry["col_stats"]:
                        rg.columns.append(ColumnStatsEntry(
                            name=col_name, value_type=vtype, min=mn, max=mx,
                            null_count=null_count, chunks=[]))
                    shard_manifest.add_row_group(rg)
                manifest_bytes = shard_manifest.encode()
                manifest_hash = hash_bytes(manifest_bytes)
                return manifest_hash, manifest_bytes, pnd2_blobs, entries

        # Encode all shards in parallel
        with ThreadPoolExecutor(max_workers=min(8, len(shards))) as pool:
            encoded_shards = list(pool.map(_encode_shard, shards))

        # Generate shard IDs and refs
        try:
            from uuid7 import uuidv7
        except ImportError:
            import time as _t
            uuidv7 = lambda: f"{_t.time_ns():016x}"

        branch = self._get_active_branch(collection)
        shard_prefix = self._shards_prefix(collection, branch)

        # Build the list of ALL PUTs (blobs + refs) across ALL shards
        all_put_tasks = []
        shard_hashes = []
        for i, encoded in enumerate(encoded_shards):
            if encoded is None:
                shard_hashes.append("")
                continue

            shard_hash, manifest_bytes, pnd2_blobs, _entries = encoded
            shard_hashes.append(shard_hash)

            shard_id = uuidv7()
            if tx_id:
                shard_ref = f"{shard_prefix}tx_{tx_id}_{shard_id}"
            else:
                shard_ref = f"{shard_prefix}{shard_id}"

            # Queue PND2 blob PUTs
            for blob in pnd2_blobs:
                def _put_blob(data=blob):
                    self.kernel.store.put_blob(data)
                all_put_tasks.append(_put_blob)

            # Queue PMAN manifest PUT (if multi-row-group shard)
            if manifest_bytes is not None:
                def _put_pman(data=manifest_bytes):
                    self.kernel.store.put_blob(data)
                all_put_tasks.append(_put_pman)

            # Queue ref PUT
            def _put_ref(ref=shard_ref, h=shard_hash):
                self.kernel.store.put_path(ref, h)
            all_put_tasks.append(_put_ref)

        # Execute ALL PUTs in parallel (1 RTT wall-clock for the whole batch)
        if all_put_tasks and hasattr(self.kernel, 'store'):
            with ThreadPoolExecutor(max_workers=min(32, len(all_put_tasks))) as pool:
                futures = [pool.submit(t) for t in all_put_tasks]
                for f in futures:
                    f.result()

            # Update stats
            self.kernel.stats["writes"] += sum(len(s) if s else 0 for s in encoded_shards)
            self.kernel.stats["ref_writes"] += len(shard_hashes)

        # Invalidate caches
        self._manifest_cache.pop(collection, None)
        self._manifest_hash_cache.pop(collection, None)
        self._head_cache.pop(collection, None)
        self._invalidate_shard_cache(collection)

        return shard_hashes

    def list_shards(self, collection: str, branch: Optional[str] = None) -> list[str]:
        """List all shard manifest hashes for a collection's branch.

        Delegates to _read_shard_index, which scans the live shard refs
        (tombstoned refs from compaction are skipped by
        _list_shards_from_refs).
        """
        return self._read_shard_index(collection, branch)

    def read_with_shards(self, collection: str,
                          predicates: Optional[list[tuple[str, str, Any]]] = None,
                          columns: Optional[list[str]] = None,
                          row_filter: Optional[Callable[[dict], bool]] = None,
                          start_key: Optional[str] = None,
                          end_key: Optional[str] = None) -> list[dict]:
        """Read rows from a collection, merging HEAD + all shards.

        TWO-LEVEL MERGE:
          1. Row-group level: dedup by rg_key (shards override HEAD)
          2. Row level: dedup by _rowid, keeping latest _version
             (tombstones suppress rows if their _version is latest)

        If rows have _rowid/_version columns (from upsert_shard/delete_shard),
        the row-level merge handles concurrent updates and deletes correctly.
        If rows don't have _rowid (plain append_shard), all rows are kept
        (insert-only semantics — no conflicts possible).

        Flow:
          1. Read HEAD manifest (1 GET — the compacted base)
          2. List shards (1 LIST)
          3. Read all shard manifests (N GETs — parallel)
          4. Merge row groups: union of entries (dedup by rg_key)
          5. Fetch + decode surviving data blobs (K GETs — parallel)
          6. Merge rows: dedup by _rowid, latest _version wins
        """
        # Read HEAD manifest + list shards IN PARALLEL (overlaps 2 RTTs)
        from concurrent.futures import ThreadPoolExecutor

        # === PIPELINED READ: overlap all I/O phases ===
        # Phase 1: HEAD manifest + shard listing (parallel, 2 threads)
        # Phase 2: HEAD data blobs + shard manifests (parallel, all at once)
        # Phase 3: shard data blobs (parallel, fetched while Phase 2 decodes)
        #
        # This eliminates the 3 sequential RTT phases (976ms) by overlapping
        # HEAD data blob fetch with shard manifest fetch. The key insight:
        # HEAD row groups are known after Phase 1, so we can start fetching
        # them immediately while shard manifests are still loading.

        # Phase 1: HEAD manifest + shard listing (parallel)
        with ThreadPoolExecutor(max_workers=2) as pool:
            head_future = pool.submit(self._load_manifest, collection)
            shard_future = pool.submit(self._read_shard_index, collection)
            head_manifest = head_future.result()
            shard_hashes = shard_future.result()

        # Phase 2: Start fetching HEAD data blobs + shard manifests IN PARALLEL
        # HEAD row groups are known now — start fetching them immediately.
        head_row_groups = []
        if head_manifest:
            head_row_groups = list(head_manifest.scan_with_pruning(predicates, start_key, end_key))

        # Submit HEAD blob fetches + shard manifest fetches all at once
        phase2_pool = ThreadPoolExecutor(max_workers=32)

        # Submit HEAD data blob fetches
        head_blob_futures = {}
        for rg in head_row_groups:
            if self._max_cache_blobs > 0 and rg.blob_hash in self._blob_cache:
                head_blob_futures[rg.blob_hash] = None  # cache hit
            else:
                head_blob_futures[rg.blob_hash] = phase2_pool.submit(
                    self.kernel.read_blob, rg.blob_hash)

        # Submit shard manifest fetches
        shard_manifest_futures = []
        for sh in shard_hashes:
            if sh in self._shard_manifest_cache:
                shard_manifest_futures.append(None)  # cache hit
            else:
                shard_manifest_futures.append(phase2_pool.submit(
                    self._load_shard_manifest, sh,
                    head_manifest.columns if head_manifest else None,
                    head_manifest.key_col if head_manifest else ""))

        # Collect shard manifests (some may already be done)
        shard_manifests = []
        for i, future in enumerate(shard_manifest_futures):
            if future is None:
                shard_manifests.append(self._shard_manifest_cache.get(shard_hashes[i]))
            else:
                sm = future.result()
                shard_manifests.append(sm)

        # Phase 3: Submit shard data blob fetches (while HEAD blobs finish)
        shard_row_groups = []
        for sm in shard_manifests:
            if sm:
                shard_row_groups.extend(sm.scan_with_pruning(predicates, start_key, end_key))

        shard_blob_futures = {}
        for rg in shard_row_groups:
            if self._max_cache_blobs > 0 and rg.blob_hash in self._blob_cache:
                shard_blob_futures[rg.blob_hash] = None  # cache hit
            else:
                shard_blob_futures[rg.blob_hash] = phase2_pool.submit(
                    self.kernel.read_blob, rg.blob_hash)

        # Now collect HEAD blob bytes + decode them (while shard blobs fetch)
        all_row_groups = head_row_groups + shard_row_groups
        all_blob_futures = []
        for rg in all_row_groups:
            h = rg.blob_hash
            if h in head_blob_futures and head_blob_futures[h] is not None:
                all_blob_futures.append(head_blob_futures[h])
            elif h in shard_blob_futures and shard_blob_futures[h] is not None:
                all_blob_futures.append(shard_blob_futures[h])
            else:
                all_blob_futures.append(None)  # cache hit

        # Wait for all blobs, then decode in parallel
        blob_bytes_list = []
        for future in all_blob_futures:
            if future is not None:
                blob_bytes_list.append(future.result())
            else:
                blob_bytes_list.append(None)

        phase2_pool.shutdown(wait=True)

        # Decode all blobs (parallel, 8 threads for CPU)
        # Use cache for cache hits, decode for misses
        col_data_list: list[Optional[dict]] = [None] * len(all_row_groups)
        decode_pool = ThreadPoolExecutor(max_workers=8)
        decode_futures = []
        for i, rg in enumerate(all_row_groups):
            blob_bytes = blob_bytes_list[i]
            if blob_bytes is None:
                # Cache hit
                if self._max_cache_blobs > 0 and rg.blob_hash in self._blob_cache:
                    self._blob_cache_order.remove(rg.blob_hash)
                    self._blob_cache_order.append(rg.blob_hash)
                    col_data_list[i] = self._blob_cache[rg.blob_hash]
                else:
                    col_data_list[i] = {}
            else:
                decode_futures.append((i, decode_pool.submit(
                    self._decode_blob, blob_bytes, columns=columns, predicates=predicates)))

        for i, future in decode_futures:
            col_data = future.result()
            col_data_list[i] = col_data
            # Cache the result
            if self._max_cache_blobs > 0:
                rg_hash = all_row_groups[i].blob_hash
                self._blob_cache[rg_hash] = col_data
                self._blob_cache_order.append(rg_hash)
                while len(self._blob_cache_order) > self._max_cache_blobs:
                    old_hash = self._blob_cache_order.pop(0)
                    self._blob_cache.pop(old_hash, None)

        decode_pool.shutdown(wait=True)

        # Combine into rows
        all_rows: list[dict] = []
        # Fill missing columns (added via schema evolution) with None.
        # Only fill when the caller wants all columns (columns=None).
        manifest_col_names = set()
        if head_manifest and columns is None:
            manifest_col_names = {name for name, _ in head_manifest.columns}
        for col_data in col_data_list:
            if not col_data:
                continue
            col_names = list(col_data.keys())
            col_lists = tuple(col_data[c] for c in col_names)
            n = max((len(v) for v in col_lists), default=0)
            padded = []
            for v in col_lists:
                if len(v) < n:
                    padded.append(list(v) + [None] * (n - len(v)))
                else:
                    padded.append(v)
            for values in zip(*padded):
                row = dict(zip(col_names, values))
                if manifest_col_names:
                    for mc in manifest_col_names:
                        if mc not in row:
                            row[mc] = None
                all_rows.append(row)

        # Level 2 merge: dedup by _rowid, latest _version wins (CRDT)
        # Only applies if rows have _rowid (from upsert_shard/delete_shard)
        has_rowid = any(r.get("_rowid") for r in all_rows)
        if has_rowid:
            key_col = head_manifest.key_col if head_manifest else ""
            all_rows = self._merge_rows_by_rowid(all_rows, key_col=key_col or None)

        # Apply row filter
        if row_filter is not None:
            all_rows = [r for r in all_rows if row_filter(r)]

        return all_rows

    def read_at_snapshot(self, collection: str, commit_hash: str,
                          columns: Optional[list[str]] = None,
                          predicates: Optional[list[tuple[str, str, Any]]] = None
                          ) -> list[dict]:
        """Read data at a specific commit — SNAPSHOT ISOLATION.

        This provides a consistent snapshot: it reads ONLY the manifest
        at the given commit, ignoring any shards written after that commit.
        Long-running analytical queries get a consistent view.

        Args:
            collection: collection name
            commit_hash: the commit hash to read at (from history())
            columns: optional column projection
            predicates: optional predicate pushdown

        Returns:
            List of rows at the snapshot (no in-flight shards included).
        """
        # Read the commit blob
        commit = self._read_commit_blob(commit_hash)
        if commit is None:
            return []

        # Get the manifest hash from the commit
        manifest_hash = commit.get("manifest")
        if not manifest_hash:
            return []

        # Load the manifest at this commit
        manifest = self._load_manifest_from_hash(manifest_hash)
        if manifest is None:
            return []

        # Read ONLY the row groups in this manifest (no shards)
        all_rows = []
        for rg in manifest.scan_with_pruning():
            try:
                blob_bytes = self.kernel.read_blob(rg.blob_hash)
                decoded = self._decode_blob(blob_bytes,
                    columns=columns, predicates=predicates)
                if decoded:
                    n = len(next(iter(decoded.values()), []))
                    col_names = list(decoded.keys())
                    for i in range(n):
                        row = {col: decoded[col][i] for col in col_names
                               if i < len(decoded[col])}
                        all_rows.append(row)
            except Exception:
                continue

        return all_rows

    def read_branch_with_shards(self, collection: str, branch: str,
                                  predicates: Optional[list[tuple[str, str, Any]]] = None,
                                  columns: Optional[list[str]] = None) -> list[dict]:
        """Read a branch's full data: branch HEAD commit's manifest + branch's shards.

        This is the branch-aware version of read_with_shards. It does NOT
        mutate HEAD or the active branch — it resolves the branch's commit
        directly and reads its manifest, then merges in the branch's shards.

        Used by LakehouseLens.read_branch() to read a branch's full state
        without checking out (which would race with concurrent readers).

        Args:
            collection: collection name
            branch: branch name to read
            predicates: optional predicate pushdown
            columns: optional projection pushdown

        Returns:
            List of row dicts (HEAD + shards merged, row-level CRDT applied).
        """
        # Resolve the branch's commit
        branch_commit_hash = self.kernel.resolve(
            self._branch_ref(collection, branch))
        if branch_commit_hash is None:
            raise KeyError(f"Branch '{branch}' not found for collection '{collection}'")

        # Read the branch's commit blob → manifest hash
        branch_commit = self._read_commit_blob(branch_commit_hash)
        if branch_commit is None or not branch_commit.get("manifest"):
            return []
        branch_manifest_hash = branch_commit["manifest"]

        # Load the branch's manifest (handles both pack and PMAN formats)
        try:
            head_manifest = self._load_manifest_from_hash(branch_manifest_hash)
        except (ValueError, KeyError):
            head_manifest = None

        # Read the branch's shards
        shard_hashes = self._read_shard_index(collection, branch)
        shard_manifests = self._parallel_fetch_shard_manifests(
            shard_hashes,
            schema_columns=(head_manifest.columns if head_manifest else None),
            key_col=(head_manifest.key_col if head_manifest else ""))

        # Level 1 merge: UNION of all row groups (no dedup by rg_key).
        merged: list[Any] = []
        if head_manifest:
            merged.extend(head_manifest.scan_with_pruning(predicates))
        for sm in shard_manifests:
            merged.extend(sm.scan_with_pruning(predicates))

        if not merged:
            return []

        # Fetch + decode
        row_groups = merged
        col_data_list = self._parallel_fetch_and_decode(
            row_groups, columns, predicates)

        all_rows: list[dict] = []
        for col_data in col_data_list:
            if not col_data:
                continue
            n = len(next(iter(col_data.values())))
            for i in range(n):
                row = {}
                for c, vals in col_data.items():
                    if i < len(vals):
                        row[c] = vals[i]
                    else:
                        row[c] = None
                all_rows.append(row)

        # Row-level CRDT merge
        has_rowid = any(r.get("_rowid") for r in all_rows)
        if has_rowid:
            key_col = head_manifest.key_col if head_manifest else ""
            all_rows = self._merge_rows_by_rowid(all_rows, key_col=key_col or None)

        return all_rows

    def _read_as_columns_with_shards(self, collection: str,
                                       predicates: Optional[list[tuple[str, str, Any]]] = None,
                                       columns: Optional[list[str]] = None,
                                       shard_hashes: Optional[list[str]] = None
                                       ) -> dict[str, list]:
        """Columnar read merging HEAD + unmerged shards.

        Same merge semantics as read_with_shards, but returns columnar
        data (dict[col_name, list[values]]) instead of row dicts. Used
        by read_as_columns / read_as_arrow so they include unmerged shards.
        """
        if shard_hashes is None:
            shard_hashes = self._read_shard_index(collection)

        head_manifest = self._load_manifest(collection)
        shard_manifests = self._parallel_fetch_shard_manifests(
            shard_hashes,
            schema_columns=(head_manifest.columns if head_manifest else None),
            key_col=(head_manifest.key_col if head_manifest else ""))

        # Level 1 merge: UNION of all row groups (no dedup by rg_key).
        merged: list[Any] = []
        if head_manifest:
            merged.extend(head_manifest.scan_with_pruning(predicates))
        for sm in shard_manifests:
            merged.extend(sm.scan_with_pruning(predicates))

        if not merged:
            return {}

        # Ensure predicate columns are decoded
        eff_columns = list(columns) if columns is not None else None
        if predicates and eff_columns is not None:
            pred_cols = {p[0] for p in predicates}
            missing = pred_cols - set(eff_columns)
            if missing:
                eff_columns = list(dict.fromkeys(eff_columns + list(missing)))

        row_groups = list(merged)
        col_results = self._parallel_fetch_and_decode(
            row_groups, eff_columns, predicates)

        auto_filter = self._build_predicate_filter(predicates)
        result: dict[str, list] = {}
        for col_data in col_results:
            if auto_filter is None:
                for col_name, values in col_data.items():
                    if columns is not None and col_name not in columns:
                        continue
                    if col_name not in result:
                        result[col_name] = list(values)
                    else:
                        result[col_name].extend(values)
            else:
                # Apply filter row by row
                n = max((len(v) for v in col_data.values()), default=0)
                for i in range(n):
                    row = {c: col_data[c][i] if i < len(col_data[c]) else None
                            for c in col_data}
                    if auto_filter(row):
                        for col_name, val in row.items():
                            if columns is not None and col_name not in columns:
                                continue
                            if col_name not in result:
                                result[col_name] = [val]
                            else:
                                result[col_name].append(val)
        return result

    def compact_shards(self, collection: str,
                        target_row_group_size: int = 100_000) -> Optional[str]:
        """Merge all shards into HEAD, then clear the shards.

        This is the ONLY place that needs coordination, and it's idempotent:
        multiple compactors produce the same result (CRDT merge is
        commutative). Last-writer-wins on HEAD is safe here because the
        result is deterministic.

        TWO COMPACTION MODES:
          - Manifest-level (fast path): when no shards have _rowid columns
            (insert-only appends), merge row group ENTRIES (metadata only)
            without reading any data blobs. O(shard_count) GETs, ZERO data I/O.
            Data blobs are immutable and content-addressed — the new manifest
            simply references them directly from HEAD + shards.
            (target_row_group_size is a no-op here — manifest-level compaction
            re-references existing row groups without rewriting them.)
          - Row-level (fallback): when shards have _rowid columns (upserts/
            deletes), decode all rows, apply CRDT merge by _rowid, re-encode
            into new row groups of `target_row_group_size` rows each.
            O(total_rows) data I/O — same as before, but the resulting row
            groups can be larger (default 100K rows) to reduce read
            amplification on the next scan.

        The fast path makes compaction viable at PB scale: merging 16 shards
        each with 10K row groups costs 16 GETs (shard manifests) + 1 PUT
        (merged manifest), regardless of total data volume.

        After compaction:
          - HEAD points to a new flat manifest containing all row groups
          - All shards are cleared (their refs are tombstoned)
          - Reads are fast again (1 manifest, no shard list)

        Args:
            collection: collection name
            target_row_group_size: row group size for row-level compaction
                re-encoding (default 100_000). Larger row groups reduce
                read amplification (fewer blobs per scan) at the cost of
                coarser pruning. Manifest-level compaction ignores this
                parameter (it doesn't re-encode).
        """
        # === PIPELINED COMPACTION READ PHASE ===
        # Phase 1: Load HEAD manifest + list shards IN PARALLEL (1 RTT)
        # Phase 2: Read ALL shard blobs IN PARALLEL (1 RTT)
        # Was: sequential load_manifest → read_shard_index → N × _load_shard_manifest
        branch = self._get_active_branch(collection)

        from concurrent.futures import ThreadPoolExecutor

        # Phase 1: Load HEAD manifest + list shard refs IN PARALLEL
        with ThreadPoolExecutor(max_workers=2) as pool:
            f_manifest = pool.submit(self._load_manifest, collection)
            f_shards = pool.submit(self._list_shard_refs_with_names, collection, branch)
            head_manifest = f_manifest.result()
            shard_refs = f_shards.result()

        shard_hashes = [h for (_n, h) in shard_refs]
        if not shard_hashes:
            return None  # nothing to compact

        head_schema = head_manifest.columns if head_manifest else None
        head_key_col = head_manifest.key_col if head_manifest else ""

        # Phase 2: Read ALL shard blobs IN PARALLEL (1 RTT for all)
        shard_blobs = {}
        if shard_hashes:
            with ThreadPoolExecutor(max_workers=min(32, len(shard_hashes))) as pool:
                futures = {pool.submit(self.kernel.read_blob, sh): sh
                            for sh in shard_hashes if sh}
                for f in futures:
                    sh = futures[f]
                    try:
                        shard_blobs[sh] = f.result()
                    except Exception:
                        shard_blobs[sh] = None

        # Decode shard blobs into manifests (no I/O — already fetched)
        merged: dict[str, Any] = {}
        if head_manifest:
            for rg in head_manifest.scan_with_pruning():
                merged[rg.key] = rg
        shard_manifests = []
        for sh in shard_hashes:
            blob_bytes = shard_blobs.get(sh)
            if blob_bytes:
                sm = self._load_shard_manifest_from_bytes(
                    sh, blob_bytes, head_schema, head_key_col)
                if sm is not None:
                    shard_manifests.append(sm)
                    for rg in sm.scan_with_pruning():
                        merged[rg.key] = rg

        if not merged:
            return None

        # Check if any shard has _rowid columns (row-level CRDT needed)
        # by inspecting the schema of HEAD + all shard manifests.
        needs_row_merge = self._manifests_have_rowid(head_manifest, shard_manifests)

        if needs_row_merge:
            return self._compact_shards_row_level(
                collection, head_manifest, shard_hashes, merged,
                target_row_group_size=target_row_group_size)
        else:
            return self._compact_shards_manifest_level(
                collection, head_manifest, shard_hashes, merged,
                target_row_group_size=target_row_group_size)

    def _manifests_have_rowid(self, head_manifest, shard_manifests) -> bool:
        """Check if any manifest has _rowid column (indicating row-level CRDT).

        Checks BOTH the manifest's schema columns AND the row groups' column
        stats. The schema may not include _rowid if the collection was created
        via write() (which doesn't add _rowid) and later upserted via
        upsert_shard (which adds _rowid to the data but not to the manifest's
        schema). The row group column stats always reflect the actual encoded
        columns, so they're the reliable signal.
        """
        manifests = [head_manifest] + shard_manifests
        for m in manifests:
            if m is None:
                continue
            # Check schema columns
            for col_name, _vtype in m.columns:
                if col_name == "_rowid":
                    return True
            # Check row group column stats (more reliable — reflects actual data)
            for rg in m.row_groups:
                for col in rg.columns:
                    if col.name == "_rowid":
                        return True
        return False

    def _compact_shards_manifest_level(self, collection, head_manifest,
                                         shard_hashes, merged,
                                         target_row_group_size: int = 100_000
                                         ) -> Optional[str]:
        """Fast-path compaction: merge manifest entries only, NO data I/O.

        Data blobs are immutable and content-addressed. The new manifest
        simply references the same blob_hash values from HEAD + shards.
        This makes compaction O(shard_count) GETs + O(1) manifest PUT,
        regardless of total data volume — viable at PB scale.

        Note: target_row_group_size is NOT used here — manifest-level
        compaction re-references existing row groups without rewriting them.
        To merge small row groups into larger ones, row-level compaction
        is required (which decodes and re-encodes). The parameter is
        accepted for API symmetry with _compact_shards_row_level.
        """
        schema = (head_manifest.columns if head_manifest else [("value", 4)])
        key_col = (head_manifest.key_col if head_manifest else "")
        rg_size = (head_manifest.row_group_size if head_manifest else 10_000)

        # Build manifest entries from merged row group entries
        # (NO data blob reads — just metadata)
        manifest_entries = []
        for rg in sorted(merged.values(), key=lambda r: r.key):
            manifest_entries.append({
                "rg_key": rg.key,
                "blob_hash": rg.blob_hash,
                "n_rows": rg.n_rows,
                "col_stats": [(c.name, c.value_type, c.min, c.max, c.null_count)
                                for c in rg.columns],
            })

        # Build manifest LOCALLY (no I/O) — encode + hash only
        new_manifest = CollectionManifest(self.kernel)
        new_manifest.set_schema(
            columns=schema, key_col=key_col,
            row_group_size=rg_size, chunk_size=0,
        )
        for entry in manifest_entries:
            rg = RowGroupEntry(
                key=entry["rg_key"], blob_hash=entry["blob_hash"],
                n_rows=entry["n_rows"], storage_mode=STORAGE_WHOLE_BLOB,
            )
            for col_name, vtype, mn, mx, null_count in entry["col_stats"]:
                rg.columns.append(ColumnStatsEntry(
                    name=col_name, value_type=vtype, min=mn, max=mx,
                    null_count=null_count, chunks=[],
                ))
            new_manifest.add_row_group(rg)

        # Build manifest LOCALLY (no I/O) — encode + hash only
        new_manifest = CollectionManifest(self.kernel)
        new_manifest.set_schema(
            columns=schema, key_col=key_col,
            row_group_size=rg_size, chunk_size=0,
        )
        for entry in manifest_entries:
            rg = RowGroupEntry(
                key=entry["rg_key"], blob_hash=entry["blob_hash"],
                n_rows=entry["n_rows"], storage_mode=STORAGE_WHOLE_BLOB,
            )
            for col_name, vtype, mn, mx, null_count in entry["col_stats"]:
                rg.columns.append(ColumnStatsEntry(
                    name=col_name, value_type=vtype,
                    min=mn, max=mx, null_count=null_count, chunks=[],
                ))
            new_manifest.add_row_group(rg)

        # Carry forward the inline bloom filter from HEAD manifest
        # so negative lookups still work after compaction.
        if head_manifest and head_manifest._inline_bloom:
            new_manifest.set_inline_bloom(head_manifest._inline_bloom)
        # Build stats tree (writer-side, P10 fix) BEFORE encoding
        try:
            from stats_tree import should_use_stats_tree, build_stats_tree
            if should_use_stats_tree(len(new_manifest.row_groups)):
                stats_root = build_stats_tree(self.kernel, new_manifest.row_groups)
                new_manifest.set_stats_tree_root(stats_root)
        except ImportError:
            pass

        # Encode manifest locally (no I/O)
        manifest_bytes, manifest_hash = self._encode_manifest_local(new_manifest)

        # O(1) warm commit: use cached HEAD + commit_index if available
        parent = self._head_cache.get(collection)
        if parent is None:
            parent = self.kernel.resolve(self._active_commit_ref(collection))
        commit_index = self._commit_index_cache.get(collection, 0)
        if commit_index == 0 and parent:
            pc = self._read_commit_blob(parent)
            if pc:
                commit_index = pc.get("index", 0) + 1

        # BATCH-PUT: pack blob (commit + manifest) + 2 refs in parallel
        # = 1 RTT wall-clock. PondPack saves 1 PUT vs separate commit + manifest.
        import json as _json
        import time as _time
        commit = {
            "parent": parent,
            "second_parent": None,
            "manifest": manifest_hash,
            "message": f"compact {len(shard_hashes)} shards (manifest-level, {len(manifest_entries)} row groups)",
            "timestamp": _time.time(),
            "index": commit_index,
        }
        pack_bytes = encode_pack(commit, manifest_bytes)
        pack_hash = hash_bytes(pack_bytes)

        active = self._active_commit_ref(collection)
        manifest_ref = self._manifest_ref(collection)

        if hasattr(self.kernel, 'store') and hasattr(self.kernel.store, 'put_blob'):
            from concurrent.futures import ThreadPoolExecutor

            def _put_pack_blob():
                self.kernel.store.put_blob(pack_bytes)

            def _put_active_ref():
                self.kernel.store.put_path(active, pack_hash)

            def _put_manifest_ref():
                self.kernel.store.put_path(manifest_ref, pack_hash)

            with ThreadPoolExecutor(max_workers=3) as pool:
                f1 = pool.submit(_put_pack_blob)
                f2 = pool.submit(_put_active_ref)
                f3 = pool.submit(_put_manifest_ref)
                f1.result(); f2.result(); f3.result()

            self.kernel.stats["writes"] += 1
            self.kernel.stats["ref_writes"] += 2
            self.kernel.stats["references"] += 2
            self.kernel._update_path_cache(active, pack_hash)
            self.kernel._update_path_cache(manifest_ref, pack_hash)
        else:
            # PondMinimal fallback: sequential
            self.kernel.write(pack_bytes)
            self.kernel.reference(active, pack_hash)
            self.kernel.reference(manifest_ref, pack_hash)

        # ASYNC TOMBSTONING + VACUUM: The shard ref deletes + blob deletes
        # are fire-and-forget. They run in a BACKGROUND thread — compact()
        # returns immediately after the commit + ref PUTs.
        #
        # This is SAFE because:
        #   1. The new manifest is already HEAD (commit + refs written).
        #   2. Readers use the new manifest — they don't need the old shards.
        #   3. If a reader sees old shards + new manifest, the CRDT union
        #      dedupes by rg_key (same row group, same blob_hash — no harm).
        #   4. The tombstone deletes will complete shortly (within seconds).
        #   5. Vacuum (blob deletes) is purely space reclamation — doesn't
        #      affect correctness.
        branch = self._get_active_branch(collection)

        # Capture the data needed by the background thread
        _shard_hashes = list(shard_hashes)
        _new_manifest = new_manifest
        _collection = collection
        _branch = branch

        import threading
        def _async_tombstone_and_vacuum():
            try:
                self._clear_branch_shards(_collection, _branch,
                                           shard_hashes=_shard_hashes)
            except Exception:
                pass
            try:
                self._auto_vacuum_after_compact(
                    _collection, _shard_hashes, new_manifest=_new_manifest)
            except Exception:
                pass  # best-effort

        t = threading.Thread(target=_async_tombstone_and_vacuum, daemon=True)
        t.start()
        self._bg_threads = getattr(self, '_bg_threads', [])
        self._bg_threads.append(t)

        # Update caches — both commit_hash and manifest_hash are pack_hash
        self._update_caches_after_write(
            collection, new_manifest, pack_hash, pack_hash,
            commit_index, is_delta=False)
        # Invalidate shard list cache — shards are being cleared in background
        self._invalidate_shard_cache(collection)

        return pack_hash

    def _compact_shards_row_level(self, collection, head_manifest,
                                    shard_hashes, merged,
                                    target_row_group_size: int = 100_000
                                    ) -> Optional[str]:
        """Fallback compaction: decode all rows, apply row-level CRDT merge.

        Used when shards have _rowid columns (upserts/deletes). This path
        reads ALL data blobs, merges by _rowid (latest _version wins),
        drops tombstones, and re-encodes into new row groups of
        `target_row_group_size` rows each (default 100K — larger than
        typical write-time row groups, reducing read amplification).

        O(total_rows) data I/O — same as the pre-optimization behavior,
        but the resulting row groups are larger (better for subsequent reads).
        """
        # Fetch + decode ALL rows from merged row groups
        row_groups = list(merged.values())
        col_data_list = self._parallel_fetch_and_decode(row_groups, None, None)
        all_rows: list[dict] = []
        for col_data in col_data_list:
            if not col_data:
                continue
            n = len(next(iter(col_data.values())))
            for i in range(n):
                row = {}
                for c, vals in col_data.items():
                    if i < len(vals):
                        row[c] = vals[i]
                    else:
                        row[c] = None
                all_rows.append(row)

        # Level 2: row-level CRDT merge (dedup by _rowid, drop tombstones)
        has_rowid = any(r.get("_rowid") for r in all_rows)
        if has_rowid:
            key_col = head_manifest.key_col if head_manifest else ""
            all_rows = self._merge_rows_by_rowid(all_rows, key_col=key_col or None)

        # Build the compacted manifest with only LIVE rows
        schema = (head_manifest.columns if head_manifest
                   else [("value", 4)])
        key_col = (head_manifest.key_col if head_manifest else "")
        # Use the adaptive target_row_group_size for re-encoding (defaults
        # to 100K — larger than typical write-time row groups, reducing
        # read amplification on subsequent scans). Fall back to the HEAD
        # manifest's row_group_size only if the caller passed 0.
        rg_size = target_row_group_size if target_row_group_size > 0 else \
            (head_manifest.row_group_size if head_manifest else 10_000)

        # BATCH rows into proper row groups (not 1 row per blob)
        if all_rows:
            manifest_entries = []
            for start in range(0, len(all_rows), max(rg_size, 1)):
                end = min(start + max(rg_size, 1), len(all_rows))
                group_rows = all_rows[start:end]
                group_source = ListColumnSource(group_rows)
                pnd2_bytes, col_stats = PND2.encode(group_source)

                # Use the actual key column value for rg_key
                if key_col and key_col in group_rows[-1]:
                    max_pk = group_rows[-1][key_col]
                else:
                    max_pk = start + len(group_rows) - 1
                rg_key = _format_rg_key(max_pk)

                blob_hash = self.kernel.write(pnd2_bytes)
                manifest_entries.append({
                    "rg_key": rg_key,
                    "blob_hash": blob_hash,
                    "n_rows": end - start,
                    "col_stats": col_stats,
                })
            manifest_hash, new_manifest, manifest_bytes = self._build_manifest_with_return(
                collection, manifest_entries, schema, key_col, rg_size)
        else:
            manifest_hash, new_manifest, manifest_bytes = self._build_manifest_with_return(
                collection, [], schema, key_col, rg_size)

        # P10 fix: Build stats tree during compaction (writer-side).
        # Re-encode manifest if stats tree was added (manifest_bytes changes).
        try:
            from stats_tree import should_use_stats_tree, build_stats_tree
            if should_use_stats_tree(len(new_manifest.row_groups)):
                stats_root = build_stats_tree(self.kernel, new_manifest.row_groups)
                new_manifest.set_stats_tree_root(stats_root)
                # Re-encode locally with the stats tree root
                manifest_bytes = new_manifest.encode()
                manifest_hash = hash_bytes(manifest_bytes)
        except ImportError:
            pass

        # Write a new commit pointing to the compacted manifest
        parent = self._head_cache.get(collection)
        if parent is None:
            parent = self.kernel.resolve(self._active_commit_ref(collection))
        commit_index = self._commit_index_cache.get(collection, 0)
        if commit_index == 0 and parent:
            pc = self._read_commit_blob(parent)
            if pc:
                commit_index = pc.get("index", 0) + 1

        commit_hash = self._write_commit_blob(
            collection, manifest_hash, parent=parent,
            message=f"compact {len(shard_hashes)} shards ({len(all_rows)} live rows)",
            index=commit_index, manifest_bytes=manifest_bytes)

        # ASYNC TOMBSTONING + VACUUM (same as manifest-level compaction):
        # Fire-and-forget background thread. compact() returns immediately
        # after the commit + ref PUTs. See _compact_shards_manifest_level
        # for the safety analysis.
        branch = self._get_active_branch(collection)

        _shard_hashes = list(shard_hashes)
        _new_manifest = new_manifest
        _collection = collection
        _branch = branch

        import threading
        def _async_tombstone_and_vacuum():
            try:
                self._clear_branch_shards(_collection, _branch,
                                           shard_hashes=_shard_hashes)
            except Exception:
                pass
            try:
                self._auto_vacuum_after_compact(
                    _collection, _shard_hashes, new_manifest=_new_manifest)
            except Exception:
                pass

        t = threading.Thread(target=_async_tombstone_and_vacuum, daemon=True)
        t.start()
        self._bg_threads = getattr(self, '_bg_threads', [])
        self._bg_threads.append(t)

        # Update caches
        self._update_caches_after_write(
            collection, new_manifest, manifest_hash, commit_hash,
            commit_index, is_delta=False)
        self._invalidate_shard_cache(collection)

        return commit_hash

    def _auto_vacuum_after_compact(self, collection: str,
                                     shard_hashes: list[str],
                                     new_manifest: Optional[CollectionManifest] = None
                                     ) -> None:
        """Delete dead blobs after compaction (best-effort).

        After compaction, the old shard manifests + their data blobs are
        unreachable (tombstoned refs point to empty blobs). This method
        deletes those dead blobs so object count actually decreases.

        Uses the kernel's delete_blob (maintenance operation, not a kernel
        primitive) to reclaim space.

        Args:
            collection: collection name (unused, kept for API symmetry)
            shard_hashes: the OLD shard hashes (PMAN manifest hashes for
                multi-row-group shards, or PND2 data blob hashes for
                inline single-row-group shards).
            new_manifest: the NEW compacted manifest. If provided, its
                row group blob hashes are PROTECTED from deletion (they
                are now part of HEAD). This is critical for manifest-level
                compaction of inline shards: the shard hash IS the data
                blob hash, and that blob is now referenced by the new
                manifest — deleting it would corrupt HEAD. If None, all
                shard hashes are deleted (used by row-level compaction
                where all data is re-encoded into new blobs).
        """
        import hashlib as _hashlib
        empty_hash = _hashlib.sha256(b"").hexdigest()

        # Build a set of blob hashes referenced by the new manifest.
        # These must NOT be deleted — they are now part of HEAD.
        protected_hashes: set[str] = set()
        if new_manifest is not None:
            for rg in new_manifest.row_groups:
                protected_hashes.add(rg.blob_hash)

        # The shard_hashes are the OLD shard hashes. For PMAN manifest
        # shards, the hash is the manifest blob (its data blobs are
        # separate and not in shard_hashes, so they're safe). For inline
        # PND2 shards, the hash IS the data blob — it must be protected
        # if the new manifest references it.
        #
        # OPTIMIZATION: deletes run in PARALLEL via thread pool (was N × RTT
        # sequential, now 1 RTT wall-clock for the whole batch).
        to_delete = []
        for sh in shard_hashes:
            if not sh or sh == empty_hash:
                continue
            if sh in protected_hashes:
                # This blob is now referenced by the new manifest —
                # deleting it would corrupt HEAD. Skip.
                continue
            to_delete.append(sh)

        if not to_delete:
            return

        if len(to_delete) == 1:
            try:
                self.kernel.store.delete_blob(to_delete[0])
            except Exception:
                pass
            return

        from concurrent.futures import ThreadPoolExecutor

        def _delete_one(h):
            try:
                self.kernel.store.delete_blob(h)
            except Exception:
                pass  # best-effort

        workers = min(16, len(to_delete))
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = [pool.submit(_delete_one, h) for h in to_delete]
            for f in futures:
                f.result()

    # ------------------------------------------------------------------
    # ATOMIC PUBLICATION — commit markers on top of CRDT shards
    #
    # Atomic publication = CRDT + commit markers. Same model, thin extension.
    #
    # IMPORTANT: this is NOT full ACID. It provides atomic VISIBILITY
    # (once the commit marker exists, all tentative shards become visible
    # together) but does NOT provide:
    #   - Isolation (readers can see committed state from other txns
    #     mid-read; no snapshot isolation)
    #   - Rollback (abort_tx is a no-op; tentative shards are orphaned
    #     until GC)
    #   - Conflict detection (two txns can write the same _rowid; merge
    #     is LWW by _version)
    #   - Serializability
    # See docs/HONEST_COMPETITOR_COMPARISON.md §3 for the honest
    # description of what this provides vs. real ACID.
    #
    #   tx = storage.begin_tx()
    #   storage.append_shard("users", rows, tx_id=tx)
    #   storage.append_shard("orders", orders, tx_id=tx)
    #   storage.commit_tx(tx)   # 1 PUT — ALL shards become visible
    #
    # Readers automatically filter: shards with tx_id are only visible
    # if the commit marker exists. No coordinator, no 2PC, no CAS.
    # ------------------------------------------------------------------

    @staticmethod
    def _tx_ref(tx_id: str) -> str:
        """The commit marker ref path.

        NEW short layout: r/tx/{tx_id}
        (was: transactions/{tx_id})
        """
        return f"transactions/{tx_id}"

    def begin_tx(self) -> str:
        """Begin a transaction. Returns the tx_id.

        The tx_id is a UUIDv7 (time-ordered, unique). Pass it to
        append_shard(tx_id=...) to make shards tentative.

        No storage operation is performed — begin_tx is free.
        The tx_id is just a unique identifier until commit_tx.
        """
        try:
            from uuid7 import uuidv7
            return uuidv7()
        except ImportError:
            import time as _t
            return f"{_t.time_ns():016x}"

    def commit_tx(self, tx_id: str, message: str = "") -> str:
        """Commit a transaction — ALL tentative shards become visible.

        Writes a commit marker (1 PUT + 1 ref). Readers checking
        tentative shards will find this marker and include them.

        This is atomic: the commit marker is a single ref update.
        Crash before = shards invisible. Crash after = all visible.

        Args:
            tx_id: the transaction ID (from begin_tx)
            message: optional commit message

        Returns:
            The commit marker hash.

        OPTIMIZATION: compute marker_hash locally (SHA-256 of marker_bytes)
        so we can PUT the blob AND the ref in PARALLEL — was 2 sequential
        RTTs (~600ms on R2), now 1 RTT wall-clock.
        """
        import json as _json
        import time as _time

        marker = {
            "tx_id": tx_id,
            "timestamp": _time.time(),
            "message": message,
        }
        marker_bytes = _json.dumps(marker, sort_keys=True).encode()

        # ObjectStoreNativeKernel path: parallel PUT
        if hasattr(self.kernel, 'store') and hasattr(self.kernel.store, 'put_blob'):
            # Compute hash locally — avoids 1 round-trip vs kernel.write()
            marker_hash = hash_bytes(marker_bytes)
            tx_ref = self._tx_ref(tx_id)

            from concurrent.futures import ThreadPoolExecutor

            def _put_blob():
                self.kernel.store.put_blob(marker_bytes)

            def _put_path():
                self.kernel.store.put_path(tx_ref, marker_hash)

            with ThreadPoolExecutor(max_workers=2) as pool:
                f1 = pool.submit(_put_blob)
                f2 = pool.submit(_put_path)
                f1.result()
                f2.result()

            self.kernel.stats["writes"] += 1
            self.kernel.stats["ref_writes"] += 1
            self.kernel.stats["references"] += 1
            self.kernel._update_path_cache(tx_ref, marker_hash)

            # Invalidate shard list cache — tentative shards are now visible
            # to readers (the commit marker exists, so _list_shards_from_refs
            # will include them). The cached list didn't include them.
            for key in list(self._shard_list_cache.keys()):
                self._shard_list_cache.pop(key, None)
            return marker_hash

        # PondMinimal fallback path: sequential
        marker_hash = self.kernel.write(marker_bytes)
        self.kernel.reference(self._tx_ref(tx_id), marker_hash)
        # Invalidate shard list cache — tentative shards are now visible
        # to readers (the commit marker exists, so _list_shards_from_refs
        # will include them). The cached list didn't include them.
        for key in list(self._shard_list_cache.keys()):
            self._shard_list_cache.pop(key, None)
        return marker_hash

    def abort_tx(self, tx_id: str) -> None:
        """Abort a transaction — tentative shards stay invisible.

        Simply don't write the commit marker. Tentative shards remain
        in storage but are invisible to readers (no commit marker).
        GC cleans them up after a configurable timeout.

        This is a no-op: abort = "don't commit". No storage operation.
        """
        pass  # Nothing to do — absence of commit marker = aborted

    def is_tx_committed(self, tx_id: str) -> bool:
        """Check if a transaction has been committed."""
        return self.kernel.resolve(self._tx_ref(tx_id)) is not None

    def shard_count(self, collection: str) -> int:
        """Return the number of unmerged shards for a collection.

        Delegates to _read_shard_index, which scans the live shard refs
        (post-compaction tombstones are skipped by _list_shards_from_refs).
        """
        return len(self._read_shard_index(collection))

    # ------------------------------------------------------------------
    # ROW-LEVEL CRDT — upsert + delete with version vectors
    #
    # The shard CRDT handles INSERT well, but UPDATE and DELETE at the
    # row level need explicit versioning. This section adds:
    #
    #   - upsert_shard: insert-or-update rows with (_rowid, _version)
    #   - delete_shard: row-level tombstones with (_rowid, _version)
    #   - read_with_shards: row-level merge (last-writer-wins by _version)
    #
    # Merge rules (deterministic, eventually consistent):
    #   - INSERT + INSERT (same _rowid): later _version wins
    #   - UPDATE + UPDATE (same _rowid): later _version wins
    #   - DELETE + anything: later _version wins (tombstone if DELETE is later)
    #   - INSERT + INSERT (different _rowid): both kept (no conflict)
    #
    # _rowid: UUIDv7 string (time-ordered, globally unique). Stable across
    #   updates — an UPDATE keeps the same _rowid, bumps _version.
    # _version: UUIDv7 string (time-ordered). Each write generates a new
    #   _version. Merge compares _version strings lexicographically
    #   (UUIDv7 is time-ordered, so lexicographic = chronological).
    # _deleted: bool. If True, this row is a tombstone (deleted at _version).
    #
    # These are REGULAR COLUMNS in PND2 — no format change. The merge
    # logic lives in read_with_shards and compact_shards.
    # ------------------------------------------------------------------

    def upsert_shard(self, collection: str, rows: list[dict],
                      key_col: Optional[str] = None,
                      row_group_size: int = 10_000) -> str:
        """Concurrent-safe upsert (insert-or-update) with row-level CRDT.

        Each row gets a _rowid (stable across updates) and _version
        (new per write). On merge, the row with the later _version wins.

        For NEW rows: caller does NOT provide _rowid — we generate one.
        For UPDATES: caller provides _rowid (from the original read),
                     we generate a new _version.

        Args:
            collection: collection name
            rows: list of row dicts. For updates, include _rowid from
                  the original row. For inserts, omit _rowid.
            key_col: sort key column (for range scans)
            row_group_size: rows per row group

        Returns:
            The shard manifest hash.
        """
        try:
            from uuid7 import uuidv7
        except ImportError:
            import time as _t
            uuidv7 = lambda: f"{_t.time_ns():016x}"

        # B5 fix: use HLC (Hybrid Logical Clock) for _version instead of UUIDv7.
        # HLC is monotonic under clock skew — UUIDv7 is not.
        # The HLC instance is shared across ALL upsert_shard/delete_shard calls
        # (stored as self._hlc). This keeps the logical counter monotonic —
        # two calls in the same millisecond get DIFFERENT _version strings
        # (physical_ms same, logical incremented), so the second update is
        # not silently lost during CRDT merge.
        if self._hlc is not None:
            _gen_version = self._hlc.tick
        else:
            _gen_version = uuidv7
        stamped = []
        for row in rows:
            r = dict(row)
            if "_rowid" not in r or not r["_rowid"]:
                r["_rowid"] = uuidv7()  # new row — generate _rowid (UUIDv7 is fine for identity)
            r["_version"] = _gen_version()  # HLC for version (clock-skew-safe)
            r["_deleted"] = False
            stamped.append(r)

        return self.append_shard(collection, stamped, key_col=key_col,
                                   row_group_size=row_group_size,
                                   message="upsert shard")

    def delete_shard(self, collection: str, rowids: list[str],
                      key_col: Optional[str] = None,
                      row_group_size: int = 10_000,
                      keys: Optional[list[str]] = None) -> str:
        """Concurrent-safe row-level delete with tombstones.

        Each deleted _rowid gets a tombstone row with _deleted=True and
        a new _version. On merge, if the tombstone's _version is later
        than any live row's _version, the row is suppressed.

        Args:
            collection: collection name
            rowids: list of _rowid strings to delete
            key_col: sort key column (for range scans)
            row_group_size: rows per row group
            keys: optional list of key_col values, one per rowid. If
                provided, each tombstone's key_col is set to the actual
                key value (not ""). This avoids rg_key collisions in
                compact_shards/merge when multiple keys are deleted —
                without distinct key_col values, all tombstones land in
                one row group with the same rg_key, and dedup-by-rg_key
                drops all but the last tombstone. If None, tombstones
                get key_col="" (legacy behavior — may lose tombstones
                during compaction when deleting multiple keys).

        Returns:
            The shard manifest hash.
        """
        try:
            from uuid7 import uuidv7
        except ImportError:
            import time as _t
            uuidv7 = lambda: f"{_t.time_ns():016x}"

        # B5 fix: use HLC for _version (shared instance — see upsert_shard)
        if self._hlc is not None:
            _gen_version = self._hlc.tick
        else:
            _gen_version = uuidv7

        tombstones = []
        for i, rid in enumerate(rowids):
            t = {
                "_rowid": rid,
                "_version": _gen_version(),
                "_deleted": True,
            }
            # Set key_col to the actual key value (if provided) so
            # tombstones get distinct rg_keys and survive compaction.
            # Without this, all tombstones share rg_key="rg/" and
            # compact_shards/merge drop all but the last one.
            if keys is not None and i < len(keys) and keys[i] is not None:
                t[key_col or "_key"] = keys[i]
            else:
                t[key_col or "_key"] = ""
            tombstones.append(t)

        return self.append_shard(collection, tombstones, key_col=key_col,
                                   row_group_size=row_group_size,
                                   message="delete shard")

    def _merge_rows_by_rowid(self, all_rows: list[dict],
                              key_col: Optional[str] = None) -> list[dict]:
        """Merge rows by _rowid, keeping the one with the latest _version.

        Tombstones (_deleted=True) suppress the row if their _version is
        the latest. If a live row has a later _version, it overrides the
        tombstone (the delete was superseded by a concurrent update).

        This is the CRDT merge — deterministic and eventually consistent.

        LEGACY ROWS: rows without _rowid (from write(), not upsert_shard)
        are kept as-is — UNLESS there are CRDT rows (with _rowid) AND a
        key_col is provided, in which case legacy rows whose key_col value
        matches a CRDT row's key_col value are dropped (the CRDT row is
        newer and supersedes the legacy snapshot). Legacy rows with unique
        key_col values are kept (they represent data not yet upserted).

        TOMBSTONE KEY MATCHING: tombstones carry the key_col value of the
        row they delete. Legacy rows with the same key_col value are
        suppressed (the tombstone says "this key is deleted"). The match
        is type-coerced (str(0) == 0) to handle the case where tombstones
        store key_col as strings (from delete_shard's keys parameter) but
        the original data has ints.

        Args:
            all_rows: rows to merge (may include both _rowid-tagged and
                      legacy rows)
            key_col: the sort key column name. Used to dedup legacy rows
                     against CRDT rows. If None, legacy rows are always kept.
        """
        # First pass: separate CRDT rows (with _rowid) from legacy rows.
        latest: dict[str, dict] = {}
        legacy_rows: list[dict] = []
        has_crdt = False
        for row in all_rows:
            rid = row.get("_rowid")
            if rid is None:
                legacy_rows.append(row)
                continue
            has_crdt = True
            ver = row.get("_version", "")
            if self._hlc is not None and ver:
                self._hlc.observe(ver)
            existing = latest.get(rid)
            if existing is None or ver > existing.get("_version", ""):
                latest[rid] = row

        result = []

        if has_crdt and key_col:
            # CRDT mode with key_col: build a set of key_col values that
            # are "claimed" by CRDT rows (either live or tombstoned).
            # Use str() coercion so that int 0 and str "0" match.
            crdt_keys = set()
            for row in latest.values():
                kv = row.get(key_col)
                if kv is not None:
                    crdt_keys.add(str(kv))
            for row in legacy_rows:
                kv = row.get(key_col)
                if kv is not None and str(kv) in crdt_keys:
                    continue  # superseded by a CRDT row (live or tombstoned) — drop
                result.append(row)
        else:
            # No CRDT rows, or no key_col to dedup by — keep legacy rows.
            result.extend(legacy_rows)

        # Add surviving CRDT rows (drop tombstones that won)
        for row in latest.values():
            if row.get("_deleted"):
                continue  # tombstone won — suppress this row
            result.append(row)

        return result

    # ------------------------------------------------------------------
    # WRITE — the ONE write path
    # ------------------------------------------------------------------

    def write(self, collection: str, rows,
              key_col: Optional[str] = None,
              row_group_size: int = 10_000,
              encoding_hints: Optional[dict[str, str]] = None,
              message: str = "") -> str:
        """Write rows to a collection as PND2 blobs.

        Args:
            collection: collection name
            rows: a ColumnSource OR PyArrow Table OR list[dict] (KV rows)
            key_col: column to use as the sort key (None = use row index)
            row_group_size: rows per row group (default 10_000)
            encoding_hints: optional dict {col_name: "auto"|"rle"|"dict"|"bitpack"|"raw"}
            message: commit message

        Returns:
            The new HEAD commit hash.
        """
        # Coerce input to a ColumnSource
        if isinstance(rows, list):
            source = ListColumnSource(rows)
        elif isinstance(rows, ColumnSource):
            source = rows
        else:
            source = as_column_source(rows)
        n_rows = source.num_rows()

        # Sort by key_col if specified (empty string = no key col)
        if key_col == "":
            key_col = None
        if key_col is not None and key_col in source.column_names():
            # For PyArrow, we can sort. For ListColumnSource, sort in Python.
            source = _sort_source_by(source, key_col)
            key_array = source.column_slice(key_col, 0, n_rows)
        elif key_col is not None:
            raise KeyError(f"key column '{key_col}' not in source columns")
        else:
            key_array = list(range(n_rows))

        # Build row groups
        manifest_entries: list[dict] = []
        col_names = source.column_names()

        # Detect value types once (from the first chunk)
        schema_columns: list[tuple[str, int]] = []
        if n_rows > 0:
            for col_name in col_names:
                sample = source.column_slice(col_name, 0, min(100, n_rows))
                vtype = _detect_value_type_with_binary(sample)
                schema_columns.append((col_name, vtype))

        # write() has overwrite semantics — the new manifest replaces the
        # old one entirely. No need to delete old row group keys from a
        # ProllyTree (removed — unified architecture uses manifest only).
        # content-addressed (deduped); the old manifest is simply not
        # referenced by the new commit.
        #
        # OPTIMIZATION: skip the existing_manifest load — it was a wasted
        # GET (the variable was never used; schema_columns comes from the
        # source). Saves 1-2 RTTs per write (significant for cold writes).

        if n_rows == 0:
            # Fix (Round 11 Issue #1): empty write must still update the
            # manifest so the collection shows as empty (not stale data).
            manifest = CollectionManifest(self.kernel)
            manifest.set_schema(columns=schema_columns, key_col=key_col or "",
                                 row_group_size=row_group_size, chunk_size=0)
            manifest_hash, manifest_bytes = self._build_manifest(
                collection, [], schema_columns,
                key_col or "", row_group_size)
            # O(1) warm write: use cached HEAD + commit_index if available
            parent = self._head_cache.get(collection)
            if parent is None:
                parent = self.kernel.resolve(self._active_commit_ref(collection))
            commit_index = self._commit_index_cache.get(collection, 0)
            if commit_index == 0 and parent:
                pc = self._read_commit_blob(parent)
                if pc:
                    commit_index = pc.get("index", 0) + 1
            commit_hash = self._write_commit_blob(
                collection, manifest_hash, parent=parent,
                message=message or "write: empty table",
                index=commit_index, manifest_bytes=manifest_bytes)
            self._update_caches_after_write(
                collection, manifest, manifest_hash, commit_hash, commit_index,
                is_delta=False)
            return commit_hash

        # ENCODE all row groups first (CPU, in parallel via thread pool),
        # then BATCH-WRITE all PND2 blobs in parallel (1 RTT wall-clock).
        # Previously: encode + write each RG sequentially = N × (encode + RTT).
        # Now: parallel encode + parallel write = ~1 RTT wall-clock total.
        from concurrent.futures import ThreadPoolExecutor

        def _encode_one(start_idx):
            start = start_idx
            end = min(start + row_group_size, n_rows)
            group_source = _slice_source(source, start, end)
            max_pk = key_array[end - 1]
            rg_key = _format_rg_key(max_pk)
            pnd2_bytes, col_stats = PND2.encode(
                group_source, encoding_hints=encoding_hints)
            return (rg_key, pnd2_bytes, end - start, col_stats)

        # Encode in parallel (CPU-bound, use up to 8 threads)
        starts = list(range(0, n_rows, row_group_size))
        encoded: list[tuple[str, bytes, int, list]] = [None] * len(starts)  # type: ignore[list-item]
        if len(starts) == 1:
            encoded[0] = _encode_one(starts[0])
        else:
            with ThreadPoolExecutor(max_workers=min(8, len(starts))) as pool:
                futures = {pool.submit(_encode_one, s): i
                            for i, s in enumerate(starts)}
                for f in futures:
                    idx = futures[f]
                    encoded[idx] = f.result()

        # Batch-write all PND2 blobs in parallel (1 RTT wall-clock for the
        # whole batch, was N × RTT sequential)
        pnd2_payloads = [e[1] for e in encoded]
        blob_hashes = self.kernel.write_batch(pnd2_payloads)

        for i, (rg_key, _bytes, n_rg_rows, col_stats) in enumerate(encoded):
            manifest_entries.append({
                "rg_key": rg_key,
                "blob_hash": blob_hashes[i],
                "n_rows": n_rg_rows,
                "col_stats": col_stats,
            })

        n_groups = (n_rows + row_group_size - 1) // row_group_size

        # Build the manifest LOCALLY (no I/O) so we can batch its PUT
        # with the commit blob + refs PUTs in one parallel batch.
        manifest_obj = CollectionManifest(self.kernel)
        manifest_obj.set_schema(
            columns=schema_columns,
            key_col=key_col or "",
            row_group_size=row_group_size,
            chunk_size=0,
        )
        for entry in manifest_entries:
            rg = RowGroupEntry(
                key=entry["rg_key"],
                blob_hash=entry["blob_hash"],
                n_rows=entry["n_rows"],
                storage_mode=STORAGE_WHOLE_BLOB,
            )
            for col_name, vtype, mn, mx, null_count in entry["col_stats"]:
                rg.columns.append(ColumnStatsEntry(
                    name=col_name, value_type=vtype, min=mn, max=mx,
                    null_count=null_count, chunks=[],
                ))
            manifest_obj.add_row_group(rg)

        # Build inline bloom filter for negative-lookup elimination.
        # Contains ALL individual row keys (formatted). If the formatted
        # lookup key is not in the bloom, the key definitely doesn't exist
        # in this manifest → skip the data blob fetch entirely.
        # Cost: ~12.5 KB per 10K rows. Saves 1 data GET per negative lookup.
        if key_array:
            from collection_manifest import _bloom_build
            # Format each key the same way find_row_group does
            formatted_keys = [_format_rg_key(k) for k in key_array]
            bloom_bits = _bloom_build(formatted_keys)
            manifest_obj.set_inline_bloom(bloom_bits)

        # Encode manifest locally — no I/O yet
        manifest_bytes, manifest_hash = self._encode_manifest_local(manifest_obj)
        new_manifest = manifest_obj

        # O(1) warm write: use cached HEAD + commit_index if available
        parent = self._head_cache.get(collection)
        if parent is None:
            parent = self.kernel.resolve(self._active_commit_ref(collection))
        commit_index = self._commit_index_cache.get(collection, 0)
        if commit_index == 0 and parent:
            pc = self._read_commit_blob(parent)
            if pc:
                commit_index = pc.get("index", 0) + 1

        # BATCH-PUT: pack blob (commit + manifest + optional inline data) + refs
        # = 1 RTT wall-clock. PondPack v2 can inline the data blob for
        # single-row-group writes, eliminating 1 GET on cold reads.
        import json as _json
        import time as _time
        commit = {
            "parent": parent,
            "second_parent": None,
            "manifest": manifest_hash,
            "message": message or f"unified write: {n_rows} rows in {n_groups} row groups",
            "timestamp": _time.time(),
            "index": commit_index,
        }

        # INLINE DATA OPTIMIZATION (SuperPack):
        # For writes whose total data fits in one pack, inline ALL PND2
        # data blobs into the PondPack. This eliminates the separate data
        # blob GET on cold reads, reducing point lookup from 2-3 GETs → 1 GET.
        #
        # Threshold: 4 MB. S3 GET cost is flat per-request (not per-byte),
        # so a 4 MB pack costs the same RTT as a 1 KB pack. At 10K rows ×
        # ~100 bytes/row = ~1 MB, most KV/lakehouse single-row-group
        # writes are now inlined. Previously 256 KB — too small for real
        # workloads, so the optimization rarely fired.
        #
        # Multi-row-group writes are also inlined if their TOTAL payload
        # fits under the threshold. This is correct because the pack
        # format supports multiple inline data blobs.
        _SUPERPACK_INLINE_MAX = 4 * 1024 * 1024  # 4 MB
        inline_data = None
        total_payload_size = sum(len(p) for p in pnd2_payloads)
        if total_payload_size < _SUPERPACK_INLINE_MAX:
            inline_data = pnd2_payloads

        # Build the pack: commit JSON + manifest bytes + optional inline data
        pack_bytes = encode_pack(commit, manifest_bytes, inline_data=inline_data)
        pack_hash = hash_bytes(pack_bytes)

        active = self._active_commit_ref(collection)
        manifest_ref = self._manifest_ref(collection)

        if hasattr(self.kernel, 'store') and hasattr(self.kernel.store, 'put_blob'):
            from concurrent.futures import ThreadPoolExecutor

            def _put_pack_blob():
                self.kernel.store.put_blob(pack_bytes)

            def _put_active_ref():
                self.kernel.store.put_path(active, pack_hash)

            def _put_manifest_ref():
                self.kernel.store.put_path(manifest_ref, pack_hash)

            with ThreadPoolExecutor(max_workers=3) as pool:
                f1 = pool.submit(_put_pack_blob)
                f2 = pool.submit(_put_active_ref)
                f3 = pool.submit(_put_manifest_ref)
                f1.result()
                f2.result()
                f3.result()

            self.kernel.stats["writes"] += 1  # 1 pack blob (was 2: commit + manifest)
            self.kernel.stats["ref_writes"] += 2
            self.kernel.stats["references"] += 2
            self.kernel._update_path_cache(active, pack_hash)
            self.kernel._update_path_cache(manifest_ref, pack_hash)
        else:
            # PondMinimal fallback: sequential (local disk, no RTT)
            self.kernel.write(pack_bytes)
            self.kernel.reference(active, pack_hash)
            self.kernel.reference(manifest_ref, pack_hash)

        # Update caches (don't invalidate) → next write is O(1)
        # Both commit_hash and manifest_hash map to pack_hash in storage.
        # The cache stores manifest_hash → manifest object; the path cache
        # stores ref → pack_hash. _load_manifest_from_hash handles the
        # pack → manifest extraction transparently.
        self._update_caches_after_write(
            collection, new_manifest, pack_hash, pack_hash, commit_index,
            is_delta=False)
        return pack_hash

    def append(self, collection: str, rows,
               key_col: Optional[str] = None,
               row_group_size: int = 10_000,
               encoding_hints: Optional[dict[str, str]] = None,
               message: str = "") -> str:
        """Append rows to an existing collection.

        UNIFIED with CRDT shards — no CAS, no HEAD contention.

        This method delegates to append_shard() (the CRDT shard model)
        and auto-compacts when the shard count exceeds a threshold.
        This makes it safe for multi-process use WITHOUT CAS:
          - Each process writes its own shard (no coordination)
          - Readers merge HEAD + all shards (CRDT union)
          - Auto-compaction merges shards into HEAD periodically

        For single-process use (same UnifiedStorage instance), the
        in-memory caches give O(1) warm shard writes (0 GETs).

        Args:
            collection: collection name (must already exist)
            rows: new rows to append
            key_col: sort key column
            row_group_size: rows per new row group
            encoding_hints: optional encoding hints
            message: commit message

        Returns:
            The shard manifest hash.
        """
        # Delegate to append_shard (CRDT — no CAS, no HEAD contention)
        result = self.append_shard(collection, rows, key_col=key_col,
                                     row_group_size=row_group_size,
                                     encoding_hints=encoding_hints,
                                     message=message)

        # Auto-compact when shard count exceeds threshold
        # (bounds read amplification — readers see at most N shards)
        AUTO_COMPACT_THRESHOLD = 4
        if self.shard_count(collection) >= AUTO_COMPACT_THRESHOLD:
            try:
                self.compact_shards(collection)
            except Exception:
                pass  # compaction is best-effort — shards still work

        return result

    def compact_manifest(self, collection: str) -> Optional[str]:
        """Compact a delta-manifest chain into a single flat manifest.

        Fix (Round 11 Issue #2): delta-manifests grow unbounded without
        compaction. After K appends, reads require K extra GETs to walk
        the parent chain. This method walks the chain, collects ALL row
        group entries, and writes a single flat manifest with no parent.

        Should be called periodically (e.g., after every 8 appends, or
        when the chain depth exceeds a threshold).

        Returns:
            The new (compacted) manifest hash, or None if no compaction
            was needed.
        """
        manifest = self._load_manifest(collection)
        if manifest is None or manifest.parent_manifest_hash is None:
            return None  # no delta chain to compact

        # Collect ALL row group entries.
        # Fix (Round 13 Issue #1): call scan_with_pruning() ONCE (it recurses).
        # Fix (Round 18 Issue #1): keep FIRST (NEWEST) for duplicate keys.
        # Fix (Round 19 Issue #2): when BOTH stats_tree_root AND parent_manifest_hash
        # are set, scan_with_pruning delegates to StatsTreeReader and returns
        # early — the parent chain is NOT walked. We must explicitly walk
        # the parent chain to collect ALL entries.
        all_entries: list[dict] = []
        seen_keys: dict[str, dict] = {}

        # Collect entries from the current (delta) manifest
        for rg in manifest.scan_with_pruning():
            if rg.key in seen_keys:
                continue
            seen_keys[rg.key] = {
                "rg_key": rg.key,
                "blob_hash": rg.blob_hash,
                "n_rows": rg.n_rows,
                "col_stats": [(c.name, c.value_type, c.min, c.max, c.null_count)
                                for c in rg.columns],
            }

        # Fix (Round 19 Issue #2): if the manifest has a stats_tree_root,
        # scan_with_pruning only yielded the delta's entries (from the stats
        # tree). We must ALSO walk the parent chain to get the OLD entries.
        # This is needed because scan_with_pruning early-returns when
        # stats_tree_root is set, skipping the parent_manifest_hash walk.
        if manifest.stats_tree_root and manifest.parent_manifest_hash:
            try:
                parent = self._load_manifest_from_hash(
                    manifest.parent_manifest_hash)
                for rg in parent.scan_with_pruning():
                    if rg.key in seen_keys:
                        continue  # newer version already collected
                    seen_keys[rg.key] = {
                        "rg_key": rg.key,
                        "blob_hash": rg.blob_hash,
                        "n_rows": rg.n_rows,
                        "col_stats": [(c.name, c.value_type, c.min, c.max, c.null_count)
                                        for c in rg.columns],
                    }
            except (ValueError, KeyError):
                pass  # parent not found — only use delta entries

        all_entries = list(seen_keys.values())

        # Sort by rg_key (fix from Round 9)
        all_entries.sort(key=lambda e: e["rg_key"])

        # Write a flat manifest (no parent, no stats tree)
        schema_columns = manifest.columns
        key_col = manifest.key_col
        row_group_size = manifest.row_group_size

        new_manifest = CollectionManifest(self.kernel)
        new_manifest.set_schema(
            columns=schema_columns,
            key_col=key_col,
            row_group_size=row_group_size,
            chunk_size=0,
        )
        for entry in all_entries:
            rg = RowGroupEntry(
                key=entry["rg_key"],
                blob_hash=entry["blob_hash"],
                n_rows=entry["n_rows"],
                storage_mode=STORAGE_WHOLE_BLOB,
            )
            for col_name, vtype, mn, mx, null_count in entry["col_stats"]:
                rg.columns.append(ColumnStatsEntry(
                    name=col_name, value_type=vtype,
                    min=mn, max=mx, null_count=null_count, chunks=[],
                ))
            new_manifest.add_row_group(rg)

        # P10 fix: Build stats tree on write() when the collection is large.
        # This is a writer-side operation — readers find it pre-built.
        # The build is O(N log N) but only when >25K row groups (PB scale).
        try:
            from stats_tree import should_use_stats_tree, build_stats_tree
            if should_use_stats_tree(len(all_entries)):
                stats_root = build_stats_tree(self.kernel, new_manifest.row_groups)
                new_manifest.set_stats_tree_root(stats_root)
        except ImportError:
            pass

        new_hash = new_manifest.commit()
        self.kernel.reference(self._manifest_ref(collection), new_hash)
        self._invalidate_manifest_cache(collection)
        self._invalidate_shard_cache(collection)
        return new_hash

    def _build_manifest_with_return(self, collection: str,
                         entries: list[dict],
                         schema_columns: list[tuple[str, int]],
                         key_col: str,
                         row_group_size: int,
                         parent_manifest_hash: Optional[str] = None
                         ) -> tuple[str, CollectionManifest]:
        """Build the manifest and return (hash, manifest_object).

        The manifest object is returned so callers can cache it for
        O(1) warm writes (avoids re-reading from storage on next write).

        OPTIMIZATION: The manifest ref PUT is skipped here — it's
        redundant because _write_commit_blob (called next) also writes
        the manifest ref. Saves 1 RTT per write.
        """
        manifest = CollectionManifest(self.kernel)
        manifest.set_schema(
            columns=schema_columns,
            key_col=key_col,
            row_group_size=row_group_size,
            chunk_size=0,
        )

        rg_entries: list[RowGroupEntry] = []
        for entry in entries:
            rg = RowGroupEntry(
                key=entry["rg_key"],
                blob_hash=entry["blob_hash"],
                n_rows=entry["n_rows"],
                storage_mode=STORAGE_WHOLE_BLOB,
            )
            for col_name, vtype, mn, mx, null_count in entry["col_stats"]:
                rg.columns.append(ColumnStatsEntry(
                    name=col_name, value_type=vtype, min=mn, max=mx,
                    null_count=null_count, chunks=[],
                ))
            manifest.add_row_group(rg)
            rg_entries.append(rg)

        # P10 fix: StatsTree is NOT built eagerly — lazy on first read.
        if parent_manifest_hash is not None:
            manifest.set_parent_manifest(parent_manifest_hash)

        # Encode manifest LOCALLY (no I/O) — the caller will include it
        # in a PondPack blob (commit + manifest in ONE blob).
        # This replaces manifest.commit() which wrote a separate blob.
        manifest_bytes = manifest.encode()
        manifest_hash = hash_bytes(manifest_bytes)
        return manifest_hash, manifest, manifest_bytes

    def _encode_manifest_local(self, manifest: CollectionManifest
                                 ) -> tuple[bytes, str]:
        """Encode a manifest locally (no I/O) — returns (bytes, hash).

        Used to defer the manifest blob PUT so it can be batched with
        the commit blob + refs PUTs in _write_commit_blob_with_manifest.
        """
        manifest_bytes = manifest.encode()
        manifest_hash = hash_bytes(manifest_bytes)
        return manifest_bytes, manifest_hash

    def _build_manifest(self, collection: str,
                         entries: list[dict],
                         schema_columns: list[tuple[str, int]],
                         key_col: str,
                         row_group_size: int,
                         parent_manifest_hash: Optional[str] = None
                         ) -> tuple[Optional[str], Optional[bytes]]:
        """Build the CollectionManifest for the just-written row groups.

        Encodes the manifest LOCALLY (no I/O) — returns (hash, bytes).
        The caller is responsible for writing the manifest, either as a
        standalone PMAN blob or as part of a PondPack blob (commit + manifest).
        """
        manifest = CollectionManifest(self.kernel)
        manifest.set_schema(
            columns=schema_columns,
            key_col=key_col,
            row_group_size=row_group_size,
            chunk_size=0,
        )
        for entry in entries:
            rg = RowGroupEntry(
                key=entry["rg_key"],
                blob_hash=entry["blob_hash"],
                n_rows=entry["n_rows"],
                storage_mode=STORAGE_WHOLE_BLOB,
            )
            for col_name, vtype, mn, mx, null_count in entry["col_stats"]:
                rg.columns.append(ColumnStatsEntry(
                    name=col_name,
                    value_type=vtype,
                    min=mn,
                    max=mx,
                    null_count=null_count,
                    chunks=[],
                ))
            manifest.add_row_group(rg)

        if parent_manifest_hash is not None:
            manifest.set_parent_manifest(parent_manifest_hash)

        manifest_bytes = manifest.encode()
        manifest_hash = hash_bytes(manifest_bytes)
        return manifest_hash, manifest_bytes

    # ------------------------------------------------------------------
    # READ — the ONE read path
    # ------------------------------------------------------------------

    def read(self, collection: str,
             predicates: Optional[list[tuple[str, str, Any]]] = None,
             columns: Optional[list[str]] = None,
             row_filter: Optional[Callable[[dict], bool]] = None,
             start_key: Optional[str] = None,
             end_key: Optional[str] = None,
             commit_hash: Optional[str] = None,
             manifest_hash: Optional[str] = None) -> list[dict]:
        """Read rows from a collection.

        Args:
            collection: collection name
            predicates: list of (column, op, value) tuples. All ANDed.
            columns: projection pushdown (None = all columns)
            row_filter: exact row-level filter
            start_key: range scan lower bound
            end_key: range scan upper bound
            commit_hash: (unused — use manifest_hash for time-travel)
            manifest_hash: load a specific manifest by hash (for time-travel
                and branch reads). No ref mutation, no race condition.
                Fix (Round 9 Issue #2): replaces the old swap-then-restore pattern.

        Returns:
            List of row dicts.

        Round trips: 3 + K S3 GETs cold (root pointer + root ref + manifest + K data blobs)
        """
        # Live read: if there are unmerged shards, include them via
        # read_with_shards. Time-travel queries (manifest_hash / commit_hash
        # set) skip this and read the snapshot manifest directly.
        #
        # Use _read_shard_index (TTL-cached) so the shard list is revalidated
        # periodically for multi-process safety.
        if manifest_hash is None and commit_hash is None:
            shard_hashes = self._read_shard_index(collection)
            if shard_hashes:
                return self.read_with_shards(
                    collection, predicates=predicates,
                    columns=columns, row_filter=row_filter,
                    start_key=start_key, end_key=end_key)

        manifest = self._load_manifest(collection, manifest_hash=manifest_hash)
        if manifest is None:
            return []

        # Build a combined row filter: caller's row_filter AND automatic
        # filters for predicates not handled at the encoded-eval level.
        auto_filter = self._build_predicate_filter(predicates)
        combined_filter = self._combine_filters(row_filter, auto_filter)

        # Fix (Round 13 Issue #2): ensure predicate columns are always decoded
        # even if the caller's projection doesn't include them. Without this,
        # the auto_filter sees None for predicate columns not in the projection
        # and silently filters out ALL rows.
        eff_columns = list(columns) if columns is not None else None
        if predicates and eff_columns is not None:
            pred_cols = {p[0] for p in predicates}
            missing = pred_cols - set(eff_columns)
            if missing:
                eff_columns = list(dict.fromkeys(eff_columns + list(missing)))

        # Fix (Round 14 Issue #2): apply row-level key range filter.
        # Fix (Round 15 Issue #2): properly unpad zfill-padded string keys
        # so the comparison works against actual row values.
        key_col_name = manifest.key_col
        if (start_key is not None or end_key is not None) and key_col_name:
            # Parse raw key values from the formatted "rg/..." strings
            # Strip "rg/" prefix and any zfill padding
            def _unpad_rg_key(formatted_key):
                if formatted_key is None:
                    return None
                # Fix (Round 21 Issue #2): only reverse bias encoding for
                # formatted keys (start with "rg/"). Raw numeric keys passed
                # directly by the caller should NOT have bias subtracted.
                if not isinstance(formatted_key, str) or not formatted_key.startswith("rg/"):
                    # Raw key — try int, else return as-is
                    try:
                        return int(formatted_key)
                    except (ValueError, TypeError):
                        return formatted_key
                raw = formatted_key[3:]  # strip "rg/"
                try:
                    # Formatted key — reverse the bias encoding
                    return int(raw) - _INT64_BIAS
                except (ValueError, TypeError):
                    # Non-numeric formatted string — return raw
                    return raw

            raw_start = _unpad_rg_key(start_key)
            raw_end = _unpad_rg_key(end_key)

            def range_filter(row):
                v = row.get(key_col_name)
                if v is None:
                    return False
                try:
                    # Try numeric comparison first
                    v_num = v if isinstance(v, (int, float)) else int(v)
                    if raw_start is not None and isinstance(raw_start, int) and v_num < raw_start:
                        return False
                    if raw_end is not None and isinstance(raw_end, int) and v_num > raw_end:
                        return False
                except (ValueError, TypeError):
                    # Fall back to string comparison
                    sv = str(v)
                    if raw_start is not None:
                        ss = str(raw_start)
                        if sv < ss:
                            return False
                    if raw_end is not None:
                        se = str(raw_end)
                        if sv > se:
                            return False
                return True
            combined_filter = self._combine_filters(combined_filter, range_filter)

        # Fix (Round 24 Issue #2): format raw caller keys to "rg/..." format,
        # matching what point_lookup does internally. This makes the API
        # consistent — callers can pass raw keys (int, string) without
        # needing to know about _format_rg_key.
        if start_key is not None and not (isinstance(start_key, str) and start_key.startswith("rg/")):
            start_key = _format_rg_key(start_key)
        if end_key is not None and not (isinstance(end_key, str) and end_key.startswith("rg/")):
            end_key = _format_rg_key(end_key)

        # Walk surviving row groups via manifest (in-memory pruning — 0 GETs)
        surviving = list(manifest.scan_with_pruning(predicates, start_key, end_key))
        if not surviving:
            return []

        # INLINE DATA FAST PATH: if the pack has inline data and all surviving
        # row groups are in the inline data, skip the data blob GETs entirely.
        if (manifest_hash is None and commit_hash is None and
            len(surviving) <= 1):
            # Check if we have inline data cached for the current manifest hash
            current_manifest_hash = self._manifest_hash_cache.get(collection)
            if current_manifest_hash and current_manifest_hash in self._inline_data_cache:
                inline_data = self._inline_data_cache[current_manifest_hash]
                if inline_data and len(inline_data) > 0:
                    # Decode the inline data blob directly (0 GETs for data!)
                    col_results = []
                    for blob_bytes in inline_data[:len(surviving)]:
                        col_data = self._decode_blob(blob_bytes, columns=eff_columns,
                                                      predicates=predicates)
                        col_results.append(col_data)
                else:
                    col_results = self._parallel_fetch_and_decode(
                        surviving, eff_columns, predicates)
            else:
                col_results = self._parallel_fetch_and_decode(
                    surviving, eff_columns, predicates)
        else:
            col_results = self._parallel_fetch_and_decode(
                surviving, eff_columns, predicates)

        # Fast row assembly — use zip() instead of per-row dict comprehension.
        # zip is 3-5x faster than {c: col_data[c][i] for c in col_names}
        # because it avoids N dict lookups per row.
        all_rows: list[dict] = []
        manifest_col_names = {name for name, _ in manifest.columns} if columns is None else set()
        for col_data in col_results:
            if not col_data:
                continue
            col_names = list(col_data.keys())
            # Get the column lists as a tuple for zip
            col_lists = tuple(col_data[c] for c in col_names)
            row_count = max((len(v) for v in col_lists), default=0)
            # Pad short columns with None
            padded = []
            for v in col_lists:
                if len(v) < row_count:
                    padded.append(list(v) + [None] * (row_count - len(v)))
                else:
                    padded.append(v)
            # zip-based row assembly — C-speed, no Python dict comprehension
            for values in zip(*padded):
                row = dict(zip(col_names, values))
                # Fill missing columns (schema evolution) with None
                if manifest_col_names:
                    for mc in manifest_col_names:
                        if mc not in row:
                            row[mc] = None
                if combined_filter is None or combined_filter(row):
                    if columns is not None and eff_columns != columns:
                        row = {c: row[c] for c in columns if c in row}
                    all_rows.append(row)

        return all_rows

    @staticmethod
    def _build_predicate_filter(
            predicates: Optional[list[tuple[str, str, Any]]]
            ) -> Optional[Callable[[dict], bool]]:
        """Build a row filter that applies ALL predicates.

        PND2.decode only evaluates the first predicate at the encoded
        level. This method builds a Python-level filter for ALL
        predicates (including the first, for safety — the encoded eval
        may not have been able to prune, e.g., for RAW encoding).

        Returns None if no predicates. Returns a function(row_dict) -> bool.
        """
        if not predicates:
            return None

        def filt(row: dict) -> bool:
            for col, op, val in predicates:
                row_val = row.get(col)
                if row_val is None:
                    return False  # NULL never matches
                try:
                    if op == "=" and not (row_val == val): return False
                    elif op == "!=" and not (row_val != val): return False
                    elif op == ">" and not (row_val > val): return False
                    elif op == ">=" and not (row_val >= val): return False
                    elif op == "<" and not (row_val < val): return False
                    elif op == "<=" and not (row_val <= val): return False
                    elif op == "in" and row_val not in val: return False
                    else:
                        pass  # unknown op — don't filter (safe default)
                except TypeError:
                    return False  # type mismatch — row doesn't match
            return True
        return filt

    @staticmethod
    def _combine_filters(
            f1: Optional[Callable], f2: Optional[Callable]
            ) -> Optional[Callable]:
        """Combine two row filters with AND. None = no filter."""
        if f1 is None:
            return f2
        if f2 is None:
            return f1
        def combined(row: dict) -> bool:
            return f1(row) and f2(row)
        return combined

    def read_as_columns(self, collection: str,
                         predicates: Optional[list[tuple[str, str, Any]]] = None,
                         columns: Optional[list[str]] = None,
                         commit_hash: Optional[str] = None,
                         manifest_hash: Optional[str] = None
                         ) -> dict[str, list]:
        """Read rows from a collection as column-oriented data.

        Like read(), but returns dict[col_name, list[values]] instead of
        list[dict]. Faster when the caller wants columnar data (e.g.,
        feeding into PyArrow or numpy).

        Uses PARALLEL blob fetch for surviving row groups (via thread pool).

        Fix (Round 12 Issue #1): applies _build_predicate_filter to ALL
        predicates (not just the first one that PND2.decode evaluates).
        Fix (Round 12 Issue #2): resolves commit_hash to manifest_hash.
        """
        # Fix (Round 12 Issue #2): resolve commit_hash if manifest_hash not provided
        if manifest_hash is None and commit_hash is not None:
            manifest_hash = self._resolve_commit_manifest(collection, commit_hash)

        # Live read: if there are unmerged shards, include them via a
        # shard-aware path. Time-travel queries use the snapshot manifest only.
        if manifest_hash is None and commit_hash is None:
            shard_hashes = self._read_shard_index(collection)
            if shard_hashes:
                return self._read_as_columns_with_shards(
                    collection, predicates=predicates, columns=columns,
                    shard_hashes=shard_hashes)

        manifest = self._load_manifest(collection, manifest_hash=manifest_hash)
        if manifest is None:
            return {}

        # Collect surviving row groups
        surviving = list(manifest.scan_with_pruning(predicates))
        if not surviving:
            return {}

        # Fix (Round 13 Issue #2): ensure predicate columns are always decoded
        eff_columns = list(columns) if columns is not None else None
        if predicates and eff_columns is not None:
            pred_cols = {p[0] for p in predicates}
            missing = pred_cols - set(eff_columns)
            if missing:
                eff_columns = list(dict.fromkeys(eff_columns + list(missing)))

        # PARALLEL fetch: fetch all surviving blobs concurrently.
        col_results = self._parallel_fetch_and_decode(
            surviving, eff_columns, predicates)

        # Fix (Round 12 Issue #1): apply multi-predicate filter.
        auto_filter = self._build_predicate_filter(predicates)

        # Merge column results across row groups, applying the filter
        result: dict[str, list] = {}
        for col_data in col_results:
            if auto_filter is None:
                # No filter needed — merge directly (but strip predicate-only cols)
                for col_name, values in col_data.items():
                    if columns is not None and col_name not in columns:
                        continue  # skip predicate-only columns
                    if col_name not in result:
                        result[col_name] = []
                    result[col_name].extend(values)
            else:
                # Apply filter row-by-row
                row_count = max((len(v) for v in col_data.values()), default=0)
                col_names = list(col_data.keys())
                for i in range(row_count):
                    row = {c: col_data[c][i] if i < len(col_data[c]) else None
                            for c in col_names}
                    if auto_filter(row):
                        # Only include requested columns (strip predicate-only cols)
                        out_cols = columns if columns is not None else col_names
                        for c in out_cols:
                            if c not in result:
                                result[c] = []
                            result[c].append(row.get(c))

        return result

    def _resolve_commit_manifest(self, collection: str,
                                  commit_hash: str) -> Optional[str]:
        """Resolve a commit hash to its manifest hash for time-travel reads.

        With the new manifest-based commit format, the manifest hash is
        stored directly IN the commit blob. We read the commit blob (1
        GET) and extract the "manifest" field.

        Falls back to the legacy ref-based lookup for old collections
        that used the old commit format (now unified to JSON).
        """
        # Manifest hash is in the commit blob (unified architecture)
        commit = self._read_commit_blob(commit_hash)
        if commit and commit.get("manifest"):
            return commit["manifest"]
        return None

    def _parallel_fetch_and_decode(
            self,
            row_groups: list,
            columns: Optional[list[str]],
            predicates: Optional[list[tuple[str, str, Any]]]
            ) -> list[dict[str, list]]:
        """Fetch and decode multiple row groups in parallel.

        THREE-PHASE PIPELINE with in-memory blob cache:
          Phase 1: Check cache for all blobs (0 I/O for cached)
          Phase 2: Fetch MISSING blobs in parallel via kernel.read_blob_batch
                    (1 RTT wall-clock for the whole batch via S3 thread pool)
          Phase 3: Decode MISSING blobs in parallel (CPU-bound, 8 threads)

        For small K (1-2 row groups), the thread pool overhead exceeds
        the benefit — we fall back to sequential.
        """
        if not row_groups:
            return []

        from concurrent.futures import ThreadPoolExecutor

        # Phase 1: Check cache — separate cached from uncached.
        # Only use cache for FULL decodes (columns=None, predicates=None).
        # A cached projected result would be wrong for a different projection.
        cache_eligible = (columns is None and predicates is None)
        cached_results: dict[int, dict[str, list]] = {}
        uncached_rgs: list[tuple[int, Any]] = []
        for i, rg in enumerate(row_groups):
            if cache_eligible and self._max_cache_blobs > 0 and rg.blob_hash in self._blob_cache:
                # Cache hit — move to end of LRU
                self._blob_cache_order.remove(rg.blob_hash)
                self._blob_cache_order.append(rg.blob_hash)
                cached_results[i] = self._blob_cache[rg.blob_hash]
            else:
                uncached_rgs.append((i, rg))

        # Phase 2: Fetch all uncached blobs in parallel
        if not uncached_rgs:
            # All cached — return immediately
            return [cached_results[i] for i in range(len(row_groups))]

        # Use read_blob_batch for parallel fetch (1 RTT wall-clock)
        uncached_hashes = [rg.blob_hash for _, rg in uncached_rgs]
        if len(uncached_rgs) <= 2:
            blob_bytes_list = [(uncached_rgs[i][0], self.kernel.read_blob(h))
                                for i, h in enumerate(uncached_hashes)]
        else:
            fetched_blobs = self.kernel.read_blob_batch(uncached_hashes)
            blob_bytes_list = [(uncached_rgs[i][0], fetched_blobs[i])
                                for i in range(len(uncached_rgs))]

        # Phase 3: Decode all uncached blobs in parallel
        def decode_blob(blob_bytes):
            return self._decode_blob(blob_bytes, columns=columns, predicates=predicates)

        if len(blob_bytes_list) <= 2:
            decoded = [(i, decode_blob(b)) for i, b in blob_bytes_list]
        else:
            max_decode_workers = min(8, len(blob_bytes_list))
            with ThreadPoolExecutor(max_workers=max_decode_workers) as pool:
                decoded_blobs = list(pool.map(decode_blob, [b for _, b in blob_bytes_list]))
                decoded = list(zip([i for i, _ in blob_bytes_list], decoded_blobs))

        # Cache the decoded results + assemble final output
        results: list[Optional[dict]] = [None] * len(row_groups)
        for i, col_data in decoded:
            results[i] = col_data
            # Cache the result — ONLY for full decodes (no projection, no predicates).
            # Caching projected/predicate-filtered results would break subsequent
            # reads with different projections.
            if self._max_cache_blobs > 0 and columns is None and predicates is None:
                rg_hash = uncached_rgs[[idx for idx, (orig_i, _) in enumerate(uncached_rgs) if orig_i == i][0]][1].blob_hash
                self._blob_cache[rg_hash] = col_data
                self._blob_cache_order.append(rg_hash)
                while len(self._blob_cache_order) > self._max_cache_blobs:
                    old_hash = self._blob_cache_order.pop(0)
                    self._blob_cache.pop(old_hash, None)

        for i, col_data in cached_results.items():
            results[i] = col_data

        return [r for r in results if r is not None]

    def read_as_arrow(self, collection: str,
                       predicates: Optional[list[tuple[str, str, Any]]] = None,
                       columns: Optional[list[str]] = None) -> "pa.Table":
        """Read rows as a PyArrow Table — ZERO-COPY from PND2 where possible.

        This is the FASTEST read path for tabular workloads:
          1. Manifest pruning (in-memory, 0 GETs)
          2. Parallel blob fetch (K GETs in ~1 RTT wall-clock)
          3. For INT64/FLOAT64 columns: np.frombuffer → pa.array (zero-copy)
          4. For STRING columns: pa.array from Python list (1 copy)

        Returns a pa.Table directly — no list[dict] intermediate.

        Args:
            collection: collection name
            predicates: list of (column, op, value) tuples for pruning
            columns: projection pushdown (None = all columns)

        Returns:
            A pyarrow.Table with the surviving rows.

        Round trips: 3 + K S3 GETs cold (but K blobs fetched in parallel
        → wall-clock ~3 + 1 RTT for the fetch phase).
        """
        try:
            import pyarrow as pa
        except ImportError:
            raise ImportError(
                "pyarrow is required for read_as_arrow. "
                "Install with: pip install pyarrow")

        col_data = self.read_as_columns(collection, predicates=predicates,
                                          columns=columns)
        if not col_data:
            return pa.table({})

        # Build Arrow arrays directly from column data
        arrays = []
        names = []
        for col_name, values in col_data.items():
            arrays.append(pa.array(values))
            names.append(col_name)

        return pa.Table.from_arrays(arrays, names=names)

    def point_lookup(self, collection: str, key: str,
                      columns: Optional[list[str]] = None,
                      manifest_hash: Optional[str] = None) -> Optional[dict]:
        """Point lookup — find the single row with the given key.

        Returns the row as a dict, or None if not found.

        Round trips: 2 S3 GETs (manifest + 1 data blob) — O(1) regardless
        of collection scale. When shards exist (unmerged appends) and the
        key isn't found in HEAD, falls back to a shard-aware path that
        checks each shard's manifest for a row group whose key range
        contains the target.
        """
        # Fast path: try HEAD manifest first (preserves the original
        # 4-GET cold-lookup cost for keys that live in HEAD).
        # Time-travel queries (manifest_hash set) use ONLY this path.
        head_result = self._point_lookup_head(
            collection, key, columns=columns, manifest_hash=manifest_hash)

        # Live reads: if HEAD didn't have the key AND there are unmerged
        # shards, the row may be in a shard. Search shards.
        if head_result is None and manifest_hash is None:
            shard_hashes = self._read_shard_index(collection)
            if shard_hashes:
                return self._point_lookup_with_shards(
                    collection, key, columns=columns,
                    shard_hashes=shard_hashes)

        return head_result

    def _point_lookup_head(self, collection: str, key: str,
                            columns: Optional[list[str]] = None,
                            manifest_hash: Optional[str] = None) -> Optional[dict]:
        """Point lookup against the HEAD (or manifest_hash) manifest only.

        INLINE DATA OPTIMIZATION:
        If the pack blob (read via manifest_ref) contains inline data blobs,
        we can skip the separate data blob GET entirely. The pack already
        has everything: commit + manifest + data. This reduces cold point
        lookup from 3 GETs → 2 GETs.
        """
        # If no manifest_hash specified, try the inline data fast path:
        # read the pack via manifest_ref, check if it has inline data
        if manifest_hash is None:
            manifest_ref_hash = self.kernel.resolve(self._manifest_ref(collection))
            if manifest_ref_hash is not None:
                pack_bytes = self.kernel.read_blob(manifest_ref_hash)
                if is_pack(pack_bytes):
                    _commit, manifest_bytes, inline_data = decode_pack(pack_bytes)
                    manifest = CollectionManifest.decode(self.kernel, manifest_bytes)

                    # Find the row group
                    target = _format_rg_key(key)
                    rg = manifest.find_row_group(target)
                    if rg is None:
                        return None

                    # Check if the data blob is inlined in the pack.
                    # inline_data is ordered same as manifest row groups
                    # (both sorted by key). Find the matching index.
                    if inline_data and len(inline_data) > 0:
                        rg_idx = manifest.row_group_index(rg)
                        if rg_idx is not None and rg_idx < len(inline_data):
                            blob_bytes = inline_data[rg_idx]
                        else:
                            # Fallback: use first blob (single-RG compat)
                            blob_bytes = inline_data[0]
                    else:
                        # Data not inlined — fetch separately (1 GET)
                        blob_bytes = self.kernel.read_blob(rg.blob_hash)

                    return self._decode_and_filter_row(blob_bytes, manifest, key, columns)

        # Standard path: load manifest, fetch data blob separately
        manifest = self._load_manifest(collection, manifest_hash=manifest_hash)
        if manifest is None:
            return None

        target = _format_rg_key(key)
        rg = manifest.find_row_group(target)
        if rg is None:
            return None

        blob_bytes = self.kernel.read_blob(rg.blob_hash)
        return self._decode_and_filter_row(blob_bytes, manifest, key, columns)

    def _decode_and_filter_row(self, blob_bytes: bytes,
                                manifest: CollectionManifest,
                                key: str,
                                columns: Optional[list[str]] = None) -> Optional[dict]:
        """Decode a PND2 blob and return the single row matching `key`."""
        # Use the key column (manifest.key_col) as the predicate for
        # encoded eval — this returns only the surviving row(s) that
        # match the key, not the entire row group.
        #
        # Fix (Round 2 Issue #4): the old code returned the FIRST row of
        # the row group, not the matching row. Now we decode with a
        # predicate on the key column and return the (single) match.
        # Fix (Round 14 Issue #3): always include key_col in decoded columns
        # so the predicate eval and verification work even with RAW encoding.
        key_col = manifest.key_col
        if key_col:
            # Try to coerce the key to the right type for comparison
            try:
                key_val = int(key) if key.lstrip("-").isdigit() else key
            except (ValueError, AttributeError):
                key_val = key
            # Fix (Round 14 Issue #3): ensure key_col is always decoded
            eff_columns = list(columns) if columns is not None else None
            if eff_columns is not None and key_col not in eff_columns:
                eff_columns = eff_columns + [key_col]
            col_data = self._decode_blob(blob_bytes, columns=eff_columns,
                                     predicates=[(key_col, "=", key_val)])
        else:
            col_data = self._decode_blob(blob_bytes, columns=columns)

        # Convert to row dicts and find the matching one
        row_count = max((len(v) for v in col_data.values()), default=0)
        col_names = list(col_data.keys())
        for i in range(row_count):
            row = {c: col_data[c][i] if i < len(col_data[c]) else None
                    for c in col_names}
            # Verify this row matches the key (defensive — the predicate
            # eval should have already filtered)
            if key_col and key_col in row:
                row_key = row[key_col]
                try:
                    if str(row_key) == str(key) or row_key == int(key):
                        # Fix (Round 14 Issue #3): strip key_col if not in caller's projection
                        if columns is not None and key_col not in columns:
                            row = {c: row[c] for c in columns if c in row}
                        return row
                except (ValueError, TypeError):
                    pass
            # Don't return first row as fallback — that's a bug (R2 fix)
        return None

    def _point_lookup_with_shards(self, collection: str, key: str,
                                    columns: Optional[list[str]],
                                    shard_hashes: list[str]) -> Optional[dict]:
        """Point lookup against HEAD + unmerged shards.

        Searches each shard manifest in parallel for a row group whose key
        range contains the target key. If found, fetches only that row group
        blob and decodes with a key predicate. Falls back to HEAD manifest
        if no shard contains the key.

        Cost: O(shard_count) manifest GETs (parallel, ~1 RTT) + 1 data GET.
        At low shard counts (<16), this is competitive with the no-shard
        path. Compaction keeps shard counts low in steady state.
        """
        # Coerce key to int if numeric (matches point_lookup behavior)
        try:
            key_val = int(key) if key.lstrip("-").isdigit() else key
        except (ValueError, AttributeError):
            key_val = key

        target = _format_rg_key(key)

        # Load HEAD manifest for schema/key_col (needed for inline shards)
        head_manifest = self._load_manifest(collection)
        head_schema = head_manifest.columns if head_manifest else None
        head_key_col = head_manifest.key_col if head_manifest else ""

        # Search shards in parallel for the row group containing this key
        shard_manifests = self._parallel_fetch_shard_manifests(
            shard_hashes,
            schema_columns=head_schema, key_col=head_key_col)

        # Find a candidate row group from any shard
        candidate_blob_hash = None
        candidate_key_col = None
        for sm in shard_manifests:
            rg = sm.find_row_group(target)
            if rg is not None:
                # Verify the key is actually within this rg's column stats
                # (defensive — find_row_group uses formatted key ordering)
                candidate_blob_hash = rg.blob_hash
                candidate_key_col = sm.key_col
                break

        # If not found in shards, try HEAD manifest
        if candidate_blob_hash is None:
            # head_manifest was loaded above for schema/key_col — reuse it
            if head_manifest is None:
                return None
            rg = head_manifest.find_row_group(target)
            if rg is None:
                return None
            candidate_blob_hash = rg.blob_hash
            candidate_key_col = head_manifest.key_col

        if candidate_blob_hash is None:
            return None

        # Fetch + decode with key predicate
        blob_bytes = self.kernel.read_blob(candidate_blob_hash)
        if not blob_bytes:
            return None
        key_col = candidate_key_col
        if key_col:
            eff_columns = list(columns) if columns is not None else None
            if eff_columns is not None and key_col not in eff_columns:
                eff_columns = eff_columns + [key_col]
            col_data = self._decode_blob(blob_bytes, columns=eff_columns,
                                     predicates=[(key_col, "=", key_val)])
        else:
            col_data = self._decode_blob(blob_bytes, columns=columns)

        row_count = max((len(v) for v in col_data.values()), default=0)
        col_names = list(col_data.keys())
        for i in range(row_count):
            row = {c: col_data[c][i] if i < len(col_data[c]) else None
                    for c in col_names}
            if key_col and key_col in row:
                row_key = row[key_col]
                try:
                    if str(row_key) == str(key) or row_key == int(key):
                        if columns is not None and key_col not in columns:
                            row = {c: row[c] for c in columns if c in row}
                        return row
                except (ValueError, TypeError):
                    pass
        return None

    def scan_with_pruning(self, collection: str,
                           predicates: Optional[list[tuple[str, str, Any]]] = None,
                           manifest_hash: Optional[str] = None
                           ) -> Iterator[tuple[str, str, dict]]:
        """Low-level scan — yields (rg_key, blob_hash, stats_dict) for
        surviving row groups. The caller fetches and decodes the blobs.

        Useful for batch processing or when the caller wants to control
        the decode step.
        """
        manifest = self._load_manifest(collection, manifest_hash=manifest_hash)
        if manifest is None:
            return

        for rg in manifest.scan_with_pruning(predicates):
            stats_dict = {c.name: (c.min, c.max, c.null_count)
                           for c in rg.columns}
            yield (rg.key, rg.blob_hash, stats_dict)

    def iter_rows(self, collection: str,
                  predicates: Optional[list[tuple[str, str, Any]]] = None,
                  columns: Optional[list[str]] = None,
                  batch_size: int = 1000,
                  manifest_hash: Optional[str] = None
                  ) -> Iterator[list[dict]]:
        """Streaming read — yields rows in batches without loading all into memory.

        This is the MEMORY-SAFE read path for large collections. Instead of
        returning list[dict] (which OOMs at 1B rows), this generator yields
        batches of `batch_size` rows at a time.

        Each batch is fetched from one row group (or a slice of one), decoded,
        and yielded. The caller processes the batch and discards it before the
        next batch is fetched.

        Args:
            collection: collection name
            predicates: list of (column, op, value) tuples for pruning
            columns: projection pushdown (None = all columns)
            batch_size: rows per batch (default 1000). Actual batch size
                may be larger if row groups are larger than batch_size.
            manifest_hash: for time-travel reads

        Yields:
            Lists of row dicts (batch_size rows at a time).

        Round trips: 3 + K S3 GETs cold (same as read()), but memory usage
        is O(batch_size) instead of O(total_rows).
        """
        manifest = self._load_manifest(collection, manifest_hash=manifest_hash)
        if manifest is None:
            return

        auto_filter = self._build_predicate_filter(predicates)

        # Fix (Round 14 Issue #1): ensure predicate columns are always decoded
        # even if not in the caller's projection (same fix as read()/read_as_columns())
        eff_columns = list(columns) if columns is not None else None
        if predicates and eff_columns is not None:
            pred_cols = {p[0] for p in predicates}
            missing = pred_cols - set(eff_columns)
            if missing:
                eff_columns = list(dict.fromkeys(eff_columns + list(missing)))

        for rg in manifest.scan_with_pruning(predicates):
            blob_bytes = self.kernel.read_blob(rg.blob_hash)
            col_data = self._decode_blob(blob_bytes, columns=eff_columns,
                                     predicates=predicates)

            row_count = max((len(v) for v in col_data.values()), default=0)
            col_names = list(col_data.keys())

            # Yield in batches
            for start in range(0, row_count, batch_size):
                end = min(start + batch_size, row_count)
                batch = []
                for i in range(start, end):
                    row = {c: col_data[c][i] if i < len(col_data[c]) else None
                            for c in col_names}
                    if auto_filter is None or auto_filter(row):
                        # Strip predicate-only columns from the result
                        if columns is not None and eff_columns != columns:
                            row = {c: row[c] for c in columns if c in row}
                        batch.append(row)
                if batch:
                    yield batch

    def iter_columns(self, collection: str,
                      predicates: Optional[list[tuple[str, str, Any]]] = None,
                      columns: Optional[list[str]] = None,
                      manifest_hash: Optional[str] = None
                      ) -> Iterator[dict[str, list]]:
        """Columnar streaming read — yields one dict[col, list[vals]] per row group.

        This is the FAST PATH for full scans — skips row dict assembly
        entirely. Each yield is a columnar batch (one row group's worth
        of data). The caller can:
          - Feed directly to DuckDB/Arrow (zero-copy)
          - Process in columnar fashion (faster than row-by-row)
          - Skip unwanted columns (projection pushdown)

        This is 3-5x faster than iter_rows for large scans because it
        avoids the O(N) dict creation that dominates Python CPU time.

        Yields:
            dict[col_name, list[values]] — one per row group
        """
        manifest = self._load_manifest(collection, manifest_hash=manifest_hash)
        if manifest is None:
            return

        eff_columns = list(columns) if columns is not None else None
        if predicates and eff_columns is not None:
            pred_cols = {p[0] for p in predicates}
            missing = pred_cols - set(eff_columns)
            if missing:
                eff_columns = list(dict.fromkeys(eff_columns + list(missing)))

        # Fetch ALL blobs in parallel first, then yield columnar batches
        # This is critical for object stores — sequential fetch is K × RTT
        row_groups = list(manifest.scan_with_pruning(predicates))
        if not row_groups:
            return

        col_results = self._parallel_fetch_and_decode(
            row_groups, eff_columns, predicates)

        for col_data in col_results:
            if col_data:
                yield col_data


# ---------------------------------------------------------------------------
# Helpers — value encoding for PND2 stats
# ---------------------------------------------------------------------------

def _encode_pnd2_value(value_type: int, value: Any) -> bytes:
    """Encode a single min/max value as binary bytes (PND2 stats section)."""
    if value_type == VALUE_TYPE_INT64:
        return struct.pack("<q", int(value))
    if value_type == VALUE_TYPE_FLOAT64:
        return struct.pack("<d", float(value))
    if value_type == VALUE_TYPE_STRING:
        s = str(value).encode("utf-8")
        return struct.pack("<I", len(s)) + s
    if value_type == VALUE_TYPE_BINARY:
        b = bytes(value) if not isinstance(value, bytes) else value
        return struct.pack("<I", len(b)) + b
    return b""


def _decode_pnd2_value(value_type: int, data: bytes, pos: int) -> tuple[Any, int]:
    """Decode a single min/max value from PND2 stats section."""
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
    if value_type == VALUE_TYPE_BINARY:
        slen = struct.unpack("<I", data[pos:pos+4])[0]
        pos += 4
        b = bytes(data[pos:pos+slen])
        return b, pos + slen
    return None, pos


# ---------------------------------------------------------------------------
# Helpers — BINARY value type + source slicing/sorting + key formatting
# ---------------------------------------------------------------------------

# Row group key format: "rg/" + zero-padded numeric key.
# Padding to 20 digits supports up to 10^20 row groups — far beyond any
# realistic workload (1 PB at 100 MB/row group = 10^7 row groups).
# Without padding, lexicographic comparison breaks: "rg/9" > "rg/42"
# because "9" > "4". This silently corrupts point_lookup and range scans
# for collections with >10 row groups.
_RG_KEY_WIDTH = 20

# Fix (Round 20 Issue #1): bias for negative INT64 keys.
# f"{-3:020d}" = "-0000000000000000003" which sorts REVERSE lexicographically
# vs positive numbers. Fix: add INT64_MAX bias so all keys are non-negative.
# -3 → (2^63 - 1) + (-3) = 9223372036854775804 → "rg/09223372036854775804"
# This preserves numeric order in lexicographic comparison.
_INT64_BIAS = 2**63 - 1

def _format_rg_key(max_pk: Any) -> str:
    """Format a row group key with zero-padding for correct lexicographic ordering.

    For numeric keys: bias-encode (add INT64_MAX) then zero-pad to 20 digits.
    This ensures negative numbers sort correctly: -3 < -1 < 0 < 5 < 42.
    For string keys: "rg/" + key (no padding — strings compared as-is)
    For float keys: "rg/" + str(float) (string comparison, prefer INT64)

    Fix (Round 20 Issue #1): negative INT64 keys now sort correctly via
    bias encoding. Previously f"{-3:020d}" = "-000...3" sorted AFTER
    positive numbers lexicographically (because "-" > "0" in ASCII).
    """
    if isinstance(max_pk, int):
        return f"rg/{(max_pk + _INT64_BIAS):0{_RG_KEY_WIDTH}d}"
    if isinstance(max_pk, float):
        return f"rg/{max_pk}"
    try:
        return f"rg/{(int(max_pk) + _INT64_BIAS):0{_RG_KEY_WIDTH}d}"
    except (ValueError, TypeError):
        # Non-numeric string key — use as-is (caller sorted lexicographically)
        return f"rg/{max_pk}"


def _detect_value_type_with_binary(values: list) -> int:
    """Detect value type, including BINARY for raw bytes."""
    for v in values:
        if v is None:
            continue
        if isinstance(v, bool):
            return VALUE_TYPE_INT64
        if isinstance(v, int):
            return VALUE_TYPE_INT64
        if isinstance(v, float):
            return VALUE_TYPE_FLOAT64
        if isinstance(v, bytes):
            return VALUE_TYPE_BINARY
        return VALUE_TYPE_STRING  # default to string
    return VALUE_TYPE_NULL


def _encode_binary_raw(values: list, hint: str = "raw") -> tuple[bytes, dict]:
    """Encode a BINARY column as raw bytes (no RLE/DICT/BITPACK).

    Layout (after the 9-byte EncodingHeader):
      n_values(4B) + [length(4B) + bytes] * n_values
    """
    n_rows = len(values)
    payload = struct.pack("<I", n_rows)
    for v in values:
        if v is None:
            payload += struct.pack("<I", 0xFFFFFFFF)  # null sentinel
        else:
            b = v if isinstance(v, bytes) else bytes(v)
            payload += struct.pack("<I", len(b))
            payload += b

    header = EncodingHeader(ColumnEncoding.RAW, n_rows).to_bytes()
    meta = {"encoding": "raw", "n_rows": n_rows, "value_type": VALUE_TYPE_BINARY,
            "payload_size": len(payload)}
    return header + payload, meta


def _decode_binary_raw(payload: bytes, expected_n_rows: int) -> list:
    """Decode a BINARY column's raw payload.

    Layout: n_values(4B) + [length(4B) + bytes] * n_values

    Args:
        payload: the column's payload bytes (after the PND2 schema/stats
            sections, NOT including any PND1 header)
        expected_n_rows: the declared n_rows from the PND2 header

    Returns:
        List of values (bytes or None for nulls).
    """
    if len(payload) < 4:
        return []
    n_values = struct.unpack("<I", payload[:4])[0]
    pos = 4
    result = []
    for _ in range(n_values):
        if pos + 4 > len(payload):
            break
        (blen,) = struct.unpack("<I", payload[pos:pos+4])
        pos += 4
        if blen == 0xFFFFFFFF:
            result.append(None)  # null sentinel
        elif blen == 0:
            result.append(b"")  # empty bytes (not null)
        else:
            result.append(bytes(payload[pos:pos+blen]))
            pos += blen
    # Pad with None if we ran out of data (defensive)
    while len(result) < expected_n_rows:
        result.append(None)
    return result


def _binary_value_matches(val: Any, op: str, target: Any) -> bool:
    """Check if a BINARY value matches a predicate.

    Supports =, !=, and "in" (target is a list of bytes). Other ops
    return True (can't prune — caller should not filter).
    """
    if op == "=":
        if val is None or target is None:
            return val is None and target is None
        if isinstance(target, str):
            target = target.encode("utf-8")
        return val == target
    if op == "!=":
        if val is None or target is None:
            return not (val is None and target is None)
        if isinstance(target, str):
            target = target.encode("utf-8")
        return val != target
    if op == "in":
        if val is None:
            return False
        targets = [t.encode("utf-8") if isinstance(t, str) else t
                    for t in target]
        return val in targets
    # Unknown op — don't filter (return True so the row survives)
    return True


def _slice_source(source: ColumnSource, start: int, end: int) -> ColumnSource:
    """Slice a ColumnSource — returns a new source with rows [start, end)."""
    # For PyArrowColumnSource, we can slice the underlying table
    if isinstance(source, PyArrowColumnSource):
        return PyArrowColumnSource(source._table.slice(start, end - start))
    # For ListColumnSource, slice the rows list
    if isinstance(source, ListColumnSource):
        return ListColumnSource(source._rows[start:end])
    # Fallback: wrap in a SlicedSource
    return _SlicedSource(source, start, end)


class _SlicedSource:
    """A slice of a ColumnSource — used when the source doesn't natively support slicing."""
    def __init__(self, parent: ColumnSource, start: int, end: int):
        self._parent = parent
        self._start = start
        self._end = end

    def column_names(self) -> list[str]:
        return self._parent.column_names()

    def num_rows(self) -> int:
        return self._end - self._start

    def column_slice(self, name: str, start: int, end: int) -> list:
        return self._parent.column_slice(name,
                                           self._start + start,
                                           self._start + end)

    def column_stats(self, name: str) -> tuple:
        values = self.column_slice(name, 0, self.num_rows())
        return compute_list_stats(values)


def _sort_source_by(source: ColumnSource, key_col: str) -> ColumnSource:
    """Sort a ColumnSource by a column — returns a new sorted source."""
    # For PyArrowColumnSource, use PyArrow's sort_by
    if isinstance(source, PyArrowColumnSource):
        return PyArrowColumnSource(source._table.sort_by(key_col))
    # For ListColumnSource, sort in Python
    if isinstance(source, ListColumnSource):
        rows = sorted(source._rows, key=lambda r: (r.get(key_col) is None, r.get(key_col)))
        return ListColumnSource(rows)
    # Fallback: read all rows, sort, wrap in ListColumnSource
    n = source.num_rows()
    col_names = source.column_names()
    rows = []
    for i in range(n):
        row = {c: source.column_slice(c, i, i+1)[0] for c in col_names}
        rows.append(row)
    rows.sort(key=lambda r: (r.get(key_col) is None, r.get(key_col)))
    return ListColumnSource(rows)
