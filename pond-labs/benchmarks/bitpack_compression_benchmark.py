#!/usr/bin/env python3
"""
Benchmark: Real bitpacking compression ratio + speed.

Verifies that the new bitpack encoding actually compresses data (the
old version stored offset values as a JSON list — no compression).

Tests:
  1. Round-trip correctness (encode → decode → original)
  2. Compression ratio vs JSON list (the old format)
  3. Compression ratio vs raw int64 bytes
  4. Predicate eval speed (O(1) min/max prune)
  5. Decode speed

Run:
    python pond-labs/benchmarks/bitpack_compression_benchmark.py
"""

from __future__ import annotations

import os
import sys
import json
import time
import struct

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(os.path.dirname(HERE))
sys.path.insert(0, os.path.join(REPO, "bindings/python/sdk", "extensions", "physical_structures"))

from encoding import (
    encode_bitpack, encode_raw, decode_column, eval_predicate_encoded,
    EncodingHeader, ColumnEncoding,
)


def benchmark_round_trip_and_compression():
    """Verify round-trip + measure compression ratio."""
    print("=" * 70)
    print("Bitpack: round-trip + compression ratio")
    print("=" * 70)

    test_cases = [
        ("ages 0-120", [i % 121 for i in range(10_000)]),       # bitwidth=7
        ("status codes 0-5", [i % 6 for i in range(10_000)]),    # bitwidth=3
        ("small ints 0-255", [i % 256 for i in range(10_000)]),  # bitwidth=8
        ("int16 range 0-1000", [i % 1001 for i in range(10_000)]),  # bitwidth=10
        ("constant (all 42)", [42] * 10_000),                    # bitwidth=1
    ]

    for name, values in test_cases:
        # Encode
        encoded, meta = encode_bitpack(values)
        encoded_size = len(encoded)

        # Decode + verify
        decoded = decode_column(encoded)
        assert decoded == values, f"{name}: round-trip failed"

        # Compare to old format (JSON list of offset values)
        offset = min(values)
        old_json = json.dumps({
            "bitwidth": meta["bitwidth"],
            "offset": offset,
            "min": min(values),
            "max": max(values),
            "packed": [v - offset for v in values],
        }).encode()
        old_size = len(old_json) + 9  # +9 for EncodingHeader

        # Compare to raw int64 bytes
        raw_int64_size = len(values) * 8

        ratio_vs_old = old_size / encoded_size
        ratio_vs_raw = raw_int64_size / encoded_size

        print(f"\n  {name} (n={len(values):,}, bitwidth={meta['bitwidth']}):")
        print(f"    Bitpack (new):     {encoded_size:>8,} bytes")
        print(f"    JSON list (old):   {old_size:>8,} bytes  →  {ratio_vs_old:.2f}x compression")
        print(f"    Raw int64:         {raw_int64_size:>8,} bytes  →  {ratio_vs_raw:.2f}x compression")
        print(f"    Round-trip:        OK")


def benchmark_predicate_eval():
    """Measure O(1) predicate eval speed via min/max in sub-header."""
    print("\n" + "=" * 70)
    print("Bitpack: O(1) predicate eval via min/max sub-header")
    print("=" * 70)

    values = list(range(10_000))  # bitwidth=14
    encoded, _ = encode_bitpack(values)

    # Predicate: value > 99999 (out of range — should fully prune in O(1))
    # N_RUNS is calibrated: measure one eval, then size the run so the
    # measured section stays ~<= 2s. Without numpy the pure-Python fallback
    # is 50-100x slower — a fixed 10_000-run budget was 291s on CI runners
    # (97% of the test's 300s subprocess timeout; flaked over the line on
    # slower machines). Calibration keeps the benchmark meaningful on ANY
    # hardware instead of timing out on the slow path.
    def _calibrated_runs(fn, budget_s: float = 2.0, max_runs: int = 10_000) -> int:
        t0 = time.perf_counter()
        fn()
        one = time.perf_counter() - t0
        if one <= 0:
            return max_runs
        return max(1, min(max_runs, int(budget_s / one)))

    n_prune = _calibrated_runs(lambda: eval_predicate_encoded(encoded, "x", ">", 99_999))
    t0 = time.perf_counter()
    for _ in range(n_prune):
        ranges, _ = eval_predicate_encoded(encoded, "x", ">", 99_999)
    elapsed_us = (time.perf_counter() - t0) * 1_000_000 / n_prune
    assert ranges == [], "Should fully prune"
    print(f"\n  Predicate 'x > 99999' (out of range, fully pruned):")
    print(f"    {elapsed_us:.2f} µs per eval (O(1) — reads 16 bytes from sub-header)")
    print(f"    {n_prune:,} evals in {(elapsed_us * n_prune / 1_000_000):.2f}s")

    # Predicate: value > 5000 (in range — vectorized scan yields matching positions)
    n_scan = _calibrated_runs(lambda: eval_predicate_encoded(encoded, "x", ">", 5_000),
                              budget_s=2.0)
    t0 = time.perf_counter()
    for _ in range(n_scan):
        ranges, _ = eval_predicate_encoded(encoded, "x", ">", 5_000)
    elapsed_us = (time.perf_counter() - t0) * 1_000_000 / n_scan
    assert ranges == [(5001, 10_000)], f"Should return matching range, got {ranges}"
    print(f"\n  Predicate 'x > 5000' (in range, vectorized scan):")
    print(f"    {elapsed_us:.2f} µs per eval (numpy vectorized — scan + compare + coalesce)")
    print(f"    {n_scan:,} evals in {(elapsed_us * n_scan / 1_000_000):.2f}s")
    print(f"    Returns [(5001, 10000)] — 4999 surviving rows (Vortex-style: no full decode)")


def benchmark_decode_speed():
    """Measure decode speed."""
    print("\n" + "=" * 70)
    print("Bitpack: decode speed")
    print("=" * 70)

    values = list(range(10_000))
    encoded, _ = encode_bitpack(values)

    # Calibrated: bounded total time regardless of numpy presence/hardware.
    t0 = time.perf_counter()
    decode_column(encoded)
    one = time.perf_counter() - t0
    n_runs = max(1, min(100, int(2.0 / one))) if one > 0 else 100
    t0 = time.perf_counter()
    for _ in range(n_runs):
        decoded = decode_column(encoded)
    elapsed_ms = (time.perf_counter() - t0) * 1000 / n_runs
    assert decoded == values

    print(f"\n  Decode 10,000 int16 values (bitwidth=14):")
    print(f"    {elapsed_ms:.2f} ms per decode")
    print(f"    {10_000 / (elapsed_ms / 1000):,.0f} values/sec")
    print(f"    (Pure Python bit-twiddling — a C extension would be 50-100x faster)")


if __name__ == "__main__":
    benchmark_round_trip_and_compression()
    benchmark_predicate_eval()
    benchmark_decode_speed()
    print("\n" + "=" * 70)
    print("ALL BITPACK BENCHMARKS PASSED")
    print("=" * 70)
