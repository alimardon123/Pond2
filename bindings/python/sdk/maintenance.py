"""
Pond SDK maintenance operations — Layer 0.5.

This module implements the maintenance operations defined in RFC-0008
(Deletion as Data):

  - TOMBSTONE_HASH: a fixed, globally-known hash that signals
    "this name has been logically deleted." It is the SHA-256 of
    a constant marker blob.
  - drop_name(kernel, name): rebind a name to TOMBSTONE_HASH. This
    is the Layer 1 "delete" operation. Idempotent.
  - is_dropped(kernel, name): True iff name is bound to TOMBSTONE_HASH.
  - resolve_active(kernel, name): resolve a name, returning None for
    unbound OR tombstoned names. This is what Lens code should call
    instead of kernel.resolve() when it wants "active names only."
  - compact_tombstones(kernel): physical maintenance — remove rows
    from the kernel's root namespace whose current binding is
    TOMBSTONE_HASH. This is the Layer 0.5 analog of VACUUM / git gc.
    Idempotent.

These are NOT kernel primitives. The kernel stays at 3 primitives
(Write, Read, Reference). Tombstones are data — a Layer 1 convention.
The kernel does not know TOMBSTONE_HASH is special.

Compatibility with existing PondGC (engineering/02_gc.py):
  - drop_name makes the previously-pointed-to blob unreachable.
  - PondGC.collect() will then sweep that blob on its next run.
  - compact_tombstones only removes the name-row from the roots
    SQLite table (~80 bytes per name); it does not touch blobs.
  - The two operations are complementary and orthogonal.
"""

from __future__ import annotations

import hashlib
from typing import Optional

# ---------------------------------------------------------------------------
# TOMBSTONE_HASH — globally-known marker for logically-deleted names.
# ---------------------------------------------------------------------------

# The marker blob is a constant. Its SHA-256 IS the tombstone hash.
# We write this blob to the kernel on first use (idempotent — content
# addressing means re-writing it is a no-op).
_TOMBSTONE_MARKER = b"__pond_tombstone__"
TOMBSTONE_HASH: str = hashlib.sha256(_TOMBSTONE_MARKER).hexdigest()


def _ensure_tombstone_blob(kernel) -> None:
    """Ensure the tombstone marker blob exists in the kernel's object
    store. Idempotent — content addressing means re-writing produces
    the same hash and is a no-op on disk."""
    # kernel.write is content-addressed; calling it with the same bytes
    # is always safe and returns TOMBSTONE_HASH.
    h = kernel.write(_TOMBSTONE_MARKER)
    assert h == TOMBSTONE_HASH, (
        f"kernel.write(tombstone marker) returned {h}, expected {TOMBSTONE_HASH}. "
        "The kernel's hash function does not match the SDK's expectation."
    )


# ---------------------------------------------------------------------------
# Layer 1: logical deletion (drop_name, is_dropped, resolve_active)
# ---------------------------------------------------------------------------

def drop_name(kernel, name: str) -> None:
    """Logically delete a name by rebinding it to TOMBSTONE_HASH.

    Idempotent: calling drop_name on an already-tombstoned name is a
    no-op (it re-binds to the same hash).

    After drop_name:
      - kernel.resolve(name) returns TOMBSTONE_HASH (the binding exists).
      - is_dropped(kernel, name) returns True.
      - resolve_active(kernel, name) returns None.
      - PondGC.collect() will sweep the previously-pointed-to blob on
        its next run (the blob is no longer reachable from any name).

    This operation does NOT remove the name's row from the kernel's
    root namespace. Use compact_tombstones() for that (Layer 0.5
    maintenance, see RFC-0008 §3).
    """
    _ensure_tombstone_blob(kernel)
    kernel.reference(name, TOMBSTONE_HASH)


def is_dropped(kernel, name: str) -> bool:
    """True iff name is bound to TOMBSTONE_HASH.

    Returns False for:
      - Names that are bound to a non-tombstone hash.
      - Names that are not bound at all (resolve returns None).
    """
    h = kernel.resolve(name)
    return h == TOMBSTONE_HASH


def resolve_active(kernel, name: str) -> Optional[str]:
    """Resolve a name to its hash, returning None for unbound OR
    tombstoned names.

    This is the function Lens code should call when it wants "active
    names only." Use kernel.resolve() directly only when you need to
    distinguish "unbound" from "tombstoned" (rare).
    """
    h = kernel.resolve(name)
    if h is None or h == TOMBSTONE_HASH:
        return None
    return h


# ---------------------------------------------------------------------------
# Layer 0.5: physical maintenance (compact_tombstones)
# ---------------------------------------------------------------------------

def compact_tombstones(kernel) -> dict:
    """Remove rows from the kernel's root namespace whose current
    binding is TOMBSTONE_HASH.

    This is a Layer 0.5 maintenance operation, analogous to VACUUM in
    PostgreSQL or `git gc` in Git. It is:
      - Idempotent: running twice has the same effect as once.
      - Safe: only removes names already marked deleted; no surprise
        data loss.
      - Optional: the system is correct without it; it only reclaims
        ~80 bytes of name-row storage per tombstoned name.

    After compact_tombstones:
      - kernel.resolve(tombstoned_name) returns None (the row is gone).
      - is_dropped(kernel, tombstoned_name) returns False (the name is
        no longer bound at all, which is different from being tombstoned).
      - The previously-pointed-to blob (already unreachable) is unchanged.
        Use PondGC.collect() to reclaim blob storage.

    Returns a dict with stats:
      {"compacted": int, "remaining_names": int}
    """
    names = kernel.list_names()
    compacted = 0
    for name in names:
        if kernel.resolve(name) == TOMBSTONE_HASH:
            # Kernel maintenance primitive — delete_reference() takes the
            # kernel's _db_lock (raw SQL against kernel.root_db raced with
            # concurrent resolve()/close() from background threads).
            if kernel.delete_reference(name):
                compacted += 1
    return {
        "compacted": compacted,
        "remaining_names": len(kernel.list_names()),
    }


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def _test_tombstone_round_trip():
    """Verify the tombstone marker blob hashes to TOMBSTONE_HASH."""
    import os
    import shutil
    import sys
    sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "bindings/python/core"))
    sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "bindings/python/core"))
    from kernel import PondMinimal

    bench_dir = "/tmp/pond_tombstone_test"
    if os.path.exists(bench_dir):
        shutil.rmtree(bench_dir)
    os.makedirs(bench_dir)
    kernel = PondMinimal(bench_dir)

    # Initially, no names are tombstoned.
    assert not is_dropped(kernel, "missing"), "Unbound name should not be 'dropped'"

    # Create a name, then drop it.
    blob_h = kernel.write(b"some data")
    kernel.reference("myname", blob_h)
    assert resolve_active(kernel, "myname") == blob_h
    assert not is_dropped(kernel, "myname")

    drop_name(kernel, "myname")

    # After drop:
    # - kernel.resolve still returns a hash (TOMBSTONE_HASH).
    # - is_dropped returns True.
    # - resolve_active returns None.
    assert kernel.resolve("myname") == TOMBSTONE_HASH, "drop_name should rebind to TOMBSTONE_HASH"
    assert is_dropped(kernel, "myname") is True
    assert resolve_active(kernel, "myname") is None

    # Idempotent: drop again, state unchanged.
    drop_name(kernel, "myname")
    assert kernel.resolve("myname") == TOMBSTONE_HASH
    assert is_dropped(kernel, "myname") is True

    # compact_tombstones removes the row.
    stats = compact_tombstones(kernel)
    assert stats["compacted"] == 1, f"Expected 1 compaction, got {stats}"
    assert kernel.resolve("myname") is None, "After compact, name should be unbound"
    assert is_dropped(kernel, "myname") is False, "After compact, name is unbound, not 'dropped'"

    # compact_tombstones is idempotent.
    stats2 = compact_tombstones(kernel)
    assert stats2["compacted"] == 0, f"Second compact should be no-op, got {stats2}"

    kernel.close()
    shutil.rmtree(bench_dir, ignore_errors=True)
    print("PASS: tombstone round-trip (drop -> is_dropped -> resolve_active -> compact)")


def _test_drop_does_not_affect_other_names():
    """Dropping one name does not affect other names."""
    import os
    import shutil
    import sys
    sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "bindings/python/core"))
    sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "bindings/python/core"))
    from kernel import PondMinimal

    bench_dir = "/tmp/pond_tombstone_isolation"
    if os.path.exists(bench_dir):
        shutil.rmtree(bench_dir)
    os.makedirs(bench_dir)
    kernel = PondMinimal(bench_dir)

    h1 = kernel.write(b"data1")
    h2 = kernel.write(b"data2")
    kernel.reference("name1", h1)
    kernel.reference("name2", h2)

    drop_name(kernel, "name1")

    assert resolve_active(kernel, "name1") is None
    assert resolve_active(kernel, "name2") == h2, "Dropping name1 must not affect name2"

    kernel.close()
    shutil.rmtree(bench_dir, ignore_errors=True)
    print("PASS: drop isolation (dropping one name does not affect others)")


def _test_tombstone_composes_with_PondGC():
    """Verify that tombstoning + PondGC sweeps the previously-pointed-to blob."""
    import os
    import shutil
    import sys
    sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "bindings/python/core"))
    sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "bindings/python/core"))
    sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "engineering"))
    from kernel import PondMinimal

    # PondGC is in engineering/02_gc.py — import lazily so this test
    # only runs when that file is present.
    try:
        from importlib import util as _util
        spec = _util.spec_from_file_location(
            "pond_gc",
            os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "engineering", "02_gc.py"),
        )
        pond_gc_mod = _util.module_from_spec(spec)
        spec.loader.exec_module(pond_gc_mod)
        PondGC = pond_gc_mod.PondGC
    except Exception as e:
        print(f"SKIP: PondGC not available ({e})")
        return

    bench_dir = "/tmp/pond_tombstone_gc"
    if os.path.exists(bench_dir):
        shutil.rmtree(bench_dir)
    os.makedirs(bench_dir)
    kernel = PondMinimal(bench_dir)

    # Write a real blob and reference it.
    real_h = kernel.write(b"real data that should be GC'd after tombstone")
    kernel.reference("temp_name", real_h)

    # Add the tombstone marker blob (created by drop_name later).
    # Verify state before GC.
    stats_before = kernel.storage_stats()
    blobs_before = stats_before["blob_count"]
    assert blobs_before >= 1, "Should have at least the real blob"

    # Drop the name.
    drop_name(kernel, "temp_name")

    # The real blob is now unreachable (the only name points to TOMBSTONE_HASH).
    # Run PondGC — it should sweep real_h.
    gc = PondGC(kernel)
    gc_result = gc.collect()

    # TOMBSTONE_HASH itself is "reachable" (it appears as a binding for temp_name)
    # but PondGC reads the tombstone marker blob, finds no embedded hashes, and
    # marks TOMBSTONE_HASH. The real_h blob is not marked and gets swept.
    assert gc_result["orphaned_deleted"] >= 1, (
        f"PondGC should sweep the tombstoned blob, got: {gc_result}"
    )

    kernel.close()
    shutil.rmtree(bench_dir, ignore_errors=True)
    print("PASS: tombstone + PondGC composition (tombstoned blob is swept)")


def _run_all_tests():
    print("=== RFC-0008 Tombstone Helpers — Test Suite ===\n")
    _test_tombstone_round_trip()
    _test_drop_does_not_affect_other_names()
    _test_tombstone_composes_with_PondGC()
    print("\n=== ALL TESTS PASSED ===")


if __name__ == "__main__":
    _run_all_tests()
