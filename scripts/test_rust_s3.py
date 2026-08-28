#!/usr/bin/env python3
"""
Pond — Rust S3 backend integration test against a MOTO-mocked S3 endpoint.

This is the HERMETIC counterpart of `test_rust_s3_r2.py`. It runs entirely
locally using moto's in-process HTTP server — no real cloud credentials,
no network egress. Safe to run in CI on every push.

WHAT IT TESTS:
  The same write-rows / read-rows / ls / history / branch / merge / write-read
  round-trip as the R2 test, but against a moto server on localhost. This
  catches regressions in the Rust S3ObjectStore's SigV4 signing, path-style
  URL construction, ListObjectsV2 pagination, and HEAD/GET/PUT/DELETE plumbing
  WITHOUT requiring a live bucket.

REQUIREMENTS:
  pip install moto boto3
  cargo build --release -p pond_cli   (or debug — release preferred)

USAGE:
  python scripts/test_rust_s3.py
  pytest tests/test_all.py::test_rust_s3_backend -v -s

ARCHITECTURE:
  1. Start moto's ThreadedMotoServer on a free localhost port.
  2. Create the bucket via boto3 (against the moto endpoint).
  3. Point `pond --root s3://<bucket>/prefix?region=us-east-1&endpoint=http://localhost:<port>`
     at the moto server.
  4. Run the test sequence.
  5. Shut down the moto server.

NOTE: moto does NOT verify SigV4 signatures — it accepts any credentials.
So this test proves the *shape* of requests (path-style URLs, headers,
query params, pagination) is correct, but does NOT prove the signature
itself is valid. Signature correctness is proven by:
  - core/s3/src/lib.rs unit tests (HMAC-SHA256 known vectors, SigV4
    signing-key known-answer test from AWS docs).
  - test_rust_s3_r2.py (real R2 rejects bad signatures with 403).
"""

from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Optional

REPO_ROOT = Path(__file__).resolve().parent.parent


# ──────────────────────────────────────────────────────────────────────────
# Configuration
# ──────────────────────────────────────────────────────────────────────────

def _find_pond_binary() -> Optional[str]:
    for cand in ("target/release/pond", "target/debug/pond"):
        p = REPO_ROOT / cand
        if p.is_file() and os.access(p, os.X_OK):
            return str(p)
    return None


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


# ──────────────────────────────────────────────────────────────────────────
# Test harness (mirrors R2 test but against moto)
# ──────────────────────────────────────────────────────────────────────────

class MotoTestRunner:
    """Runs `pond` CLI commands against a moto-mocked S3 endpoint."""

    def __init__(self, moto_server_url: str, bucket: str, root_url: str) -> None:
        self.pond = _find_pond_binary()
        self.moto_url = moto_server_url
        self.bucket = bucket
        self.root = root_url
        # Use time_ns() to avoid same-second collisions (review C7).
        self.run_id = f"moto_{time.time_ns() % 100000000}"
        self.passed = 0
        self.failed = 0

    def pond_cmd(self, *args: str, expect_ok: bool = True, timeout: int = 30) -> subprocess.CompletedProcess:
        """Run `pond --root <url> <args>`. Raises AssertionError on expect_ok
        failure — prevents cascading failures (review C3/C4)."""
        cmd = [self.pond, "--root", self.root, *args]
        try:
            r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        except subprocess.TimeoutExpired as e:
            msg = (f"TIMEOUT after {timeout}s\n"
                   f"  cmd: {' '.join(cmd)}")
            if expect_ok:
                raise AssertionError(msg) from e
            return subprocess.CompletedProcess(cmd, 124, str(e.stdout or ""), str(e.stderr or ""))
        if expect_ok and r.returncode != 0:
            msg = (f"exit {r.returncode}\n"
                   f"  cmd: {' '.join(cmd)}\n"
                   f"  stdout: {r.stdout[-800:]}\n"
                   f"  stderr: {r.stderr[-800:]}")
            raise AssertionError(msg)
        return r

    def check(self, name: str, cond: bool, detail: str = "") -> None:
        if cond:
            self.passed += 1
            print(f"  [OK] {name}")
        else:
            self.failed += 1
            print(f"  [FAIL] {name} {detail}")

    def _fail(self, args: tuple, why: str, cmd: list, r: Optional[subprocess.CompletedProcess]) -> None:
        """Print a diagnostic. Does NOT increment failed (caller's check does
        that) — avoids double-counting (review C3)."""
        print(f"  [FAIL] pond {' '.join(args)} — {why}", file=sys.stderr)
        print(f"         cmd: {' '.join(cmd)}", file=sys.stderr)
        if r is not None:
            if r.stdout:
                print(f"         stdout: {r.stdout[-800:]}", file=sys.stderr)
            if r.stderr:
                print(f"         stderr: {r.stderr[-800:]}", file=sys.stderr)

    # — test steps ——————————————————————————————————————————————

    def test_version(self) -> None:
        print("\n[1/8] pond version — binary sanity")
        r = subprocess.run([self.pond, "version"], capture_output=True, text=True, timeout=15)
        self.check("pond version exits 0", r.returncode == 0)
        self.check("output starts with 'pond '",
                   r.stdout.strip().startswith("pond "),
                   f"(got: {r.stdout.strip()!r})")

    def test_init(self) -> None:
        print("\n[2/10] pond init — connect to moto S3 endpoint")
        r = self.pond_cmd("init", self.root)
        self.check("init exits 0", r.returncode == 0)
        # Tightened: assert ONLY on the literal bucket name, not on the echoed
        # s3:// URL (review C2 — avoids false positives on failure).
        out = r.stdout + r.stderr
        self.check("init mentions our bucket name",
                   self.bucket in out,
                   f"(got: {out[:300]!r})")

    def test_write_read_rows(self) -> None:
        coll = f"users_{self.run_id}"
        print(f"\n[3/10] pond write-rows — write PND2 rows (collection={coll})")
        rows = [
            {"id": 1, "name": "alice", "age": 30},
            {"id": 2, "name": "bob", "age": 25},
            {"id": 3, "name": "carol", "age": 35},
        ]
        r = self.pond_cmd("write-rows", coll, "--json", json.dumps(rows), "-m", "seed")
        self.check("write-rows exits 0", r.returncode == 0)
        parts = r.stdout.strip().split("\t")
        self.check("write-rows output is '<hash>\\t<coll>'",
                   len(parts) == 2 and len(parts[0]) == 12,
                   f"(got: {r.stdout.strip()!r})")

        print(f"\n[4/10] pond read-rows — read back all rows")
        r = self.pond_cmd("read-rows", coll)
        self.check("read-rows exits 0", r.returncode == 0)
        try:
            got = json.loads(r.stdout)
        except json.JSONDecodeError as e:
            self.check("read-rows outputs valid JSON", False, f"(err: {e})")
            got = []
        else:
            self.check("read-rows outputs valid JSON", True)
        self.check("row count matches (3)", isinstance(got, list) and len(got) == 3,
                   f"(got {len(got) if isinstance(got, list) else 'non-list'})")
        if isinstance(got, list) and got:
            names = sorted(row.get("name") for row in got)
            self.check("row data matches (alice, bob, carol)",
                       names == ["alice", "bob", "carol"], f"(got: {names})")

        print(f"\n[5/10] pond read-rows --where — predicate pushdown")
        r = self.pond_cmd("read-rows", coll, "--where", "age > 28")
        self.check("read-rows --where exits 0", r.returncode == 0,
                   f"(stderr: {r.stderr[:200]})")
        try:
            got = json.loads(r.stdout)
        except json.JSONDecodeError:
            got = []
        self.check("predicate filters correctly (2 rows, age>28)",
                   isinstance(got, list) and len(got) == 2,
                   f"(got {len(got) if isinstance(got, list) else 'non-list'})")

        print(f"\n[6/10] pond read-rows --columns --format table — projection + table fmt")
        r = self.pond_cmd("read-rows", coll, "--columns", "id,name", "--format", "table")
        self.check("read-rows --format table exits 0", r.returncode == 0)
        lines = [l for l in r.stdout.splitlines() if l.strip()]
        self.check("table output has >=3 lines (header, sep, rows)",
                   len(lines) >= 3, f"(got {len(lines)} lines)")
        if lines:
            self.check("header contains 'id' and 'name'",
                       "id" in lines[0] and "name" in lines[0],
                       f"(header: {lines[0]!r})")

    def test_ls_history(self) -> None:
        coll = f"users_{self.run_id}"
        print(f"\n[7/10] pond ls — list collections")
        r = self.pond_cmd("ls")
        self.check("ls exits 0", r.returncode == 0)
        lines = [l for l in r.stdout.splitlines() if l.strip()]
        self.check("ls shows our collection", any(coll in l for l in lines))

        print(f"\n[8/10] pond history — commit log")
        r = self.pond_cmd("history", coll)
        self.check("history exits 0", r.returncode == 0)
        lines = [l for l in r.stdout.splitlines() if l.strip()]
        self.check("history has >=1 commit", len(lines) >= 1)
        if lines:
            # JOURNAL ERA (D3): history is journal-aware — live entries +
            # snapshot chain + each fold's absorbed writes. The first line
            # may be the bootstrap fold ("journal compaction"); the user's
            # "seed" write must still appear (via the fold's `folds` list).
            self.check("history line has hash + message",
                       "\t" in lines[0] and any("seed" in l for l in lines),
                       f"(first line: {lines[0]!r})")

    def test_branch_merge(self) -> None:
        coll = f"users_{self.run_id}"
        branch = f"feat_{self.run_id}"
        print(f"\n[9/10] pond branch/checkout/merge — git-like ops")
        r = self.pond_cmd("branch", coll, branch)
        self.check("branch creates a new branch", r.returncode == 0,
                   f"(stderr: {r.stderr[:200]})")
        r = self.pond_cmd("checkout", coll, branch)
        self.check("checkout switches to branch", r.returncode == 0)
        new_rows = [{"id": 4, "name": "dave", "age": 40}]
        r = self.pond_cmd("write-rows", coll, "--json", json.dumps(new_rows), "-m", "add dave")
        self.check("write-rows on branch exits 0", r.returncode == 0)
        r = self.pond_cmd("read-rows", coll)
        try:
            got = json.loads(r.stdout)
            n = len(got) if isinstance(got, list) else 0
        except json.JSONDecodeError:
            n = 0
        # JOURNAL ERA (D3): `write-rows` APPENDS to the branch's journal —
        # the branch inherited main's 3 rows at the branch point (the
        # branch command folds the source first), dave's write lands on
        # top, and readers union snapshot + live entries → 4 rows. (The
        # old expectation of 1 row asserted the C9 history-loss bug:
        # write-rows REPLACED the branch HEAD, silently destroying the 3
        # inherited rows — every commit after the first hid its parent.)
        # `merge` unions both sides (still 4 rows; dave present).
        self.check("branch has 4 rows after write (journal append semantics)",
                   n == 4, f"(got {n})")
        r = self.pond_cmd("merge", coll, branch, "-i", "main", "-m", "merge")
        self.check("merge exits 0", r.returncode == 0,
                   f"(stderr: {r.stderr[:200]})")
        r = self.pond_cmd("checkout", coll, "main")
        self.check("checkout back to main", r.returncode == 0)
        r = self.pond_cmd("read-rows", coll)
        try:
            got = json.loads(r.stdout)
            n = len(got) if isinstance(got, list) else 0
            names = sorted(row.get("name") for row in got) if isinstance(got, list) else []
        except json.JSONDecodeError:
            n, names = 0, []
        self.check("main has 4 rows after merge", n == 4, f"(got {n})")
        self.check("dave is present on main after merge", "dave" in names, f"(got: {names})")

    def test_raw_blobs(self) -> None:
        """10. Raw content-addressed blob write/read — mirrors R2 test."""
        coll = f"docs_{self.run_id}"
        print(f"\n[10/10] pond write/read — raw content-addressed blobs")
        payload = json.dumps({"hello": "moto", "ts": int(time.time())})
        r = self.pond_cmd("write", coll, "--json", payload, "-m", "raw blob test")
        self.check("write (raw) exits 0", r.returncode == 0)
        parts = r.stdout.strip().split("\t")
        self.check("write output has hash prefix", len(parts) == 2 and len(parts[0]) == 12)
        # read back the collection HEAD
        r = self.pond_cmd("read", coll)
        self.check("read (raw) exits 0", r.returncode == 0)
        self.check("read returns the payload we wrote", r.stdout.strip() == payload,
                   f"(got: {r.stdout.strip()[:100]!r})")

    # — runner ————————————————————————————————————————————————

    def run_all(self) -> int:
        print("=" * 70)
        print("Pond Rust S3 backend — moto-mocked S3 hermetic integration test")
        print("=" * 70)
        print(f"  binary   : {self.pond}")
        print(f"  moto_url : {self.moto_url}")
        print(f"  bucket   : {self.bucket}")
        print(f"  root     : {self.root}")
        print(f"  run_id   : {self.run_id}")
        print("-" * 70)

        steps = [
            self.test_version,
            self.test_init,
            self.test_write_read_rows,
            self.test_ls_history,
            self.test_branch_merge,
            self.test_raw_blobs,
        ]
        for step in steps:
            try:
                step()
            except SystemExit:
                raise
            except AssertionError as e:
                # pond_cmd raises AssertionError on expect_ok failures —
                # counted once here (review C3/C4: no double-count).
                self.failed += 1
                msg = str(e).splitlines()[0] if str(e) else "(no message)"
                print(f"  [FAIL] {step.__name__}: {msg}", file=sys.stderr)
            except Exception as e:
                self.failed += 1
                print(f"  [FAIL] unhandled exception in {step.__name__}: {e}", file=sys.stderr)
                import traceback
                traceback.print_exc()

        print("\n" + "=" * 70)
        print(f"RESULT: {self.passed} passed, {self.failed} failed")
        print("=" * 70)
        if self.failed == 0:
            print("ALL S3 TESTS PASSED")
            return 0
        print("S3 TESTS FAILED")
        return 1


# ──────────────────────────────────────────────────────────────────────────
# Moto server bootstrap
# ──────────────────────────────────────────────────────────────────────────

def _start_moto() -> tuple:
    """Start a moto ThreadedMotoServer on a free port. Returns (server, url, bucket)."""
    try:
        from moto.server import ThreadedMotoServer
        import boto3
        from botocore.config import Config
    except ImportError as e:
        print(f"[FAIL] moto/boto3 not installed: {e}", file=sys.stderr)
        print("       install with: pip install moto boto3", file=sys.stderr)
        sys.exit(2)

    port = _free_port()
    bucket = f"pond-moto-test-{int(time.time()) % 100000}"
    server = ThreadedMotoServer(ip_address="127.0.0.1", port=port)
    server.start()
    # Wait for the server to be ready
    url = f"http://127.0.0.1:{port}"
    deadline = time.time() + 10
    ready = False
    while time.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                ready = True
                break
        except OSError:
            time.sleep(0.1)
    if not ready:
        server.stop()
        print("[FAIL] moto server did not become ready", file=sys.stderr)
        sys.exit(2)

    # Create the bucket via boto3 (moto accepts any creds)
    client = boto3.client(
        "s3",
        endpoint_url=url,
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
        config=Config(connect_timeout=5, read_timeout=30, retries={"max_attempts": 3}),
    )
    try:
        client.create_bucket(Bucket=bucket)
    except Exception as e:
        server.stop()
        print(f"[FAIL] could not create bucket in moto: {e}", file=sys.stderr)
        sys.exit(2)

    return server, url, bucket


def main() -> int:
    pond = _find_pond_binary()
    if not pond:
        print("[FAIL] pond binary not found — run `cargo build --release -p pond_cli`",
              file=sys.stderr)
        return 2

    server, moto_url, bucket = _start_moto()
    try:
        # pond reads AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY from env.
        # moto doesn't check them, but the Rust from_url() requires them present.
        os.environ.setdefault("AWS_ACCESS_KEY_ID", "test")
        os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "test")
        os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")

        prefix = f"pond-moto-itest-{int(time.time())}"
        root = f"s3://{bucket}/{prefix}?region=us-east-1&endpoint={moto_url}"
        runner = MotoTestRunner(moto_url, bucket, root)
        return runner.run_all()
    finally:
        try:
            server.stop()
        except Exception:
            pass


if __name__ == "__main__":
    sys.exit(main())
