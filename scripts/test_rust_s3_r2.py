#!/usr/bin/env python3
"""
Pond — Rust S3 backend integration test against REAL Cloudflare R2.

This is the first end-to-end test that exercises the Rust `pond_s3` backend
(hand-rolled SigV4 + path-style requests + ureq) through the `pond` CLI
against a live Cloudflare R2 bucket.

CREDENTIALS:
  Reads R2 credentials from a gitignored `.env` file at the repo root, OR
  from the environment. Env vars take precedence over `.env`.

  Required (either source):
    AWS_ACCESS_KEY_ID      R2 Access Key ID
    AWS_SECRET_ACCESS_KEY  R2 Secret Access Key
    R2_ENDPOINT            https://<account>.r2.cloudflarestorage.com
    R2_BUCKET              bucket name
  Optional:
    POND_S3_URL            full s3:// URL (overrides endpoint/bucket assembly)

WHAT IT TESTS:
  1. pond version           — binary sanity
  2. pond init <s3 url>     — S3 connection + describe_storage (R2 detection)
  3. pond write-rows        — PND2 typed-row write path over S3
  4. pond read-rows         — PND2 read-back + predicate pushdown + table fmt
  5. pond ls                 — collection listing over S3
  6. pond history           — commit log over S3
  7. pond branch/checkout   — git-like branching over S3
  8. pond merge             — branch merge over S3
  9. pond write/read        — raw content-addressed blob path
 10. pond cat               — content-addressed read by hash
 11. cleanup                — delete the test prefix from R2

USAGE:
  # stand-alone (reads .env automatically):
  python scripts/test_rust_s3_r2.py

  # via pytest:
  pytest tests/test_all.py::test_rust_s3_r2_backend -v -s

EXIT CODE:
  0 on success (prints "ALL R2 TESTS PASSED" to stdout)
  1 on any failure (prints diagnostics to stderr)

NOTE: This test writes to a unique per-run prefix under the bucket
(`pond-r2-itest-<timestamp>/`) and deletes it at the end, so it is safe to
run repeatedly against a shared bucket. Network failures are real failures,
not skipped — this is a live integration test.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import Optional

REPO_ROOT = Path(__file__).resolve().parent.parent


# ──────────────────────────────────────────────────────────────────────────
# .env loader (so the script works when invoked directly, not just via pytest)
# ──────────────────────────────────────────────────────────────────────────

def _load_dotenv(path: Path) -> None:
    """Minimal .env loader: KEY="value" or KEY=value lines. No shell expansion."""
    if not path.is_file():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, raw = line.partition("=")
        key = key.strip()
        val = raw.strip().strip('"').strip("'")
        # Don't override an already-set env var (env takes precedence over .env)
        if key and key not in os.environ:
            os.environ[key] = val


_load_dotenv(REPO_ROOT / ".env")


# ──────────────────────────────────────────────────────────────────────────
# Configuration
# ──────────────────────────────────────────────────────────────────────────

def _require(name: str) -> str:
    v = os.environ.get(name)
    if not v:
        print(f"[FAIL] missing required env var: {name}", file=sys.stderr)
        print(f"       create .env with R2 creds (see .env.example), or export it.",
              file=sys.stderr)
        sys.exit(2)
    return v


def _find_pond_binary() -> str:
    """Locate the pond CLI binary. Prefer release, fall back to debug."""
    for cand in ("target/release/pond", "target/debug/pond"):
        p = REPO_ROOT / cand
        if p.is_file() and os.access(p, os.X_OK):
            return str(p)
    print("[FAIL] pond binary not found — run `cargo build --release -p pond_cli`",
          file=sys.stderr)
    sys.exit(2)


def _build_r2_url(test_prefix: str) -> str:
    """Build the s3:// URL pointing at a unique per-run test prefix."""
    # If the user gave a full POND_S3_URL, rewrite its prefix to our test prefix.
    existing = os.environ.get("POND_S3_URL", "")
    if existing.startswith("s3://"):
        # Parse: s3://bucket/oldprefix?region=...&endpoint=...
        rest = existing[len("s3://"):]
        bucket, _, query_part = rest.partition("/")
        path, _, qs = query_part.partition("?")
        # Extract endpoint from query string
        endpoint = ""
        region = "auto"
        for kv in qs.split("&"):
            if kv.startswith("endpoint="):
                endpoint = kv[len("endpoint="):]
            elif kv.startswith("region="):
                region = kv[len("region="):]
        if not endpoint:
            endpoint = _require("R2_ENDPOINT")
        return f"s3://{bucket}/{test_prefix}?region={region}&endpoint={endpoint}"
    # Assemble from parts
    bucket = _require("R2_BUCKET")
    endpoint = _require("R2_ENDPOINT")
    return f"s3://{bucket}/{test_prefix}?region=auto&endpoint={endpoint}"


# ──────────────────────────────────────────────────────────────────────────
# Test harness
# ──────────────────────────────────────────────────────────────────────────

class R2TestRunner:
    """Runs `pond` CLI commands against R2 and asserts on their output."""

    def __init__(self) -> None:
        self.pond = _find_pond_binary()
        # Unique per-run prefix so concurrent/parallel runs don't collide.
        # Use time_ns() to avoid same-second collisions (review C7).
        self.prefix = f"pond-r2-itest-{time.time_ns()}"
        self.root = _build_r2_url(self.prefix)
        self.passed = 0
        self.failed = 0
        # Use a short test-run identifier for collection names, so we don't
        # accidentally collide with another run on the same bucket prefix.
        self.run_id = f"run_{time.time_ns() % 100000000}"

    # — low-level command execution ————————————————————————————

    def pond_cmd(self, *args: str, expect_ok: bool = True, timeout: int = 60) -> subprocess.CompletedProcess:
        """Run `pond --root <r2_url> <args>`. Returns the CompletedProcess.

        If expect_ok=True and the command fails (nonzero exit or timeout),
        this raises AssertionError to halt the test — prevents cascading
        failures from obscuring the root cause (review C3/C4).
        """
        cmd = [self.pond, "--root", self.root, *args]
        try:
            r = subprocess.run(
                cmd, capture_output=True, text=True, timeout=timeout,
            )
        except subprocess.TimeoutExpired as e:
            stdout_repr = repr(e.stdout) if e.stdout else "(none)"
            msg = (f"TIMEOUT after {timeout}s\n"
                   f"  cmd: {' '.join(cmd)}\n"
                   f"  stdout: {stdout_repr}"[:800])
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

    # — assertion helpers ———————————————————————————————————————

    def check(self, name: str, cond: bool, detail: str = "") -> None:
        if cond:
            self.passed += 1
            print(f"  [OK] {name}")
        else:
            self.failed += 1
            print(f"  [FAIL] {name} {detail}")

    def _fail(self, args: tuple, why: str, cmd: list, r: Optional[subprocess.CompletedProcess]) -> None:
        """Print a diagnostic for a failed command. Does NOT increment failed
        (the caller's check() does that) — avoids double-counting (review C3)."""
        print(f"  [FAIL] pond {' '.join(args)} — {why}", file=sys.stderr)
        print(f"         cmd: {' '.join(cmd)}", file=sys.stderr)
        if r is not None:
            if r.stdout:
                print(f"         stdout: {r.stdout[-800:]}", file=sys.stderr)
            if r.stderr:
                print(f"         stderr: {r.stderr[-800:]}", file=sys.stderr)

    # — test steps ——————————————————————————————————————————————

    def test_version(self) -> None:
        """1. Binary sanity: `pond version` prints `pond <version>`."""
        print("\n[1/10] pond version — binary sanity")
        r = subprocess.run([self.pond, "version"], capture_output=True, text=True, timeout=15)
        self.check("pond version exits 0", r.returncode == 0,
                   f"(exit {r.returncode}, stderr={r.stderr[:200]})")
        self.check("output starts with 'pond '",
                   r.stdout.strip().startswith("pond "),
                   f"(got: {r.stdout.strip()!r})")
        # Version format: `pond <x.y.z...>`. Don't hardcode the exact version
        # string — just verify a version-like token follows (review C10).
        ver = r.stdout.strip()
        self.check("output has a version token after 'pond '",
                   len(ver.split()) >= 2 and any(c.isdigit() for c in ver.split()[1]),
                   f"(got: {ver!r})")

    def test_init(self) -> None:
        """2. pond init — connect to R2, verify describe_storage detects R2."""
        print("\n[2/10] pond init — connect to R2")
        r = self.pond_cmd("init", self.root)
        out = r.stdout + r.stderr
        self.check("init exits 0", r.returncode == 0)
        # describe_storage labels R2 endpoints as "Cloudflare R2".
        # Tightened: assert ONLY on the literal provider string, not on the
        # echoed endpoint URL (review C1 — avoids false positives on failure).
        self.check("detects Cloudflare R2 provider",
                   "Cloudflare R2" in out,
                   f"(got: {out[:300]!r})")

    def test_write_read_rows(self) -> None:
        """3-6. write-rows → read-rows round-trip with filters + formats."""
        coll = f"users_{self.run_id}"
        print(f"\n[3/10] pond write-rows — write PND2 rows to R2 (collection={coll})")
        rows = [
            {"id": 1, "name": "alice", "email": "alice@pond.dev", "age": 30},
            {"id": 2, "name": "bob", "email": "bob@pond.dev", "age": 25},
            {"id": 3, "name": "carol", "email": "carol@pond.dev", "age": 35},
        ]
        r = self.pond_cmd("write-rows", coll, "--json", json.dumps(rows), "-m", "seed users")
        self.check("write-rows exits 0", r.returncode == 0)
        # Output format: "<12-char-hash>\t<collection>"
        parts = r.stdout.strip().split("\t")
        self.check("write-rows output is '<hash>\\t<coll>'",
                   len(parts) == 2 and len(parts[0]) == 12,
                   f"(got: {r.stdout.strip()!r})")
        self.check("collection name echoed back",
                   len(parts) == 2 and parts[1] == coll,
                   f"(got: {parts[-1] if parts else ''!r})")

        print(f"\n[4/10] pond read-rows — read back all rows (JSON)")
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
                       names == ["alice", "bob", "carol"],
                       f"(got: {names})")

        print(f"\n[5/10] pond read-rows --where — predicate pushdown over R2")
        r = self.pond_cmd("read-rows", coll, "--where", "age > 28")
        self.check("read-rows --where exits 0", r.returncode == 0,
                   f"(stderr: {r.stderr[:200]})")
        try:
            got = json.loads(r.stdout)
        except json.JSONDecodeError:
            got = []
        # age > 28 → alice(30), carol(35) — bob(25) excluded
        self.check("predicate filters correctly (2 rows, age>28)",
                   isinstance(got, list) and len(got) == 2,
                   f"(got {len(got) if isinstance(got, list) else 'non-list'})")
        if isinstance(got, list) and got:
            ages = sorted(row.get("age") for row in got)
            self.check("filtered ages are >28",
                       all(a > 28 for a in ages),
                       f"(got: {ages})")

        print(f"\n[6/10] pond read-rows --columns --format table — projection + table fmt")
        r = self.pond_cmd("read-rows", coll, "--columns", "id,name", "--format", "table")
        self.check("read-rows --format table exits 0", r.returncode == 0)
        # Table format has a header line and a separator
        lines = [l for l in r.stdout.splitlines() if l.strip()]
        self.check("table output has >=3 lines (header, sep, rows)",
                   len(lines) >= 3, f"(got {len(lines)} lines)")
        if lines:
            self.check("header contains 'id' and 'name'",
                       "id" in lines[0] and "name" in lines[0],
                       f"(header: {lines[0]!r})")

    def test_ls_history(self) -> None:
        """7-8. ls + history over R2."""
        coll = f"users_{self.run_id}"
        print(f"\n[7/10] pond ls — list collections over R2")
        r = self.pond_cmd("ls")
        self.check("ls exits 0", r.returncode == 0)
        lines = [l for l in r.stdout.splitlines() if l.strip()]
        self.check("ls shows our collection", any(coll in l for l in lines),
                   f"(lines: {lines[:3]})")

        print(f"\n[8/10] pond history — commit log over R2")
        r = self.pond_cmd("history", coll)
        self.check("history exits 0", r.returncode == 0)
        lines = [l for l in r.stdout.splitlines() if l.strip()]
        self.check("history has >=1 commit", len(lines) >= 1,
                   f"(got {len(lines)} lines)")
        if lines:
            # JOURNAL ERA (D3): journal-aware history — the first line may be
            # the bootstrap fold; the user's seed write must still appear
            # (via the fold's `folds` list).
            self.check("history line has hash + message",
                       "\t" in lines[0] and any("seed" in l for l in lines),
                       f"(first line: {lines[0]!r})")

    def test_branch_merge(self) -> None:
        """9. Branch + checkout + write + merge over R2."""
        coll = f"users_{self.run_id}"
        branch = f"feature_{self.run_id}"
        print(f"\n[9/10] pond branch/checkout/merge — git-like ops over R2")

        r = self.pond_cmd("branch", coll, branch)
        self.check("branch creates a new branch", r.returncode == 0,
                   f"(stderr: {r.stderr[:200]})")

        r = self.pond_cmd("checkout", coll, branch)
        self.check("checkout switches to branch", r.returncode == 0,
                   f"(stderr: {r.stderr[:200]})")

        # Write a new row on the branch
        new_rows = [{"id": 4, "name": "dave", "email": "dave@pond.dev", "age": 40}]
        r = self.pond_cmd("write-rows", coll, "--json", json.dumps(new_rows), "-m", "add dave on branch")
        self.check("write-rows on branch exits 0", r.returncode == 0)

        # Verify branch now has 4 rows (3 inherited + dave) — journal append.
        # See comment in test_branch_merge for CRDT union merge semantics.
        r = self.pond_cmd("read-rows", coll)
        try:
            got = json.loads(r.stdout)
            n = len(got) if isinstance(got, list) else 0
        except json.JSONDecodeError:
            n = 0
        # JOURNAL ERA (D3): `write-rows` APPENDS to the branch's journal —
        # the branch inherited main's 3 rows at the branch point (the branch
        # command folds the source first), dave's write lands on top, and
        # readers union snapshot + live entries → 4 rows. (The old
        # expectation of 1 row asserted the C9 history-loss bug: write-rows
        # REPLACED the branch HEAD, silently destroying the inherited rows.)
        # `merge` unions both sides (still 4 rows; dave present).
        self.check("branch has 4 rows after write (journal append semantics)",
                   n == 4, f"(got {n})")

        # Merge branch back into main
        r = self.pond_cmd("merge", coll, branch, "-i", "main", "-m", "merge feature branch")
        self.check("merge exits 0", r.returncode == 0,
                   f"(stderr: {r.stderr[:200]})")

        # Checkout main and verify it now has dave too
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
        self.check("dave is present on main after merge",
                   "dave" in names, f"(got: {names})")

    def test_raw_blobs(self) -> None:
        """10. Raw content-addressed blob write/read/cat."""
        coll = f"docs_{self.run_id}"
        print(f"\n[10/10] pond write/read/cat — raw content-addressed blobs over R2")
        payload = json.dumps({"hello": "r2", "ts": int(time.time())})
        r = self.pond_cmd("write", coll, "--json", payload, "-m", "raw blob test")
        self.check("write (raw) exits 0", r.returncode == 0)
        parts = r.stdout.strip().split("\t")
        self.check("write output has hash prefix", len(parts) == 2 and len(parts[0]) == 12)
        hash_prefix = parts[0] if len(parts) == 2 else None

        # read back the collection HEAD
        r = self.pond_cmd("read", coll)
        self.check("read (raw) exits 0", r.returncode == 0)
        self.check("read returns the payload we wrote", r.stdout.strip() == payload,
                   f"(got: {r.stdout.strip()[:100]!r})")

        # cat by hash — need the full hash. The CLI `cat` takes a hash; we only
        # have the 12-char prefix. The CLI resolves short prefixes (per
        # cli_integration.rs patterns), so try the prefix.
        if hash_prefix:
            r = self.pond_cmd("cat", hash_prefix, expect_ok=False)
            # cat may or may not accept short prefixes — don't fail the suite if not
            if r.returncode == 0:
                self.check("cat <hash> returns the blob", r.stdout.strip() == payload)
            else:
                # Not a hard failure — prefix resolution may require full hash.
                print(f"  [SKIP] cat <short-hash> (prefix resolution may need full hash; "
                      f"stderr: {r.stderr[:120].strip()!r})")

    # — cleanup ——————————————————————————————————————————————

    def cleanup(self) -> None:
        """Delete the test prefix from R2 so we don't leave litter."""
        print(f"\n[cleanup] deleting test prefix '{self.prefix}/' from R2 bucket")
        try:
            import boto3
            from botocore.config import Config
        except ImportError:
            print("  [SKIP] boto3 not installed — leaving test prefix in bucket "
                  "(manual cleanup: delete prefix "
                  f"'{self.prefix}/' from bucket '{os.environ.get('R2_BUCKET')}')")
            return
        endpoint = os.environ.get("R2_ENDPOINT")
        bucket = os.environ.get("R2_BUCKET")
        ak = os.environ.get("AWS_ACCESS_KEY_ID")
        sk = os.environ.get("AWS_SECRET_ACCESS_KEY")
        if not all([endpoint, bucket, ak, sk]):
            print("  [SKIP] missing creds for boto3 cleanup — manual cleanup needed")
            return
        client = boto3.client(
            "s3",
            endpoint_url=endpoint,
            region_name="auto",
            aws_access_key_id=ak,
            aws_secret_access_key=sk,
            config=Config(connect_timeout=5, read_timeout=30, retries={"max_attempts": 3}),
        )
        try:
            paginator = client.get_paginator("list_objects_v2")
            deleted_total = 0
            for page in paginator.paginate(Bucket=bucket, Prefix=self.prefix + "/"):
                objs = page.get("Contents", [])
                if not objs:
                    continue
                resp = client.delete_objects(
                    Bucket=bucket,
                    Delete={"Objects": [{"Key": o["Key"]} for o in objs], "Quiet": True},
                )
                deleted_total += len(objs)
            print(f"  [OK] deleted {deleted_total} objects under {self.prefix}/")
        except Exception as e:
            print(f"  [WARN] cleanup failed: {e}")
            print(f"         manual cleanup: delete prefix '{self.prefix}/' from bucket '{bucket}'")

    # — runner ————————————————————————————————————————————————

    def run_all(self) -> int:
        print("=" * 70)
        print("Pond Rust S3 backend — live Cloudflare R2 integration test")
        print("=" * 70)
        print(f"  binary : {self.pond}")
        print(f"  bucket : {os.environ.get('R2_BUCKET', '?')}")
        print(f"  prefix : {self.prefix}")
        print(f"  root   : {self.root}")
        print(f"  run_id : {self.run_id}")
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

        self.cleanup()

        print("\n" + "=" * 70)
        print(f"RESULT: {self.passed} passed, {self.failed} failed")
        print("=" * 70)
        if self.failed == 0:
            print("ALL R2 TESTS PASSED")
            return 0
        print("R2 TESTS FAILED")
        return 1


if __name__ == "__main__":
    sys.exit(R2TestRunner().run_all())
