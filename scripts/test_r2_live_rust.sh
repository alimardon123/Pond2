#!/usr/bin/env bash
# R2 LIVE harness driver (Rust layer) — sources .env, maps credentials to
# POND_R2_* env vars, runs the env-gated integration test.
#
# NEVER runs in CI (no credentials there by design — zero secret material
# in the repo; .env is gitignored). Run locally:
#   scripts/test_r2_live_rust.sh
#
# Companion: scripts/test_rust_s3_r2.py (the CLI-level harness, run via
# pytest tests/test_all.py::test_rust_s3_r2_backend).
set -euo pipefail
cd "$(dirname "$0")/.."

# Load .env if present (env vars win, same convention as the Python harness).
if [ -f .env ]; then
  set -a; . ./.env; set +a
fi

# Map the .env spelling to the POND_R2_* gate.
export POND_R2_ENDPOINT="${POND_R2_ENDPOINT:-${R2_ENDPOINT:-}}"
export POND_R2_BUCKET="${POND_R2_BUCKET:-${R2_BUCKET:-}}"
export POND_R2_ACCESS_KEY_ID="${POND_R2_ACCESS_KEY_ID:-${AWS_ACCESS_KEY_ID:-}}"
export POND_R2_SECRET_ACCESS_KEY="${POND_R2_SECRET_ACCESS_KEY:-${AWS_SECRET_ACCESS_KEY:-}}"

if [ -z "${POND_R2_ENDPOINT}" ] || [ -z "${POND_R2_BUCKET}" ] \
   || [ -z "${POND_R2_ACCESS_KEY_ID}" ] || [ -z "${POND_R2_SECRET_ACCESS_KEY}" ]; then
  echo "SKIP: R2 credentials not found (.env absent or incomplete — see .env.example)" >&2
  exit 0
fi

echo "[r2-live-rust] endpoint=${POND_R2_ENDPOINT} bucket=${POND_R2_BUCKET}"
cargo test -p pond_s3 --test r2_live -- --nocapture
