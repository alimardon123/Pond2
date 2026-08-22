#!/usr/bin/env python3
"""
Verify KNOWLEDGE_GRAPH.md covers 100% of active files.

Usage:
    python scripts/verify_knowledge_graph.py

Exits 0 if all active files are covered, 1 otherwise.
"""

import os
import sys

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
KG_PATH = os.path.join(REPO_ROOT, "KNOWLEDGE_GRAPH.md")

# Directories to skip (historical, not active)
SKIP_DIRS = {".git", "archive", "__pycache__", ".pytest_cache",
              ".venv", ".venv-pond", "node_modules", ".mypy_cache",
              ".ruff_cache", ".target"}

# File extensions to check
CHECK_EXTENSIONS = {".py", ".md", ".tla", ".cfg"}

# Files to skip (generated, temporary, or not worth tracking)
SKIP_FILES = {"KNOWLEDGE_GRAPH.md", "fix_ci.py"}  # self + gitignored temp scripts


def find_active_files():
    """Find all active files in the repo (not in archive/ or .git/)."""
    active_files = []
    for root, dirs, files in os.walk(REPO_ROOT):
        # Skip archive and other non-active dirs
        dirs[:] = [d for d in dirs if d not in SKIP_DIRS]
        for f in files:
            ext = os.path.splitext(f)[1]
            if ext not in CHECK_EXTENSIONS:
                continue
            if f in SKIP_FILES:
                continue
            full_path = os.path.join(root, f)
            rel_path = os.path.relpath(full_path, REPO_ROOT)
            active_files.append(rel_path)
    return sorted(active_files)


def verify_coverage():
    """Verify every active file is mentioned in KNOWLEDGE_GRAPH.md."""
    with open(KG_PATH) as f:
        kg_content = f.read()

    active_files = find_active_files()
    missing = []

    for f in active_files:
        if f not in kg_content:
            missing.append(f)

    print(f"Active files: {len(active_files)}")
    print(f"Covered:      {len(active_files) - len(missing)}")
    print(f"Missing:      {len(missing)}")

    if missing:
        print("\nMISSING FROM KNOWLEDGE_GRAPH.md:")
        for f in missing:
            print(f"  {f}")
        return 1
    else:
        print("\n✓ All active files are covered in KNOWLEDGE_GRAPH.md")
        return 0


if __name__ == "__main__":
    sys.exit(verify_coverage())
