# SCORECARD.md — Rubric scores per component (with trend)

> Crucible state file. Updated after every tribunal. Scores 1–10 against the
> eight principles + done-statements. Prior whole-repo review scores: 54/100
> (2026-08-27), 58/100 (2026-08-28). Tribunal r1 (cron-2026-08-28-0120-b)
> scored the pruned-read-pipeline iteration — PASS-WITH-REPAIRS, repaired
> same-cycle. Tribunal r2 (cron-2026-08-28-0353-b) scored the D3 journal
> cycle — verdict **FAIL (repairable)** with 10 findings; the correctness
> holes (F1, F2-common), the scalability defects (F3, F6), and the test-
> honesty gap (F8) were repaired SAME-CYCLE with child-process regression
> tests for the exact tribunal probes. Tribunal r3 (cron-2026-08-28-1100-c)
> scored the N+3 LAWS cycle (D6 read plan + C3 proptests) — verdict
> **PASS-WITH-REPAIRS** (no HIGH finding; the D6 algorithm cleared a
> line-level attack on coverage chains, lenient classification, plan order,
> and dropped-entry reachability); repairs (multi-writer law, C11 chain
> test, shard.rs caveat, state-file sync) landed same-cycle.

## Tribunal r3 scores (N+3 LAWS cycle: D6 read plan + C3 proptests)

| Axis | r3 score | Note |
|---|---|---|
| Simplicity | 9 | the 5-way duplicated pack-list loop now lives ONCE (resolve_packs) |
| Efficiency | 8 | fast path pinned at zero extra reads; compact-present path re-reads manifests (pre-D6 shape, NIT) |
| Beauty | 9 | ONE plan builder for every reader; RG-identity coverage is set algebra |
| Scalability | 8 | zombie-entry cleanup closes the F6-class growth; C15 residual recorded |
| Power | 8 | plan introspection (only_rgs) enables future per-RG scheduling |
| Performance | 8 | only_rgs applied BEFORE zone-map/bloom pruning and I/O |
| Full-functionality | 8 | ~1700 pinned-seed law cases attacking the owner's CRDT core |
| Versatility | 8 | same plan for pyo3/SQL/CLI/lenses; legacy + journal surfaces both covered |
| D-S1..D-S6 | 9/7/9/10/9/9 | item 2 scored pre-repair (single-writer laws); the multi-writer law closed it same-cycle |

## Component scorecard (whole repo, updated for the N+5 substrate cycle)

| Component | Simplicity | Efficiency | Beauty | Scalability | Power | Performance | Full-func | Versatility | Trend / note |
|---|---|---|---|---|---|---|---|---|---|
| **Journal (D3+D6, journal.rs)** | 9 | 9 | 9 | 9 | 9 | 8 | 9 | 8 | resolve_packs: ONE plan builder, RG-level coverage, zombie cleanup; F1/F2/F3/F6 + C7/C11 all closed |
| Pruned read pipeline (read.rs) | 9 | 9 | 8 | 9 | 8 | 9 | 8 | 7 | delegates to the plan; lenient non-PND2 skip (C12) |
| pyo3 binding | 8 | 8 | 8 | 8 | 8 | 8 | 8 | 8 | journal-aware via reader; C10 tiebreak fixed; compact_shards real fold; N+5: +ObjectStore raw surface (GIL-releasing, cache-wired) — the Python substrate delegates through it |
| Python core/kernel (bindings/python/core) | 8 | 9 | 8 | 8 | 8 | 9 | 8 | 9 | N+5: RustObjectStore adapter (byte-identical layout, old-layout fallbacks, KeyError parity) + make_kernel(backend=…) auto-Rust with byte-identical pure-Python fallback; moto-proven S3 via the Rust client |
| Write path | 8 | 9 | 8 | 8 | 9 | 8 | 9 | 8 | all 5 paths journal-append; CAS DELETED; zero shared-object writes |
| Branch/merge (branch.rs) | 7 | 7 | 7 | 8 | 8 | 7 | 8 | 8 | journal-aware (fold-first); deletion-as-data; PMAN normalization |
| Codec (PND2 + zstd) | 8 | 9 | 8 | 8 | 7 | 9 | 8 | 7 | PMAN normalize + roundtrip laws now proven (C3); PSLB/PNPK laws still open |
| S3/SigV4/R2 client | 8 | 8 | 8 | 8 | 8 | 8 | 9 | 8 | +list_dirs (delimiter LIST) + store_id; R2-validated |
| Cache (disk+moka) | 8 | 8 | 8 | 8 | 7 | 8 | 8 | 8 | unchanged |
| CLI | 8 | 8 | 8 | 8 | 8 | 8 | 8 | 8 | +journal-status/compact; journal-aware history (folds list) |
| SQL (core/sql) | 8 | 8 | 8 | 8 | 8 | 8 | 8 | 7 | inherits journal reads + the plan; C8 RESOLVED N+5 (errors propagate; no-commits stays zero-rows); C17 ref-read blindness open |
| Testing | 9 | 9 | 8 | 8 | 8 | 8 | 9 | 8 | 583 tests; 11 property laws (~1700 pinned-seed cases); child-process + fabricated multi-writer harnesses; honest finding #1; N+5: +20 substrate interop tests (byte-compat both directions, moto-S3, capability probes) + BlobOutage C8 test |
| Docs/laws | 9 | 7 | 8 | 7 | 7 | 7 | 8 | 7 | D3+D6 settled; laws specs; shard.rs caveat honesty |

Thresholds: nothing below 8 on principles is "done"; nothing below 9 on
done-statements. Remaining lowest columns: C17 get_path ref-read
blindness (N+5 discovery), C13 raw-reader journal routing (documented
N+5), C5-python phase 2 (conditional format unification), C12 lenient
skip, C15 duplicate-identical-RG NIT, C16 CRDT-RG read cost, i64-only
leaf/bloom pruning (format limits), PSLB/PNPK codec laws.
