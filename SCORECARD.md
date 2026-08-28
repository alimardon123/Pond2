# SCORECARD.md — Rubric scores per component (with trend)

> Crucible state file. Updated after every tribunal. Scores 1–10 against the
> eight principles + done-statements. Prior whole-repo review scores: 54/100
> (2026-08-27), 58/100 (2026-08-28). Tribunal r1 (cron-2026-08-28-0120-b)
> scored the pruned-read-pipeline iteration — PASS-WITH-REPAIRS, repaired
> same-cycle. Tribunal r2 (cron-2026-08-28-0353-b) scored the D3 journal
> cycle — verdict **FAIL (repairable)** with 10 findings; the correctness
> holes (F1, F2-common), the scalability defects (F3, F6), and the test-
> honesty gap (F8) were repaired SAME-CYCLE with child-process regression
> tests for the exact tribunal probes. Process findings (F4 worklog, F5
> commit/CI) were the cycle's closing steps. Post-repair state recorded
> below; the residual C11/C12/C13/C14 are open in CRITIQUE.md.

## Tribunal r2 scores (D3 no-CAS journal cycle, pre-repair)

| Axis | r2 score | Note |
|---|---|---|
| Simplicity | 7 | pack-list loop duplicated 5× (C7 grew); two bugs from under-articulated probe-start invariant |
| Efficiency | 7 | zero-LIST warm path real; compaction re-deleted history per run (F3, fixed) |
| Beauty | 8 | append-only logs + epoch probing + snapshot-as-cache; safety arguments written down |
| Scalability | 6 | unbounded writer-set/upto growth (F6, fixed); O(N²) deletes (F3, fixed) |
| Power | 8 | multi-writer no-CAS + history + compaction + introspection |
| Performance | 7 | 2 PUTs/write, parallel probes; historical-writer probe growth (fixed with dir GC) |
| Full-functionality | 6 | F1 raw-write blindness + F2 compactor-race duplication (both fixed in repair) |
| Versatility | 8 | localfs/S3/moto/R2 + CLI/pyo3/SQL/lenses routed; env knobs; legacy compat |
| D-S1..D-S9 | 8/9/10/9/6/10/8/3/2 | D-S5 compat scored 6 (F1/F2 lived there); D-S8/D-S9 were pre-commit process state (worklog + push landed same-cycle) |

## Component scorecard (whole repo, updated for the journal cycle)

| Component | Simplicity | Efficiency | Beauty | Scalability | Power | Performance | Full-func | Versatility | Trend / note |
|---|---|---|---|---|---|---|---|---|---|
| **Journal (D3, journal.rs)** | 8 | 9 | 9 | 8 | 9 | 8 | 9 | 8 | **NEW**: no-CAS writes, epoch probes, TTL discovery, compaction w/ benign LWW; F1/F2/F3/F6 repaired |
| Pruned read pipeline (read.rs) | 8 | 9 | 8 | 9 | 8 | 9 | 8 | 7 | journal-aware (snapshot ∪ entries); lenient non-PND2 skip (C12) |
| pyo3 binding | 8 | 8 | 7 | 8 | 8 | 8 | 8 | 7 | journal-aware via reader; C10 tiebreak fixed; compact_shards now a real fold |
| Write path | 8 | 9 | 8 | 8 | 9 | 8 | 9 | 8 | **rewritten**: all 5 paths journal-append; CAS loop DELETED; zero shared-object writes |
| Branch/merge (branch.rs) | 7 | 7 | 7 | 8 | 8 | 7 | 8 | 8 | journal-aware (fold-first); deletion-as-data; PMAN normalization; C11 residual |
| Codec (PND2 + zstd) | 8 | 9 | 8 | 8 | 7 | 9 | 8 | 6 | VT_BOOLEAN merge round-trip fixed; proptests still zero (C3) |
| S3/SigV4/R2 client | 8 | 8 | 8 | 8 | 8 | 8 | 9 | 8 | +list_dirs (delimiter LIST) + store_id; R2-validated |
| Cache (disk+moka) | 8 | 8 | 8 | 8 | 7 | 8 | 8 | 8 | unchanged |
| CLI | 8 | 8 | 8 | 8 | 8 | 8 | 8 | 8 | +journal-status/compact; journal-aware history (folds list) |
| SQL (core/sql) | 8 | 8 | 8 | 8 | 7 | 8 | 8 | 7 | inherits journal reads; C8 error-swallowing open |
| Testing | 8 | 8 | 8 | 8 | 8 | 8 | 8 | 8 | 542 tests (+31); child-process tests (fresh-cache probes); fabricated multi-writer logs; honest F8 docs |
| Docs/laws | 8 | 7 | 8 | 7 | 7 | 7 | 8 | 7 | D3 settled; builder spec + worklog protocol |

Thresholds: nothing below 8 on principles is "done"; nothing below 9 on
done-statements. Remaining lowest columns: C7 duplication (pack-list loop ×5,
determine_rowid ×3), C8 executor error swallowing, C3 codec proptests, C11
partial-overlap compaction residual, i64-only leaf/bloom pruning.
