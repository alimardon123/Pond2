# SCORECARD.md — Rubric scores per component (with trend)

> Crucible state file. Updated after every tribunal. Scores 1–10 against the
> eight principles + done-statements. Prior whole-repo review scores: 54/100
> (2026-08-27), 58/100 (2026-08-28). Tribunal r1 (cron-2026-08-28-0120-b)
> scored the pruned-read-pipeline iteration below — verdict
> PASS-WITH-REPAIRS; repairs landed same-cycle.

## Tribunal r1 scores (this cycle's change: pruned read pipeline + routing)

| Axis | r1 score | Note |
|---|---|---|
| Simplicity | 8 | 3 duplicated readers deleted; −1 for 3rd determine_rowid / 2nd base64 copies (C7) |
| Efficiency | 9 | Full pruning stack behind every reader; cache-friendly canonical ranges |
| Beauty | 8 | Rationale-dense comments; `unwrap_or("")` wart fixed in repair |
| Scalability | 9 | v3 leaf pruning everywhere; old readers couldn't even decode v3 roots (real bug fixed) |
| Power | 8 | Projection + WHERE pushdown on all 3 surfaces; UPDATE/DELETE correctly conservative |
| Performance | 9 | ~4% of old bytes on 1-of-24 selective read; zero data bytes on no-match |
| Full-functionality | 8 | All types round-trip; string-`!=` regression found & fixed in repair |
| Versatility | 8 | One reader backs pyo3/SQL/CLI; i64-only leaf/bloom pruning is a format limit (honest) |
| D-S1..D-S5 | 10/9/9/9/9 | Contract files, routing proof, green harness, CI edit verified, honest records |

## Component scorecard (whole repo, updated for this cycle)

| Component | Simplicity | Efficiency | Beauty | Scalability | Power | Performance | Full-func | Versatility | Trend / note |
|---|---|---|---|---|---|---|---|---|---|
| Pruned read pipeline (read.rs) | 8 | 9 | 8 | 9 | 8 | 9 | 8 | 7 | **C1 resolved**: all types, all readers route through it; bool + type-strict filter |
| pyo3 binding (read_rows path) | 8 | 8 | 7 | 7 | 8 | 8 | 8 | 7 | routed through pruned reader; LIST debt remains (C2) |
| Write path (shards + CAS loop) | 6 | 6 | 5 | 7 | 7 | 6 | 8 | 7 | unchanged; journal design pending (D3, C5) |
| Codec (PND2 + zstd) | 8 | 9 | 8 | 8 | 7 | 9 | 8 | 6 | unchanged |
| S3/SigV4/R2 client | 8 | 8 | 7 | 8 | 8 | 8 | 9 | 8 | unchanged, R2-validated |
| Cache (disk+moka) | 8 | 8 | 8 | 8 | 7 | 8 | 8 | 8 | unchanged |
| CLI | 8 | 8 | 8 | 8 | 7 | 8 | 8 | 7 | **routed through pruned reader** (D4); legacy fallback preserved |
| SQL (core/sql) | 8 | 8 | 8 | 8 | 7 | 8 | 8 | 7 | WHERE pushdown landed; C8 error-swallowing open |
| Testing (unit+moto+R2) | 8 | 8 | 8 | 7 | 7 | 8 | 8 | 7 | 511 tests, +byte-budget tests; proptests still zero (C3) |
| Docs/laws | 8 | 7 | 8 | 7 | 7 | 7 | 8 | 7 | +Crucible state files; staledb naming corrected |

Thresholds: nothing below 8 on principles is "done"; nothing below 9 on
done-statements. Remaining lowest columns: write-path Beauty/Simplicity
(CAS loop — replaced by D3 journal in a write-path cycle), Versatility of
codec/pruned-pipeline (i64-only leaf/bloom pruning), C2 per-read LIST.
