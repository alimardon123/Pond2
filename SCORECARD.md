# SCORECARD.md — Rubric scores per component (with trend)

> Crucible state file. Updated after every tribunal. Scores 1–10 against the
> eight principles + done-statements. Prior whole-repo review scores: 54/100
> (2026-08-27), 58/100 (2026-08-28) — trend ↑.

| Component | Simplicity | Efficiency | Beauty | Scalability | Power | Performance | Full-func | Versatility | Trend / note |
|---|---|---|---|---|---|---|---|---|---|
| Pruned read pipeline (read.rs) | 8 | 9 | 7 | 9 | 8 | 9 | 7 | 4 | i64-only → versatility is the gap; this cycle generalizes |
| pyo3 binding (read_rows path) | 7 | 4 | 6 | 6 | 8 | 4 | 8 | 7 | **Not routed through pruned reader — this cycle's target** |
| Write path (shards + CAS loop) | 6 | 6 | 5 | 7 | 7 | 6 | 8 | 7 | Correct but CAS-centric; journal design pending (D3) |
| Codec (PND2 + zstd) | 8 | 9 | 8 | 8 | 7 | 9 | 8 | 6 | native zstd landed (66ecca3) |
| S3/SigV4/R2 client | 8 | 8 | 7 | 8 | 8 | 8 | 9 | 8 | R2-validated live |
| Cache (disk+moka) | 8 | 8 | 8 | 8 | 7 | 8 | 8 | 8 | wired into product paths |
| CLI | 7 | 7 | 7 | 7 | 6 | 8 | 7 | 7 | exists, must stay first-class (D2) |
| SQL (core/sql) | 7 | 6 | 7 | 7 | 6 | 6 | 7 | 7 | pushdown improving |
| Testing (unit+moto+R2) | 7 | 7 | 7 | 6 | 6 | 7 | 8 | 7 | zero proptests — open gap |
| Docs/laws | 8 | 7 | 8 | 7 | 7 | 7 | 8 | 7 | good corpus |

Thresholds: nothing below 8 on principles is "done"; nothing below 9 on
done-statements. Lowest columns today: pyo3 read-path Efficiency/Performance
(4) and read-pipeline Versatility (4) — exactly what this cycle attacks.
