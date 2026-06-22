# TPC-DS harness fidelity audit — table-scan counts vs canonical Trino plan (2026-06-20)

Triggered by the q9 finding (Velox harness had fused scalar subqueries Trino/Nitro replicate). This audits
**every** TPC-DS query's table-scan count across three sources to find other apples-to-apples divergences.

## Method
For each query, count table-scan nodes (one per table reference) in:
- **Canonical** = `EXPLAIN` of the real Trino TPC-DS SQL (`~/notes/trino` benchmark queries, `tpcds` connector),
  counting `LocalTpcdsTableHandle[tableName=...]` plan nodes. This is the ground truth the harnesses should match.
- **Nitro harness** = `TpcdsParquetSupport.SCAN_COUNT` incremented in the scan helpers while the operator tree
  is built (`CountScans` test).
- **Velox harness** = `scan[...]` nodes in `NitroTpcdsBenchmark --run_query_verbose` output.

Raw counts: `scan-count-audit-20260620.csv`.

## Result (99 queries)
| verdict | count | meaning |
|---|---|---|
| match (canon == nitro == velox) | 83 | apples-to-apples ✓ (includes q9, now fixed) |
| **VELOX fewer than Nitro** | **3** | **N/V ratio inflated — Velox harness fuses scans the canonical plan replicates** |
| symmetric deviation (nitro == velox ≠ canon) | 12 | both harnesses model a different scan count than raw Trino; **fair for N/V** (both equal) |
| incomplete | 1 | q14 (no Velox port) |

## The 3 N/V-biasing divergences (same class as the q9 bug — fix the Velox harness)
| q | canon | nitro | velox | board N/V | note |
|---|-------|-------|-------|-----------|------|
| 56 | 15 | 15 | 12 | 2.02× | Velox shares item/date_dim/customer_address subquery scans across the 3 channels; Nitro==canonical. Real N/V is lower. |
| 60 | 15 | 15 | 12 | 1.88× | same structure as q56 (sibling query). Nitro==canonical. |
| 59 | 8 | 7 | 5 | 0.84× | both sides deviate (Velox 5, Nitro 7, canon 8); Nitro already faster, lower priority. |

q9 itself is now in the **match** bucket (canon=nitro=velox=16) after the Velox-harness fix.

## The 12 symmetric deviations (do NOT bias N/V; flagged for completeness)
Both Nitro and Velox use the same scan count, but it differs from raw Trino `EXPLAIN` — the operator harnesses
model these queries with more/fewer scans than Trino's optimizer chooses (e.g. per-channel scans where Trino
shares one, or vice versa). Because both engines agree, the Nitro-vs-Velox comparison stays apples-to-apples.
- More than canonical: q5 (+6), q21 (+4), q16 (+3), q94 (+1)
- Fewer than canonical: q4 (−5), q83 (−3), q95 (−2), q8/q18/q24/q72/q84 (−1)

## Why only TPC-DS
The q9-class risk is fused **scalar/correlated subqueries over the same table** — a TPC-DS-specific shape.
TPC-H queries are straight join/aggregate pipelines (one scan per table reference, no same-table subquery
families) and ClickBench is single-table scans, so neither suite is susceptible to this divergence.
