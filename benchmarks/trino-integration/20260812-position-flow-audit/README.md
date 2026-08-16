# Position-flow audit

This audit separates three quantities that must not be conflated:

1. **Physical source positions**: rows admitted by the Parquet reader after row-group pruning. Lower is better when the final result is unchanged.
2. **Active positions**: rows selected by the current Nitro mask and therefore visited by downstream kernels.
3. **Materialized positions**: rows compacted into an exchange batch or Trino page.

Generic Trino operator counters describe operator boundary batches. A Nitro operator may fuse a filter, projection, join, or partial aggregation and may carry a sparse mask over a larger backing vector. Its generic input/output counters therefore cannot be compared to an unfused Trino operator merely by class name. Plan-node and pipeline traces are required to align equivalent logical boundaries.

## Initial full-board finding

The synchronized pre-fix board showed exact physical-source parity across TPC-H and near parity across ClickBench. TPC-DS had 24 queries above 1.05x, led by:

| Query | Trino physical | Nitro physical | Nitro / Trino |
|---|---:|---:|---:|
| q20 | 1,910,207 | 14,576,276 | 7.631x |
| q15 | 4,296,049 | 15,224,310 | 3.544x |
| q21 | 50,547,667 | 133,285,059 | 2.637x |
| q32 | 4,165,469 | 8,281,966 | 1.988x |
| q37 | 5,426,585 | 8,890,534 | 1.638x |
| q82 | 5,773,665 | 8,382,876 | 1.452x |

## Root cause

Nitro applied complete runtime filters before these fact scans and used Parquet min/max statistics, but it did not evaluate filters against dictionary-only row groups. A row group's min/max interval can overlap a sparse filter even when its dictionary contains no accepted value. Nitro consequently decoded entire row groups that Trino rejected from their dictionaries.

The reader now performs bounded dictionary pruning for numeric dictionary-only row groups. The admission limit is an immutable construction-owned policy; the default of 8,096 values matches Trino's bound. Mixed/plain encodings, unsupported carriers, and oversized dictionaries conservatively remain admitted.

Focused validation after the fix:

| Query | Nitro physical before | Nitro physical after | Focused Trino physical | Result |
|---|---:|---:|---:|---|
| q15 | 15,224,310 | 4,296,049 | 11,716,619 | Nitro pruned 10.93M positions; Trino's dynamic-filter timing varied in this isolated run |
| q20 | 14,576,276 | 1,910,207 | 12,805,119 | Nitro pruned 12.67M positions |
| q21 | 133,285,059 | 50,547,667 | 133,285,059 | Nitro pruned 82.74M positions |

The focused runs compare final rows and types before measuring. Physical counts need not be identical: either engine may safely prune more. The required invariants are identical results and identical active cardinality at corresponding logical boundaries; physical input should be no greater unless a documented reader capability differs.

## Artifacts

- `source-position-board-before.csv`: all-query pre-fix source audit.
- `source-position-focused-after.csv`: focused post-fix source audit.
- `q15-plan-node-flow.log`: per-plan-node and per-pipeline trace used to align fused and unfused boundaries.
- `audit-source-positions.py`: reproducible source-position extraction from operator logs.

The warmed post-fix TPC-DS sweep will replace the focused figures as the canonical comparison once complete.
