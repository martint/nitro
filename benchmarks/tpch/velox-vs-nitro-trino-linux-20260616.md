# TPC-H whole-query: Velox vs Nitro vs Trino — Linux sf10, single-thread, 2026-06-16

Velox via its own `velox_tpch_benchmark` (TpchQueryBuilder) reading the same sf10 parquet at
/root/data/tpch-parquet-sf10/sf10, pinned single-thread (num_drivers=1 num_io_threads=1
num_splits_per_file=1, bm_min_iters=3). Nitro/Trino from the three-engine sweep (same data, JMH -f 1).
Velox runs its OWN canonical TPC-H plans (not the Trino-shaped operator trees the Nitro/Trino harnesses
use), so per-query ratios reflect plan differences too; the geomean is the aggregate signal.

## Geomean ratio to Velox (>1 = slower than Velox)
| engine               | geomean / Velox |
|----------------------|-----------------|
| Nitro interpreted    | 1.95x           |
| Trino                | 1.90x           |
| Nitro compiled       | 1.32x           |

Velox is the fastest on TPC-H whole-query: ~2x ahead of both JVM operator engines (Nitro interpreted ~=
Trino), ~1.3x ahead of Nitro's compiled engine. This is the JVM-vs-native gap, dominated by scan/decode
(consistent with the operator-level finding that Nitro's operators are competitive but its Parquet
scan/decode trails). Nitro's compiled engine is the closest to Velox.

Velox raw (ms/query, single-thread): ~/notes/velox/results-tpch.txt
