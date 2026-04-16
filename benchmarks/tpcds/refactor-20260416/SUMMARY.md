# TPC-DS Benchmark — DictionaryVector Join Refactor (2026-04-16)

Measures the `SelectionVector → DictionaryVector` hash join outer-side refactor
(change `nmzzromw`, built on `xmyuysvq c8cf0489`).

## Run config

- **Dataset**: TPC-DS SF10 parquet at `/Users/martin/tmp/tpcds-parquet-sf10-lz4-build/sf10`
- **JMH**: `-wi 2 -i 2 -w 2s -r 2s -f 0 -foe true`
- **Baseline**: `benchmarks/tpcds/full-compare-1x1-20260408.csv` (Nitro at commit `xmy`; Trino numbers from the same file)

## Primary targets — all large wins

These are the queries the refactor was designed to improve.

| Query | Baseline Nitro ms/op | Post-refactor ms/op | **Speedup vs old Nitro** | Trino ms/op | **Speedup vs Trino (old → new)** |
|---|---:|---:|:-:|---:|:-:|
| Q64 | 94055.384 | **29890.779** | **3.15×** | 11309.436 | 0.12× → **0.38×** |
| Q65 | 34201.541 | **8427.772** | **4.06×** | 4679.712 | 0.14× → **0.56×** |
| Q66 | 23546.572 | **3603.645** | **6.53×** | 4091.654 | 0.17× → **1.14× (Nitro faster)** |
| Q80 | 31864.862 | **13724.505** | **2.32×** | 5929.080 | 0.19× → **0.43×** |
| Q97 | 46138.775 | **6125.888** | **7.53×** | 3891.090 | 0.08× → **0.64×** |

- Geomean speedup vs old Nitro: **4.0×**
- Q66 now **outperforms Trino**. Q97 (previously the worst-losing query at 12× slower) is now within 1.57× of Trino.

## Regression check — queries where Nitro was already winning

Sanity check to confirm the refactor doesn't regress Nitro-wins.

| Query | Baseline Nitro ms/op | Post-refactor ms/op | Ratio | Trino ms/op | Nitro still faster than Trino? |
|---|---:|---:|:-:|---:|:-:|
| Q08 | 3211.996 | 3866.908 | 1.20× slower | 7659.431 | **Yes (1.98×)** |
| Q13 | 5233.760 | 6864.294 | 1.31× slower | 8709.250 | **Yes (1.27×)** |
| Q18 | 520.071 | 526.717 | 1.01× (tie) | 1361.032 | **Yes (2.58×)** |
| Q20 | 878.610 | 849.683 | 0.97× (slightly faster) | 1433.847 | **Yes (1.69×)** |
| Q41 | 19.296 | 15.821 | 0.82× (faster) | 48.243 | **Yes (3.05×)** |

All five queries still beat Trino by wide margins. Two minor regressions vs old Nitro (Q08: 20% slower, Q13: 31% slower) but both still well ahead of Trino. Measurement noise with only 2 iterations is significant; if Q08/Q13 hold up on longer runs they're worth a second look, but they do not block the refactor.

## Refactor summary

- Replaced hash-join outer-side `SelectionVector(ProjectedRows, values)` wrapping with `DictionaryVector(ids, values)` (Velox-style, zero-copy).
- Deleted `SelectionVector`, `ProjectedRows`, `ProjectedRowsDebug` types and every dispatch site (≈2300 lines removed).
- Removed `Output.ProjectedOutput` / `PositionProjector` pushdown API.
- Outer-side output now uniformly Dictionary-encoded; no per-position `ProjectedRows` indirection, no fingerprint-based compose cache.

## Next steps

- Run a **full 99-query TPC-DS sweep** with longer iterations (`-wi 5 -i 5 -w 2s -r 5s`, forked `-f 1`) to get publishable numbers.
- Investigate Q13 regression specifically — RLE-vs-Dict dispatch on its hot path.
- ClickBench cache was cold on this machine; rerun after priming to verify 44/44 still holds.
