# Cross-join plan-fidelity board

This board measures Trino commit `1bea4d01` (`Preserve nested-loop plans for
cross joins`) against Nitro `8eda7e24`.  Every suite used a 12 GiB test JVM,
five warmups, three measured executions, alternating engine order, exact result
comparison, thread-allocation measurement, peak query/heap sampling, and
operator/boundary metrics.

| Suite | Queries | Nitro / Trino wall | Nitro / Trino CPU | Previous wall | Previous CPU |
| --- | ---: | ---: | ---: | ---: | ---: |
| TPC-H | 22 | 0.696x | 0.682x | 0.700x | 0.681x |
| TPC-DS | 103 | 0.563x | 0.389x | 0.583x | 0.393x |
| ClickBench | 43 | 0.893x | 0.698x | 0.871x | 0.684x |

Ratios are geometric means of per-query p50 ratios, matching the established
board convention.  All 168 query variants passed exact comparison with zero
test failures or skips.  Sum-weighted wall/CPU ratios were 0.672x/0.723x for
TPC-H, 0.649x/0.525x for TPC-DS, and 0.778x/0.773x for ClickBench.

TPC-DS q09 improved from the previous 2.46x wall-time regression to 0.578x wall
and 0.467x CPU in the complete in-sequence run.  The suite aggregate improved
about 3.4% relatively on wall and 1.1% on CPU.  Current TPC-DS wall laggards are
q30 (1.306x), q81 (1.217x), q57 (1.139x), q23a (1.056x), q85 (1.047x), q02
(1.038x), q10 (1.035x), q83 (1.033x), and q78 (1.020x).  q20 is the only
material CPU regression at 1.216x, but it is faster on wall at 0.865x.

ClickBench's current CPU regressions are q19 (1.096x), q40 (1.092x), and q05
(1.024x).  Its largest wall ratios are dominated by short queries: q43
(1.768x), q41 (1.471x), q07 (1.428x), q01 (1.296x), q39 (1.270x), and q42
(1.231x).  Longer q19 remains faster on wall at 0.803x despite its CPU delta.

The original paired TPC-H log contained non-CPU idle waits on q02, q11, and
q16.  In that run Trino q16 took 7.749 s wall with 0.694 s CPU; an immediate
paired control moved the same approximately 7.5 s wait to Nitro while CPU stayed
normal.  Engine-isolated five-warmup/three-measurement controls were normal for
both engines and supply the accepted rows for those three queries.  Their logs
are named `tpch-q02-*-only.log`, `tpch-q11-*-only.log`, and
`tpch-q16-*-only.log`.  The uncorrected paired TPC-H wall aggregate must not be
used.

Maximum measured query peak memory was 1,205.8 MiB Trino / 1,117.2 MiB Nitro
for TPC-H, 1,758.6 / 2,153.4 MiB for TPC-DS, and 4,776.3 / 5,921.9 MiB for
ClickBench.  Maximum sampled heap was near the 12 GiB ceiling for both engines
in all suites.  Mean per-query p50 query peak was 216.7 / 111.2 MiB for TPC-H,
156.2 / 119.6 MiB for TPC-DS, and 577.5 / 629.9 MiB for ClickBench.

`tpch-stale-installed-parent.log` and
`tpcds-aborted-stale-installed-parent.log` are explicitly invalid: the test
module had resolved the previously installed Trino snapshot.  After installing
`1bea4d01`, `q09-installed-verify.log` confirmed the fix before the accepted
sweeps.  No JFR, heap dump, or Kata artifact was created.
