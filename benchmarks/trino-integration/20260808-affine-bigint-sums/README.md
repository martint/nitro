# Exact affine integral sum fusion

ClickBench q30 contains 90 sums over `ResolutionWidth + constant`. `ResolutionWidth` is physically and logically
Trino `INTEGER`: each addition therefore has checked integer overflow semantics, and the result is widened to BIGINT
before `sum`. The first fusion rule handled only expressions whose addition was already BIGINT. It passed its synthetic
BIGINT tests but did not activate on the real q30 plan. The earlier 965 ms / 6.25 CPU-second result is retained as an
inactive control and must not be attributed to that rule.

The registry-owned rule now recognizes both native BIGINT affine expressions and BIGINT-widened INTEGER affine
expressions. It compiles only the common base and supplies a multi-output physical aggregation; the generic planner
still knows no function, query, table, column, or fixed arity. The INTEGER path preserves checked integer projection
addition and checked BIGINT sum accumulation.

For a dense null-free batch, the aggregation scans the complete base vector once and computes its exact sum and
extrema. It derives all outputs from that shared reduction only when the extrema prove every affine term is
expression-safe and one-signed, so row-ordered accumulation is monotone and final-range checking is equivalent. Mixed
signs, nullable/sparse masks, or an unrepresentable reduction use the original checked row-order loop. Flat,
dictionary, and RLE representations use the same logical-vector contract without materialization.

After five warmups and three alternating measurements with exact result comparison, a 12 GiB JVM, and an 8 GiB query
limit:

| backend | wall p50 (ms) | mean CPU (ms) | allocation p50 (MiB) | query peak p50 (MiB) |
|---|---:|---:|---:|---:|
| Nitro | 163.212 | 263.667 | 234.344 | 0.424 |
| Trino | 3,169.689 | 21,053.667 | 70,386.642 | 8.101 |
| Nitro / Trino | 0.051x | 0.0125x | 0.0033x | 0.052x |

The SQL source operators account for about 245--249 CPU-ms across all 165 splits. The independently shaped Nitro
operator benchmark measures 236.636 ms p50 (260.302 ms mean) and 100.8 MiB allocated. SQL source CPU is therefore
within about 5% of the operator p50: the operator/SQL performance premise is reproduced after matching both topology
and expression type semantics.

`q30-paired-integer.log` is the final paired report and `nitro-operator.json` is the operator result. Earlier
`q30-paired.xml` and `q30-paired-monotone.log` are retained as inactive-rule controls. The Nitro suite passes 1,689
tests with zero failures and 567 skipped; the Trino core Nitro cohort passes 261 tests. No JFR, heap dump, or Kata
artifact was created. The accepted Nitro harness and Trino integration commits are `76e9ab5b` and `b91d5a45`.
