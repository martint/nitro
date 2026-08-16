# Bound LIKE literal-contains specialization

ClickBench q21 is `COUNT(*) WHERE URL LIKE '%google%'`.  The SQL-shaped operator benchmark represented the predicate
with Nitro's `contains_utf8` function and measured Nitro at 5,588.8 ms versus Trino at 11,206.5 ms (0.499x).  The
Trino integration correctly bound the constant pattern dynamically, but `BoundLikeUtf8` used the generic per-row LIKE
matcher and the preceding isolated SQL run measured only 0.898x wall / 0.928x CPU.

Core commit `0936b2f8` recognizes the general one-literal `%literal%` LIKE shape at function construction and compiles the
literal into Nitro's existing constant-needle containment kernel.  Flat null/error-free all-mask inputs use the dense
concatenated-buffer sweep; dictionary, RLE, sparse, nullable, and error-bearing inputs retain their encoding-aware or
row-aware paths.  Multi-segment LIKE patterns remain on the generic matcher.  There is no query, table, column, Trino
type, or host-plan knowledge in the specialization.

The paired SQL run used the native Nitro Parquet reader, exact result comparison, five warmups, three alternating
measurements, a fresh 12 GiB JVM, and an 8 GiB query-memory limit.

| engine | p50 wall ms | mean CPU ms | allocation MiB | median query peak MiB |
|---|---:|---:|---:|---:|
| Nitro | 965.190 | 6,217.667 | 3,978.801 | 60.037 |
| Trino | 1,919.448 | 12,084.333 | 25,141.714 | 80.000 |
| Nitro / Trino | 0.503 | 0.515 | 0.158 | 0.750 |

The Nitro source pipeline reports 6,180--6,870 CPU-ms per measurement, down from about 11,250 ms before the change.
The resulting 1.94x SQL CPU speedup is within 3% of the operator benchmark's 2.005x speedup, so this closes the q21
operator/SQL-shape discrepancy.  `clickbench-q21.xml` contains the complete command properties, per-plan operator CPU,
native metrics, allocation, and memory samples.  No JFR, heap dump, or Kata artifact was created.
