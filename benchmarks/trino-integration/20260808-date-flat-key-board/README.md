# Post-DATE flat-key full benchmark board

This directory contains the complete warmed SQL board after Trino commit
`3258b36b` (`Publish Trino date flat-key storage`) with Nitro core commit
`23e32d77` (`Preserve sparse multi-run join dictionaries`). All 168 SF10
Parquet/ClickBench query variants passed exact cross-engine comparison.

Each query used five query-specific warmups and three alternating measurements
in a 12 GiB JVM with an 8 GiB query-memory limit. The harness recorded query
wall time, query CPU, thread allocation, sampled query/process peak memory, and
operator/boundary metrics by plan node. No JFR, heap dump, or Kata artifact was
created.

| Suite | Queries | Wall geomean | CPU geomean | Weighted wall | Weighted CPU | Wall wins | CPU wins |
|---|---:|---:|---:|---:|---:|---:|---:|
| TPC-H | 22 | 0.720x | 0.688x | 0.695x | 0.730x | 22/22 | 22/22 |
| TPC-DS | 103 | 0.588x | 0.408x | 0.683x | 0.556x | 91/103 | 100/103 |
| ClickBench | 43 | 0.843x | 0.664x | 0.745x | 0.730x | 33/43 | 41/43 |

Geometric ratios use per-query p50 wall and CPU ratios. Weighted ratios use
the sums of per-query mean wall and mean CPU. Ratios below one favor Nitro.

Valid p50 thread-allocation geomeans are 0.173x for TPC-H (21 rows), 0.185x
for TPC-DS (97 rows), and 0.329x for ClickBench (41 rows). The excluded rows
contain impossible negative deltas caused by the known thread-retirement and
counter-population defect: TPC-H q13; TPC-DS q19, q20, q39a, q54, q79, and
q80; ClickBench q14 and q35. They are retained in the XML but are not treated
as allocation measurements.

Maximum sampled per-query peak memory was 1,153.535 MiB Trino (q09) versus
880.894 MiB Nitro (q18) for TPC-H; 1,734.056 MiB Trino (q23b) versus
2,097.368 MiB Nitro (q23a) for TPC-DS; and 4,746.427 MiB Trino versus
5,540.548 MiB Nitro (both q33) for ClickBench. These are individual-query
peaks, not simultaneous suite retention.

The raw TPC-DS CPU losses are q20 (1.255x), q23b (1.084x), and q23a (1.074x).
They have current fresh-JVM controls: q20 reverses strongly; q23a is 0.957x
CPU in separate JVMs; and the adjacent DATE-capability q23b A/B is 0.908x CPU,
0.908x allocation, and 0.687x sampled query peak versus the parent. The raw
ClickBench CPU losses are q19 (1.065x) and q40 (1.167x). Prior warmed fresh-JVM
controls put q19 at practical parity (1.011x) and clear q40 as sequencing
variance. Neither query can activate DATE flat-key storage.

Relative to the preceding corrected provider-flat-key board (0.716x/0.689x,
0.586x/0.406x, and 0.851x/0.681x wall/CPU), aggregate movement is small and
not an activation signal outside DATE-bearing plans. The directly activating
q23 controls remain the acceptance evidence: each high-cardinality final
aggregation falls from 633.6 to 607.2 MiB, q23a sampled query peak falls about
4.7%, and q23b benefits cascade to a 31.3% lower sampled query peak without a
CPU penalty.

Raw Surefire XML:

- `tpch.xml`
- `tpcds.xml`
- `clickbench.xml`
