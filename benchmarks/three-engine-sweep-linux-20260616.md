# Three-engine sweep — Linux, sf10/full, 2026-06-16

Whole-query apples-to-apples (same operator trees per query): Nitro interpreted operators vs Nitro
compiled engine vs Trino operators. Run on the Linux box (not the macOS baselines — absolute ms are
not comparable across machines; the **engine ratios within this run** are the signal). JMH `-wi 3 -i 5
-f 1` (fresh JVM per query, after the `-f 0` runs OOM'd by accumulating heap). Data: sf10 TPC-H/TPC-DS,
full 14 GB ClickBench at /root/data. CSVs: `benchmarks/<suite>/three-engine-linux-20260616.csv`.

## Geomeans (ratio to Trino; <1 = faster than Trino)

| suite      | #q | Nitro interpreted / Trino | Nitro compiled / Trino |
|------------|----|---------------------------|------------------------|
| TPC-H      | 21 | 1.08x                     | 0.70x                  |
| TPC-DS     | 95 | 0.68x  (1.47x faster)     | 0.35x  (2.86x faster)  |
| ClickBench | 36 | 0.63x  (1.59x faster)     | 0.42x  (2.4x faster)   |

Nitro's interpreted engine already beats Trino on TPC-DS and ClickBench and is within ~8% on TPC-H;
the compiled engine is well ahead on all three.

## Interpreted-engine laggards (Nitro/Trino, slower than Trino) — all scan/decode-bound
- TPC-H: q06 1.60x, q19 1.52x, q10 1.49x  (q06 is the canonical scan-bound sum/filter; compiled q06=0.94x)
- TPC-DS: q98 1.65x, q12 1.44x, q62 1.27x, q50 1.23x
- ClickBench: q02 2.11x, q08 1.74x, q20 1.38x

Consistent with the session-long finding: at the operator level Nitro is competitive/ahead; the residual
whole-query gap is the Parquet **scan/decode** path, which is exactly where the interpreted laggards cluster.

## Notes / caveats
- q21 has no interpreted-harness port in TPC-H (ran in compiled+Trino); a few TPC-DS/ClickBench queries
  likewise unported on one engine (counts above use the intersection per ratio).
- First ClickBench attempt produced empty CSVs: the exec profile hardcodes
  `-Dnitro.clickbench.hits.path=${nitro.clickbench.hits.path}`, which expands to empty unless passed as a
  Maven property (env via JAVA_TOOL_OPTIONS is overridden by the later empty -D). Pass it as `-D` on mvnd.
- Velox is NOT in this sweep (it's the operator-level peer; whole-query Velox harnesses are being built
  under ~/notes/velox).
