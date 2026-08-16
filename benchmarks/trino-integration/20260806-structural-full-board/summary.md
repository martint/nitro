# Structural full board — 2026-08-06

Configuration: SF10 Parquet inputs, both engines in one runner, five warmups and three measurements per query,
12 GiB JVM heap, ClickBench query-memory cap of 8 GiB, operator CPU attribution, thread allocation, query peak
memory, and Nitro boundary/encoding metrics. All 168 queries passed with correct results.

| Suite | Queries | Nitro/Trino wall geomean | Nitro/Trino CPU geomean |
|---|---:|---:|---:|
| TPC-H | 22 | 0.726x | 0.683x |
| TPC-DS | 103 | 0.568x | 0.391x |
| ClickBench | 43 | 0.859x | 0.685x |
| All suites | 168 | 0.652x | 0.485x |

The TPC-H sweep was captured immediately before commit `636b758c` preserved Nitro RLE at Page egress. Its
q21/q22 post-change gate moved all 5.24 million RLE channel-positions through the Page and exchange boundaries;
q21 CPU fell from 6.67 to 6.36 seconds and q22 stayed near 0.70 seconds. TPC-DS and ClickBench include the fix.

Maximum measured median query peak memory:

| Suite | Nitro | Trino |
|---|---:|---:|
| TPC-H | q18, 825.8 MiB | q09, 1,021.0 MiB |
| TPC-DS | q23a, 2,249.4 MiB | q23b, 1,728.5 MiB |
| ClickBench | q33, 5,921.9 MiB | q33, 4,724.4 MiB |

Encoding preservation is effectively complete. TPC-DS retains billions of dictionary/flat positions and 22.1
million RLE positions across native-to-Page egress. ClickBench moves only 945 of 5.24 billion instrumented
channel-positions from dictionary to flat. The remaining bulk boundary is Page serialization for remote exchange,
not accidental dictionary/RLE flattening inside Nitro pipelines.

ClickBench CPU regressions are q05 at 1.009x, q19 at 1.042x, and q40 at 1.000x; prior fresh-JVM controls put all
three at parity. q24 is 0.766x CPU and 1.031x wall, reproducing the earlier driver result rather than its old 1.3x
wall outlier. Major GCs contaminate wall/allocation readings for the largest ClickBench queries; CPU attribution
and correctness remain useful, while wall outliers require isolated controls.

An RLE-ingress prototype was tested after the sweep. TPC-DS has 35.5 million incoming Page RLE positions, but
globally retaining them as Nitro RLE vectors was neutral on q97 and made q75 8.5% slower in a same-host 5W5M A/B.
Repeated downstream grouping access benefits from one-time flattening, so the prototype was removed.
