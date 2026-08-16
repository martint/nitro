# Sparse-exchange post-change board

This board uses commit `c82f2235` (`Avoid retaining sparse exchange generations`). ClickBench ran in four fresh 12
GiB JVM chunks. Every backend/query combination received five complete warmups and three measurements against the
same 100-million-row Parquet data. Results were exact and include allocation, peak memory, operator CPU, boundary,
and aggregation-phase metrics. No JFR, heap dump, or Kata artifact was created.

## ClickBench summary

- 43/43 queries passed exactly.
- Nitro/Trino geometric mean: **0.838x wall, 0.658x CPU**.
- Nitro won wall on 30/43 and CPU on 41/43 in the chunked board.
- The preceding key-only-grouping board was 0.868x wall / 0.686x CPU, so this change improves both aggregate ratios.
- q23, the motivating regression, now measures **0.617x wall / 0.613x CPU** in sequence. Its fresh control is
  0.622x / 0.623x. The former 1.141x wall regression is gone.

The remaining board wall ratios above 1 are dominated by short-query fixed latency or process order. Fresh q07 with
nine measurements is 136.9 ms Nitro versus 120.1 ms Trino (1.140x wall), while Nitro uses 222 versus 430 CPU-ms
(0.516x). Fresh q40 is 207.4 versus 192.9 ms (1.075x wall) and CPU parity at 434 versus 430 ms (1.009x). The q41 and
q43 board gaps are only 22--27 ms absolute while Nitro uses 0.541x and 0.729x CPU respectively.

q19 is the only substantive residual CPU comparison. Its five-measurement isolated result is 0.797x wall and 1.013x
CPU. Nitro's raw partial composite grouping is slower, while its final grouping is faster; the opposing phases leave
a 290 CPU-ms net difference on a 22.7 CPU-second Trino query. This does not justify a query-specific grouping change,
especially while Nitro is 1.34 seconds faster on wall and allocates materially less.

The four range logs are the complete board. `clickbench-q07-isolated.log`, `clickbench-q19-isolated.log`, and
`clickbench-q40-isolated.log` contain the fresh controls used to classify the apparent laggards.
