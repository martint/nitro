# ClickBench q24 TopN input ownership

All q24 runs used fresh 12 GiB JVMs, five warmups, five measurements, the Nitro
Parquet source, thread-allocation measurement, peak-memory measurement, and
operator-level CPU/metrics.

Deferring dense TopN payload copies until candidates settled was neutral and was
removed. Its run (`q24.log`) measured 5229.343 ms wall p50, 21220 ms CPU p50,
and 36512.036 MiB allocation p50.

The structural issue was at the Trino/Nitro boundary: a streaming terminal owns
the batches passed to it, but `TrinoNitroTopNTerminal` did not close batches
after `TopNSession` copied its retained rows. Closing each consumed batch makes
its vectors available for reuse.

The accepted close-only state (`q24-close-only.log`) measured:

- wall p50: 5102.537 ms (3.4% below the 5279.459 ms adjacent baseline)
- CPU p50 / mean: 21312 / 21288 ms (flat)
- thread allocation p50: 35680.788 MiB (2.0% below baseline)
- peak memory p50: 143.691 MiB (16.8% below baseline)
- Nitro allocator allocation: 4,828,739,091 bytes (10.5% below baseline)

`q24-close.log` contains the same lifecycle fix plus the rejected payload-copy
experiment and corroborates the allocation reduction.

No JFR, heap dump, or Kata review artifacts were created.
