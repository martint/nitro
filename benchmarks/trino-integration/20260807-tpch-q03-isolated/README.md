# TPC-H q03 isolated warmed control

Clean accepted Nitro `23e32d77` and Trino `75bf4c27`; SF10 Parquet, 12 GiB JVM,
8 GiB query limit, five warmups, and seven alternating measurements.

Nitro measured 575.178 ms mean wall / 2,851.714 CPU-ms versus Trino's
652.386 / 3,074.286 (0.881x / 0.928x). Wall medians were 572.153 / 620.556
(0.922x). Thread allocation was 1,468.426 / 8,103.076 MiB (0.181x), and
median sampled query memory was 5.905 / 170.558 MiB.

The long-lived full-board q03 CPU loss does not reproduce after query-specific
warmup. No code change was made; no JFR, heap dump, or Kata artifact was
created.
