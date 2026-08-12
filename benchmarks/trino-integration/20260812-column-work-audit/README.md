# Per-column source-work audit

This directory contains opt-in Nitro Parquet source traces used after physical-position parity was established.
`summarize_source_work.py` aggregates full and selected decode positions, dictionary predicate work, published and
copied values, lazy omissions, null examinations, and decompression. Pass `--by-source` to retain individual scan
instances; this is required for queries that scan the same table in multiple subplans.

Benchmark fixture setup is part of a process-wide diagnostic trace. In particular, the TPC-H and ClickBench
performance fixtures run `ANALYZE` before measurements. Query-only correctness runs or per-source plan attribution
must be used before treating a column total as query work. The initial ClickBench q16 XML intentionally remains as
evidence of this contamination; `clickbench-q16-source-work-clean.log` is the isolated query trace.

The q03 control/candidate artifacts test a 50% versus 60% filtered-payload bulk-decode cutoff. The candidate avoids
about 83 million value decodes but loses on CPU, instructions, cycles, and branch misses, so it was rejected and the
production default was not changed.
