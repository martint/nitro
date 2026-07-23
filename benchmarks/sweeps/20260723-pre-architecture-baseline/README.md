# Pre-architecture performance baseline

This directory captures the source-identical performance baseline immediately
before Nitro's core, connector, function, type, execution, and Trino-adapter
abstractions are changed.

Measurement policy:

- JDK `/opt/java/openjdk`, version 26;
- Nitro and Trino unpinned, one JMH thread, `-Xmx12g`, transparent huge pages;
- five one-second warmups, five one-second measurements, one fork for the full
  broad sweep;
- duration, JMH allocation/GC metrics, and the explicit eight-event hardware
  bundle captured in each JVM invocation;
- Velox pinned to CPU 0, one driver and one I/O thread, one split per file, ten
  repeats, 12 GiB cache;
- Velox duration, managed task-pool allocation statistics, the same eight
  hardware events, host memory, and peak RSS retained per query;
- engines run serially, with a 16 GiB available-memory admission check before
  every suite/engine leg.

Any noisy, incomplete, or failed rows are recaptured separately with the same
engine policy and three forks/repeats as appropriate. No incomplete row is
published.

The sweep completed on 2026-07-23. The fail-closed generator validated 165
query rows, all JVM allocation/GC metrics, all Velox managed allocation/peak
metrics, every peak-memory field, and all eight hardware events for all three
engines. Published artifacts are:

- `operator-architecture-baseline-jdk26-20260723.md`: human-readable full board;
- `operator-architecture-baseline-jdk26-20260723.csv`: flattened machine-readable
  data for every query and metric;
- `source-manifest.md`: Nitro/Velox revisions and source/binary digests;
- `q14-correctness/normalized/`: exact Nitro/Velox q14 parity evidence;
- `recapture-q14-restored/`: current-source full Velox q14 row;
- `recapture-q34-clean/`: isolated full Velox ClickBench q34 row;
- `recapture-rss/`: peak-RSS-only overlays for queries shorter than the
  original five-second sampler interval.

The broad baseline records overall N/T at 3.682x geometric mean and 3.220x by
summed duration (165/165 wins), and N/V at 1.702x and 1.998x (163/165 wins).
The two N/V duration losses are TPC-DS q13 and q48. This ordinary one-fork
baseline is the architecture comparison point; it does not replace the
focused multi-fork overlay board.
