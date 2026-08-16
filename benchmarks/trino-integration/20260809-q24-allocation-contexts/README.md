# ClickBench q24 allocator-context attribution

All runs used a fresh 12 GiB JVM, three warmups, the Nitro-native Parquet reader, and one measured q24 execution.
The production split geometry created 165 source operators; the 1 GiB split control created 100 whole-file sources.

| source geometry | allocator bytes | Parquet-source bytes | evaluator/functions/TopN |
|---|---:|---:|---:|
| 165 ranges | 4,828,739,091 | 4,300,463,304 | 528,275,787 |
| 100 whole files | 3,243,958,084 | 2,815,947,730 | 428,010,354 |
| range delta | 1,584,781,007 | 1,484,515,574 | 100,265,433 |

The Parquet source accounts for 93.7% of the extra-range allocation, or about 22.84 MB for each additional source.
This ruled out expression adaptation, TopN, and Nitro/Trino page egress as the primary split penalty.

`q24-contexts.log` and `q24-whole-file-contexts.log` contain the measured controls. `q24.log` is the earlier stale
artifact control and is retained only to document the local-artifact installation issue. No JFR, heap dump, or Kata
artifact was created.
