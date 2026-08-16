# Direct sparse remote-output source experiment

This experiment tested whether sparse Nitro exchange output could avoid its selected-value host materialization while
retaining Trino's destination batching. The candidate exposed borrowed flat and dictionary source blocks only during a
synchronous callback, computed assignments for the selected positions, and forced each destination appender into
direct-copy mode so no Nitro-owned dictionary or RLE backing survived the callback. Focused adapter, partitioning, and
dictionary-lifetime tests passed.

The initial 10-warmup/10-measurement pair was too small and suggested a roughly 1.7% CPU improvement. The decisive
30-warmup/50-measurement adjacent pair rejected the design:

| Nitro q30 | Wall p50 (ms) | Wall mean (ms) | CPU p50 (ms) | CPU mean (ms) | Allocation mean (MiB) |
| --- | ---: | ---: | ---: | ---: | ---: |
| direct sparse source | 239.832 | 243.716 | 474 | 474.860 | 1,624.188 |
| disabled control | 227.631 | 230.727 | 456 | 457.600 | 1,493.631 |
| candidate / control | 1.054x | 1.056x | 1.039x | 1.038x | 1.087x |

The candidate avoids selected payload materialization, but adapting the much broader physical source requires null and
dictionary bookkeeping across dead rows and creates selected partition-key blocks before destination copying. That
extra physical work exceeds the saved copy. The sampled near-8-GiB maximum occurred on both sides and is not evidence
of candidate retention. The complete prototype was removed; no production source, JFR, heap dump, or Kata artifact
was retained.
