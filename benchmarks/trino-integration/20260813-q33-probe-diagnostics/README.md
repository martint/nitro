# ClickBench q33 flat-table probe diagnostics

A one-shot instrumented build counted exact lookup outcomes for the two source grouping tables. The resolved Nitro artifact was verified on the Trino test classpath before execution. Instrumentation was removed after capture; timing from this run is intentionally not used.

| metric | table 1 | table 2 | combined |
|---|---:|---:|---:|
| lookups | 49,989,484 | 50,008,013 | 99,997,497 |
| existing-key hits | 1 | 3 | 4 |
| new-key misses | 49,989,483 | 50,008,010 | 99,997,493 |
| 8-byte control groups examined | 65,008,431 | 64,982,002 | 129,990,433 |
| full-hash candidates | 321,736 | 320,721 | 642,457 |
| final capacity | 67,108,864 | 67,108,864 | — |

Thus 99.999996% of lookups insert a new group, each lookup examines 1.300 control groups on average, and only 0.642% reaches a full-hash candidate. Exact key equality is negligible. The dominant opportunity is latency hiding for the first random control-table access and the subsequent random slot/record writes, with insertions committed in source order to preserve dense group identities and exact duplicate handling.
