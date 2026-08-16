# ClickBench q33 16-byte control-group screen

This screen tested two adjacent 8-byte Swiss-table control words per probe iteration while retaining the accepted packed-record representation and hash/equality behavior.

The candidate passed the focused flat-grouping and distinct-key tests (65 tests total), then ran with five warmups and three measured executions:

| implementation | p50 wall (ms) | p50 CPU (ms) | allocation (MiB) | query peak (MiB) | mean grouping (ms) |
|---|---:|---:|---:|---:|---:|
| accepted 8-byte control | 8,705.398 | 17,208 | 29,370.469 | 4,205.408 | 12,087.99 |
| 16-byte candidate | 8,601.197 | 17,988 | 29,378.413 | 4,205.406 | 12,045.16 |

The attributed grouping difference was only -0.36%, within run noise, while total query CPU increased 4.5%. Allocation and peak memory were unchanged. The candidate was rejected and the production implementation restored.

The result indicates that q33's remaining lookup cost is not caused primarily by the number of dependent control-group iterations. Simply widening the control scan does not address the random key-record access and memory latency that dominate the profile.
