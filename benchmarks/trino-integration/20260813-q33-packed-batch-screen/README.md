# ClickBench q33 packed batch-loop screen

This screen tests whether keeping the admitted pair keys in locals across hash, packed-record probe, comparison, and
insertion improves q33. The temporary implementation retained exact equality, the accepted packed record and hash
slot representation, provider-owned I32 storage, and dense first-seen group ids. It passed all 52 focused flat
grouping tests, including repeated high-cardinality keys across rehashes.

| Representation | p50 wall ms | p50 CPU ms | allocation MiB | query peak MiB | mean grouping ms |
|---|---:|---:|---:|---:|---:|
| Fused packed pair loop | 8,724.8 | 16,795 | 29,402.1 | 4,204.8 | 12,201.0 |
| Adjacent accepted control | 8,705.4 | 17,208 | 29,370.5 | 4,205.4 | 12,088.7 |

The candidate is rejected. Although total query CPU happened to be lower, the directly attributed grouping phase
regressed 0.9%; source and output variation produced the query-level movement. The accepted path deliberately
separates batch hashing from probing so independent work can overlap cache misses. Keeping keys in locals removes
accessor calls but loses that memory-level parallelism. No candidate code remains.

Artifacts:

- `candidate-w5m3.log`: temporary fused packed pair loop.
- `../20260813-q33-direct-generated-screen/control-w5m3.log`: immediately preceding accepted control.
