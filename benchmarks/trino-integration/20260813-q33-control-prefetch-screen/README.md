# Packed-identity control-prefetch screen

ClickBench q33 performs 99,997,497 flat grouping-table lookups, of which only four are hits. The accepted
hash-then-probe path exposes independent hashes but still serializes each lookup on its first random control-word
load. This candidate stages the first control word for a bounded tile, then commits probes and insertions in source
order. A staged word is used only when it still equals the authoritative current word, so an earlier insertion in
the tile cannot hide a duplicate or collision.

Both q33 legs used the resolved Nitro jar verified from the Trino test classpath, JDK 26, a 12 GiB heap, five
warmups, three measurements, an 8 GiB query-memory limit, exact result validation, operator CPU attribution, thread
allocation, and sampled peak memory. The candidate was measured before and after the adjacent control.

| Run | p50 wall ms | p50 CPU ms | allocation MiB | query peak MiB | mean grouping ms |
|---|---:|---:|---:|---:|---:|
| Candidate | 6,928.7 | 12,529 | 29,370.2 | 4,205.4 | 7,867.2 |
| Adjacent control | 8,103.6 | 17,120 | 29,387.6 | 4,205.4 | 11,497.4 |
| Reverse candidate | 6,226.9 | 13,230 | 29,396.7 | 4,205.4 | 7,506.3 |

Relative to the adjacent control, the two candidate runs reduce total CPU by 22.7--26.8%, wall by 14.5--23.2%,
and directly attributed grouping time by 31.6--34.7%. Allocation and query peak are unchanged. Correctness tests
explicitly cover repeated new keys whose initial empty control word was staged before the first copy was inserted.

Neighboring guards retained exact results. ClickBench q32 improved from the synchronized 4,590 CPU-ms to 4,081
CPU-ms; ClickBench q31 improved from 4,014 to 3,720 CPU-ms; and the wide TPC-DS q67 guard improved from 19,221 to
18,332 CPU-ms. Whole-query wall for q31 and q67 was affected by heap/GC timing and is not used as evidence.

Artifacts:

- `candidate-w5m3.log` and `candidate-confirm-w5m3.log`: enabled runs around the control.
- `control-adjacent-w5m3.log`: exact restored parent, verified absent from the resolved jar.
- `q31-guard-w3m3.log`, `q32-guard-w3m3.log`, and `q67-guard-w3m3.log`: correctness and CPU guards.

