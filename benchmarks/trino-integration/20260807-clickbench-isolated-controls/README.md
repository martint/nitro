# ClickBench isolated controls and sparse exchange fix

Fresh 12 GiB JVMs used five complete warmups per backend. The control runs used five measurements; attribution and
candidate runs used one or three after the same five warmups. Results were exact and include allocation, peak-memory,
operator, boundary, and (where noted) per-driver metrics. No JFR or heap-dump artifact was created.

The isolated q16 control reversed its stale full-board wall regression: Nitro measured 0.937x Trino wall and 0.829x
CPU. With the final exchange change generalized to all Nitro output operators, q16 remained healthy at 0.927x wall /
0.844x CPU and allocated 3.82 GiB versus Trino's 8.56 GiB.

The initial q23 control reproduced a real mismatch: Nitro was 1.141x wall despite 0.719x CPU and 32.5 GiB allocation
versus Trino's 53.1 GiB. The scan/filter pipeline emitted only 7,128 rows from 99,997,497 inputs, but published 2,814
native batches retaining a cumulative 23.4 GiB of producer vector trees. Per-driver attribution placed the critical
path in that source/exchange pipeline rather than aggregation.

A blanket Page-output A/B measured 0.665x wall / 0.626x CPU and only 8.88 GiB allocation. A retained-bytes admission
candidate improved q23 to 0.890x wall but exposed the missing case: its 1,191 non-empty sparse batches correctly fell
back to compact Page output, while 1,623 zero-row native batches still retained 10.6 GiB. Dropping empty batches and
admitting native ownership transfer only when retained backing is bounded per selected position produced the final
result:

| Variant | Nitro/Trino wall p50 | Nitro/Trino CPU p50 | Nitro allocation |
|---|---:|---:|---:|
| Original native exchange | 1.141x | 0.719x | 32.47 GiB |
| Retained-bytes admission only | 0.890x | 0.661x | 18.75 GiB |
| Admission plus empty-output elimination | **0.622x** | **0.623x** | **8.65 GiB** |

The final wall ratio now matches the CPU ratio: the SQL integration preserves the operator speedup instead of losing
it to sparse backing retention. Dense q16 continues to use native exchange, so the rule is representation-driven and
not query-specific.

`q16-control.log` and `q23-control.log` are the original isolated controls. `q23-driver-attribution.log` contains the
critical-path evidence. `q23-page-remote-output-ab.log` is the blanket Page A/B,
`q23-capture-admission-candidate.log` is the intermediate candidate, and `q23-empty-output-fix.log` plus
`q16-capture-admission-control.log` validate the final implementation.
