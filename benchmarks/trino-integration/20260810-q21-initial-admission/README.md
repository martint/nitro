# TPC-H q21 initial adaptive-admission experiment

This rejected candidate used Trino's ordinary 1x partial-aggregation memory limit only for the first adaptive
sample, then restored Nitro's accepted 2x steady-state limit after the first aggregated flush. Existing explicit
policy constructors retained their prior behavior. The run used JDK 26, five warmups, three alternating
measurements, a 12 GiB test JVM, an 8 GiB query-memory limit, exact result comparison, thread-allocation accounting,
and plan-node/operator metrics.

| Engine | p50 wall | mean CPU | p50 allocation |
| --- | ---: | ---: | ---: |
| Trino | 2029.250 ms | 6926.333 ms | 20564.837 MiB |
| Nitro | 2024.868 ms | 8008.000 ms | 11080.510 MiB |
| Nitro / Trino | 0.998x | 1.156x | 0.539x |

The accepted adjacent control was 0.975x wall, 1.168x CPU, and 0.526x allocation. Absolute Nitro CPU increased
from 7860.667 to 8008.000 ms, and the near-identity source partial aggregation still spent about 2.71 CPU-seconds
in grouping. The small ratio movement is noise rather than evidence that first-sample size accounts for q21's
CPU gap. The candidate was removed and the accepted runtime artifact restored. `q21-candidate.log` contains the
complete measurement. No JFR, heap dump, or Kata artifact was created.
