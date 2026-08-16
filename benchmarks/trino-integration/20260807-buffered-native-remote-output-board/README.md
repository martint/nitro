# Buffered native remote-output board

This board measures the committed buffered native remote-output path with fresh 12 GiB JVMs, five warmups, and three
measurements per query. Every query uses the SF10 Parquet inputs, passes exact result comparison, and satisfies the
fully-Nitro compute assertion.

| Suite | Queries | Nitro/Trino wall geomean | Nitro/Trino CPU geomean |
| --- | ---: | ---: | ---: |
| TPC-H | 22 | 0.729 | 0.700 |
| TPC-DS | 103 | 0.580 | 0.403 |
| ClickBench | 43 | 0.881 | 0.703 |

Fresh isolated controls distinguish suite-order contamination and near-parity queries from structural regressions:
TPC-H q03 measured 0.990x wall / 0.908x CPU; TPC-DS q20 0.613x / 0.123x; ClickBench q12 1.181x / 0.977x, q19
0.783x / 1.007x, and q40 0.664x / 1.126x. q40's earlier counters showed equal IPC and no branch- or cache-miss
regression, so its CPU delta is not accompanied by a wall regression.

The follow-up remote-output change preserves Trino's authoritative partition assignments but avoids native
destination compaction followed by a second host-buffer copy for flat streams. It converts a flat batch once and
applies the precomputed assignments to Trino's existing destination builders. Dictionary- or RLE-encoded streams keep
the native compacting path because preserving their physical encoding can avoid substantially more value copying.
TPC-DS q23a improved from 1.108x wall / 1.082x CPU in the initial isolated control to 1.085x / 1.052x; TPC-DS q22
remained a strong win at 0.770x / 0.667x, with a 0.373x allocation ratio and nearly equal query peak memory.

An adaptive partial-aggregation experiment was rejected. Sampling logical input bytes and directly materializing
portable intermediate state made q23a roughly neutral, but caused q22 to regress to 1.331x wall / 1.654x CPU because
the controller disabled aggregation early and the identity-output path was more expensive than continued reduction.
All experimental controller, protocol, decimal-state, moved-key, and VarHandle changes were removed. The accepted
remote-output slice passed 252 focused Trino/Nitro execution tests.

Function-owned direct lowering of raw inputs to portable initial partial state was subsequently accepted. This removes
the synthetic aggregation state previously allocated for each identity-output row while leaving the adaptive
controller and its admission policy unchanged. In adjacent five-warmup/three-measurement controls, q23a measured
1.034x wall / 1.022x CPU and q22 measured 0.766x / 0.672x; allocation ratios were 0.761x and 0.372x respectively.
The Nitro and Trino slices are commits `8eda7e24` and `ad800644`. Hardware counters changed by only about 0.3% on
q23a, leaving Nitro at roughly 1.40x instructions and 1.54x branches despite near-equal query CPU.

Two follow-up candidates were rejected. Transferring ownership of the dense grouped stream retained the complete
upstream dictionary backing, increasing Nitro q23a query peak memory to 3,090 MiB and producing 1.077x wall / 1.061x
CPU; q22 remained 0.781x / 0.675x. A null-free dense specialization of decimal initial-state serialization measured
1.068x / 1.026x on q23a, neutral against the accepted implementation. These controls rule out both blanket
dictionary-backed zero-copy transfer and decimal null-mask handling as the primary remaining q23a cost.

The raw logs, including rejected-candidate guardrails, remain beside this file. No JFR or heap-dump artifact was
created.

A symbolized q23a process profile localizes the remaining Nitro work. Flat grouping is the largest named engine
kernel, while decimal accumulation is only about 1.5% of sampled cycles. Nitro allocates materially fewer total bytes
than Trino (roughly 57 versus 72 GiB per query), but its faster allocation bursts repeatedly reach the 12 GiB heap
ceiling and incur more G1 work. Operator timing must also be read as pipeline timing: Nitro aggregation `getOutput`
drains deferred input evaluation and grouping, so comparing that field alone with Trino's output materialization is
invalid. Combined aggregation plus projection is the relevant comparison.

Three further controls were rejected. Compacting encoded grouping columns in the initial-state path measured 1.144x
wall / 1.074x CPU and left peak memory unchanged. Pre-scanning substring null/error streams to skip per-row control
checks increased CPU and allocation. Declaring Trino substring strictly null-propagating was semantically valid but
did not activate dictionary peeling for q23a's physical input shape; the two identity partial nodes remained at about
1.4--1.6 CPU-seconds each. All three code candidates were removed. An allocator-only diagnostic found an 8 MiB local
pool cap about 3% faster than the 64 MiB default on one five-warmup/five-measurement Nitro run, but zero retention lost
that CPU improvement and added about 5.3 GiB of allocation. None of the caps reduced the heap high-water, so no
allocator default was changed from this single-query diagnostic.
