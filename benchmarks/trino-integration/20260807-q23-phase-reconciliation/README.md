# TPC-DS q23 aggregation-phase reconciliation

Fresh 12 GiB JVM runs used five complete warmups, exact SF10 Parquet results, allocation and peak-memory sampling,
and per-plan-node operator metrics. No JFR or heap-dump artifact was created.

The paired control measured Trino at 6,037.8 ms wall / 27,654 ms query CPU and Nitro at 6,300.7 ms / 28,415 ms,
or 1.044x wall / 1.028x CPU. Nitro allocated 51.9 GiB versus Trino's 71.2 GiB. This reproduces a small residual gap,
not the approximately 1.17x in-sequence wall ratio from the complete board.

Low-overhead elapsed phase counters localize the two repeated high-cardinality partial aggregations. Their fused
input pipelines consume only about 3 ms each and aggregate accumulation consumes 2--7 ms. Early hash grouping costs
roughly 0.5--0.9 seconds per node. After adaptive partial aggregation disables grouping, lowering raw rows to initial
intermediate state dominates: grouping-key copying costs roughly 1.3 seconds per node while decimal intermediate-state
construction costs only about 10 ms.

The physical-encoding split is decisive. In the pass-through path, the two flat integer/date keys together consume
only 19--27 ms per node. The dictionary-backed `substr(i_item_desc, 1, 30)` key accounts for 98.5--99.1% of key-copy
time. A dense flat ownership-transfer candidate did not activate on q23's sparse branch masks and was removed; it was
neutral on wall and CPU. Earlier controls already rejected blanket dictionary ownership transfer, compact per-batch
dictionaries, and low-level decimal/copy variants because they retained large upstream dictionaries, added remapping
work, or regressed q22.

The remaining impedance mismatch is therefore encoded intermediate-key transport, not function adaptation or
aggregate execution. Nitro materializes the selected dictionary key before the partial/final boundary, whereas an
effective future design must preserve exact dictionary identity across the boundary without retaining every upstream
dictionary generation or repeatedly serializing the dictionary payload. That likely requires a bounded, reusable
dictionary transport protocol at the native exchange boundary, or an optimizer-proven functional-dependency rewrite;
another local copy-loop specialization is not supported by the measurements.

`q23a-phase-control.log` is the paired control. `q23a-nitro-internal-phases.log`,
`q23a-nitro-initial-phases.log`, and `q23a-key-encoding-phases.log` contain the successive attribution runs.
`q23a-flat-transfer-candidate.log` is the removed negative candidate. The last encoding run's 9.4-second wall time is
invalid as a duration comparison because multiple 10--13 second application intervals were interrupted by major-GC
pauses; its 28,198 ms query CPU and phase proportions remain useful attribution evidence.
