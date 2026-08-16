# Lazy dictionary copy experiment

Rejected experiment against the accepted q23a baseline in
`../20260808-adaptive-compact-long-record/q23a-physical-refined.log`.

The experiment composed sparse-mask and nested-dictionary selections through
`SelectedPositions` and materialized binary values directly, avoiding the legacy
intermediate dictionary-position arrays while retaining an owned dense result.

| run | wall p50 ms | CPU p50 ms | allocation p50 MiB | peak memory p50 MiB |
| --- | ---: | ---: | ---: | ---: |
| accepted baseline | 5806.072 | 27183 | 47100.115 | 2527.487 |
| growth-capacity candidate | 5765.618 | 28075 | 49230.261 | 2570.956 |
| exact-capacity candidate | 5776.505 | 28227 | 47819.426 | 2487.354 |

The initial candidate unintentionally used append-oriented growth capacity for
one-shot copies. Exact allocation removed most of that allocation increase, but
CPU remained about 4% worse than the accepted baseline and allocation remained
about 0.7 GiB higher. Repeated lazy nested-ID mapping was not competitive with
the compact position arrays. All production and test changes were restored; the
accepted Nitro artifacts were reinstalled afterward.

No JFR or heap-dump artifacts were created.
