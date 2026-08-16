# ClickBench q33 direct generated-table screen

This screen tests whether a key sample already proven to contain full-width fixed-width values should instantiate
Nitro's existing generated multi-long grouping table directly, bypassing both compact-table promotion and the
accepted packed-record flat table. Both runs use JDK 26, a 12 GiB heap, five warmups, three measurements, an 8 GiB
query-memory limit, exact result validation, operator CPU attribution, thread allocation, and sampled peak memory.

| Representation | p50 wall ms | p50 CPU ms | allocation MiB | query peak MiB | process peak MiB |
|---|---:|---:|---:|---:|---:|
| Direct generated full-width | 8,164.0 | 17,130 | 31,426.4 | 4,230.2 | 10,924.3 |
| Accepted packed-record control | 8,705.4 | 17,208 | 29,370.5 | 4,205.4 | 10,515.0 |

The candidate is rejected. It improves wall time by 6.2%, but query CPU falls only 0.5%, allocation rises 7.0%,
query peak rises 0.6%, and process peak rises 3.9%. This corroborates that the existing generated row loop is not by
itself the missing CPU lever. The next useful experiment must retain the compact canonical packed-record storage and
remove repeated key access/layout callbacks within its hash/probe/insert batch loop.

No candidate production code remains.

Artifacts:

- `candidate-w5m3.log`: direct generated full-width table.
- `control-w5m3.log`: restored accepted packed-record flat table.
