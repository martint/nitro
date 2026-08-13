# General flat-table control-prefetch screen

This slice extends the accepted packed-identity first-control staging protocol to every flat grouping table with
prepared batch hashes. Address calculation uses the table's physical slot representation; staged control metadata is
never authoritative, and every source-order probe reloads the current word before matching or inserting. General
tables retain their existing group-id mapping, exact record equality, and rehash semantics.

TPC-DS q67 used JDK 26, a 12 GiB heap, five warmups, five measurements, an 8 GiB query-memory limit, exact result
validation, operator CPU attribution, thread allocation, and sampled peak memory. The candidate was measured before
and after an adjacent control loaded from the resolved Trino classpath.

| Run | p50 wall ms | mean CPU ms | allocation MiB | mean grouping ms |
|---|---:|---:|---:|---:|
| Candidate | 5,578.1 | 17,680.8 | 30,144.8 | 4,888.6 |
| Adjacent control | 5,629.5 | 18,031.4 | 30,203.5 | 5,078.2 |
| Reverse candidate | 5,568.4 | 17,640.2 | 29,971.1 | 4,982.3 |

Relative to the control, candidate CPU falls 1.9--2.2%, wall falls 0.9--1.1%, and directly attributed grouping falls
1.9--3.7%. Allocation is neutral to slightly lower. Query and process peak samples varied in both directions and are
not evidence for this slice.

The closest-to-parity wide-grouping guard, ClickBench q40, is neutral in an exact 10-warmup/15-measurement A/B:
399.3/401.3 ms mean CPU, 128.65/128.14 ms grouping, and 205.8/206.9 ms median wall for candidate/control. The packed
q33 path retains its larger accepted gain at 6,019 ms wall, 13,027 mean CPU-ms, and 6,844 ms mean grouping in a
three-warmup/three-measurement guard. A direct non-identity flat-table test covers repeated new keys staged against
the same initially empty control word.

Artifacts:

- `q67-candidate-w5m5.log`, `q67-control-w5m5.log`, and `q67-candidate-confirm-w5m5.log`.
- `q40-guard-w10m15.log` and `q40-control-w10m15.log`.
- `q33-guard-w3m3.log`.

