# Nested join-copy input-offset correction

A warmed TPC-DS q65 screen exposed an intermittent Nitro failure before measurement:
`Index 37175 out of bounds for length 37175`. The stack reached `JoinBufferSupport.copyLongPositions()` through a
struct-valued retained join payload while native exchange capture was inspecting the lazy output. The same query had
passed both preceding full boards, making this an execution-order-sensitive multi-run shape rather than unsupported
SQL.

The generic join copier accepted `(sourcePositions, sourceStart, sourceCount)`, but its array, map, and struct branches
dropped `sourceStart`. A later build-batch run therefore copied positions belonging to an earlier run. Scalar vectors
already honored the range correctly. The correction propagates the same range through all nested branches and through
nested element/entry expansion. A focused unit test uses a deliberately invalid prefix position and verifies nonzero
offset copying for struct fields, array elements, and map keys/values.

The failing reproduction is `../20260810-q65-wall-control.log`. After installing the candidate, q65 passed exact
comparison through eight warmups and seven measurements per engine. Nitro measured 1,689.4 ms p50 wall, 3,797.4 mean
CPU-ms, and 12,913.3 MiB p50 allocation versus Trino at 2,045.5 ms, 4,759.6 CPU-ms, and 18,291.5 MiB: 0.826x wall,
0.798x CPU, and 0.706x allocation. The successful log is `../20260810-q65-nested-offset-fix.log`.

All 1,706 Nitro tests pass with zero failures and 567 skipped. All 251 Trino Nitro tests also pass against the
installed candidate. No JFR recording, heap dump, or Kata artifact was created.
