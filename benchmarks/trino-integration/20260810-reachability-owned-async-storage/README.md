# Rejected reachability-owned asynchronous vector storage

Date: 2026-08-10

This rejected slice replaced premature transport-close recycling with query-owned reachability release. A detached vector's
resettable primitive arrays are offered to the bounded `PrimitiveArrayPool` only after the vector wrapper becomes
phantom reachable. The pool owns and drains its own reference queue; there is no static cleaner, background thread,
or process-wide registry. New consumers receive fresh vector wrappers.

## Initial verification

- Nitro full suite: 1,706 tests, 0 failures, 567 skipped.
- Trino `TestTrinoNitro*`: 250 tests, 0 failures.
- TPC-DS Parquet SF10 sequential exact gate: all 103 variants passed in one JVM with both engines, zero warmups and
  one measurement. This includes q01 followed by q02, which deterministically failed every premature-recycling
  candidate.
- Focused allocator tests prove that reachable detached storage is not reused and that storage becomes reusable,
  through a fresh wrapper, after the final wrapper reference is reclaimed.

These gates were insufficient. A later correctly instrumented warmed sequential board failed exact comparison at
TPC-DS q64 after q01--q63, with concatenated/reused variable-width fields. A retained raw array/accessor can therefore
outlive its wrapper, so wrapper reachability is not a safe storage lifetime token. The candidate commit was abandoned.

## Warmed controls

Both controls used JDK 26, a 12 GiB test JVM, an 8 GiB query-memory limit, five warmups, three measurements, and exact
Trino/Nitro comparison against the Parquet SF10 data.

| Query | Nitro wall p50 | Trino wall p50 | wall ratio | Nitro CPU mean | Trino CPU mean | CPU ratio |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| TPC-DS q22 | 5,181.516 ms | 6,284.653 ms | 0.824x | 11,120.000 ms | 15,592.333 ms | 0.713x |
| TPC-DS q23a | 7,771.048 ms | 7,665.585 ms | 1.014x | 27,758.000 ms | 25,955.000 ms | 1.069x |

These focused runs did not enable thread-allocation accounting; their native-exchange byte counters must not be
interpreted as allocation. The later instrumented partial board measured q23a allocation only 1.6% below the preceding
board and q22 allocation above it, providing no suite-level performance case for weakening ownership.

No JFR, heap dump, or Kata artifact was created.
