# Adaptive compact long flat-record experiment

All SQL controls use the TPC-DS Parquet SF10 data, JDK 26, a 12 GiB JVM, an 8 GiB query limit, and warmed measurements.

The accepted candidate stores physically narrow logical-long fields in four-byte flat-record tokens only for large
mixed composite batches that also contain dictionary-backed binary input and an I32 physical key. A stable sidecar
preserves exact full-width long values encountered after layout admission.

## Accepted results

| Query/control | p50 wall (ms) | mean CPU (ms) | p50 allocation (MiB) | p50 peak (MiB) |
|---|---:|---:|---:|---:|
| q23a accepted physical cohort | 5,806.072 | 27,147.6 | 47,100.115 | 2,527.487 |
| q23a disabled | 5,790.326 | 27,132.2 | 47,873.427 | 2,541.638 |
| q22 accepted guard | 4,695.362 | 10,958.3 | 11,541.149 | 314.318 |
| q22 disabled | 4,699.593 | 10,848.3 | 11,540.251 | 300.023 |

Q23a allocation falls 1.6% while CPU and wall remain neutral. Q22's severe broad-rule regression is absent. Q33 is a
non-activating control: runtime tracing shows only single-key I32/I64 grouping shapes.

## Rejected variants

- Broad admission (`q23a-candidate.log`) saved about 3.0% allocation on q23a but regressed q22 wall by about 18% and
  CPU by 16% (`q22-candidate.log`); rejected.
- Logical-provider-gated admission (`q23a-refined.log`) did not activate because Trino retained logical BIGINT bindings
  around the physically compact exchanged vector; rejected.
- Extending admission to q23a's later all-I64 final grouping (`q23a-final-cohort.log`) increased allocation and CPU;
  rejected.

The focused flat-grouping suite passes 47/47. The complete Nitro suite passes 1,691 tests with zero failures and 567
skipped. No JFR, heap dump, or Kata artifact was created.
