# Provider-owned compact flat-key screen

This screen evaluates a provider-owned `TypeBinding` capability that declares
canonical signed-32 flat storage for Trino `INTEGER`, without inferring logical
width from an `I32Vector`. Runs use a 12 GiB JVM and the 100-million-row
ClickBench Parquet data.

The five-warmup q33 candidate reduced the pair record from 16 to 12 bytes and
produced the expected memory result:

| q33 Nitro | p50 wall ms | p50 CPU ms | allocation MiB | sampled query peak MiB |
|---|---:|---:|---:|---:|
| Parent board | 8,428.4 | 17,220 | 30,585.1 | 5,921.9 |
| Provider-owned compact integer | 8,311.9 | 17,320 | 30,460.6 | 5,540.5 |

The 381.4 MiB (6.4%) peak reduction agrees with two high-cardinality grouping
tables retaining four fewer bytes per record. CPU is effectively unchanged.

The first adjacent q32 exact-result guardrail reported one different top-10
row. That result was not reproducible: an immediate compact replay passed, a
full-width control passed, and a second fully warmed compact run also passed
all three exact comparisons. The warmed replay measured 1,229.2 ms / 4,499
CPU-ms for Nitro versus 1,283.9 ms / 5,086 CPU-ms for Trino. A separate q33
Nitro/Trino run also passed exact comparison. The isolated core table passes
signed values, nullable keys, hash precomputation, rehashing, mixed I32/I64
batches, high cardinality, and group-key materialization. The lone q32 mismatch
is therefore classified as a non-reproducing distributed result/order flake,
not evidence of deterministic compact-key corruption.

Artifacts:

- `q33-nitro.log`: valid five-warmup candidate measurement.
- `q32-guardrail.log`: exact-result failure with the capability enabled.
- `q32-guardrail-replay.log`: fully warmed exact-result replay; all three
  comparisons pass.
- `stale-trino-main-control.log`: invalid initial screen that loaded an older
  installed `trino-main`; retained only to document why it is excluded.
