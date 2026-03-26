# TPC-DS Parquet Nitro vs Trino (2026-03-26)

This snapshot compares the currently supported parquet-backed TPC-DS operator-assembly queries on Nitro and the matching Trino operator harness.

Dataset and benchmark shape:

- TPC-DS parquet dump: `sf10`
- parquet path: `/Users/martin/tmp/tpcds-parquet-sf10`
- scan batch default: `10000` rows
- hash join batch default: `4096` rows
- JMH options: `-wi 1 -i 3 -w 2s -r 5s -f 0`

Caveat:

- these are still non-forked runs, so they are useful as an engineering snapshot, not a publication-quality benchmark
- some queries in this corpus still show wide error bars, so focused follow-up reruns are still the right tool before making fine-grained performance claims

Headline:

- Nitro faster on `2/7`
- Trino faster on `5/7`
- geometric mean of per-query times favors Trino by about `1.10x`
- derived summed per-query totals are almost tied:
  - Nitro: `22.18s`
  - Trino: `22.23s`

Per-query results:

| Query | Nitro ms/op | Trino ms/op | Faster |
|---|---:|---:|---|
| `Q10` | `7966.669` | `9595.399` | Nitro |
| `Q41` | `72.666` | `59.355` | Trino |
| `Q62` | `1322.328` | `860.155` | Trino |
| `Q73` | `1698.786` | `1607.624` | Trino |
| `Q88` | `8606.856` | `7577.767` | Trino |
| `Q96` | `1016.457` | `924.080` | Trino |
| `Q99` | `1498.949` | `1607.402` | Nitro |

Raw outputs:

- Nitro: `/tmp/nitro-tpcds-supported.txt`
- Trino: `/tmp/trino-tpcds-supported.txt`

Notes:

- `Q73` had improved in focused single-query reruns after moving the customer join build side to scanned pages, but that gain did not fully carry through in this mixed corpus run
- `Q88` and `Q96` have both shown meaningful run-to-run movement in focused experiments, so they remain good candidates for steadier follow-up measurement
