# TPC-DS Unsupported Query Roadmap

This note captures what the unsupported parquet-backed TPC-DS queries currently
need, based on Trino logical `EXPLAIN` output dumped by
`org.weakref.nitro.tpcds.ExplainUnsupportedQueries`.

The dump currently lives under `target/tpcds-explain/` and covers the `79`
benchmark queries that were not yet implemented in the parquet-backed Nitro and
Trino operator harnesses when this snapshot was generated.

## Current coverage

Supported parquet-backed queries today:

- `Q01`
- `Q06`
- `Q10`
- `Q35`
- `Q41`
- `Q44`
- `Q45`
- `Q51`
- `Q53`
- `Q62`
- `Q67`
- `Q69`
- `Q70`
- `Q73`
- `Q80`
- `Q84`
- `Q88`
- `Q90`
- `Q96`
- `Q97`
- `Q99`

Everything else is currently unsupported in the parquet-backed operator
assemblies.

## Lowering workflow

For unsupported queries, the intended workflow is:

1. dump Trino logical `EXPLAIN`
2. inline `WITH` clauses as a tree at each use site
3. map the optimized logical plan onto equivalent Nitro and Trino operator
   assemblies
4. only then implement the query

The important point is that `EXPLAIN` is not only for classifying queries. It
is the source of truth for the operator topology we need to reproduce.

## What the explain dump shows

Most unsupported queries already lower into plan shapes that are largely within
the current Nitro vocabulary:

- `79/79` use `Aggregate`
- `77/79` use joins
- `73/79` use `ScanFilter`
- `70/79` use `ScanFilterProject`
- `67/79` use `Project`
- `57/79` use `TopN`

That means a large fraction of the remaining work is not "invent a whole new
engine", but rather:

- implement the missing advanced operators
- encode a few repeatable lowering patterns
- then batch in the simpler query shapes

## Missing operator families

### Window

Queries:

- `Q12`, `Q20`, `Q36`, `Q47`, `Q49`, `Q57`, `Q63`, `Q86`, `Q89`,
  `Q98`

Needed shape:

- partitioned order-sensitive window evaluation
- running aggregates such as `sum(...) OVER (...)` and `max(...) OVER (...)`
- ranking such as `rank()`

Representative plans:

- `Q51` uses two partitioned running-sum windows plus a final window after a
  `FullJoin`
- `Q53` uses a partition-wide quarterly average window
- `Q70` uses `Window` together with `GroupId` and `TopNRanking`

Status:

- implemented in Nitro and in the Trino parquet harness
- validated by `Q51`, `Q53`, and `Q70`
- still needs broader coverage across the remaining window-heavy queries

### TopNRanking

Queries:

- none in the current unsupported set

Needed shape:

- per-partition ranking with limit pushdown, as Trino lowers to
  `TopNRanking[partitionBy = ..., orderBy = ..., limit = ...]`

Representative plans:

- `Q70` uses `TopNRanking` under a grouped state summary

Status:

- implemented in Nitro and in the Trino parquet harness
- validated by `Q44`, `Q67`, and `Q70`

### GroupId

Queries:

- `Q05`, `Q14`, `Q16`, `Q18`, `Q22`, `Q27`, `Q28`, `Q36`, `Q67`, `Q77`,
  `Q86`, `Q94`, `Q95`

Needed shape:

- grouping sets / rollup style lowering
- one input stream duplicated across multiple grouping identities
- downstream aggregate grouped by `(keys..., groupid)`

Representative plans:

- `Q70` uses `GroupId` before ranking

Status:

- implemented in Nitro and in the Trino parquet harness
- no longer a blocking operator family for the next expansion batch

### FullJoin

Queries:

- none of the current near-term targets

Needed shape:

- full outer equi-join
- null-extension on both unmatched sides
- downstream projection over coalesced join keys

Representative plans:

- `Q51` full-joins running web/store sales streams before a final window
- `Q97` full-joins pre-aggregated customer/item fact streams

Status:

- implemented in Nitro and in the Trino parquet harness
- no longer a blocking operator family for the next expansion batch

### EnforceSingleRow

Queries:

- `Q06`, `Q54`, `Q58`

Needed shape:

- scalar-subquery enforcement after aggregation
- error if more than one row survives
- often paired with `CrossJoin` of scalar aggregates

Representative plans:

- scalar-subquery comparisons still lower through `EnforceSingleRow`

Status:

- implemented in Nitro and in the Trino parquet harness
- validated by `Q06`, `Q44`, `Q54`, and `Q58`
- no longer a blocking operator family for scalar-subquery expansion

## Already-covered advanced shapes

These do appear in unsupported plans, but we already have a basis to build on:

### SemiJoin

Queries:

- queries with `IN` / existence filtering beyond the currently supported set

Status:

- Nitro and the Trino harness already have semi-join support
- this operator family is no longer a blocker for the next expansion batch

### CrossJoin

Queries include:

- `Q06`, `Q09`, `Q14`, `Q23`, `Q24`, `Q28`, `Q30`, `Q32`, `Q44`,
  `Q54`, `Q61`, `Q77`, `Q81`, `Q92`

Status:

- many of these are scalar-subquery cross joins over one-row aggregate inputs
- the harder missing piece is usually `EnforceSingleRow`, not the cross join
  itself

## Recommended implementation order

### Batch 1: no new operator families

Target queries whose logical plans are mostly:

- scan/filter/project
- inner/left join
- aggregate
- topN/sort

This should unlock a sizable chunk of the remaining corpus quickly.

### Batch 2: scalar-subquery enforcement

Implement:

- `EnforceSingleRow`

Then add the queries that mainly combine:

- `CrossJoin`
- scalar aggregate branches
- filter/project logic

### Batch 3: window and ranking

Implement:

- `Window`
- `TopNRanking`

This is likely the biggest single feature unlock for unsupported TPC-DS.

### Batch 4: full outer join

Implement:

- `FullJoin`

This is a smaller query count, but it no longer blocks the current expansion path.

## Practical next targets

Good first unsupported queries after this explain pass:

- `Q51`
  - good design target for `Window` plus `FullJoin`
