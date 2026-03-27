# TPC-DS Unsupported Query Roadmap

This note captures what the unsupported parquet-backed TPC-DS queries currently
need, based on Trino logical `EXPLAIN` output dumped by
`org.weakref.nitro.tpcds.ExplainUnsupportedQueries`.

The dump currently lives under `target/tpcds-explain/` and covers the `88`
benchmark queries that are not yet implemented in the parquet-backed Nitro and
Trino operator harnesses.

## Current coverage

Supported parquet-backed queries today:

- `Q10`
- `Q35`
- `Q41`
- `Q62`
- `Q69`
- `Q73`
- `Q84`
- `Q88`
- `Q90`
- `Q96`
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

- `88/88` use `Aggregate`
- `86/88` use joins
- `82/88` use `ScanFilter`
- `79/88` use `ScanFilterProject`
- `76/88` use `Project`
- `66/88` use `TopN`

That means a large fraction of the remaining work is not "invent a whole new
engine", but rather:

- implement the missing advanced operators
- encode a few repeatable lowering patterns
- then batch in the simpler query shapes

## Missing operator families

### Window

Queries:

- `Q12`, `Q20`, `Q36`, `Q47`, `Q49`, `Q51`, `Q53`, `Q57`, `Q63`, `Q70`,
  `Q86`, `Q89`, `Q98`

Needed shape:

- partitioned order-sensitive window evaluation
- running aggregates such as `sum(...) OVER (...)` and `max(...) OVER (...)`
- ranking such as `rank()`

Representative plans:

- `Q51` uses two partitioned running-sum windows plus a final window after a
  `FullJoin`
- `Q70` uses `Window` together with `GroupId` and `TopNRanking`

Status:

- missing as a reusable operator family

### TopNRanking

Queries:

- `Q44`, `Q67`, `Q70`

Needed shape:

- per-partition ranking with limit pushdown, as Trino lowers to
  `TopNRanking[partitionBy = ..., orderBy = ..., limit = ...]`

Representative plans:

- `Q44` uses two separate `TopNRanking` branches plus scalar-subquery
  enforcement
- `Q70` uses `TopNRanking` under a grouped state summary

Status:

- missing as a dedicated lowering/operator

### GroupId

Queries:

- `Q05`, `Q14`, `Q16`, `Q18`, `Q22`, `Q27`, `Q28`, `Q36`, `Q67`, `Q70`,
  `Q77`, `Q80`, `Q86`, `Q94`, `Q95`

Needed shape:

- grouping sets / rollup style lowering
- one input stream duplicated across multiple grouping identities
- downstream aggregate grouped by `(keys..., groupid)`

Representative plans:

- `Q80` uses `GroupId` for channel/id subtotal output
- `Q70` uses `GroupId` before ranking

Status:

- missing as a reusable operator

### FullJoin

Queries:

- `Q51`, `Q97`

Needed shape:

- full outer equi-join
- null-extension on both unmatched sides
- downstream projection over coalesced join keys

Representative plans:

- `Q51` full-joins running web/store sales streams before a final window
- `Q97` full-joins pre-aggregated customer/item fact streams

Status:

- missing from the current reusable join vocabulary

### EnforceSingleRow

Queries:

- `Q06`, `Q44`, `Q54`, `Q58`

Needed shape:

- scalar-subquery enforcement after aggregation
- error if more than one row survives
- often paired with `CrossJoin` of scalar aggregates

Representative plans:

- `Q44` uses `EnforceSingleRow` under scalar comparison branches

Status:

- missing as an explicit operator/lowering step

## Already-covered advanced shapes

These do appear in unsupported plans, but we already have a basis to build on:

### SemiJoin

Queries:

- `Q45`

Status:

- Nitro and the Trino harness already have semi-join support
- `Q45` should be a good early unsupported query once the surrounding assembly
  is wired

### CrossJoin

Queries include:

- `Q01`, `Q06`, `Q09`, `Q14`, `Q23`, `Q24`, `Q28`, `Q30`, `Q32`, `Q44`,
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

### Batch 4: grouping sets

Implement:

- `GroupId`

This should unlock several report-style queries that are otherwise ordinary
join/aggregate/topN pipelines.

### Batch 5: full outer join

Implement:

- `FullJoin`

This is a smaller query count, but currently blocks `Q51` and `Q97`.

## Practical next targets

Good first unsupported queries after this explain pass:

- `Q45`
  - has `SemiJoin`, but not the larger missing operator families
- `Q01`
  - complex, but still mostly aggregate/join/topN without missing advanced
    operators
- `Q44`
  - good design target for `TopNRanking` plus `EnforceSingleRow`
- `Q80`
  - good design target for `GroupId`
- `Q51`
  - good design target for `Window` plus `FullJoin`
