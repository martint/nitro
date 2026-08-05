# TPC-H q3 SQL-shape reconciliation

This directory records the investigation of the gap between the q3 operator benchmark and the
Nitro-backed Trino query.

The original operator fixture built the orders side synchronously before scanning lineitem. Its
join constraint therefore crossed the partial aggregation and reduced lineitem from 59.99 million
rows to 5.85 million physical rows (302,114 rows after the ship-date predicate). The distributed
SQL fragment starts its split-local partial aggregations before that replicated join filter is
available, so it reads all 59.99 million rows and groups the 32.33 million ship-date survivors.
The old 1.376 s Nitro operator result in `q03-sql-shaped-operator.log` was consequently not a valid
expectation for the SQL execution topology.

The corrected fixtures model the SQL topology explicitly: 19 independent source-driver partial
aggregation states, followed by the orders join, a three-key hash exchange into two final
aggregation tasks, and TopN. With five warmups and seven measurements:

| Engine | Operator fixture (ms) |
| --- | ---: |
| Nitro | 2,330.944 |
| Trino | 2,687.481 |
| Nitro / Trino | 0.867 |

The general page-plan aggregation/source fusion in Trino combines the native Parquet scan,
ship-date filter, revenue projection, and partial aggregation even when nested Nitro plan
boundaries separate them. The warmed SQL candidate in
`q03-general-page-aggregation-candidate.log` measured 0.927x Trino CPU, 0.875x wall time, and
0.184x allocation. Its CPU ratio is within six percentage points of the corrected 0.867 operator
ratio; the remaining difference includes distributed scheduling, build/exchange work, and the
connector integration around the same operators.

`q03-standalone-operator-profile.log` captures the invalid early-filtered fixture, while
`q03-sql-topology-operator-profile.log` captures the corrected 59.99M -> 32.33M -> 8.28M row
progression. The other diagnostic logs preserve the successive planner-fusion experiments and
the distributed physical plan used to reconcile the fixture.
