# ClickBench q19 final-grouping investigation

This investigation used JDK 26, a 12 GiB test JVM, five complete query
warmups, three measured executions, exact Nitro SQL integration, and no JFR or
heap dump. The query groups 100 million input rows by `UserID`, extracted
minute, and `SearchPhrase`; partial aggregation sends about 57.55 million rows
through the exchange to a two-driver final aggregation.

## Packed record identity A/B

The existing packed-record identity specialization is beneficial in the
distributed SQL shape. A Nitro-only composition-time A/B reported:

| Configuration | p50 wall | p50 query CPU | query peak |
| --- | ---: | ---: | ---: |
| default packed identity | 5,477.856 ms | 23,050 ms | 3,191 MiB |
| `nitro.flatGrouping.packedHashRecordSlots=false` | 5,745.270 ms | 23,800 ms | 3,426 MiB |
| disabled / default | 1.049x | 1.033x | 1.074x |

The old single-stage operator specialization therefore generalizes to the
post-exchange final stage and must remain enabled.

## Symbolized profiles

`q19-nitro-c.perf.data` and `q19-trino.perf.data` cover the complete measured
phase after five warmups. The JVM perf maps were emitted before measurement so
JIT methods remain attributable. Profile attachment adds sampling overhead, so
the timings are diagnostic rather than board replacements:

| Engine | p50 wall | p50 query CPU | sampled cycles |
| --- | ---: | ---: | ---: |
| Nitro | 5,822.691 ms | 21,740 ms | 408.73 B |
| Trino | 6,622.924 ms | 22,500 ms | 393.24 B |
| Nitro / Trino | 0.879x | 0.966x | 1.039x |

Nitro's leading attributed methods are flat-key binary retention (10.10%),
table probing (8.60%), new-group insertion (7.45%), batch hashing (5.46%), and
rehashing (3.00%). Trino spends 20.53% in `FlatHash.addNewGroup`, 7.06% in
rehashing, 6.93% in its non-dictionary group-id driver, and 5.19% in hash-table
matching. The host hardcodes an initial expectation of 10,000 groups; Nitro
starts from the roughly 48,000-position first final-stage batch, so no planner
cardinality estimate is being dropped at the Nitro boundary.

The SQL expression for minute extraction accounts for about 5.4% of Nitro's
whole-process sampled cycles in the adapted `from_unixtime` and
timestamp-with-time-zone functions. The historical operator fixture used a
direct minute projection and therefore did not model this part of the SQL
shape. This is a real fixture/SQL mismatch, but it does not produce a
controlled q19 CPU regression: the profile run is 3.4% lower in query CPU, and
an earlier five-warmup/five-measurement control was effectively at parity
(1.007x). The broad sweep's 1.096x CPU row is not reproducible and is cleared
as noise. No production change was made from this investigation.
