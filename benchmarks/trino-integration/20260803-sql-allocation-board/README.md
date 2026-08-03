# Full SQL allocation board

This board measures allocation for every native-Parquet TPC-H, TPC-DS, and ClickBench query through both complete
SQL engines after the Nitro variable-width boundary cleanup. It uses JDK 26, a 4 GiB no-spill test heap, one warmup,
and one measured invocation. All 168 Nitro/Trino result pairs matched exactly and all three full correctness gates
passed: 22 TPC-H, 103 TPC-DS variants, and 43 ClickBench queries.

| Suite | Queries | Nitro allocation MiB | Trino allocation MiB | Nitro/Trino | Nitro wins |
|---|---:|---:|---:|---:|---:|
| TPC-H | 22 | 48,334.404 | 164,741.684 | 0.293 | 22 |
| TPC-DS, controlled | 103 | 301,114.971 | 957,649.146 | 0.314 | 102 |
| ClickBench, controlled | 43 | 335,796.481 | 828,740.163 | 0.405 | 43 |
| All suites | 168 | 685,245.856 | 1,951,130.993 | 0.351 | 167 |

The allocation metric is `com.sun.management.ThreadMXBean` allocated-byte delta summed over the live thread set.
That produces exact nonnegative measurements while query workers survive, but a worker that terminates before the
ending snapshot removes its cumulative counter and can make a late sequential row negative or artificially small.
The controlled CSVs therefore replace only affected or suspicious rows with fresh isolated-JVM measurements. The
`source` column identifies every replacement. TPC-DS uses isolated q15, q22, q41, q47, q57, q68, and q70 rows;
ClickBench uses isolated q27, q28, q33, and q35 rows. TPC-H needed no replacement.

Only TPC-DS q41 allocates more under Nitro, and it is 69.351 versus 65.757 MiB (1.055x, a 3.594 MiB difference).
Every ClickBench query and every TPC-H query is an allocation win.

The original q22 live-thread overlay was itself invalid: different terminated-worker histories produced 26,561.669
versus 25,716.957 MiB and a false 1.033x loss. Separate fixed-interval allocation profiles sampled 5,314 Nitro
allocations versus 15,703 Trino allocations (0.338x). They also exposed missing GroupId batch-release callbacks.
After commit `4c915e23`, a three-measurement paired run reports median allocation of 9,281.302 versus 33,686.337 MiB
(0.276x), independently corroborating the sampled ratio. Q22 now passes at the default 2 GiB per-node query limit;
before the fix `GroupIdOperator` retained 1.96 GiB by itself and the query required the enlarged 4 GiB envelope.

The post-fix default-memory TPC-DS gate exercised all 103 variants. Q01-q16, q18-q22, and q24a-q99 completed in
three fresh processes with the normal 2 GiB per-node query limit; q17 and q23b also executed successfully and
returned their expected empty results, after which the audit harness stopped because dynamic filtering had rejected
one and four now-empty source attempts respectively. Those audit-accounting assertions are orthogonal to execution
and GroupId ownership. The q24a-q99 continuation completed all 79 remaining variants in one process. Logs are
retained as `tpcds-default-memory-*`.

Two real SQL-boundary regressions were found and removed without changing an operator kernel. TPC-DS q57 copied
every flat CHAR/VARBINARY value through `Slice` objects at Trino-to-Nitro ingress. Nitro now borrows the raw
`VariableWidthBlock` bytes and offsets for every compatible Slice-backed type, retaining CHAR's trimming fallback
when a block contains trailing spaces. Its isolated allocation is now 12,031.927 versus 12,197.194 MiB (0.986x).

TPC-DS q15 adapted constant positive `substring` arguments through a scalar Slice path on every selected row. The
registry implementation now dispatches that shape to a direct UTF-8 vector loop, writes into a pooled binary vector,
observes the supplied branch mask, and preserves CHAR virtual-padding semantics. General, dynamic, and negative
arguments retain the scalar fallback. Q15 moved from roughly 1,234 versus 712 MiB before the fix to 719.251 versus
729.059 MiB (0.987x).

The full logs, isolated controls, q22 post-fix controls, and per-query controlled CSVs are retained here. The profiler
used only sampled allocation stacks in collapsed text form during diagnosis, and the temporary profiler installation
was not retained in the board.

During the later q22 diagnosis, one initial misconfigured 3 GiB launch inherited Surefire's heap-dump flag and
created a transient 2.7 GiB dump. It was removed immediately; subsequent profiling and test JVMs explicitly disabled
heap dumps. No heap-dump or JFR artifact is retained.
