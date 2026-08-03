# Full SQL allocation board

This board measures allocation for every native-Parquet TPC-H, TPC-DS, and ClickBench query through both complete
SQL engines after the Nitro variable-width boundary cleanup. It uses JDK 26, a 4 GiB no-spill test heap, one warmup,
and one measured invocation. All 168 Nitro/Trino result pairs matched exactly and all three full correctness gates
passed: 22 TPC-H, 103 TPC-DS variants, and 43 ClickBench queries.

| Suite | Queries | Nitro allocation MiB | Trino allocation MiB | Nitro/Trino | Nitro wins |
|---|---:|---:|---:|---:|---:|
| TPC-H | 22 | 48,334.404 | 164,741.684 | 0.293 | 22 |
| TPC-DS, controlled | 103 | 318,395.338 | 949,679.766 | 0.335 | 101 |
| ClickBench, controlled | 43 | 335,796.481 | 828,740.163 | 0.405 | 43 |
| All suites | 168 | 702,526.223 | 1,943,161.613 | 0.362 | 166 |

The allocation metric is `com.sun.management.ThreadMXBean` allocated-byte delta summed over the live thread set.
That produces exact nonnegative measurements while query workers survive, but a worker that terminates before the
ending snapshot removes its cumulative counter and can make a late sequential row negative or artificially small.
The controlled CSVs therefore replace only affected or suspicious rows with fresh isolated-JVM measurements. The
`source` column identifies every replacement. TPC-DS uses isolated q15, q22, q41, q47, q57, q68, and q70 rows;
ClickBench uses isolated q27, q28, q33, and q35 rows. TPC-H needed no replacement.

Only TPC-DS q22 and q41 allocate more under Nitro. Q22 is 26,561.669 versus 25,716.957 MiB (1.033x); q41 is only
69.351 versus 65.757 MiB (1.055x, a 3.594 MiB difference). Q22's existing 2 GiB admission failure reproduces with
the old VARCHAR-only boundary adapter and is not caused by this slice; the established no-spill correctness envelope
is 4 GiB. Every ClickBench query and every TPC-H query is an allocation win.

Two real SQL-boundary regressions were found and removed without changing an operator kernel. TPC-DS q57 copied
every flat CHAR/VARBINARY value through `Slice` objects at Trino-to-Nitro ingress. Nitro now borrows the raw
`VariableWidthBlock` bytes and offsets for every compatible Slice-backed type, retaining CHAR's trimming fallback
when a block contains trailing spaces. Its isolated allocation is now 12,031.927 versus 12,197.194 MiB (0.986x).

TPC-DS q15 adapted constant positive `substring` arguments through a scalar Slice path on every selected row. The
registry implementation now dispatches that shape to a direct UTF-8 vector loop, writes into a pooled binary vector,
observes the supplied branch mask, and preserves CHAR virtual-padding semantics. General, dynamic, and negative
arguments retain the scalar fallback. Q15 moved from roughly 1,234 versus 712 MiB before the fix to 719.251 versus
729.059 MiB (0.987x).

The full logs, isolated controls, and per-query controlled CSVs are retained here. The profiler used only sampled
allocation stacks in collapsed text form during diagnosis; no JFR or heap dump was created, and the temporary
profiler installation was not retained in the board.
