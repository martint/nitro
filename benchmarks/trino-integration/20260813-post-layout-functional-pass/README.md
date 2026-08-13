# Post-layout functional pass

Correctness-only canary over the complete benchmark corpus after the flat-grouping
layout-admission correction. Each query ran once in both engines with no warmup;
the harness compared result rows and result types. These timings are not performance
measurements and are excluded from the benchmark board.

| Suite | Variants | Result | Log |
|---|---:|---|---|
| TPC-DS | 103 | pass | `tpcds-w0m1.log` |
| TPC-H | 22 | pass | `tpch-w0m1.log` |
| ClickBench | 43 | pass | `clickbench-w0m1.log` |
| Total | 168 | pass | |

All three Maven test invocations completed successfully with zero failures and
zero errors.
