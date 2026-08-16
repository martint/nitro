# Async-storage full-board gate

This warmed three-suite gate was stopped when the detached-storage recycling candidate failed exact TPC-DS q02
comparison with 2,513 duplicated rows. Removing the candidate made an isolated q02 run pass exact comparison again.
The optimization is rejected; these files are retained as correctness and diagnostic evidence.

The completed TPC-H portion was essentially flat against the preceding safe board: wall geometric mean 0.731x, CPU
geometric mean 0.695x, weighted wall 0.702x, weighted CPU 0.736x, with 21/22 wall wins and 20/22 CPU wins. Q16 was a
single-run outlier and is not accepted as a regression without an isolated warmed rerun. TPC-DS q01 passed exact at
0.701x wall and 0.732x CPU before q02 stopped the sweep.

`tpcds-q02-revert-guard.log` is the exact-result guard after restoring lease-owned storage. No ClickBench results from
this aborted gate should be inferred.
