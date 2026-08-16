# Rejected sparse preaggregation-cardinality board

This board tested Nitro `3b6e79eb` with Trino integration `ffab14a7`. It is rejected evidence, not an accepted
baseline. All 168 query variants passed exact comparison, but the first 128 physical rows were not a representative
admission sample for clustered or large-key-space sources.

The board measured 0.6793x wall / 0.4981x CPU / 0.2379x valid allocation overall, with workload-weighted wall and
CPU of 0.7815x / 0.6490x. TPC-DS q02, TPC-DS q59, ClickBench q12, and ClickBench q16 incorrectly bypassed useful
partial aggregation. The failure was structural: batch-local uniqueness does not imply query- or driver-local lack
of reuse.

The successor board under `20260811-representative-cardinality-full-board/` replaces the sparse prefix with a
bounded complete-physical-batch observation and qualifies the corrected behavior across all three suites. Raw logs
here remain useful for comparing the misclassified source-exchange row counts and CPU attribution. No JFR recording,
heap dump, or Kata artifact was created.
