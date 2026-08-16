# Rejected vector-reachability storage board

Date: 2026-08-10

This board tested Nitro candidate `cecab7cf` (`Recycle detached storage after wrapper reclamation`) against Trino
integration `bd131814`. The candidate used vector-wrapper phantom reachability as the release signal for detached
primitive storage. It has been abandoned.

## Decisive result

The fully instrumented, five-warmup/three-measure TPC-DS sweep passed q01 through q63, then failed exact comparison at
q64. Nitro returned two concatenated/reused variable-width fields (`Jackson Oak` and `8619216871`) where Trino
returned `Oak` and `16871`. This proves that a retained raw array/accessor can outlive its vector wrapper in the SQL
execution topology. Wrapper reachability is not a complete ownership protocol.

The candidate also lacked a compelling partial-board performance result. Relative to the preceding accepted board:

- q23a median Nitro allocation moved from 21,894.099 MiB to 21,548.165 MiB (1.6% lower), with CPU essentially flat;
- q22 median Nitro allocation moved from 13,359.636 MiB to 14,544.794 MiB (8.9% higher);
- the completed 22-query TPC-H pass measured 0.7347x wall, 0.7107x CPU, and 0.1727x allocation versus the prior
  board's 0.7150x, 0.6874x, and 0.1777x. Q16/q21 sequence noise accounts for most of the timing movement.

`tpch-with-allocation.log` is the correctly instrumented TPC-H record. `tpch.log` is a preliminary timing-only pass
without allocation accounting and is retained only as diagnostic evidence. `tpcds.log` is the partial failing sweep.

The candidate commit was abandoned and the safe lease/GC-owned Nitro artifacts were reinstalled. A final
five-warmup q63-to-q64 exact guard then passed on the restored runtime. No JFR, heap dump, or Kata artifact was
created.
