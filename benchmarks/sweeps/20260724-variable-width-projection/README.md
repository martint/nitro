# Provider-authored variable-width projection

Date: 2026-07-24

This slice adds generic generated variable-width output support and registers
provider-owned lowering for the UTF-8 conditional. The backend represents a
variable-width value as data/offset/length components, runs an explicit sizing
pass, allocates exact-capacity output through the allocator, and emits offsets
and bytes in a second pass. It has no function-name, query, arity, or logical
type cases.

The exact parent is `3ee9e66a`. All measurements use JDK 26, one unpinned JMH
thread, a 12 GB heap cap, transparent huge pages, 10x1s warmup, 5x1s
measurement, steady-state allocation, and the fixed eight-event `perfnorm`
bundle.

The initial q40 three-fork comparison improved every captured metric:

| metric | candidate | parent | delta |
|---|---:|---:|---:|
| duration | 619.456 ms | 634.325 ms | -2.34% |
| allocation | 172.667 MB | 175.322 MB | -1.51% |
| instructions | 10.026B | 10.194B | -1.65% |
| cycles | 3.437B | 3.617B | -4.97% |
| L1D misses | 101.239M | 105.235M | -3.80% |
| L1D loads | 4.375B | 4.451B | -1.72% |
| dTLB misses | 170.7K | 226.9K | -24.78% |
| dTLB loads | 2.151M | 2.432M | -11.55% |
| branch misses | 29.643M | 29.850M | -0.69% |
| branches | 2.099B | 2.133B | -1.58% |

The first TPC-DS screen found that q86's two-operation UTF-8 root reduced all
retired-work counters but regressed duration 2.25% across three forks. A
generic physical admission rule now leaves UTF-8 roots shorter than three
operations interpreted because their sizing and write passes do not amortize.
That restored q86 duration to parity. It also leaves the analogous q36/q70
two-step shapes interpreted.

The final q40 three-fork comparison after that rule still wins every metric:

| metric | candidate | parent | delta |
|---|---:|---:|---:|
| duration | 629.744 ms | 634.325 ms | -0.72% |
| allocation | 171.347 MB | 175.322 MB | -2.27% |
| instructions | 10.044B | 10.194B | -1.47% |
| cycles | 3.444B | 3.617B | -4.77% |
| L1D misses | 101.783M | 105.235M | -3.28% |
| L1D loads | 4.400B | 4.451B | -1.16% |
| dTLB misses | 203.2K | 226.9K | -10.46% |
| dTLB loads | 2.076M | 2.432M | -14.63% |
| branch misses | 29.698M | 29.850M | -0.51% |
| branches | 2.106B | 2.133B | -1.24% |

Correctness coverage includes dense and sparse masks, skipped-position offset
chains, null selected branches, and rejection of short variable-width slices.
This is an architecture qualification, so no published board row changes.
