# Rejected join diagnostic ownership variants

The current `HashJoinOperator` materialization profiler uses process-global
`ThreadLocal` state and violates the explicit-resource principle. Two local
removal designs were tested against exact parent `72ad8472`; neither is
retained.

## Method

All captures use JDK 26, unpinned one-thread Nitro operator harnesses, a 12 GiB
heap, THP, allocation, and all eight publication hardware counters in the same
invocation.

## Variant 1: delete dormant profiling hooks

The profiler interface, ThreadLocal, timers, and hot-path recording branches
were removed. In the reverse-order one-fork q18 confirmation, duration regressed
16.42%, instructions 8.38%, cycles 17.44%, L1D misses 15.79%, branch misses
8.19%, and branches 7.26%. Allocation was flat. This form is rejected.

## Variant 2: inject an optional operator-instance observer

The established timing/branch shape was restored, while a nullable observer was
passed to the join constructor and read from an instance field. A short q64
screen was neutral-to-favorable. Short q18 screens remained adverse.

The required 10-warmup, 5-measurement, 3-fork q18 pair was bimodal. Its aggregate
favored the candidate on duration (-8.95%), instructions (-3.05%), cycles
(-6.23%), L1D misses (-8.44%), branch misses (-3.19%), and branches (-2.86%),
but regressed dTLB misses 68.71% and dTLB loads 31.79%. Allocation was flat.
This form also fails the hardware-counter gate and is rejected.

The exact parent source is retained. The ThreadLocal remains an acknowledged
architectural violation to remove through a broader JIT-stable,
execution-scoped diagnostics integration.
