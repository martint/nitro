# Rejected take-only storage-reuse full-board gate

The TPC-DS publication sweep stopped at q02 with an exact 2,513-row result mismatch after q01 had populated the
shared primitive storage pool. The same q02 passed in a fresh isolated JVM, proving that the take-only lifetime model
was sequence-dependent and unsafe. TPC-H and ClickBench were deliberately not run after this correctness failure.

The candidate code was abandoned. The accepted parent keeps asynchronously detached storage lease/GC-owned rather
than recycling it at transport-batch close. No JFR or heap dump was produced.
