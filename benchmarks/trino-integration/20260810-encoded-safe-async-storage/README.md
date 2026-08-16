# Rejected encoded/storage-identity asynchronous reuse follow-up

This directory contains zero-warmup, single-measurement correctness gates used only to reproduce the q01-to-q02
suite-order corruption quickly. They are diagnostics, not publishable performance measurements.

Restricting recycling to directly detached flat roots did not fix q02. Tracking backing-array identities within one
allocator also did not help: instrumentation observed no overlapping aliases in that allocator. Moving identity
leases to the query-owned `AllocationResources`, shared by driver allocators, reduced the mismatch from 2,513 rows to
2,170 but still failed exact comparison. This establishes that some aliases cross allocator boundaries and others
outlive the producer lease before a later consumer registers them.

All candidate code was removed. The accepted implementation remains `6ec25808` in Nitro and `9acf478d` in Trino:
detached exchange storage stays lease/GC-owned and is not returned to the primitive pool. Safe reuse requires an
explicit lease to accompany every retained borrow/forward at handoff time.
