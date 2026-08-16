# Nitro allocator attribution

ClickBench q24 was measured in a fresh 12 GiB JVM with five warmups and five
measurements. `Allocator.allocatedBytes()` was exposed as the
`nitroAllocatorAllocatedBytes` source-operator metric so allocator-owned vector
and mask storage could be separated from total thread allocation.

The accepted pre-fix run reported:

- wall p50: 5279.459 ms
- CPU p50 / mean: 21242 / 21258 ms
- thread allocation p50: 36426.279 MiB
- peak memory p50: 172.727 MiB
- Nitro allocator allocation: 5,397,543,087 bytes

Only about 5.4 GB of the roughly 38.2 GB total thread allocation was storage
allocated through Nitro's allocator. The remaining SQL/operator-benchmark gap
therefore was not primarily vector-pool churn.

No JFR, heap dump, or Kata review artifacts were created.
