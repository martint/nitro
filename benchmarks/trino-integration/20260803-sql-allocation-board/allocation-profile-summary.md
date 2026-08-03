# Allocation-profile diagnosis

Async-profiler allocation sampling was used only to localize the two anomalous TPC-DS rows. Profiles were captured
as collapsed stacks and stopped before test-JVM teardown through the benchmark's explicit profiler handshake.

For q15, the pre-fix Nitro profile contained 1,574 sampled `Slice` allocations and 393 sampled boxed `Integer`
allocations. The decisive stacks were 517 samples through `TrinoVarcharSubstringFunction.apply` and
`Slices.wrappedBuffer`, 471 through `Chars.padSpaces` and `Slices.allocate`, 461 through Trino's scalar
`StringFunctions.substring` and `Slice.slice`, and 392 through boxed mask iteration. This led to the direct masked
vector substring implementation.

For q57, 3,139 samples copied VARBINARY values through `TrinoPrimitiveNitroTypeAdapter.toValues`,
`VarbinaryType.getSlice`, and `VariableWidthBlock.getSlice`; another 487 followed the same route through CHAR.
This led to generalized compatible flat-storage borrowing at the Trino/Nitro boundary.

The profiles did not implicate Nitro's aggregation, join, scan, or other standalone operator kernels. They exposed
host adaptation work that does not exist in an isolated operator benchmark, so both corrections are confined to the
explicit Trino/Nitro boundary and function registry integration.
