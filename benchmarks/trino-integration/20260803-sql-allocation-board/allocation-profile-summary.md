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

Q22 subsequently showed why live-thread deltas are unsuitable for the last anomalous row. With identical 2 MiB
allocation-sampling intervals, Nitro produced 5,314 samples and Trino 15,703 (0.338x), overturning the apparent
1.033x live-thread loss. Nitro's largest avoidable family was grouping-set expansion: GroupId streams, copied
dictionary IDs, copied null streams, and related materialization accounted for roughly 22% of its samples. The
operator supplied transfer callbacks but no release callbacks, so downstream borrowing retained every resolved batch
buffer until operator teardown. The same lifecycle omission retained output masks and caused the default 2 GiB q22
admission failure.

Commit `4c915e23` releases borrowed GroupId vectors and masks when each batch closes while preserving take/transfer
semantics for downstream ownership. The full Nitro suite passes 1,618 tests with no failures or errors and 566
skips. A paired three-measurement SQL control at the default 2 GiB query limit passes exact results and reports Nitro
at 0.713x median wall, 0.594x median CPU, and 0.276x median live-thread allocation; the allocation-sample ratio is
0.338x. This is a general batch-lifecycle repair, not a q22 or ROLLUP special case.
