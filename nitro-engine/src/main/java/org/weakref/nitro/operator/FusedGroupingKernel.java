/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.weakref.nitro.operator;

import org.weakref.nitro.core.function.aggregation.GroupedStateUpdate;

/**
 * One fused pass over a batch for single-long-key grouped aggregation: probe the open-addressed
 * group table and accumulate every selected accumulator's state in the same loop, with no group-id
 * vector round-trip and no per-row accumulator dispatch.
 * <p>
 * Implementations are generated per accumulator-set shape by
 * {@link FusedGroupingAggregationKernelGenerator}; the body is emitted as bytecode so each
 * state-update call site uses an engine-owned SPI that provider state objects may implement across classloaders.
 */
interface FusedGroupingKernel
{
    /**
     * Probes and accumulates the rows selected by {@code positions}/{@code count}.
     *
     * @param positions selected row positions, or {@code null} for a dense {@code 0..count-1} batch
     * @param count number of selected rows
     * @param keys the primitive group-key base values ({@code int[]} or {@code long[]}); the generated
     *        implementation is specialized to the concrete array and optional one-level dictionary shape
     * @param keyIds optional dictionary ids mapping logical positions into {@code keys}
     * @param tableKeys open-addressed table: key per slot, or empty when the physical shape resolves exact keys
     *        through {@code keysByGroup}
     * @param tableIds open-addressed table: either a group id per slot ({@code -1} when empty), or a packed hash
     *        fragment plus {@code group+1} ({@code 0} when empty) for an id-indexed physical shape
     * @param tableMask {@code capacity - 1} bitmask for the table
     * @param keysByGroup reverse map: key by group id
     * @param startNextId the next group id to assign
     * @param inputs per-accumulator primitive base value column ({@code int[]} or {@code long[]});
     *        {@code inputs[a]} is {@code null} when accumulator
     *        {@code a} increments by a constant)
     * @param inputIds optional per-accumulator dictionary ids mapping logical positions into {@code inputs}
     * @param inputNulls per-accumulator null base column, or {@code null} when that input is known
     *        null-free (and for constant-increment accumulators)
     * @param inputNullIds optional per-accumulator dictionary ids mapping logical positions into {@code inputNulls}
     * @param states per-accumulator opaque state-update target
     * @return the next group id after assigning any new groups encountered
     */
    long accumulate(
            int[] positions,
            int count,
            Object keys,
            int[] keyIds,
            long[] tableKeys,
            int[] tableIds,
            int tableMask,
            long[] keysByGroup,
            long startNextId,
            long[] outputGroups,
            Object[] inputs,
            int[][] inputIds,
            boolean[][] inputNulls,
            int[][] inputNullIds,
            GroupedStateUpdate[] states);
}
