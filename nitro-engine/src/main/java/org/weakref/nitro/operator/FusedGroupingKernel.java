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

/**
 * One fused pass over a batch for single-long-key grouped aggregation: probe the open-addressed
 * group table and accumulate every selected accumulator's state in the same loop, with no group-id
 * vector round-trip and no per-row accumulator dispatch.
 * <p>
 * Implementations are generated per accumulator-set shape by
 * {@link FusedGroupingAggregationKernelGenerator}; the body is emitted as bytecode so each
 * state-update call site links an exact provider handle without naming the provider state class.
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
     * @param keyOffset physical base offset added after optional dictionary mapping
     * @param tableKeys open-addressed table: key per slot, or empty when the physical shape resolves exact keys
     *        through {@code keysByGroup}
     * @param tableIds open-addressed table: either a group id per slot ({@code -1} when empty), or a packed hash
     *        fragment plus {@code group+1} ({@code 0} when empty) for an id-indexed physical shape
     * @param tableMask {@code capacity - 1} bitmask for the table
     * @param keysByGroup reverse map: key by group id
     * @param startNextId the next group id to assign
     * @param inputs primitive base value columns flattened in accumulator/contribution order
     *        ({@code int[]}, {@code long[]}, or {@code double[]}); an entry is null for a constant contribution
     * @param inputIds optional per-contribution dictionary ids mapping logical positions into {@code inputs}
     * @param inputOffsets physical base offsets added after optional dictionary mapping
     * @param inputNulls per-contribution null base column, or {@code null} when that input is known null-free
     * @param inputNullIds optional per-contribution dictionary ids mapping logical positions into {@code inputNulls}
     * @param inputNullOffsets physical base offsets added after optional dictionary mapping
     * @param states per-accumulator opaque state-update target
     * @return the next group id after assigning any new groups encountered
     */
    long accumulate(
            int[] positions,
            int count,
            Object keys,
            int[] keyIds,
            int keyOffset,
            long[] tableKeys,
            int[] tableIds,
            int tableMask,
            long[] keysByGroup,
            long startNextId,
            long[] outputGroups,
            Object[] inputs,
            int[][] inputIds,
            int[] inputOffsets,
            boolean[][] inputNulls,
            int[][] inputNullIds,
            int[] inputNullOffsets,
            Object[] states);
}
