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
 * {@code increment} call site is monomorphic and inlines.
 */
interface FusedGroupingKernel
{
    /**
     * Probes and accumulates the rows selected by {@code positions}/{@code count}.
     *
     * @param positions selected row positions, or {@code null} for a dense {@code 0..count-1} batch
     * @param count number of selected rows
     * @param keys the long group-key column values
     * @param tableKeys open-addressed table: key per slot
     * @param tableIds open-addressed table: group id per slot ({@code -1} when empty)
     * @param tableMask {@code capacity - 1} bitmask for the table
     * @param keysByGroup reverse map: key by group id
     * @param startNextId the next group id to assign
     * @param inputs per-accumulator value column ({@code inputs[a]} is {@code null} when accumulator
     *        {@code a} increments by a constant)
     * @param states per-accumulator state vector (each cast to its declared state-vector type)
     * @return the next group id after assigning any new groups encountered
     */
    long accumulate(
            int[] positions,
            int count,
            long[] keys,
            long[] tableKeys,
            int[] tableIds,
            int tableMask,
            long[] keysByGroup,
            long startNextId,
            long[][] inputs,
            Object[] states);
}
