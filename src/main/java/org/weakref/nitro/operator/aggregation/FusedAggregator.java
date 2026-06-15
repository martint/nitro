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
package org.weakref.nitro.operator.aggregation;

/**
 * Marks an {@link Accumulator} whose per-row update is a single {@code stateVector.increment(group,
 * amount)} call, so it can participate in the fused single-long-key grouped-aggregation kernel — one
 * inlined pass that probes the group table and accumulates with no group-id vector and no per-row
 * accumulator dispatch.
 */
public interface FusedAggregator
        extends Accumulator
{
    /**
     * Declares how the fused kernel updates this accumulator's state per row.
     */
    FusedAccumulatorSpec fusedSpec();
}
