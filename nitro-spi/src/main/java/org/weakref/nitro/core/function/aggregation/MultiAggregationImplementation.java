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
package org.weakref.nitro.core.function.aggregation;

import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.Vector;

/**
 * Classloader-neutral state protocol for a planner-selected set of compatible aggregates that
 * share one raw-input traversal.
 *
 * <p>The registry owns aggregate compatibility and output meanings. The engine only schedules
 * the opaque state protocol and routes the declared result slots.
 */
public interface MultiAggregationImplementation
{
    int outputCount();

    /**
     * Selects the logical state capacity for a required group count.
     *
     * <p>The default preserves the engine's amortized-growth capacity. Implementations backed by
     * incrementally allocated storage may return {@code requiredGroups} to avoid unused geometric
     * headroom and whole-state copy peaks.
     */
    default int stateCapacity(int requiredGroups, int defaultCapacity)
    {
        return defaultCapacity;
    }

    Object allocate(AggregationExecution execution, int groups);

    Object grow(Allocator allocator, Allocator.Context allocationContext, Object state, int groups);

    void initialize(Object state, int offset, int length);

    void addRawInput(Object state, int group, Mask mask, AggregationInput input);

    void addRawInput(Object state, Vector groups, Mask mask, AggregationInput input);

    Streams intermediate(
            int output,
            int maxGroup,
            Object state,
            Streams existing,
            Allocator allocator,
            Allocator.Context allocationContext);

    default Streams intermediate(
            int output,
            int maxGroup,
            Object state,
            Mask mask,
            Streams existing,
            Allocator allocator,
            Allocator.Context allocationContext)
    {
        return intermediate(output, maxGroup, state, existing, allocator, allocationContext);
    }

    Streams result(
            int output,
            int maxGroup,
            Object state,
            Streams existing,
            Allocator allocator,
            Allocator.Context allocationContext);

    default Streams result(
            int output,
            int maxGroup,
            Object state,
            Mask mask,
            Streams existing,
            Allocator allocator,
            Allocator.Context allocationContext)
    {
        return result(output, maxGroup, state, existing, allocator, allocationContext);
    }
}
