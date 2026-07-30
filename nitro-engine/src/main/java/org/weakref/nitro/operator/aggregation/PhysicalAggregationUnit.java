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

import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.Vector;

/**
 * One execution unit in an already-lowered physical aggregation program.
 *
 * <p>A unit owns one state lifetime and may expose any number of result slots. The aggregation
 * operators schedule units and route their declared result slots; they do not infer relationships
 * between functions or rewrite the supplied program. Each result slot must materialize an
 * independently owned output bundle; slots must not alias mutable vectors.
 */
public interface PhysicalAggregationUnit
{
    int outputCount();

    default int stateCapacity(int requiredGroups, int defaultCapacity)
    {
        return defaultCapacity;
    }

    default int[] distinctInputColumns()
    {
        return null;
    }

    default int filterInputColumn()
    {
        return -1;
    }

    Object allocate(AggregationExecutionContext context, int size);

    Object grow(Allocator allocator, Allocator.Context allocationContext, Object state, int size);

    void initialize(Object state, int offset, int length);

    void accumulate(Object state, int group, Mask mask, StreamAccessor streams);

    default void accumulateDistinctSelected(Object state, int group, Mask mask, StreamAccessor streams)
    {
        accumulate(state, group, mask, streams);
    }

    void accumulate(Object state, Vector groups, Mask mask, StreamAccessor streams);

    default void accumulateDistinctSelected(Object state, Vector groups, Mask mask, StreamAccessor streams)
    {
        accumulate(state, groups, mask, streams);
    }

    default Streams result(int output, int maxGroup, Object state, Mask mask, Streams existing, Allocator allocator, Allocator.Context allocationContext)
    {
        return result(output, maxGroup, state, existing, allocator, allocationContext);
    }

    default Streams copyResultPosition(int output, int group, int maxGroup, Object state, Streams existing, int outputPosition, int size, Allocator allocator, Allocator.Context allocationContext)
    {
        return null;
    }

    Streams result(int output, int maxGroup, Object state, Streams existing, Allocator allocator, Allocator.Context allocationContext);
}
