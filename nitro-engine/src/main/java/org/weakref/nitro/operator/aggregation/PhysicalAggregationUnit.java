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

import org.weakref.nitro.core.function.aggregation.GroupedAggregationDomain;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.ValueDemand;
import org.weakref.nitro.data.Vector;

import java.util.Map;

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
    /** Required physical content keyed by aggregation-program input channel. */
    default Map<Integer, ValueDemand> inputValueDemands()
    {
        return Map.of();
    }

    /** Returns an equivalent unit whose intermediate outputs may use provider-owned physical streams. */
    default PhysicalAggregationUnit physicalIntermediateOutput()
    {
        return this;
    }

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

    /** Whether {@link #accumulate(Object, Vector, Mask, StreamAccessor)} accepts encoded group-id vectors. */
    default boolean supportsEncodedGroupedInput()
    {
        return false;
    }

    default boolean supportsGroupedDomainInput(StreamAccessor streams)
    {
        return false;
    }

    default boolean supportsGroupedDomainInput(GroupedAggregationDomain domain, StreamAccessor streams)
    {
        return supportsGroupedDomainInput(streams);
    }

    default boolean supportsGroupedDomainInput(DictionaryVector rowMapping, StreamAccessor streams)
    {
        return supportsGroupedDomainInput(streams);
    }

    default boolean requiresGroupedDomainRepresentatives(DictionaryVector rowMapping, StreamAccessor streams)
    {
        return false;
    }

    default void accumulateGroupedDomain(Object state, GroupedAggregationDomain domain, StreamAccessor streams)
    {
        throw new UnsupportedOperationException("grouped domain input is not supported");
    }

    default void accumulateDistinctSelected(Object state, Vector groups, Mask mask, StreamAccessor streams)
    {
        accumulate(state, groups, mask, streams);
    }

    default boolean supportsInitialInput()
    {
        return false;
    }

    default Streams initialInput(
            int output,
            Mask mask,
            StreamAccessor streams,
            Allocator allocator,
            Allocator.Context allocationContext)
    {
        throw new UnsupportedOperationException("direct initial input is not supported");
    }

    default boolean supportsPositionPreservingInitialInput()
    {
        return false;
    }

    default Streams positionPreservingInitialInput(
            int output,
            Mask mask,
            StreamAccessor streams,
            Allocator allocator,
            Allocator.Context allocationContext)
    {
        throw new UnsupportedOperationException("position-preserving initial input is not supported");
    }

    default Streams result(int output, int maxGroup, Object state, Mask mask, Streams existing, Allocator allocator, Allocator.Context allocationContext)
    {
        return result(output, maxGroup, state, existing, allocator, allocationContext);
    }

    default Streams copyResultPosition(int output, int group, int maxGroup, Object state, Streams existing, int outputPosition, int size, Allocator allocator, Allocator.Context allocationContext)
    {
        return null;
    }

    default Streams copyResultRange(int output, int groupStart, int groupCount, int maxGroup, Object state, Streams existing, int outputStart, int size, Allocator allocator, Allocator.Context allocationContext)
    {
        return null;
    }

    Streams result(int output, int maxGroup, Object state, Streams existing, Allocator allocator, Allocator.Context allocationContext);
}
