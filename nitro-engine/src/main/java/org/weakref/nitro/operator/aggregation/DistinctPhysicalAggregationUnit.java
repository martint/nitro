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

import static java.util.Objects.requireNonNull;

/**
 * Declares the input-key identity for an exact DISTINCT aggregation while delegating the
 * function-specific state protocol unchanged.
 */
public final class DistinctPhysicalAggregationUnit
        implements PhysicalAggregationUnit
{
    private final PhysicalAggregationUnit delegate;
    private final int[] distinctInputColumns;

    public DistinctPhysicalAggregationUnit(PhysicalAggregationUnit delegate, int[] distinctInputColumns)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.distinctInputColumns = requireNonNull(distinctInputColumns, "distinctInputColumns is null").clone();
        if (this.distinctInputColumns.length == 0) {
            throw new IllegalArgumentException("distinctInputColumns is empty");
        }
    }

    @Override
    public int outputCount()
    {
        return delegate.outputCount();
    }

    @Override
    public int stateCapacity(int requiredGroups, int defaultCapacity)
    {
        return delegate.stateCapacity(requiredGroups, defaultCapacity);
    }

    @Override
    public int[] distinctInputColumns()
    {
        return distinctInputColumns.clone();
    }

    @Override
    public int filterInputColumn()
    {
        return delegate.filterInputColumn();
    }

    @Override
    public Object allocate(AggregationExecutionContext context, int size)
    {
        return delegate.allocate(context, size);
    }

    @Override
    public Object grow(Allocator allocator, Allocator.Context allocationContext, Object state, int size)
    {
        return delegate.grow(allocator, allocationContext, state, size);
    }

    @Override
    public void initialize(Object state, int offset, int length)
    {
        delegate.initialize(state, offset, length);
    }

    @Override
    public void accumulate(Object state, int group, Mask mask, StreamAccessor streams)
    {
        delegate.accumulate(state, group, mask, streams);
    }

    @Override
    public void accumulateDistinctSelected(Object state, int group, Mask mask, StreamAccessor streams)
    {
        delegate.accumulateDistinctSelected(state, group, mask, streams);
    }

    @Override
    public void accumulate(Object state, Vector groups, Mask mask, StreamAccessor streams)
    {
        delegate.accumulate(state, groups, mask, streams);
    }

    @Override
    public void accumulateDistinctSelected(Object state, Vector groups, Mask mask, StreamAccessor streams)
    {
        delegate.accumulateDistinctSelected(state, groups, mask, streams);
    }

    @Override
    public Streams result(
            int output,
            int maxGroup,
            Object state,
            Mask mask,
            Streams existing,
            Allocator allocator,
            Allocator.Context allocationContext)
    {
        return delegate.result(output, maxGroup, state, mask, existing, allocator, allocationContext);
    }

    @Override
    public Streams copyResultPosition(
            int output,
            int group,
            int maxGroup,
            Object state,
            Streams existing,
            int outputPosition,
            int size,
            Allocator allocator,
            Allocator.Context allocationContext)
    {
        return delegate.copyResultPosition(output, group, maxGroup, state, existing, outputPosition, size, allocator, allocationContext);
    }

    @Override
    public Streams copyResultRange(
            int output,
            int groupStart,
            int groupCount,
            int maxGroup,
            Object state,
            Streams existing,
            int outputStart,
            int size,
            Allocator allocator,
            Allocator.Context allocationContext)
    {
        return delegate.copyResultRange(output, groupStart, groupCount, maxGroup, state, existing, outputStart, size, allocator, allocationContext);
    }

    @Override
    public Streams result(
            int output,
            int maxGroup,
            Object state,
            Streams existing,
            Allocator allocator,
            Allocator.Context allocationContext)
    {
        return delegate.result(output, maxGroup, state, existing, allocator, allocationContext);
    }
}
